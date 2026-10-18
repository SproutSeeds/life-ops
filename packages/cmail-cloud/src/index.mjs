import baseWorker, { MailQueue as BaseQueue, createWorker, sha256Hex, encryptArchivePayload, storeArchiveEnvelope, verifyLifeOpsRequest, PostalMime } from './production-base.mjs';
import { SUPPORT_MAILBOX, SYNC_STATE, normalizePayload, sourceKey, decodeRaw, syncReceived } from './resend.mjs';
import { handleReply } from './reply.mjs';
import { withReplies } from './reply-http.mjs';

const ADMIN_ROUTES = new Map([
  ['/api/mail/resend/sync', '/resend/sync'],
  ['/api/mail/resend/status', '/resend/status'],
  ['/api/mail/archive/import', '/resend/import'],
  ['/api/mail/archive/verify', '/resend/verify'],
]);
const json = (value, status = 200) => Response.json(value, { status, headers: { 'Cache-Control': 'private, no-store', 'X-Content-Type-Options': 'nosniff' } });

export class MailQueue extends BaseQueue {
  constructor(state, env) {
    super(state, env);
    this.indexReady = state.blockConcurrencyWhile(() => this.ensureDateIndex());
  }

  dateKey(record) {
    const time = Date.parse(record.received_at);
    return `cmail-date:${new Date(Number.isFinite(time) ? time : 0).toISOString()}:${record.id}`;
  }

  async ensureDateIndex() {
    const storage = this.state.storage;
    const meta = await storage.get('meta') || {};
    if (await storage.get('cmail-date-count') === Number(meta.total_stored || 0)) return;
    const records = await storage.list({ prefix: 'mail:' });
    const entries = [...records.values()];
    for (let offset = 0; offset < entries.length; offset += 100) {
      await storage.put(Object.fromEntries(entries.slice(offset, offset + 100).map((record) => [this.dateKey(record), record.id])));
    }
    await storage.put('cmail-date-count', entries.length);
  }

  async listMailRecords(options) {
    await this.indexReady;
    const indexed = { ...options, prefix: 'cmail-date:' };
    if (options.end) {
      const cursorRecord = await this.state.storage.get(options.end);
      if (!cursorRecord) return new Map();
      indexed.end = this.dateKey(cursorRecord);
    }
    const ids = [...(await this.state.storage.list(indexed)).values()];
    const records = new Map();
    for (const id of ids) {
      const record = await this.state.storage.get(`mail:${id}`);
      if (record) records.set(`mail:${id}`, record);
    }
    return records;
  }

  // All writers, including the original Cloudflare email receiver, use the same
  // atomic allocation. Historical imports never enter the local delivery queue.
  async enqueue(input, { archiveOnly = false, read = false, archived = false } = {}) {
    await this.indexReady;
    const payload = await normalizePayload(input);
    const fingerprint = payload.payload_hash;
    const source = sourceKey(payload);
    return this.state.storage.transaction(async (storage) => {
      const existingId = source && await storage.get(source) || await storage.get(`dedupe:${fingerprint}`);
      if (existingId) {
        if (source) await storage.put(source, existingId);
        return { stored: false, duplicate: true, id: existingId };
      }
      const meta = await storage.get('meta') || { next_seq: 0, total_stored: 0, total_acknowledged: 0 };
      const seq = Number(meta.next_seq || 0) + 1;
      const id = `mail_${String(seq).padStart(16, '0')}`;
      const receivedAt = payload.received_at || new Date().toISOString();
      const envelope = await encryptArchivePayload({
        masterKey: this.env.LIFE_OPS_MASTER_KEY, purpose: 'cloudflare-mail-archive-v1', payload,
        metadata: { id, seq, fingerprint, received_at: receivedAt },
      });
      const ref = await storeArchiveEnvelope({ storage, id, envelope });
      await storage.put(`mail:${id}`, {
        id, seq, fingerprint, received_at: receivedAt, acknowledged: archiveOnly,
        acknowledged_at: archiveOnly ? new Date().toISOString() : null, payload_ref: ref,
      });
      await storage.put(this.dateKey({ id, received_at: receivedAt }), id);
      await storage.put('cmail-date-count', Number(meta.total_stored || 0) + 1);
      if (!archiveOnly) await storage.put(`queue:${String(seq).padStart(16, '0')}`, id);
      if (read) await storage.put(`cmail-read:${id}`, true);
      if (archived) {
        await storage.put(`cmail-archived:${id}`, true);
        await storage.put('cmail-archived-count', Number(await storage.get('cmail-archived-count') || 0) + 1);
      }
      await storage.put(`dedupe:${fingerprint}`, id);
      if (source) await storage.put(source, id);
      await storage.put('meta', { ...meta, next_seq: seq, total_stored: Number(meta.total_stored || 0) + 1, total_acknowledged: Number(meta.total_acknowledged || 0) + (archiveOnly ? 1 : 0) });
      return { stored: true, duplicate: false, id, seq, sha256: fingerprint };
    });
  }

  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (path.startsWith('/reply/')) return handleReply(this, request);
    if (!path.startsWith('/resend/')) return super.fetch(request);
    if (request.method !== 'POST') return json({ error: 'method_not_allowed' }, 405);
    let input;
    try { input = await request.json(); } catch { return json({ error: 'invalid_json' }, 400); }
    if (path === '/resend/status') return json(await this.state.storage.get(SYNC_STATE) || { status: 'pending' });
    if (path === '/resend/sync') {
      if (!this.syncPromise) {
        this.syncPromise = syncReceived(this, this.env, { trigger: input.trigger === 'scheduled' ? 'scheduled' : 'manual', verifyExisting: input.verify_existing === true && input.trigger !== 'scheduled' }).finally(() => { this.syncPromise = null; });
      }
      const result = await this.syncPromise;
      return json(result, result.status === 'failed' ? 503 : 200);
    }
    if (path === '/resend/import') {
      if (input.envelope_to !== SUPPORT_MAILBOX || !['resend-receiving', 'resend-sent-history'].includes(input.provider) || !['inbound', 'outbound'].includes(input.direction) || !Number.isFinite(Date.parse(input.received_at))) return json({ error: 'invalid_import' }, 400);
      if ((input.direction === 'outbound') !== (input.provider === 'resend-sent-history')) return json({ error: 'invalid_import' }, 400);
      const { read, archived, ...payload } = input;
      try {
        return json(await this.enqueue(payload, { archiveOnly: true, read: read === true, archived: input.direction === 'outbound' || archived === true }));
      } catch (error) {
        if (/^(invalid_|raw_hash_mismatch)/.test(error.message || '')) return json({ error: error.message }, 400);
        throw error;
      }
    }
    if (path === '/resend/verify') {
      if (!Array.isArray(input.ids) || input.ids.length > 10 || input.ids.some((id) => !/^mail_\d{16}$/.test(id))) return json({ error: 'invalid_ids' }, 400);
      const items = [];
      for (const id of input.ids) {
        const record = await this.state.storage.get(`mail:${id}`);
        if (!record) { items.push({ id, missing: true }); continue; }
        const payload = await this.loadPayload(record);
        const raw = decodeRaw(payload.raw_base64);
        const parsed = await PostalMime.parse(raw);
        items.push({ id, sha256: await sha256Hex(raw), bytes: raw.length, mailbox: payload.envelope_to,
          direction: payload.direction || 'inbound', message_id: parsed.messageId || '', date: parsed.date || '',
          read: await this.state.storage.get(`cmail-read:${id}`) === true,
          archived: await this.state.storage.get(`cmail-archived:${id}`) === true,
          attachments: await Promise.all((parsed.attachments || []).map(async (attachment) => ({ bytes: attachment.content.byteLength, sha256: await sha256Hex(attachment.content) }))),
        });
      }
      return json({ items });
    }
    return json({ error: 'not_found' }, 404);
  }
}

export function withResend(worker = baseWorker) {
  return {
    email: worker.email,
    async scheduled(event, env, context) {
      const work = env.MAIL_QUEUE.getByName('primary').fetch('https://mail-queue.internal/resend/sync', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ trigger: 'scheduled' }),
      }).then(async (response) => {
        if (!response.ok) { await response.body?.cancel(); throw new Error('resend_scheduled_sync_failed'); }
        await response.body?.cancel();
      });
      context.waitUntil(work);
      await work;
    },
    async fetch(request, env, context) {
      const url = new URL(request.url);
      const route = ADMIN_ROUTES.get(url.pathname);
      if (route) {
        if (request.method !== 'POST') return json({ error: 'method_not_allowed' }, 405);
        if (!env.LIFE_OPS_MAIL_INGEST_SECRET) return json({ error: 'unconfigured' }, 503);
        const body = await request.text();
        if (body.length > 42 * 1024 * 1024) return json({ error: 'body_too_large' }, 413);
        const verified = await verifyLifeOpsRequest({ secret: env.LIFE_OPS_MAIL_INGEST_SECRET,
          timestamp: request.headers.get('X-Life-Ops-Timestamp') || '', signature: request.headers.get('X-Life-Ops-Signature') || '', body, maxSkewSeconds: 300 });
        if (!verified.ok) return json({ error: verified.reason }, 401);
        return env.MAIL_QUEUE.getByName('primary').fetch(`https://mail-queue.internal${route}`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' }, body,
        });
      }
      // Existing authentication, message rendering, attachments and navigation
      // remain in the recovered application. Identity config now includes support.
      const mailboxes = [...new Set(`${env.CMAIL_MAILBOXES || ''},${SUPPORT_MAILBOX}`.split(',').map((value) => value.trim()).filter(Boolean))].join(',');
      return worker.fetch(request, { ...env, CMAIL_MAILBOXES: mailboxes }, context);
    },
  };
}

export { createWorker };
export default withReplies(withResend());
