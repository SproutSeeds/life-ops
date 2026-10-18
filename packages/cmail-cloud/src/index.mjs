import baseWorker, { MailQueue as BaseQueue, createWorker, sha256Hex, encryptArchivePayload, storeArchiveEnvelope, verifyLifeOpsRequest, PostalMime } from './production-base.mjs';
import { SUPPORT_MAILBOX, SYNC_STATE, normalizePayload, sourceKey, decodeRaw, syncReceived } from './resend.mjs';
import { handleReply } from './reply.mjs';
import { withReplies } from './reply-http.mjs';
import { MailIndex } from './mail-index.mjs';
import { SentIndex } from './sent-index.mjs';

const ADMIN_ROUTES = new Map([
  ['/api/mail/resend/sync', '/resend/sync'],
  ['/api/mail/resend/status', '/resend/status'],
  ['/api/mail/archive/import', '/resend/import'],
  ['/api/mail/archive/verify', '/resend/verify'],
  ['/api/mail/performance/status', '/performance/status'],
  ['/api/mail/performance/backfill', '/performance/backfill'],
  ['/api/mail/performance/probe', '/performance/probe'],
  ['/api/mail/sent/status', '/sent/status'],
  ['/api/mail/sent/backfill', '/sent/backfill'],
]);
const json = (value, status = 200) => Response.json(value, { status, headers: { 'Cache-Control': 'private, no-store', 'X-Content-Type-Options': 'nosniff' } });

export class MailQueue extends BaseQueue {
  constructor(state, env) {
    super(state, env);
    this.views = new MailIndex(this);
    this.sent = new SentIndex(this);
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
    if (!payload.received_at) payload.received_at = new Date().toISOString();
    const fingerprint = payload.payload_hash;
    const source = sourceKey(payload);
    const prepared = await this.views.prepare(payload);
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
      await this.views.put(storage, { id, received_at: receivedAt }, prepared, archived);
      await this.sent.put(storage, id, prepared.locator, payload);
      return { stored: true, duplicate: false, id, seq, sha256: fingerprint };
    });
  }

  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (this.env.CMAIL_FAST_READS === '1' && path === '/archive/list') {
      if (request.method !== 'POST') return json({ error: 'method_not_allowed' }, 405);
      try { return json(await this.listArchive(await request.json())); }
      catch (error) { return json({ error: error.status ? error.message : 'mail_temporarily_unavailable' }, error.status || 503); }
    }
    if (path === '/sent/status' || path === '/sent/backfill') {
      if (request.method !== 'POST') return json({ error: 'method_not_allowed' }, 405);
      try {
        const input = await request.json();
        if (path === '/sent/status') return json(await this.sent.readiness());
        this.sentBackfillPromise ||= this.sent.backfill(input).finally(() => { this.sentBackfillPromise = null; });
        return json(await this.sentBackfillPromise);
      } catch (error) { return json({ error: error.status ? error.message : 'sent_index_unavailable' }, error.status || 503); }
    }
    if (path.startsWith('/performance/') || path === '/archive/conversation') {
      if (request.method !== 'POST') return json({ error: 'method_not_allowed' }, 405);
      try {
        const input = await request.json();
        if (path === '/performance/status') return json(await this.views.readiness());
        if (path === '/performance/backfill') {
          this.backfillPromise ||= this.views.backfill(input).finally(() => { this.backfillPromise = null; });
          return json(await this.backfillPromise);
        }
        if (path === '/performance/probe') return json(await this.performanceProbe(input));
        const result = await this.views.conversation(input);
        return json(result, result ? 200 : 404);
      } catch (error) { return json({ error: error.status ? error.message : 'index_unavailable' }, error.status || 503); }
    }
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
      const allowed = String(this.env.CMAIL_MAILBOXES || '').split(',').map((value) => value.trim().toLowerCase());
      const mailboxAllowed = input.envelope_to === SUPPORT_MAILBOX || input.direction === 'outbound' && allowed.includes(input.envelope_to);
      if (!mailboxAllowed || !['resend-receiving', 'resend-sent-history'].includes(input.provider) || !['inbound', 'outbound'].includes(input.direction) || !Number.isFinite(Date.parse(input.received_at))) return json({ error: 'invalid_import' }, 400);
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

  async listArchive(parameters) {
    if (parameters?.view === 'sent' && this.env.CMAIL_SENT_ENABLED === '1') return this.sent.list(parameters);
    return this.env.CMAIL_FAST_READS === '1' ? this.views.list(parameters) : super.listArchive(parameters);
  }
  async listThreadIndex(parameters = {}) {
    if (this.env.CMAIL_FAST_READS !== '1') return super.listThreadIndex(parameters);
    // Older open tabs still request this route until their next reload. They
    // receive compact indexed metadata, never another raw-archive scan.
    const page = await this.views.list({ cursor: parameters.cursor, view: 'all', limit: 100 });
    return { ...page, items: page.items.map(({ id, received_at, date, message_id, in_reply_to, references }) => ({ id, received_at, date, message_id, in_reply_to, references })) };
  }
  async readArchiveMessage(parameters = {}) {
    if (this.env.CMAIL_FAST_READS !== '1' || parameters.raw || parameters.attachment !== undefined) return super.readArchiveMessage(parameters);
    return await this.views.message(parameters.id) || super.readArchiveMessage(parameters);
  }
  async applyAction({ ids, action } = {}) {
    if (!Array.isArray(ids) || !ids.length || ids.length > 100 || ids.some((id) => !/^mail_\d{16}$/.test(id)) || new Set(ids).size !== ids.length || !['read', 'unread', 'archive', 'unarchive'].includes(action)) return super.applyAction({ ids, action });
    return this.state.storage.transaction(async (tx) => {
      for (const id of ids) if (!await tx.get(`mail:${id}`)) return null;
      let count = Number(await tx.get('cmail-archived-count') || 0);
      const items = [];
      for (const id of ids) {
        let read = await tx.get(`cmail-read:${id}`) === true, archived = await tx.get(`cmail-archived:${id}`) === true;
        if (action === 'read' || action === 'unread') { read = action === 'read'; await tx.put(`cmail-read:${id}`, read); }
        else {
          const next = action === 'archive'; count += Number(next) - Number(archived);
          await this.views.moveArchive(tx, id, archived, next); archived = next;
          await tx.put(`cmail-archived:${id}`, archived);
        }
        items.push({ id, read, archived });
      }
      await tx.put('cmail-archived-count', Math.max(0, count));
      return { items };
    });
  }
  async performanceProbe({ operation = 'list', legacy = false, parameters = {} } = {}) {
    if (!['list', 'conversation', 'message'].includes(operation)) throw Object.assign(new Error('invalid_request'), { status: 400 });
    const metrics = { get_calls: 0, list_calls: 0, keys_read: 0, raw_payload_loads: 0 };
    const storage = this.state.storage;
    const facade = Object.create(this);
    facade.state = { storage: {
      async get(key) { metrics.get_calls++; metrics.keys_read += Array.isArray(key) ? key.length : 1; return storage.get(key); },
      async list(options) { metrics.list_calls++; const rows = await storage.list(options); metrics.keys_read += rows.size; return rows; },
    } };
    facade.loadPayload = async (record) => { if (record.id?.startsWith('mail_')) metrics.raw_payload_loads++; return BaseQueue.prototype.loadPayload.call(facade, record); };
    const views = new MailIndex(facade);
    facade.views = views; facade.sent = new SentIndex(facade);
    const started = performance.now();
    const result = legacy ? await (operation === 'list' ? BaseQueue.prototype.listArchive.call(facade, parameters) : operation === 'conversation' ? BaseQueue.prototype.listThreadIndex.call(facade, parameters) : BaseQueue.prototype.readArchiveMessage.call(facade, parameters))
      : await (operation === 'list' ? parameters.view === 'sent' ? facade.sent.list(parameters) : views.list(parameters) : operation === 'conversation' ? views.conversation(parameters) : views.message(parameters.id));
    return { operation, legacy, milliseconds: Math.round((performance.now() - started) * 100) / 100, ...metrics,
      items: result?.items?.length ?? (result ? 1 : 0), scanned: result?.scanned ?? null, more: Boolean(result?.next_cursor), total: result?.total_stored ?? result?.total ?? null };
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
