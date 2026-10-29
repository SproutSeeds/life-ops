import { sha256Hex } from './production-base.mjs';
import { getMany, putMany } from './mail-index.mjs';
import { prepareReply, sendingIdentities, sendCompose, sendReply } from './reply.mjs';
import { brandedMessage, signatureLogo } from './signature.mjs';
import { attachmentHttp } from './attachments.mjs';

const P = 'cmail-drafts-v1:';
const UUID = '[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}';
const ID = new RegExp('^draft_' + UUID + '$'), SAVE = new RegExp('^' + UUID + '$');
const CURSOR = new RegExp('^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z:draft_' + UUID + '$');
const bad = (message, status = 400) => Object.assign(new Error(message), { status });
const hash = (value) => sha256Hex(new TextEncoder().encode(value));
const order = (scope) => P + 'order:' + scope + ':';
const recordKey = (id) => P + 'record:' + id;
const chunkKey = (id, n) => P + 'body:' + id + ':' + n;
const active = (record) => record && ['editing', 'sending'].includes(record.state);

export class DraftStore {
  constructor(queue) { this.queue = queue; this.storage = queue.state.storage; }
  validateId(id) { if (!ID.test(id || '')) throw bad('invalid_draft'); }
  async body(record, storage = this.storage) {
    if (!record.chunks) {
      const envelope = await storage.get(P + 'content:' + record.id);
      if (!envelope) throw bad('draft_unavailable', 503);
      return this.queue.views.unseal(envelope);
    }
    const keys = Array.from({ length: record.chunks }, (_, n) => chunkKey(record.id, n));
    const chunks = await getMany(storage, keys);
    if (keys.some((key) => !chunks.has(key))) throw bad('draft_unavailable', 503);
    return this.queue.views.unseal(JSON.parse(keys.map((key) => chunks.get(key)).join('')));
  }
  async writeBody(tx, record, value) {
    const envelope = await this.queue.views.seal(value), serialized = JSON.stringify(envelope), entries = {};
    const oldChunks = record.chunks || 0;
    record.chunks = serialized.length < 90000 ? 0 : Math.ceil(serialized.length / 60000);
    if (record.chunks) await tx.delete(P + 'content:' + record.id);
    else entries[P + 'content:' + record.id] = envelope;
    for (let n = 0; n < record.chunks; n++) entries[chunkKey(record.id, n)] = serialized.slice(n * 60000, (n + 1) * 60000);
    for (let n = record.chunks; n < oldChunks; n++) await tx.delete(chunkKey(record.id, n));
    await putMany(tx, entries);
    record.summary = await this.queue.views.seal({ id: record.id, revision: record.revision, sender: value.sender, to: value.to,
      pending: value.pending, subject: value.subject, preview: value.text.replace(/\s+/g, ' ').slice(0, 180),
      kind: value.kind, reply_id: value.reply_id || '', cc: value.cc || [], bcc: value.bcc || [], attachment_count: value.attachments?.length || 0, updated_at: record.updated_at, state: record.state });
  }
  async move(tx, before, after) {
    const changes = new Map();
    for (const [record, delta] of [[before, -1], [after, 1]]) {
      if (!active(record)) continue;
      for (const scope of ['*', record.scope]) {
        const key = order(scope) + record.updated_at + ':' + record.id;
        if (delta === -1) await tx.delete(key); else await tx.put(key, record.id);
        changes.set(scope, (changes.get(scope) || 0) + delta);
      }
    }
    const counts = await getMany(tx, [...changes.keys()].map((scope) => P + 'count:' + scope));
    await putMany(tx, Object.fromEntries([...changes].map(([scope, delta]) => [P + 'count:' + scope, Number(counts.get(P + 'count:' + scope) || 0) + delta])));
  }
  normalize(value) {
    if (!value || !['compose', 'reply'].includes(value.kind) || typeof value.sender !== 'string' || !Object.hasOwn(sendingIdentities(this.queue.env), value.sender) ||
      !Array.isArray(value.to) || value.to.length > 10 || value.to.some((item) => typeof item !== 'string' || item.length > 254 || /[\r\n\0]/.test(item)) ||
      typeof value.pending !== 'string' || value.pending.length > 3000 || value.pending.includes('\0') ||
      typeof value.subject !== 'string' || value.subject.length > 1000 || /[\r\n\0]/.test(value.subject) ||
      typeof value.text !== 'string' || value.text.length > 100000 || value.text.includes('\0') ||
      typeof value.signature !== 'string' || value.signature.length > 10000 ||
      value.kind === 'reply' && !/^mail_\d{16}$/.test(value.reply_id || '')) throw bad('invalid_draft');
    for (const values of [value.cc || [], value.bcc || []]) if (!Array.isArray(values) || values.length > 10 || values.some((v) => typeof v !== 'string' || v.length > 254 || /[\r\n\0]/.test(v))) throw bad('invalid_draft');
    const messageId = /^<[\x21-\x3b\x3d\x3f-\x7e]{1,247}>$/;
    if (value.in_reply_to && (typeof value.in_reply_to !== 'string' || !messageId.test(value.in_reply_to)) ||
      value.references && (!Array.isArray(value.references) || value.references.length > 20 || value.references.some((v) => typeof v !== 'string' || !messageId.test(v)))) throw bad('invalid_thread_headers');
    if (value.kind === 'reply' && (value.cc?.length || value.bcc?.length)) throw bad('reply_recipients_locked');
    return { kind: value.kind, sender: value.sender, to: [...value.to], pending: value.pending, subject: value.subject,
      text: value.text, signature: value.signature, ...(value.kind === 'reply' ? { reply_id: value.reply_id } : {}),
      ...(value.cc?.length ? { cc: [...value.cc] } : {}), ...(value.bcc?.length ? { bcc: [...value.bcc] } : {}),
      ...(value.in_reply_to ? { in_reply_to: value.in_reply_to } : {}), ...(value.references?.length ? { references: [...value.references] } : {}) };
  }
  async save(input) {
    this.validateId(input.id);
    if (!Number.isSafeInteger(input.revision) || input.revision < 0 || !SAVE.test(input.save_id || '')) throw bad('invalid_draft');
    let value = this.normalize(input.draft);
    if (Object.hasOwn(input.draft, 'attachments')) value.attachments = await this.queue.attachments.validate(input.draft.attachments);
    const digest = await hash(JSON.stringify(value));
    // Reply routing comes from the archived message; client edits cannot change
    // its sender, recipients, subject, or thread headers.
    if (value.kind === 'reply') {
      const context = await prepareReply(this.queue, value.reply_id);
      value = { ...value, sender: context.sender, to: context.to, pending: '', subject: context.subject, signature: context.signature };
    }
    // Freeze the rendered alternative with this saved revision. Client-supplied
    // HTML is ignored, and a later signature change cannot alter an approved send.
    const identity = sendingIdentities(this.queue.env)[value.sender];
    const logo = signatureLogo(identity);
    if (logo) {
      value.html = brandedMessage(identity, value.text);
      value.signature = identity.signature;
      value.signature_logo = value.html ? logo : null;
    }
    const scope = await hash(value.sender);
    await this.storage.transaction(async (tx) => {
      const previous = await tx.get(recordKey(input.id));
      if (previous && !active(previous)) throw bad('draft_closed', 410);
      if (previous?.state === 'sending') throw bad('draft_sending', 409);
      if (previous?.save_id === input.save_id) {
        if (previous.digest !== digest) throw bad('draft_changed', 409);
        return;
      }
      if ((previous?.revision || 0) !== input.revision) throw bad('draft_conflict', 409);
      const old = previous ? await this.body(previous, tx) : {};
      if (previous) {
        if (old.kind !== value.kind || old.reply_id !== value.reply_id) throw bad('draft_changed', 409);
        // Tabs opened before CC/header support omit these fields entirely.
        // Preserve them unless a current client explicitly supplies [] or ''.
        for (const field of ['cc', 'bcc', 'in_reply_to', 'references', 'attachments']) {
          if (!Object.hasOwn(input.draft, field) && old[field] !== undefined) value[field] = old[field];
        }
      }
      const now = new Date().toISOString();
      const record = { ...previous, id: input.id, revision: input.revision + 1, scope, save_id: input.save_id, digest,
        state: 'editing', created_at: previous?.created_at || now, updated_at: now };
      await this.queue.attachments.references(tx, input.id, old.attachments, value.attachments);
      await this.writeBody(tx, record, value); await this.move(tx, previous, record); await tx.put(recordKey(input.id), record);
      if (value.reply_id) await tx.put(P + 'reply:' + value.reply_id, input.id);
    });
    return this.get(input.id);
  }
  async finish(id, result) {
    return this.storage.transaction(async (tx) => {
      const before = await tx.get(recordKey(id));
      if (!before || before.state !== 'sending' || result.request_id && result.request_id !== before.request_id) return before;
      const record = { ...before };
      if (result.state === 'sent') {
        await this.queue.attachments.references(tx, id, (await this.body(before, tx)).attachments);
        record.state = 'sent'; record.archive_id = result.archive_id; record.revision++;
        for (let n = 0; n < (record.chunks || 0); n++) await tx.delete(chunkKey(id, n));
        await tx.delete(P + 'content:' + id);
        delete record.summary; record.chunks = 0;
      } else if (result.can_edit) {
        record.state = 'editing'; record.revision++; record.request_id = null;
        await this.writeBody(tx, record, await this.body(before, tx));
      }
      record.error = result.error || null;
      await this.move(tx, before, record); await tx.put(recordKey(id), record); return record;
    });
  }
  async get(id) {
    this.validateId(id);
    let record = await this.storage.get(recordKey(id));
    if (!record) throw bad('draft_not_found', 404);
    if (record.state === 'sending') {
      const receipt = await this.storage.get('cmail-outbox:' + record.request_id);
      if (receipt && ['sent', 'rejected'].includes(receipt.state)) record = await this.finish(id, { ...receipt, can_edit: receipt.state === 'rejected' });
    }
    return { id, revision: record.revision, state: record.state, updated_at: record.updated_at, request_id: record.request_id || null,
      archive_id: record.archive_id || null, error: record.error || null, ...(active(record) ? { draft: await this.body(record) } : {}) };
  }
  async discard({ id, revision }) {
    this.validateId(id);
    return this.storage.transaction(async (tx) => {
      const record = await tx.get(recordKey(id));
      if (!record) throw bad('draft_not_found', 404);
      if (record.state === 'discarded') return { id, state: 'discarded' };
      if (record.state !== 'editing') throw bad('draft_sending', 409);
      if (record.revision !== revision) throw bad('draft_conflict', 409);
      await this.queue.attachments.references(tx, id, (await this.body(record, tx)).attachments);
      await this.move(tx, record, null);
      for (let n = 0; n < (record.chunks || 0); n++) await tx.delete(chunkKey(id, n));
      await tx.delete(P + 'content:' + id);
      // Keep a small tombstone so a stale tab cannot resurrect a discarded draft.
      await tx.put(recordKey(id), { id, state: 'discarded', revision: revision + 1 });
      return { id, state: 'discarded' };
    });
  }
  async reply(id) {
    if (!/^mail_\d{16}$/.test(id || '')) throw bad('invalid_message');
    const saved = await this.storage.get(P + 'reply:' + id);
    if (saved) { const item = await this.get(saved); if (item.draft) return item; }
    const { credential, ...context } = await prepareReply(this.queue, id);
    return { context };
  }
  async send({ id, revision }, options) {
    this.validateId(id);
    const record = await this.storage.transaction(async (tx) => {
      const before = await tx.get(recordKey(id));
      if (!before || before.state === 'discarded') throw bad('draft_closed', 410);
      if (before.state === 'sent' || before.state === 'sending') return before;
      if (before.revision !== revision) throw bad('draft_conflict', 409);
      const value = await this.body(before, tx);
      if (value.pending.trim()) throw bad('invalid_recipients');
      const record = { ...before, state: 'sending', request_id: crypto.randomUUID() };
      await this.writeBody(tx, record, value); await tx.put(recordKey(id), record); return record;
    });
    if (record.state === 'sent') return this.get(id);
    const value = await this.body(record);
    options = { ...options, html: value.html };
    try {
      const result = value.kind === 'reply' ? await sendReply(this.queue, { id: value.reply_id, request_id: record.request_id, text: value.text, attachments: value.attachments }, options)
        : await sendCompose(this.queue, { ...value, request_id: record.request_id }, options);
      await this.finish(id, result);
    } catch (error) {
      if (error.status === 400) await this.finish(id, { can_edit: true, error: error.message, request_id: record.request_id });
      throw error;
    }
    return this.get(id);
  }
  async list({ mailbox = '', query = '', cursor = '', limit = 50 } = {}) {
    if (typeof mailbox !== 'string' || mailbox.length > 254 || typeof query !== 'string' || query.length > 200 ||
      typeof cursor !== 'string' || cursor && !CURSOR.test(cursor) || !Number.isInteger(limit) || limit < 1 || limit > 100) throw bad('invalid_draft_query');
    const scope = mailbox ? await hash(mailbox.toLowerCase()) : '*', prefix = order(scope);
    const size = query ? 100 : limit;
    const rows = [...await this.storage.list({ prefix, reverse: true, limit: size + 1, ...(cursor ? { end: prefix + cursor } : {}) })];
    const records = await getMany(this.storage, rows.slice(0, size).map(([, id]) => recordKey(id)));
    const items = [];
    for (const [, id] of rows.slice(0, size)) {
      const record = records.get(recordKey(id));
      if (!active(record)) continue;
      const summary = await this.queue.views.unseal(record.summary);
      if (query && ![summary.subject, summary.to.join(' '), (summary.cc || []).join(' '), (summary.bcc || []).join(' '), summary.pending, summary.sender, summary.preview].join(' ').toLowerCase().includes(query.toLowerCase())) continue;
      items.push(summary);
    }
    return { items, total: Number(await this.storage.get(P + 'count:' + scope) || 0),
      next_cursor: rows.length > size ? rows[size - 1][0].slice(prefix.length) : null };
  }
}

export async function handleDrafts(queue, request) {
  const json = (value, status = 200) => Response.json(value, { status, headers: { 'Cache-Control': 'private, no-store' } });
  try {
    const attachment = new URL(request.url).pathname.match(/^\/drafts\/attachments\/([a-f0-9-]{36})$/);
    if (attachment) return await attachmentHttp(queue, request, attachment[1]);
    if (request.method !== 'POST') return json({ error: 'method_not_allowed' }, 405);
    const input = await request.json(), path = new URL(request.url).pathname;
    if (path === '/drafts/status') return json({ ready: true, drafts: Number(await queue.state.storage.get(P + 'count:*') || 0) });
    if (path === '/drafts/list') return json(await queue.drafts.list(input));
    if (path === '/drafts/get') return json(await queue.drafts.get(input.id));
    if (path === '/drafts/reply') return json(await queue.drafts.reply(input.id));
    if (path === '/drafts/save') return json(await queue.drafts.save(input));
    if (path === '/drafts/discard') return json(await queue.drafts.discard(input));
    if (path === '/drafts/send') {
      const previous = queue.replyTail || Promise.resolve();
      const work = previous.catch(() => {}).then(() => queue.drafts.send(input));
      queue.replyTail = work.catch(() => {}); return json(await work);
    }
    return json({ error: 'not_found' }, 404);
  } catch (error) { return json({ error: error.status ? error.message : 'draft_unavailable' }, error.status || 503); }
}
