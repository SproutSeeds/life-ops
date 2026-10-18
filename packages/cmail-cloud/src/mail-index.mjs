import { PostalMime, encryptArchivePayload, decryptArchivePayload, storeArchiveEnvelope, sha256Hex, plainTextFromHtml } from './production-base.mjs';
import { decodeRaw } from './resend.mjs';

const PREFIX = 'cmail-v2:';
const PURPOSE = 'cloudflare-mail-archive-v1';
const ID = /^mail_\d{16}$/;
const encoder = new TextEncoder();
const bad = (message = 'invalid_request', status = 400) => Object.assign(new Error(message), { status });
const address = (value) => value?.group ? value.group.map(address).join(', ') : value?.name ? `${value.name} <${value.address || ''}>` : value?.address || '';
export const messageTokens = (value) => {
  const text = Array.isArray(value) ? value.join(' ') : String(value || '');
  return [...new Set(text.match(/<[^<>\s]+>/g) || text.split(/[\s,]+/).filter((v) => v.includes('@')).map((v) => `<${v.replace(/^<|>$/g, '')}>`))];
};
const digest = (value) => sha256Hex(encoder.encode(value));
const orderPrefix = (scope, view) => `${PREFIX}order:${scope}:${view}:`;
const countKey = (scope, view) => `${PREFIX}count:${scope}:${view}`;
const locatorKey = (id) => PREFIX + 'locator:' + id;
const summaryKey = (id) => PREFIX + 'summary:' + id;
const bodyKey = (id) => PREFIX + 'body:' + id;

// Keep storage requests bounded below the legacy KV 128-key limit too.
export async function getMany(storage, keys) {
  const result = new Map();
  for (let offset = 0; offset < keys.length; offset += 128) {
    for (const [key, value] of await storage.get(keys.slice(offset, offset + 128))) result.set(key, value);
  }
  return result;
}
export async function putMany(storage, entries) {
  const values = Object.entries(entries);
  for (let offset = 0; offset < values.length; offset += 128) await storage.put(Object.fromEntries(values.slice(offset, offset + 128)));
}

export class MailIndex {
  constructor(queue) { this.queue = queue; this.storage = queue.state.storage; this.summaryCache = new Map(); this.summaryBytes = 0; this.bodyCache = new Map(); this.cacheBytes = 0; }
  async seal(payload) { return encryptArchivePayload({ masterKey: this.queue.env.LIFE_OPS_MASTER_KEY, purpose: PURPOSE, payload }); }
  async unseal(envelope) { return decryptArchivePayload({ masterKey: this.queue.env.LIFE_OPS_MASTER_KEY, purpose: PURPOSE, envelope }); }

  async prepare(payload) {
    const parsed = await PostalMime.parse(decodeRaw(payload.raw_base64));
    const mailbox = String(payload.envelope_to || '').trim().toLowerCase();
    const date = Number.isFinite(Date.parse(payload.received_at)) ? new Date(payload.received_at).toISOString() : new Date(0).toISOString();
    const tokens = [...new Set([...messageTokens(parsed.messageId), ...messageTokens(parsed.inReplyTo), ...messageTokens(parsed.references)])];
    const summary = { received_at: date, date: parsed.date || date, from: address(parsed.from) || String(payload.envelope_from || ''),
      to: (parsed.to || []).map(address).join(', ') || mailbox, mailbox, subject: parsed.subject || '(No subject)',
      message_id: parsed.messageId || '', in_reply_to: parsed.inReplyTo || '', references: parsed.references || '' };
    const body = { ...summary, from: parsed.from, to: parsed.to || [], cc: parsed.cc || [], reply_to: parsed.replyTo || [],
      text: parsed.text || plainTextFromHtml(parsed.html) || '(This message has no text body.)',
      attachments: (parsed.attachments || []).map((file, index) => ({ index, filename: file.filename || `attachment-${index + 1}`, mime_type: file.mimeType || 'application/octet-stream', size: file.content.byteLength })) };
    return { summary, body, locator: { date, scope: await digest(mailbox), tokens: await Promise.all(tokens.map(digest)) } };
  }

  async put(storage, record, prepared, archived) {
    if (await storage.get(locatorKey(record.id))) return false;
    const { summary, body, locator } = prepared;
    const suffix = `${locator.date}:${record.id}`;
    const summaryEnvelope = await this.seal({ ...summary, id: record.id });
    const entries = { [locatorKey(record.id)]: locator, [summaryKey(record.id)]: JSON.stringify(summaryEnvelope).length < 90000 ? { payload_encrypted: summaryEnvelope }
      : { payload_ref: await storeArchiveEnvelope({ storage, id: `summary_${record.id}`, envelope: summaryEnvelope }) } };
    const bodyEnvelope = await this.seal({ ...body, id: record.id });
    // Short bodies need one point read. Large text uses the same encrypted
    // chunk format as raw MIME, without downloading its attachment bytes.
    entries[bodyKey(record.id)] = JSON.stringify(bodyEnvelope).length < 90000 ? { payload_encrypted: bodyEnvelope }
      : { payload_ref: await storeArchiveEnvelope({ storage, id: `view_${record.id}`, envelope: bodyEnvelope }) };
    const scopes = ['*', locator.scope];
    const keys = scopes.flatMap((scope) => ['all', archived ? 'archive' : 'inbox'].map((view) => countKey(scope, view)));
    const counts = await getMany(storage, keys);
    for (const scope of scopes) for (const view of ['all', archived ? 'archive' : 'inbox']) {
      entries[orderPrefix(scope, view) + suffix] = record.id;
      entries[countKey(scope, view)] = Number(counts.get(countKey(scope, view)) || 0) + 1;
    }
    for (const token of locator.tokens) entries[`${PREFIX}edge:${token}:${record.id}`] = record.id;
    await putMany(storage, entries);
    return true;
  }

  async moveArchive(storage, id, previous, archived) {
    if (previous === archived) return;
    const locator = await storage.get(locatorKey(id));
    if (!locator) return; // Backfill reads the authoritative flags at commit.
    const from = previous ? 'archive' : 'inbox', to = archived ? 'archive' : 'inbox';
    const scopes = ['*', locator.scope];
    const keys = scopes.flatMap((scope) => [countKey(scope, from), countKey(scope, to)]);
    const counts = await getMany(storage, keys);
    for (const scope of scopes) {
      await storage.delete(orderPrefix(scope, from) + `${locator.date}:${id}`);
      await storage.put({ [orderPrefix(scope, to) + `${locator.date}:${id}`]: id,
        [countKey(scope, from)]: Math.max(0, Number(counts.get(countKey(scope, from)) || 0) - 1),
        [countKey(scope, to)]: Number(counts.get(countKey(scope, to)) || 0) + 1 });
    }
  }

  async readiness() {
    const values = await getMany(this.storage, ['meta', countKey('*', 'all')]);
    const total = Number(values.get('meta')?.total_stored || 0), indexed = Number(values.get(countKey('*', 'all')) || 0);
    return { ready: total === indexed, total, indexed };
  }
  async requireReady() { if (!(await this.readiness()).ready) throw bad('mail_index_warming', 503); }

  async summaries(ids) {
    // Retain this request's values even if another request evicts the shared
    // cache while decryption yields. Account for concurrent fills of one ID.
    const values = new Map(ids.filter((id) => this.summaryCache.has(id)).map((id) => [id, this.summaryCache.get(id)]));
    const missing = [...new Set(ids.filter((id) => !values.has(id)))];
    const envelopes = await getMany(this.storage, missing.map(summaryKey));
    for (const id of missing) {
      if (!envelopes.has(summaryKey(id))) throw bad('mail_index_incomplete', 503);
      const record = envelopes.get(summaryKey(id));
      const value = record.payload_encrypted ? await this.unseal(record.payload_encrypted) : await this.queue.loadPayload(record);
      values.set(id, value);
      if (this.summaryCache.has(id)) this.summaryBytes -= JSON.stringify(this.summaryCache.get(id)).length * 2;
      this.summaryCache.set(id, value); this.summaryBytes += JSON.stringify(value).length * 2;
      while (this.summaryCache.size > 500 || this.summaryBytes > 4 * 1024 * 1024) {
        const first = this.summaryCache.keys().next().value;
        this.summaryBytes -= JSON.stringify(this.summaryCache.get(first)).length * 2; this.summaryCache.delete(first);
      }
    }
    return ids.map((id) => values.get(id));
  }
  async states(ids, view = 'all') {
    const values = await getMany(this.storage, ids.flatMap((id) => view === 'all' ? [`cmail-read:${id}`, `cmail-archived:${id}`] : [`cmail-read:${id}`]));
    return new Map(ids.map((id) => [id, { read: values.get(`cmail-read:${id}`) === true, archived: view === 'all' ? values.get(`cmail-archived:${id}`) === true : view === 'archive' }]));
  }
  async body(id, prefetched) {
    if (this.bodyCache.has(id)) return this.bodyCache.get(id).value;
    const record = prefetched || await this.storage.get(bodyKey(id));
    if (!record) return null;
    const value = record.payload_encrypted ? await this.unseal(record.payload_encrypted) : await this.queue.loadPayload(record);
    const size = JSON.stringify(value).length * 2;
    if (size < 2 * 1024 * 1024) {
      if (this.bodyCache.has(id)) this.cacheBytes -= this.bodyCache.get(id).size;
      this.bodyCache.set(id, { value, size }); this.cacheBytes += size;
      while (this.cacheBytes > 4 * 1024 * 1024 || this.bodyCache.size > 40) {
        const first = this.bodyCache.keys().next().value; this.cacheBytes -= this.bodyCache.get(first).size; this.bodyCache.delete(first);
      }
    }
    return value;
  }
  async message(id) {
    if (!ID.test(id || '')) throw bad();
    const body = await this.body(id);
    if (!body) return null;
    return { ...body, ...(await this.states([id])).get(id) };
  }

  async list(parameters = {}) {
    const query = String(parameters.query || '').trim().toLowerCase(), mailbox = String(parameters.mailbox || '').trim().toLowerCase();
    const view = parameters.view || 'inbox', cursor = parameters.cursor || '', after = parameters.after || '', before = parameters.before || '';
    const limit = Number(parameters.limit || 50);
    if (query.length > 200 || mailbox.length > 254 || !['all', 'inbox', 'archive'].includes(view) || !Number.isSafeInteger(limit) || limit < 1 || limit > 100 || cursor && !ID.test(cursor) || [after, before].some((v) => v && (!/^\d{4}-\d{2}-\d{2}$/.test(v) || Number.isNaN(Date.parse(v))))) throw bad();
    const scope = mailbox ? await digest(mailbox) : '*', prefix = orderPrefix(scope, view);
    const counts = await getMany(this.storage, [...new Set(['meta', countKey(scope, view), countKey('*', 'all')])]);
    if (Number(counts.get('meta')?.total_stored || 0) !== Number(counts.get(countKey('*', 'all')) || 0)) throw bad('mail_index_warming', 503);
    const options = { prefix, reverse: true, limit: (query ? 100 : limit) + 1 };
    if (after) options.start = prefix + after;
    if (before) options.end = prefix + new Date(Date.parse(before) + 86400000).toISOString();
    if (cursor) {
      const locator = await this.storage.get(locatorKey(cursor));
      if (!locator) throw bad();
      const end = prefix + `${locator.date}:${cursor}`;
      if (!options.end || end < options.end) options.end = end;
    }
    const rows = [...(await this.storage.list(options)).values()];
    const batch = rows.slice(0, options.limit - 1);
    const summaries = await this.summaries(batch);
    const matchesHeader = (item) => [item.subject, item.from, item.to, item.mailbox].some((v) => v.toLowerCase().includes(query));
    const searchBodies = query ? await getMany(this.storage, summaries.filter((item) => !matchesHeader(item) && !this.bodyCache.has(item.id)).map((item) => bodyKey(item.id))) : new Map();
    const items = []; let scanned = 0, last;
    for (const item of summaries) {
      scanned++; last = item.id;
      if (query && !matchesHeader(item)) {
        const body = await this.body(item.id, searchBodies.get(bodyKey(item.id)));
        if (!body.text.toLowerCase().includes(query)) continue;
      }
      items.push(item);
      if (items.length === limit) break;
    }
    const states = await this.states(items.map((item) => item.id), view);
    return { items: items.map((item) => ({ ...item, ...states.get(item.id) })), next_cursor: scanned < rows.length ? last : null,
      scanned, failed: 0, total_stored: Number(counts.get(countKey('*', 'all')) || 0), total_view: Number(counts.get(countKey(scope, view)) || 0),
      exact_view_total: !query && !after && !before, indexed: true, read_only: false };
  }

  async conversation({ id } = {}) {
    if (!ID.test(id || '')) throw bad();
    await this.requireReady();
    if (this.queue.env.CMAIL_SENT_ENABLED === '1') {
      await this.queue.sent.requireReady();
      const members = await this.queue.sent.members(id);
      return members ? this.conversationResult(await this.summaries(members), id) : null;
    }
    const first = await this.storage.get(locatorKey(id));
    if (!first) return null;
    const found = new Map([[id, first]]), visited = new Set(), pending = [first];
    for (let offset = 0; offset < pending.length; offset++) {
      const tokens = pending[offset].tokens.filter((token) => !visited.has(token));
      const ids = new Set();
      for (const token of tokens) {
        visited.add(token);
        const rows = await this.storage.list({ prefix: `${PREFIX}edge:${token}:`, limit: 5001 });
        if (rows.size > 5000) throw bad('conversation_too_large', 413);
        for (const member of rows.values()) if (!found.has(member)) ids.add(member);
      }
      if (found.size + ids.size > 5000 || visited.size > 20000) throw bad('conversation_too_large', 413);
      const locators = await getMany(this.storage, [...ids].map(locatorKey));
      for (const member of ids) { const node = locators.get(locatorKey(member)); if (!node) throw bad('mail_index_incomplete', 503); found.set(member, node); pending.push(node); }
    }
    const summaries = await this.summaries([...found.keys()]);
    return this.conversationResult(summaries, id);
  }
  conversationResult(summaries, id) {
    const unique = new Map();
    for (const item of summaries) { const key = messageTokens(item.message_id)[0] || item.id; if (!unique.has(key) || item.id === id) unique.set(key, item); }
    const items = [...unique.values()].sort((a, b) => (Date.parse(b.date) || Date.parse(b.received_at)) - (Date.parse(a.date) || Date.parse(a.received_at)) || b.id.localeCompare(a.id));
    const available = new Set(summaries.flatMap((item) => messageTokens(item.message_id)));
    const references = new Set(summaries.flatMap((item) => [...messageTokens(item.in_reply_to), ...messageTokens(item.references)]));
    return { items, total: items.length, missing: [...references].filter((token) => !available.has(token)).length, indexed: true };
  }

  async backfill({ limit = 20 } = {}) {
    limit = Number(limit); if (!Number.isInteger(limit) || limit < 1 || limit > 50) throw bad();
    const initial = await this.readiness();
    if (initial.ready) return { ...initial, imported: 0, more: false, cursor: null };
    const previous = await this.storage.get(PREFIX + 'build') || {};
    const options = { prefix: 'mail:', reverse: true, limit: limit + 1 };
    if (previous.cursor) options.end = 'mail:' + previous.cursor;
    const records = [...(await this.storage.list(options)).values()];
    // A rollback may have ingested higher IDs without indexing them. Restart
    // the idempotent pass once the prior cursor reaches the end in that case.
    if (!records.length && previous.cursor) {
      await this.storage.put(PREFIX + 'build', { cursor: null, updated_at: new Date().toISOString() });
      return { ...initial, imported: 0, more: true, cursor: null };
    }
    let imported = 0;
    for (const record of records.slice(0, limit)) {
      if (!await this.storage.get(locatorKey(record.id))) {
        const prepared = await this.prepare({ ...await this.queue.loadPayload(record), received_at: record.received_at });
        await this.storage.transaction(async (tx) => {
          if (await this.put(tx, record, prepared, await tx.get(`cmail-archived:${record.id}`) === true)) imported++;
        });
      }
      await this.storage.put(PREFIX + 'build', { cursor: record.id, updated_at: new Date().toISOString() });
    }
    const status = await this.readiness();
    if (records.length <= limit && !status.ready) {
      await this.storage.put(PREFIX + 'build', { cursor: null, updated_at: new Date().toISOString() });
    }
    return { ...status, imported, more: !status.ready, cursor: records[Math.min(limit, records.length) - 1]?.id || previous.cursor || null };
  }
}
