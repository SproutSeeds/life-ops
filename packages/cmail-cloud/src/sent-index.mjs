import { sha256Hex } from './production-base.mjs';
import { getMany, putMany } from './mail-index.mjs';

const P = 'cmail-sent-v1:';
const ID = /^mail_\d{16}$/;
const CURSOR = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z:mail_\d{16}$/;
const bad = (message = 'invalid_request', status = 400) => Object.assign(new Error(message), { status });
const hash = (value) => sha256Hex(new TextEncoder().encode(value));
const newer = (a, b) => !a || b && `${b.date}:${b.id}` > `${a.date}:${a.id}` ? b : a;
const order = (scope) => `${P}order:${scope}:`;
const count = (scope) => `${P}count:${scope}`;
const groupKey = (id) => P + 'group:' + id;
const nodeKey = (id) => P + 'node:' + id;
const membersPrefix = (id) => P + 'members:' + id + ':';

// Connected components are maintained on writes. Readers never discover Sent
// membership by walking every outbound message or scanning the mail archive.
export class SentIndex {
  constructor(queue) { this.queue = queue; this.storage = queue.state.storage; }

  async root(storage, id) {
    for (let depth = 0; depth < 64; depth++) {
      const group = await storage.get(groupKey(id));
      if (!group) throw bad('sent_index_incomplete', 503);
      if (!group.parent) return group;
      id = group.parent;
    }
    throw bad('sent_index_incomplete', 503);
  }

  async put(storage, id, locator, payload) {
    if (await storage.get(nodeKey(id))) return false;
    const tokens = [...new Set([...locator.tokens, 'archive_' + id,
      ...(ID.test(payload.reply_to_archive_id || '') ? ['archive_' + payload.reply_to_archive_id] : [])])];
    const previous = await getMany(storage, tokens.map((token) => P + 'token:' + token));
    const roots = new Map();
    for (const linked of new Set(previous.values())) {
      const root = await this.root(storage, linked); roots.set(root.id, root);
    }
    // Merge the smaller components into the largest, keeping alias depth and
    // moved membership entries bounded logarithmically over repeated merges.
    const ordered = [...roots.values()].sort((a, b) => b.size - a.size || a.id.localeCompare(b.id));
    const group = ordered.length ? structuredClone(ordered[0]) : { id, size: 0, latest: null, sent: {} };
    const changes = new Map();
    const bump = (scope, delta) => changes.set(scope, (changes.get(scope) || 0) + delta);
    for (const old of ordered) {
      for (const scope of Object.keys(old.sent)) {
        await storage.delete(order(scope) + `${old.latest.date}:${old.id}`); bump(scope, -1);
      }
      if (old.id !== group.id) {
        const members = await storage.list({ prefix: membersPrefix(old.id) });
        await putMany(storage, Object.fromEntries([...members.values()].map((member) => [membersPrefix(group.id) + member, member])));
        for (const key of members.keys()) await storage.delete(key);
        await storage.put(groupKey(old.id), { parent: group.id });
        group.size += old.size;
        group.latest = newer(group.latest, old.latest);
        for (const [scope, sent] of Object.entries(old.sent)) group.sent[scope] = newer(group.sent[scope], sent);
      }
    }
    const message = { id, date: locator.date };
    group.size++;
    group.latest = newer(group.latest, message);
    const outbound = payload.direction === 'outbound'; // Never infer a send from a forgeable From header.
    if (outbound) for (const scope of ['*', locator.scope]) group.sent[scope] = newer(group.sent[scope], message);
    const entries = { [nodeKey(id)]: group.id, [groupKey(group.id)]: group, [membersPrefix(group.id) + id]: id };
    for (const token of tokens) entries[P + 'token:' + token] = group.id;
    for (const [scope, sent] of Object.entries(group.sent)) {
      entries[order(scope) + `${group.latest.date}:${group.id}`] = { group: group.id, latest: group.latest.id, sent: sent.id };
      bump(scope, 1);
    }
    const keys = ['indexed', 'outbound'].map((key) => P + key).concat([...changes.keys()].map(count));
    const counts = await getMany(storage, keys);
    entries[P + 'indexed'] = Number(counts.get(P + 'indexed') || 0) + 1;
    entries[P + 'outbound'] = Number(counts.get(P + 'outbound') || 0) + Number(outbound);
    for (const [scope, delta] of changes) entries[count(scope)] = Number(counts.get(count(scope)) || 0) + delta;
    await putMany(storage, entries);
    return true;
  }

  async readiness() {
    const values = await getMany(this.storage, ['meta', P + 'indexed', P + 'outbound', count('*')]);
    const total = Number(values.get('meta')?.total_stored || 0), indexed = Number(values.get(P + 'indexed') || 0);
    return { ready: total === indexed, total, indexed, sent_messages: Number(values.get(P + 'outbound') || 0), conversations: Number(values.get(count('*')) || 0) };
  }
  async requireReady() { if (!(await this.readiness()).ready) throw bad('sent_index_warming', 503); }
  async members(id) {
    const node = await this.storage.get(nodeKey(id));
    if (!node) return null;
    const group = await this.root(this.storage, node);
    return this.groupMembers(group.id);
  }
  async groupMembers(id) {
    const values = await this.storage.list({ prefix: membersPrefix(id), limit: 5001 });
    if (values.size > 5000) throw bad('conversation_too_large', 413);
    return [...values.values()];
  }
  async matches(group, query) {
    const summaries = await this.queue.views.summaries(await this.groupMembers(group));
    if (summaries.some((item) => [item.from, item.to, item.subject, item.mailbox].some((v) => v.toLowerCase().includes(query)))) return true;
    for (let offset = 0; offset < summaries.length; offset += 100) {
      const ids = summaries.slice(offset, offset + 100).map((item) => item.id);
      const bodies = await getMany(this.storage, ids.filter((id) => !this.queue.views.bodyCache.has(id)).map((id) => 'cmail-v2:body:' + id));
      for (const id of ids) if ((await this.queue.views.body(id, bodies.get('cmail-v2:body:' + id)))?.text.toLowerCase().includes(query)) return true;
    }
    return false;
  }
  async list(parameters = {}) {
    const query = String(parameters.query || '').trim().toLowerCase(), mailbox = String(parameters.mailbox || '').trim().toLowerCase();
    const cursor = parameters.cursor || '', after = parameters.after || '', before = parameters.before || '', limit = Number(parameters.limit || 50);
    if (query.length > 200 || mailbox.length > 254 || !Number.isSafeInteger(limit) || limit < 1 || limit > 100 || cursor && !CURSOR.test(cursor) ||
      [after, before].some((v) => v && (!/^\d{4}-\d{2}-\d{2}$/.test(v) || Number.isNaN(Date.parse(v))))) throw bad();
    const scope = mailbox ? await hash(mailbox) : '*', prefix = order(scope);
    const counts = await getMany(this.storage, ['meta', P + 'indexed', count(scope)]);
    const total = Number(counts.get('meta')?.total_stored || 0);
    if (total !== Number(counts.get(P + 'indexed') || 0)) throw bad('sent_index_warming', 503);
    const options = { prefix, reverse: true, limit: (query ? Math.min(limit, 10) : limit) + 1 };
    if (after) options.start = prefix + after;
    if (before) options.end = prefix + new Date(Date.parse(before) + 86400000).toISOString();
    if (cursor && (!options.end || prefix + cursor < options.end)) options.end = prefix + cursor;
    const rows = [...await this.storage.list(options)];
    const selected = [];
    for (const row of rows.slice(0, -1 + options.limit)) if (!query || await this.matches(row[1].group, query)) selected.push(row);
    const ids = [...new Set(selected.flatMap(([, row]) => [row.latest, row.sent]))];
    const summaries = new Map((await this.queue.views.summaries(ids)).map((item) => [item.id, item]));
    const states = await this.queue.views.states(selected.map(([, row]) => row.latest));
    const items = selected.map(([, row]) => {
      const latest = summaries.get(row.latest), sent = summaries.get(row.sent);
      return { ...latest, ...states.get(row.latest), thread_id: row.group, sent_to: sent.to, sent_from: sent.from, mailbox: sent.mailbox };
    });
    return { items, next_cursor: rows.length === options.limit ? rows.at(-2)[0].slice(prefix.length) : null,
      scanned: Math.min(rows.length, options.limit - 1), failed: 0, total_stored: total, total_view: Number(counts.get(count(scope)) || 0),
      exact_view_total: !query && !after && !before, indexed: true, read_only: false, conversations: true };
  }

  async backfill({ limit = 20 } = {}) {
    if (!Number.isInteger(limit) || limit < 1 || limit > 50) throw bad();
    const initial = await this.readiness();
    if (initial.ready) return { ...initial, imported: 0, more: false };
    const cursor = await this.storage.get(P + 'build');
    const records = [...(await this.storage.list({ prefix: 'mail:', reverse: true, limit: limit + 1, ...(cursor ? { end: 'mail:' + cursor } : {}) })).values()];
    let imported = 0;
    for (const record of records.slice(0, limit)) {
      if (!await this.storage.get(nodeKey(record.id))) {
        const locator = await this.storage.get('cmail-v2:locator:' + record.id);
        if (!locator) throw bad('mail_index_warming', 503);
        const payload = await this.queue.loadPayload(record);
        await this.storage.transaction(async (tx) => { if (await this.put(tx, record.id, locator, payload)) imported++; });
      }
      await this.storage.put(P + 'build', record.id);
    }
    const status = await this.readiness();
    if (records.length <= limit && !status.ready) await this.storage.delete(P + 'build');
    return { ...status, imported, more: !status.ready };
  }
}
