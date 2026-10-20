import { sha256Hex } from './production-base.mjs';
import { getMany, putMany } from './mail-index.mjs';

const P = 'cmail-contacts-v1:';
const EMAIL = /^[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9.-]+\.[a-z]{2,}$/i;
const CURSOR = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z:[a-f0-9]{64}$/;
const bad = (message, status = 400) => Object.assign(new Error(message), { status });
const flatten = (items) => (items || []).flatMap((item) => item?.group ? flatten(item.group) : item ? [item] : []);

// One encrypted, deduplicated record per address. The browser loads this small
// catalog once per editing session and filters it locally, with zero reads per
// keystroke. Range pages and batched point reads keep cold loads bounded too.
export class ContactIndex {
  constructor(queue) { this.queue = queue; this.storage = queue.state.storage; }

  async prepare(body, payload) {
    const own = new Set(String(this.queue.env.CMAIL_MAILBOXES || '').split(',').map((value) => value.trim().toLowerCase()));
    const outbound = payload.direction === 'outbound';
    const candidates = flatten(outbound ? [...body.to, ...body.cc] : [body.from, ...body.reply_to]);
    const contacts = new Map();
    for (const item of candidates) {
      const address = String(item.address || '').trim().toLowerCase();
      if (!EMAIL.test(address) || address.length > 254 || own.has(address)) continue;
      const name = String(item.name || '').replace(/[\x00-\x1f\x7f]/g, ' ').trim().slice(0, 200);
      if (!contacts.has(address) || name) contacts.set(address, { address, name });
      if (contacts.size === 500) break;
    }
    // MIME has a bounded raw size; guard pathological header fanout separately.
    return Promise.all([...contacts.values()].map(async (contact) => ({ ...contact,
      hash: await sha256Hex(new TextEncoder().encode(contact.address)), outbound,
      date: new Date(Number.isFinite(Date.parse(payload.received_at)) ? Date.parse(payload.received_at) : 0).toISOString() })));
  }

  async put(storage, id, prepared) {
    if (await storage.get(P + 'message:' + id)) return false;
    const prior = await getMany(storage, [P + 'indexed', P + 'count', ...prepared.map((item) => P + 'contact:' + item.hash)]);
    let count = Number(prior.get(P + 'count') || 0);
    const entries = { [P + 'message:' + id]: true, [P + 'indexed']: Number(prior.get(P + 'indexed') || 0) + 1 };
    for (const item of prepared) {
      const envelope = prior.get(P + 'contact:' + item.hash);
      const previous = envelope ? await this.queue.views.unseal(envelope) : null;
      if (!previous) count++;
      const lastSeen = previous?.last_seen > item.date ? previous.last_seen : item.date;
      const contact = { address: item.address, name: item.name && (!previous?.name || item.date >= previous.last_seen) ? item.name : previous?.name || '',
        last_seen: lastSeen, sent: (previous?.sent || 0) + Number(item.outbound), received: (previous?.received || 0) + Number(!item.outbound) };
      if (previous && previous.last_seen !== lastSeen) await storage.delete(P + 'order:' + previous.last_seen + ':' + item.hash);
      entries[P + 'contact:' + item.hash] = await this.queue.views.seal(contact);
      entries[P + 'order:' + lastSeen + ':' + item.hash] = item.hash;
    }
    entries[P + 'count'] = count;
    await putMany(storage, entries);
    return true;
  }

  async readiness() {
    const values = await getMany(this.storage, ['meta', P + 'indexed', P + 'count']);
    const total = Number(values.get('meta')?.total_stored || 0), indexed = Number(values.get(P + 'indexed') || 0);
    return { ready: total === indexed, total, indexed, contacts: Number(values.get(P + 'count') || 0) };
  }

  async list({ cursor = '', revision } = {}) {
    if (typeof cursor !== 'string' || cursor && !CURSOR.test(cursor) || revision !== undefined && (!Number.isSafeInteger(revision) || revision < 0)) throw bad('invalid_contacts_request');
    const state = await this.readiness();
    if (!state.ready) throw bad('contacts_warming', 503);
    if (revision !== undefined && revision !== state.indexed) throw bad('contacts_changed', 409);
    const prefix = P + 'order:';
    const rows = [...await this.storage.list({ prefix, reverse: true, limit: 251, ...(cursor ? { end: prefix + cursor } : {}) })];
    const records = await getMany(this.storage, rows.slice(0, 250).map(([, hash]) => P + 'contact:' + hash));
    const items = await Promise.all([...records.values()].map((record) => this.queue.views.unseal(record)));
    return { items, revision: state.indexed, total: state.contacts, next_cursor: rows.length > 250 ? rows[249][0].slice(prefix.length) : null };
  }

  async backfill({ limit = 20 } = {}) {
    if (!Number.isInteger(limit) || limit < 1 || limit > 50) throw bad('invalid_contacts_request');
    const initial = await this.readiness();
    if (initial.ready) return { ...initial, imported: 0, more: false };
    const cursor = await this.storage.get(P + 'build');
    const records = [...(await this.storage.list({ prefix: 'mail:', reverse: true, limit: limit + 1, ...(cursor ? { end: 'mail:' + cursor } : {}) })).values()];
    let imported = 0;
    for (const record of records.slice(0, limit)) {
      if (!await this.storage.get(P + 'message:' + record.id)) {
        const body = await this.queue.views.body(record.id);
        if (!body) throw bad('mail_index_warming', 503);
        // Historical direction is trusted archive metadata, never inferred from From.
        const payload = await this.queue.loadPayload(record);
        const prepared = await this.prepare(body, payload);
        await this.storage.transaction(async (tx) => { if (await this.put(tx, record.id, prepared)) imported++; });
      }
      await this.storage.put(P + 'build', record.id);
    }
    const state = await this.readiness();
    if (records.length <= limit && !state.ready) await this.storage.delete(P + 'build');
    return { ...state, imported, more: !state.ready };
  }
}
