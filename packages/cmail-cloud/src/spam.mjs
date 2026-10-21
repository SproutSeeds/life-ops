import { sha256Hex } from './production-base.mjs';

const PREFIX = 'cmail-spam-v1:sender:';
const EMAIL = /^[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9.-]+\.[a-z]{2,}$/i;
const bad = (message, status = 400) => Object.assign(new Error(message), { status });
const senderKey = async (sender) => PREFIX + await sha256Hex(new TextEncoder().encode(sender));
export function spamAddress(value) {
  const address = String(value || '').trim().toLowerCase();
  return address.length <= 254 && EMAIL.test(address) ? address : '';
}

export class SpamStore {
  constructor(queue) { this.queue = queue; this.storage = queue.state.storage; }
  own(sender) { return String(this.queue.env.CMAIL_MAILBOXES || '').split(',').some((value) => spamAddress(value) === sender); }
  async prepare(body, payload) {
    const sender = spamAddress(body.from?.address);
    if (!sender || payload.direction === 'outbound' || this.own(sender)) return null;
    return { sender, key: await senderKey(sender) };
  }
  async matches(storage, prepared) { return Boolean(prepared && await storage.get(prepared.key)); }
  async set(storage, prepared, enabled, id) {
    if (!prepared) throw bad('sender_cannot_be_blocked');
    if (!enabled) { await storage.delete(prepared.key); return; }
    if (!await storage.get(prepared.key)) await storage.put(prepared.key,
      await this.queue.views.seal({ sender: prepared.sender, created_at: new Date().toISOString(), message_id: id }));
  }
  async forMessage(id, restoring = false) {
    const body = await this.queue.views.body(id);
    if (!body) throw bad('message_not_found', 404);
    const sender = spamAddress(body.from?.address);
    if (!sender) throw bad('sender_cannot_be_blocked');
    // Adding a previously blocked address as an owned identity must never trap
    // its old mail in Spam. Restoration is safe even after identity changes.
    if (restoring && await this.storage.get('cmail-spam:' + id) === true) return { sender, key: await senderKey(sender) };
    if (this.own(sender)) throw bad('sender_cannot_be_blocked');
    // Pre-cutover index records lack direction. Read only a selected legacy
    // message's envelope, never scan or rebuild the archive to enable Spam.
    let direction = body.direction;
    if (!direction) {
      const record = await this.storage.get('mail:' + id);
      direction = (await this.queue.loadPayload(record)).direction || 'inbound';
    }
    const prepared = await this.prepare(body, { direction });
    if (!prepared) throw bad('sender_cannot_be_blocked');
    return prepared;
  }
  async list({ cursor = '', limit = 50 } = {}) {
    limit = Number(limit);
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 100 || cursor && !/^[a-f0-9]{64}$/.test(cursor)) throw bad('invalid_request');
    const rows = [...await this.storage.list({ prefix: PREFIX, limit: limit + 1, ...(cursor ? { start: PREFIX + cursor + '\0' } : {}) })];
    return { items: await Promise.all(rows.slice(0, limit).map(([, envelope]) => this.queue.views.unseal(envelope))),
      next_cursor: rows.length > limit ? rows[limit - 1][0].slice(PREFIX.length) : null };
  }
}
