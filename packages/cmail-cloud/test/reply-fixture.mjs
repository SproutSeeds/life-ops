import { MailQueue } from '../src/index.mjs';
import { encodeRaw } from '../src/resend.mjs';

export class Storage {
  constructor(entries = []) { this.data = new Map(entries); this.tail = Promise.resolve(); }
  async get(key) { return Array.isArray(key) ? new Map(key.filter((k) => this.data.has(k)).map((k) => [k, structuredClone(this.data.get(k))])) : structuredClone(this.data.get(key)); }
  async put(key, value) {
    if (typeof key === 'object') { for (const [name, item] of Object.entries(key)) this.data.set(name, structuredClone(item)); }
    else this.data.set(key, structuredClone(value));
  }
  async delete(key) { this.data.delete(key); }
  async list({ prefix = '', start, end, reverse = false, limit = Infinity } = {}) {
    let rows = [...this.data].filter(([key]) => key.startsWith(prefix) && (!start || key >= start) && (!end || key < end)).sort(([a], [b]) => a.localeCompare(b));
    if (reverse) rows.reverse();
    return new Map(structuredClone(rows.slice(0, limit)));
  }
  async transaction(callback) {
    const work = this.tail.then(async () => {
      const draft = new Storage(structuredClone([...this.data]));
      const result = await callback(draft); this.data = draft.data; return result;
    });
    this.tail = work.catch(() => {}); return work;
  }
}
export const support = 'support@bindmysoul.com';
export const primary = 'cody@frg.earth';
export const uuid = (n) => `10000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
export const bindings = {
  LIFE_OPS_MASTER_KEY: Buffer.alloc(32, 19).toString('base64url'), CMAIL_MAILBOXES: `${primary},${support}`,
  CMAIL_RESEND_API_KEY: 'fixture-primary-key', CMAIL_SUPPORT_SEND_KEY: 'fixture-support-key',
  CMAIL_SEND_IDENTITIES: JSON.stringify({
    [primary]: { from: `Cody <${primary}>`, signature: 'Cody', key: 'primary' },
    [support]: { from: `Bind My Soul Support <${support}>`, signature: 'Bind My Soul Support\nhttps://bindmysoul.com', key: 'support' },
  }),
};
export function payload({ mailbox = support, direction = 'inbound', replyTo = 'Reply Desk <reply@example.test>', subject = 'Reply fixture', references = '<root@example.test>', from } = {}) {
  const raw = `From: ${from || (direction === 'outbound' ? mailbox : 'Player <player@example.test>')}\r\nTo: ${direction === 'outbound' ? 'player@example.test' : mailbox}\r\n${replyTo ? `Reply-To: ${replyTo}\r\n` : ''}Subject: ${subject}\r\nDate: Tue, 13 Oct 2026 17:20:22 +0000\r\nMessage-ID: <original@example.test>\r\nReferences: ${references}\r\n\r\nA synthetic support question.\r\n`;
  return { envelope_to: mailbox, direction, received_at: '2026-10-13T17:20:22Z', raw_base64: encodeRaw(new TextEncoder().encode(raw)) };
}
export async function fixture(options) {
  const storage = new Storage(); const env = { ...bindings };
  const queue = new MailQueue({ storage, blockConcurrencyWhile: (fn) => fn() }, env);
  await queue.indexReady;
  env.MAIL_QUEUE = { getByName: () => ({ fetch: (url, init) => queue.fetch(new Request(url, init)) }) };
  const { id } = await queue.enqueue(payload(options), { archiveOnly: true });
  return { queue, storage, env, id };
}
