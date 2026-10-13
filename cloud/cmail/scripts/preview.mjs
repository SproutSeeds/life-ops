// Local visual QA only. This file is never imported by the production entrypoint.
import http from 'node:http';
import { createWorker, MailQueue } from '../dist/index.js';
const records = new Map();
const storage = {
  async get(key) { return structuredClone(records.get(key)); },
  async put(key, value) { records.set(key, structuredClone(value)); },
  async delete(key) { records.delete(key); },
  async list({ prefix = '', end, reverse, limit = Infinity } = {}) {
    let list = [...records].filter(([key]) => key.startsWith(prefix) && (!end || key < end)).sort(([a], [b]) => a.localeCompare(b));
    if (reverse) list.reverse(); return new Map(list.slice(0, limit));
  },
};
const env = {
  LIFE_OPS_MASTER_KEY: Buffer.alloc(32, 7).toString('base64url'),
  CMAIL_MAILBOXES: 'preview@example.com,team@example.com,empty@example.com',
};
const queue = new MailQueue({ storage }, env);
env.MAIL_QUEUE = { getByName: () => ({ fetch: (url, options) => queue.fetch(new Request(url, options)) }) };
for (const [index, [from, subject, text]] of [
  ['The Workshop <hello@example.com>', 'Your workspace is ready', 'Your documents and notes are ready whenever you need them.'],
  ['Alex Chen <alex@example.com>', 'A few thoughts on the project', 'Hi there,\n\nI took a look at the latest version. The direction looks good. Let’s talk through the next steps this week.\n\nAlex'],
  ['Calendar <calendar@example.com>', 'Tomorrow’s conversation', 'A reminder for tomorrow’s conversation.\n\nTuesday, October 13\n11:00–11:30 AM Eastern\n\nThis is sample data for a local interface check.'],
  ['Jordan Lee <jordan@example.com>', 'Interview details and what to expect', 'Hi Cody,\n\nLooking forward to our conversation tomorrow. We’ll discuss the Orchard project with Rowan.\n\nPlease bring any questions you have for the team.\n\nBest,\nJordan\n\nSample message — no real interview information.'],
].entries()) {
  const mailbox = index === 3 ? 'team@example.com' : 'preview@example.com';
  const raw = `From: ${from}\r\nTo: ${mailbox}\r\nSubject: ${subject}\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n${text}`;
  await queue.enqueue({ headers: { from, to: mailbox, subject }, envelope_to: mailbox, raw_base64: Buffer.from(raw).toString('base64'), received_at: new Date(Date.now() - (3 - index) * 3600000).toISOString() });
}
const worker = createWorker(async () => ({ ok: true, email: 'preview@example.com' }));
// Put the sample conversation beyond the first archive scan to exercise search.
for (let i = 0; i < 123; i++) {
  const subject = `Routine update ${i + 1}`;
  const raw = `From: updates@example.com\r\nTo: preview@example.com\r\nSubject: ${subject}\r\nContent-Type: text/plain\r\n\r\nA recent sample update.`;
  const { id } = await queue.enqueue({ headers: { from: 'updates@example.com', to: 'preview@example.com', subject }, envelope_to: 'preview@example.com', raw_base64: Buffer.from(raw).toString('base64'), received_at: new Date().toISOString() });
  if (i % 3 === 0) await queue.setReadStatus({ id, read: true });
}
http.createServer(async (incoming, outgoing) => {
  const chunks = []; for await (const chunk of incoming) chunks.push(chunk);
  const response = await worker.fetch(new Request(`http://127.0.0.1:8788${incoming.url}`, {
    method: incoming.method, headers: incoming.headers,
    body: ['GET', 'HEAD'].includes(incoming.method) ? undefined : Buffer.concat(chunks),
  }), env);
  outgoing.writeHead(response.status, Object.fromEntries(response.headers));
  outgoing.end(Buffer.from(await response.arrayBuffer()));
}).listen(8788, '127.0.0.1', () => console.log('Local sample mailbox: http://127.0.0.1:8788/cmail/'));
