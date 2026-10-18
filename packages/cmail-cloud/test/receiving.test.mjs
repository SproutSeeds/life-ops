import test from 'node:test';
import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { MailQueue, createWorker, withResend } from '../src/index.mjs';
import { SUPPORT_MAILBOX, SOURCE_PREFIX, SYNC_STATE, encodeRaw, syncReceived } from '../src/resend.mjs';
import { sha256Hex } from '../src/production-base.mjs';

class Storage {
  constructor(entries = []) { this.data = new Map(entries); this.tail = Promise.resolve(); }
  async get(key) { return Array.isArray(key) ? new Map(key.filter((k) => this.data.has(k)).map((k) => [k, structuredClone(this.data.get(k))])) : structuredClone(this.data.get(key)); }
  async put(key, value) {
    if (typeof key === 'object') { for (const [name, item] of Object.entries(key)) this.data.set(name, structuredClone(item)); }
    else this.data.set(key, structuredClone(value));
  }
  async delete(key) { this.data.delete(key); }
  async list({ prefix = '', end, reverse = false, limit = Infinity } = {}) {
    let rows = [...this.data].filter(([key]) => key.startsWith(prefix) && (!end || key < end)).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0);
    if (reverse) rows.reverse();
    return new Map(structuredClone(rows.slice(0, limit)));
  }
  async transaction(callback) {
    const work = this.tail.then(async () => {
      const draft = new Storage(structuredClone([...this.data]));
      const result = await callback(draft);
      this.data = draft.data;
      return result;
    });
    this.tail = work.catch(() => {});
    return work;
  }
}
const uuid = (n) => `10000000-0000-4000-8000-${String(n).padStart(12, '0')}`;
const rawMail = (n, extra = '') => new TextEncoder().encode(`From: Player <player@example.test>\r\nTo: ${SUPPORT_MAILBOX}\r\nSubject: Support ${n}\r\nDate: Tue, 13 Oct 2026 17:20:22 +0000\r\nMessage-ID: <support-${n}@example.test>\r\n${extra}\r\nPreserve this original body.\r\n`);
const payload = (n, received = '2026-10-13T17:20:22Z') => ({ provider: 'resend-receiving', external_id: `resend-receiving:${uuid(n)}`, envelope_to: SUPPORT_MAILBOX, received_at: received, direction: 'inbound', raw_base64: encodeRaw(rawMail(n)) });
async function fixture() {
  const storage = new Storage();
  const state = { storage, blockConcurrencyWhile: (fn) => fn() };
  const env = { LIFE_OPS_MASTER_KEY: Buffer.alloc(32, 19).toString('base64url'), LIFE_OPS_MAIL_INGEST_SECRET: 'fixture-ingest-secret', RESEND_RECEIVING_API_KEY: 'fixture-resend-key', CMAIL_MAILBOXES: 'cody@frg.earth' };
  const queue = new MailQueue(state, env);
  await queue.indexReady;
  env.MAIL_QUEUE = { getByName: () => ({ fetch: (url, options) => queue.fetch(new Request(url, options)) }) };
  return { queue, storage, env };
}
const call = async (queue, path, body) => queue.fetch(new Request(`https://internal${path}`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }));
const signed = (path, body, secret, time = new Date().toISOString()) => {
  const text = JSON.stringify(body);
  return new Request(`https://worker.test${path}`, { method: 'POST', headers: {
    'X-Life-Ops-Timestamp': time,
    'X-Life-Ops-Signature': `sha256=${createHmac('sha256', secret).update(`${time}.${text}`).digest('hex')}`,
  }, body: text });
};

test('encrypted history import round-trips original MIME, retains read state, and never queues local re-ingestion', async () => {
  const { queue, storage } = await fixture();
  const input = { ...payload(1), read: true };
  const first = await (await call(queue, '/resend/import', input)).json();
  assert.equal(first.stored, true);
  assert.equal((await queue.status()).pending_count, 0);
  assert.equal((await queue.status()).total_acknowledged, 1);
  const record = await storage.get(`mail:${first.id}`);
  assert.equal(record.payload, undefined);
  assert(!JSON.stringify([...storage.data]).includes('Preserve this original body'));
  assert.equal((await queue.readArchiveMessage({ id: first.id, raw: true })).content_base64, input.raw_base64);
  await storage.put(`cmail-read:${first.id}`, false);
  const duplicate = await (await call(queue, '/resend/import', input)).json();
  assert.equal(duplicate.id, first.id);
  assert.equal(duplicate.duplicate, true);
  assert.equal(await storage.get(`cmail-read:${first.id}`), false);
  assert.equal((await queue.status()).total_stored, 1);
});

test('received and sent histories retain original date ordering and sent replies remain outside Inbox', async () => {
  const { queue } = await fixture();
  const recent = await queue.enqueue(payload(1, '2026-10-17T20:00:00Z'));
  await call(queue, '/resend/import', payload(2, '2026-08-01T20:00:00Z'));
  const sent = await (await call(queue, '/resend/import', { ...payload(3), provider: 'resend-sent-history', direction: 'outbound', read: true })).json();
  const inbox = await queue.listArchive({ view: 'inbox', limit: 100 });
  assert.equal(inbox.items[0].id, recent.id);
  assert.equal(inbox.items.length, 2);
  assert(!inbox.items.some((item) => item.id === sent.id));
  assert.equal((await queue.listArchive({ view: 'archive' })).items[0].id, sent.id);
  assert.equal((await queue.listThreadIndex()).items.length, 3);
});

test('chronological cursor pagination covers all messages once, including equal timestamps and out-of-order imports', async () => {
  const { queue } = await fixture();
  for (let n = 1; n <= 46; n++) await queue.enqueue(payload(n, n % 2 ? '2026-08-01T00:00:00Z' : '2026-10-17T00:00:00Z'));
  const seen = []; let cursor = ''; let page;
  do { page = await queue.listArchive({ view: 'all', limit: 7, cursor }); seen.push(...page.items); cursor = page.next_cursor; } while (cursor);
  assert.equal(seen.length, 46);
  assert.equal(new Set(seen.map((m) => m.id)).size, 46);
  assert(seen.slice(0, 23).every((m) => m.received_at.startsWith('2026-10')));
});

test('concurrent legacy and Resend ingestion use unique atomic IDs and hash validation', async () => {
  const { queue } = await fixture();
  const rows = await Promise.all([queue.enqueue(payload(1)), queue.enqueue({ ...payload(2), provider: 'cloudflare-email-routing' }), queue.enqueue(payload(1))]);
  assert.equal(rows.filter((r) => r.stored).length, 2);
  assert.equal(new Set(rows.map((r) => r.id)).size, 2);
  await assert.rejects(queue.enqueue({ ...payload(3), payload_hash: 'wrong' }), /raw_hash_mismatch/);
  assert.equal((await queue.status()).total_stored, 2);
});

test('raw MIME attachments survive encryption, chunking and download', async () => {
  const { queue } = await fixture();
  const attachment = Buffer.alloc(120000, 37);
  const mime = Buffer.from(`From: player@example.test\r\nTo: ${SUPPORT_MAILBOX}\r\nSubject: Screenshot\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary="sample"\r\n\r\n--sample\r\nContent-Type: text/plain\r\n\r\nHelp with this screenshot.\r\n--sample\r\nContent-Type: image/png\r\nContent-Disposition: attachment; filename="test.png"\r\nContent-Transfer-Encoding: base64\r\n\r\n${attachment.toString('base64')}\r\n--sample--\r\n`);
  const stored = await queue.enqueue({ ...payload(1), raw_base64: mime.toString('base64') });
  const result = await queue.readArchiveMessage({ id: stored.id, attachment: 0 });
  assert.equal(result.content_base64, attachment.toString('base64'));
  const verified = await (await call(queue, '/resend/verify', { ids: [stored.id] })).json();
  assert.equal(verified.items[0].sha256, await sha256Hex(mime));
  assert.equal(verified.items[0].attachments[0].sha256, await sha256Hex(attachment));
});

test('provider pagination skips other mailboxes, deduplicates stable provider IDs and sends no credential to raw storage', async () => {
  const { queue, storage, env } = await fixture();
  let rawDownloads = 0; const paths = [];
  const fetcher = async (url, options) => {
    const u = new URL(url); paths.push(u.pathname + u.search);
    assert.equal(options.redirect, 'manual');
    if (u.host === 'raw.example.test') {
      assert.equal(options.headers, undefined); rawDownloads += 1;
      return new Response(rawMail(1));
    }
    assert.equal(options.headers.Authorization, 'Bearer fixture-resend-key');
    if (u.pathname === '/emails/receiving') return Response.json(u.searchParams.has('after') ? { data: [{ id: uuid(2) }], has_more: false } : { data: [{ id: uuid(1) }], has_more: true });
    if (u.pathname.endsWith(uuid(1))) return Response.json({ to: [SUPPORT_MAILBOX], from: 'player@example.test', created_at: '2026-10-13T17:20:22Z', raw: { download_url: 'https://raw.example.test/mail.eml' } });
    return Response.json({ to: ['someone@example.test'] });
  };
  const options = { fetcher, sleep: async () => {} };
  const first = await syncReceived(queue, env, options);
  assert.equal(first.status, 'healthy'); assert.equal(first.imported, 1); assert.equal(first.skipped, 1); assert.equal(first.pages, 2);
  assert(paths.some((p) => p.includes('&after=')));
  const second = await syncReceived(queue, env, { ...options, trigger: 'scheduled' });
  assert.equal(second.duplicates, 1); assert.equal(rawDownloads, 1);
  assert(second.last_scheduled_success_at);
  assert(await storage.get(`${SOURCE_PREFIX}resend-receiving:${uuid(1)}`));
  assert.equal((await queue.status()).pending_count, 0);
  const reverified = await syncReceived(queue, env, { ...options, verifyExisting: true });
  assert.equal(reverified.duplicates, 1); assert.equal(rawDownloads, 2);
  assert.equal((await queue.status()).total_stored, 1);
});

test('source failures stay visible, do not advance past failed messages, and retry without leaking provider bodies', async () => {
  const { queue, storage, env } = await fixture();
  const options = { sleep: async () => {}, fetcher: async (url) => url.endsWith('limit=100') ? Response.json({ data: [{ id: uuid(1) }], has_more: false }) : new Response('secret provider body', { status: 429 }) };
  const failed = await syncReceived(queue, env, options);
  assert.equal(failed.status, 'failed'); assert.equal(failed.error, 'resend_http_429'); assert.equal(failed.cursor, '');
  assert(!JSON.stringify(await storage.get(SYNC_STATE)).includes('secret provider body'));
  assert.equal((await queue.status()).total_stored, 0);
});

test('bounded provider batches resume without skipping new messages and reject stalled cursors', async () => {
  const { queue, env } = await fixture();
  const ids = Array.from({ length: 23 }, (_, n) => uuid(n + 1));
  const fetcher = async (url) => {
    const u = new URL(url);
    if (u.host === 'raw.example.test') return new Response(rawMail(Number(u.pathname.slice(1))));
    if (u.pathname === '/emails/receiving') {
      const start = u.searchParams.has('after') ? ids.indexOf(u.searchParams.get('after')) + 1 : 0;
      return Response.json({ data: ids.slice(start).map((id) => ({ id })), has_more: false });
    }
    const n = ids.indexOf(u.pathname.split('/').at(-1)) + 1;
    return Response.json({ to: [SUPPORT_MAILBOX], created_at: '2026-10-13T17:20:22Z', raw: { download_url: `https://raw.example.test/${n}` } });
  };
  const options = { fetcher, sleep: async () => {} };
  const first = await syncReceived(queue, env, options);
  assert.equal(first.imported, 20); assert.equal(first.status, 'catching_up'); assert.equal(first.cursor, uuid(20));
  const resumed = await syncReceived(queue, env, options);
  assert.equal(resumed.imported, 3); assert.equal(resumed.status, 'healthy'); assert.equal(resumed.cursor, '');
  assert.equal((await queue.status()).total_stored, 23);
  const stalled = await syncReceived(queue, env, { sleep: async () => {}, fetcher: async () => Response.json({ data: [{ id: uuid(1) }], has_more: true }) });
  assert.equal(stalled.status, 'failed'); assert.equal(stalled.error, 'invalid_resend_cursor');
});

test('a failed encrypted storage transaction leaves no partial mail, dedupe or counters', async () => {
  const { queue, storage } = await fixture();
  const original = storage.transaction.bind(storage);
  storage.transaction = (callback) => original(async (draft) => { await callback(draft); throw new Error('fixture storage failure'); });
  await assert.rejects(queue.enqueue(payload(1)), /fixture storage failure/);
  assert.equal((await queue.status()).total_stored, 0);
  assert.equal((await storage.list({ prefix: 'archive:' })).size, 0);
  assert.equal((await storage.list({ prefix: 'dedupe:' })).size, 0);
  storage.transaction = original;
  assert.equal((await queue.enqueue(payload(1))).seq, 1);
});

test('signed admin endpoints reject missing, stale and tampered signatures; normal C-mail stays authenticated', async () => {
  const { env } = await fixture(); const worker = withResend();
  const path = '/api/mail/resend/status';
  assert.equal((await worker.fetch(new Request(`https://worker.test${path}`, { method: 'POST', body: '{}' }), env, {})).status, 401);
  assert.equal((await worker.fetch(signed(path, {}, env.LIFE_OPS_MAIL_INGEST_SECRET, '2020-01-01T00:00:00Z'), env, {})).status, 401);
  assert.equal((await worker.fetch(signed(path, {}, 'wrong-secret'), env, {})).status, 401);
  assert.equal((await worker.fetch(signed(path, {}, env.LIFE_OPS_MAIL_INGEST_SECRET), env, {})).status, 200);
  assert.equal((await worker.fetch(new Request('https://cmail.frg.earth/cmail/api/session'), env, {})).status, 503);
  const authenticated = withResend(createWorker(async () => ({ ok: true, email: 'owner@example.test' })));
  const session = await (await authenticated.fetch(new Request('https://cmail.frg.earth/cmail/api/session'), env, {})).json();
  assert.deepEqual(session.mailboxes, ['cody@frg.earth', SUPPORT_MAILBOX]);
});

test('scheduled handler awaits intake and propagates failure to Cloudflare', async () => {
  const worker = withResend(); let pending;
  const env = { MAIL_QUEUE: { getByName: () => ({ fetch: async (_url, options) => { assert.equal(JSON.parse(options.body).trigger, 'scheduled'); return Response.json({ status: 'failed' }, { status: 503 }); } }) } };
  await assert.rejects(worker.scheduled({}, env, { waitUntil: (work) => { pending = work; } }), /resend_scheduled_sync_failed/);
  await assert.rejects(pending, /resend_scheduled_sync_failed/);
});
