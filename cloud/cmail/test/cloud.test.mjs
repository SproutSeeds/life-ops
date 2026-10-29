import test from 'node:test';
import assert from 'node:assert/strict';
import { generateKeyPair, exportJWK, SignJWT, createLocalJWKSet } from 'jose';
import { createAuthorizer } from '../src/access.mjs';
import { MailQueue } from '../src/archive.mjs';
import { createWorker } from '../dist/index.js';
import { MailQueue as LegacyMailQueue } from '../src/legacy-worker.mjs';

class Storage {
  values = new Map();
  async get(key) { return structuredClone(this.values.get(key)); }
  async put(key, value) { this.values.set(key, structuredClone(value)); }
  async delete(key) { return this.values.delete(key); }
  async transaction(callback) {
    const tx = new Storage(); tx.values = structuredClone(this.values);
    const result = await callback(tx); this.values = tx.values; return result;
  }
  async list({ prefix = '', end, start, reverse = false, limit = Infinity } = {}) {
    let entries = [...this.values.entries()].filter(([key]) => key.startsWith(prefix) && (!end || key < end) && (!start || key >= start)).sort(([a], [b]) => a.localeCompare(b));
    if (reverse) entries.reverse();
    return new Map(structuredClone(entries.slice(0, limit)));
  }
}
const masterKey = Buffer.alloc(32, 19).toString('base64url');
const env = { LIFE_OPS_MASTER_KEY: masterKey, LIFE_OPS_MAIL_INGEST_SECRET: 'test-fixture-key' };
function fixture(subject, sequence, overrides = {}) {
  const raw = `From: Jordan <jordan@example.com>\r\nTo: owner@example.com\r\nSubject: ${subject}\r\nMessage-ID: <message-${sequence}@example.com>\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nPlease join https://meet.example.com/conversation at 11 AM.\r\n`;
  return { headers: { from: 'Jordan <jordan@example.com>', to: 'owner@example.com', subject }, envelope_from: 'jordan@example.com', envelope_to: 'owner@example.com', received_at: '2026-10-01T14:00:00Z', raw_base64: Buffer.from(raw).toString('base64'), ...overrides };
}
function setup() {
  const state = { storage: new Storage() };
  return { state, legacy: new LegacyMailQueue(state, env), archive: new MailQueue(state, env) };
}

test('previously acknowledged mail remains readable without changing queue or encrypted records', async () => {
  const { state, legacy, archive } = setup();
  const old = await legacy.enqueue(fixture('Interview invitation', 1));
  await legacy.ack({ ids: [old.id] });
  const pending = await legacy.enqueue(fixture('New message', 2));
  const snapshot = structuredClone([...state.storage.values]);
  const results = await archive.listArchive({ query: 'INTERVIEW' });
  assert.deepEqual(results.items.map((item) => item.id), [old.id]);
  const message = await archive.readArchiveMessage({ id: old.id });
  assert.match(message.text, /https:\/\/meet.example.com\/conversation/);
  assert.equal(message.from.address, 'jordan@example.com');
  assert.equal((await legacy.pull({})).items[0].id, pending.id);
  assert.deepEqual([...state.storage.values], snapshot);
  assert(!JSON.stringify([...state.storage.values]).includes('Interview invitation'));
});

test('search paginates older archives without missing or duplicating matches', async () => {
  const { legacy, archive } = setup();
  for (let i = 1; i <= 64; i++) await legacy.enqueue(fixture(`Interview ${i}`, i));
  const ids = [];
  let cursor;
  do {
    const page = await archive.listArchive({ query: 'interview', cursor });
    ids.push(...page.items.map((item) => item.id)); cursor = page.next_cursor;
  } while (cursor);
  assert.equal(ids.length, 64); assert.equal(new Set(ids).size, 64);
  assert(ids[0] > ids.at(-1));
});

test('requested page sizes respect bounded scans and retain every message across cursors', async () => {
  const { legacy, archive } = setup();
  for (let i = 1; i <= 127; i++) await legacy.enqueue(fixture(`Page fixture ${i}`, i));
  for (const size of [20, 50, 100]) {
    const pages = []; let cursor; let done = false; let page = [];
    while (!done) {
      const result = await archive.listArchive({ limit: size - page.length, cursor });
      assert(result.scanned <= 40);
      page.push(...result.items.map(item => item.id)); cursor = result.next_cursor; done = !cursor;
      if (page.length === size || done) { pages.push(page); page = []; }
    }
    assert.equal(pages[0].length, size);
    assert.equal(pages.flat().length, 127);
    assert.equal(new Set(pages.flat()).size, 127);
    assert.deepEqual(pages.flat(), [...pages.flat()].sort().reverse());
  }
  for (const limit of [0, -1, 101, 1.5, 'invalid']) await assert.rejects(archive.listArchive({ limit }));
});

test('read status persists independently of archived contents and delivery acknowledgements', async () => {
  const { state, legacy, archive } = setup();
  const old = await legacy.enqueue(fixture('Previously synced', 1));
  await legacy.ack({ ids: [old.id] });
  const pending = await legacy.enqueue(fixture('Still pending', 2));
  const snapshot = structuredClone([...state.storage.values]);
  assert.equal((await archive.readArchiveMessage({ id: old.id })).read, false);
  assert((await archive.listArchive()).items.every(item => item.read === false));
  await archive.setReadStatus({ id: old.id, read: true });
  const restored = new MailQueue(state, env);
  assert.equal((await restored.readArchiveMessage({ id: old.id })).read, true);
  assert.equal((await restored.listArchive()).items.find(item => item.id === old.id).read, true);
  await restored.setReadStatus({ id: old.id, read: false });
  assert.equal((await archive.readArchiveMessage({ id: old.id })).read, false);
  assert.deepEqual([...state.storage.values].filter(([key]) => !key.startsWith('cmail-read:')), snapshot);
  assert.equal((await legacy.pull({})).items[0].id, pending.id);
  await assert.rejects(archive.setReadStatus({ id: old.id, read: 'true' }));
  assert.equal(await archive.setReadStatus({ id: 'mail_9999999999999999', read: true }), null);
});

test('bulk actions change only selected flags, preserve delivery, and support reversible archive views', async () => {
  const { state, legacy, archive } = setup();
  const ids = [];
  for (let i = 0; i < 3; i++) ids.push((await legacy.enqueue(fixture(`Bulk fixture ${i}`, i))).id);
  await legacy.ack({ ids: [ids[0]] });
  const original = structuredClone([...state.storage.values]);
  const selection = ids.slice(0, 2);
  let result = await archive.applyAction({ ids: selection, action: 'read' });
  assert(result.items.every(item => item.read && !item.archived));
  assert.equal((await archive.readArchiveMessage({ id: ids[2] })).read, false);
  await archive.applyAction({ ids: selection, action: 'archive' });
  // Retrying the same request does not double-count archived messages.
  await archive.applyAction({ ids: selection, action: 'archive' });
  const restored = new MailQueue(state, env);
  const inbox = await restored.listArchive();
  assert.deepEqual(inbox.items.map(item => item.id), [ids[2]]); assert.equal(inbox.total_view, 1);
  const archived = await restored.listArchive({ view: 'archive' });
  assert.deepEqual(archived.items.map(item => item.id), selection.toReversed()); assert.equal(archived.total_view, 2);
  assert(archived.items.every(item => item.archived && item.read));
  assert.equal((await restored.listArchive({ view: 'all' })).total_view, 3);
  assert.equal((await restored.listArchive({ view: 'archive', mailbox: 'absent@example.com' })).items.length, 0);
  assert.equal((await restored.listArchive({ query: 'Bulk fixture 0' })).items.length, 0);
  assert.equal((await restored.listArchive({ view: 'archive', query: 'Bulk fixture 0' })).items[0].id, ids[0]);
  await restored.applyAction({ ids: selection, action: 'unread' });
  assert((await restored.listArchive({ view: 'archive' })).items.every(item => !item.read && item.archived));
  await restored.applyAction({ ids: selection, action: 'unarchive' });
  await restored.applyAction({ ids: selection, action: 'unarchive' });
  assert.equal((await restored.listArchive()).total_view, 3);
  assert.equal((await restored.listArchive({ view: 'archive' })).items.length, 0);
  assert.deepEqual([...state.storage.values].filter(([key]) => !key.startsWith('cmail-')), original);
  assert.equal((await legacy.pull({})).items.length, 2);
});

test('bulk selection is bounded and invalid or missing IDs cannot partly change messages', async () => {
  const { state, legacy, archive } = setup();
  const ids = [];
  for (let i = 0; i < 101; i++) ids.push((await legacy.enqueue(fixture(`Bounded ${i}`, i))).id);
  const snapshot = structuredClone([...state.storage.values]);
  for (const params of [
    { ids: [], action: 'read' }, { ids, action: 'archive' }, { ids: [ids[0],ids[0]], action: 'read' },
    { ids: [ids[0],'invalid'], action: 'read' }, { ids: [ids[0]], action: 'delete' },
  ]) await assert.rejects(archive.applyAction(params));
  assert.equal(await archive.applyAction({ ids: [ids[0], 'mail_9999999999999999'], action: 'archive' }), null);
  assert.deepEqual([...state.storage.values], snapshot);
  await archive.applyAction({ ids: ids.slice(0,100), action: 'archive' });
  let cursor; const found = [];
  do { const page = await archive.listArchive({ view: 'archive', cursor, limit: 100 }); found.push(...page.items.map(item => item.id)); cursor = page.next_cursor; } while (cursor);
  assert.equal(found.length, 100); assert.equal(new Set(found).size, 100);
  assert.equal((await archive.listArchive()).items[0].id, ids[100]);
  await assert.rejects(archive.listArchive({ view: 'invalid' }));
});

test('empty filtered pages retain a cursor; mailbox and date filters apply', async () => {
  const { legacy, archive } = setup();
  await legacy.enqueue(fixture('Needed older email', 0));
  for (let i = 1; i <= 41; i++) await legacy.enqueue(fixture(`Recent ${i}`, i, { envelope_to: 'second@example.com', received_at: '2026-10-10T10:00:00Z' }));
  const first = await archive.listArchive({ mailbox: 'owner@example.com', before: '2026-10-02' });
  assert.equal(first.items.length, 0); assert(first.next_cursor);
  const next = await archive.listArchive({ mailbox: 'owner@example.com', before: '2026-10-02', cursor: first.next_cursor });
  assert.equal(next.items[0].subject, 'Needed older email');
  assert.equal(next.next_cursor, null);
});

test('individual inboxes use the delivery address, preserve search scope, and combine without changing mail', async () => {
  const { state, legacy, archive } = setup();
  const owner = await legacy.enqueue(fixture('Shared topic', 1));
  // The visible To header can differ for forwarded or Bcc mail.
  const second = await legacy.enqueue(fixture('Shared topic', 2, { envelope_to: 'Second@Example.com' }));
  await legacy.ack({ ids: [second.id] });
  const snapshot = structuredClone([...state.storage.values]);
  const filtered = await archive.listArchive({ mailbox: 'SECOND@example.com', query: 'shared' });
  assert.deepEqual(filtered.items.map(item => item.id), [second.id]);
  assert.deepEqual((await archive.listArchive({ mailbox: 'owner@example.com' })).items.map(item => item.id), [owner.id]);
  assert.deepEqual((await archive.listArchive({})).items.map(item => item.id), [second.id, owner.id]);
  assert.equal((await archive.listArchive({ mailbox: 'empty@example.com' })).items.length, 0);
  assert.deepEqual([...state.storage.values], snapshot);
});

test('encoded headers are searchable and a malformed archive does not hide intact mail', async () => {
  const { state, legacy, archive } = setup();
  await legacy.enqueue(fixture('=?UTF-8?B?SW50ZXJ2aWV3IOKckw==?=', 1));
  const bad = await legacy.enqueue(fixture('Broken', 2));
  const record = await state.storage.get(`mail:${bad.id}`); record.payload_ref.prefix = 'missing:';
  await state.storage.put(`mail:${bad.id}`, record);
  const page = await archive.listArchive({ query: 'interview' });
  assert.equal(page.items[0].subject, 'Interview ✓'); assert.equal(page.failed, 1);
});

test('search finds names inside encoded text and HTML bodies beyond the first scan without changing stored mail', async () => {
  const { state, legacy, archive } = setup();
  const headers = 'From: calendar@example.com\r\nTo: owner@example.com\r\nSubject: Conversation details\r\nMIME-Version: 1.0\r\n';
  const text = `${headers}Content-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: base64\r\n\r\n${Buffer.from('Your conversation is with Rowan about the Orchard project.').toString('base64')}\r\n`;
  const html = `${headers}Content-Type: text/html; charset=utf-8\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n<p>You will meet Row=61n to discuss the Orchard project.</p>`;
  const textMessage = await legacy.enqueue(fixture('Conversation details', 1, { raw_base64: Buffer.from(text).toString('base64') }));
  const htmlMessage = await legacy.enqueue(fixture('Conversation details', 2, { raw_base64: Buffer.from(html).toString('base64') }));
  await legacy.ack({ ids: [textMessage.id, htmlMessage.id] });
  for (let i = 3; i < 48; i++) await legacy.enqueue(fixture(`Recent update ${i}`, i));
  const snapshot = structuredClone([...state.storage.values]);
  const first = await archive.listArchive({ query: 'RoWaN' });
  assert.equal(first.items.length, 0); assert(first.next_cursor);
  const second = await archive.listArchive({ query: 'RoWaN', cursor: first.next_cursor });
  assert.deepEqual(second.items.map((item) => item.id), [htmlMessage.id, textMessage.id]);
  assert.equal(second.next_cursor, null); assert.equal(second.failed, 0);
  assert.deepEqual([...state.storage.values], snapshot);
});

test('calendar attachment contents and exact raw messages can be recovered', async () => {
  const { legacy, archive } = setup();
  const calendar = 'BEGIN:VCALENDAR\r\nVERSION:2.0\r\nEND:VCALENDAR\r\n';
  const raw = `From: jordan@example.com\r\nTo: owner@example.com\r\nSubject: Calendar\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary="test-boundary"\r\n\r\n--test-boundary\r\nContent-Type: text/plain\r\n\r\nMeeting details\r\n--test-boundary\r\nContent-Type: text/calendar; name="invite.ics"\r\nContent-Disposition: attachment; filename="invite.ics"\r\nContent-Transfer-Encoding: base64\r\n\r\n${Buffer.from(calendar).toString('base64')}\r\n--test-boundary--\r\n`;
  const { id } = await legacy.enqueue(fixture('Calendar', 1, { raw_base64: Buffer.from(raw).toString('base64') }));
  const message = await archive.readArchiveMessage({ id });
  assert.equal(message.attachments[0].filename, 'invite.ics');
  const file = await archive.readArchiveMessage({ id, attachment: 0 });
  // MIME parsers normalize text-part line endings; the full .eml stays byte-exact.
  assert.equal(Buffer.from(file.content_base64, 'base64').toString().replace(/\r\n/g, '\n'), calendar.replace(/\r\n/g, '\n'));
  assert.equal(Buffer.from((await archive.readArchiveMessage({ id, raw: true })).content_base64, 'base64').toString(), raw);
  await assert.rejects(archive.readArchiveMessage({ id, attachment: -1 }));
});

test('thread metadata scans all inboxes and archived deliveries without changing flags or mail', async () => {
  const { state, legacy, archive } = setup();
  const root = await legacy.enqueue(fixture('Root message', 1));
  await legacy.ack({ ids: [root.id] });
  await archive.applyAction({ ids:[root.id], action:'archive' });
  for (let i = 2; i < 45; i++) await legacy.enqueue(fixture('Unrelated mail', i));
  const raw = 'From: sender@example.com\r\nTo: second@example.com\r\nSubject: Re: Root message\r\nMessage-ID: <reply@example.com>\r\nIn-Reply-To: <message-1@example.com>\r\nReferences: <message-1@example.com>\r\n\r\nPrivate reply body';
  const reply = await legacy.enqueue(fixture('Re: Root message', 45, { envelope_to:'second@example.com', raw_base64:Buffer.from(raw).toString('base64') }));
  const snapshot = structuredClone([...state.storage.values]);
  const first = await archive.listThreadIndex(); assert.equal(first.scanned,40); assert(first.next_cursor);
  const second = await archive.listThreadIndex({ cursor:first.next_cursor }); assert.equal(second.next_cursor,null);
  const nodes = [...first.items,...second.items]; assert.equal(nodes.length,45);
  const replyNode = nodes.find(node=>node.id===reply.id); assert.equal(replyNode.in_reply_to,'<message-1@example.com>'); assert.equal(replyNode.references,'<message-1@example.com>');
  assert(nodes.some(node=>node.id===root.id)); assert(!JSON.stringify(nodes).includes('Private reply body'));
  const message = await archive.readArchiveMessage({ id:reply.id }); assert.equal(message.in_reply_to,'<message-1@example.com>');
  assert.deepEqual([...state.storage.values],snapshot);
  await assert.rejects(archive.listThreadIndex({ cursor:'bad' }));
  const broken = await state.storage.get(`mail:${root.id}`); broken.payload_ref.prefix='missing:'; await state.storage.put(`mail:${root.id}`,broken);
  const partial = await archive.listThreadIndex({ cursor:first.next_cursor }); assert.equal(partial.failed,1);
});

const { privateKey, publicKey } = await generateKeyPair('RS256');
const publicJwk = await exportJWK(publicKey); publicJwk.kid = 'fixture';
const authorize = createAuthorizer(() => createLocalJWKSet({ keys: [publicJwk] }));
const accessEnv = { CMAIL_ACCESS_TEAM_DOMAIN: 'fixture.cloudflareaccess.com', CMAIL_ACCESS_AUD: 'cmail-app', CMAIL_OWNER_EMAIL: 'owner@example.com' };
async function token(overrides = {}) {
  return new SignJWT({ email: 'owner@example.com', ...overrides }).setProtectedHeader({ alg: 'RS256', kid: 'fixture' })
    .setIssuer('https://fixture.cloudflareaccess.com').setAudience('cmail-app').setSubject('owner').setIssuedAt().setExpirationTime('1h').sign(privateKey);
}
const requestWith = (value) => new Request('https://mail.example.com/cmail/api/messages', { headers: { 'Cf-Access-Jwt-Assertion': value } });

test('authentication verifies signature and restricts access to the configured owner', async () => {
  assert.equal((await authorize(requestWith(await token()), accessEnv)).ok, true);
  assert.equal((await authorize(requestWith(await token({ email: 'someone-else@example.com' })), accessEnv)).status, 403);
  assert.equal((await authorize(requestWith('not-a-jwt'), accessEnv)).status, 401);
  assert.equal((await authorize(requestWith(await token()), { ...accessEnv, CMAIL_ACCESS_AUD: 'other-app' })).status, 401);
  assert.equal((await authorize(requestWith(await token()), { ...accessEnv, CMAIL_ACCESS_TEAM_DOMAIN: 'other.cloudflareaccess.com' })).status, 401);
  const expired = await new SignJWT({ email: 'owner@example.com' }).setProtectedHeader({ alg: 'RS256', kid: 'fixture' }).setIssuer('https://fixture.cloudflareaccess.com').setAudience('cmail-app').setSubject('owner').setIssuedAt(100).setExpirationTime(200).sign(privateKey);
  assert.equal((await authorize(requestWith(expired), accessEnv)).status, 401);
});

test('missing sign-in configuration or identity fails closed before accessing storage', async () => {
  const worker = createWorker(authorize);
  for (const path of ['/cmail', '/cmail/', '/cmail/api/session', '/cmail/api/messages', '/cmail/api/thread-index', '/cmail/api/messages/mail_0000000000000001/raw']) {
    const request = new Request(`https://mail.example.com${path}`);
    assert.equal((await worker.fetch(request, {})).status, 503);
    assert.equal((await worker.fetch(request, accessEnv)).status, 401);
  }
  const publicRoot = new Request('https://cmail.frg.earth/');
  assert.equal((await worker.fetch(publicRoot, {})).status, 503);
  assert.equal((await worker.fetch(publicRoot, accessEnv)).status, 401);
  const otherOwner = new Request(publicRoot, { headers: { 'Cf-Access-Jwt-Assertion': await token({ email: 'someone-else@example.com' }) } });
  assert.equal((await worker.fetch(otherOwner, accessEnv)).status, 403);
  const owner = new Request(publicRoot, { headers: { 'Cf-Access-Jwt-Assertion': await token() } });
  const page = await worker.fetch(owner, accessEnv);
  assert.equal(page.status, 200);
  assert.match(await page.text(), /<title>C-mail<\/title>/);
});

test('authenticated inbox choices include configured addresses even before mail arrives', async () => {
  const worker = createWorker(async () => ({ ok: true, email: 'owner@example.com' }));
  const config = { CMAIL_MAILBOXES: ' Owner@Example.com,empty@example.com\nowner@example.com, bad address, <bad@example.com>' };
  const response = await worker.fetch(new Request('https://mail.example.com/cmail/api/session'), config);
  const session = await response.json();
  assert.equal(session.email, 'owner@example.com');
  assert.deepEqual(session.mailboxes, ['owner@example.com', 'empty@example.com']);
  assert.equal(response.headers.get('cache-control'), 'private, no-store');
  assert.equal((await worker.fetch(new Request('https://mail.example.com/cmail/api/session', { method: 'POST' }), config)).status, 405);
});

test('mail content routes are read-only, private, and preserve the signed sync API', async () => {
  const { legacy, archive } = setup();
  const { id } = await legacy.enqueue(fixture('Welcome', 1));
  const worker = createWorker(async () => ({ ok: true, email: 'owner@example.com' }));
  const workerEnv = { ...env, MAIL_QUEUE: { getByName(name) { assert.equal(name, 'primary'); return { fetch: (url, options) => archive.fetch(new Request(url, options)) }; } } };
  const list = await worker.fetch(new Request('https://mail.example.com/cmail/api/messages'), workerEnv);
  assert.equal((await list.json()).items[0].id, id);
  assert.equal(list.headers.get('cache-control'), 'private, no-store');
  const threadIndex = await worker.fetch(new Request('https://mail.example.com/cmail/api/thread-index'), workerEnv);
  assert.equal(threadIndex.headers.get('cache-control'), 'private, no-store'); assert.equal((await threadIndex.json()).items[0].id,id);
  assert.equal((await worker.fetch(new Request('https://mail.example.com/cmail/api/thread-index', { method:'POST' }), workerEnv)).status,405);
  assert.equal((await worker.fetch(new Request('https://mail.example.com/cmail/api/messages', { method: 'POST' }), workerEnv)).status, 405);
  assert.equal((await worker.fetch(new Request('https://mail.example.com/api/mail/queue/pull', { method: 'POST', body: '{}' }), workerEnv)).status, 401);
  const body = '{}'; const timestamp = new Date().toISOString();
  const key = await crypto.subtle.importKey('raw', new TextEncoder().encode(env.LIFE_OPS_MAIL_INGEST_SECRET), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']);
  const signature = Buffer.from(await crypto.subtle.sign('HMAC', key, new TextEncoder().encode(`${timestamp}.${body}`))).toString('hex');
  const sync = await worker.fetch(new Request('https://mail.example.com/api/mail/queue/pull', { method: 'POST', body, headers: { 'X-Life-Ops-Timestamp': timestamp, 'X-Life-Ops-Signature': `sha256=${signature}` } }), workerEnv);
  assert.equal(sync.status, 200); assert.equal((await sync.json()).items[0].id, id);
  const health = await worker.fetch(new Request('https://mail.example.com/healthz'), workerEnv);
  assert.equal((await health.json()).archive_encryption_enabled, true);
});

test('read-state writes require the owner, same origin, POST, and a boolean JSON value', async () => {
  const { legacy, archive, state } = setup();
  const { id } = await legacy.enqueue(fixture('Read state', 1));
  const worker = createWorker(authorize);
  const workerEnv = { ...env, ...accessEnv, MAIL_QUEUE: { getByName: () => ({ fetch: (url, options) => archive.fetch(new Request(url, options)) }) } };
  const ownerToken = await token();
  const url = `https://mail.example.com/cmail/api/messages/${id}/read-status`;
  const headers = { 'Cf-Access-Jwt-Assertion': ownerToken, Origin: 'https://mail.example.com', 'Content-Type': 'application/json' };
  const write = (changes = {}) => new Request(url, { method: 'POST', headers, body: '{"read":true}', ...changes });
  assert.equal((await worker.fetch(write({ headers: { ...headers, 'Cf-Access-Jwt-Assertion': '' } }), workerEnv)).status, 401);
  assert.equal((await worker.fetch(write({ headers: { ...headers, 'Cf-Access-Jwt-Assertion': await token({ email: 'other@example.com' }) } }), workerEnv)).status, 403);
  assert.equal((await worker.fetch(write({ headers: { ...headers, Origin: 'https://other.example.com' } }), workerEnv)).status, 403);
  assert.equal((await worker.fetch(write({ headers: { ...headers, Origin: '' } }), workerEnv)).status, 403);
  assert.equal((await worker.fetch(write({ headers: { ...headers, 'Content-Type': 'text/plain' } }), workerEnv)).status, 400);
  assert.equal((await worker.fetch(write({ body: '{"read":"true"}' }), workerEnv)).status, 400);
  assert.equal((await worker.fetch(new Request(url, { headers }), workerEnv)).status, 405);
  assert.equal(await state.storage.get(`cmail-read:${id}`), undefined);
  const changed = await worker.fetch(write(), workerEnv);
  assert.equal(changed.status, 200); assert.deepEqual(await changed.json(), { id, read: true });
  assert.equal(changed.headers.get('cache-control'), 'private, no-store');
  const listed = await worker.fetch(new Request('https://mail.example.com/cmail/api/messages?limit=1', { headers }), workerEnv);
  const items = (await listed.json()).items; assert.equal(items.length, 1); assert.equal(items[0].read, true);
  assert.equal((await worker.fetch(write({ body: '{"read":false}' }), workerEnv)).status, 200);
  assert.equal(await state.storage.get(`cmail-read:${id}`), false);
});

test('bulk action HTTP routes enforce owner authentication, origin, method, and complete selection validation', async () => {
  const { legacy, archive, state } = setup();
  const { id } = await legacy.enqueue(fixture('Bulk endpoint', 1));
  const worker = createWorker(authorize);
  const workerEnv = { ...env, ...accessEnv, MAIL_QUEUE: { getByName: () => ({ fetch: (url, options) => archive.fetch(new Request(url, options)) }) } };
  const headers = { 'Cf-Access-Jwt-Assertion': await token(), Origin: 'https://mail.example.com', 'Content-Type': 'application/json' };
  const url = 'https://mail.example.com/cmail/api/messages/actions';
  const write = (changes = {}) => new Request(url, { method: 'POST', headers, body: JSON.stringify({ ids: [id], action: 'archive' }), ...changes });
  assert.equal((await worker.fetch(write({ headers: { ...headers, 'Cf-Access-Jwt-Assertion': '' } }), workerEnv)).status, 401);
  assert.equal((await worker.fetch(write({ headers: { ...headers, 'Cf-Access-Jwt-Assertion': await token({ email: 'other@example.com' }) } }), workerEnv)).status, 403);
  for (const overrides of [{ Origin: '' }, { Origin: 'https://other.example.com' }, { 'Sec-Fetch-Site': 'cross-site' }]) assert.equal((await worker.fetch(write({ headers: { ...headers, ...overrides } }), workerEnv)).status, 403);
  assert.equal((await worker.fetch(write({ headers: { ...headers, 'Content-Type': 'text/plain' } }), workerEnv)).status, 400);
  assert.equal((await worker.fetch(new Request(url, { headers }), workerEnv)).status, 405);
  for (const body of ['null', '{', '{"ids":[],"action":"archive"}', JSON.stringify({ ids:[id], action:'delete' }), ' '.repeat(8193)]) assert.equal((await worker.fetch(write({ body }), workerEnv)).status, 400);
  assert.equal(await state.storage.get(`cmail-archived:${id}`), undefined);
  const changed = await worker.fetch(write(), workerEnv);
  assert.equal(changed.status, 200); assert.equal(changed.headers.get('cache-control'), 'private, no-store');
  assert.deepEqual((await changed.json()).items, [{ id, read: false, archived: true }]);
  const response = await worker.fetch(new Request('https://mail.example.com/cmail/api/messages?view=archive', { headers }), workerEnv);
  assert.equal((await response.json()).items[0].id, id);
  assert.equal((await worker.fetch(write({ body: JSON.stringify({ ids:[id], action:'unarchive' }) }), workerEnv)).status, 200);
  assert.equal(await state.storage.get(`cmail-archived:${id}`), false);
});
