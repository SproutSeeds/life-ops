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
  for (const path of ['/cmail', '/cmail/', '/cmail/api/messages', '/cmail/api/messages/mail_0000000000000001/raw']) {
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

test('cloud routes are read-only, private, and preserve the signed sync API', async () => {
  const { legacy, archive } = setup();
  const { id } = await legacy.enqueue(fixture('Welcome', 1));
  const worker = createWorker(async () => ({ ok: true, email: 'owner@example.com' }));
  const workerEnv = { ...env, MAIL_QUEUE: { getByName(name) { assert.equal(name, 'primary'); return { fetch: (url, options) => archive.fetch(new Request(url, options)) }; } } };
  const list = await worker.fetch(new Request('https://mail.example.com/cmail/api/messages'), workerEnv);
  assert.equal((await list.json()).items[0].id, id);
  assert.equal(list.headers.get('cache-control'), 'private, no-store');
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
