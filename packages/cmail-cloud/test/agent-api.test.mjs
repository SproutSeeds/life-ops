import test from 'node:test';
import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { withResend, createWorker } from '../src/index.mjs';
import { handleAgent } from '../src/agent-api.mjs';
import { sendCompose } from '../src/reply.mjs';
import { fixture, payload, uuid, support } from './reply-fixture.mjs';
import { decodeRaw } from '../src/resend.mjs';
const value = { kind: 'compose', sender: support, to: ['person@example.test'], cc: ['copy@example.test'], bcc: ['hidden@example.test'],
  pending: '', subject: 'Preserved reply', text: 'Exact original text.\n\nSupport', signature: 'Support',
  in_reply_to: '<original@example.test>', references: ['<root@example.test>', '<original@example.test>'] };
const envelope = { id: 'draft_' + uuid(1), revision: 0, save_id: uuid(2), draft: value };
const local = (queue, action, parameters) => handleAgent(queue, new Request('https://internal/agent/request', {
  method: 'POST', body: JSON.stringify({ action, parameters }),
}));

test('agent transport rejects unsigned, stale and tampered requests; disabled deployments fail closed', async () => {
  const { env } = await fixture(); const worker = withResend(createWorker(async () => ({ ok: false })));
  env.LIFE_OPS_MAIL_INGEST_SECRET = 'synthetic-secret';
  const body = JSON.stringify({ action: 'status' });
  const call = (text, timestamp, signature) => worker.fetch(new Request('https://example.test/api/mail/agent', {
    method: 'POST', body: text, headers: { 'X-Life-Ops-Timestamp': timestamp, 'X-Life-Ops-Signature': signature },
  }), env, {});
  const stamp = new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');
  const sign = (stamp) => 'sha256=' + createHmac('sha256', env.LIFE_OPS_MAIL_INGEST_SECRET).update(stamp + '.' + body).digest('hex');
  assert.equal((await call(body, '', '')).status, 401);
  assert.equal((await call(body, '2020-01-01T00:00:00Z', sign('2020-01-01T00:00:00Z'))).status, 401);
  assert.equal((await call(body + ' ', stamp, sign(stamp))).status, 401);
  assert.equal((await call(body, stamp, sign(stamp))).status, 503);
  env.CMAIL_AGENT_ENABLED = '1';
  const ok = await call(body, stamp, sign(stamp)); assert.equal(ok.status, 200);
  assert.equal((await ok.json()).system, 'cmail-cloud');
});

test('agents and browser share drafts exactly; reads and saves never create outbox entries', async () => {
  const { queue, env, storage } = await fixture(); env.CMAIL_AGENT_ENABLED = '1';
  const saved = await (await local(queue, 'draft-save', envelope)).json();
  assert.deepEqual(saved.draft, value);
  assert.deepEqual((await queue.drafts.get(saved.id)).draft, value);
  assert.equal((await (await local(queue, 'drafts', {})).json()).total, 1);
  assert.deepEqual((await (await local(queue, 'draft-save', envelope)).json()).draft, value);
  assert.equal((await local(queue, 'draft-save', { ...envelope, save_id: uuid(3) })).status, 409);
  assert.equal((await local(queue, 'draft-send', { id: saved.id, revision: 1 })).status, 400);
  assert.equal((await local(queue, 'message-action', { action: 'delete', ids: [] })).status, 400);
  assert.equal((await local(queue, 'arbitrary-internal-route', {})).status, 400);
  assert(![...storage.data.keys()].some((key) => key.startsWith('cmail-outbox:')));
});

test('preserved CC, BCC and thread headers reach the provider and private Sent with exact idempotent retries', async () => {
  const { queue, env, storage } = await fixture(); env.CMAIL_FAST_READS = env.CMAIL_SENT_ENABLED = '1';
  const input = { ...value, request_id: uuid(50) }; let submitted, calls = 0;
  const options = { fetcher: async (_, options) => {
    submitted = JSON.parse(options.body); calls++;
    assert.deepEqual(submitted.cc, value.cc); assert.deepEqual(submitted.bcc, value.bcc);
    assert.equal(submitted.headers.Bcc, undefined); assert.equal(submitted.headers['In-Reply-To'], value.in_reply_to);
    assert.equal(submitted.headers.References, value.references.join(' '));
    return Response.json({ id: uuid(51) });
  } };
  const result = await sendCompose(queue, input, options);
  assert.equal(result.state, 'sent');
  assert.deepEqual(await sendCompose(queue, input, options), result); assert.equal(calls, 1);
  await assert.rejects(sendCompose(queue, { ...input, cc: ['changed@example.test'] }, options), /reply_changed/);
  const record = await storage.get('mail:' + result.archive_id);
  const raw = new TextDecoder().decode(decodeRaw((await queue.loadPayload(record)).raw_base64));
  assert.match(raw, /Cc: copy@example.test/); assert.match(raw, /Bcc: hidden@example.test/);
  assert.equal((await queue.views.conversation({ id: result.archive_id })).total, 2);
  assert.equal((await queue.listArchive({ view: 'sent' })).total_view, 1);
});

test('projection cursor is ascending, bounded, restartable and does not alter read/archive state', async () => {
  const { queue, env, id } = await fixture(); env.CMAIL_AGENT_ENABLED = '1';
  await queue.enqueue({ ...payload({ subject: 'Newer arrival' }), received_at: '2026-10-01T00:00:00Z' }, { archiveOnly: true });
  const before = await queue.status();
  const first = await (await local(queue, 'changes', { limit: 1 })).json();
  assert.equal(first.items[0].id, id); assert(first.has_more);
  const next = await (await local(queue, 'changes', { cursor: first.cursor, limit: 1 })).json();
  assert.equal(next.items.length, 1); assert.notEqual(next.items[0].id, id); assert(!next.has_more);
  assert.equal((await (await local(queue, 'changes', { cursor: next.cursor })).json()).items.length, 0);
  assert.deepEqual(await queue.status(), before);
});

test('older browser saves preserve migrated routing, while explicit empty fields clear copies', async () => {
  const { queue } = await fixture();
  await queue.drafts.save(envelope);
  const { cc, bcc, in_reply_to, references, ...legacy } = value;
  const oldSave = { ...envelope, revision: 1, save_id: uuid(60), draft: { ...legacy, text: 'Old tab edit' } };
  const updated = await queue.drafts.save(oldSave);
  for (const field of ['cc', 'bcc', 'in_reply_to', 'references']) assert.deepEqual(updated.draft[field], value[field]);
  assert.deepEqual(await queue.drafts.save(oldSave), updated);
  const cleared = await queue.drafts.save({ ...envelope, revision: 2, save_id: uuid(61),
    draft: { ...updated.draft, cc: [], bcc: [] } });
  assert.equal(cleared.draft.cc, undefined); assert.equal(cleared.draft.bcc, undefined);
  assert.equal(cleared.draft.in_reply_to, in_reply_to);
});

test('projection stops after a large payload and leaves the following record for its next cursor', async () => {
  const { queue, env } = await fixture(); env.CMAIL_AGENT_ENABLED = '1';
  await queue.enqueue(payload({ subject: 'Second attachment' }), { archiveOnly: true });
  let reads = 0; queue.loadPayload = async () => { reads++; return { raw_base64: 'A'.repeat(4 * 1024 * 1024) }; };
  const response = await (await local(queue, 'changes', { limit: 10 })).json();
  assert.equal(response.items.length, 1); assert.equal(reads, 1); assert(response.has_more);
  const next = await (await local(queue, 'changes', { cursor: response.cursor, limit: 10 })).json();
  assert.equal(next.items.length, 1); assert(!next.has_more); assert.notEqual(next.cursor, response.cursor);
});
