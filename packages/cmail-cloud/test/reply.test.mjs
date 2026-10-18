import test from 'node:test';
import assert from 'node:assert/strict';
import { prepareReply, sendReply } from '../src/reply.mjs';
import { withReplies } from '../src/reply-http.mjs';
import { createWorker, withResend } from '../src/index.mjs';
import { PostalMime } from '../src/production-base.mjs';
import { decodeRaw } from '../src/resend.mjs';
import { fixture, uuid, support, primary } from './reply-fixture.mjs';

const accepted = () => Response.json({ id: uuid(900) });
const neverSend = async () => { throw new Error('unexpected provider call'); };
const input = (id, n = 1) => ({ id, request_id: uuid(n), text: 'Here is my reply.\n\nBind My Soul Support\nhttps://bindmysoul.com' });

test('prepare uses mailbox sender, Reply-To, existing signature and thread headers', async () => {
  const { queue, id } = await fixture(); const context = await prepareReply(queue, id);
  assert.equal(context.sender, support); assert.equal(context.credential, 'support');
  assert.deepEqual(context.to, ['reply@example.test']); assert.equal(context.subject, 'Re: Reply fixture');
  assert.equal(context.in_reply_to, '<original@example.test>');
  assert.deepEqual(context.references, ['<root@example.test>', '<original@example.test>']);
  assert.equal(context.signature, 'Bind My Soul Support\nhttps://bindmysoul.com');
  const sent = await fixture({ mailbox: primary, direction: 'outbound' });
  const followup = await prepareReply(sent.queue, sent.id);
  assert.equal(followup.sender, primary); assert.deepEqual(followup.to, ['player@example.test']);
  const simple = await fixture({ replyTo: '' });
  assert.deepEqual((await prepareReply(simple.queue, simple.id)).to, ['player@example.test']);
});

test('confirmed send stores encrypted outbound MIME, preserves thread and never re-sends same request', async () => {
  const { queue, storage, id } = await fixture(); let calls = 0; let submitted;
  const result = await sendReply(queue, input(id), { fetcher: async (url, options) => {
    calls++; assert.equal(url, 'https://api.resend.com/emails');
    assert.equal(options.headers.Authorization, 'Bearer fixture-support-key');
    assert.equal(options.headers['Idempotency-Key'], 'cmail-reply/' + uuid(1));
    assert.equal(options.redirect, 'manual'); submitted = JSON.parse(options.body); return accepted();
  } });
  assert.equal(result.state, 'sent'); assert.equal(calls, 1);
  assert.equal(submitted.from, `Bind My Soul Support <${support}>`);
  assert.deepEqual(submitted.to, ['reply@example.test']); assert.equal(submitted.text, input(id).text);
  assert.equal(submitted.headers['In-Reply-To'], '<original@example.test>');
  const record = await storage.get('mail:' + result.archive_id); const saved = await queue.loadPayload(record);
  const parsed = await PostalMime.parse(decodeRaw(saved.raw_base64));
  assert.equal(parsed.text.trim(), input(id).text); assert.equal(parsed.messageId, submitted.headers['Message-ID']);
  assert.equal(parsed.inReplyTo, '<original@example.test>'); assert.equal(saved.direction, 'outbound');
  assert.equal(await storage.get('cmail-read:' + result.archive_id), true);
  assert.equal(await storage.get('cmail-archived:' + result.archive_id), true);
  assert.equal((await queue.status()).pending_count, 0);
  assert(!JSON.stringify([...storage.data]).includes('Here is my reply'));
  assert.deepEqual(await sendReply(queue, input(id), { fetcher: neverSend }), result);
  await assert.rejects(sendReply(queue, { ...input(id), text: 'changed' }, { fetcher: neverSend }), /reply_changed/);
});

test('primary mailbox uses its own provider credential', async () => {
  const { queue, id } = await fixture({ mailbox: primary });
  assert.equal((await sendReply(queue, input(id), { fetcher: async (_, options) => {
    assert.equal(options.headers.Authorization, 'Bearer fixture-primary-key'); return accepted();
  } })).state, 'sent');
});

test('ambiguous timeout retries byte-identical request and refuses an expired idempotency window', async () => {
  const { queue, id } = await fixture(); let original; let time = Date.parse('2026-10-18T00:00:00Z');
  const first = await sendReply(queue, input(id), { now: () => time, fetcher: async (_, options) => { original = options; throw new Error('network lost'); } });
  assert.equal(first.state, 'uncertain'); assert.equal(first.can_edit, false);
  time += 60000;
  const retry = await sendReply(queue, input(id), { now: () => time, fetcher: async (_, options) => {
    assert.equal(options.body, original.body); assert.deepEqual(options.headers, original.headers); return accepted();
  } }); assert.equal(retry.state, 'sent');
  await sendReply(queue, input(id, 2), { now: () => time, fetcher: async () => { throw new Error('network lost'); } });
  time += 24 * 3600000;
  await assert.rejects(sendReply(queue, input(id, 2), { now: () => time, fetcher: neverSend }), /confirmation_expired/);
});

test('provider acceptance survives archive failure; recovery archives without another send', async () => {
  const { queue, id } = await fixture(); const enqueue = queue.enqueue.bind(queue);
  queue.enqueue = async () => { throw new Error('temporary storage failure'); };
  const first = await sendReply(queue, input(id), { fetcher: accepted });
  assert.equal(first.state, 'accepted'); assert.equal(first.error, 'archive_pending');
  queue.enqueue = enqueue;
  assert.equal((await sendReply(queue, input(id), { fetcher: neverSend })).state, 'sent');
  assert.equal((await queue.status()).total_stored, 2);
});

test('definitive rejection preserves editable draft while empty/signature-only replies never send', async () => {
  const { queue, id } = await fixture();
  const result = await sendReply(queue, input(id), { fetcher: async () => Response.json({ message: 'fixture rejection' }, { status: 403 }) });
  assert.equal(result.state, 'rejected'); assert.equal(result.can_edit, true);
  assert.equal((await queue.status()).total_stored, 1);
  assert.deepEqual(await sendReply(queue, input(id), { fetcher: neverSend }), result);
  await assert.rejects(sendReply(queue, { ...input(id, 2), text: '   ' }, { fetcher: neverSend }), /invalid_reply/);
  await assert.rejects(sendReply(queue, { ...input(id, 2), text: '\n\nBind My Soul Support\nhttps://bindmysoul.com' }, { fetcher: neverSend }), /empty_reply/);
});

test('HTTP enforces Access and same-origin explicit send; browser cannot override addresses', async () => {
  const { env, id } = await fixture();
  const denied = withReplies(createWorker(), async () => ({ ok: false, status: 401, error: 'unauthorized' }));
  const url = `https://cmail.frg.earth/cmail/api/messages/${id}/reply`;
  assert.equal((await denied.fetch(new Request(url), env)).status, 401);
  const allowed = async () => ({ ok: true, email: primary });
  const worker = withReplies(withResend(createWorker(allowed)), allowed);
  assert.equal((await worker.fetch(new Request(url), env)).status, 200);
  for (const headers of [{}, { Origin: 'https://evil.example', 'X-Cmail-Action': 'reply' }, { Origin: 'https://cmail.frg.earth', 'X-Cmail-Action': 'reply', 'Sec-Fetch-Site': 'cross-site' }]) {
    assert.equal((await worker.fetch(new Request(url, { method: 'POST', headers, body: '{}' }), env)).status, 403);
  }
  let proxied;
  env.MAIL_QUEUE = { getByName: () => ({ fetch: async (_, init) => { proxied = JSON.parse(init.body); return Response.json({ state: 'fixture' }); } }) };
  await worker.fetch(new Request(url, { method: 'POST', headers: { Origin: 'https://cmail.frg.earth', 'X-Cmail-Action': 'reply', 'Content-Type': 'application/json' }, body: JSON.stringify({ ...input(id), from: 'attacker@example.test', to: ['attacker@example.test'] }) }), env);
  assert.deepEqual(proxied, input(id));
  const html = await worker.fetch(new Request('https://cmail.frg.earth/'), env);
  assert.match(await html.text(), /src="\/cmail\/reply.js"/);
  assert.match(html.headers.get('Content-Security-Policy'), /script-src 'self'/);
  const session = await (await worker.fetch(new Request('https://cmail.frg.earth/cmail/api/session'), env)).json();
  assert.equal(session.reply_enabled, true); assert.equal(session.read_only, false);
});
