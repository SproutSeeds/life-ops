import test from 'node:test';
import assert from 'node:assert/strict';
import { DraftStore } from '../src/draft-store.mjs';
import { MailQueue, createWorker, withResend } from '../src/index.mjs';
import { withReplies } from '../src/reply-http.mjs';
import { withDrafts } from '../src/draft-http.mjs';
import { fixture, uuid, support, primary } from './reply-fixture.mjs';
import { draftsScript } from '../src/drafts-ui.mjs';
import { composeScript } from '../src/compose-ui.mjs';

const value = (changes = {}) => ({ kind: 'compose', sender: support, to: ['draft.person@example.test'], pending: '', subject: 'Synthetic saved subject', text: 'Private draft body.', signature: 'Support', ...changes });
const save = (n = 1, changes = {}) => ({ id: 'draft_' + uuid(n), revision: 0, save_id: uuid(n + 1000), draft: value(), ...changes });
const accepted = async () => Response.json({ id: uuid(900) });
const neverSend = async () => { throw new Error('Unexpected provider send'); };

test('Multiple encrypted drafts survive a new object instance, including incomplete recipients and empty content', async () => {
  const { queue, storage, env } = await fixture();
  const before = await queue.status();
  const one = await queue.drafts.save(save());
  const two = await queue.drafts.save(save(2, { draft: value({ to: [], pending: 'Jane D', subject: '', text: '' }) }));
  assert.equal(one.state, 'editing'); assert.equal(two.draft.pending, 'Jane D');
  const restarted = new MailQueue({ storage, blockConcurrencyWhile: (fn) => fn() }, env); await restarted.indexReady;
  assert.equal((await restarted.drafts.get(one.id)).draft.text, 'Private draft body.');
  assert.equal((await restarted.drafts.list()).total, 2);
  assert.deepEqual(await queue.status(), before);
  assert.equal(JSON.stringify([...storage.data]).includes('Private draft body'), false);
  assert.equal(JSON.stringify([...storage.data]).includes('Synthetic saved subject'), false);
  assert.equal([...storage.data.keys()].some((key) => key.startsWith('cmail-outbox:')), false);
  assert.doesNotThrow(() => new Function(draftsScript)); assert.doesNotThrow(() => new Function(composeScript));
});

test('Save retries are idempotent and concurrent editing rejects stale revisions without overwriting', async () => {
  const { queue } = await fixture();
  const original = await queue.drafts.save(save());
  assert.deepEqual(await queue.drafts.save(save()), original);
  const inputs = [2000, 2001].map((id) => ({ ...save(), revision: 1, save_id: uuid(id), draft: value({ text: 'Editor ' + id }) }));
  const results = await Promise.allSettled(inputs.map((input) => queue.drafts.save(input)));
  assert.equal(results.filter((result) => result.status === 'fulfilled').length, 1);
  assert.equal(results.find((result) => result.status === 'rejected').reason.message, 'draft_conflict');
  const latest = await queue.drafts.get(original.id); assert.equal(latest.revision, 2);
  assert.equal(latest.draft.text, results.find((result) => result.status === 'fulfilled').value.draft.text);
  assert.equal((await queue.drafts.list()).total, 1);
  await assert.rejects(queue.drafts.save({ ...inputs[0], save_id: save().save_id }), /draft_conflict|draft_changed/);
});

test('Large encrypted draft chunks round-trip and are removed when shortened or discarded', async () => {
  const { queue, storage } = await fixture();
  const text = '界'.repeat(100000);
  const big = await queue.drafts.save(save(1, { draft: value({ text }) }));
  assert.equal((await queue.drafts.get(big.id)).draft.text, text);
  assert([...storage.data.keys()].filter((key) => key.startsWith('cmail-drafts-v1:body:')).length > 1);
  const small = await queue.drafts.save({ ...save(), revision: 1, save_id: uuid(3000), draft: value({ text: 'Shortened' }) });
  assert.equal([...storage.data.keys()].filter((key) => key.startsWith('cmail-drafts-v1:body:')).length, 0);
  await queue.drafts.discard({ id: small.id, revision: small.revision });
  assert.equal((await queue.drafts.list()).total, 0);
  assert.equal((await queue.drafts.get(small.id)).state, 'discarded');
  assert.equal(JSON.stringify([...storage.data]).includes('Shortened'), false);
  await assert.rejects(queue.drafts.save(save()), /draft_closed/);
  assert.equal((await queue.drafts.discard({ id: small.id, revision: 2 })).state, 'discarded');
});

test('Draft list uses bounded index pages and mailbox counts, and never reads raw mail', async () => {
  const { queue, storage } = await fixture();
  for (let i = 1; i <= 56; i++) await queue.drafts.save(save(i, { draft: value({ sender: i % 2 ? support : primary, subject: 'Draft ' + i }) }));
  queue.loadPayload = neverSend;
  const first = await queue.drafts.list(), second = await queue.drafts.list({ cursor: first.next_cursor });
  const dates = first.items.map((item) => item.updated_at + ':' + item.id);
  assert.deepEqual(dates, [...dates].sort().reverse());
  assert.equal(first.items.length, 50); assert.equal(second.items.length, 6); assert.equal(second.next_cursor, null);
  assert.equal(new Set([...first.items, ...second.items].map((item) => item.id)).size, 56);
  assert.equal((await queue.drafts.list({ mailbox: support })).total, 28);
  const target = first.items[0];
  await queue.drafts.save(save(Number(target.id.slice(-12)), { revision: target.revision, save_id: uuid(6000), draft: value({ sender: target.sender === support ? primary : support }) }));
  assert.equal((await queue.drafts.list({ mailbox: target.sender })).total, 27);
  assert.equal((await queue.drafts.list({ query: 'Draft 55' })).items.length, 1);
  const originalGet = storage.get.bind(storage);
  storage.get = (keys) => { assert(![].concat(keys).some((key) => key.startsWith('cmail-drafts-v1:content:') || key.startsWith('cmail-drafts-v1:body:'))); return originalGet(keys); };
  const probe = await queue.performanceProbe({ operation: 'drafts' });
  assert.equal(probe.items, 50); assert.equal(probe.get_calls, 2); assert.equal(probe.list_calls, 1); assert.equal(probe.raw_payload_loads, 0);
});

test('Draft sending is explicit, preserves retries, removes only confirmed sends and rejects stale edits', async () => {
  const { queue, env } = await fixture(); env.CMAIL_FAST_READS = '1'; env.CMAIL_SENT_ENABLED = '1';
  const saved = await queue.drafts.save(save());
  let original;
  const pending = await queue.drafts.send({ id: saved.id, revision: saved.revision }, { fetcher: async (_, options) => { original = options; throw new Error('connection lost'); } });
  assert.equal(pending.state, 'sending'); assert.equal((await queue.drafts.list()).total, 1);
  await assert.rejects(queue.drafts.save({ ...save(), revision: 1, save_id: uuid(4000) }), /draft_sending/);
  await assert.rejects(queue.drafts.discard({ id: saved.id, revision: 1 }), /draft_sending/);
  const sent = await queue.drafts.send({ id: saved.id, revision: 1 }, { fetcher: async (_, options) => { assert.equal(options.body, original.body); assert.deepEqual(options.headers, original.headers); return accepted(); } });
  assert.equal(sent.state, 'sent'); assert.equal(sent.draft, undefined); assert.equal((await queue.drafts.list()).total, 0);
  assert.equal((await queue.listArchive({ view: 'sent' })).total_view, 1);
  assert.equal((await queue.views.conversation({ id: sent.archive_id })).total, 1);
  assert.deepEqual(await queue.drafts.send({ id: saved.id, revision: 1 }, { fetcher: neverSend }), sent);
  await assert.rejects(queue.drafts.save({ ...save(), revision: 1 }), /draft_closed/);
});

test('Provider rejection and input validation restore editing; accepted archive recovery does not resend', async () => {
  const { queue } = await fixture();
  let saved = await queue.drafts.save(save(1, { draft: value({ to: [], pending: 'unfinished' }) }));
  await assert.rejects(queue.drafts.send({ id: saved.id, revision: saved.revision }, { fetcher: neverSend }), /invalid_recipients/);
  saved = await queue.drafts.save({ ...save(), revision: 1, save_id: uuid(5000) });
  const rejected = await queue.drafts.send({ id: saved.id, revision: saved.revision }, { fetcher: async () => Response.json({}, { status: 422 }) });
  assert.equal(rejected.state, 'editing'); assert(rejected.revision > saved.revision);
  const enqueue = queue.enqueue.bind(queue); queue.enqueue = async () => { throw new Error('archive failure'); };
  const pending = await queue.drafts.send({ id: saved.id, revision: rejected.revision }, { fetcher: accepted });
  assert.equal(pending.state, 'sending'); assert.equal(pending.error, 'archive_pending');
  queue.enqueue = enqueue;
  assert.equal((await queue.drafts.send({ id: saved.id, revision: pending.revision }, { fetcher: neverSend })).state, 'sent');
});

test('Opening a draft repairs a confirmed outbox receipt after interrupted finalization without a send', async () => {
  const { queue } = await fixture(); const saved = await queue.drafts.save(save());
  const finish = queue.drafts.finish.bind(queue.drafts); queue.drafts.finish = async () => { throw new Error('crash after send'); };
  await assert.rejects(queue.drafts.send({ id: saved.id, revision: 1 }, { fetcher: accepted }), /crash after send/);
  queue.drafts = new DraftStore(queue);
  const result = await queue.drafts.get(saved.id); assert.equal(result.state, 'sent'); assert.equal((await queue.drafts.list()).total, 0);
  assert.deepEqual(await queue.drafts.send({ id: saved.id, revision: 1 }, { fetcher: neverSend }), result);
});

test('Reply drafts use server-derived routing and retain thread headers through save and send', async () => {
  const { queue, id, env } = await fixture(); env.CMAIL_FAST_READS = '1'; env.CMAIL_SENT_ENABLED = '1';
  const original = await queue.drafts.reply(id); assert.equal(original.context.sender, support);
  const saved = await queue.drafts.save(save(1, { draft: value({ kind: 'reply', reply_id: id, sender: primary, to: ['injected@example.test'], subject: 'Injected subject' }) }));
  assert.equal(saved.draft.sender, support); assert.deepEqual(saved.draft.to, ['reply@example.test']); assert.equal(saved.draft.subject, 'Re: Reply fixture');
  assert.equal((await queue.drafts.reply(id)).id, saved.id);
  const sent = await queue.drafts.send({ id: saved.id, revision: saved.revision }, { fetcher: async (_, options) => {
    const request = JSON.parse(options.body); assert.equal(request.headers['In-Reply-To'], '<original@example.test>'); assert.deepEqual(request.to, ['reply@example.test']); return accepted();
  } });
  assert.equal((await queue.views.conversation({ id: sent.archive_id })).total, 2);
  assert((await queue.drafts.reply(id)).context);
});

test('Draft endpoints enforce Access, feature activation and explicit same-origin mutation; saving never sends', async () => {
  const { env, queue } = await fixture();
  const allowed = async () => ({ ok: true, email: primary });
  const base = withReplies(withResend(createWorker(allowed)), allowed);
  const denied = withDrafts(base, async () => ({ ok: false, status: 401, error: 'unauthorized' }));
  const worker = withDrafts(base, allowed), origin = 'https://cmail.frg.earth';
  assert.equal((await denied.fetch(new Request(origin + '/cmail/api/drafts'), env)).status, 401);
  assert.equal((await worker.fetch(new Request(origin + '/cmail/api/drafts'), env)).status, 404);
  env.CMAIL_DRAFTS_ENABLED = '1';
  for (const headers of [{}, { Origin: 'https://evil.test', 'X-Cmail-Action': 'draft' }, { Origin: origin, 'X-Cmail-Action': 'compose' }]) {
    assert.equal((await worker.fetch(new Request(origin + '/cmail/api/drafts/save', { method: 'POST', headers, body: '{}' }), env)).status, 403);
  }
  const response = await worker.fetch(new Request(origin + '/cmail/api/drafts/save', { method: 'POST', headers: { Origin: origin, 'X-Cmail-Action': 'draft', 'Content-Type': 'application/json' }, body: JSON.stringify(save()) }), env);
  assert.equal(response.status, 200); assert.equal((await response.json()).state, 'editing');
  assert.equal((await queue.status()).total_stored, 1);
  const html = await (await worker.fetch(new Request(origin), env)).text();
  assert.match(html, /value="drafts">Drafts/); assert.match(html, /data-cmail-drafts="1"/);
  await assert.rejects(queue.drafts.save(save(2, { draft: value({ sender: '__proto__' }) })), /invalid_draft/);
});
