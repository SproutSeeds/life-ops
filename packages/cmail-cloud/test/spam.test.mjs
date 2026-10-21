import test from 'node:test';
import assert from 'node:assert/strict';
import { MailQueue, createWorker, withResend } from '../src/index.mjs';
import { withReplies } from '../src/reply-http.mjs';
import { handleAgent } from '../src/agent-api.mjs';
import { spamAddress } from '../src/spam.mjs';
import { Storage, bindings, primary, support } from './reply-fixture.mjs';

import { spamPayload } from './spam-fixture.mjs';
async function fixture() {
  const storage = new Storage(), state = { storage, blockConcurrencyWhile: (fn) => fn() };
  const env = { ...bindings, CMAIL_FAST_READS: '1', CMAIL_SENT_ENABLED: '1', CMAIL_AGENT_ENABLED: '1' };
  const queue = new MailQueue(state, env); await queue.indexReady;
  env.MAIL_QUEUE = { getByName: () => ({ fetch: (url, init) => queue.fetch(new Request(url, init)) }) };
  return { queue, storage, state, env };
}
const add = (queue, n, options, flags = {}) => queue.enqueue(spamPayload(n, options), { archiveOnly: true, ...flags });
const action = (queue, name, ...ids) => queue.applyAction({ action: name, ids });

test('Spam archives selected mail and exact future From addresses across inboxes, without a history sweep', async () => {
  const { queue } = await fixture();
  const old = await add(queue, 1), selected = await add(queue, 2);
  await action(queue, 'spam', selected.id);
  const caught = await add(queue, 3, { from: 'New name <OFFERS@example.test>', mailbox: support });
  for (const [n, options] of [[4, { from: 'other@example.test' }], [5, { from: 'offers@sub.example.test' }], [6, { from: 'offers+tag@example.test' }], [7, { from: 'safe@example.test', replyTo: 'offers@example.test' }]]) await add(queue, n, options);
  const spam = await queue.listArchive({ view: 'spam' });
  assert.deepEqual(spam.items.map(i => i.id), [caught.id, selected.id]);
  assert(spam.items.every(i => i.spam && i.archived));
  assert.equal((await queue.listArchive({ view: 'inbox' })).total_view, 5);
  assert((await queue.listArchive({ view: 'inbox' })).items.some(i => i.id === old.id));
  assert.equal((await queue.listArchive({ view: 'archive' })).total_view, 0);
  assert.equal((await queue.listArchive({ view: 'all' })).total_view, 7);
  assert.equal((await queue.listArchive({ view: 'spam', mailbox: support })).total_view, 1);
  assert.equal((await queue.spam.list()).items[0].sender, 'offers@example.test');
});

test('Not spam restores selected mail and stops future filtering while leaving other quarantined mail recoverable', async () => {
  const { queue, storage } = await fixture();
  const first = await add(queue, 1, { attachment: true }, { read: true });
  const original = await queue.readArchiveMessage({ id: first.id }), raw = await queue.readArchiveMessage({ id: first.id, raw: true });
  const attachment = await queue.readArchiveMessage({ id: first.id, attachment: 0 });
  await action(queue, 'spam', first.id); await action(queue, 'spam', first.id);
  const caught = await add(queue, 2);
  await action(queue, 'not-spam', first.id); await action(queue, 'not-spam', first.id);
  const next = await add(queue, 3);
  assert.deepEqual((await queue.listArchive({ view: 'inbox' })).items.map(i => i.id), [next.id, first.id]);
  assert.deepEqual((await queue.listArchive({ view: 'spam' })).items.map(i => i.id), [caught.id]);
  assert.equal((await queue.spam.list()).items.length, 0);
  assert.deepEqual(await queue.readArchiveMessage({ id: first.id }), original);
  assert.deepEqual(await queue.readArchiveMessage({ id: first.id, raw: true }), raw);
  assert.deepEqual(await queue.readArchiveMessage({ id: first.id, attachment: 0 }), attachment);
  assert.equal(await storage.get('cmail-archived-count'), 1);
  assert((await add(queue, 1, { attachment: true })).duplicate);
  assert.equal((await queue.readArchiveMessage({ id: first.id })).spam, false);
});

test('ordinary archive/read actions preserve Spam and unarchive directs callers to Not spam', async () => {
  const { queue } = await fixture(); const first = await add(queue, 1), second = await add(queue, 2, { from: 'safe@example.test' });
  await action(queue, 'archive', first.id); await action(queue, 'spam', first.id);
  await action(queue, 'archive', first.id); await action(queue, 'read', first.id);
  assert.equal((await queue.listArchive({ view: 'spam' })).total_view, 1);
  assert.equal((await queue.listArchive({ view: 'archive' })).total_view, 0);
  await assert.rejects(action(queue, 'unarchive', second.id, first.id), /use_not_spam_to_restore/);
  assert.equal((await queue.readArchiveMessage({ id: first.id })).read, true);
  await action(queue, 'not-spam', first.id);
  assert.equal((await queue.listArchive({ view: 'inbox' })).total_view, 2);
});

test('rules survive cold restarts and provider replay, and encrypted storage contains no sender plaintext', async () => {
  const { queue, storage, state, env } = await fixture(); const first = await add(queue, 1);
  await action(queue, 'spam', first.id);
  assert(!JSON.stringify([...storage.data]).includes('offers@example.test'));
  const restarted = new MailQueue(state, env); await restarted.indexReady;
  assert((await add(restarted, 1)).duplicate);
  const future = await add(restarted, 2);
  assert.equal((await restarted.readArchiveMessage({ id: future.id })).spam, true);
  assert.equal((await restarted.listArchive({ view: 'spam' })).total_view, 2);
  assert.equal((await restarted.spam.list()).items.length, 1);
});

test('outbound mail and owned addresses cannot create rules or be auto-caught, including forged self From', async () => {
  const { queue } = await fixture(); const first = await add(queue, 1);
  await action(queue, 'spam', first.id);
  const outgoing = await add(queue, 2, { from: 'offers@example.test', direction: 'outbound' });
  const own = await add(queue, 3, { from: primary });
  assert.equal((await queue.readArchiveMessage({ id: outgoing.id })).spam, false);
  for (const id of [outgoing.id, own.id]) await assert.rejects(action(queue, 'spam', id), /sender_cannot_be_blocked/);
  assert.equal((await queue.spam.list()).items.length, 1);
  assert.equal((await queue.listArchive({ view: 'sent' })).total_view, 1);
});

test('a failed or invalid bulk action changes no flags, indexes or sender rules', async () => {
  const { queue } = await fixture(); const first = await add(queue, 1), second = await add(queue, 2, { from: 'another@example.test' }), own = await add(queue, 3, { from: primary });
  await assert.rejects(action(queue, 'spam', first.id, own.id), /sender_cannot_be_blocked/);
  await assert.rejects(action(queue, 'spam', first.id, first.id), /invalid_request/);
  await assert.rejects(action(queue, 'spam', first.id, 'mail_0000000000099999'), /message_not_found/);
  const originalMove = queue.views.moveArchive.bind(queue.views);
  queue.views.moveArchive = async (...args) => { await originalMove(...args); if (args[1] === second.id) throw Error('synthetic storage failure'); };
  await assert.rejects(action(queue, 'spam', first.id, second.id), /synthetic storage failure/);
  assert.equal((await queue.spam.list()).items.length, 0);
  assert.equal((await queue.listArchive({ view: 'spam' })).total_view, 0);
  assert.equal((await queue.listArchive({ view: 'inbox' })).total_view, 3);
  queue.views.moveArchive = originalMove;
  await action(queue, 'spam', first.id, second.id);
  assert.equal((await queue.spam.list()).items.length, 2);
});

test('concurrent marks and incoming deliveries serialize without lost rules or double-counted messages', async () => {
  const { queue } = await fixture(); const first = await add(queue, 1), second = await add(queue, 2, { from: 'second@example.test' });
  await Promise.all([action(queue, 'spam', first.id), action(queue, 'spam', second.id), action(queue, 'spam', first.id)]);
  const results = await Promise.all([add(queue, 3), add(queue, 3), add(queue, 4, { from: 'second@example.test' })]);
  assert.equal(results.filter(r => r.duplicate).length, 1);
  assert.equal((await queue.listArchive({ view: 'spam' })).total_view, 4);
  assert.equal((await queue.listArchive({ view: 'inbox' })).total_view, 0);
  assert.equal((await queue.spam.list()).items.length, 2);
});

test('Spam search and pagination are newest-first with bounded index reads and no raw mail scans', async () => {
  const { queue } = await fixture(); const first = await add(queue, 1); await action(queue, 'spam', first.id);
  const ids = [first.id]; for (let n = 2; n <= 60; n++) ids.push((await add(queue, n)).id);
  queue.loadPayload = async () => { throw Error('No raw reads allowed'); };
  const seen = []; let cursor;
  do { const page = await queue.listArchive({ view: 'spam', limit: 20, cursor }); seen.push(...page.items.map(i => i.id)); cursor = page.next_cursor; assert.equal(page.total_view, 60); } while(cursor);
  assert.deepEqual(seen, ids.reverse());
  assert.equal((await queue.listArchive({ view: 'spam', query: 'fixture 42' })).items.length, 1);
  const probe = await queue.performanceProbe({ parameters: { view: 'spam', limit: 20 } });
  assert.equal(probe.list_calls, 1); assert(probe.get_calls <= 4); assert(probe.keys_read <= 85); assert.equal(probe.raw_payload_loads, 0);
});

test('legacy indexed mail derives its direction only on selection and remains reversible without backfill', async () => {
  const { queue } = await fixture(); const first = await add(queue, 1);
  const originalBody = queue.views.body.bind(queue.views);
  queue.views.body = async (...args) => { const { direction, ...legacy } = await originalBody(...args); return legacy; };
  await action(queue, 'spam', first.id); await action(queue, 'not-spam', first.id);
  assert.equal((await queue.listArchive({ view: 'inbox' })).total_view, 1);
});

test('browser and agents share Spam, with same-origin and access enforcement on every action', async () => {
  const { queue, env } = await fixture(); const first = await add(queue, 1);
  const worker = withReplies(withResend(createWorker(async () => ({ ok: true, email: primary }))), async () => ({ ok: true }));
  const body = JSON.stringify({ ids: [first.id], action: 'spam' });
  const call = (headers, method='POST') => worker.fetch(new Request('https://cmail.frg.earth/cmail/api/messages/actions', { method, headers, ...(method === 'POST' ? {body} : {}) }), env, {});
  assert.equal((await call({'Content-Type':'application/json'})).status, 403);
  assert.equal((await call({'Content-Type':'application/json',Origin:'https://evil.test'})).status, 403);
  assert.equal((await call({'Content-Type':'application/json',Origin:'https://cmail.frg.earth','Sec-Fetch-Site':'cross-site'})).status, 403);
  assert.equal((await call({},'GET')).status, 405);
  const denied = withResend(createWorker(async () => ({ok:false,status:401,error:'unauthorized'})));
  assert.equal((await denied.fetch(new Request('https://cmail.frg.earth/cmail/api/messages/actions',{method:'POST',body}),env,{})).status,401);
  assert.equal((await queue.spam.list()).items.length,0);
  assert.equal((await call({'Content-Type':'application/json',Origin:'https://cmail.frg.earth'})).status,200);
  const agent = (action,parameters) => handleAgent(queue,new Request('https://internal/agent/request',{method:'POST',body:JSON.stringify({action,parameters})}));
  assert.equal((await (await agent('messages',{view:'spam'})).json()).items[0].id,first.id);
  assert.equal((await (await agent('spam-senders',{})).json()).items[0].sender,'offers@example.test');
  assert.equal((await agent('message-action',{action:'not-spam',ids:[first.id]})).status,200);
  assert.equal((await queue.spam.list()).items.length,0);
});

test('sender normalization rejects domains and malformed addresses without folding aliases', () => {
  assert.equal(spamAddress(' OFFERS@Example.test '),'offers@example.test');
  assert.equal(spamAddress('offers+tag@example.test'),'offers+tag@example.test');
  for(const value of ['example.test','A <a@example.test>','a@example.test\nBcc: x@example.test','']) assert.equal(spamAddress(value),'');
});

test('mail can still be restored if a blocked sender later becomes an owned identity', async () => {
  const {queue,env} = await fixture(); const first = await add(queue,1);
  await action(queue,'spam',first.id);
  env.CMAIL_MAILBOXES += ',offers@example.test';
  await action(queue,'not-spam',first.id);
  assert.equal((await queue.readArchiveMessage({id:first.id})).spam,false);
  assert.equal((await queue.spam.list()).items.length,0);
  assert.equal((await queue.listArchive({view:'inbox'})).total_view,1);
});
