import test from 'node:test';
import assert from 'node:assert/strict';
import { prepareCompose, sendCompose, sendReply } from '../src/reply.mjs';
import { withReplies } from '../src/reply-http.mjs';
import { createWorker, withResend } from '../src/index.mjs';
import { matchContacts, composeScript } from '../src/compose-ui.mjs';
import { fixture, uuid, support, primary } from './reply-fixture.mjs';

const input = (n = 1) => ({ request_id: uuid(n), sender: support, to: ['new.person@example.test'], subject: 'A new conversation ✉', text: 'Hello from a synthetic fixture.\n\nBind My Soul Support' });
const accepted = async () => Response.json({ id: uuid(900) });
const neverSend = async () => { throw new Error('Unexpected provider send'); };

test('Compose validates identity, recipient boundaries and headers before any provider call', async () => {
  const { queue } = await fixture();
  for (const changed of [
    { sender: 'outsider@example.test' }, { to: [] }, { to: ['Name <a@example.test>'] },
    { to: ['a@example.test\n'] }, { to: ['a@example.test\r\nBcc: b@example.test'] },
    { to: Array(11).fill('a@example.test') }, { subject: 'Title\r\nBcc: b@example.test' },
    { subject: 'x'.repeat(1001) }, { text: '' }, { text: '\n\nBind My Soul Support\nhttps://bindmysoul.com' },
  ]) await assert.rejects(sendCompose(queue, { ...input(), ...changed }, { fetcher: neverSend }));
  assert.deepEqual(prepareCompose(queue, { ...input(), to: ['FIRST@example.test', 'first@example.test'] }).to, ['first@example.test']);
});

test('Compose archives a standalone Sent conversation, using the chosen identity and stable replay', async () => {
  const { queue, storage, env } = await fixture(); env.CMAIL_FAST_READS = '1'; env.CMAIL_SENT_ENABLED = '1';
  let submitted, calls = 0;
  const result = await sendCompose(queue, input(), { fetcher: async (_, options) => {
    calls++; assert.equal(options.headers.Authorization, 'Bearer fixture-support-key');
    assert.equal(options.headers['Idempotency-Key'], 'cmail-compose/' + uuid(1));
    submitted = JSON.parse(options.body); return accepted();
  } });
  assert.equal(result.state, 'sent'); assert.equal(calls, 1);
  assert.equal(submitted.from, `Bind My Soul Support <${support}>`); assert.deepEqual(submitted.to, input().to);
  assert.equal(submitted.headers['In-Reply-To'], undefined); assert.equal(submitted.headers.References, undefined);
  const message = await queue.views.message(result.archive_id);
  assert.equal(message.subject, input().subject); assert.equal(message.text.trim(), input().text);
  assert.equal(message.from.address, support); assert.equal(message.to[0].address, input().to[0]);
  const sent = await queue.listArchive({ view: 'sent' }); assert.equal(sent.total_view, 1);
  assert.equal((await queue.views.conversation({ id: result.archive_id })).total, 1);
  assert.equal((await queue.status()).pending_count, 0);
  assert.equal(JSON.stringify([...storage.data]).includes(input().text), false);
  assert.deepEqual(await sendCompose(queue, input(), { fetcher: neverSend }), result);
  await sendCompose(queue, { ...input(2), sender: primary }, { fetcher: async (_, options) => { assert.equal(options.headers.Authorization, 'Bearer fixture-primary-key'); return accepted(); } });
});

test('Uncertain Compose binds every field and cannot cross over to a reply or resend on archive recovery', async () => {
  const { queue, id } = await fixture(); let original;
  const first = await sendCompose(queue, input(), { fetcher: async (_, options) => { original = options; throw new Error('Lost connection'); } });
  assert.equal(first.state, 'uncertain');
  for (const changed of [{ sender: primary }, { to: ['other@example.test'] }, { subject: 'Changed' }, { text: 'Changed' }]) {
    await assert.rejects(sendCompose(queue, { ...input(), ...changed }, { fetcher: neverSend }), /reply_changed/);
  }
  await assert.rejects(sendReply(queue, { id, request_id: uuid(1), text: input().text }, { fetcher: neverSend }), /reply_changed/);
  const enqueue = queue.enqueue.bind(queue); queue.enqueue = async () => { throw new Error('storage failed'); };
  const retry = await sendCompose(queue, input(), { fetcher: async (_, options) => { assert.equal(options.body, original.body); assert.deepEqual(options.headers, original.headers); return accepted(); } });
  assert.equal(retry.state, 'accepted'); queue.enqueue = enqueue;
  assert.equal((await sendCompose(queue, input(), { fetcher: neverSend })).state, 'sent');
});

test('Contact history merges inbound senders and Reply-To with outgoing recipients, excluding own accounts', async () => {
  const { queue, storage } = await fixture();
  await sendCompose(queue, { ...input(), to: ['PLAYER@example.test', primary, 'new.person@example.test'] }, { fetcher: accepted });
  const readiness = await queue.contacts.readiness(); assert.equal(readiness.contacts, 3); assert.equal(readiness.ready, true);
  const raw = queue.loadPayload; queue.loadPayload = neverSend;
  const catalog = await queue.contacts.list(); queue.loadPayload = raw;
  const byAddress = new Map(catalog.items.map((contact) => [contact.address, contact]));
  assert.equal(byAddress.get('player@example.test').sent, 1); assert.equal(byAddress.get('player@example.test').received, 1);
  assert.equal(byAddress.get('player@example.test').name, 'Player'); assert.equal(byAddress.get('reply@example.test').name, 'Reply Desk');
  assert.equal(byAddress.has(primary), false); assert.equal(byAddress.has(support), false);
  const indexRows = [...storage.data].filter(([key]) => key.startsWith('cmail-contacts-v1:'));
  assert.equal(JSON.stringify(indexRows).includes('player@example.test'), false);
  assert.equal(JSON.stringify(indexRows).includes('Reply Desk'), false);
  const probe = await queue.performanceProbe({ operation: 'contacts' });
  assert.equal(probe.list_calls, 1); assert.equal(probe.get_calls, 2); assert.equal(probe.raw_payload_loads, 0);
});

test('Contact backfill is resumable and idempotent, without changing raw archive or mail state', async () => {
  const { queue, storage, id } = await fixture();
  await sendCompose(queue, input(), { fetcher: accepted });
  for (const key of [...storage.data.keys()]) if (key.startsWith('cmail-contacts-v1:')) storage.data.delete(key);
  const original = [...storage.data];
  await assert.rejects(queue.contacts.list(), /contacts_warming/);
  assert.equal((await queue.contacts.backfill({ limit: 1 })).more, true);
  assert.equal((await queue.contacts.backfill({ limit: 1 })).ready, true);
  assert.equal((await queue.contacts.backfill()).imported, 0);
  assert.deepEqual([...storage.data].filter(([key]) => !key.startsWith('cmail-contacts-v1:')), original);
  assert.equal((await queue.contacts.list()).total, 3);
  assert.equal((await queue.views.message(id)).read, false);
});

test('Contact catalog pages are bounded, complete and restart when a write changes the revision', async () => {
  const { queue, storage } = await fixture();
  const body = { to: Array.from({ length: 300 }, (_, i) => ({ address: `person${i}@example.test`, name: `Person ${i}` })), cc: [] };
  const prepared = await queue.contacts.prepare(body, { direction: 'outbound', received_at: '2026-10-18T00:00:00Z' });
  await storage.transaction(async (tx) => { await queue.contacts.put(tx, 'synthetic-contacts', prepared); const meta = await tx.get('meta'); await tx.put('meta', { ...meta, total_stored: meta.total_stored + 1 }); });
  const first = await queue.contacts.list(); assert.equal(first.items.length, 250); assert(first.next_cursor);
  const second = await queue.contacts.list({ cursor: first.next_cursor, revision: first.revision });
  assert.equal(new Set([...first.items, ...second.items].map((item) => item.address)).size, 302); assert.equal(second.next_cursor, null);
  await sendCompose(queue, input(), { fetcher: accepted });
  await assert.rejects(queue.contacts.list({ cursor: first.next_cursor, revision: first.revision }), /contacts_changed/);
});

test('Autocomplete narrows by name or email, ranks prior sends, excludes selected and hides complete addresses', () => {
  const contacts = [
    { address: 'jane@example.test', name: 'Jane Doe', last_seen: '2026-10-19', sent: 2 },
    { address: 'jason@example.test', name: 'Jason Dóe', last_seen: '2026-10-20', sent: 0 },
    { address: 'unrelated@example.test', name: 'Other Person', last_seen: '2026-10-20', sent: 99 },
  ];
  assert.equal(matchContacts(contacts, 'j').total, 0);
  assert.equal(matchContacts(contacts, 'ja').total, 2); assert.equal(matchContacts(contacts, 'ja').items[0].address, 'jane@example.test');
  assert.equal(matchContacts(contacts, 'jane').total, 1); assert.equal(matchContacts(contacts, 'doe jason').total, 1);
  assert.equal(matchContacts(contacts, 'jane@example.test').total, 0);
  assert.equal(matchContacts(contacts, 'ja', ['jane@example.test']).total, 1);
  assert.equal(matchContacts(contacts, 'nobody').total, 0);
  assert.doesNotThrow(() => new Function(composeScript));
});

test('Compose and contacts require Access, activation and same-origin explicit send; credentials stay server-side', async () => {
  const { env } = await fixture();
  const denied = withReplies(createWorker(), async () => ({ ok: false, status: 401, error: 'unauthorized' }));
  const allowed = async () => ({ ok: true, email: primary });
  const worker = withReplies(withResend(createWorker(allowed)), allowed);
  const url = 'https://cmail.frg.earth/cmail/api/compose';
  for (const path of ['/cmail/api/compose', '/cmail/api/contacts', '/cmail/compose.js']) assert.equal((await denied.fetch(new Request('https://cmail.frg.earth' + path), env)).status, 401);
  assert.equal((await worker.fetch(new Request(url), env)).status, 404); env.CMAIL_COMPOSE_ENABLED = '1';
  const context = await (await worker.fetch(new Request(url), env)).json();
  assert.equal(context.identities.length, 2); assert.equal(JSON.stringify(context).includes('credential'), false);
  assert.equal(JSON.stringify(context).includes('fixture-support-key'), false);
  for (const headers of [{}, { Origin: 'https://evil.test', 'X-Cmail-Action': 'compose' }, { Origin: 'https://cmail.frg.earth', 'X-Cmail-Action': 'reply' }]) assert.equal((await worker.fetch(new Request(url, { method: 'POST', headers, body: '{}' }), env)).status, 403);
  let proxied;
  env.MAIL_QUEUE = { getByName: () => ({ fetch: async (_, init) => { proxied = JSON.parse(init.body); return Response.json({ state: 'fixture' }); } }) };
  await worker.fetch(new Request(url, { method: 'POST', headers: { Origin: 'https://cmail.frg.earth', 'X-Cmail-Action': 'compose', 'Content-Type': 'application/json' }, body: JSON.stringify({ ...input(), credential: 'injected', from: 'fake@example.test', headers: { Bcc: 'fake@example.test' } }) }), env);
  assert.deepEqual(proxied, input());
  assert.match(await (await worker.fetch(new Request('https://cmail.frg.earth/'), env)).text(), /src="\/cmail\/compose.js"/);
});
