import test from 'node:test';
import assert from 'node:assert/strict';
import { MailQueue, createWorker, withResend } from '../src/index.mjs';
import { withReplies } from '../src/reply-http.mjs';
import { Storage, bindings, support, primary } from './reply-fixture.mjs';

function mail(n, { outbound = false, mailbox = support, references = '', messageId = `<sent-test-${n}@example.test>`, parent, body = `Body ${n}`, date, from } = {}) {
  const received_at = date || new Date(Date.UTC(2026, 9, 1, 0, n)).toISOString();
  const raw = `From: ${from || (outbound ? mailbox : 'Player <player@example.test>')}\r\nTo: ${outbound ? 'Player <player@example.test>' : mailbox}\r\nSubject: Topic ${n}\r\nDate: ${new Date(received_at).toUTCString()}\r\n${messageId ? `Message-ID: ${messageId}\r\n` : ''}References: ${references}\r\n\r\n${body}\r\n`;
  return { envelope_to: mailbox, direction: outbound ? 'outbound' : 'inbound', received_at, raw_base64: Buffer.from(raw).toString('base64'), reply_to_archive_id: parent };
}
async function fixture() {
  const storage = new Storage(), state = { storage, blockConcurrencyWhile: (fn) => fn() };
  const env = { ...bindings, CMAIL_FAST_READS: '1', CMAIL_SENT_ENABLED: '1' };
  const queue = new MailQueue(state, env); await queue.indexReady;
  const add = (n, options) => queue.enqueue(mail(n, options), { archiveOnly: true, read: options?.outbound === true, archived: options?.outbound === true });
  return { storage, state, env, queue, add };
}

test('Sent contains standalone sends and one row for a complete mixed conversation, independent of Archive', async () => {
  const { queue, add } = await fixture();
  const inbound = await add(1);
  const standalone = await add(2, { outbound: true });
  await add(3, { outbound: true, references: '<sent-test-1@example.test>' });
  await add(4, { outbound: true, references: '<sent-test-1@example.test> <sent-test-3@example.test>' });
  const latest = await add(5, { references: '<sent-test-4@example.test>' });
  await add(6, { from: support }); // A sender header alone must never create Sent membership.
  let sent = await queue.listArchive({ view: 'sent' });
  assert.equal(sent.total_view, 2);
  assert.deepEqual(sent.items.map((item) => item.id), [latest.id, standalone.id]);
  assert.equal(sent.items[0].sent_to, 'Player <player@example.test>');
  const conversation = await queue.views.conversation({ id: latest.id });
  assert.equal(conversation.total, 4);
  assert.equal(conversation.items.at(-1).id, inbound.id);
  assert.equal((await queue.views.conversation({ id: standalone.id })).total, 1);
  await queue.applyAction({ ids: [latest.id, standalone.id], action: 'archive' });
  await queue.applyAction({ ids: [latest.id, standalone.id], action: 'unarchive' });
  sent = await queue.listArchive({ view: 'sent' });
  assert.equal(sent.total_view, 2);
  await queue.setReadStatus({ id: latest.id, read: true });
  assert.equal((await queue.listArchive({ view: 'sent' })).items[0].read, true);
});

test('late linking merges Sent groups and counters across identities, including replies without Message-ID', async () => {
  const { queue, add } = await fixture();
  const a = await add(1, { outbound: true, references: '<missing-a@example.test>' });
  await add(2, { outbound: true, mailbox: primary, references: '<missing-b@example.test>' });
  assert.equal((await queue.sent.readiness()).conversations, 2);
  const merged = await add(3, { references: '<missing-a@example.test> <missing-b@example.test>' });
  assert.equal((await queue.sent.readiness()).conversations, 1);
  for (const mailbox of ['', support, primary]) {
    const page = await queue.listArchive({ view: 'sent', mailbox });
    assert.equal(page.total_view, 1); assert.equal(page.items[0].id, merged.id);
    if (mailbox) assert.equal(page.items[0].mailbox, mailbox);
  }
  assert.equal((await queue.views.conversation({ id: a.id })).total, 3);
  const noHeader = await add(4, { messageId: '' });
  const reply = await add(5, { outbound: true, parent: noHeader.id });
  assert.equal((await queue.views.conversation({ id: reply.id })).total, 2);
  assert.equal((await queue.sent.readiness()).sent_messages, 3);
  assert.equal((await queue.sent.readiness()).conversations, 2);
});

test('Sent uses bounded indexed pagination, mailbox/date filters and complete-thread body search', async () => {
  const { queue, add } = await fixture();
  const expected = [];
  for (let n = 1; n <= 150; n++) {
    const result = await add(n, { outbound: true, mailbox: n % 3 ? support : primary });
    if (n % 3) expected.unshift(result.id);
  }
  queue.loadPayload = async () => { throw new Error('ordinary Sent listing must not load original MIME'); };
  const seen = []; let cursor;
  do {
    const page = await queue.listArchive({ view: 'sent', mailbox: support, cursor, limit: 20 });
    assert.equal(page.total_view, 100); seen.push(...page.items.map((item) => item.id)); cursor = page.next_cursor;
  } while (cursor);
  assert.deepEqual(seen, expected);
  const probe = await queue.performanceProbe({ parameters: { view: 'sent', mailbox: support, limit: 20 } });
  assert.equal(probe.raw_payload_loads, 0); assert.equal(probe.list_calls, 1); assert(probe.get_calls <= 4, JSON.stringify(probe));
  // Read, archived and Spam flags share the same batched state read.
  assert(probe.keys_read <= 110, JSON.stringify(probe));
  const root = await add(151, { body: 'Needle in an older inbound message' });
  const response = await add(152, { outbound: true, references: '<sent-test-151@example.test>' });
  let matched = []; cursor = null;
  do {
    const page = await queue.listArchive({ view: 'sent', query: 'needle', cursor });
    matched.push(...page.items.map((item) => item.id)); cursor = page.next_cursor;
  } while (cursor);
  assert.deepEqual(matched, [response.id]);
  assert.equal((await queue.views.conversation({ id: root.id })).total, 2);
  assert.equal((await queue.listArchive({ view: 'sent', after: '2026-10-02' })).items.length, 0);
  await assert.rejects(queue.listArchive({ view: 'sent', cursor: root.id }), /invalid_request/);
});

test('Sent backfill resumes, merges out-of-order history, and preserves originals and mail state', async () => {
  const { queue, storage, state, env, add } = await fixture();
  const first = await add(1);
  await add(2, { outbound: true, references: '<sent-test-1@example.test>' });
  await add(3, { outbound: true });
  await queue.setReadStatus({ id: first.id, read: true });
  for (const key of [...storage.data.keys()]) if (key.startsWith('cmail-sent-v1:')) storage.data.delete(key);
  const originals = [...storage.data].filter(([key]) => key.startsWith('mail:') || key.startsWith('archive:') || key.startsWith('cmail-read:') || key.startsWith('cmail-archived:'));
  await assert.rejects(queue.listArchive({ view: 'sent' }), /sent_index_warming/);
  assert.equal((await queue.sent.backfill({ limit: 1 })).ready, false);
  await add(4, { references: '<sent-test-2@example.test>' });
  const restarted = new MailQueue(state, env); await restarted.indexReady;
  while (!(await restarted.sent.backfill({ limit: 1 })).ready) {}
  assert.equal((await restarted.sent.readiness()).conversations, 2);
  assert.equal((await restarted.sent.backfill()).imported, 0);
  for (const [key, value] of originals) assert.deepEqual(storage.data.get(key), value);
  assert.equal((await restarted.readArchiveMessage({ id: first.id })).read, true);
  const realPut = restarted.sent.put;
  restarted.sent.put = async () => false; // Simulate an older writer during rollback.
  await restarted.enqueue(mail(5, { outbound: true }), { archiveOnly: true, archived: true });
  restarted.sent.put = realPut;
  for (let run = 0; run < 10 && !(await restarted.sent.readiness()).ready; run++) await restarted.sent.backfill({ limit: 2 });
  assert.equal((await restarted.sent.readiness()).ready, true);
  assert.equal((await restarted.sent.readiness()).conversations, 3);
});

test('Sent HTML and list stay authenticated and the view is exposed only after activation', async () => {
  const { queue, env, add } = await fixture();
  await add(1, { outbound: true });
  env.MAIL_QUEUE = { getByName: () => ({ fetch: (url, init) => queue.fetch(new Request(url, init)) }) };
  const allow = async () => ({ ok: true, email: 'fixture@example.test' });
  const worker = withReplies(withResend(createWorker(allow)), allow);
  const html = await (await worker.fetch(new Request('https://cmail.frg.earth/'), env)).text();
  assert.match(html, /<option value="sent">Sent<\/option>/);
  const page = await worker.fetch(new Request('https://cmail.frg.earth/cmail/api/messages?view=sent'), env);
  assert.equal(page.status, 200); assert.equal((await page.json()).total_view, 1);
  const disabled = await (await worker.fetch(new Request('https://cmail.frg.earth/'), { ...env, CMAIL_SENT_ENABLED: '0' })).text();
  assert.doesNotMatch(disabled, /<option value="sent">/);
  const deny = async () => ({ ok: false, status: 401, error: 'unauthorized' });
  const protectedWorker = withReplies(withResend(createWorker(deny)), deny);
  assert.equal((await protectedWorker.fetch(new Request('https://cmail.frg.earth/cmail/api/messages?view=sent'), env)).status, 401);
});

test('history import accepts sent mail only for configured identities and leaves foreign inbound imports rejected', async () => {
  const { queue } = await fixture();
  const imported = (input) => queue.fetch(new Request('https://internal/resend/import', { method: 'POST', body: JSON.stringify(input) }));
  const sent = { ...mail(1, { outbound: true, mailbox: primary }), provider: 'resend-sent-history', read: true };
  assert.equal((await imported(sent)).status, 200);
  assert.equal((await imported({ ...sent, envelope_to: 'unknown@example.test' })).status, 400);
  assert.equal((await imported({ ...mail(2, { mailbox: primary }), provider: 'resend-receiving' })).status, 400);
  assert.equal((await queue.listArchive({ view: 'sent', mailbox: primary })).total_view, 1);
});
