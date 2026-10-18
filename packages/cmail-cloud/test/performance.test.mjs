import test from 'node:test';
import assert from 'node:assert/strict';
import { MailQueue } from '../src/index.mjs';
import { MailQueue as LegacyQueue } from '../src/production-base.mjs';
import { Storage, bindings, support, primary } from './reply-fixture.mjs';

function payload(n, { mailbox = support, date, messageId, references = '', body = `Body ${n}`, subject = `Mail ${n}` } = {}) {
  const raw = `From: Player <player@example.test>\r\nTo: ${mailbox}\r\nSubject: ${subject}\r\nMessage-ID: ${messageId || `<mail-${n}@example.test>`}\r\nReferences: ${references}\r\nDate: ${new Date(date || Date.UTC(2026, 9, n)).toUTCString()}\r\n\r\n${body}\r\n`;
  return { envelope_to: mailbox, received_at: date || new Date(Date.UTC(2026, 9, n)).toISOString(), raw_base64: Buffer.from(raw).toString('base64') };
}
async function fixture() {
  const storage = new Storage(), state = { storage, blockConcurrencyWhile: (fn) => fn() };
  const env = { ...bindings, CMAIL_FAST_READS: '1' };
  const queue = new MailQueue(state, env); await queue.indexReady;
  return { storage, state, env, queue };
}

test('indexed mailbox pages read no unrelated MIME and keep newest-first stable pagination', async () => {
  const { queue } = await fixture(); const ids = [];
  for (let n = 1; n <= 180; n++) {
    const result = await queue.enqueue(payload(n, { mailbox: n % 3 ? primary : support }), { archiveOnly: true });
    if (n % 3 === 0) ids.unshift(result.id);
  }
  queue.loadPayload = async () => { throw new Error('raw archive should not be read'); };
  const received = []; let cursor;
  do {
    const page = await queue.listArchive({ mailbox: support, limit: 20, cursor });
    assert.equal(page.total_view, 60); assert.equal(page.exact_view_total, true);
    received.push(...page.items.map((item) => item.id)); cursor = page.next_cursor;
  } while (cursor);
  assert.deepEqual(received, ids);
  const probe = await queue.performanceProbe({ parameters: { mailbox: support, limit: 20 } });
  assert.equal(probe.raw_payload_loads, 0); assert.equal(probe.list_calls, 1); assert(probe.get_calls <= 4);
  assert(probe.keys_read <= 85, JSON.stringify(probe));
});

test('thread lookup joins missing parents, deduplicates Message-ID and ignores unrelated mail', async () => {
  const { queue } = await fixture();
  const root = await queue.enqueue(payload(1), { archiveOnly: true });
  const reply = await queue.enqueue(payload(2, { references: '<mail-1@example.test> <missing@example.test>' }), { archiveOnly: true, archived: true });
  const sibling = await queue.enqueue(payload(3, { references: '<missing@example.test>' }), { archiveOnly: true });
  await queue.enqueue(payload(4, { messageId: '<mail-2@example.test>', references: '<mail-1@example.test>' }), { archiveOnly: true });
  for (let n = 5; n <= 65; n++) await queue.enqueue(payload(n), { archiveOnly: true });
  queue.loadPayload = async () => { throw new Error('no MIME during conversation lookup'); };
  const thread = await queue.views.conversation({ id: reply.id });
  assert.deepEqual(thread.items.map((item) => item.id), [sibling.id, reply.id, root.id]);
  assert.equal(thread.missing, 1);
  const probe = await queue.performanceProbe({ operation: 'conversation', parameters: { id: reply.id } });
  assert.equal(probe.items, 3); assert.equal(probe.raw_payload_loads, 0); assert(probe.keys_read < 35, JSON.stringify(probe));
});

test('archiving and restoring atomically move mailbox indexes and exact counts; read state stays live', async () => {
  const { queue } = await fixture();
  const first = await queue.enqueue(payload(1), { archiveOnly: true });
  const second = await queue.enqueue(payload(2), { archiveOnly: true });
  await queue.listArchive({ mailbox: support }); // populate caches before mutations
  await queue.applyAction({ ids: [second.id], action: 'archive' });
  await queue.applyAction({ ids: [second.id], action: 'archive' });
  let inbox = await queue.listArchive({ mailbox: support });
  assert.equal(inbox.total_view, 1); assert.equal(inbox.items[0].id, first.id);
  const archive = await queue.listArchive({ mailbox: support, view: 'archive' });
  assert.equal(archive.total_view, 1); assert.equal(archive.items[0].id, second.id);
  await queue.setReadStatus({ id: first.id, read: true });
  assert.equal((await queue.readArchiveMessage({ id: first.id })).read, true);
  await queue.applyAction({ ids: [second.id], action: 'unarchive' });
  inbox = await queue.listArchive({ mailbox: support }); assert.equal(inbox.total_view, 2);
});

test('resumable backfill preserves original bytes and state across restarts and concurrent new mail', async () => {
  const storage = new Storage(), state = { storage, blockConcurrencyWhile: (fn) => fn() }, env = { ...bindings, CMAIL_FAST_READS: '1' };
  const legacy = new LegacyQueue(state, env);
  const original = [];
  for (let n = 1; n <= 5; n++) original.push(await legacy.enqueue(payload(n)));
  await legacy.applyAction({ ids: [original[1].id], action: 'archive' });
  await legacy.setReadStatus({ id: original[2].id, read: true });
  const queue = new MailQueue(state, env); await queue.indexReady;
  await assert.rejects(queue.views.list({ mailbox: support }), /mail_index_warming/);
  assert.equal((await queue.views.backfill({ limit: 2 })).ready, false);
  await queue.enqueue(payload(6), { archiveOnly: true });
  const restarted = new MailQueue(state, env); await restarted.indexReady;
  while (!(await restarted.views.backfill({ limit: 2 })).ready) {}
  assert.equal((await restarted.views.backfill({ limit: 2 })).imported, 0);
  assert.equal((await restarted.listArchive({ mailbox: support })).total_view, 5);
  const archived = await restarted.listArchive({ mailbox: support, view: 'archive' }); assert.equal(archived.items[0].id, original[1].id);
  assert.equal((await restarted.readArchiveMessage({ id: original[2].id })).read, true);
  assert.equal((await restarted.readArchiveMessage({ id: original[0].id, raw: true })).content_base64, payload(1).raw_base64);
  assert.equal((await restarted.status()).pending_count, 5);
  assert(!JSON.stringify([...storage.data]).includes('Body 1'));
  // A temporary old-code rollback can append original mail without v2 views.
  await legacy.enqueue(payload(7));
  for (let run = 0; run < 10 && !(await restarted.views.readiness()).ready; run++) await restarted.views.backfill({ limit: 2 });
  assert.equal((await restarted.views.readiness()).ready, true);
});

test('date bounds, body search and equal-date cursor boundaries preserve matching messages', async () => {
  const { queue } = await fixture();
  const date = '2026-10-18T00:00:00Z';
  const first = await queue.enqueue(payload(1, { date, body: 'Unique body needle' }), { archiveOnly: true });
  const second = await queue.enqueue(payload(2, { date }), { archiveOnly: true });
  await queue.enqueue(payload(3, { date: '2026-10-19T00:00:00Z' }), { archiveOnly: true });
  const page = await queue.listArchive({ after: '2026-10-18', before: '2026-10-18', limit: 1 });
  assert.equal(page.items[0].id, second.id);
  const next = await queue.listArchive({ after: '2026-10-18', before: '2026-10-18', cursor: page.next_cursor, limit: 1 });
  assert.equal(next.items[0].id, first.id); assert.equal(next.next_cursor, null);
  assert.equal((await queue.listArchive({ query: 'body needle' })).items[0].id, first.id);
});

test('overlapping cold requests preserve results and exact bounded cache accounting', async () => {
  const { queue } = await fixture();
  const first = await queue.enqueue(payload(1, { body: 'a'.repeat(800000) }), { archiveOnly: true });
  const second = await queue.enqueue(payload(2, { body: 'b'.repeat(800000) }), { archiveOnly: true });
  const ids = [first.id, second.id];
  const results = await Promise.all(Array.from({ length: 4 }, () => queue.views.summaries(ids)));
  for (const result of results) assert.deepEqual(result.map((item) => item.id), ids);
  assert.equal(queue.views.summaryBytes, [...queue.views.summaryCache.values()].reduce((sum, value) => sum + JSON.stringify(value).length * 2, 0));
  const bodies = await Promise.all(Array.from({ length: 4 }, () => queue.views.body(first.id)));
  assert(bodies.every((body) => body.text.trim() === 'a'.repeat(800000)));
  await queue.views.body(second.id);
  assert.equal(queue.views.cacheBytes, [...queue.views.bodyCache.values()].reduce((sum, item) => sum + item.size, 0));
  assert(queue.views.cacheBytes <= 4 * 1024 * 1024);
});
