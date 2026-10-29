import test from 'node:test';
import assert from 'node:assert/strict';
import { fixture, uuid, support } from './reply-fixture.mjs';
import { MailQueue, createWorker, withResend } from '../src/index.mjs';
import { withDrafts } from '../src/draft-http.mjs';
import { withReplies } from '../src/reply-http.mjs';
import { attachmentMetadata, ATTACHMENT_LIMITS } from '../src/attachments.mjs';
import { PostalMime } from '../src/production-base.mjs';

const value = (attachments = []) => ({ kind: 'compose', sender: support, to: ['files@example.test'], pending: '', subject: 'Files', text: 'Attached.', signature: 'Support', attachments });
const upload = (queue, n, filename = 'sample.pdf', bytes = new Uint8Array([0, 255, 42, 10])) => queue.attachments.upload(uuid(n), filename, '', bytes);
const save = (queue, attachments, extra = {}) => queue.drafts.save({ id: 'draft_' + uuid(100), revision: 0, save_id: uuid(101), draft: value(attachments), ...extra });

test('Attachments survive encrypted storage, draft reopen, older clients, removal, and conflict copies', async () => {
  const { queue, storage, env } = await fixture();
  const bytes = Uint8Array.from({ length: 110000 }, (_, n) => n % 256);
  const file = await upload(queue, 1, 'Résumé ☕.pdf', bytes);
  assert.equal(await queue.attachments.content(file), Buffer.from(bytes).toString('base64'));
  assert(!JSON.stringify([...storage.data]).includes('Résumé'));
  assert.deepEqual(await upload(queue, 1, 'Résumé ☕.pdf', bytes), file);
  await assert.rejects(upload(queue, 1, 'different.pdf', bytes), /attachment_changed/);
  let saved = await save(queue, [{ id: file.id, filename: 'spoof.exe', size: 0 }]);
  assert.deepEqual(saved.draft.attachments, [file]);
  const restarted = new MailQueue({ storage, blockConcurrencyWhile: (fn) => fn() }, env); await restarted.indexReady;
  assert.deepEqual((await restarted.drafts.get(saved.id)).draft.attachments, [file]);
  assert.equal((await restarted.drafts.list()).items[0].attachment_count, 1);
  const older = value(); delete older.attachments;
  saved = await save(queue, [], { revision: 1, save_id: uuid(102), draft: older });
  assert.deepEqual(saved.draft.attachments, [file]);
  const copy = await save(queue, [file], { id: 'draft_' + uuid(200), save_id: uuid(201) });
  await assert.rejects(save(queue, [], { revision: 1, save_id: uuid(103) }), /draft_conflict/);
  saved = await save(queue, [], { revision: 2, save_id: uuid(104) });
  assert.deepEqual(saved.draft.attachments, []);
  assert.deepEqual((await queue.drafts.get(copy.id)).draft.attachments, [file]);
  assert.equal(await queue.attachments.content(file), Buffer.from(bytes).toString('base64'));
  assert(![...storage.data.keys()].some((key) => key.startsWith('cmail-outbox:')));
});

test('Standard and unknown extensions are accepted; unsafe names, empty files, limits and provider blocks are explicit', async () => {
  for (const name of ['file.pdf', 'file.docx', 'file.xlsx', 'file.pptx', 'image.heic', 'image.svg', 'file.zip', 'file.7z', 'file.mp3', 'file.mp4', 'file.csv', 'file.json', 'file.custom', 'README']) {
    assert.equal(attachmentMetadata(name, '', 1).filename, name);
  }
  for (const name of ['file.EXE', 'script.js', 'file.exe. ', 'file.ps1', 'file.vbs']) assert.throws(() => attachmentMetadata(name, '', 1), /attachment_type_blocked/);
  for (const name of ['../a.pdf', 'a\r\nBcc: bad', 'a\\b', '']) assert.throws(() => attachmentMetadata(name, '', 1), /invalid_attachment_name/);
  assert.equal(attachmentMetadata('a.bin', 'text/html\r\nX: injected', 1).content_type, 'application/octet-stream');
  assert.throws(() => attachmentMetadata('empty.txt', '', 0), /attachment_empty/);
  assert.throws(() => attachmentMetadata('big.zip', '', ATTACHMENT_LIMITS.max_bytes + 1), /attachments_too_large/);
  const { queue } = await fixture();
  await assert.rejects(queue.attachments.validate(Array.from({ length: 51 }, () => ({}))), /too_many_attachments/);
  await assert.rejects(save(queue, [{ id: uuid(999) }]), /attachment_not_found/);
  const file = await upload(queue, 1);
  await assert.rejects(save(queue, [file, file]), /invalid_attachment/);
});

test('Compose and reply send exact bytes, retain Unicode MIME filenames, and safely recover an uncertain send', async () => {
  for (const reply of [false, true]) {
    const { queue, id, env } = await fixture(); env.CMAIL_FAST_READS = env.CMAIL_SENT_ENABLED = '1';
    const files = [await upload(queue, 1, 'Résumé ☕.pdf'), await upload(queue, 2, 'data.zip')];
    const draft = { ...value(files), text: '', ...(reply ? { kind: 'reply', reply_id: id } : {}) };
    let saved = await save(queue, files, { draft });
    let body, headers;
    saved = await queue.drafts.send({ id: saved.id, revision: saved.revision }, { fetcher: async (_, options) => { body = options.body; headers = options.headers; throw new Error('timeout'); } });
    assert.equal(saved.state, 'sending');
    const sent = await queue.drafts.send({ id: saved.id, revision: saved.revision }, { fetcher: async (_, options) => {
      assert.equal(options.body, body); assert.deepEqual(options.headers, headers);
      const request = JSON.parse(body);
      assert.deepEqual(request.attachments.map((f) => f.filename), files.map((f) => f.filename));
      for (const file of request.attachments) assert.deepEqual([...Buffer.from(file.content, 'base64')], [0, 255, 42, 10]);
      if (reply) assert.equal(request.headers['In-Reply-To'], '<original@example.test>');
      return Response.json({ id: uuid(900) });
    } });
    assert.equal(sent.state, 'sent');
    const archived = await queue.loadPayload(await queue.state.storage.get('mail:' + sent.archive_id));
    const parsed = await PostalMime.parse(Buffer.from(archived.raw_base64, 'base64'));
    assert.deepEqual(parsed.attachments.map((file) => file.filename), files.map((f) => f.filename));
    for (const file of parsed.attachments) assert.deepEqual([...new Uint8Array(file.content)], [0, 255, 42, 10]);
    assert.equal((await queue.listArchive({ view: 'sent' })).total_view, 1);
    assert.equal((await queue.readArchiveMessage({ id: sent.archive_id })).attachments.length, 2);
  }
});

test('Upload/download require Access; uploads also require same origin and the draft action header', async () => {
  const { queue, env } = await fixture(); env.CMAIL_DRAFTS_ENABLED = '1';
  const allowed = async () => ({ ok: true }), base = withReplies(withResend(createWorker(allowed)), allowed);
  const worker = withDrafts(base, allowed), denied = withDrafts(base, async () => ({ ok: false, status: 401, error: 'unauthorized' }));
  const url = 'https://cmail.frg.earth/cmail/api/drafts/attachments/' + uuid(1);
  const headers = { Origin: 'https://cmail.frg.earth', 'X-Cmail-Action': 'draft', 'X-Cmail-Filename': encodeURIComponent('image.svg'), 'X-Cmail-Content-Type': 'image/svg+xml' };
  assert.equal((await denied.fetch(new Request(url), env)).status, 401);
  for (const h of [{}, { ...headers, Origin: 'https://evil.test' }, { ...headers, 'Sec-Fetch-Site': 'cross-site' }]) {
    assert.equal((await worker.fetch(new Request(url, { method: 'POST', headers: h, body: 'abc' }), env)).status, 403);
  }
  const response = await worker.fetch(new Request(url, { method: 'POST', headers, body: '<svg>fixture</svg>' }), env);
  assert.equal(response.status, 200);
  const info = (await response.json()).attachment;
  const download = await worker.fetch(new Request(url), env);
  assert.equal(await download.text(), '<svg>fixture</svg>');
  assert.equal(download.headers.get('Content-Type'), 'application/octet-stream');
  assert.match(download.headers.get('Content-Disposition'), /^attachment;/);
  assert.match(download.headers.get('Content-Security-Policy'), /sandbox/);
  assert.equal((await queue.drafts.list()).total, 0);
  assert.equal(info.filename, 'image.svg');
});

test('Cleanup removes only expired unreferenced uploads and preserves saved copies and outbox retries', async () => {
  const { queue } = await fixture();
  const staged = await upload(queue, 1), kept = await upload(queue, 2);
  const saved = await save(queue, [kept]);
  await queue.attachments.cleanup(Date.now() + 8 * 24 * 3600 * 1000);
  await assert.rejects(queue.attachments.metadata(staged.id), /attachment_not_found/);
  assert.deepEqual(await queue.attachments.metadata(kept.id), kept);
  await save(queue, [kept], { id: 'draft_' + uuid(200), save_id: uuid(201) });
  await queue.drafts.discard({ id: saved.id, revision: saved.revision });
  await queue.attachments.cleanup(Date.now() + 8 * 24 * 3600 * 1000);
  assert.deepEqual(await queue.attachments.metadata(kept.id), kept);
  const sent = await queue.drafts.send({ id: 'draft_' + uuid(200), revision: 1 }, { fetcher: async () => Response.json({ id: uuid(900) }) });
  assert.equal(sent.state, 'sent');
  await queue.attachments.cleanup(Date.now() + 8 * 24 * 3600 * 1000);
  assert.deepEqual(await queue.attachments.metadata(kept.id), kept);
});

test('Attachments wrap branded text/HTML alternatives without changing either body', async () => {
  const { queue, env } = await fixture();
  const identities = JSON.parse(env.CMAIL_SEND_IDENTITIES);
  identities[support].signature = 'Support';
  identities[support].signature_logo = { url: 'https://bindmysoul.com/logo.png', alt: 'Support' };
  env.CMAIL_SEND_IDENTITIES = JSON.stringify(identities);
  const file = await upload(queue, 1, '🧪'.repeat(50) + '.pdf');
  const saved = await save(queue, [file], { draft: { ...value([file]), text: 'Message body.\n\nSupport' } });
  // Freeze the alternative explicitly to exercise MIME nesting independently
  // of identity-specific logo configuration rules.
  const record = await queue.state.storage.get('cmail-drafts-v1:record:' + saved.id);
  const content = await queue.drafts.body(record); content.html = '<p>Message body.</p><p>Support</p>';
  await queue.drafts.writeBody(queue.state.storage, record, content);
  await queue.state.storage.put('cmail-drafts-v1:record:' + saved.id, record);
  const sent = await queue.drafts.send({ id: saved.id, revision: 1 }, { fetcher: async (_, options) => {
    assert.equal(JSON.parse(options.body).html, content.html); return Response.json({ id: uuid(901) });
  } });
  assert.equal(sent.state, 'sent');
  const archive = await queue.loadPayload(await queue.state.storage.get('mail:' + sent.archive_id));
  const parsed = await PostalMime.parse(Buffer.from(archive.raw_base64, 'base64'));
  assert.equal(parsed.text.trim(), content.text); assert.equal(parsed.html.trim(), content.html);
  assert.equal(parsed.attachments[0].filename, file.filename);
});
