// Network-isolated, synthetic attachment regression and optional browser fixture.
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import { bindings, payload, uuid, support } from '../test/reply-fixture.mjs';
import { ATTACHMENT_LIMITS } from '../src/attachments.mjs';
const { Miniflare, createFetchMock } = await import(process.argv[2]);
const serve = process.argv.includes('--serve'), large = process.argv.includes('--large');
const root = fileURLToPath(new URL('../', import.meta.url));
const digest = (bytes) => createHash('sha256').update(bytes).digest('hex');
let sends = 0;
const fetchMock = createFetchMock(); fetchMock.disableNetConnect();
fetchMock.get('https://api.resend.com').intercept({ path: '/emails', method: 'POST' }).reply(200, async (options) => {
  const data = await new Response(options.body).json();
  assert(data.to.every((address) => address.endsWith('@example.test'))); sends++;
  if (data.subject === 'Attachment runtime maximum') {
    assert.equal(data.attachments.length, 1);
    assert.equal(digest(Buffer.from(data.attachments[0].content, 'base64')), expectedHash);
  }
  return JSON.stringify({ id: uuid(900 + sends) });
}, { headers: { 'Content-Type': 'application/json' } }).persist();
const mf = new Miniflare({ name: 'cmail-attachments-fixture', host: '127.0.0.1', port: serve ? 9346 : 0,
  modules: true, modulesRoot: root, scriptPath: root + 'test/compose-worker.mjs', compatibilityDate: '2026-03-27',
  bindings: { ...bindings, CMAIL_FAST_READS: '1', CMAIL_SENT_ENABLED: '1', CMAIL_COMPOSE_ENABLED: '1', CMAIL_DRAFTS_ENABLED: '1' },
  fetchMock, durableObjects: { MAIL_QUEUE: { className: 'MailQueue', useSQLite: true } },
});
let expectedHash;
try {
  const namespace = await mf.getDurableObjectNamespace('MAIL_QUEUE'), stub = namespace.get(namespace.idFromName('primary'));
  await stub.fetch('https://internal/enqueue', { method: 'POST', body: JSON.stringify(payload()) });
  const origin = String(await mf.ready).replace(/\/$/, '');
  const api = async (path, body) => {
    const response = await mf.dispatchFetch(origin + '/cmail/api/' + path, body === undefined ? {} : { method: 'POST', headers: { Origin: origin, 'X-Cmail-Action': 'draft', 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    const data = await response.json(); assert.equal(response.status, 200, JSON.stringify(data)); return data;
  };
  const bytes = Buffer.alloc(large ? ATTACHMENT_LIMITS.max_bytes : 1024 * 1024);
  for (let n = 0; n < bytes.length; n++) bytes[n] = n % 251;
  expectedHash = digest(bytes);
  const uploaded = await mf.dispatchFetch(origin + '/cmail/api/drafts/attachments/' + uuid(1), { method: 'POST', headers: { Origin: origin, 'X-Cmail-Action': 'draft', 'X-Cmail-Filename': encodeURIComponent('Résumé ☕.zip'), 'X-Cmail-Content-Type': 'application/zip' }, body: bytes });
  assert.equal(uploaded.status, 200, await uploaded.clone().text());
  const file = (await uploaded.json()).attachment; assert.equal(file.sha256, expectedHash);
  const download = await mf.dispatchFetch(origin + '/cmail/api/drafts/attachments/' + file.id);
  assert.equal(digest(Buffer.from(await download.arrayBuffer())), expectedHash);
  const value = { kind: 'compose', sender: support, to: ['files@example.test'], pending: '', subject: 'Attachment runtime maximum', text: 'Files attached.', signature: 'Support', attachments: [file] };
  const saved = await api('drafts/save', { id: 'draft_' + uuid(10), revision: 0, save_id: uuid(11), draft: value });
  assert.equal(sends, 0);
  if (large) {
    const extraUpload = await mf.dispatchFetch(origin + '/cmail/api/drafts/attachments/' + uuid(2), { method: 'POST', headers: { Origin: origin, 'X-Cmail-Action': 'draft', 'X-Cmail-Filename': 'extra.txt' }, body: 'x' });
    const extra = (await extraUpload.json()).attachment;
    const overLimit = await mf.dispatchFetch(origin + '/cmail/api/drafts/save', { method: 'POST', headers: { Origin: origin, 'X-Cmail-Action': 'draft', 'Content-Type': 'application/json' },
      body: JSON.stringify({ id: saved.id, revision: saved.revision, save_id: uuid(12), draft: { ...value, attachments: [file, extra] } }) });
    assert.equal(overLimit.status, 400); assert.equal((await overLimit.json()).error, 'attachments_too_large');
  }
  const reopened = await api('drafts/' + saved.id); assert.deepEqual(reopened.draft.attachments, [file]);
  const sent = await api('drafts/' + saved.id + '/send', { revision: saved.revision });
  assert.equal(sent.state, 'sent', JSON.stringify(sent)); assert.equal(sends, 1);
  const archived = await (await stub.fetch('https://internal/archive/message', { method: 'POST', body: JSON.stringify({ id: sent.archive_id, attachment: 0 }) })).json();
  assert.equal(archived.filename, file.filename);
  assert.equal(digest(Buffer.from(archived.content_base64, 'base64')), expectedHash);
  assert.equal((await api('drafts')).total, 0);
  console.log(JSON.stringify({ passed: true, bytes: bytes.length, sha256: expectedHash, provider_calls: sends,
    checks: ['native runtime upload/download', 'encrypted draft persistence', 'exact provider attachment bytes', 'Sent MIME attachment bytes and Unicode filename', 'no send during upload/save'] }));
  if (serve) {
    console.log(JSON.stringify({ browser_fixture_url: origin + '/cmail/?view=drafts', pid: process.pid, network: 'fake provider only; external connections disabled' }));
    await new Promise((resolve) => { process.on('SIGINT', resolve); process.on('SIGTERM', resolve); });
  }
} finally { await mf.dispose(); }
