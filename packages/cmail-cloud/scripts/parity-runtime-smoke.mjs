// Actual CLI parser + signed HTTP + native Worker + browser routes. Synthetic
// mail only; all outbound network connections are disabled except a fake sender.
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { createHash } from 'node:crypto';
import { mkdtemp, writeFile, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { bindings, payload, uuid, primary } from '../test/reply-fixture.mjs';
import { ATTACHMENT_LIMITS } from '../src/attachments.mjs';
const { Miniflare, createFetchMock } = await import(process.argv[2]);
const root = fileURLToPath(new URL('../', import.meta.url)), repo = fileURLToPath(new URL('../../../', import.meta.url));
const temp = await mkdtemp(tmpdir() + '/cmail-cli-parity-');
const digest = (data) => createHash('sha256').update(data).digest('hex');
const bytes = Buffer.alloc(process.argv.includes('--large') ? ATTACHMENT_LIMITS.max_bytes : 1024 * 1024);
for (let n = 0; n < bytes.length; n++) bytes[n] = n % 251;
const sha256 = digest(bytes), filePath = temp + '/Résumé ☕.zip';
await writeFile(filePath, bytes);
let sends = 0;
const fetchMock = createFetchMock(); fetchMock.disableNetConnect();
fetchMock.get('https://api.resend.com').intercept({ path: '/emails', method: 'POST' }).reply(200, async (options) => {
  const data = await new Response(options.body).json();
  assert.deepEqual(data.to, ['files@example.test']);
  assert.equal(digest(Buffer.from(data.attachments[0].content, 'base64')), sha256);
  sends++; return JSON.stringify({ id: uuid(900 + sends) });
}, { headers: { 'Content-Type': 'application/json' } }).persist();
const mf = new Miniflare({ name: 'cmail-cli-parity-fixture', host: '127.0.0.1', port: 0,
  modules: true, modulesRoot: root, scriptPath: root + 'test/compose-worker.mjs', compatibilityDate: '2026-03-27',
  bindings: { ...bindings, LIFE_OPS_MAIL_INGEST_SECRET: 'fixture-agent-secret', CMAIL_AGENT_ENABLED: '1',
    CMAIL_FAST_READS: '1', CMAIL_SENT_ENABLED: '1', CMAIL_COMPOSE_ENABLED: '1', CMAIL_DRAFTS_ENABLED: '1' },
  fetchMock, durableObjects: { MAIL_QUEUE: { className: 'MailQueue', useSQLite: true } },
});
try {
  const origin = String(await mf.ready).replace(/\/$/, '');
  const cli = async (...args) => {
    // The production client's destination stays pinned. Only this isolated test
    // replaces its transport, using the real timestamp/HMAC request helper.
    const code = `import sys\nfrom life_ops import cmail_cloud, cloudflare_email\nassert sys.argv[1].startswith('http://127.0.0.1:')\ndef request(action, parameters=None):\n return cloudflare_email._cloudflare_worker_request_json(worker_public_url=sys.argv[1], secret='fixture-agent-secret', path='/api/mail/agent', payload={'action': action, 'parameters': parameters or {}})\ncmail_cloud.request = request\nraise SystemExit(cmail_cloud.main(sys.argv[2:]))`;
    const result = await promisify(execFile)(repo + '.venv/bin/python', ['-c', code, origin, ...args], {
      cwd: repo, env: { ...process.env, PYTHONPATH: repo + 'src' }, maxBuffer: 1024 * 1024,
    });
    return JSON.parse(result.stdout);
  };
  const web = async (path, body) => {
    const response = await mf.dispatchFetch(origin + '/cmail/api/' + path, body === undefined ? {} : {
      method: 'POST', headers: { Origin: origin, 'X-Cmail-Action': 'draft', 'Content-Type': 'application/json' }, body: JSON.stringify(body),
    });
    const data = await response.json(); assert.equal(response.status, 200, JSON.stringify(data)); return data;
  };
  const ns = await mf.getDurableObjectNamespace('MAIL_QUEUE'), stub = ns.get(ns.idFromName('primary'));
  const seed = await (await stub.fetch('https://internal/enqueue', { method: 'POST', body: JSON.stringify(payload({ mailbox: primary })) })).json();
  const saveArgs = ['draft-save', '--save-id', uuid(1), '--to', 'files@example.test', '--subject', 'CLI parity maximum', '--text', 'Synthetic CLI draft.', '--attach', filePath];
  let saved = await cli(...saveArgs);
  assert.equal(sends, 0); assert.equal(saved.draft.attachments[0].sha256, sha256);
  assert.deepEqual(await cli(...saveArgs), saved);
  assert.deepEqual((await web('drafts/' + saved.id)).draft, saved.draft);
  const webDownload = await mf.dispatchFetch(origin + '/cmail/api/drafts/attachments/' + saved.draft.attachments[0].id);
  assert.equal(digest(Buffer.from(await webDownload.arrayBuffer())), sha256);
  await cli('draft', saved.id, '--attachment', '0', '--output', temp + '/cli-download.zip');
  assert.equal(digest(await readFile(temp + '/cli-download.zip')), sha256);
  const copy = await cli('draft-copy', saved.id, '--revision', '1', '--save-id', uuid(2));
  assert.deepEqual(copy.draft.attachments, saved.draft.attachments); assert.equal(sends, 0);
  const sent = await cli('draft-send', copy.id, '--revision', '1', '--confirm-send');
  assert.equal(sent.state, 'sent'); assert.equal(sends, 1);
  assert.deepEqual(await cli('draft-send', copy.id, '--revision', '1', '--confirm-send'), sent); assert.equal(sends, 1);
  await cli('message', sent.archive_id, '--attachment', '0', '--output', temp + '/sent.zip');
  assert.equal(digest(await readFile(temp + '/sent.zip')), sha256);
  const small = Buffer.from('Synthetic website-created attachment\n');
  const upload = await mf.dispatchFetch(origin + '/cmail/api/drafts/attachments/' + uuid(3), { method: 'POST', body: small,
    headers: { Origin: origin, 'X-Cmail-Action': 'draft', 'X-Cmail-Filename': 'website.txt', 'X-Cmail-Content-Type': 'text/plain' } });
  assert.equal(upload.status, 200);
  const websiteFile = (await upload.json()).attachment;
  saved = await web('drafts/save', { id: saved.id, revision: 1, save_id: uuid(4), draft: { ...saved.draft, text: 'Website edited body', attachments: [websiteFile] } });
  assert.deepEqual((await cli('draft', saved.id)).draft, saved.draft);
  await cli('draft', saved.id, '--attachment', websiteFile.id, '--output', temp + '/website.txt');
  assert.deepEqual(await readFile(temp + '/website.txt'), small);
  await assert.rejects(cli('draft-save', '--id', saved.id, '--revision', '1', '--save-id', uuid(5), '--subject', 'Stale overwrite'), /draft_conflict/);
  saved = await cli('draft-save', '--id', saved.id, '--revision', '2', '--save-id', uuid(6), '--remove-attachment', websiteFile.id);
  assert.equal(saved.draft.text, 'Website edited body'); assert.deepEqual((await web('drafts/' + saved.id)).draft.attachments, []);
  await cli('draft-discard', saved.id, '--revision', String(saved.revision));
  await writeFile(temp + '/reply.png', Buffer.from([0, 1, 255, 128]));
  const reply = await cli('draft-save', '--save-id', uuid(7), '--reply-to-message', seed.id, '--text', 'Synthetic reply', '--attach', temp + '/reply.png');
  assert.equal(reply.draft.kind, 'reply'); assert.equal(reply.draft.attachments.length, 1);
  assert.deepEqual((await web('drafts/' + reply.id)).draft, reply.draft);
  await cli('draft-discard', reply.id, '--revision', String(reply.revision));
  assert.equal((await web('drafts')).total, 0);
  console.log(JSON.stringify({ passed: true, bytes: bytes.length, sha256, real_sends: 0, fake_provider_calls: sends,
    checks: ['actual CLI signed upload', 'exact retry', 'browser read/download', 'CLI verified download', 'copy and frozen send', 'Sent bytes', 'website upload to CLI', 'stale revision rejection', 'CLI removal', 'reply attachment', 'test draft cleanup'] }));
} finally { await mf.dispose(); await rm(temp, { recursive: true, force: true }); }
