// Synthetic mail only; external networking is disabled and Resend is mocked.
import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { bindings, payload, uuid, primary } from '../test/reply-fixture.mjs';
const { Miniflare, createFetchMock } = await import(process.argv[2]);
const root = fileURLToPath(new URL('../', import.meta.url)), serve = process.argv.includes('--serve');
const secret = 'fixture-agent-secret', mock = createFetchMock(); mock.disableNetConnect(); let sends = 0;
mock.get('https://api.resend.com').intercept({ path: '/emails', method: 'POST' }).reply(200, async (options) => {
  const value = await new Response(options.body).json();
  assert.deepEqual(value.cc, ['copy@example.test']); assert.deepEqual(value.bcc, ['hidden@example.test']);
  assert.equal(value.headers.Bcc, undefined); assert.equal(value.headers['In-Reply-To'], '<original@example.test>');
  sends++; return JSON.stringify({ id: uuid(900 + sends) });
}, { headers: { 'Content-Type': 'application/json' } }).persist();
const mf = new Miniflare({ name: 'cmail-agent-fixture', host: '127.0.0.1', port: serve ? 9346 : 0,
  modules: true, modulesRoot: root, scriptPath: root + 'test/compose-worker.mjs', compatibilityDate: '2026-03-27',
  bindings: { ...bindings, LIFE_OPS_MAIL_INGEST_SECRET: secret, CMAIL_AGENT_ENABLED: '1',
    CMAIL_FAST_READS: '1', CMAIL_SENT_ENABLED: '1', CMAIL_COMPOSE_ENABLED: '1', CMAIL_DRAFTS_ENABLED: '1' },
  fetchMock: mock, durableObjects: { MAIL_QUEUE: { className: 'MailQueue', useSQLite: true } },
});
try {
  const origin = 'http://127.0.0.1:9346';
  const api = async (action, parameters = {}, expected = 200) => {
    const body = JSON.stringify({ action, parameters }), timestamp = new Date().toISOString().replace(/\.\d{3}Z$/, 'Z');
    const signature = 'sha256=' + createHmac('sha256', secret).update(timestamp + '.' + body).digest('hex');
    const response = await mf.dispatchFetch(origin + '/api/mail/agent', { method: 'POST', body,
      headers: { 'X-Life-Ops-Timestamp': timestamp, 'X-Life-Ops-Signature': signature } });
    const data = await response.json(); assert.equal(response.status, expected, JSON.stringify(data)); return data;
  };
  const ns = await mf.getDurableObjectNamespace('MAIL_QUEUE'), stub = ns.get(ns.idFromName('primary'));
  await stub.fetch('https://internal/enqueue', { method: 'POST', body: JSON.stringify(payload({ mailbox: primary })) });
  const value = { kind: 'compose', sender: primary, to: ['person@example.test'], pending: '', subject: 'Migrated draft with CC',
    cc: ['copy@example.test'], bcc: ['hidden@example.test'], text: 'Synthetic migration body.\n\nCody', signature: '',
    in_reply_to: '<original@example.test>', references: ['<original@example.test>'] };
  let saved = await api('draft-save', { id: 'draft_' + uuid(1), revision: 0, save_id: uuid(2), draft: value });
  const web = await (await mf.dispatchFetch(origin + '/cmail/api/drafts/' + saved.id)).json();
  assert.deepEqual(web.draft, value); assert.equal(sends, 0);
  const { cc, bcc, in_reply_to, references, ...oldTabValue } = value;
  saved = await (await mf.dispatchFetch(origin + '/cmail/api/drafts/save', { method: 'POST',
    headers: { Origin: origin, 'X-Cmail-Action': 'draft', 'Content-Type': 'application/json' },
    body: JSON.stringify({ id: saved.id, revision: 1, save_id: uuid(20), draft: oldTabValue }),
  })).json();
  assert.equal(saved.revision, 2); assert.deepEqual(saved.draft, value);
  await api('draft-send', { id: saved.id, revision: 2 }, 400); assert.equal(sends, 0);
  const results = await Promise.all([1, 2].map(() => api('draft-send', { id: saved.id, revision: 2, confirm_send: true })));
  assert.equal(results[0].state, 'sent'); assert.deepEqual(results[0], results[1]); assert.equal(sends, 1);
  const changes = await api('changes', { limit: 1 });
  const next = await api('changes', { cursor: changes.cursor, limit: 1 });
  assert(changes.has_more); assert.equal(next.items[0].payload.direction, 'outbound'); assert(!next.has_more);
  const display = await api('draft-save', { id: 'draft_' + uuid(3), revision: 0, save_id: uuid(4), draft: value });
  console.log(JSON.stringify({ passed: true, fake_provider_calls: sends, checks: ['agent/browser share one store', 'exact CC/BCC/thread preservation', 'explicit Send only', 'concurrent retry deduplication', 'native projection cursor'] }));
  if (serve) {
    console.log(JSON.stringify({ browser_fixture_url: String(await mf.ready) + 'cmail/?view=drafts&draft=' + display.id, pid: process.pid }));
    await new Promise((resolve) => { process.on('SIGINT', resolve); process.on('SIGTERM', resolve); });
  }
} finally { await mf.dispose(); }
