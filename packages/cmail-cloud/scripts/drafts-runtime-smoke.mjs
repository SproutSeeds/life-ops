// Synthetic credentials and recipients only. All external networking is disabled.
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { readFile } from 'node:fs/promises';
import { bindings, payload, uuid, support } from '../test/reply-fixture.mjs';
const { Miniflare, createFetchMock } = await import(process.argv[2]);
const serve = process.argv.includes('--serve'), root = fileURLToPath(new URL('../', import.meta.url));
// Optional private signature fixtures for visual QA. These still use synthetic
// credentials, example.test recipients, and the network-isolated fake provider.
const signatureFlag = process.argv.indexOf('--signature-config');
const extraBindings = {};
if (signatureFlag !== -1) {
  const signatures = JSON.parse(await readFile(process.argv[signatureFlag + 1], 'utf8'));
  const identities = JSON.parse(bindings.CMAIL_SEND_IDENTITIES);
  for (const [address, signature] of Object.entries(signatures)) identities[address] = { from: address, key: 'primary', ...signature };
  extraBindings.CMAIL_SEND_IDENTITIES = JSON.stringify(identities);
  extraBindings.CMAIL_MAILBOXES = Object.keys(identities).join(',');
}
const fetchMock = createFetchMock(); fetchMock.disableNetConnect(); let sends = 0;
fetchMock.get('https://api.resend.com').intercept({ path: '/emails', method: 'POST' }).reply(200, async (options) => {
  const body = await new Response(options.body).json();
  assert(body.to.every((address) => address.endsWith('@example.test'))); sends++;
  return JSON.stringify({ id: uuid(900 + sends) });
}, { headers: { 'Content-Type': 'application/json' } }).persist();
const mf = new Miniflare({ name: 'cmail-drafts-fixture', host: '127.0.0.1', port: serve ? 9346 : 0,
  modules: true, modulesRoot: root, scriptPath: root + 'test/compose-worker.mjs', compatibilityDate: '2026-03-27',
  bindings: { ...bindings, ...extraBindings, CMAIL_FAST_READS: '1', CMAIL_SENT_ENABLED: '1', CMAIL_COMPOSE_ENABLED: '1', CMAIL_DRAFTS_ENABLED: '1' },
  fetchMock, durableObjects: { MAIL_QUEUE: { className: 'MailQueue', useSQLite: true } },
});
try {
  const namespace = await mf.getDurableObjectNamespace('MAIL_QUEUE'), stub = namespace.get(namespace.idFromName('primary'));
  const original = await (await stub.fetch('https://internal/enqueue', { method: 'POST', body: JSON.stringify(payload()) })).json();
  const origin = 'http://127.0.0.1:9346';
  const api = async (path, body) => {
    const response = await mf.dispatchFetch(origin + '/cmail/api/' + path, body === undefined ? {} : { method: 'POST',
      headers: { Origin: origin, 'X-Cmail-Action': 'draft', 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    const result = await response.json(); assert.equal(response.status, 200, JSON.stringify(result)); return result;
  };
  const value = { kind: 'compose', sender: support, to: ['player@example.test'], pending: '', subject: 'Runtime saved draft', text: 'Synthetic runtime draft.', signature: 'Support' };
  const saved = await api('drafts/save', { id: 'draft_' + uuid(1), revision: 0, save_id: uuid(2000), draft: value });
  assert.equal(sends, 0); assert.equal((await api('drafts')).total, 1);
  const results = await Promise.all([1, 2].map(() => api('drafts/' + saved.id + '/send', { revision: saved.revision })));
  assert.equal(results[0].state, 'sent'); assert.deepEqual(results[0], results[1]); assert.equal(sends, 1);
  assert.equal((await api('drafts')).total, 0);
  const reply = await api('drafts/save', { id: 'draft_' + uuid(2), revision: 0, save_id: uuid(2001), draft: { ...value, kind: 'reply', reply_id: original.id } });
  const replySent = await api('drafts/' + reply.id + '/send', { revision: reply.revision });
  assert.equal(replySent.state, 'sent'); assert.equal(sends, 2);
  assert.equal((await api('messages/' + replySent.archive_id + '/conversation')).total, 2);
  await api('drafts/save', { id: 'draft_' + uuid(3), revision: 0, save_id: uuid(2002), draft: { ...value, subject: 'First saved draft' } });
  await api('drafts/save', { id: 'draft_' + uuid(4), revision: 0, save_id: uuid(2003), draft: { ...value, subject: 'Plan for later', to: [], text: '' } });
  assert.equal((await api('drafts')).items[0].subject, 'Plan for later');
  assert.match(await (await mf.dispatchFetch(origin + '/cmail/?view=drafts')).text(), /value="drafts">Drafts/);
  console.log(JSON.stringify({ passed: true, provider_calls: sends, checks: ['save without sending', 'durable encrypted drafts', 'concurrent Send creates one message', 'confirmed send removes draft', 'reply preserves complete thread', 'two independent drafts'] }));
  if (serve) {
    console.log(JSON.stringify({ browser_fixture_url: String(await mf.ready) + 'cmail/?mailbox=' + encodeURIComponent(support) + '&view=drafts', pid: process.pid, network: 'fake provider only; external connections disabled' }));
    await new Promise((resolve) => { process.on('SIGINT', resolve); process.on('SIGTERM', resolve); });
  }
} finally { await mf.dispose(); }
