// Native workerd/SQLite and a fake provider; external connections are disabled.
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { bindings, payload, uuid, support, primary } from '../test/reply-fixture.mjs';
const { Miniflare, createFetchMock } = await import(process.argv[2]);
const serve = process.argv.includes('--serve');
const root = fileURLToPath(new URL('../', import.meta.url));
const fetchMock = createFetchMock(); fetchMock.disableNetConnect();
const calls = [];
fetchMock.get('https://api.resend.com').intercept({ path: '/emails', method: 'POST' }).reply(200, async (options) => {
  const body = await new Response(options.body).json();
  assert([`Bind My Soul Support <${support}>`, `Cody <${primary}>`].includes(body.from));
  assert(body.to.every((address) => address.endsWith('@example.test')));
  assert.equal(body.headers['In-Reply-To'], undefined);
  calls.push({ headers: options.headers, body });
  return JSON.stringify({ id: uuid(900 + calls.length) });
}, { headers: { 'Content-Type': 'application/json' } }).persist();
const mf = new Miniflare({ name: 'cmail-compose-fixture', host: '127.0.0.1', port: serve ? 9346 : 0,
  modules: true, modulesRoot: root, scriptPath: root + 'test/compose-worker.mjs', compatibilityDate: '2026-03-27',
  bindings: { ...bindings, CMAIL_FAST_READS: '1', CMAIL_SENT_ENABLED: '1', CMAIL_COMPOSE_ENABLED: '1' }, fetchMock,
  durableObjects: { MAIL_QUEUE: { className: 'MailQueue', useSQLite: true } },
});
try {
  const namespace = await mf.getDurableObjectNamespace('MAIL_QUEUE');
  const stub = namespace.get(namespace.idFromName('primary'));
  const call = (path, input) => stub.fetch('https://internal' + path, { method: 'POST', body: JSON.stringify(input) });
  for (const [index, from] of ['Jane Doe <jane@example.test>', 'Jason Doe <jason@example.test>', 'Zoë Edwards <zoe@example.test>'].entries()) {
    const item = payload({ from, replyTo: '', subject: 'Synthetic message ' + index, references: '' });
    item.raw_base64 = Buffer.from(Buffer.from(item.raw_base64, 'base64').toString().replace('<original@example.test>', `<fixture${index}@example.test>`)).toString('base64');
    await call('/enqueue', item);
  }
  const origin = 'http://127.0.0.1:9346';
  const body = { request_id: uuid(1), sender: support, to: ['jane@example.test'], subject: 'A fresh thread', text: 'A synthetic runtime message.' };
  const send = () => mf.dispatchFetch(origin + '/cmail/api/compose', { method: 'POST', headers: { Origin: origin, 'X-Cmail-Action': 'compose', 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  const results = await Promise.all((await Promise.all([send(), send()])).map((response) => response.json()));
  assert.equal(results[0].state, 'sent'); assert.deepEqual(results[0], results[1]); assert.equal(calls.length, 1);
  assert.equal((await (await send()).json()).state, 'sent'); assert.equal(calls.length, 1);
  const sent = await (await mf.dispatchFetch(origin + '/cmail/api/messages?view=sent')).json();
  assert.equal(sent.total_view, 1); assert.equal(sent.items[0].subject, body.subject);
  const conversation = await (await mf.dispatchFetch(origin + '/cmail/api/messages/' + results[0].archive_id + '/conversation')).json();
  assert.equal(conversation.total, 1);
  const contacts = await (await mf.dispatchFetch(origin + '/cmail/api/contacts')).json();
  assert.equal(contacts.total, 3); assert.equal(contacts.items.find((item) => item.address === 'jane@example.test').sent, 1);
  assert.match(await (await mf.dispatchFetch(origin + '/cmail/')).text(), /compose.js/);
  console.log(JSON.stringify({ passed: true, runtime: 'workerd with SQLite Durable Objects', checks: ['new-message sender routing', 'concurrent double-submit sends once', 'durable replay', 'standalone Sent thread', 'encrypted contacts', 'compose assets'], provider_calls: calls.length }));
  if (serve) {
    console.log(JSON.stringify({ browser_fixture_url: String(await mf.ready) + 'cmail/?mailbox=' + encodeURIComponent(support), pid: process.pid, network: 'external connections disabled; fake mail only' }));
    await new Promise((resolve) => { process.on('SIGINT', resolve); process.on('SIGTERM', resolve); });
    console.log(JSON.stringify({ browser_provider_calls: calls.length - 1, total_provider_calls: calls.length }));
  }
} finally { await mf.dispose(); }
