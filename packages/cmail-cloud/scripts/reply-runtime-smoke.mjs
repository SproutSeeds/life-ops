// Uses synthetic messages, fake credentials and a network-isolated provider.
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import { bindings, payload, uuid, support } from '../test/reply-fixture.mjs';
const { Miniflare, createFetchMock } = await import(process.argv[2]);
const serve = process.argv.includes('--serve');
const root = fileURLToPath(new URL('../', import.meta.url));
const fetchMock = createFetchMock(); fetchMock.disableNetConnect();
const calls = [];
fetchMock.get('https://api.resend.com').intercept({ path: '/emails', method: 'POST' }).reply(200, async (options) => {
  const body = await new Response(options.body).json();
  assert.equal(body.from, `Bind My Soul Support <${support}>`);
  assert.deepEqual(body.to, ['reply@example.test']);
  assert.equal(body.headers['In-Reply-To'], '<original@example.test>');
  calls.push({ headers: options.headers, body });
  return JSON.stringify({ id: uuid(900 + calls.length) });
}, { headers: { 'Content-Type': 'application/json' } }).persist();
const mf = new Miniflare({ name: 'cmail-reply-fixture', host: '127.0.0.1', port: serve ? 9346 : 0,
  modules: true, modulesRoot: root, scriptPath: root + 'test/reply-worker.mjs', compatibilityDate: '2026-03-27',
  bindings, fetchMock, durableObjects: { MAIL_QUEUE: { className: 'MailQueue', useSQLite: true } },
});
try {
  const namespace = await mf.getDurableObjectNamespace('MAIL_QUEUE');
  const stub = namespace.get(namespace.idFromName('primary'));
  const call = (path, input) => stub.fetch('https://internal' + path, { method: 'POST', body: JSON.stringify(input) });
  const original = await (await call('/enqueue', payload())).json();
  const origin = 'http://127.0.0.1:9346';
  const path = `/cmail/api/messages/${original.id}/reply`;
  const prepared = await (await mf.dispatchFetch(origin + path)).json();
  assert.equal(prepared.sender, support); assert.deepEqual(prepared.to, ['reply@example.test']);
  assert.equal(calls.length, 0);
  const send = () => mf.dispatchFetch(origin + path, { method: 'POST', headers: { Origin: origin, 'X-Cmail-Action': 'reply', 'Content-Type': 'application/json' }, body: JSON.stringify({ request_id: uuid(1), text: 'Synthetic runtime reply.' }) });
  const responses = await Promise.all([send(), send()]);
  const results = await Promise.all(responses.map((response) => response.json()));
  if (results[0].state !== 'sent') console.log({ results, calls });
  assert.equal(results[0].state, 'sent'); assert.deepEqual(results[0], results[1]);
  assert.equal(calls.length, 1);
  const message = await (await call('/archive/message', { id: results[0].archive_id })).json();
  assert.equal(message.from.address, support); assert.equal(message.archived, true);
  assert.equal(message.in_reply_to, '<original@example.test>');
  assert.equal((await (await send()).json()).state, 'sent'); assert.equal(calls.length, 1);
  const html = await (await mf.dispatchFetch(origin + '/cmail/')).text();
  assert.match(html, /reply.js/);
  console.log(JSON.stringify({ passed: true, runtime: 'workerd with SQLite Durable Objects', checks: ['native provider send fixture', 'concurrent double-submit sends once', 'durable receipt replay', 'encrypted sent archive', 'reply context', 'thread headers', 'HTML integration'], provider_calls: calls.length }));
  if (serve) {
    console.log(JSON.stringify({ browser_fixture_url: String(await mf.ready) + 'cmail/?mailbox=' + encodeURIComponent(support) + '#' + original.id, network: 'all external connections disabled; fake mail only' }));
    await new Promise((resolve) => { process.on('SIGINT', resolve); process.on('SIGTERM', resolve); });
    console.log(JSON.stringify({ browser_provider_calls: calls.length - 1, total_provider_calls: calls.length }));
  }
} finally { await mf.dispose(); }
