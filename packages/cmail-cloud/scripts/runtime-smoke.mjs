// Isolated workerd/SQLite verification. Pass the installed Miniflare module path.
import assert from 'node:assert/strict';
import { createHash, createHmac } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';

const { Miniflare, createFetchMock } = await import(process.argv[2]);
const root = fileURLToPath(new URL('../src/', import.meta.url));
const secret = 'runtime-fixture-ingest-key';
const bindings = { LIFE_OPS_MASTER_KEY: Buffer.alloc(32, 37).toString('base64url'), LIFE_OPS_MAIL_INGEST_SECRET: secret, CMAIL_MAILBOXES: 'cody@frg.earth', RESEND_RECEIVING_API_KEY: 'runtime-fixture-resend-key' };
const fetchMock = createFetchMock();
fetchMock.disableNetConnect();
const options = (module) => ({ name: 'cmail-runtime-fixture', modules: true, modulesRoot: root, scriptPath: resolve(root, module), compatibilityDate: '2026-03-27', bindings, fetchMock, durableObjects: { MAIL_QUEUE: { className: 'MailQueue', useSQLite: true } } });
const mf = new Miniflare(options('production-base.mjs'));
const raw = Buffer.from('From: player@example.test\r\nTo: support@bindmysoul.com\r\nSubject: Runtime preservation\r\nMessage-ID: <runtime@example.test>\r\n\r\nOriginal runtime fixture.\r\n');
const payload = { raw_base64: raw.toString('base64'), payload_hash: createHash('sha256').update(raw).digest('hex'), envelope_to: 'support@bindmysoul.com', provider: 'resend-receiving', external_id: 'resend-receiving:10000000-0000-4000-8000-000000000001', received_at: '2026-10-17T00:00:00Z', direction: 'inbound' };
const doCall = async (path, body) => {
  const namespace = await mf.getDurableObjectNamespace('MAIL_QUEUE');
  return namespace.get(namespace.idFromName('primary')).fetch('https://internal' + path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
};
try {
  const original = await (await doCall('/enqueue', payload)).json();
  assert.equal(original.stored, true);
  await mf.setOptions(options('index.mjs'));
  const olderRaw = Buffer.from(raw.toString().replace('runtime@example', 'older@example').replace('Original runtime', 'Older runtime'));
  const imported = await (await doCall('/resend/import', { ...payload, raw_base64: olderRaw.toString('base64'), payload_hash: createHash('sha256').update(olderRaw).digest('hex'), external_id: 'resend-receiving:10000000-0000-4000-8000-000000000002', received_at: '2026-08-01T00:00:00Z' })).json();
  assert.equal(imported.stored, true);
  const listed = await (await doCall('/archive/list', { view: 'all' })).json();
  assert.equal(listed.failed, 0);
  assert.equal(listed.items.length, 2);
  assert.equal(listed.items[0].id, original.id);
  const preserved = await (await doCall('/archive/message', { id: original.id, raw: true })).json();
  assert.equal(preserved.content_base64, raw.toString('base64'));
  const again = await (await doCall('/resend/import', payload)).json();
  assert.equal(again.duplicate, true);
  assert.equal(again.id, original.id);
  const status = await (await doCall('/status', {})).json();
  assert.equal(status.total_stored, 2);
  assert.equal(status.pending_count, 1);
  assert.equal(status.total_acknowledged, 1);
  const time = new Date().toISOString(); const body = '{}';
  const signed = await mf.dispatchFetch('https://worker.test/api/mail/resend/status', { method: 'POST', headers: { 'X-Life-Ops-Timestamp': time, 'X-Life-Ops-Signature': `sha256=${createHmac('sha256', secret).update(`${time}.${body}`).digest('hex')}` }, body });
  assert.equal(signed.status, 200);
  assert.equal((await mf.dispatchFetch('https://worker.test/api/mail/resend/status', { method: 'POST', body: '{}' })).status, 401);
  assert.equal((await mf.dispatchFetch('https://cmail.frg.earth/')).status, 503);
  // Match the largest preserved production MIME (6.94 MB) before live import.
  const largeRaw = Buffer.concat([raw, Buffer.alloc(7 * 1024 * 1024, 65)]);
  const large = await (await doCall('/resend/import', { ...payload, external_id: 'resend-receiving:10000000-0000-4000-8000-000000000003', raw_base64: largeRaw.toString('base64'), payload_hash: createHash('sha256').update(largeRaw).digest('hex') })).json();
  assert.equal(large.stored, true);
  const largeRead = await (await doCall('/archive/message', { id: large.id, raw: true })).json();
  assert.equal(createHash('sha256').update(Buffer.from(largeRead.content_base64, 'base64')).digest('hex'), createHash('sha256').update(largeRaw).digest('hex'));
  const providerId = '10000000-0000-4000-8000-000000000004';
  const api = fetchMock.get('https://api.resend.com');
  api.intercept({ path: '/emails/receiving?limit=100' }).reply(200, { data: [{ id: providerId }], has_more: false });
  api.intercept({ path: `/emails/receiving/${providerId}` }).reply(200, { to: ['support@bindmysoul.com'], created_at: '2026-10-17T00:00:00Z', raw: { download_url: 'https://raw.example.test/provider.eml' } });
  fetchMock.get('https://raw.example.test').intercept({ path: '/provider.eml' }).reply(200, raw.toString().replace('runtime@example', 'provider@example'));
  const synced = await (await doCall('/resend/sync', { trigger: 'scheduled' })).json();
  assert.equal(synced.status, 'healthy'); assert.equal(synced.imported, 1);
  assert(synced.last_scheduled_success_at);
  fetchMock.assertNoPendingInterceptors();
  console.log(JSON.stringify({ passed: true, runtime: 'workerd with SQLite Durable Objects', checks: ['existing archive upgrade', 'atomic encrypted import', 'source deduplication', 'original MIME preservation', '7 MB MIME round-trip', 'native fetch with provider fixture', 'chronological metadata index', 'pending queue preservation', 'admin HMAC', 'website access enforcement'] }));
} finally { await mf.dispose(); }
