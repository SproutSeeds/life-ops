// Local workerd fixture: no real credentials, no provider and no external network.
import assert from 'node:assert/strict';
import { fileURLToPath } from 'node:url';
import fs from 'node:fs';
import { bindings, support } from '../test/reply-fixture.mjs';
const { Miniflare, createFetchMock } = await import(process.argv[2]);
const serve = process.argv.includes('--serve'), root = fileURLToPath(new URL('../', import.meta.url));
const fetchMock = createFetchMock(); fetchMock.disableNetConnect();
const cases = [
  { subject: 'Order confirmation with excessive spacing', text: 'Order confirmation\n' + '  \t\u00a0\u200b\n'.repeat(38) + '                  Thank you for your order!\n\n\n\n                Shipping address\n\n   123 Example Lane\nExample Town, ZZ 12345\n\nTotal: $123.45\n\n' + 'Receipt: https://example.test/orders/receipt?tracking=' + 'a'.repeat(300) },
  { subject: 'Technical note, list and quoted reply', text: 'The result is ready.\n\n1. Keep the first item\n  - Keep this nested item\n2. Keep the second item\n\n```python\nif ready:\n    print("ready")\n\n\n    print("done")\n```\n\nItem        Qty     Total\nExample     2       12.00\n\n\n\n> Earlier message\n>\n>\n> Still relevant.' },
  { subject: 'Untrusted markup remains text', text: '<script>alert("should never run")</script>\n<img src="http://127.0.0.1:9346/fixture/tracker" onerror="alert(1)">\n<iframe src="http://127.0.0.1:9346/fixture/frame"></iframe>\n<style>body{display:none}</style>\njavascript:alert(2)\n\nPlain text below the markup.' },
];
for (let i = 3; i < process.argv.length; i++) {
  if (process.argv[i] === '--sample') {
    const sample = JSON.parse(fs.readFileSync(process.argv[++i], 'utf8'));
    cases.push({ subject: 'Private local sample ' + (cases.length - 2), text: sample.text });
  }
}
const mf = new Miniflare({ name: 'cmail-readable-fixture', host: '127.0.0.1', port: serve ? 9346 : 0,
  modules: true, modulesRoot: root, scriptPath: root + 'test/reader-worker.mjs', compatibilityDate: '2026-03-27',
  bindings: { ...bindings, CMAIL_FAST_READS: '1', CMAIL_SENT_ENABLED: '1', CMAIL_COMPOSE_ENABLED: '1', CMAIL_DRAFTS_ENABLED: '1' },
  fetchMock, durableObjects: { MAIL_QUEUE: { className: 'MailQueue', useSQLite: true } },
});
try {
  const namespace = await mf.getDurableObjectNamespace('MAIL_QUEUE'), stub = namespace.get(namespace.idFromName('primary'));
  for (const [index, item] of cases.entries()) {
    const raw = `From: Fixture <reader@example.test>\r\nTo: ${support}\r\nSubject: ${item.subject}\r\nMessage-ID: <reader-${index}@example.test>\r\n${index === 1 ? 'References: <reader-0@example.test>\r\nIn-Reply-To: <reader-0@example.test>\r\n' : ''}Date: Tue, 20 Oct 2026 17:00:00 +0000\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: base64\r\n\r\n${Buffer.from(item.text).toString('base64')}\r\n`;
    const response = await stub.fetch('https://internal/enqueue', { method: 'POST', body: JSON.stringify({ envelope_to: support, received_at: new Date(Date.UTC(2026, 9, 20, 17, index)).toISOString(), raw_base64: Buffer.from(raw).toString('base64') }) });
    const result = await response.json(); assert.equal(response.status, 200); assert(result.id);
    await stub.fetch('https://internal/archive/read-status', { method: 'POST', body: JSON.stringify({ id: result.id, read: true }) });
  }
  const origin = 'http://127.0.0.1:9346';
  const first = await (await mf.dispatchFetch(origin + '/cmail/api/messages/mail_0000000000000001')).json();
  assert.equal(first.text.trimEnd(), cases[0].text.trimEnd());
  assert.equal((await (await mf.dispatchFetch(origin + '/cmail/api/messages/mail_0000000000000001/conversation')).json()).total, 2);
  assert.match(await (await mf.dispatchFetch(origin + '/cmail/app.js')).text(), /Original text/);
  assert.match(await (await mf.dispatchFetch(origin + '/cmail/reply.css')).text(), /cmail-readable/);
  console.log(JSON.stringify({ passed: true, messages: cases.length, provider_calls: 0, checks: ['original API content unchanged', 'lazy conversation metadata', 'readability assets'] }));
  if (serve) {
    console.log(JSON.stringify({ browser_fixture_url: String(await mf.ready) + 'cmail/#mail_0000000000000001', pid: process.pid, network: 'external connections disabled; no provider' }));
    await new Promise((resolve) => { process.on('SIGINT', resolve); process.on('SIGTERM', resolve); });
  }
} finally { await mf.dispose(); }
