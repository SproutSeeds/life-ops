import test from 'node:test';
import assert from 'node:assert/strict';
import { createHmac } from 'node:crypto';
import { recoveryTransport, handleRitmoRecovery } from '../src/ritmo-recovery.mjs';
import { fixture } from './reply-fixture.mjs';

const input = { token_hash: 'a'.repeat(56) };
const request = (value = input) => new Request('https://internal/ritmo/recovery', { method: 'POST', body: JSON.stringify(value) });
async function setup() {
  const value = await fixture();
  value.env.CMAIL_RITMO_SEND_KEY = 'fixture-ritmo-key';
  value.env.CMAIL_MAILBOXES += ',hello@ritmo.coffee,cody@ritmo.coffee';
  value.env.CMAIL_SEND_IDENTITIES = JSON.stringify({ ...JSON.parse(value.env.CMAIL_SEND_IDENTITIES),
    'hello@ritmo.coffee': { from: 'Ritmo Coffee <hello@ritmo.coffee>', signature: 'Ritmo Coffee\nhttps://ritmo.coffee', key: 'ritmo' } });
  return value;
}
test('dedicated transport rejects missing, forged, stale and oversized requests', async () => {
  let calls = 0;
  const env = { RITMO_AUTH_MAIL_SECRET: 'synthetic-recovery-secret', MAIL_QUEUE: { getByName: () => ({ fetch: async () => { calls++; return Response.json({ ok: true }); } }) } };
  const body = JSON.stringify(input), stamp = new Date().toISOString();
  const call = (timestamp, payload = body, secret = env.RITMO_AUTH_MAIL_SECRET) => recoveryTransport(new Request('https://example.test/api/mail/ritmo-recovery', {
    method: 'POST', body: payload, headers: { 'X-Life-Ops-Timestamp': timestamp,
      'X-Life-Ops-Signature': 'sha256=' + createHmac('sha256', secret).update(`${timestamp}.${payload}`).digest('hex') },
  }), env);
  assert.equal((await recoveryTransport(request(), env)).status, 401);
  assert.equal((await call(stamp, body, 'other-admin-secret')).status, 401);
  assert.equal((await call('2020-01-01T00:00:00Z')).status, 401);
  assert.equal((await call(stamp, 'x'.repeat(1025))).status, 413);
  assert.equal(calls, 0);
  assert.equal((await call(stamp)).status, 200); assert.equal(calls, 1);
});
test('fixed owner, template, origin and C-mail receipt survive identical retries', async () => {
  const { queue } = await setup(); let calls = 0;
  const options = { fetcher: async (_, options) => {
    calls++; const sent = JSON.parse(options.body);
    assert.equal(sent.from, 'Ritmo Coffee <hello@ritmo.coffee>');
    assert.deepEqual(sent.to, ['cody@ritmo.coffee']);
    assert.equal(sent.subject, 'Reset your Ritmo password');
    assert(sent.text.includes('https://ritmo.coffee/admin/reset#token_hash=' + input.token_hash));
    return Response.json({ id: '12345678-1234-4234-8234-123456789012' });
  } };
  assert.deepEqual(await (await handleRitmoRecovery(queue, request(), options)).json(), { state: 'sent' });
  assert.deepEqual(await (await handleRitmoRecovery(queue, request(), options)).json(), { state: 'sent' });
  assert.equal(calls, 1);
});
test('caller cannot change recipients, body, subject, or reset URL', async () => {
  const { queue } = await setup();
  for (const value of [{ ...input, to: 'attacker@example.test' }, { ...input, text: 'arbitrary' }, { ...input, url: 'https://bad.test' }, { token_hash: 'not-a-token' }, {}]) {
    assert.equal((await handleRitmoRecovery(queue, request(value))).status, 400);
  }
});
test('mail budget prevents floods and provider failures expose no sensitive details', async () => {
  const { queue, storage } = await setup();
  const options = { fetcher: async () => { throw new Error('provider-secret'); } };
  const failed = await handleRitmoRecovery(queue, request(), options);
  assert.equal(failed.status, 503); assert.deepEqual(await failed.json(), { error: 'delivery_pending' });
  await storage.put('ritmo-recovery-rate', { start: Date.now(), count: 8 });
  assert.equal((await handleRitmoRecovery(queue, request({ token_hash: 'b'.repeat(56) }), options)).status, 429);
});
