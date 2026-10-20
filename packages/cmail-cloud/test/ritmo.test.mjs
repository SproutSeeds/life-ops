import test from 'node:test';
import assert from 'node:assert/strict';
import { prepareReply, sendCompose, sendReply, sendingIdentities } from '../src/reply.mjs';
import { fixture, uuid } from './reply-fixture.mjs';

function addRitmo(env) {
  const identities = JSON.parse(env.CMAIL_SEND_IDENTITIES);
  for (const address of ['hello@ritmo.coffee', 'cody@ritmo.coffee']) {
    identities[address] = { from: `Ritmo Coffee <${address}>`, signature: 'Ritmo Coffee', key: 'ritmo' };
    env.CMAIL_MAILBOXES += ',' + address;
  }
  env.CMAIL_SEND_IDENTITIES = JSON.stringify(identities);
  env.CMAIL_RITMO_SEND_KEY = 'fixture-ritmo-key';
}

test('both Ritmo senders use their scoped credential for Compose and replies', async () => {
  for (const sender of ['hello@ritmo.coffee', 'cody@ritmo.coffee']) {
    const { queue, env, id } = await fixture({ mailbox: sender });
    addRitmo(env);
    assert.equal((await prepareReply(queue, id)).sender, sender);
    let calls = 0;
    const fetcher = async (_, options) => {
      calls++;
      assert.equal(options.headers.Authorization, 'Bearer fixture-ritmo-key');
      assert.equal(JSON.parse(options.body).from, `Ritmo Coffee <${sender}>`);
      return Response.json({ id: uuid(950) });
    };
    const composition = { sender, to: ['customer@example.test'], subject: 'Ritmo fixture', text: 'Synthetic fixture.', request_id: uuid(951) };
    assert.equal((await sendCompose(queue, composition, { fetcher })).state, 'sent');
    assert.equal((await sendReply(queue, { id, text: 'Synthetic reply.', request_id: uuid(952) }, { fetcher })).state, 'sent');
    assert.equal(calls, 2);
    assert.equal((await sendCompose(queue, composition, { fetcher })).state, 'sent');
    assert.equal(calls, 2);
  }
});

test('missing or unrecognized Ritmo credential cannot fall back to another mailbox key', async () => {
  const { queue, env } = await fixture();
  addRitmo(env);
  delete env.CMAIL_RITMO_SEND_KEY;
  let calls = 0;
  await assert.rejects(sendCompose(queue, { sender: 'hello@ritmo.coffee', to: ['customer@example.test'], subject: 'Fixture', text: 'Fixture', request_id: uuid(953) }, {
    fetcher: async () => { calls++; throw new Error('unexpected provider call'); },
  }), /sender_not_configured/);
  assert.equal(calls, 0);
  const identities = JSON.parse(env.CMAIL_SEND_IDENTITIES);
  identities['hello@ritmo.coffee'].key = '__proto__';
  env.CMAIL_SEND_IDENTITIES = JSON.stringify(identities);
  assert.equal(sendingIdentities(env)['hello@ritmo.coffee'], undefined);
});
