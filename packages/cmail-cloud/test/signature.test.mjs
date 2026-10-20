import test from 'node:test';
import assert from 'node:assert/strict';
import { brandedMessage, publicIdentity, signatureLogo, splitSignature } from '../src/signature.mjs';
import { sendCompose, sendReply } from '../src/reply.mjs';
import { PostalMime } from '../src/production-base.mjs';
import { decodeRaw } from '../src/resend.mjs';
import { fixture, uuid, primary } from './reply-fixture.mjs';
import { composeScript } from '../src/compose-ui.mjs';

const signature = 'Example Coffee\nhello@example.test\nhttps://example.test';
const logo = { url: 'https://example.test/logo.png', href: 'https://example.test', alt: 'Example Coffee' };
function branded(env) {
  const identities = JSON.parse(env.CMAIL_SEND_IDENTITIES);
  identities[primary] = { ...identities[primary], signature, signature_logo: logo };
  env.CMAIL_SEND_IDENTITIES = JSON.stringify(identities);
  return identities[primary];
}

test('branded messages escape all body content, keep text links and skip edited or missing signatures', () => {
  const identity = { signature, signature_logo: logo };
  const text = '<img src=x onerror=alert(1)> & "Hi"\nSecond line\n\n' + signature;
  const html = brandedMessage(identity, text);
  assert.match(html, /&lt;img src=x onerror=alert\(1\)&gt; &amp; &quot;Hi&quot;<br>Second line/);
  assert.match(html, /mailto:hello@example.test/);
  assert.equal((html.match(/<img /g) || []).length, 1);
  assert.equal(brandedMessage(identity, text + '\nChanged'), '');
  assert.equal(brandedMessage(identity, 'Message without a signature'), '');
  assert.equal(splitSignature('partial' + signature, signature), null);
  for (const url of ['javascript:alert(1)', 'data:image/png;base64,AA', 'https://foreign.test/logo.png', 'https://example.test/logo.svg', "https://example.test/x';.png"]) {
    assert.equal(signatureLogo({ signature_logo: { ...logo, url } }), null);
  }
  assert.equal(publicIdentity(primary, { ...identity, from: primary, key: 'secret' }).key, undefined);
  assert.doesNotThrow(() => new Function(composeScript));
});

test('Compose and replies send matching HTML/plain-text alternatives and archive both with stable retries', async () => {
  for (const kind of ['compose', 'reply']) {
    const { queue, env, id, storage } = await fixture({ mailbox: primary }); branded(env);
    const text = 'Synthetic body café.\n\n' + signature;
    const input = { id, sender: primary, to: ['person@example.test'], subject: 'Fixture', request_id: uuid(kind === 'compose' ? 801 : 802), text };
    const send = kind === 'compose' ? sendCompose : sendReply;
    let first;
    const pending = await send(queue, input, { fetcher: async (_, options) => { first = options.body; throw new Error('timeout'); } });
    assert.equal(pending.state, 'uncertain');
    const identities = JSON.parse(env.CMAIL_SEND_IDENTITIES); identities[primary].signature_logo.url = 'https://example.test/new.png'; env.CMAIL_SEND_IDENTITIES = JSON.stringify(identities);
    const sent = await send(queue, input, { fetcher: async (_, options) => { assert.equal(options.body, first); return Response.json({ id: uuid(899) }); } });
    const payload = await queue.loadPayload(await storage.get('mail:' + sent.archive_id));
    const parsed = await PostalMime.parse(decodeRaw(payload.raw_base64));
    assert.equal(parsed.text.trimEnd(), text);
    assert.equal(parsed.html.trimEnd(), JSON.parse(first).html);
    assert.equal(JSON.parse(first).text, text);
    assert.match(parsed.html, /example.test\/logo.png/);
    assert.doesNotMatch(parsed.html, /new.png/);
  }
});

test('saved drafts freeze server-generated signature HTML and ignore client HTML or logo injection', async () => {
  const { queue, env } = await fixture(); branded(env);
  const text = 'Approved synthetic body.\n\n' + signature;
  const saved = await queue.drafts.save({ id: 'draft_' + uuid(810), revision: 0, save_id: uuid(811), draft: {
    kind: 'compose', sender: primary, to: ['person@example.test'], pending: '', subject: 'Fixture', text, signature,
    html: '<script>bad()</script>', signature_logo: { url: 'https://bad.test/logo.png' },
  } });
  assert.equal(saved.draft.text, text);
  assert.match(saved.draft.html, /example.test\/logo.png/);
  assert.doesNotMatch(saved.draft.html, /bad.test|<script/);
  const identities = JSON.parse(env.CMAIL_SEND_IDENTITIES); identities[primary].signature = 'Different signature'; env.CMAIL_SEND_IDENTITIES = JSON.stringify(identities);
  const reopened = await queue.drafts.get(saved.id);
  assert.equal(reopened.draft.html, saved.draft.html);
  const sent = await queue.drafts.send({ id: saved.id, revision: saved.revision }, { fetcher: async (_, options) => {
    assert.equal(JSON.parse(options.body).html, saved.draft.html);
    assert.equal(JSON.parse(options.body).text, text);
    return Response.json({ id: uuid(812) });
  } });
  assert.equal(sent.state, 'sent');
});
