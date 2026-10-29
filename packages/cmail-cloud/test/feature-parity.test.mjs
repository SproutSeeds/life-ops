import test from 'node:test';
import assert from 'node:assert/strict';
import { handleAgent } from '../src/agent-api.mjs';
import { attachmentHttp, ATTACHMENT_LIMITS, BLOCKED_EXTENSIONS } from '../src/attachments.mjs';
import { fixture, uuid, support } from './reply-fixture.mjs';

const agent = (queue, action, parameters = {}) => handleAgent(queue, new Request('https://internal/agent/request', {
  method: 'POST', body: JSON.stringify({ action, parameters }),
}));
const value = { kind: 'compose', sender: support, to: ['person@example.test'], pending: '', subject: 'Parity fixture', text: '', signature: '' };

test('agent/browser attachment policy, bytes, save, copy, removal and revisions share one store', async () => {
  const { queue, env, storage } = await fixture(); env.CMAIL_AGENT_ENABLED = '1';
  const status = await (await agent(queue, 'status')).json();
  assert(status.capabilities.includes('draft-attachments'));
  const prepared = await (await queue.fetch(new Request('https://internal/compose/prepare', { method: 'POST', body: '{}' }))).json();
  assert.deepEqual(status.attachments, prepared.attachments);
  const data = Uint8Array.from([0, 255, 128, 1, 2]);
  const input = { id: uuid(101), filename: 'Résumé ☕.custom', content_type: 'application/octet-stream', content_base64: Buffer.from(data).toString('base64') };
  const uploaded = await (await agent(queue, 'attachment-upload', input)).json();
  assert.deepEqual(await (await agent(queue, 'attachment-upload', input)).json(), uploaded);
  assert.equal((await agent(queue, 'attachment-upload', { ...input, content_base64: 'eA==' })).status, 409);
  const webDownload = await attachmentHttp(queue, new Request('https://internal/file'), uploaded.attachment.id);
  assert.deepEqual(new Uint8Array(await webDownload.arrayBuffer()), data);
  const browserFile = (await (await attachmentHttp(queue, new Request('https://internal/file', {
    method: 'POST', headers: { 'X-Cmail-Filename': 'website.pdf', 'X-Cmail-Content-Type': 'application/pdf' }, body: 'Synthetic browser PDF',
  }), uuid(102))).json()).attachment;
  let saved = await (await agent(queue, 'draft-save', { id: 'draft_' + uuid(103), revision: 0, save_id: uuid(104), draft: { ...value, attachments: [uploaded.attachment, browserFile] } })).json();
  assert.deepEqual((await queue.drafts.get(saved.id)).draft.attachments, saved.draft.attachments);
  for (const attachment of [1, browserFile.id]) {
    const result = await (await agent(queue, 'draft-attachment', { id: saved.id, attachment })).json();
    assert.equal(Buffer.from(result.content_base64, 'base64').toString(), 'Synthetic browser PDF');
    assert.equal(result.sha256, browserFile.sha256);
  }
  assert.equal((await agent(queue, 'draft-attachment', { id: saved.id, attachment: -1 })).status, 404);
  const copy = await (await agent(queue, 'draft-save', { id: 'draft_' + uuid(105), revision: 0, save_id: uuid(106), draft: saved.draft })).json();
  assert.deepEqual(copy.draft.attachments, saved.draft.attachments);
  saved = await queue.drafts.save({ id: saved.id, revision: 1, save_id: uuid(107), draft: { ...saved.draft, attachments: [browserFile] } });
  assert.equal((await agent(queue, 'draft-save', { id: saved.id, revision: 1, save_id: uuid(108), draft: value })).status, 409);
  const agentRead = await (await agent(queue, 'draft', { id: saved.id })).json();
  assert.deepEqual(agentRead.draft.attachments, [browserFile]);
  saved = await (await agent(queue, 'draft-save', { id: saved.id, revision: saved.revision, save_id: uuid(109), draft: { ...saved.draft, attachments: [] } })).json();
  assert.deepEqual((await queue.drafts.get(saved.id)).draft.attachments, []);
  assert.deepEqual((await queue.drafts.get(copy.id)).draft.attachments, copy.draft.attachments);
  assert(![...storage.data.keys()].some((key) => key.startsWith('cmail-outbox:')));
});

test('both adapters reject empty, blocked, oversized and forged attachments', async () => {
  const { queue, env } = await fixture(); env.CMAIL_AGENT_ENABLED = '1';
  for (const extension of BLOCKED_EXTENSIONS) {
    const result = await agent(queue, 'attachment-upload', { id: uuid(201), filename: 'unsafe.' + extension.toUpperCase(), content_base64: 'eA==' });
    assert.equal(result.status, 400); assert.equal((await result.json()).error, 'attachment_type_blocked');
  }
  for (const [content_base64, error] of [['', 'attachment_empty'], ['@@@', 'invalid_attachment_base64'], ['A'.repeat(Math.ceil(ATTACHMENT_LIMITS.max_bytes / 3) * 4 + 4), 'attachments_too_large']]) {
    const result = await agent(queue, 'attachment-upload', { id: uuid(202), filename: 'file.txt', content_base64 });
    assert.equal((await result.json()).error, error);
  }
  const result = await agent(queue, 'draft-save', { id: 'draft_' + uuid(203), revision: 0, save_id: uuid(204), draft: { ...value, attachments: [{ id: uuid(205), filename: 'forged.txt', size: 1 }] } });
  assert.equal(result.status, 404);
});
