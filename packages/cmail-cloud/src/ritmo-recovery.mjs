import { sha256Hex, verifyLifeOpsRequest } from './production-base.mjs';
import { sendingIdentities } from './reply.mjs';

const json = (value, status = 200) => Response.json(value, { status, headers: { 'Cache-Control': 'private, no-store' } });
const sender = 'hello@ritmo.coffee', recipient = 'cody@ritmo.coffee';

// A separate capability, restricted to one template and one existing owner.
// It grants no access to the administrative agent API or mailbox contents.
export async function recoveryTransport(request, env) {
  if (request.method !== 'POST') return json({ error: 'method_not_allowed' }, 405);
  if (!env.RITMO_AUTH_MAIL_SECRET) return json({ error: 'unconfigured' }, 503);
  const reader = request.body?.getReader();
  if (!reader) return json({ error: 'invalid_request' }, 400);
  const chunks = []; let size = 0;
  while (true) {
    const { done, value } = await reader.read(); if (done) break;
    size += value.length;
    if (size > 1024) { await reader.cancel(); return json({ error: 'body_too_large' }, 413); }
    chunks.push(value);
  }
  const bytes = new Uint8Array(size); let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.length; }
  const body = new TextDecoder().decode(bytes);
  const verified = await verifyLifeOpsRequest({ secret: env.RITMO_AUTH_MAIL_SECRET, body, maxSkewSeconds: 120,
    timestamp: request.headers.get('X-Life-Ops-Timestamp') || '', signature: request.headers.get('X-Life-Ops-Signature') || '' });
  if (!verified.ok) return json({ error: 'unauthorized' }, 401);
  return env.MAIL_QUEUE.getByName('primary').fetch('https://mail-queue.internal/ritmo/recovery', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body,
  });
}

export async function handleRitmoRecovery(queue, request, options) {
  try {
    const input = await request.json();
    if (!input || Object.keys(input).length !== 1 || typeof input.token_hash !== 'string' || !/^[a-f0-9]{40,128}$/.test(input.token_hash)) return json({ error: 'invalid_request' }, 400);
    const identity = sendingIdentities(queue.env)[sender];
    if (!identity || identity.key !== 'ritmo' || !queue.env.CMAIL_RITMO_SEND_KEY) return json({ error: 'unconfigured' }, 503);
    // Stable draft/save IDs carry the existing C-mail send receipt across retries.
    const digest = await sha256Hex(new TextEncoder().encode('ritmo-recovery:' + input.token_hash));
    const uuid = `${digest.slice(0,8)}-${digest.slice(8,12)}-4${digest.slice(13,16)}-8${digest.slice(17,20)}-${digest.slice(20,32)}`;
    const id = 'draft_' + uuid;
    let draft;
    try { draft = await queue.drafts.get(id); } catch (error) { if (error.status !== 404) throw error; }
    if (!draft) {
      const allowed = await queue.state.storage.transaction(async (tx) => {
        const previous = await tx.get('ritmo-recovery-rate') || { start: 0, count: 0 };
        const now = Date.now();
        const rate = now - previous.start < 3600000 ? previous : { start: now, count: 0 };
        if (rate.count >= 8) return false;
        await tx.put('ritmo-recovery-rate', { ...rate, count: rate.count + 1 }); return true;
      });
      if (!allowed) return json({ error: 'rate_limited' }, 429);
      const link = `https://ritmo.coffee/admin/reset#token_hash=${input.token_hash}`;
      const text = `Choose a new password for your Ritmo sales desk:\n\n${link}\n\nThis link works once. If you didn’t request it, you can ignore this email.\n\n${identity.signature}`;
      try {
        draft = await queue.drafts.save({ id, revision: 0, save_id: uuid, draft: {
          kind: 'compose', sender, to: [recipient], pending: '', subject: 'Reset your Ritmo password', text, signature: identity.signature,
        } });
      } catch (error) {
        if (![409, 410].includes(error.status)) throw error;
        draft = await queue.drafts.get(id);
      }
    }
    const receipt = await queue.drafts.send({ id, revision: draft.revision }, options);
    if (receipt.state !== 'sent') return json({ error: 'delivery_pending' }, 503);
    return json({ state: 'sent' });
  } catch {
    // No tokens, message bodies, provider responses or mailbox records in errors.
    return json({ error: 'recovery_unavailable' }, 503);
  }
}
