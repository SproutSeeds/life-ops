import { handleDrafts } from './draft-store.mjs';
import { sendingIdentities } from './reply.mjs';
import { publicIdentity } from './signature.mjs';

// Reached only through the existing timestamped HMAC administrative transport.
// This is an allowlist, not an arbitrary Durable Object route proxy.
export async function handleAgent(queue, request) {
  const json = (value, status = 200) => Response.json(value, { status, headers: { 'Cache-Control': 'private, no-store' } });
  if (queue.env.CMAIL_AGENT_ENABLED !== '1') return json({ error: 'agent_api_disabled' }, 503);
  if (request.method !== 'POST') return json({ error: 'method_not_allowed' }, 405);
  try {
    const { action, parameters: p = {} } = await request.json();
    if (!p || typeof p !== 'object' || Array.isArray(p)) return json({ error: 'invalid_parameters' }, 400);
    const identities = Object.entries(sendingIdentities(queue.env)).map(([address, item]) => publicIdentity(address, item));
    if (action === 'status') return json({ system: 'cmail-cloud', website: 'https://cmail.frg.earth', ready: true,
      drafts: (await queue.state.storage.get('cmail-drafts-v1:count:*')) || 0, identities,
      capabilities: ['messages', 'search', 'threads', 'raw', 'attachments-read', 'drafts', 'send', 'read-state', 'archive', 'spam'] });
    if (action === 'identities') return json({ identities });
    // Append-only ingestion cursor for the local Life Ops projection. Ordinary
    // mailbox reads use the bounded indexes above, never this raw export.
    if (action === 'changes') {
      if (p.cursor && !/^mail_\d{16}$/.test(p.cursor)) return json({ error: 'invalid_cursor' }, 400);
      const limit = Math.min(10, Math.max(1, Number(p.limit) || 5));
      const records = await queue.state.storage.list({ prefix: 'mail:', ...(p.cursor ? { start: 'mail:' + p.cursor + '\0' } : {}), limit: limit + 1 });
      const values = [...records.values()], items = [];
      let bytes = 0;
      for (const record of values.slice(0, limit)) {
        const payload = await queue.loadPayload(record);
        items.push({ id: record.id, payload });
        bytes += payload.raw_base64?.length || 0;
        // Large attachments travel alone rather than accumulating ten full MIME
        // archives in memory. The cursor still advances only through this page.
        if (bytes >= 4 * 1024 * 1024) break;
      }
      return json({ items, cursor: items.at(-1)?.id || p.cursor || null, has_more: values.length > items.length });
    }
    if (action === 'messages') return json(await queue.listArchive(p));
    if (action === 'message') {
      const result = await queue.readArchiveMessage({ id: p.id, ...(p.raw === true ? { raw: true } : {}), ...(Number.isInteger(p.attachment) ? { attachment: p.attachment } : {}) });
      return json(result || { error: 'message_not_found' }, result ? 200 : 404);
    }
    if (action === 'conversation') return json(await queue.views.conversation({ id: p.id }));
    if (action === 'contacts') return json(await queue.contacts.list(p));
    if (action === 'spam-senders') return json(await queue.spam.list(p));
    if (action === 'message-action') {
      if (!['read', 'unread', 'archive', 'unarchive', 'spam', 'not-spam'].includes(p.action)) return json({ error: 'unsupported_action' }, 400);
      const result = await queue.applyAction({ ids: p.ids, action: p.action });
      return json(result || { error: 'message_not_found' }, result ? 200 : 404);
    }
    const drafts = { 'drafts': 'list', 'draft': 'get', 'reply-context': 'reply', 'draft-save': 'save', 'draft-discard': 'discard', 'draft-send': 'send' };
    if (!Object.hasOwn(drafts, action)) return json({ error: 'unsupported_action' }, 400);
    if (action === 'draft-send' && (p.confirm_send !== true || !Number.isSafeInteger(p.revision))) return json({ error: 'explicit_send_confirmation_required' }, 400);
    return handleDrafts(queue, new Request('https://mail-queue.internal/drafts/' + drafts[action], { method: 'POST', body: JSON.stringify(p) }));
  } catch (error) { return json({ error: error.status ? error.message : 'agent_request_unavailable' }, error.status || 503); }
}
