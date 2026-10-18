import { PostalMime, sha256Hex, encryptArchivePayload, storeArchiveEnvelope } from './production-base.mjs';
import { addresses, decodeRaw, encodeRaw } from './resend.mjs';

const MAIL_ID = /^mail_\d{16}$/;
const REQUEST_ID = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
const EMAIL = /^[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9.-]+\.[a-z]{2,}$/i;
const OUTBOX = 'cmail-outbox:';
const fail = (code, status = 400) => Object.assign(new Error(code), { status });
const json = (value, status = 200) => Response.json(value, { status, headers: { 'Cache-Control': 'private, no-store' } });
const headerIds = (value) => [...new Set(String(value || '').match(/<[^<>\s]+>/g) || [])].filter((id) => id.length < 250 && /^[\x21-\x7e]+$/.test(id));
const emails = (values) => [...new Set((values || []).map((item) => String(item.address || '').toLowerCase()).filter((address) => EMAIL.test(address)))];

export function sendingIdentities(env) {
  let configured;
  try { configured = JSON.parse(env.CMAIL_SEND_IDENTITIES || '{}'); } catch { return {}; }
  const allowed = new Set(String(env.CMAIL_MAILBOXES || '').toLowerCase().split(',').map((v) => v.trim()));
  return Object.fromEntries(Object.entries(configured).filter(([address, item]) => allowed.has(address) && EMAIL.test(address) &&
    item && ['primary', 'support'].includes(item.key) && addresses(item.from).length === 1 && addresses(item.from)[0] === address && !/[\r\n]/.test(item.from)));
}

export async function prepareReply(queue, id) {
  if (!MAIL_ID.test(id || '')) throw fail('invalid_message');
  const record = await queue.state.storage.get(`mail:${id}`);
  if (!record) throw fail('message_not_found', 404);
  const payload = await queue.loadPayload(record);
  const parsed = await PostalMime.parse(decodeRaw(payload.raw_base64));
  const identities = sendingIdentities(queue.env);
  const candidates = [...addresses(payload.envelope_to), ...emails(parsed.to), ...emails([parsed.from || {}])];
  const sender = candidates.find((address) => identities[address]);
  if (!sender) throw fail('sender_not_configured', 503);
  const identity = identities[sender];
  const outbound = payload.direction === 'outbound';
  const recipients = emails(outbound ? parsed.to : parsed.replyTo?.length ? parsed.replyTo : [parsed.from || {}]).filter((address) => address !== sender);
  if (!recipients.length || recipients.length > 10) throw fail('recipient_unavailable');
  const subject = String(parsed.subject || '(No subject)').replace(/[\r\n]/g, ' ').trim();
  if (subject.length > 1000) throw fail('subject_too_long');
  const originalId = headerIds(parsed.messageId)[0] || '';
  const references = [...new Set([...headerIds(parsed.references), ...headerIds(parsed.inReplyTo), ...(originalId ? [originalId] : [])])].slice(-20);
  return { id, from: identity.from, sender, to: recipients, subject: /^re:/i.test(subject) ? subject : `Re: ${subject}`,
    signature: String(identity.signature || ''), in_reply_to: originalId, references, credential: identity.key };
}

function encodedSubject(subject) {
  const words = []; let chunk = '';
  for (const character of subject) {
    if (new TextEncoder().encode(chunk + character).length > 42) { words.push(chunk); chunk = ''; }
    chunk += character;
  }
  if (chunk) words.push(chunk);
  return words.map((word) => `=?UTF-8?B?${encodeRaw(new TextEncoder().encode(word))}?=`).join('\r\n ');
}

function makeMessage(prepared, text, requestId, createdAt) {
  const messageId = `<cmail.${requestId}@${prepared.sender.split('@')[1]}>`;
  const headers = { 'Message-ID': messageId };
  if (prepared.in_reply_to) headers['In-Reply-To'] = prepared.in_reply_to;
  if (prepared.references.length) headers.References = prepared.references.join(' ');
  const request = { from: prepared.from, to: prepared.to, subject: prepared.subject, text, reply_to: prepared.sender, headers };
  const body64 = encodeRaw(new TextEncoder().encode(text)).match(/.{1,76}/g)?.join('\r\n') || '';
  const lines = [`From: ${prepared.from}`, `To: ${prepared.to.join(', ')}`, `Reply-To: ${prepared.sender}`,
    `Subject: ${encodedSubject(prepared.subject)}`, `Date: ${new Date(createdAt).toUTCString()}`,
    ...Object.entries(headers).map(([key, value]) => `${key}: ${key === 'References' ? value.split(' ').join('\r\n ') : value}`), 'MIME-Version: 1.0',
    'Content-Type: text/plain; charset=utf-8', 'Content-Transfer-Encoding: base64', '', body64, ''];
  return { request, credential: prepared.credential, archive: { provider: 'resend-cloud-reply', external_id: `cmail-reply:${requestId}`,
    envelope_from: prepared.sender, envelope_to: prepared.sender, direction: 'outbound', received_at: createdAt,
    raw_base64: encodeRaw(new TextEncoder().encode(lines.join('\r\n'))), reply_to_archive_id: prepared.id } };
}

function receipt(record) {
  return { request_id: record.request_id, state: record.state, archive_id: record.archive_id || null,
    error: record.error || null, can_edit: record.state === 'rejected', can_retry: ['uncertain', 'accepted', 'prepared'].includes(record.state) };
}

async function archiveSent(queue, key, record, saved) {
  try {
    const imported = await queue.enqueue({ ...saved.archive, provider_id: record.provider_id }, { archiveOnly: true, read: true, archived: true });
    record = { ...record, state: 'sent', archive_id: imported.id, error: null };
    await queue.state.storage.put(key, record);
  } catch {
    record = { ...record, state: 'accepted', error: 'archive_pending' };
    await queue.state.storage.put(key, record);
  }
  return receipt(record);
}

export async function sendReply(queue, input, { fetcher = fetch, now = Date.now } = {}) {
  if (!MAIL_ID.test(input.id || '') || !REQUEST_ID.test(input.request_id || '') || typeof input.text !== 'string' || !input.text.trim() || input.text.length > 100000 || input.text.includes('\0')) throw fail('invalid_reply');
  const key = OUTBOX + input.request_id;
  const digest = await sha256Hex(new TextEncoder().encode(JSON.stringify([input.id, input.text])));
  let record = await queue.state.storage.get(key);
  if (record && (record.message_id !== input.id || record.digest !== digest)) throw fail('reply_changed', 409);
  if (record?.state === 'sent' || record?.state === 'rejected') return receipt(record);
  let saved;
  if (record) {
    saved = await queue.loadPayload(record);
  } else {
    const prepared = await prepareReply(queue, input.id);
    if (input.text.trim() === prepared.signature.trim()) throw fail('empty_reply');
    const createdAt = new Date(now()).toISOString();
    saved = makeMessage(prepared, input.text, input.request_id, createdAt);
    const envelope = await encryptArchivePayload({ masterKey: queue.env.LIFE_OPS_MASTER_KEY, purpose: 'cloudflare-mail-archive-v1', payload: saved,
      metadata: { id: `reply_${input.request_id}`, created_at: createdAt } });
    const payloadRef = await storeArchiveEnvelope({ storage: queue.state.storage, id: `reply_${input.request_id}`, envelope });
    record = { request_id: input.request_id, message_id: input.id, digest, created_at: createdAt, state: 'prepared', payload_ref: payloadRef };
    await queue.state.storage.put(key, record);
  }
  if (record.provider_id) return archiveSent(queue, key, record, saved);
  // Resend retains its idempotency keys for 24 hours. An ambiguous older send
  // needs review; never turn a timed-out retry into a second delivery.
  if (now() - Date.parse(record.created_at) >= 23 * 60 * 60 * 1000) throw fail('confirmation_expired', 409);
  const apiKey = saved.credential === 'support' ? queue.env.CMAIL_SUPPORT_SEND_KEY : queue.env.CMAIL_RESEND_API_KEY;
  if (!apiKey) throw fail('sender_not_configured', 503);
  let response;
  try {
    record = { ...record, state: 'uncertain', error: 'send_unconfirmed' };
    await queue.state.storage.put(key, record);
    response = await fetcher('https://api.resend.com/emails', {
      method: 'POST', redirect: 'manual', signal: AbortSignal.timeout(25000),
      headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json', 'Idempotency-Key': `cmail-reply/${input.request_id}` },
      body: JSON.stringify(saved.request),
    });
    const result = await response.json();
    if (!response.ok || !/^[a-f0-9-]{36}$/.test(result?.id || '')) {
      const rejected = [400, 401, 403, 404, 405, 413, 422, 429].includes(response.status);
      record = { ...record, state: rejected ? 'rejected' : 'uncertain', error: rejected ? (response.status === 429 ? 'send_rate_limited' : 'send_rejected') : 'send_unconfirmed' };
      await queue.state.storage.put(key, record);
      return receipt(record);
    }
    record = { ...record, state: 'accepted', provider_id: result.id, error: null };
    await queue.state.storage.put(key, record);
  } catch {
    // Keep the exact encrypted request and idempotency key for user-driven retry.
    return receipt(record);
  }
  return archiveSent(queue, key, record, saved);
}

export async function handleReply(queue, request) {
  if (request.method !== 'POST') return json({ error: 'method_not_allowed' }, 405);
  try {
    const input = await request.json();
    const path = new URL(request.url).pathname;
    if (path === '/reply/prepare') {
      const { credential, ...prepared } = await prepareReply(queue, input.id);
      return json(prepared);
    }
    if (path === '/reply/status') {
      if (!REQUEST_ID.test(input.request_id || '')) throw fail('invalid_reply');
      const record = await queue.state.storage.get(OUTBOX + input.request_id);
      return record ? json(receipt(record)) : json({ state: 'unsent' });
    }
    if (path === '/reply/send') {
      // Serialize request preparation too, so concurrent double-clicks cannot
      // construct different dates/bodies for one provider idempotency key.
      const previous = queue.replyTail || Promise.resolve();
      const work = previous.catch(() => {}).then(() => sendReply(queue, input));
      queue.replyTail = work.catch(() => {});
      return json(await work);
    }
    return json({ error: 'not_found' }, 404);
  } catch (error) {
    return json({ error: error.status ? error.message : 'reply_unavailable' }, error.status || 503);
  }
}
