import { sha256Hex } from './production-base.mjs';

export const SUPPORT_MAILBOX = 'support@bindmysoul.com';
export const MAX_RAW_BYTES = 30 * 1024 * 1024;
export const SOURCE_PREFIX = 'resend-source:';
export const SYNC_STATE = 'resend-sync:state';

export function addresses(value) {
  return String(Array.isArray(value) ? value.join(',') : value || '')
    .toLowerCase().match(/[^\s<>(),;"@]+@[^\s<>(),;"@]+/g) || [];
}

export function rawHeaders(raw) {
  // Header extraction leaves the complete original MIME untouched in raw_base64.
  const text = new TextDecoder().decode(raw.subarray(0, 256 * 1024));
  const boundary = text.search(/\r?\n\r?\n/);
  if (boundary < 0) throw new Error('invalid_mime_headers');
  const headers = {};
  for (const line of text.slice(0, boundary).replace(/\r?\n[\t ]+/g, ' ').split(/\r?\n/)) {
    const colon = line.indexOf(':');
    if (colon <= 0) continue;
    const key = line.slice(0, colon).trim().toLowerCase();
    const value = line.slice(colon + 1).trim();
    headers[key] = headers[key] ? `${headers[key]} ${value}` : value;
  }
  return headers;
}

export function encodeRaw(raw) {
  let binary = '';
  for (let offset = 0; offset < raw.length; offset += 32768) binary += String.fromCharCode(...raw.subarray(offset, offset + 32768));
  return btoa(binary);
}

export function decodeRaw(value) {
  if (typeof value !== 'string' || value.length > Math.ceil(MAX_RAW_BYTES / 3) * 4) throw new Error('invalid_raw_size');
  let binary;
  try { binary = atob(value); } catch { throw new Error('invalid_raw_base64'); }
  if (!binary.length || binary.length > MAX_RAW_BYTES) throw new Error('invalid_raw_size');
  return Uint8Array.from(binary, (c) => c.charCodeAt(0));
}

export async function normalizePayload(payload) {
  if (!payload || typeof payload !== 'object') throw new Error('invalid_payload');
  const raw = decodeRaw(payload.raw_base64);
  const hash = await sha256Hex(raw);
  if (payload.payload_hash && payload.payload_hash !== hash) throw new Error('raw_hash_mismatch');
  const headers = rawHeaders(raw);
  return { ...payload, headers, raw_size: raw.length, payload_hash: hash };
}

export function sourceKey(payload) {
  return payload.provider === 'resend-receiving' && /^resend-receiving:[a-f0-9-]{36}$/.test(payload.external_id || '')
    ? `${SOURCE_PREFIX}${payload.external_id}` : null;
}

export async function syncReceived(queue, env, { fetcher = fetch, sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)), now = Date.now, trigger = 'manual', verifyExisting = false } = {}) {
  const previous = await queue.state.storage.get(SYNC_STATE) || {};
  const state = { ...previous, status: 'running', trigger, started_at: new Date(now()).toISOString(), listed: 0, imported: 0, duplicates: 0, skipped: 0, pages: 0, error: null };
  await queue.state.storage.put(SYNC_STATE, state);
  let cursor = previous.cursor || '';
  let lastApiAt = 0;
  const api = async (path) => {
    const wait = 650 - (now() - lastApiAt);
    if (wait > 0) await sleep(wait);
    lastApiAt = now();
    const response = await fetcher(`https://api.resend.com${path}`, {
      headers: { Authorization: `Bearer ${env.RESEND_RECEIVING_API_KEY}`, 'User-Agent': 'C-mail receiving sync/1' },
      // workerd supports manual/follow only; reject redirects through !ok below.
      redirect: 'manual', signal: AbortSignal.timeout(20000),
    });
    if (!response.ok) throw new Error(`resend_http_${response.status}`);
    return response.json();
  };
  try {
    if (!env.RESEND_RECEIVING_API_KEY) throw new Error('missing_resend_receiving_key');
    let complete = false;
    for (let page = 0; page < 10 && !complete; page++) {
      const pageStart = cursor;
      const listing = await api(`/emails/receiving?limit=100${cursor ? `&after=${encodeURIComponent(cursor)}` : ''}`);
      if (!Array.isArray(listing.data)) throw new Error('invalid_resend_listing');
      if (listing.has_more && !listing.data.length) throw new Error('invalid_resend_cursor');
      state.pages += 1;
      for (const item of listing.data) {
        if (!/^[a-f0-9-]{36}$/.test(item.id || '')) throw new Error('invalid_resend_id');
        state.listed += 1;
        const externalId = `resend-receiving:${item.id}`;
        if (!verifyExisting && await queue.state.storage.get(`${SOURCE_PREFIX}${externalId}`)) {
          state.duplicates += 1;
        } else {
          const detail = await api(`/emails/receiving/${item.id}`);
          const recipients = addresses([...(detail.to || []), ...(detail.cc || []), ...(detail.bcc || []), detail.received_for || '']);
          if (!recipients.includes(SUPPORT_MAILBOX)) {
            state.skipped += 1;
          } else {
            const rawUrl = new URL(detail.raw?.download_url || '');
            if (rawUrl.protocol !== 'https:' || rawUrl.username || rawUrl.password) throw new Error('invalid_resend_raw_url');
            // Signed storage download gets no Resend credential or request headers.
            const response = await fetcher(rawUrl.href, { redirect: 'manual', signal: AbortSignal.timeout(30000) });
            if (!response.ok) throw new Error(`resend_raw_http_${response.status}`);
            const raw = new Uint8Array(await response.arrayBuffer());
            if (!raw.length || raw.length > MAX_RAW_BYTES) throw new Error('invalid_raw_size');
            const receivedAt = detail.created_at || item.created_at;
            if (!Number.isFinite(Date.parse(receivedAt))) throw new Error('invalid_received_at');
            const result = await queue.enqueue({
              provider: 'resend-receiving', external_id: externalId,
              envelope_from: detail.from || item.from || '', envelope_to: SUPPORT_MAILBOX,
              received_at: receivedAt, direction: 'inbound', raw_base64: encodeRaw(raw),
            }, { archiveOnly: true });
            if (result.stored) state.imported += 1;
            else state.duplicates += 1;
          }
        }
        cursor = item.id;
        // Save progress only after this message is durably stored or explicitly skipped.
        await queue.state.storage.put(SYNC_STATE, { ...state, cursor });
        if (state.imported >= 20) break;
      }
      const last = listing.data.at(-1)?.id || '';
      complete = !listing.has_more && (!listing.data.length || cursor === last);
      if (complete) cursor = '';
      else if (!cursor || cursor === pageStart) throw new Error('invalid_resend_cursor');
      if (state.imported >= 20) break;
    }
    Object.assign(state, { status: cursor ? 'catching_up' : 'healthy', cursor, completed_at: new Date(now()).toISOString(), last_success_at: new Date(now()).toISOString(), error: null });
    if (trigger === 'scheduled') state.last_scheduled_success_at = state.last_success_at;
  } catch (error) {
    // API bodies, raw URLs, MIME, and credentials never enter diagnostics.
    const known = /^(resend_(?:raw_)?http_\d{3}|missing_resend_receiving_key|invalid_[a-z_]+|raw_hash_mismatch)$/.test(error.message || '');
    Object.assign(state, { status: 'failed', cursor, completed_at: new Date(now()).toISOString(), error: known ? error.message : 'resend_sync_failed' });
  }
  await queue.state.storage.put(SYNC_STATE, state);
  return state;
}
