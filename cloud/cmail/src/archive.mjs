import PostalMime, { decodeWords } from 'postal-mime';
import { MailQueue as LegacyMailQueue, decryptArchivePayload, loadArchiveEnvelope } from './legacy-worker.mjs';

const RECORD_PREFIX = 'mail:';
const ID_PATTERN = /^mail_\d{16}$/;
const SCAN_LIMIT = 40;
const PAGE_LIMIT = 20;
const PURPOSE = 'cloudflare-mail-archive-v1';

function header(payload, name) {
  const item = Object.entries(payload.headers || {}).find(([key]) => key.toLowerCase() === name);
  return String(item?.[1] || '');
}

function fromBase64(value) {
  const binary = atob(value);
  return Uint8Array.from(binary, (character) => character.charCodeAt(0));
}

function toBase64(bytes) {
  let value = '';
  for (let offset = 0; offset < bytes.length; offset += 32768) {
    value += String.fromCharCode(...bytes.subarray(offset, offset + 32768));
  }
  return btoa(value);
}

function plainTextFromHtml(html) {
  return String(html || '')
    .replace(/<(script|style)\b[^>]*>[\s\S]*?<\/\1\s*>/gi, '')
    .replace(/<(?:br|\/p|\/div|\/tr|\/h[1-6])\b[^>]*>/gi, '\n')
    .replace(/<[^>]*>/g, '')
    .replace(/&(?:nbsp|amp|lt|gt|quot|apos);/g, (entity) => ({
      '&nbsp;': ' ', '&amp;': '&', '&lt;': '<', '&gt;': '>', '&quot;': '"', '&apos;': "'",
    })[entity])
    .replace(/\n{3,}/g, '\n\n').trim();
}

export class ArchiveInputError extends Error {}

export class MailQueue extends LegacyMailQueue {
  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (path !== '/archive/list' && path !== '/archive/message') return super.fetch(request);
    if (request.method !== 'POST') return Response.json({ error: 'method_not_allowed' }, { status: 405 });
    try {
      const parameters = await request.json();
      const result = path === '/archive/list'
        ? await this.listArchive(parameters)
        : await this.readArchiveMessage(parameters);
      return Response.json(result, { status: result ? 200 : 404 });
    } catch (error) {
      if (error instanceof ArchiveInputError || error instanceof SyntaxError) {
        return Response.json({ error: 'invalid_request' }, { status: 400 });
      }
      // Never put message contents, addresses, or encryption material in logs/errors.
      return Response.json({ error: 'archive_read_failed' }, { status: 503 });
    }
  }

  async loadPayload(record) {
    if (record.payload) return record.payload;
    const envelope = record.payload_encrypted || (record.payload_ref
      ? await loadArchiveEnvelope({ storage: this.state.storage, ref: record.payload_ref })
      : null);
    if (!envelope) throw new Error('missing_archive_payload');
    return decryptArchivePayload({ masterKey: this.env.LIFE_OPS_MASTER_KEY, purpose: PURPOSE, envelope });
  }

  async listArchive(parameters = {}) {
    const query = String(parameters.query || '').trim().toLowerCase();
    const mailbox = String(parameters.mailbox || '').trim().toLowerCase();
    const cursor = String(parameters.cursor || '');
    const after = String(parameters.after || '');
    const before = String(parameters.before || '');
    if (query.length > 200 || mailbox.length > 254 ||
        (cursor && !ID_PATTERN.test(cursor)) ||
        [after, before].some((value) => value && (!/^\d{4}-\d{2}-\d{2}$/.test(value) || Number.isNaN(Date.parse(value))))) {
      throw new ArchiveInputError();
    }
    const options = { prefix: RECORD_PREFIX, reverse: true, limit: SCAN_LIMIT + 1 };
    if (cursor) options.end = `${RECORD_PREFIX}${cursor}`;
    const records = await this.state.storage.list(options);
    const entries = [...records.entries()];
    const items = [];
    let scanned = 0;
    let failed = 0;
    let lastId = null;
    for (const [, record] of entries.slice(0, SCAN_LIMIT)) {
      scanned += 1;
      lastId = record.id;
      const date = String(record.received_at || '').slice(0, 10);
      if ((after && date < after) || (before && date > before)) continue;
      try {
        const payload = await this.loadPayload(record);
        const item = {
          id: record.id,
          received_at: record.received_at,
          from: decodeWords(header(payload, 'from')) || String(payload.envelope_from || ''),
          to: decodeWords(header(payload, 'to')) || String(payload.envelope_to || ''),
          mailbox: String(payload.envelope_to || ''),
          subject: decodeWords(header(payload, 'subject')) || '(No subject)',
        };
        if (mailbox && item.mailbox.toLowerCase() !== mailbox) continue;
        if (query && ![item.subject, item.from, item.to, item.mailbox].some((value) => value.toLowerCase().includes(query))) {
          const parsed = await PostalMime.parse(fromBase64(payload.raw_base64));
          const text = `${parsed.text || ''}\n${plainTextFromHtml(parsed.html)}`;
          if (!text.toLowerCase().includes(query)) continue;
        }
        items.push(item);
        if (items.length === PAGE_LIMIT) break;
      } catch {
        // Surface incomplete reads while allowing intact messages to be recovered.
        failed += 1;
      }
    }
    const meta = await this.state.storage.get('meta') || {};
    return {
      items,
      next_cursor: scanned < entries.length ? lastId : null,
      scanned,
      failed,
      total_stored: Number(meta.total_stored || 0),
      read_only: true,
    };
  }

  async readArchiveMessage({ id, attachment, raw = false } = {}) {
    if (!ID_PATTERN.test(String(id || ''))) throw new ArchiveInputError();
    const record = await this.state.storage.get(`${RECORD_PREFIX}${id}`);
    if (!record) return null;
    const payload = await this.loadPayload(record);
    if (raw) return { content_base64: payload.raw_base64, filename: `${id}.eml` };
    const parsed = await PostalMime.parse(fromBase64(payload.raw_base64));
    const attachments = parsed.attachments || [];
    if (attachment !== undefined) {
      if (!Number.isSafeInteger(attachment) || attachment < 0 || attachment >= attachments.length) throw new ArchiveInputError();
      const file = attachments[attachment];
      return {
        filename: file.filename || `attachment-${attachment + 1}`,
        content_base64: toBase64(new Uint8Array(file.content)),
      };
    }
    return {
      id,
      received_at: record.received_at,
      date: parsed.date || record.received_at,
      subject: parsed.subject || '(No subject)',
      from: parsed.from,
      to: parsed.to || [],
      cc: parsed.cc || [],
      reply_to: parsed.replyTo || [],
      mailbox: String(payload.envelope_to || ''),
      text: parsed.text || plainTextFromHtml(parsed.html) || '(This message has no text body.)',
      message_id: parsed.messageId || '',
      attachments: attachments.map((file, index) => ({
        index,
        filename: file.filename || `attachment-${index + 1}`,
        mime_type: file.mimeType || 'application/octet-stream',
        size: file.content.byteLength,
      })),
    };
  }
}
