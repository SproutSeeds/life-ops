import PostalMime, { decodeWords } from 'postal-mime';
import { MailQueue as LegacyMailQueue, decryptArchivePayload, loadArchiveEnvelope } from './legacy-worker.mjs';

const RECORD_PREFIX = 'mail:';
const ID_PATTERN = /^mail_\d{16}$/;
const SCAN_LIMIT = 40;
const PAGE_LIMIT = 20;
const READ_PREFIX = 'cmail-read:';
const ARCHIVED_PREFIX = 'cmail-archived:';
const ARCHIVED_COUNT = 'cmail-archived-count';
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
    if (!['/archive/list', '/archive/message', '/archive/read-status', '/archive/actions', '/archive/thread-index'].includes(path)) return super.fetch(request);
    if (request.method !== 'POST') return Response.json({ error: 'method_not_allowed' }, { status: 405 });
    try {
      const parameters = await request.json();
      const result = path === '/archive/list'
        ? await this.listArchive(parameters)
        : path === '/archive/read-status' ? await this.setReadStatus(parameters)
        : path === '/archive/actions' ? await this.applyAction(parameters)
        : path === '/archive/thread-index' ? await this.listThreadIndex(parameters)
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

  async listThreadIndex({ cursor = '' } = {}) {
    if (typeof cursor !== 'string' || (cursor && !ID_PATTERN.test(cursor))) throw new ArchiveInputError();
    const options = { prefix: RECORD_PREFIX, reverse: true, limit: SCAN_LIMIT + 1 };
    if (cursor) options.end = `${RECORD_PREFIX}${cursor}`;
    const entries = [...(await this.state.storage.list(options)).values()];
    const items = []; let scanned = 0; let failed = 0; let lastId = null;
    for (const record of entries.slice(0, SCAN_LIMIT)) {
      scanned += 1; lastId = record.id;
      try {
        const payload = await this.loadPayload(record);
        // Production captures all envelope headers. Older imports may keep
        // only a subset, so parse the original MIME message when necessary.
        const parsed = header(payload, 'message-id') ? null : await PostalMime.parse(fromBase64(payload.raw_base64));
        items.push({
          id: record.id, received_at: record.received_at,
          date: parsed?.date || header(payload, 'date') || record.received_at,
          message_id: parsed?.messageId || header(payload, 'message-id'),
          in_reply_to: parsed?.inReplyTo || header(payload, 'in-reply-to'),
          references: parsed?.references || header(payload, 'references'),
        });
      } catch { failed += 1; }
    }
    const meta = await this.state.storage.get('meta') || {};
    return { items, scanned, failed, next_cursor: scanned < entries.length ? lastId : null, total_stored: Number(meta.total_stored || 0) };
  }

  async listArchive(parameters = {}) {
    const query = String(parameters.query || '').trim().toLowerCase();
    const mailbox = String(parameters.mailbox || '').trim().toLowerCase();
    const cursor = String(parameters.cursor || '');
    const after = String(parameters.after || '');
    const before = String(parameters.before || '');
    const view = String(parameters.view || 'inbox');
    const limit = parameters.limit === undefined || parameters.limit === '' ? PAGE_LIMIT : Number(parameters.limit);
    if (query.length > 200 || mailbox.length > 254 ||
        !['inbox', 'archive', 'all'].includes(view) ||
        !Number.isSafeInteger(limit) || limit < 1 || limit > 100 ||
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
        const archived = (await this.state.storage.get(`${ARCHIVED_PREFIX}${record.id}`)) === true;
        if ((view === 'inbox' && archived) || (view === 'archive' && !archived)) continue;
        const payload = await this.loadPayload(record);
        const item = {
          id: record.id,
          received_at: record.received_at,
          from: decodeWords(header(payload, 'from')) || String(payload.envelope_from || ''),
          to: decodeWords(header(payload, 'to')) || String(payload.envelope_to || ''),
          mailbox: String(payload.envelope_to || ''),
          subject: decodeWords(header(payload, 'subject')) || '(No subject)',
          read: (await this.state.storage.get(`${READ_PREFIX}${record.id}`)) === true,
          archived,
        };
        if (mailbox && item.mailbox.toLowerCase() !== mailbox) continue;
        if (query && ![item.subject, item.from, item.to, item.mailbox].some((value) => value.toLowerCase().includes(query))) {
          const parsed = await PostalMime.parse(fromBase64(payload.raw_base64));
          const text = `${parsed.text || ''}\n${plainTextFromHtml(parsed.html)}`;
          if (!text.toLowerCase().includes(query)) continue;
        }
        items.push(item);
        if (items.length === limit) break;
      } catch {
        // Surface incomplete reads while allowing intact messages to be recovered.
        failed += 1;
      }
    }
    const meta = await this.state.storage.get('meta') || {};
    const archivedCount = Number(await this.state.storage.get(ARCHIVED_COUNT) || 0);
    const totalStored = Number(meta.total_stored || 0);
    return {
      items,
      next_cursor: scanned < entries.length ? lastId : null,
      scanned,
      failed,
      total_stored: totalStored,
      total_view: view === 'all' ? totalStored : view === 'archive' ? archivedCount : Math.max(0, totalStored - archivedCount),
      read_only: true,
    };
  }

  async setReadStatus({ id, read } = {}) {
    if (!ID_PATTERN.test(String(id || '')) || typeof read !== 'boolean') throw new ArchiveInputError();
    if (!await this.state.storage.get(`${RECORD_PREFIX}${id}`)) return null;
    // Reading in C-mail is independent of desktop delivery acknowledgements.
    // Keep message contents, queue entries, and encryption metadata untouched.
    await this.state.storage.put(`${READ_PREFIX}${id}`, read);
    return { id, read };
  }

  async applyAction({ ids, action } = {}) {
    if (!Array.isArray(ids) || !ids.length || ids.length > 100 ||
        ids.some(id => typeof id !== 'string' || !ID_PATTERN.test(id)) ||
        new Set(ids).size !== ids.length || !['read', 'unread', 'archive', 'unarchive'].includes(action)) {
      throw new ArchiveInputError();
    }
    // Commit the complete selection and its count together. Never modify the
    // encrypted messages, delivery acknowledgements, or legacy queue metadata.
    return this.state.storage.transaction(async (tx) => {
      for (const id of ids) if (!await tx.get(`${RECORD_PREFIX}${id}`)) return null;
      let archivedCount = Number(await tx.get(ARCHIVED_COUNT) || 0);
      const items = [];
      for (const id of ids) {
        let read = (await tx.get(`${READ_PREFIX}${id}`)) === true;
        let archived = (await tx.get(`${ARCHIVED_PREFIX}${id}`)) === true;
        if (action === 'read' || action === 'unread') {
          read = action === 'read'; await tx.put(`${READ_PREFIX}${id}`, read);
        } else {
          const next = action === 'archive';
          if (next !== archived) archivedCount += next ? 1 : -1;
          archived = next; await tx.put(`${ARCHIVED_PREFIX}${id}`, archived);
        }
        items.push({ id, read, archived });
      }
      if (action === 'archive' || action === 'unarchive') await tx.put(ARCHIVED_COUNT, Math.max(0, archivedCount));
      return { items };
    });
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
      read: (await this.state.storage.get(`${READ_PREFIX}${id}`)) === true,
      archived: (await this.state.storage.get(`${ARCHIVED_PREFIX}${id}`)) === true,
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
      in_reply_to: parsed.inReplyTo || '',
      references: parsed.references || '',
      attachments: attachments.map((file, index) => ({
        index,
        filename: file.filename || `attachment-${index + 1}`,
        mime_type: file.mimeType || 'application/octet-stream',
        size: file.content.byteLength,
      })),
    };
  }
}
