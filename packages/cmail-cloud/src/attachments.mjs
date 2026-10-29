import { sha256Hex } from './production-base.mjs';
import { encodeRaw, decodeRaw } from './resend.mjs';

// Leave room for MIME/base64 overhead in the existing 30 MiB archive and the
// provider's 40 MB encoded-message limit. No file-type allowlist is imposed.
export const ATTACHMENT_LIMITS = Object.freeze({ max_bytes: 20 * 1024 * 1024, max_count: 50 });
// Resend's published sending restrictions, checked 2026-10-29.
export const BLOCKED_EXTENSIONS = 'adp app asp bas bat cer chm cmd com cpl crt csh der exe fxp gadget hlp hta inf ins isp its js jse ksh lib lnk mad maf mag mam maq mar mas mat mau mav maw mda mdb mde mdt mdw mdz msc msh msh1 msh2 mshxml msh1xml msh2xml msi msp mst ops pcd pif plg prf prg reg scf scr sct shb shs sys ps1 ps1xml ps2 ps2xml psc1 psc2 tmp url vb vbe vbs vps vsmacros vss vst vsw vxd ws wsc wsf wsh xnk'.split(' ');
const ID = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
const P = 'cmail-attachments-v1:';
const CHUNK = 48 * 1024;
const GRACE = 7 * 24 * 60 * 60 * 1000;
const bad = (message, status = 400) => Object.assign(new Error(message), { status });

export function attachmentMetadata(filename, contentType, size) {
  if (typeof filename !== 'string' || !filename.trim() || filename.length > 255 || /[\x00-\x1f\x7f/\\]/.test(filename)) throw bad('invalid_attachment_name');
  filename = filename.trim();
  if (BLOCKED_EXTENSIONS.includes(filename.replace(/[. ]+$/, '').split('.').pop().toLowerCase())) throw bad('attachment_type_blocked');
  if (!Number.isSafeInteger(size) || size < 1) throw bad('attachment_empty');
  if (size > ATTACHMENT_LIMITS.max_bytes) throw bad('attachments_too_large');
  const content_type = typeof contentType === 'string' && /^[a-z0-9!#$&^_.+-]+\/[a-z0-9!#$&^_.+-]+$/i.test(contentType) && contentType.length <= 150 ? contentType.toLowerCase() : 'application/octet-stream';
  return { filename, content_type, size };
}

export class AttachmentStore {
  constructor(queue) { this.queue = queue; this.storage = queue.state.storage; }
  async upload(id, filename, contentType, bytes) {
    if (!ID.test(id || '')) throw bad('invalid_attachment');
    const info = { id, ...attachmentMetadata(filename, contentType, bytes.length), sha256: await sha256Hex(bytes) };
    await this.storage.transaction(async (tx) => {
      const previous = await tx.get(P + id);
      if (previous) {
        if (JSON.stringify(await this.queue.views.unseal(previous)) !== JSON.stringify(info)) throw bad('attachment_changed', 409);
        return;
      }
      for (let offset = 0; offset < bytes.length; offset += CHUNK) {
        await tx.put(P + id + ':' + offset / CHUNK, await this.queue.views.seal({ content: encodeRaw(bytes.subarray(offset, offset + CHUNK)) }));
      }
      await tx.put(P + id, await this.queue.views.seal(info));
      const expires = Date.now() + GRACE;
      await tx.put(P + 'expiry:' + expires + ':' + id, id);
      await tx.put(P + 'expiry-for:' + id, expires);
    });
    return info;
  }
  async references(tx, owner, before = [], after = []) {
    const old = new Set(before.map((file) => file.id)), next = new Set(after.map((file) => file.id));
    for (const id of next) {
      if (!await tx.get(P + id)) throw bad('attachment_not_found', 404);
      await tx.put(P + 'ref:' + id + ':' + owner, true);
      const expires = await tx.get(P + 'expiry-for:' + id);
      if (expires) { await tx.delete(P + 'expiry:' + expires + ':' + id); await tx.delete(P + 'expiry-for:' + id); }
    }
    for (const id of old) if (!next.has(id)) {
      await tx.delete(P + 'ref:' + id + ':' + owner);
      if (!(await tx.list({ prefix: P + 'ref:' + id + ':', limit: 1 })).size) {
        const expires = Date.now() + GRACE;
        const previous = await tx.get(P + 'expiry-for:' + id);
        if (previous) await tx.delete(P + 'expiry:' + previous + ':' + id);
        await tx.put(P + 'expiry:' + expires + ':' + id, id);
        await tx.put(P + 'expiry-for:' + id, expires);
      }
    }
  }
  async cleanup(now = Date.now()) {
    // One expired upload per scheduled tick bounds work. Referenced drafts and
    // frozen outboxes are retained; a grace period protects stale-tab copies.
    await this.storage.transaction(async (tx) => {
      const rows = await tx.list({ prefix: P + 'expiry:', end: P + 'expiry:' + now + ':\uffff', limit: 1 });
      for (const [key, id] of rows) {
        if (!(await tx.list({ prefix: P + 'ref:' + id + ':', limit: 1 })).size) {
          const envelope = await tx.get(P + id);
          if (envelope) {
            const file = await this.queue.views.unseal(envelope);
            for (let n = 0; n < Math.ceil(file.size / CHUNK); n++) await tx.delete(P + id + ':' + n);
            await tx.delete(P + id);
          }
        }
        await tx.delete(key); await tx.delete(P + 'expiry-for:' + id);
      }
    });
  }
  async metadata(id) {
    if (!ID.test(id || '')) throw bad('invalid_attachment');
    const envelope = await this.storage.get(P + id);
    if (!envelope) throw bad('attachment_not_found', 404);
    return this.queue.views.unseal(envelope);
  }
  async validate(values = []) {
    if (!Array.isArray(values) || values.length > ATTACHMENT_LIMITS.max_count) throw bad('too_many_attachments');
    const files = [];
    for (const value of values) files.push(await this.metadata(value?.id));
    if (new Set(files.map((file) => file.id)).size !== files.length) throw bad('invalid_attachment');
    if (files.reduce((total, file) => total + file.size, 0) > ATTACHMENT_LIMITS.max_bytes) throw bad('attachments_too_large');
    return files;
  }
  async content(file) {
    const chunks = [];
    for (let n = 0; n < Math.ceil(file.size / CHUNK); n++) {
      const envelope = await this.storage.get(P + file.id + ':' + n);
      if (!envelope) throw bad('attachment_unavailable', 503);
      chunks.push((await this.queue.views.unseal(envelope)).content);
    }
    // CHUNK is divisible by three: only the final chunk has base64 padding.
    return chunks.join('');
  }
  async providerFiles(files) {
    const result = [];
    for (const file of files) result.push({ filename: file.filename, content_type: file.content_type, content: await this.content(file) });
    return result;
  }
}

function filenameParameter(filename) {
  const encoded = encodeURIComponent(filename).replace(/[!'()*]/g, (c) => '%' + c.charCodeAt(0).toString(16).toUpperCase());
  // RFC 2231 continuations keep Unicode and long names without long MIME lines.
  const tokens = encoded.match(/%[0-9A-F]{2}|./g) || [], parts = []; let part = '';
  for (const token of tokens) { if (part.length + token.length > 60) { parts.push(part); part = ''; } part += token; }
  if (part) parts.push(part);
  return parts.map((part, n) => `filename*${n}*=${n === 0 ? "UTF-8''" : ''}${part}`).join(';\r\n ');
}

export async function attachToArchive(queue, archive, files, requestId) {
  if (!files?.length) return archive;
  const raw = new TextDecoder().decode(decodeRaw(archive.raw_base64));
  const split = raw.indexOf('MIME-Version: 1.0\r\n') + 'MIME-Version: 1.0\r\n'.length;
  const boundary = 'cmail-mixed-' + requestId;
  const parts = [raw.slice(0, split), `Content-Type: multipart/mixed; boundary="${boundary}"\r\n\r\n--${boundary}\r\n`, raw.slice(split)];
  for (const file of files) {
    const content = await queue.attachments.content(file);
    parts.push(`\r\n--${boundary}\r\nContent-Type: ${file.content_type}\r\nContent-Disposition: attachment;\r\n ${filenameParameter(file.filename)}\r\nContent-Transfer-Encoding: base64\r\n\r\n`);
    for (let offset = 0; offset < content.length; offset += 76 * 512) parts.push(content.slice(offset, offset + 76 * 512).replace(/.{1,76}/g, '$&\r\n'));
  }
  parts.push(`--${boundary}--\r\n`);
  return { ...archive, raw_base64: encodeRaw(new TextEncoder().encode(parts.join(''))) };
}

export async function attachmentHttp(queue, request, id) {
  if (request.method === 'GET') {
    const file = await queue.attachments.metadata(id);
    const content = await queue.attachments.content(file);
    return new Response(decodeRaw(content), { headers: { 'Content-Type': 'application/octet-stream', 'Content-Disposition': "attachment; filename*=UTF-8''" + encodeURIComponent(file.filename).replace(/'/g, '%27'),
      'Cache-Control': 'private, no-store', 'X-Content-Type-Options': 'nosniff', 'Content-Security-Policy': "default-src 'none'; sandbox" } });
  }
  if (request.method !== 'POST') throw bad('method_not_allowed', 405);
  if (Number(request.headers.get('Content-Length')) > ATTACHMENT_LIMITS.max_bytes) throw bad('attachments_too_large', 413);
  let filename;
  try { filename = decodeURIComponent(request.headers.get('X-Cmail-Filename') || ''); } catch { throw bad('invalid_attachment_name'); }
  // Bound actual bytes even when Content-Length is missing or dishonest.
  const reader = request.body?.getReader(), chunks = []; let size = 0;
  if (!reader) throw bad('attachment_empty');
  try {
    while (true) {
      const { value, done } = await reader.read(); if (done) break;
      size += value.length;
      if (size > ATTACHMENT_LIMITS.max_bytes) { await reader.cancel(); throw bad('attachments_too_large', 413); }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(size); let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.length; }
  return Response.json({ attachment: await queue.attachments.upload(id, filename, request.headers.get('X-Cmail-Content-Type'), bytes) }, { headers: { 'Cache-Control': 'private, no-store' } });
}
