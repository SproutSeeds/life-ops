const MAIL_QUEUE_OBJECT = "primary";
const MAX_SIGNATURE_SKEW_SECONDS = 300;
const DEFAULT_PULL_LIMIT = 25;
const MAX_PULL_LIMIT = 100;
const MAX_ARCHIVE_CHUNK_CHARS = 90000;

export default {
  async email(message, env, ctx) {
    const headers = Object.fromEntries(message.headers);
    const rawBytes = new Uint8Array(await new Response(message.raw).arrayBuffer());
    const payloadHash = await sha256Hex(rawBytes);
    let binary = "";
    const chunkSize = 0x8000;
    for (let index = 0; index < rawBytes.length; index += chunkSize) {
      binary += String.fromCharCode(...rawBytes.subarray(index, index + chunkSize));
    }

    const payload = {
      provider: "cloudflare-email-routing",
      worker: "life-ops-email-ingest",
      received_at: new Date().toISOString(),
      envelope_from: message.from,
      envelope_to: message.to,
      headers,
      raw_base64: btoa(binary),
      raw_size: message.rawSize,
      payload_hash: payloadHash,
    };

    const queue = mailQueueStub(env);
    const enqueueResponse = await queue.fetch("https://mail-queue.internal/enqueue", {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    if (!enqueueResponse.ok) {
      const reason = await enqueueResponse.text();
      throw new Error(`mail enqueue failed: ${enqueueResponse.status} ${reason}`);
    }

    if (env.FORWARD_TO) {
      await message.forward(env.FORWARD_TO, new Headers({
        "X-Life-Ops-Envelope-To": message.to,
        "X-Life-Ops-Worker": "life-ops-email-ingest",
      }));
    }
  },

  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/healthz") {
      return jsonResponse({
        ok: true,
        worker: "life-ops-email-ingest",
        queue_mode: "encrypted_durable_queue",
        forwarding_enabled: Boolean(env.FORWARD_TO),
        archive_encryption_enabled: Boolean(env.LIFE_OPS_MASTER_KEY),
      });
    }

    if (!url.pathname.startsWith("/api/mail/queue/")) {
      return jsonResponse({ error: "not_found" }, 404);
    }
    if (request.method !== "POST") {
      return jsonResponse({ error: "method_not_allowed" }, 405);
    }
    if (!env.LIFE_OPS_MAIL_INGEST_SECRET) {
      return jsonResponse({ error: "missing_ingest_secret" }, 500);
    }
    if (!env.LIFE_OPS_MASTER_KEY) {
      return jsonResponse({ error: "missing_master_key" }, 500);
    }

    const rawBody = await request.text();
    const verified = await verifyLifeOpsRequest({
      secret: env.LIFE_OPS_MAIL_INGEST_SECRET,
      timestamp: request.headers.get("X-Life-Ops-Timestamp") || "",
      signature: request.headers.get("X-Life-Ops-Signature") || "",
      body: rawBody,
      maxSkewSeconds: MAX_SIGNATURE_SKEW_SECONDS,
    });
    if (!verified.ok) {
      return jsonResponse({ error: verified.reason }, 401);
    }

    const queue = mailQueueStub(env);
    let queuePath = "";
    if (url.pathname === "/api/mail/queue/inject") {
      queuePath = "/enqueue";
    } else if (url.pathname === "/api/mail/queue/pull") {
      queuePath = "/pull";
    } else if (url.pathname === "/api/mail/queue/ack") {
      queuePath = "/ack";
    } else if (url.pathname === "/api/mail/queue/status") {
      queuePath = "/status";
    } else {
      return jsonResponse({ error: "not_found" }, 404);
    }

    return queue.fetch(`https://mail-queue.internal${queuePath}`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: rawBody,
    });
  },
};

export class MailQueue {
  constructor(state, env) {
    this.state = state;
    this.env = env;
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (request.method !== "POST") {
      return jsonResponse({ error: "method_not_allowed" }, 405);
    }
    const payload = await request.json().catch(() => ({}));
    if (url.pathname === "/enqueue") {
      return jsonResponse(await this.enqueue(payload));
    }
    if (url.pathname === "/pull") {
      return jsonResponse(await this.pull(payload));
    }
    if (url.pathname === "/ack") {
      return jsonResponse(await this.ack(payload));
    }
    if (url.pathname === "/status") {
      return jsonResponse(await this.status());
    }
    return jsonResponse({ error: "not_found" }, 404);
  }

  async enqueue(payload) {
    const fingerprint = await fingerprintPayload(payload);
    const dedupeKey = `dedupe:${fingerprint}`;
    const existingId = await this.state.storage.get(dedupeKey);
    if (existingId) {
      return {
        stored: false,
        duplicate: true,
        id: existingId,
        pending_count: await this.pendingCount(),
      };
    }

    const meta = (await this.state.storage.get("meta")) || {
      next_seq: 0,
      total_stored: 0,
      total_acknowledged: 0,
    };
    const seq = Number(meta.next_seq || 0) + 1;
    const id = `mail_${String(seq).padStart(16, "0")}`;
    const receivedAt = cleanString(payload.received_at) || new Date().toISOString();
    const encryptedPayload = await encryptArchivePayload({
      masterKey: this.env.LIFE_OPS_MASTER_KEY,
      purpose: "cloudflare-mail-archive-v1",
      payload,
      metadata: {
        id,
        seq,
        fingerprint,
        received_at: receivedAt,
      },
    });
    const payloadRef = await storeArchiveEnvelope({
      storage: this.state.storage,
      id,
      envelope: encryptedPayload,
    });
    const record = {
      id,
      seq,
      fingerprint,
      received_at: receivedAt,
      acknowledged: false,
      acknowledged_at: null,
      payload_ref: payloadRef,
    };

    await this.state.storage.put(`mail:${id}`, record);
    await this.state.storage.put(`queue:${padSeq(seq)}`, id);
    await this.state.storage.put(dedupeKey, id);

    meta.next_seq = seq;
    meta.total_stored = Number(meta.total_stored || 0) + 1;
    await this.state.storage.put("meta", meta);

    return {
      stored: true,
      duplicate: false,
      id,
      seq,
      pending_count: await this.pendingCount(),
      total_stored: meta.total_stored,
    };
  }

  async pull(payload) {
    const limit = clampLimit(payload.limit);
    const entries = await this.state.storage.list({ prefix: "queue:", limit });
    const items = [];
    for (const [queueKey, itemId] of entries) {
      const record = await this.state.storage.get(`mail:${itemId}`);
      if (!record) {
        await this.state.storage.delete(queueKey);
        continue;
      }
      let payload = record.payload || null;
      if (!payload && record.payload_encrypted) {
        payload = await decryptArchivePayload({
          masterKey: this.env.LIFE_OPS_MASTER_KEY,
          purpose: "cloudflare-mail-archive-v1",
          envelope: record.payload_encrypted,
        });
      }
      if (!payload && record.payload_ref) {
        const archivedEnvelope = await loadArchiveEnvelope({
          storage: this.state.storage,
          ref: record.payload_ref,
        });
        payload = await decryptArchivePayload({
          masterKey: this.env.LIFE_OPS_MASTER_KEY,
          purpose: "cloudflare-mail-archive-v1",
          envelope: archivedEnvelope,
        });
      }
      items.push({
        id: record.id,
        seq: record.seq,
        received_at: record.received_at,
        payload_hash: record.fingerprint,
        payload,
      });
    }
    const meta = (await this.state.storage.get("meta")) || {};
    return {
      items,
      pulled_count: items.length,
      pending_count: await this.pendingCount(),
      total_stored: Number(meta.total_stored || 0),
      total_acknowledged: Number(meta.total_acknowledged || 0),
      forwarding_enabled: Boolean(this.env.FORWARD_TO),
    };
  }

  async ack(payload) {
    const ids = Array.isArray(payload.ids)
      ? payload.ids.map((value) => cleanString(value)).filter(Boolean)
      : [];
    const meta = (await this.state.storage.get("meta")) || {
      next_seq: 0,
      total_stored: 0,
      total_acknowledged: 0,
    };
    let acknowledgedCount = 0;
    const missingIds = [];

    for (const id of ids) {
      const recordKey = `mail:${id}`;
      const record = await this.state.storage.get(recordKey);
      if (!record) {
        missingIds.push(id);
        continue;
      }
      if (!record.acknowledged) {
        record.acknowledged = true;
        record.acknowledged_at = new Date().toISOString();
        await this.state.storage.put(recordKey, record);
        await this.state.storage.delete(`queue:${padSeq(record.seq)}`);
        acknowledgedCount += 1;
      }
    }

    meta.total_acknowledged = Number(meta.total_acknowledged || 0) + acknowledgedCount;
    await this.state.storage.put("meta", meta);

    return {
      acknowledged_count: acknowledgedCount,
      missing_ids: missingIds,
      pending_count: await this.pendingCount(),
      total_stored: Number(meta.total_stored || 0),
      total_acknowledged: Number(meta.total_acknowledged || 0),
    };
  }

  async status() {
    const meta = (await this.state.storage.get("meta")) || {
      next_seq: 0,
      total_stored: 0,
      total_acknowledged: 0,
    };
    return {
      pending_count: await this.pendingCount(),
      total_stored: Number(meta.total_stored || 0),
      total_acknowledged: Number(meta.total_acknowledged || 0),
      worker: "life-ops-email-ingest",
      forwarding_enabled: Boolean(this.env.FORWARD_TO),
      cloud_backup_mode: "encrypted_durable_queue",
      archive_encryption_enabled: Boolean(this.env.LIFE_OPS_MASTER_KEY),
    };
  }

  async pendingCount() {
    const entries = await this.state.storage.list({ prefix: "queue:" });
    return entries.size;
  }
}

function mailQueueStub(env) {
  return env.MAIL_QUEUE.getByName(MAIL_QUEUE_OBJECT);
}

function cleanString(value) {
  return String(value || "").trim();
}

function padSeq(value) {
  return String(value).padStart(16, "0");
}

function clampLimit(rawValue) {
  const numeric = Number(rawValue || DEFAULT_PULL_LIMIT);
  if (!Number.isFinite(numeric)) {
    return DEFAULT_PULL_LIMIT;
  }
  return Math.max(1, Math.min(MAX_PULL_LIMIT, Math.trunc(numeric)));
}

async function fingerprintPayload(payload) {
  const direct = cleanString(payload.payload_hash);
  if (direct) {
    return direct;
  }
  const fallback = cleanString(payload.raw_base64) || JSON.stringify(payload || {});
  return sha256Hex(new TextEncoder().encode(fallback));
}

async function sha256Hex(bytes) {
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)]
    .map((value) => value.toString(16).padStart(2, "0"))
    .join("");
}

async function verifyLifeOpsRequest({ secret, timestamp, signature, body, maxSkewSeconds }) {
  if (!cleanString(timestamp)) {
    return { ok: false, reason: "missing_timestamp" };
  }
  if (!cleanString(signature)) {
    return { ok: false, reason: "missing_signature" };
  }
  const signedAt = new Date(timestamp);
  if (Number.isNaN(signedAt.getTime())) {
    return { ok: false, reason: "invalid_timestamp" };
  }
  const skewSeconds = Math.abs(Date.now() - signedAt.getTime()) / 1000;
  if (skewSeconds > maxSkewSeconds) {
    return { ok: false, reason: "stale_timestamp" };
  }
  const expected = await signLifeOpsPayload(secret, timestamp, body);
  if (!(await timingSafeEqual(expected, signature))) {
    return { ok: false, reason: "invalid_signature" };
  }
  return { ok: true, reason: "ok" };
}

async function signLifeOpsPayload(secret, timestamp, body) {
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const bytes = encoder.encode(`${timestamp}.${body}`);
  const signature = await crypto.subtle.sign("HMAC", key, bytes);
  const digest = [...new Uint8Array(signature)]
    .map((value) => value.toString(16).padStart(2, "0"))
    .join("");
  return `sha256=${digest}`;
}

async function timingSafeEqual(left, right) {
  const encoder = new TextEncoder();
  const [leftDigest, rightDigest] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(String(left || ""))),
    crypto.subtle.digest("SHA-256", encoder.encode(String(right || ""))),
  ]);
  const a = new Uint8Array(leftDigest);
  const b = new Uint8Array(rightDigest);
  let mismatch = 0;
  for (let index = 0; index < a.length; index += 1) {
    mismatch |= a[index] ^ b[index];
  }
  return mismatch === 0;
}

function b64urlEncode(bytes) {
  let binary = "";
  const chunkSize = 0x8000;
  for (let index = 0; index < bytes.length; index += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(index, index + chunkSize));
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function b64urlDecode(value) {
  const clean = cleanString(value);
  const padded = clean + "=".repeat((4 - (clean.length % 4)) % 4);
  const binary = atob(padded.replace(/-/g, "+").replace(/_/g, "/"));
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes;
}

async function deriveArchiveKey(masterKey, purpose) {
  const material = await crypto.subtle.importKey(
    "raw",
    b64urlDecode(masterKey),
    "HKDF",
    false,
    ["deriveKey"]
  );
  return crypto.subtle.deriveKey(
    {
      name: "HKDF",
      hash: "SHA-256",
      salt: new TextEncoder().encode("life-ops-hkdf-salt-v1"),
      info: new TextEncoder().encode(cleanString(purpose)),
    },
    material,
    { name: "AES-GCM", length: 256 },
    false,
    ["encrypt", "decrypt"]
  );
}

async function encryptArchivePayload({ masterKey, purpose, payload, metadata }) {
  const aad = {
    purpose: cleanString(purpose),
    version: 1,
    created_at: new Date().toISOString(),
    metadata: metadata || {},
  };
  const aadBytes = new TextEncoder().encode(JSON.stringify(aad));
  const plaintext = new TextEncoder().encode(JSON.stringify(payload || {}));
  const nonce = crypto.getRandomValues(new Uint8Array(12));
  const key = await deriveArchiveKey(masterKey, purpose);
  const ciphertext = new Uint8Array(
    await crypto.subtle.encrypt({ name: "AES-GCM", iv: nonce, additionalData: aadBytes }, key, plaintext)
  );
  return {
    version: 1,
    alg: "AES-256-GCM",
    kdf: "HKDF-SHA256",
    purpose: cleanString(purpose),
    nonce_b64: b64urlEncode(nonce),
    aad_b64: b64urlEncode(aadBytes),
    ciphertext_b64: b64urlEncode(ciphertext),
  };
}

async function decryptArchivePayload({ masterKey, purpose, envelope }) {
  const key = await deriveArchiveKey(masterKey, purpose);
  const plaintext = await crypto.subtle.decrypt(
    {
      name: "AES-GCM",
      iv: b64urlDecode(envelope.nonce_b64),
      additionalData: b64urlDecode(envelope.aad_b64),
    },
    key,
    b64urlDecode(envelope.ciphertext_b64)
  );
  return JSON.parse(new TextDecoder().decode(plaintext));
}

async function storeArchiveEnvelope({ storage, id, envelope }) {
  const serialized = JSON.stringify(envelope || {});
  const chunkCount = Math.max(1, Math.ceil(serialized.length / MAX_ARCHIVE_CHUNK_CHARS));
  const prefix = `archive:${id}:chunk:`;
  for (let index = 0; index < chunkCount; index += 1) {
    const start = index * MAX_ARCHIVE_CHUNK_CHARS;
    const end = start + MAX_ARCHIVE_CHUNK_CHARS;
    await storage.put(`${prefix}${String(index).padStart(4, "0")}`, serialized.slice(start, end));
  }
  return {
    version: 1,
    kind: "chunked_json",
    prefix,
    chunk_count: chunkCount,
  };
}

async function loadArchiveEnvelope({ storage, ref }) {
  if (!ref || ref.kind !== "chunked_json") {
    return ref || {};
  }
  const chunkCount = Number(ref.chunk_count || 0);
  if (!Number.isFinite(chunkCount) || chunkCount < 1) {
    throw new Error("invalid_archive_chunk_count");
  }
  let serialized = "";
  for (let index = 0; index < chunkCount; index += 1) {
    const chunkKey = `${ref.prefix}${String(index).padStart(4, "0")}`;
    const chunk = await storage.get(chunkKey);
    if (typeof chunk !== "string") {
      throw new Error(`missing_archive_chunk:${chunkKey}`);
    }
    serialized += chunk;
  }
  return JSON.parse(serialized || "{}");
}

function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      "content-type": "application/json",
    },
  });
}

// Shared only with the authenticated archive reader; no additional public routes.
export { decryptArchivePayload, loadArchiveEnvelope };
