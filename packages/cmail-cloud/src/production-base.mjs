// src/legacy-worker.mjs
var MAIL_QUEUE_OBJECT = "primary";
var MAX_SIGNATURE_SKEW_SECONDS = 300;
var DEFAULT_PULL_LIMIT = 25;
var MAX_PULL_LIMIT = 100;
var MAX_ARCHIVE_CHUNK_CHARS = 9e4;
var legacy_worker_default = {
  async email(message2, env, ctx) {
    const headers = Object.fromEntries(message2.headers);
    const rawBytes = new Uint8Array(await new Response(message2.raw).arrayBuffer());
    const payloadHash = await sha256Hex(rawBytes);
    let binary = "";
    const chunkSize = 32768;
    for (let index = 0; index < rawBytes.length; index += chunkSize) {
      binary += String.fromCharCode(...rawBytes.subarray(index, index + chunkSize));
    }
    const payload = {
      provider: "cloudflare-email-routing",
      worker: "life-ops-email-ingest",
      received_at: (/* @__PURE__ */ new Date()).toISOString(),
      envelope_from: message2.from,
      envelope_to: message2.to,
      headers,
      raw_base64: btoa(binary),
      raw_size: message2.rawSize,
      payload_hash: payloadHash
    };
    const queue = mailQueueStub(env);
    const enqueueResponse = await queue.fetch("https://mail-queue.internal/enqueue", {
      method: "POST",
      headers: {
        "content-type": "application/json"
      },
      body: JSON.stringify(payload)
    });
    if (!enqueueResponse.ok) {
      const reason = await enqueueResponse.text();
      throw new Error(`mail enqueue failed: ${enqueueResponse.status} ${reason}`);
    }
    if (env.FORWARD_TO) {
      await message2.forward(env.FORWARD_TO, new Headers({
        "X-Life-Ops-Envelope-To": message2.to,
        "X-Life-Ops-Worker": "life-ops-email-ingest"
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
        archive_encryption_enabled: Boolean(env.LIFE_OPS_MASTER_KEY)
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
      maxSkewSeconds: MAX_SIGNATURE_SKEW_SECONDS
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
        "content-type": "application/json"
      },
      body: rawBody
    });
  }
};
var MailQueue = class {
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
        pending_count: await this.pendingCount()
      };
    }
    const meta = await this.state.storage.get("meta") || {
      next_seq: 0,
      total_stored: 0,
      total_acknowledged: 0
    };
    const seq = Number(meta.next_seq || 0) + 1;
    const id = `mail_${String(seq).padStart(16, "0")}`;
    const receivedAt = cleanString(payload.received_at) || (/* @__PURE__ */ new Date()).toISOString();
    const encryptedPayload = await encryptArchivePayload({
      masterKey: this.env.LIFE_OPS_MASTER_KEY,
      purpose: "cloudflare-mail-archive-v1",
      payload,
      metadata: {
        id,
        seq,
        fingerprint,
        received_at: receivedAt
      }
    });
    const payloadRef = await storeArchiveEnvelope({
      storage: this.state.storage,
      id,
      envelope: encryptedPayload
    });
    const record = {
      id,
      seq,
      fingerprint,
      received_at: receivedAt,
      acknowledged: false,
      acknowledged_at: null,
      payload_ref: payloadRef
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
      total_stored: meta.total_stored
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
      let payload2 = record.payload || null;
      if (!payload2 && record.payload_encrypted) {
        payload2 = await decryptArchivePayload({
          masterKey: this.env.LIFE_OPS_MASTER_KEY,
          purpose: "cloudflare-mail-archive-v1",
          envelope: record.payload_encrypted
        });
      }
      if (!payload2 && record.payload_ref) {
        const archivedEnvelope = await loadArchiveEnvelope({
          storage: this.state.storage,
          ref: record.payload_ref
        });
        payload2 = await decryptArchivePayload({
          masterKey: this.env.LIFE_OPS_MASTER_KEY,
          purpose: "cloudflare-mail-archive-v1",
          envelope: archivedEnvelope
        });
      }
      items.push({
        id: record.id,
        seq: record.seq,
        received_at: record.received_at,
        payload_hash: record.fingerprint,
        payload: payload2
      });
    }
    const meta = await this.state.storage.get("meta") || {};
    return {
      items,
      pulled_count: items.length,
      pending_count: await this.pendingCount(),
      total_stored: Number(meta.total_stored || 0),
      total_acknowledged: Number(meta.total_acknowledged || 0),
      forwarding_enabled: Boolean(this.env.FORWARD_TO)
    };
  }
  async ack(payload) {
    const ids = Array.isArray(payload.ids) ? payload.ids.map((value) => cleanString(value)).filter(Boolean) : [];
    const meta = await this.state.storage.get("meta") || {
      next_seq: 0,
      total_stored: 0,
      total_acknowledged: 0
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
        record.acknowledged_at = (/* @__PURE__ */ new Date()).toISOString();
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
      total_acknowledged: Number(meta.total_acknowledged || 0)
    };
  }
  async status() {
    const meta = await this.state.storage.get("meta") || {
      next_seq: 0,
      total_stored: 0,
      total_acknowledged: 0
    };
    return {
      pending_count: await this.pendingCount(),
      total_stored: Number(meta.total_stored || 0),
      total_acknowledged: Number(meta.total_acknowledged || 0),
      worker: "life-ops-email-ingest",
      forwarding_enabled: Boolean(this.env.FORWARD_TO),
      cloud_backup_mode: "encrypted_durable_queue",
      archive_encryption_enabled: Boolean(this.env.LIFE_OPS_MASTER_KEY)
    };
  }
  async pendingCount() {
    const entries = await this.state.storage.list({ prefix: "queue:" });
    return entries.size;
  }
};
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
async function sha256Hex(bytes2) {
  const digest = await crypto.subtle.digest("SHA-256", bytes2);
  return [...new Uint8Array(digest)].map((value) => value.toString(16).padStart(2, "0")).join("");
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
  const skewSeconds = Math.abs(Date.now() - signedAt.getTime()) / 1e3;
  if (skewSeconds > maxSkewSeconds) {
    return { ok: false, reason: "stale_timestamp" };
  }
  const expected = await signLifeOpsPayload(secret, timestamp, body);
  if (!await timingSafeEqual(expected, signature)) {
    return { ok: false, reason: "invalid_signature" };
  }
  return { ok: true, reason: "ok" };
}
async function signLifeOpsPayload(secret, timestamp, body) {
  const encoder2 = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    encoder2.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  const bytes2 = encoder2.encode(`${timestamp}.${body}`);
  const signature = await crypto.subtle.sign("HMAC", key, bytes2);
  const digest = [...new Uint8Array(signature)].map((value) => value.toString(16).padStart(2, "0")).join("");
  return `sha256=${digest}`;
}
async function timingSafeEqual(left, right) {
  const encoder2 = new TextEncoder();
  const [leftDigest, rightDigest] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder2.encode(String(left || ""))),
    crypto.subtle.digest("SHA-256", encoder2.encode(String(right || "")))
  ]);
  const a = new Uint8Array(leftDigest);
  const b = new Uint8Array(rightDigest);
  let mismatch = 0;
  for (let index = 0; index < a.length; index += 1) {
    mismatch |= a[index] ^ b[index];
  }
  return mismatch === 0;
}
function b64urlEncode(bytes2) {
  let binary = "";
  const chunkSize = 32768;
  for (let index = 0; index < bytes2.length; index += chunkSize) {
    binary += String.fromCharCode(...bytes2.subarray(index, index + chunkSize));
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}
function b64urlDecode(value) {
  const clean = cleanString(value);
  const padded = clean + "=".repeat((4 - clean.length % 4) % 4);
  const binary = atob(padded.replace(/-/g, "+").replace(/_/g, "/"));
  const bytes2 = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes2[index] = binary.charCodeAt(index);
  }
  return bytes2;
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
      info: new TextEncoder().encode(cleanString(purpose))
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
    created_at: (/* @__PURE__ */ new Date()).toISOString(),
    metadata: metadata || {}
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
    ciphertext_b64: b64urlEncode(ciphertext)
  };
}
async function decryptArchivePayload({ masterKey, purpose, envelope }) {
  const key = await deriveArchiveKey(masterKey, purpose);
  const plaintext = await crypto.subtle.decrypt(
    {
      name: "AES-GCM",
      iv: b64urlDecode(envelope.nonce_b64),
      additionalData: b64urlDecode(envelope.aad_b64)
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
    chunk_count: chunkCount
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
      "content-type": "application/json"
    }
  });
}

// node_modules/jose/dist/webapi/lib/buffer_utils.js
var encoder = new TextEncoder();
var decoder = new TextDecoder();
var strictDecoder = new TextDecoder("utf-8", { fatal: true });
var MAX_INT32 = 2 ** 32;
function concat(...buffers) {
  const size = buffers.reduce((acc, { length }) => acc + length, 0), buf = new Uint8Array(size);
  let i = 0;
  for (const buffer of buffers)
    buf.set(buffer, i), i += buffer.length;
  return buf;
}
var NON_ASCII = /[^\x00-\x7f]/;
function encode(string) {
  if (typeof string == "string" && string.length >= 128) {
    if (NON_ASCII.test(string))
      throw new TypeError("non-ASCII string encountered in encode()");
    return encoder.encode(string);
  }
  const bytes2 = new Uint8Array(string.length);
  for (let i = 0; i < string.length; i++) {
    const code = string.charCodeAt(i);
    if (code > 127)
      throw new TypeError("non-ASCII string encountered in encode()");
    bytes2[i] = code;
  }
  return bytes2;
}
function decodeBase64(encoded, url = false) {
  if (Uint8Array.fromBase64)
    return Uint8Array.fromBase64(encoded, { alphabet: url ? "base64url" : "base64" });
  if (url) {
    if (encoded.includes("+") || encoded.includes("/"))
      throw new TypeError("Invalid base64url");
    encoded = encoded.replace(/-/g, "+").replace(/_/g, "/");
  }
  const binary = atob(encoded), bytes2 = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++)
    bytes2[i] = binary.charCodeAt(i);
  return bytes2;
}

// node_modules/jose/dist/webapi/util/errors.js
var JOSEError = class extends Error {
  static code = "ERR_JOSE_GENERIC";
  code = "ERR_JOSE_GENERIC";
  constructor(message2, options) {
    super(message2, options), this.name = this.constructor.name, Error.captureStackTrace?.(this, this.constructor);
  }
};
var JWTClaimValidationFailed = class extends JOSEError {
  static code = "ERR_JWT_CLAIM_VALIDATION_FAILED";
  code = "ERR_JWT_CLAIM_VALIDATION_FAILED";
  claim;
  reason;
  payload;
  constructor(message2, payload, claim = "unspecified", reason = "unspecified") {
    super(message2, { cause: { claim, reason, payload } }), this.claim = claim, this.reason = reason, this.payload = payload;
  }
};
var JWTExpired = class extends JOSEError {
  static code = "ERR_JWT_EXPIRED";
  code = "ERR_JWT_EXPIRED";
  claim;
  reason;
  payload;
  constructor(message2, payload, claim = "unspecified", reason = "unspecified") {
    super(message2, { cause: { claim, reason, payload } }), this.claim = claim, this.reason = reason, this.payload = payload;
  }
};
var JOSEAlgNotAllowed = class extends JOSEError {
  static code = "ERR_JOSE_ALG_NOT_ALLOWED";
  code = "ERR_JOSE_ALG_NOT_ALLOWED";
};
var JOSENotSupported = class extends JOSEError {
  static code = "ERR_JOSE_NOT_SUPPORTED";
  code = "ERR_JOSE_NOT_SUPPORTED";
};
var JWSInvalid = class extends JOSEError {
  static code = "ERR_JWS_INVALID";
  code = "ERR_JWS_INVALID";
};
var JWTInvalid = class extends JOSEError {
  static code = "ERR_JWT_INVALID";
  code = "ERR_JWT_INVALID";
};
var JWKSInvalid = class extends JOSEError {
  static code = "ERR_JWKS_INVALID";
  code = "ERR_JWKS_INVALID";
};
var JWKSNoMatchingKey = class extends JOSEError {
  static code = "ERR_JWKS_NO_MATCHING_KEY";
  code = "ERR_JWKS_NO_MATCHING_KEY";
  constructor(message2 = "no applicable key found in the JSON Web Key Set", options) {
    super(message2, options);
  }
};
var JWKSMultipleMatchingKeys = class extends JOSEError {
  [Symbol.asyncIterator] = async function* () {
  };
  static code = "ERR_JWKS_MULTIPLE_MATCHING_KEYS";
  code = "ERR_JWKS_MULTIPLE_MATCHING_KEYS";
  constructor(message2 = "multiple matching keys found in the JSON Web Key Set", options) {
    super(message2, options);
  }
};
var JWKSTimeout = class extends JOSEError {
  static code = "ERR_JWKS_TIMEOUT";
  code = "ERR_JWKS_TIMEOUT";
  constructor(message2 = "request timed out", options) {
    super(message2, options);
  }
};
var JWSSignatureVerificationFailed = class extends JOSEError {
  static code = "ERR_JWS_SIGNATURE_VERIFICATION_FAILED";
  code = "ERR_JWS_SIGNATURE_VERIFICATION_FAILED";
  constructor(message2 = "signature verification failed", options) {
    super(message2, options);
  }
};

// node_modules/jose/dist/webapi/util/base64url.js
var invalid = "The input to be decoded is not correctly encoded.";
function decode(input) {
  try {
    return decodeBase64(typeof input == "string" ? input : decoder.decode(input), true);
  } catch (cause) {
    throw new TypeError(invalid, { cause });
  }
}

// node_modules/jose/dist/webapi/lib/validate.js
function isObject(input) {
  if (typeof input != "object" || input === null || Object.prototype.toString.call(input) !== "[object Object]")
    return false;
  const prototype = Object.getPrototypeOf(input);
  return prototype === null || Object.getPrototypeOf(prototype) === null;
}
function isJwkSet(input) {
  return isObject(input) && Array.isArray(input.keys) && Array.from(input.keys).every(isObject);
}
function isDisjoint(...headers) {
  const parameters = /* @__PURE__ */ new Set();
  for (const header2 of headers)
    if (header2)
      for (const parameter of Object.keys(header2)) {
        if (parameters.has(parameter))
          return false;
        parameters.add(parameter);
      }
  return true;
}
function decodeBase64url(value, label, ErrorClass) {
  try {
    return decode(value);
  } catch {
    throw new ErrorClass(`Failed to base64url decode the ${label}`);
  }
}
function encodeBase64url(value, label, ErrorClass) {
  try {
    return encode(value);
  } catch {
    throw new ErrorClass(`The ${label} is not a valid base64url string`);
  }
}
function parseJoseHeader(b64, ErrorClass, message2) {
  let parsed;
  try {
    parsed = JSON.parse(strictDecoder.decode(decode(b64)));
  } catch {
    throw new ErrorClass(message2);
  }
  if (!isObject(parsed))
    throw new ErrorClass(message2);
  return parsed;
}
var JWS_RECOGNIZED = { __proto__: null, b64: true };
function validateAlgorithms(option, algorithms) {
  if (algorithms !== void 0 && (!Array.isArray(algorithms) || algorithms.some((s) => typeof s != "string")))
    throw new TypeError(`"${option}" option must be an array of strings`);
  return algorithms === void 0 ? void 0 : new Set(algorithms);
}
function validateCrit(Err, recognizedDefault, recognizedOption, protectedHeader, joseHeader) {
  if (joseHeader.crit !== void 0 && protectedHeader?.crit === void 0)
    throw new Err('"crit" (Critical) Header Parameter MUST be integrity protected');
  if (!protectedHeader || protectedHeader.crit === void 0)
    return [];
  if (!Array.isArray(protectedHeader.crit) || protectedHeader.crit.length === 0 || protectedHeader.crit.some((input) => typeof input != "string" || input.length === 0))
    throw new Err('"crit" (Critical) Header Parameter MUST be an array of non-empty strings when present');
  const recognized = recognizedOption === void 0 ? recognizedDefault : { __proto__: null, ...recognizedOption, ...recognizedDefault };
  for (const parameter of protectedHeader.crit) {
    if (!(parameter in recognized))
      throw new JOSENotSupported(`Extension Header Parameter "${parameter}" is not recognized`);
    if (!Object.hasOwn(joseHeader, parameter) || joseHeader[parameter] === void 0)
      throw new Err(`Extension Header Parameter "${parameter}" is missing`);
    if (recognized[parameter] && (!Object.hasOwn(protectedHeader, parameter) || protectedHeader[parameter] === void 0))
      throw new Err(`Extension Header Parameter "${parameter}" MUST be integrity protected`);
  }
  return protectedHeader.crit;
}
function validateB64(protectedHeader, extensions) {
  if (extensions.includes("b64")) {
    const b64 = protectedHeader.b64;
    if (typeof b64 != "boolean")
      throw new JWSInvalid('The "b64" (base64url-encode payload) Header Parameter must be a boolean');
    return b64;
  }
  return true;
}

// node_modules/jose/dist/webapi/lib/key.js
var tag = (key) => key[Symbol.toStringTag];
var jwkMatchesOp = (entry, key, usage) => {
  const { alg } = entry;
  if (key.use !== void 0) {
    const expected = usage === "sign" || usage === "verify" ? "sig" : "enc";
    if (key.use !== expected)
      throw new TypeError(`Invalid key for this operation, its "use" must be "${expected}" when present`);
  }
  if (key.alg !== void 0 && key.alg !== alg)
    throw new TypeError(`Invalid key for this operation, its "alg" must be "${alg}" when present`);
  if (Array.isArray(key.key_ops)) {
    const expectedKeyOp = usage === "encrypt" || usage === "decrypt" ? entry.ops?.[usage === "encrypt" ? 0 : 1] : usage;
    if (expectedKeyOp && !key.key_ops.includes(expectedKeyOp))
      throw new TypeError(`Invalid key for this operation, its "key_ops" must include "${expectedKeyOp}" when present`);
  }
};
async function prepareKey(entry, key, usage) {
  const { alg, secret } = entry, privateKey = usage === "decrypt" || usage === "sign";
  if (secret && key instanceof Uint8Array)
    return key;
  let normalized, keyObject;
  if (isObject(key)) {
    if (normalized = normalizeJwk(key), typeof normalized.kty != "string")
      throw invalidKeyType(alg, key, secret);
    if (!(secret ? normalized.kty === "oct" && typeof normalized.k == "string" : normalized.kty !== "oct" && (privateKey ? normalized.kty === "AKP" && typeof normalized.priv == "string" || typeof normalized.d == "string" : normalized.d === void 0 && normalized.priv === void 0)))
      throw new TypeError(secret ? 'JSON Web Key for symmetric algorithms must have JWK "kty" (Key Type) equal to "oct" and the JWK "k" (Key Value) present' : `JSON Web Key for this operation must be a ${privateKey ? "private" : "public"} JWK`);
    if (jwkMatchesOp(entry, normalized, usage), normalized.kty === "oct")
      return decode(normalized.k);
    if (!Object.isFrozen(key)) {
      const { key_ops } = key;
      Array.isArray(key_ops) && Object.freeze(key_ops), Object.freeze(key);
    }
  } else {
    if (!isKeyLike(key))
      throw invalidKeyType(alg, key, secret);
    const expectedType = secret ? "secret" : privateKey ? "private" : "public";
    if (key.type !== expectedType && (secret || ["secret", "public", "private"].includes(key.type)))
      throw new TypeError(`${tag(key)} instances must be of type "${expectedType}" for the ${alg} algorithm`);
    if (isCryptoKey(key))
      return key;
    if (keyObject = key, keyObject.type === "secret")
      return keyObject.export();
  }
  cache ||= /* @__PURE__ */ new WeakMap();
  const cacheKey = key;
  let cached = cache.get(cacheKey);
  if (cached?.[alg])
    return cached[alg];
  if (cached || cache.set(cacheKey, cached = {}), keyObject && typeof keyObject.toCryptoKey == "function") {
    const isPublic = keyObject.type === "public", crv = nist[keyObject.asymmetricKeyDetails?.namedCurve], params = entry.resolve?.({ crv, asymmetricKeyType: keyObject.asymmetricKeyType }) ?? entry.subtle;
    return cached[alg] = keyObject.toCryptoKey(params, isPublic, entry.usages[isPublic ? 0 : 1]);
  }
  return normalized ??= keyObject.export({ format: "jwk" }), normalized.alg = alg, cached[alg] = await jwkToKey(entry, normalized);
}
var cache;
var nist = {
  __proto__: null,
  prime256v1: "P-256",
  secp384r1: "P-384",
  secp521r1: "P-521"
};
var isCryptoKey = (key) => {
  if (key?.[Symbol.toStringTag] === "CryptoKey")
    return true;
  try {
    return key instanceof CryptoKey;
  } catch {
    return false;
  }
};
var isKeyObject = (key) => key?.[Symbol.toStringTag] === "KeyObject";
var isKeyLike = (key) => isCryptoKey(key) || isKeyObject(key);
function message(msg, actual, ...types) {
  if (types.length > 2) {
    const last = types.pop();
    msg += `one of type ${types.join(", ")}, or ${last}.`;
  } else types.length === 2 ? msg += `one of type ${types[0]} or ${types[1]}.` : msg += `of type ${types[0]}.`;
  return actual == null ? msg += ` Received ${actual}` : typeof actual == "function" && actual.name ? msg += ` Received function ${actual.name}` : typeof actual == "object" && actual != null && actual.constructor?.name && (msg += ` Received an instance of ${actual.constructor.name}`), msg;
}
function invalidKeyType(alg, actual, secret) {
  const types = ["CryptoKey", "KeyObject", "JSON Web Key"];
  return secret && types.push("Uint8Array"), new TypeError(message(`Key for the ${alg} algorithm must be `, actual, ...types));
}
var unusable = (name, prop = "algorithm.name") => new TypeError(`CryptoKey does not support this operation, its ${prop} must be ${name}`);
function checkUsage(key, usage) {
  if (usage && !key.usages.includes(usage))
    throw new TypeError(`CryptoKey does not support this operation, its usages must include ${usage}.`);
}
function checkModulusLength(alg, key) {
  const { modulusLength } = key.algorithm;
  if (typeof modulusLength != "number" || modulusLength < 2048)
    throw new TypeError(`${alg} requires key modulusLength to be 2048 bits or larger`);
}
function checkCryptoKey(key, expected, usage) {
  const algorithm = key.algorithm;
  if (algorithm.name !== expected.name)
    throw unusable(expected.name);
  if (expected.hash && algorithm.hash?.name !== expected.hash)
    throw unusable(expected.hash, "algorithm.hash");
  if (expected.namedCurve && algorithm.namedCurve !== expected.namedCurve)
    throw unusable(expected.namedCurve, "algorithm.namedCurve");
  if (expected.length !== void 0 && algorithm.length !== expected.length)
    throw unusable(expected.length, "algorithm.length");
  checkUsage(key, usage);
}
function snapshotJwk(jwk) {
  return { __proto__: null, ...jwk };
}
function normalizeJwk(jwk) {
  const normalized = snapshotJwk(jwk);
  if (normalized.ext !== void 0 && typeof normalized.ext != "boolean")
    throw new TypeError('"ext" (Extractable) Parameter must be a boolean');
  if (normalized.key_ops !== void 0) {
    const value = normalized.key_ops, keyOps = Array.isArray(value) ? [...value] : void 0;
    if (!keyOps || keyOps.some((operation) => typeof operation != "string") || new Set(keyOps).size !== keyOps.length)
      throw new TypeError('"key_ops" (Key Operations) Parameter must be an array of unique strings');
    normalized.key_ops = keyOps;
  }
  return normalized;
}
async function jwkToKey(entry, jwk, extractable) {
  if (!entry.kty.includes(jwk.kty))
    throw new JOSENotSupported('Invalid or unsupported JWK "alg" (Algorithm) Parameter value');
  const algorithm = entry.resolve?.({ kty: jwk.kty, crv: jwk.crv }) ?? entry.subtle, isPrivate = !!(jwk.d || jwk.priv), keyData = { ...jwk, ext: extractable ?? jwk.ext };
  return keyData.kty !== "AKP" && delete keyData.alg, delete keyData.use, crypto.subtle.importKey("jwk", keyData, algorithm, keyData.ext ?? !isPrivate, jwk.key_ops ?? entry.usages[isPrivate ? 1 : 0]);
}
async function rawKey(key, expected, usage, extractable = false) {
  return key instanceof Uint8Array && (key = await crypto.subtle.importKey("raw", key, expected, extractable, [usage])), checkCryptoKey(key, expected, usage), key;
}

// node_modules/jose/dist/webapi/lib/key_descriptor.js
function table(entries) {
  const out = { __proto__: null };
  for (const alg in entries)
    out[alg] = { ...entries[alg], alg };
  return out;
}

// node_modules/jose/dist/webapi/lib/jws_algorithms.js
var sig = [["verify"], ["sign"]];
function hmac(bits) {
  const subtle = { name: "HMAC", hash: `SHA-${bits}` };
  return { kty: ["oct"], secret: true, subtle, signing: subtle, usages: sig };
}
function rsa(bits, saltLength) {
  const subtle = { name: saltLength ? "RSA-PSS" : "RSASSA-PKCS1-v1_5", hash: `SHA-${bits}` };
  return {
    kty: ["RSA"],
    subtle,
    signing: saltLength ? { ...subtle, saltLength } : subtle,
    usages: sig,
    minRsaBits: 2048
  };
}
function ecdsa(crv, bits) {
  return {
    kty: ["EC"],
    crv,
    subtle: { name: "ECDSA", namedCurve: crv },
    signing: { name: "ECDSA", hash: `SHA-${bits}` },
    usages: sig
  };
}
function eddsa() {
  const subtle = { name: "Ed25519" };
  return {
    kty: ["OKP"],
    crv: "Ed25519",
    subtle,
    signing: subtle,
    usages: sig
  };
}
function mldsa(bits) {
  const subtle = { name: `ML-DSA-${bits}` };
  return {
    kty: ["AKP"],
    subtle,
    signing: subtle,
    usages: sig
  };
}
var JWS = table({
  HS256: hmac(256),
  HS384: hmac(384),
  HS512: hmac(512),
  RS256: rsa(256),
  RS384: rsa(384),
  RS512: rsa(512),
  PS256: rsa(256, 32),
  PS384: rsa(384, 48),
  PS512: rsa(512, 64),
  ES256: ecdsa("P-256", 256),
  ES384: ecdsa("P-384", 384),
  ES512: ecdsa("P-521", 512),
  EdDSA: eddsa(),
  Ed25519: eddsa(),
  "ML-DSA-44": mldsa(44),
  "ML-DSA-65": mldsa(65),
  "ML-DSA-87": mldsa(87)
});
function jwsAlgorithm(alg) {
  const entry = typeof alg == "string" ? JWS[alg] : void 0;
  if (!entry)
    throw new JOSENotSupported(`alg ${alg} is not supported either by JOSE or your javascript runtime`);
  return entry;
}

// node_modules/jose/dist/webapi/lib/jws_verify.js
function prepareVerify(options) {
  return [options && validateAlgorithms("algorithms", options.algorithms), options?.crit];
}
function parseProtectedHeader(encodedProtected) {
  return encodedProtected === void 0 ? {} : parseJoseHeader(encodedProtected, JWSInvalid, "JWS Protected Header is invalid");
}
function encodeCompactUnencodedPayload(payload) {
  try {
    return encode(payload);
  } catch {
    throw new JWSInvalid("JWS Compact Serialization payload must use only ASCII characters");
  }
}
async function verifySignature(jws, shared, key, encodeUnencodedPayload, parsedProtected) {
  const { protected: encodedProtected, header: header2, payload: inputPayload } = jws, parsedProt = parsedProtected ?? parseProtectedHeader(encodedProtected);
  if (!isDisjoint(parsedProt, header2))
    throw new JWSInvalid("JWS Protected and JWS Unprotected Header Parameter names must be disjoint");
  const joseHeader = { ...parsedProt, ...header2 }, b64 = validateB64(parsedProt, validateCrit(JWSInvalid, JWS_RECOGNIZED, shared[1], parsedProt, joseHeader)), { alg } = joseHeader;
  if (typeof alg != "string" || !alg)
    throw new JWSInvalid('JWS "alg" (Algorithm) Header Parameter missing or invalid');
  if (shared[0] && !shared[0].has(alg))
    throw new JOSEAlgNotAllowed('"alg" (Algorithm) Header Parameter value not allowed');
  if (b64) {
    if (typeof inputPayload != "string")
      throw new JWSInvalid("JWS Payload must be a string");
  } else if (typeof inputPayload != "string" && !(inputPayload instanceof Uint8Array))
    throw new JWSInvalid("JWS Payload must be a string or an Uint8Array instance");
  const signingPayload = b64 || typeof inputPayload != "string" ? inputPayload : encodeUnencodedPayload(inputPayload);
  let resolvedKey = false;
  typeof key == "function" && (key = await key(parsedProt, jws), resolvedKey = true);
  const entry = jwsAlgorithm(alg), data = concat(encodedProtected !== void 0 ? encode(encodedProtected) : new Uint8Array(), encode("."), typeof signingPayload == "string" ? shared[2] ??= encodeBase64url(signingPayload, "payload", JWSInvalid) : signingPayload), signature = decodeBase64url(jws.signature, "signature", JWSInvalid), k = await prepareKey(entry, key, "verify"), cryptoKey = await rawKey(k, entry.subtle, "verify");
  entry.minRsaBits && checkModulusLength(entry.alg, cryptoKey);
  let verified = false;
  try {
    verified = await crypto.subtle.verify(entry.signing, cryptoKey, signature, data);
  } catch {
  }
  if (!verified)
    throw new JWSSignatureVerificationFailed();
  const result = { payload: typeof signingPayload == "string" ? decodeBase64url(signingPayload, "payload", JWSInvalid) : signingPayload };
  return encodedProtected !== void 0 && (result.protectedHeader = parsedProt), header2 !== void 0 && (result.unprotectedHeader = header2), resolvedKey ? [{ ...result, key: k }, b64] : [result, b64];
}
async function verifyCompact(jws, shared, key) {
  if (jws instanceof Uint8Array && (jws = decoder.decode(jws)), typeof jws != "string")
    throw new JWSInvalid("Compact JWS must be a string or Uint8Array");
  const { 0: protectedHeader, 1: payload, 2: signature, length } = jws.split(".");
  if (length !== 3)
    throw new JWSInvalid("Invalid Compact JWS");
  return verifySignature({ payload, protected: protectedHeader, signature }, shared, key, encodeCompactUnencodedPayload);
}

// node_modules/jose/dist/webapi/lib/jwt_claims_set.js
var epoch = (date) => Math.floor(date.getTime() / 1e3);
var multipliers = {
  s: 1,
  m: 60,
  h: 3600,
  d: 86400,
  w: 604800,
  y: 31557600
};
var REGEX = /^(\+|\-)? ?(\d+|\d+\.\d+) ?(seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d|weeks?|w|years?|yrs?|y)(?: (ago|from now))?$/i;
var checkFailed = "check_failed";
function invalidDuration() {
  throw new TypeError("Invalid time period format");
}
function secs(str) {
  typeof str != "string" && invalidDuration();
  const matched = REGEX.exec(str);
  (!matched || matched[4] && matched[1]) && invalidDuration();
  const value = parseFloat(matched[2]), numericDate2 = Math.round(value * multipliers[matched[3][0].toLowerCase()]);
  return Number.isFinite(numericDate2) || invalidDuration(), matched[1] === "-" || matched[4] === "ago" ? -numericDate2 : numericDate2;
}
function validateInput(label, input) {
  if (!Number.isFinite(input))
    throw new TypeError(`Invalid ${label} input`);
  return input;
}
var normalizeTyp = (value) => {
  const normalized = value.toLowerCase();
  return value.includes("/") ? normalized : `application/${normalized}`;
};
var checkAudiencePresence = (audPayload, audOption) => typeof audPayload == "string" ? audOption.includes(audPayload) : Array.isArray(audPayload) ? audOption.some((aud) => audPayload.includes(aud)) : false;
function validateNumericDate(payload, claim, required = false) {
  const value = payload[claim];
  if (!(value === void 0 && !required)) {
    if (typeof value != "number")
      throw new JWTClaimValidationFailed(`"${claim}" claim must be a number`, payload, claim, "invalid");
    return value;
  }
}
function unexpectedClaim(payload, claim) {
  throw new JWTClaimValidationFailed(`unexpected "${claim}" claim value`, payload, claim, checkFailed);
}
function validateClaimsSet(protectedHeader, encodedPayload, options = {}) {
  let payload;
  try {
    payload = JSON.parse(strictDecoder.decode(encodedPayload));
  } catch {
  }
  if (!isObject(payload))
    throw new JWTInvalid("JWT Claims Set must be a top-level JSON object");
  const { typ } = options;
  if (typ !== void 0 && (typeof protectedHeader.typ != "string" || normalizeTyp(protectedHeader.typ) !== normalizeTyp(typ)))
    throw new JWTClaimValidationFailed('unexpected "typ" JWT header value', payload, "typ", checkFailed);
  const { requiredClaims = [], issuer, subject, audience, maxTokenAge } = options, presenceCheck = [...requiredClaims];
  maxTokenAge !== void 0 && presenceCheck.push("iat"), audience !== void 0 && presenceCheck.push("aud"), subject !== void 0 && presenceCheck.push("sub"), issuer !== void 0 && presenceCheck.push("iss");
  for (const claim of new Set(presenceCheck.reverse()))
    if (!Object.hasOwn(payload, claim))
      throw new JWTClaimValidationFailed(`missing required "${claim}" claim`, payload, claim, "missing");
  issuer !== void 0 && !(Array.isArray(issuer) ? issuer : [issuer]).includes(payload.iss) && unexpectedClaim(payload, "iss"), subject !== void 0 && payload.sub !== subject && unexpectedClaim(payload, "sub"), audience !== void 0 && !checkAudiencePresence(payload.aud, typeof audience == "string" ? [audience] : audience) && unexpectedClaim(payload, "aud");
  const { clockTolerance } = options;
  let tolerance = 0;
  if (typeof clockTolerance == "string")
    tolerance = secs(clockTolerance);
  else if (clockTolerance !== void 0) {
    if (typeof clockTolerance != "number")
      throw new TypeError("Invalid clockTolerance option type");
    tolerance = clockTolerance;
  }
  validateInput("clockTolerance option", tolerance);
  const { currentDate } = options, now = validateInput("currentDate option", epoch(currentDate === void 0 ? /* @__PURE__ */ new Date() : currentDate)), iat = validateNumericDate(payload, "iat", maxTokenAge !== void 0), nbf = validateNumericDate(payload, "nbf");
  if (nbf !== void 0 && nbf > now + tolerance)
    throw new JWTClaimValidationFailed('"nbf" claim timestamp check failed', payload, "nbf", checkFailed);
  const exp = validateNumericDate(payload, "exp");
  if (exp !== void 0 && exp <= now - tolerance)
    throw new JWTExpired('"exp" claim timestamp check failed', payload, "exp", checkFailed);
  if (maxTokenAge !== void 0) {
    const age = now - iat, max = validateInput("maxTokenAge option", typeof maxTokenAge == "number" ? maxTokenAge : secs(maxTokenAge));
    if (age - tolerance > max)
      throw new JWTExpired('"iat" claim timestamp check failed (too far in the past)', payload, "iat", checkFailed);
    if (age < -tolerance)
      throw new JWTClaimValidationFailed('"iat" claim timestamp check failed (it should be in the past)', payload, "iat", checkFailed);
  }
  return payload;
}

// node_modules/jose/dist/webapi/jwt/verify.js
async function jwtVerify(jwt, key, options) {
  const [verified, b64] = await verifyCompact(jwt, prepareVerify(options), key);
  if (!b64)
    throw new JWTInvalid("JWTs MUST NOT use unencoded payload");
  const payload = validateClaimsSet(verified.protectedHeader, verified.payload, options);
  return { ...verified, payload };
}

// node_modules/jose/dist/webapi/jwks/local.js
function isUsableJWK(jwk, entry, alg, kid) {
  const { kty, key_ops: keyOps, ext, kid: jwkKid, alg: jwkAlg, use, crv } = jwk;
  return (ext === void 0 || typeof ext == "boolean") && (keyOps === void 0 || Array.isArray(keyOps) && keyOps.every((operation, index) => typeof operation == "string" && keyOps.indexOf(operation) === index) && keyOps.includes("verify")) && entry.kty.includes(kty) && (kid === void 0 || typeof kid == "string" && kid === jwkKid) && (jwkAlg === void 0 ? kty !== "AKP" : alg === jwkAlg) && (use === void 0 || use === "sig") && (!entry.crv || crv === entry.crv);
}
async function importWithAlgCache(cache2, jwk, entry) {
  const cached = cache2.get(jwk) || cache2.set(jwk, {}).get(jwk), { alg } = entry;
  if (cached[alg] === void 0) {
    const pending = jwkToKey(entry, jwk, true).then((key) => {
      if (key.type !== "public")
        throw new JWKSInvalid("JSON Web Key Set members must be public keys");
      return cached[alg] = key, key;
    }).catch((error) => {
      throw cached[alg] === pending && delete cached[alg], error;
    });
    cached[alg] = pending;
  }
  return cached[alg];
}
function createLocalJWKSet(jwks) {
  let snapshot;
  try {
    snapshot = structuredClone(jwks);
  } catch {
  }
  if (!isJwkSet(snapshot))
    throw new JWKSInvalid("JSON Web Key Set malformed");
  const metadata = snapshot.keys.map((jwk) => {
    const normalized = snapshotJwk(jwk);
    return Array.isArray(normalized.key_ops) && (normalized.key_ops = [...normalized.key_ops]), normalized;
  }), cached = /* @__PURE__ */ new WeakMap();
  return Object.defineProperty(async (protectedHeader, token) => {
    const { alg, kid } = { ...protectedHeader, ...token?.header }, entry = typeof alg == "string" ? JWS[alg] : void 0;
    if (!entry || entry.secret)
      throw new JOSENotSupported('Unsupported "alg" value for a JSON Web Key Set');
    const candidates = snapshot.keys.filter((_, index) => isUsableJWK(metadata[index], entry, alg, kid)), { 0: jwk, length } = candidates;
    if (!length)
      throw new JWKSNoMatchingKey();
    if (length !== 1) {
      const error = new JWKSMultipleMatchingKeys();
      throw error[Symbol.asyncIterator] = async function* () {
        for (const jwk2 of candidates)
          try {
            yield await importWithAlgCache(cached, jwk2, entry);
          } catch {
          }
      }, error;
    }
    return importWithAlgCache(cached, jwk, entry);
  }, "jwks", {
    value: () => structuredClone(snapshot)
  });
}

// node_modules/jose/dist/webapi/jwks/remote.js
function isCloudflareWorkers() {
  return typeof WebSocketPair < "u" || typeof navigator < "u" && navigator.userAgent === "Cloudflare-Workers" || typeof EdgeRuntime < "u" && EdgeRuntime === "vercel";
}
var USER_AGENT;
(typeof navigator > "u" || !navigator.userAgent?.startsWith?.("Mozilla/5.0 ")) && (USER_AGENT = "jose/v6.2.12");
var customFetch = /* @__PURE__ */ Symbol();
async function fetchJwks(url, headers, signal, fetchImpl = fetch) {
  const response2 = await fetchImpl(url, {
    method: "GET",
    signal,
    redirect: "manual",
    headers
  }).catch((err) => {
    throw err.name === "TimeoutError" ? new JWKSTimeout() : err;
  });
  if (response2.status !== 200)
    throw new JOSEError("Expected 200 OK from the JSON Web Key Set HTTP response");
  try {
    return await response2.json();
  } catch {
    throw new JOSEError("Failed to parse the JSON Web Key Set HTTP response as JSON");
  }
}
var jwksCache = /* @__PURE__ */ Symbol();
function isFreshFor(timestamp, duration) {
  return Number.isFinite(timestamp) && Date.now() < timestamp + duration;
}
function validateDuration(value, fallback, option) {
  if (Number.isNaN(value))
    throw new TypeError(`"${option}" option must not be NaN`);
  return typeof value == "number" ? value : fallback;
}
function createRemoteJWKSet(url, options) {
  if (!(url instanceof URL))
    throw new TypeError("url must be an instance of URL");
  const href = new URL(url.href).href, opts = options ?? {}, timeoutOption = opts.timeoutDuration;
  if (typeof timeoutOption == "number" && (!Number.isInteger(timeoutOption) || timeoutOption < 0))
    throw new TypeError('"timeoutDuration" option must be a non-negative integer');
  const timeoutDuration = typeof timeoutOption == "number" ? timeoutOption : 5e3, cooldownDuration = validateDuration(opts.cooldownDuration, 3e4, "cooldownDuration"), cacheMaxAge = validateDuration(opts.cacheMaxAge, 6e5, "cacheMaxAge"), headers = new Headers(opts.headers);
  USER_AGENT && !headers.has("User-Agent") && headers.set("User-Agent", USER_AGENT), headers.has("accept") || headers.set("accept", "application/json, application/jwk-set+json");
  const fetchImpl = opts[customFetch], cache2 = opts[jwksCache];
  let jwksTimestamp, pendingFetch, reloadSequence = 0, appliedSequence = 0, local;
  if (cache2 && typeof cache2 == "object") {
    const { uat, jwks } = cache2;
    isFreshFor(uat, cacheMaxAge) && isJwkSet(jwks) && (jwksTimestamp = uat, local = createLocalJWKSet(jwks));
  }
  const reload = async () => {
    if (pendingFetch && isCloudflareWorkers() && (pendingFetch = void 0), !pendingFetch) {
      const sequence = ++reloadSequence, current = pendingFetch = fetchJwks(href, headers, AbortSignal.timeout(timeoutDuration), fetchImpl).then((json2) => {
        const next = createLocalJWKSet(json2);
        if (sequence <= appliedSequence)
          return;
        local = next;
        const updatedAt = Date.now();
        cache2 && (cache2.uat = updatedAt, cache2.jwks = json2), jwksTimestamp = updatedAt, appliedSequence = sequence;
      }).finally(() => {
        pendingFetch === current && (pendingFetch = void 0);
      });
    }
    await pendingFetch;
  };
  return Object.defineProperties(async (protectedHeader, token) => {
    (!local || !isFreshFor(jwksTimestamp, cacheMaxAge)) && await reload();
    try {
      return await local(protectedHeader, token);
    } catch (err) {
      if (err instanceof JWKSNoMatchingKey && !isFreshFor(jwksTimestamp, cooldownDuration))
        return await reload(), local(protectedHeader, token);
      throw err;
    }
  }, {
    coolingDown: {
      get: () => isFreshFor(jwksTimestamp, cooldownDuration),
      enumerable: true
    },
    fresh: {
      get: () => isFreshFor(jwksTimestamp, cacheMaxAge),
      enumerable: true
    },
    reload: {
      value: reload,
      enumerable: true
    },
    reloading: {
      get: () => !!pendingFetch,
      enumerable: true
    },
    jwks: {
      value: () => local?.jwks(),
      enumerable: true
    }
  });
}

// src/access.mjs
function accessConfiguration(env) {
  const team = String(env.CMAIL_ACCESS_TEAM_DOMAIN || "").trim();
  const audience = String(env.CMAIL_ACCESS_AUD || "").trim();
  const owner = String(env.CMAIL_OWNER_EMAIL || "").trim().toLowerCase();
  if (!/^[a-z0-9-]+\.cloudflareaccess\.com$/.test(team) || !audience || !owner.includes("@")) {
    return null;
  }
  return { issuer: `https://${team}`, audience, owner };
}
function createAuthorizer(keySetFactory = createRemoteJWKSet) {
  const keySets = /* @__PURE__ */ new Map();
  return async function authorize2(request, env) {
    const config = accessConfiguration(env);
    if (!config) return { ok: false, status: 503, error: "sign_in_not_configured" };
    const token = request.headers.get("Cf-Access-Jwt-Assertion");
    if (!token) return { ok: false, status: 401, error: "sign_in_required" };
    try {
      if (!keySets.has(config.issuer)) {
        keySets.set(config.issuer, keySetFactory(new URL(`${config.issuer}/cdn-cgi/access/certs`)));
      }
      const { payload } = await jwtVerify(token, keySets.get(config.issuer), {
        algorithms: ["RS256"],
        issuer: config.issuer,
        audience: config.audience,
        requiredClaims: ["exp", "iat", "sub", "email"],
        clockTolerance: 5
      });
      if (String(payload.email || "").toLowerCase() !== config.owner) {
        return { ok: false, status: 403, error: "account_not_allowed" };
      }
      return { ok: true, email: config.owner };
    } catch {
      return { ok: false, status: 401, error: "invalid_session" };
    }
  };
}
var authorize = createAuthorizer();

// node_modules/postal-mime/dist/esm/decode-strings.js
var textEncoder = new TextEncoder();
var isWsp = (c) => c === 32 || c === 9;
var base64Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
var base64Lookup = new Uint8Array(256);
for (let i = 0; i < base64Chars.length; i++) {
  base64Lookup[base64Chars.charCodeAt(i)] = i;
}
function decodeBase642(base64) {
  let len = base64.length;
  while (len > 0 && base64.charAt(len - 1) === "=") {
    len--;
  }
  if (len % 4 === 1) {
    len--;
  }
  const remainder = len % 4;
  const bufferLength = Math.floor(len / 4) * 3 + (remainder ? remainder - 1 : 0);
  const arrayBuffer = new ArrayBuffer(bufferLength);
  const bytes2 = new Uint8Array(arrayBuffer);
  let p = 0;
  for (let i = 0; i < len; i += 4) {
    let encoded1 = base64Lookup[base64.charCodeAt(i)];
    let encoded2 = base64Lookup[base64.charCodeAt(i + 1)];
    let encoded3 = base64Lookup[base64.charCodeAt(i + 2)];
    let encoded4 = base64Lookup[base64.charCodeAt(i + 3)];
    bytes2[p++] = encoded1 << 2 | encoded2 >> 4;
    if (p < bufferLength) {
      bytes2[p++] = (encoded2 & 15) << 4 | encoded3 >> 2;
    }
    if (p < bufferLength) {
      bytes2[p++] = (encoded3 & 3) << 6 | encoded4 & 63;
    }
  }
  return arrayBuffer;
}
var charsetAliases = /* @__PURE__ */ new Map([
  // Hebrew. The logical and explicit ordering variants share the iso-8859-8 index.
  ["iso88598i", "iso-8859-8"],
  ["iso88598e", "iso-8859-8"],
  // Japanese. WHATWG shift_jis is the Windows-31J index, so cp932 text decodes
  // identically, including the NEC and IBM extension rows.
  ["shiftjis", "shift_jis"],
  ["windows31j", "shift_jis"],
  ["mskanji", "shift_jis"],
  ["eucjp", "euc-jp"],
  // ISO-2022-JP, including the -1 / -2 supersets. Escape sequences outside plain
  // ISO-2022-JP (JIS X 0212, the non-Japanese G2 sets, and the SO/SI katakana shifts
  // cp50222 uses) decode to replacement characters, but the Japanese text around them
  // still comes out right.
  ["iso2022jp", "iso-2022-jp"],
  ["iso2022jp1", "iso-2022-jp"],
  ["iso2022jp2", "iso-2022-jp"],
  ["junet", "iso-2022-jp"],
  // Korean. The WHATWG euc-kr index is the extended cp949 / UHC index.
  ["euckr", "euc-kr"],
  ["uhc", "euc-kr"],
  // Thai.
  ["tis620", "windows-874"]
]);
var codePageAliases = /* @__PURE__ */ new Map([
  ["932", "shift_jis"],
  ["936", "gbk"],
  ["949", "euc-kr"],
  ["950", "big5"],
  ["874", "windows-874"],
  // Microsoft's EUC-JP and ISO-2022-JP variants. euc-jp and iso-2022-jp cover
  // everything they can express.
  ["51932", "euc-jp"],
  ["50220", "iso-2022-jp"],
  ["50221", "iso-2022-jp"],
  ["50222", "iso-2022-jp"]
]);
var codePagePattern = /^(?:cp|windows|ms|ibm)(\d+)$/;
function normalizeCharset(charset) {
  return charset.replace(/^(?:x-ms-|x-|cs)/, "").replace(/[\s._-]+/g, "");
}
var ENCODING_LABELS = new Set([
  // UTF-8
  "unicode-1-1-utf-8 unicode11utf8 unicode20utf8 utf-8 utf8 x-unicode20utf8",
  // IBM866
  "866 cp866 csibm866 ibm866",
  // ISO-8859-2
  "csisolatin2 iso-8859-2 iso-ir-101 iso8859-2 iso88592 iso_8859-2 iso_8859-2:1987 l2 latin2",
  // ISO-8859-3
  "csisolatin3 iso-8859-3 iso-ir-109 iso8859-3 iso88593 iso_8859-3 iso_8859-3:1988 l3 latin3",
  // ISO-8859-4
  "csisolatin4 iso-8859-4 iso-ir-110 iso8859-4 iso88594 iso_8859-4 iso_8859-4:1988 l4 latin4",
  // ISO-8859-5
  "csisolatincyrillic cyrillic iso-8859-5 iso-ir-144 iso8859-5 iso88595 iso_8859-5 iso_8859-5:1988",
  // ISO-8859-6
  "arabic asmo-708 csiso88596e csiso88596i csisolatinarabic ecma-114 iso-8859-6 iso-8859-6-e",
  "iso-8859-6-i iso-ir-127 iso8859-6 iso88596 iso_8859-6 iso_8859-6:1987",
  // ISO-8859-7
  "csisolatingreek ecma-118 elot_928 greek greek8 iso-8859-7 iso-ir-126 iso8859-7 iso88597 iso_8859-7",
  "iso_8859-7:1987 sun_eu_greek",
  // ISO-8859-8
  "csiso88598e csisolatinhebrew hebrew iso-8859-8 iso-8859-8-e iso-ir-138 iso8859-8 iso88598 iso_8859-8",
  "iso_8859-8:1988 visual",
  // ISO-8859-8-I
  "csiso88598i iso-8859-8-i logical",
  // ISO-8859-10
  "csisolatin6 iso-8859-10 iso-ir-157 iso8859-10 iso885910 l6 latin6",
  // ISO-8859-13
  "iso-8859-13 iso8859-13 iso885913",
  // ISO-8859-14
  "iso-8859-14 iso8859-14 iso885914",
  // ISO-8859-15
  "csisolatin9 iso-8859-15 iso8859-15 iso885915 iso_8859-15 l9",
  // ISO-8859-16
  "iso-8859-16",
  // KOI8-R
  "cskoi8r koi koi8 koi8-r koi8_r",
  // KOI8-U
  "koi8-ru koi8-u",
  // macintosh
  "csmacintosh mac macintosh x-mac-roman",
  // windows-874
  "dos-874 iso-8859-11 iso8859-11 iso885911 tis-620 windows-874",
  // windows-1250
  "cp1250 windows-1250 x-cp1250",
  // windows-1251
  "cp1251 windows-1251 x-cp1251",
  // windows-1252
  "ansi_x3.4-1968 ascii cp1252 cp819 csisolatin1 ibm819 iso-8859-1 iso-ir-100 iso8859-1 iso88591",
  "iso_8859-1 iso_8859-1:1987 l1 latin1 us-ascii windows-1252 x-cp1252",
  // windows-1253
  "cp1253 windows-1253 x-cp1253",
  // windows-1254
  "cp1254 csisolatin5 iso-8859-9 iso-ir-148 iso8859-9 iso88599 iso_8859-9 iso_8859-9:1989 l5 latin5",
  "windows-1254 x-cp1254",
  // windows-1255
  "cp1255 windows-1255 x-cp1255",
  // windows-1256
  "cp1256 windows-1256 x-cp1256",
  // windows-1257
  "cp1257 windows-1257 x-cp1257",
  // windows-1258
  "cp1258 windows-1258 x-cp1258",
  // x-mac-cyrillic
  "x-mac-cyrillic x-mac-ukrainian",
  // GBK
  "chinese csgb2312 csiso58gb231280 gb2312 gb_2312 gb_2312-80 gbk iso-ir-58 x-gbk",
  // gb18030
  "gb18030",
  // Big5
  "big5 big5-hkscs cn-big5 csbig5 x-x-big5",
  // EUC-JP
  "cseucpkdfmtjapanese euc-jp x-euc-jp",
  // ISO-2022-JP
  "csiso2022jp iso-2022-jp",
  // Shift_JIS
  "csshiftjis ms932 ms_kanji shift-jis shift_jis sjis windows-31j x-sjis",
  // EUC-KR
  "cseuckr csksc56011987 euc-kr iso-ir-149 korean ks_c_5601-1987 ks_c_5601-1989 ksc5601 ksc_5601",
  "windows-949",
  // replacement
  "csiso2022kr hz-gb-2312 iso-2022-cn iso-2022-cn-ext iso-2022-kr replacement",
  // UTF-16BE
  "unicodefffe utf-16be",
  // UTF-16LE
  "csunicode iso-10646-ucs-2 ucs-2 unicode unicodefeff utf-16 utf-16le",
  // x-user-defined
  "x-user-defined"
].join(" ").split(" "));
var decoders = /* @__PURE__ */ new Map();
function tryDecoder(charset) {
  if (!ENCODING_LABELS.has(charset)) {
    return null;
  }
  let decoder2 = decoders.get(charset);
  if (decoder2 === void 0) {
    decoder2 = null;
    try {
      decoder2 = new TextDecoder(charset);
    } catch {
    }
    decoders.set(charset, decoder2);
  }
  return decoder2;
}
var utf8Decoder = new TextDecoder();
function findDecoder(charset) {
  charset = (charset || "utf8").trim().toLowerCase();
  const decoder2 = tryDecoder(charset);
  if (decoder2) {
    return decoder2;
  }
  const normalized = normalizeCharset(charset);
  const codePage = normalized.match(codePagePattern);
  const alias = codePage && codePageAliases.get(codePage[1]) || charsetAliases.get(normalized) || normalized;
  return tryDecoder(alias);
}
function getDecoder(charset) {
  return findDecoder(charset) || tryDecoder("windows-1252") || utf8Decoder;
}
function isUtf8(bytes2) {
  for (let i = 0; i < bytes2.length; ) {
    const lead = bytes2[i];
    if (lead < 128) {
      i++;
      continue;
    }
    let count;
    if (lead >= 194 && lead <= 223) {
      count = 1;
    } else if (lead >= 224 && lead <= 239) {
      count = 2;
    } else if (lead >= 240 && lead <= 244) {
      count = 3;
    } else {
      return false;
    }
    if (i + count >= bytes2.length) {
      return false;
    }
    const second = bytes2[i + 1];
    const min = lead === 224 ? 160 : lead === 240 ? 144 : 128;
    const max = lead === 237 ? 159 : lead === 244 ? 143 : 191;
    if (second < min || second > max) {
      return false;
    }
    for (let k = 2; k <= count; k++) {
      if ((bytes2[i + k] & 192) !== 128) {
        return false;
      }
    }
    i += count + 1;
  }
  return true;
}
function decodeUtf8OrWindows1252(bytes2, utf8 = utf8Decoder) {
  const view = bytes2 instanceof Uint8Array ? bytes2 : new Uint8Array(bytes2);
  return isUtf8(view) ? utf8.decode(view) : getDecoder("windows-1252").decode(view);
}
function decodeCharset(bytes2, charset) {
  const decoder2 = findDecoder(charset);
  return decoder2 ? decoder2.decode(bytes2) : decodeUtf8OrWindows1252(bytes2);
}
var META_PRESCAN_BYTES = 1024;
var CHARSET_ATTRIBUTE = /charset\s*=\s*["']?\s*([^\s"';>]+)/;
var UTF16_ENCODINGS = /* @__PURE__ */ new Set(["utf-16le", "utf-16be"]);
function sniffHtmlCharset(bytes2) {
  const head = utf8Decoder.decode(bytes2.subarray(0, META_PRESCAN_BYTES)).toLowerCase();
  let pos = 0;
  while ((pos = head.indexOf("<meta", pos)) >= 0) {
    let end = head.indexOf(">", pos);
    if (end < 0) {
      end = head.length;
    }
    const match = CHARSET_ATTRIBUTE.exec(head.slice(pos, end));
    if (match) {
      return match[1];
    }
    pos = end;
  }
  return null;
}
function decodeText(bytes2, charset, html) {
  const view = new Uint8Array(bytes2);
  if (charset) {
    return decodeCharset(view, charset);
  }
  const sniffed = html ? sniffHtmlCharset(view) : null;
  const decoder2 = sniffed ? findDecoder(sniffed) : null;
  if (!decoder2 || UTF16_ENCODINGS.has(decoder2.encoding)) {
    return decodeUtf8OrWindows1252(view);
  }
  return (decoder2.encoding === "x-user-defined" ? getDecoder("windows-1252") : decoder2).decode(view);
}
async function blobToArrayBuffer(blob) {
  if ("arrayBuffer" in blob) {
    return await blob.arrayBuffer();
  }
  const fr = new FileReader();
  return new Promise((resolve, reject) => {
    fr.onload = () => {
      resolve(fr.result);
    };
    fr.onerror = () => {
      reject(fr.error);
    };
    fr.readAsArrayBuffer(blob);
  });
}
function hexNibble(c) {
  if (c >= 48 && c <= 57) {
    return c - 48;
  }
  if (c >= 97 && c <= 102) {
    return c - 97 + 10;
  }
  if (c >= 65 && c <= 70) {
    return c - 65 + 10;
  }
  return -1;
}
function foldLineBreaks(str) {
  return str.replace(/[\r\n]+/g, " ");
}
function decodeWord(charset, encoding, str) {
  let splitPos = charset.indexOf("*");
  if (splitPos >= 0) {
    charset = charset.slice(0, splitPos);
  }
  encoding = encoding.toUpperCase();
  let byteStr;
  if (encoding === "Q") {
    str = str.replace(/=\s+([0-9a-fA-F])/g, "=$1").replace(/[_\s]/g, " ");
    let buf = textEncoder.encode(str);
    let encodedBytes = [];
    for (let i = 0, len = buf.length; i < len; i++) {
      let c = buf[i];
      if (i <= len - 2 && c === 61) {
        let high = hexNibble(buf[i + 1]);
        let low = hexNibble(buf[i + 2]);
        if (high >= 0 && low >= 0) {
          encodedBytes.push(high << 4 | low);
          i += 2;
          continue;
        }
      }
      encodedBytes.push(c);
    }
    byteStr = new ArrayBuffer(encodedBytes.length);
    let dataView = new DataView(byteStr);
    for (let i = 0, len = encodedBytes.length; i < len; i++) {
      dataView.setUint8(i, encodedBytes[i]);
    }
  } else if (encoding === "B") {
    byteStr = decodeBase642(str.replace(/[^a-zA-Z0-9\+\/=]+/g, ""));
  } else {
    byteStr = textEncoder.encode(str);
  }
  return foldLineBreaks(decodeCharset(byteStr, charset));
}
var ENCODED_WORD_PATTERN = "=\\?([^?\\s]+)\\?([QqBb])\\?([^?]*)\\?=";
var ENCODED_WORD_REGEX = new RegExp(ENCODED_WORD_PATTERN, "g");
var WORD_SEPARATOR_REGEX = /^[ \t\r\n]+$/;
var ENCODED_WORDS_ONLY_REGEX = new RegExp(`^(?:${ENCODED_WORD_PATTERN}\\s*)+$`);
function isEncodedWordsOnly(str) {
  return ENCODED_WORDS_ONLY_REGEX.test(str);
}
function splitEncodedWords(str) {
  const tokens = [];
  ENCODED_WORD_REGEX.lastIndex = 0;
  let pos = 0;
  let match;
  while (match = ENCODED_WORD_REGEX.exec(str)) {
    if (match.index > pos) {
      tokens.push({ text: str.substring(pos, match.index) });
    }
    tokens.push({ charset: match[1], encoding: match[2], encodedText: match[3] });
    pos = match.index + match[0].length;
  }
  if (pos < str.length) {
    tokens.push({ text: str.substring(pos) });
  }
  return tokens;
}
function canJoinWords(left, right) {
  const encoding = left.encoding.toUpperCase();
  if (left.charset !== right.charset || encoding !== right.encoding.toUpperCase()) {
    return false;
  }
  if (encoding === "B") {
    return left.encodedText.length % 4 === 0 && !/=$/.test(left.encodedText);
  }
  return true;
}
function renderTokens(tokens, joinWords) {
  let result = "";
  let pending = null;
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    if (token.text !== void 0) {
      const nextToken = tokens[i + 1];
      if (pending && nextToken && nextToken.text === void 0 && WORD_SEPARATOR_REGEX.test(token.text)) {
        continue;
      }
      if (pending) {
        result += decodeWord(pending.charset, pending.encoding, pending.encodedText);
        pending = null;
      }
      result += token.text;
      continue;
    }
    if (pending && joinWords && canJoinWords(pending, token)) {
      pending.encodedText += token.encodedText;
      continue;
    }
    if (pending) {
      result += decodeWord(pending.charset, pending.encoding, pending.encodedText);
    }
    pending = { charset: token.charset, encoding: token.encoding, encodedText: token.encodedText };
  }
  if (pending) {
    result += decodeWord(pending.charset, pending.encoding, pending.encodedText);
  }
  return result;
}
function decodeWords(str) {
  const tokens = splitEncodedWords((str || "").toString());
  const result = renderTokens(tokens, true);
  return result.indexOf("\uFFFD") < 0 ? result : renderTokens(tokens, false);
}
function decodeURIComponentWithCharset(encodedStr, charset) {
  charset = charset || "utf-8";
  let encodedBytes = [];
  for (let i = 0; i < encodedStr.length; i++) {
    let c = encodedStr.charAt(i);
    const high = c === "%" ? hexNibble(encodedStr.charCodeAt(i + 1)) : -1;
    const low = high >= 0 ? hexNibble(encodedStr.charCodeAt(i + 2)) : -1;
    if (low >= 0) {
      encodedBytes.push(high << 4 | low);
      i += 2;
    } else if (c.charCodeAt(0) > 126) {
      const bytes2 = textEncoder.encode(c);
      for (let j = 0; j < bytes2.length; j++) {
        encodedBytes.push(bytes2[j]);
      }
    } else {
      encodedBytes.push(c.charCodeAt(0));
    }
  }
  const byteStr = new ArrayBuffer(encodedBytes.length);
  const dataView = new DataView(byteStr);
  for (let i = 0, len = encodedBytes.length; i < len; i++) {
    dataView.setUint8(i, encodedBytes[i]);
  }
  return decodeCharset(byteStr, charset);
}
function decodeParameterValueContinuations(header2) {
  let paramKeys = /* @__PURE__ */ new Map();
  Object.keys(header2.params).forEach((key) => {
    let match = key.match(/\*((\d+)\*?)?$/);
    if (!match) {
      return;
    }
    let actualKey = key.slice(0, match.index).toLowerCase();
    let nr = Number(match[2]) || 0;
    let paramVal = paramKeys.get(actualKey);
    if (!paramVal) {
      paramVal = {
        charset: null,
        values: []
      };
      paramKeys.set(actualKey, paramVal);
    }
    let value = header2.params[key];
    let encoded = match[0].charAt(match[0].length - 1) === "*";
    if (nr === 0 && encoded && (match = value.match(/^([^']*)'[^']*'(.*)$/))) {
      paramVal.charset = match[1] || "utf-8";
      value = match[2];
    }
    paramVal.values.push({ nr, value, encoded });
    delete header2.params[key];
  });
  paramKeys.forEach((paramVal, key) => {
    if (Object.prototype.hasOwnProperty.call(header2.params, key)) {
      return;
    }
    let result = "";
    let pending = "";
    for (let part of paramVal.values.sort((a, b) => a.nr - b.nr)) {
      if (part.encoded) {
        pending += part.value;
        continue;
      }
      if (pending) {
        result += decodeURIComponentWithCharset(pending, paramVal.charset);
        pending = "";
      }
      result += part.value;
    }
    if (pending) {
      result += decodeURIComponentWithCharset(pending, paramVal.charset);
    }
    header2.params[key] = result;
  });
}

// node_modules/postal-mime/dist/esm/pass-through-decoder.js
var PassThroughDecoder = class {
  constructor() {
    this.chunks = [];
    this.runBuffer = null;
    this.runStart = 0;
    this.runEnd = 0;
    this.pendingLineBreak = null;
  }
  // Whether the bytes sit in the run's buffer right at the end of the run
  continuesRun(bytes2) {
    return bytes2.buffer === this.runBuffer && bytes2.byteOffset === this.runEnd;
  }
  flushRun() {
    if (this.runBuffer && this.runEnd > this.runStart) {
      this.chunks.push(new Uint8Array(this.runBuffer, this.runStart, this.runEnd - this.runStart));
    }
    this.runBuffer = null;
  }
  update(line, lineBreak) {
    const pending = this.pendingLineBreak;
    if (pending && this.continuesRun(pending) && line.buffer === this.runBuffer && line.byteOffset === this.runEnd + pending.byteLength) {
      this.runEnd = line.byteOffset + line.byteLength;
    } else {
      this.flushRun();
      if (pending) {
        this.chunks.push(pending);
      }
      this.runBuffer = line.buffer;
      this.runStart = line.byteOffset;
      this.runEnd = line.byteOffset + line.byteLength;
    }
    this.pendingLineBreak = lineBreak;
  }
  // A part ended by the end of the message has no boundary to own its final line
  // break, so there it stays content
  finalize(boundaryEnded = false) {
    const pending = this.pendingLineBreak;
    if (!boundaryEnded && pending) {
      if (this.continuesRun(pending)) {
        this.runEnd += pending.byteLength;
      } else {
        this.flushRun();
        this.chunks.push(pending);
      }
    }
    this.flushRun();
    return blobToArrayBuffer(new Blob(this.chunks, { type: "application/octet-stream" }));
  }
};

// node_modules/postal-mime/dist/esm/base64-decoder.js
var Base64Decoder = class {
  constructor(opts) {
    opts = opts || {};
    this.decoder = opts.decoder || new TextDecoder();
    this.maxChunkSize = 100 * 1024;
    this.chunks = [];
    this.remainder = "";
  }
  pushChunk(base64Str) {
    if (base64Str.length) {
      this.chunks.push(decodeBase642(base64Str));
    }
  }
  flushRemainder() {
    this.pushChunk(this.remainder);
    this.remainder = "";
  }
  update(buffer) {
    let str = this.decoder.decode(buffer).replace(/[^a-zA-Z0-9+/=]+/g, "");
    const units = str.split(/=+/);
    for (let i = 0; i < units.length; i++) {
      this.remainder += units[i];
      if (i < units.length - 1) {
        this.flushRemainder();
      }
    }
    if (this.remainder.length >= this.maxChunkSize) {
      const alignedLength = Math.floor(this.remainder.length / 4) * 4;
      this.pushChunk(this.remainder.substring(0, alignedLength));
      this.remainder = this.remainder.substring(alignedLength);
    }
  }
  finalize() {
    this.flushRemainder();
    return blobToArrayBuffer(new Blob(this.chunks, { type: "application/octet-stream" }));
  }
};

// node_modules/postal-mime/dist/esm/qp-decoder.js
var CHR_EQUALS = 61;
var QPDecoder = class {
  constructor() {
    this.maxChunkSize = 100 * 1024;
    this.buffer = new Uint8Array(this.maxChunkSize);
    this.bufferPos = 0;
    this.chunks = [];
    this.pendingLineBreak = null;
  }
  writeLineBreak() {
    if (this.pendingLineBreak) {
      this.writeBytes(this.pendingLineBreak, 0, this.pendingLineBreak.length);
    }
  }
  writeByte(byte) {
    if (this.bufferPos >= this.buffer.length) {
      this.flushBuffer();
    }
    this.buffer[this.bufferPos++] = byte;
  }
  // Literal text is the bulk of a typical body, so it is copied in runs rather than a
  // byte at a time
  writeBytes(line, start, end) {
    while (start < end) {
      if (this.bufferPos >= this.buffer.length) {
        this.flushBuffer();
      }
      const count = Math.min(end - start, this.buffer.length - this.bufferPos);
      this.buffer.set(line.subarray(start, start + count), this.bufferPos);
      this.bufferPos += count;
      start += count;
    }
  }
  flushBuffer() {
    if (this.bufferPos) {
      this.chunks.push(this.buffer.slice(0, this.bufferPos));
      this.bufferPos = 0;
    }
  }
  // Quoted-printable source is 7 bit by definition, so it is decoded byte by byte and
  // the result is handed on as bytes. Running the body charset over the encoded source
  // instead corrupted every part whose charset was not ASCII compatible: the same
  // content that decoded correctly in base64 came out as mojibake in quoted-printable.
  //
  // As in PassThroughDecoder, a hard line break is only written once the next line
  // arrives, so the CRLF that belongs to a following boundary is not decoded as content,
  // and the break written is the one the line came with.
  update(line, lineBreak) {
    this.writeLineBreak();
    let len = line.length;
    while (len > 0 && isWsp(line[len - 1])) {
      len--;
    }
    const softBreak = len > 0 && line[len - 1] === CHR_EQUALS;
    if (softBreak) {
      len--;
    }
    let literalStart = 0;
    for (let i = 0; i < len; i++) {
      if (line[i] !== CHR_EQUALS || i + 2 >= len) {
        continue;
      }
      const high = hexNibble(line[i + 1]);
      const low = hexNibble(line[i + 2]);
      if (high < 0 || low < 0) {
        continue;
      }
      this.writeBytes(line, literalStart, i);
      this.writeByte(high << 4 | low);
      i += 2;
      literalStart = i + 1;
    }
    this.writeBytes(line, literalStart, len);
    this.pendingLineBreak = softBreak ? null : lineBreak;
  }
  // see PassThroughDecoder.finalize
  finalize(boundaryEnded = false) {
    if (!boundaryEnded) {
      this.writeLineBreak();
    }
    this.flushBuffer();
    return blobToArrayBuffer(new Blob(this.chunks, { type: "application/octet-stream" }));
  }
};

// node_modules/postal-mime/dist/esm/mime-node.js
var headerDecoder = new TextDecoder("utf-8", { ignoreBOM: true });
var trimWsp = (str) => {
  let start = 0;
  let end = str.length;
  while (start < end && isWsp(str.charCodeAt(start))) {
    start++;
  }
  while (end > start && isWsp(str.charCodeAt(end - 1))) {
    end--;
  }
  return str.slice(start, end);
};
var isBlankLine = (line) => line.every(isWsp);
var FIELD_START = /^[!-9;-~]+[ \t]*:/;
var CONTENT_HEADERS = /* @__PURE__ */ new Set([
  "content-type",
  "content-transfer-encoding",
  "content-disposition",
  "content-id",
  "content-description"
]);
var MimeNode = class {
  constructor(options) {
    this.options = options;
    this.postalMime = options.postalMime;
    this.postalMime.partCount++;
    if (this.postalMime.partCount > options.maxPartCount) {
      throw new Error(`Maximum MIME part count of ${options.maxPartCount} parts exceeded`);
    }
    this.childNodes = [];
    this.finalizedChildCount = 0;
    if (options.parentNode) {
      this.parentNode = options.parentNode;
      this.depth = this.parentNode.depth + 1;
      if (this.depth > options.maxNestingDepth) {
        throw new Error(`Maximum MIME nesting depth of ${options.maxNestingDepth} levels exceeded`);
      }
      options.parentNode.childNodes.push(this);
    } else {
      this.depth = 0;
    }
    this.state = "header";
    this.headerLines = [];
    this.lastLineBlank = false;
    const parentMultipartType = options.parentMultipartType || null;
    const defaultContentType = parentMultipartType === "digest" ? "message/rfc822" : "text/plain";
    this.contentType = {
      value: defaultContentType,
      parsed: { value: defaultContentType, params: {} },
      multipart: false
    };
    this.contentTransferEncoding = {
      value: "8bit",
      encoding: ""
    };
    this.contentDisposition = {
      value: "",
      parsed: { value: "", params: {} }
    };
    this.headers = [];
    this.rawHeaderLines = [];
    this.contentDecoder = null;
    this.content = null;
  }
  setupContentDecoder(transferEncoding) {
    if (/base64/i.test(transferEncoding)) {
      this.contentDecoder = new Base64Decoder();
    } else if (/quoted-printable/i.test(transferEncoding)) {
      this.contentDecoder = new QPDecoder();
    } else {
      this.contentDecoder = new PassThroughDecoder();
    }
  }
  // `boundaryEnded` tells that the part was ended by a multipart boundary, which owns
  // the line break after the last line of the part (RFC 2046 5.1.1)
  async finalize(boundaryEnded = false) {
    if (this.state === "finished") {
      return;
    }
    if (this.state === "header") {
      this.endHeaders();
    }
    let boundaries = this.postalMime.boundaries;
    for (let i = boundaries.length - 1; i >= 0; i--) {
      let boundary = boundaries[i];
      if (boundary.node === this) {
        boundaries.splice(i, 1);
        break;
      }
    }
    await this.finalizeChildNodes(boundaryEnded);
    this.content = this.contentDecoder ? await this.contentDecoder.finalize(boundaryEnded) : null;
    this.contentDecoder = null;
    this.state = "finished";
  }
  async finalizeChildNodes(boundaryEnded = false) {
    while (this.finalizedChildCount < this.childNodes.length) {
      await this.childNodes[this.finalizedChildCount++].finalize(boundaryEnded);
    }
  }
  // Strip RFC 822 comments (parenthesized text) from structured header values.
  //
  // Inside an unquoted parameter value a parenthesis that continues the current token is
  // content, because `filename=Invoice(1).pdf` is a filename and not a token followed by
  // a comment, and deleting the parens silently renames the attachment.
  stripComments(str) {
    let result = "";
    let depth = 0;
    let escaped = false;
    let inQuote = false;
    let commentStart = -1;
    let inParameterValue = false;
    let endsWithWsp = false;
    const append = (c) => {
      result += c;
      endsWithWsp = c === " " || c === "	";
    };
    const opensComment = () => !inParameterValue || !result.length || endsWithWsp;
    for (let i = 0; i < str.length; i++) {
      const chr = str.charAt(i);
      if (escaped) {
        if (depth === 0) {
          append(chr);
        }
        escaped = false;
        continue;
      }
      if (chr === "\\") {
        escaped = true;
        if (depth === 0) {
          append(chr);
        }
        continue;
      }
      if (chr === '"' && depth === 0) {
        inQuote = !inQuote;
        append(chr);
        continue;
      }
      if (!inQuote) {
        if (chr === "(" && opensComment()) {
          if (depth === 0) {
            commentStart = i;
          }
          depth++;
          continue;
        }
        if (chr === ")" && depth > 0) {
          depth--;
          continue;
        }
        if (depth === 0) {
          if (chr === "=") {
            inParameterValue = true;
          } else if (chr === ";") {
            inParameterValue = false;
          }
        }
      }
      if (depth === 0) {
        append(chr);
      }
    }
    if (depth === 0) {
      return result;
    }
    return str.indexOf(";", commentStart) < 0 ? result : str;
  }
  parseStructuredHeader(str) {
    str = this.stripComments(str);
    let response2 = {
      value: "",
      params: {}
    };
    let key = false;
    let value = "";
    let stage = "value";
    let pendingSpace = "";
    let quoteClosed = false;
    let quote = false;
    let escaped = false;
    let chr;
    const flushSpace = () => {
      if (value.length) {
        value += pendingSpace;
      }
      pendingSpace = "";
    };
    const addChr = (c) => {
      flushSpace();
      value += c;
    };
    const takeValue = () => {
      const result = value;
      value = "";
      pendingSpace = "";
      quoteClosed = false;
      return result;
    };
    const storeParam = (name, result) => {
      if (name && !Object.prototype.hasOwnProperty.call(response2.params, name)) {
        response2.params[name] = result;
      }
    };
    const storeValue = () => {
      const result = takeValue();
      if (key === false) {
        response2.value = result;
      } else {
        storeParam(key, result);
      }
    };
    const storeEmptyKey = () => {
      storeParam(takeValue().trim().toLowerCase(), "");
    };
    for (let i = 0, len = str.length; i < len; i++) {
      chr = str.charAt(i);
      switch (stage) {
        case "key":
          if (chr === "=") {
            key = takeValue().trim().toLowerCase();
            stage = "value";
            break;
          }
          if (chr === ";") {
            storeEmptyKey();
            break;
          }
          value += chr;
          break;
        case "value":
          if (quoteClosed && chr !== ";") {
            escaped = false;
            break;
          }
          if (escaped) {
            addChr(chr);
          } else if (quote && chr === "\\") {
            escaped = true;
            continue;
          } else if (quote && chr === quote) {
            quote = false;
            quoteClosed = true;
          } else if (!quote && chr === '"') {
            quote = chr;
            flushSpace();
          } else if (!quote && chr === ";") {
            storeValue();
            stage = "key";
          } else if (!quote && (chr === " " || chr === "	")) {
            pendingSpace += chr;
          } else {
            addChr(chr);
          }
          escaped = false;
          break;
      }
    }
    if (stage === "value") {
      storeValue();
    } else {
      storeEmptyKey();
    }
    if (response2.value) {
      response2.value = response2.value.toLowerCase();
    }
    decodeParameterValueContinuations(response2);
    return response2;
  }
  // Unfolds format=flowed text (RFC 3676 4.1). A line is read as its quote marks, one
  // space of stuffing and its content, in that order, and a line whose content ends in a
  // space is flowed: the next line of the same quote depth continues its paragraph. The
  // paragraph ends at a fixed line, at a change of quote depth and at the signature
  // separator, which is neither flowed nor fixed. Joining a quoted paragraph used to
  // leave the quote marks of the continuation lines inside the text, so a quoted reply
  // read `> one > two` instead of `> one two`.
  //
  // The quote marks and stuffing of the first line of a paragraph are kept as they were
  // written, so a fixed quoted line comes out exactly as it went in. An unquoted stuffed
  // line loses its stuffing, which is all the stuffing is for.
  decodeFlowedText(str, delSp) {
    const parts = [];
    let flowed = false;
    let depth = 0;
    const lines = str.split("\n");
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      let quoteDepth = 0;
      while (line.charCodeAt(quoteDepth) === 62) {
        quoteDepth++;
      }
      const contentStart = line.charCodeAt(quoteDepth) === 32 ? quoteDepth + 1 : quoteDepth;
      const content = line.slice(contentStart);
      const isSignature = content === "-- ";
      if (flowed && quoteDepth === depth && !isSignature) {
        if (delSp) {
          const last = parts[parts.length - 1];
          parts[parts.length - 1] = last.slice(0, -1);
        }
        parts.push(content);
      } else {
        if (i) {
          parts.push("\n");
        }
        parts.push(quoteDepth ? line : content);
      }
      flowed = !isSignature && content.endsWith(" ");
      depth = quoteDepth;
    }
    return parts.join("");
  }
  getTextContent() {
    if (!this.content) {
      return "";
    }
    let str = decodeText(this.content, this.contentType.parsed.params.charset, this.contentType.parsed.value === "text/html").replace(/\r\n/g, "\n");
    if (/^flowed$/i.test(this.contentType.parsed.params.format)) {
      str = this.decodeFlowedText(str, /^yes$/i.test(this.contentType.parsed.params.delsp));
    }
    return str;
  }
  processHeaders() {
    let foldedLines = [];
    for (let line of this.headerLines) {
      if (foldedLines.length && /^[ \t]/.test(line)) {
        foldedLines[foldedLines.length - 1].push(line);
      } else {
        foldedLines.push([line]);
      }
    }
    let seenContentHeaders = /* @__PURE__ */ new Set();
    for (let parts of foldedLines) {
      let rawLine = parts.join("\n");
      let sep = rawLine.indexOf(":");
      let rawKey2 = trimWsp(sep < 0 ? rawLine : rawLine.slice(0, sep));
      this.rawHeaderLines.push({
        key: rawKey2.toLowerCase(),
        line: rawLine
      });
      let unfoldedLine = parts.join("");
      sep = unfoldedLine.indexOf(":");
      let key = trimWsp(sep < 0 ? unfoldedLine : unfoldedLine.slice(0, sep));
      let value = sep < 0 ? "" : trimWsp(foldLineBreaks(unfoldedLine.slice(sep + 1)));
      this.headers.push({ key: key.toLowerCase(), originalKey: key, value });
      const lowerKey = key.toLowerCase();
      if (CONTENT_HEADERS.has(lowerKey) && !seenContentHeaders.has(lowerKey)) {
        seenContentHeaders.add(lowerKey);
        switch (lowerKey) {
          case "content-type":
            this.contentType.value = value;
            break;
          case "content-transfer-encoding":
            this.contentTransferEncoding.value = value;
            break;
          case "content-disposition":
            this.contentDisposition.value = value;
            break;
          case "content-id":
            this.contentId = value;
            break;
          case "content-description":
            this.contentDescription = value;
            break;
        }
      }
    }
    this.contentType.parsed = this.parseStructuredHeader(this.contentType.value);
    this.contentType.multipart = /^multipart\//i.test(this.contentType.parsed.value) ? this.contentType.parsed.value.slice(this.contentType.parsed.value.indexOf("/") + 1) : false;
    if (this.contentType.multipart && !this.contentType.parsed.params.boundary) {
      this.contentType.parsed.value = "text/plain";
      this.contentType.multipart = false;
    }
    if (this.contentType.multipart) {
      this.postalMime.boundaries.push({
        value: textEncoder.encode(this.contentType.parsed.params.boundary),
        node: this
      });
    }
    this.contentDisposition.parsed = this.parseStructuredHeader(this.contentDisposition.value);
    this.contentTransferEncoding.encoding = (this.stripComments(this.contentTransferEncoding.value).toLowerCase().match(/[\w-]+/) || [""])[0];
    this.setupContentDecoder(this.contentTransferEncoding.encoding);
  }
  // Ends the header section and sets up the body
  endHeaders() {
    this.state = "body";
    this.processHeaders();
  }
  // Returns true when the line was not consumed: the headers ended with the line before
  // it, and the parser has to read it again, since the boundary this part declares was
  // not known yet when the line was checked for one
  feed(line, lineBreak) {
    switch (this.state) {
      case "header": {
        if (!line.length) {
          this.endHeaders();
          return false;
        }
        this.postalMime.headerSize += line.length;
        if (this.postalMime.headerSize > this.options.maxHeadersSize) {
          let error = new Error(`Maximum header size of ${this.options.maxHeadersSize} bytes exceeded`);
          throw error;
        }
        const text = decodeUtf8OrWindows1252(line, headerDecoder);
        if (this.lastLineBlank && !isWsp(line[0]) && !FIELD_START.test(text)) {
          this.headerLines.pop();
          this.endHeaders();
          return true;
        }
        this.headerLines.push(text);
        this.lastLineBlank = isBlankLine(line);
        break;
      }
      case "body":
        if (this.contentDecoder) {
          this.contentDecoder.update(line, lineBreak);
        }
        break;
    }
    return false;
  }
};

// node_modules/postal-mime/dist/esm/html-entities.js
var htmlEntities = {
  "&AElig": "\xC6",
  "&AElig;": "\xC6",
  "&AMP": "&",
  "&AMP;": "&",
  "&Aacute": "\xC1",
  "&Aacute;": "\xC1",
  "&Abreve;": "\u0102",
  "&Acirc": "\xC2",
  "&Acirc;": "\xC2",
  "&Acy;": "\u0410",
  "&Afr;": "\u{1D504}",
  "&Agrave": "\xC0",
  "&Agrave;": "\xC0",
  "&Alpha;": "\u0391",
  "&Amacr;": "\u0100",
  "&And;": "\u2A53",
  "&Aogon;": "\u0104",
  "&Aopf;": "\u{1D538}",
  "&ApplyFunction;": "\u2061",
  "&Aring": "\xC5",
  "&Aring;": "\xC5",
  "&Ascr;": "\u{1D49C}",
  "&Assign;": "\u2254",
  "&Atilde": "\xC3",
  "&Atilde;": "\xC3",
  "&Auml": "\xC4",
  "&Auml;": "\xC4",
  "&Backslash;": "\u2216",
  "&Barv;": "\u2AE7",
  "&Barwed;": "\u2306",
  "&Bcy;": "\u0411",
  "&Because;": "\u2235",
  "&Bernoullis;": "\u212C",
  "&Beta;": "\u0392",
  "&Bfr;": "\u{1D505}",
  "&Bopf;": "\u{1D539}",
  "&Breve;": "\u02D8",
  "&Bscr;": "\u212C",
  "&Bumpeq;": "\u224E",
  "&CHcy;": "\u0427",
  "&COPY": "\xA9",
  "&COPY;": "\xA9",
  "&Cacute;": "\u0106",
  "&Cap;": "\u22D2",
  "&CapitalDifferentialD;": "\u2145",
  "&Cayleys;": "\u212D",
  "&Ccaron;": "\u010C",
  "&Ccedil": "\xC7",
  "&Ccedil;": "\xC7",
  "&Ccirc;": "\u0108",
  "&Cconint;": "\u2230",
  "&Cdot;": "\u010A",
  "&Cedilla;": "\xB8",
  "&CenterDot;": "\xB7",
  "&Cfr;": "\u212D",
  "&Chi;": "\u03A7",
  "&CircleDot;": "\u2299",
  "&CircleMinus;": "\u2296",
  "&CirclePlus;": "\u2295",
  "&CircleTimes;": "\u2297",
  "&ClockwiseContourIntegral;": "\u2232",
  "&CloseCurlyDoubleQuote;": "\u201D",
  "&CloseCurlyQuote;": "\u2019",
  "&Colon;": "\u2237",
  "&Colone;": "\u2A74",
  "&Congruent;": "\u2261",
  "&Conint;": "\u222F",
  "&ContourIntegral;": "\u222E",
  "&Copf;": "\u2102",
  "&Coproduct;": "\u2210",
  "&CounterClockwiseContourIntegral;": "\u2233",
  "&Cross;": "\u2A2F",
  "&Cscr;": "\u{1D49E}",
  "&Cup;": "\u22D3",
  "&CupCap;": "\u224D",
  "&DD;": "\u2145",
  "&DDotrahd;": "\u2911",
  "&DJcy;": "\u0402",
  "&DScy;": "\u0405",
  "&DZcy;": "\u040F",
  "&Dagger;": "\u2021",
  "&Darr;": "\u21A1",
  "&Dashv;": "\u2AE4",
  "&Dcaron;": "\u010E",
  "&Dcy;": "\u0414",
  "&Del;": "\u2207",
  "&Delta;": "\u0394",
  "&Dfr;": "\u{1D507}",
  "&DiacriticalAcute;": "\xB4",
  "&DiacriticalDot;": "\u02D9",
  "&DiacriticalDoubleAcute;": "\u02DD",
  "&DiacriticalGrave;": "`",
  "&DiacriticalTilde;": "\u02DC",
  "&Diamond;": "\u22C4",
  "&DifferentialD;": "\u2146",
  "&Dopf;": "\u{1D53B}",
  "&Dot;": "\xA8",
  "&DotDot;": "\u20DC",
  "&DotEqual;": "\u2250",
  "&DoubleContourIntegral;": "\u222F",
  "&DoubleDot;": "\xA8",
  "&DoubleDownArrow;": "\u21D3",
  "&DoubleLeftArrow;": "\u21D0",
  "&DoubleLeftRightArrow;": "\u21D4",
  "&DoubleLeftTee;": "\u2AE4",
  "&DoubleLongLeftArrow;": "\u27F8",
  "&DoubleLongLeftRightArrow;": "\u27FA",
  "&DoubleLongRightArrow;": "\u27F9",
  "&DoubleRightArrow;": "\u21D2",
  "&DoubleRightTee;": "\u22A8",
  "&DoubleUpArrow;": "\u21D1",
  "&DoubleUpDownArrow;": "\u21D5",
  "&DoubleVerticalBar;": "\u2225",
  "&DownArrow;": "\u2193",
  "&DownArrowBar;": "\u2913",
  "&DownArrowUpArrow;": "\u21F5",
  "&DownBreve;": "\u0311",
  "&DownLeftRightVector;": "\u2950",
  "&DownLeftTeeVector;": "\u295E",
  "&DownLeftVector;": "\u21BD",
  "&DownLeftVectorBar;": "\u2956",
  "&DownRightTeeVector;": "\u295F",
  "&DownRightVector;": "\u21C1",
  "&DownRightVectorBar;": "\u2957",
  "&DownTee;": "\u22A4",
  "&DownTeeArrow;": "\u21A7",
  "&Downarrow;": "\u21D3",
  "&Dscr;": "\u{1D49F}",
  "&Dstrok;": "\u0110",
  "&ENG;": "\u014A",
  "&ETH": "\xD0",
  "&ETH;": "\xD0",
  "&Eacute": "\xC9",
  "&Eacute;": "\xC9",
  "&Ecaron;": "\u011A",
  "&Ecirc": "\xCA",
  "&Ecirc;": "\xCA",
  "&Ecy;": "\u042D",
  "&Edot;": "\u0116",
  "&Efr;": "\u{1D508}",
  "&Egrave": "\xC8",
  "&Egrave;": "\xC8",
  "&Element;": "\u2208",
  "&Emacr;": "\u0112",
  "&EmptySmallSquare;": "\u25FB",
  "&EmptyVerySmallSquare;": "\u25AB",
  "&Eogon;": "\u0118",
  "&Eopf;": "\u{1D53C}",
  "&Epsilon;": "\u0395",
  "&Equal;": "\u2A75",
  "&EqualTilde;": "\u2242",
  "&Equilibrium;": "\u21CC",
  "&Escr;": "\u2130",
  "&Esim;": "\u2A73",
  "&Eta;": "\u0397",
  "&Euml": "\xCB",
  "&Euml;": "\xCB",
  "&Exists;": "\u2203",
  "&ExponentialE;": "\u2147",
  "&Fcy;": "\u0424",
  "&Ffr;": "\u{1D509}",
  "&FilledSmallSquare;": "\u25FC",
  "&FilledVerySmallSquare;": "\u25AA",
  "&Fopf;": "\u{1D53D}",
  "&ForAll;": "\u2200",
  "&Fouriertrf;": "\u2131",
  "&Fscr;": "\u2131",
  "&GJcy;": "\u0403",
  "&GT": ">",
  "&GT;": ">",
  "&Gamma;": "\u0393",
  "&Gammad;": "\u03DC",
  "&Gbreve;": "\u011E",
  "&Gcedil;": "\u0122",
  "&Gcirc;": "\u011C",
  "&Gcy;": "\u0413",
  "&Gdot;": "\u0120",
  "&Gfr;": "\u{1D50A}",
  "&Gg;": "\u22D9",
  "&Gopf;": "\u{1D53E}",
  "&GreaterEqual;": "\u2265",
  "&GreaterEqualLess;": "\u22DB",
  "&GreaterFullEqual;": "\u2267",
  "&GreaterGreater;": "\u2AA2",
  "&GreaterLess;": "\u2277",
  "&GreaterSlantEqual;": "\u2A7E",
  "&GreaterTilde;": "\u2273",
  "&Gscr;": "\u{1D4A2}",
  "&Gt;": "\u226B",
  "&HARDcy;": "\u042A",
  "&Hacek;": "\u02C7",
  "&Hat;": "^",
  "&Hcirc;": "\u0124",
  "&Hfr;": "\u210C",
  "&HilbertSpace;": "\u210B",
  "&Hopf;": "\u210D",
  "&HorizontalLine;": "\u2500",
  "&Hscr;": "\u210B",
  "&Hstrok;": "\u0126",
  "&HumpDownHump;": "\u224E",
  "&HumpEqual;": "\u224F",
  "&IEcy;": "\u0415",
  "&IJlig;": "\u0132",
  "&IOcy;": "\u0401",
  "&Iacute": "\xCD",
  "&Iacute;": "\xCD",
  "&Icirc": "\xCE",
  "&Icirc;": "\xCE",
  "&Icy;": "\u0418",
  "&Idot;": "\u0130",
  "&Ifr;": "\u2111",
  "&Igrave": "\xCC",
  "&Igrave;": "\xCC",
  "&Im;": "\u2111",
  "&Imacr;": "\u012A",
  "&ImaginaryI;": "\u2148",
  "&Implies;": "\u21D2",
  "&Int;": "\u222C",
  "&Integral;": "\u222B",
  "&Intersection;": "\u22C2",
  "&InvisibleComma;": "\u2063",
  "&InvisibleTimes;": "\u2062",
  "&Iogon;": "\u012E",
  "&Iopf;": "\u{1D540}",
  "&Iota;": "\u0399",
  "&Iscr;": "\u2110",
  "&Itilde;": "\u0128",
  "&Iukcy;": "\u0406",
  "&Iuml": "\xCF",
  "&Iuml;": "\xCF",
  "&Jcirc;": "\u0134",
  "&Jcy;": "\u0419",
  "&Jfr;": "\u{1D50D}",
  "&Jopf;": "\u{1D541}",
  "&Jscr;": "\u{1D4A5}",
  "&Jsercy;": "\u0408",
  "&Jukcy;": "\u0404",
  "&KHcy;": "\u0425",
  "&KJcy;": "\u040C",
  "&Kappa;": "\u039A",
  "&Kcedil;": "\u0136",
  "&Kcy;": "\u041A",
  "&Kfr;": "\u{1D50E}",
  "&Kopf;": "\u{1D542}",
  "&Kscr;": "\u{1D4A6}",
  "&LJcy;": "\u0409",
  "&LT": "<",
  "&LT;": "<",
  "&Lacute;": "\u0139",
  "&Lambda;": "\u039B",
  "&Lang;": "\u27EA",
  "&Laplacetrf;": "\u2112",
  "&Larr;": "\u219E",
  "&Lcaron;": "\u013D",
  "&Lcedil;": "\u013B",
  "&Lcy;": "\u041B",
  "&LeftAngleBracket;": "\u27E8",
  "&LeftArrow;": "\u2190",
  "&LeftArrowBar;": "\u21E4",
  "&LeftArrowRightArrow;": "\u21C6",
  "&LeftCeiling;": "\u2308",
  "&LeftDoubleBracket;": "\u27E6",
  "&LeftDownTeeVector;": "\u2961",
  "&LeftDownVector;": "\u21C3",
  "&LeftDownVectorBar;": "\u2959",
  "&LeftFloor;": "\u230A",
  "&LeftRightArrow;": "\u2194",
  "&LeftRightVector;": "\u294E",
  "&LeftTee;": "\u22A3",
  "&LeftTeeArrow;": "\u21A4",
  "&LeftTeeVector;": "\u295A",
  "&LeftTriangle;": "\u22B2",
  "&LeftTriangleBar;": "\u29CF",
  "&LeftTriangleEqual;": "\u22B4",
  "&LeftUpDownVector;": "\u2951",
  "&LeftUpTeeVector;": "\u2960",
  "&LeftUpVector;": "\u21BF",
  "&LeftUpVectorBar;": "\u2958",
  "&LeftVector;": "\u21BC",
  "&LeftVectorBar;": "\u2952",
  "&Leftarrow;": "\u21D0",
  "&Leftrightarrow;": "\u21D4",
  "&LessEqualGreater;": "\u22DA",
  "&LessFullEqual;": "\u2266",
  "&LessGreater;": "\u2276",
  "&LessLess;": "\u2AA1",
  "&LessSlantEqual;": "\u2A7D",
  "&LessTilde;": "\u2272",
  "&Lfr;": "\u{1D50F}",
  "&Ll;": "\u22D8",
  "&Lleftarrow;": "\u21DA",
  "&Lmidot;": "\u013F",
  "&LongLeftArrow;": "\u27F5",
  "&LongLeftRightArrow;": "\u27F7",
  "&LongRightArrow;": "\u27F6",
  "&Longleftarrow;": "\u27F8",
  "&Longleftrightarrow;": "\u27FA",
  "&Longrightarrow;": "\u27F9",
  "&Lopf;": "\u{1D543}",
  "&LowerLeftArrow;": "\u2199",
  "&LowerRightArrow;": "\u2198",
  "&Lscr;": "\u2112",
  "&Lsh;": "\u21B0",
  "&Lstrok;": "\u0141",
  "&Lt;": "\u226A",
  "&Map;": "\u2905",
  "&Mcy;": "\u041C",
  "&MediumSpace;": "\u205F",
  "&Mellintrf;": "\u2133",
  "&Mfr;": "\u{1D510}",
  "&MinusPlus;": "\u2213",
  "&Mopf;": "\u{1D544}",
  "&Mscr;": "\u2133",
  "&Mu;": "\u039C",
  "&NJcy;": "\u040A",
  "&Nacute;": "\u0143",
  "&Ncaron;": "\u0147",
  "&Ncedil;": "\u0145",
  "&Ncy;": "\u041D",
  "&NegativeMediumSpace;": "\u200B",
  "&NegativeThickSpace;": "\u200B",
  "&NegativeThinSpace;": "\u200B",
  "&NegativeVeryThinSpace;": "\u200B",
  "&NestedGreaterGreater;": "\u226B",
  "&NestedLessLess;": "\u226A",
  "&NewLine;": "\n",
  "&Nfr;": "\u{1D511}",
  "&NoBreak;": "\u2060",
  "&NonBreakingSpace;": "\xA0",
  "&Nopf;": "\u2115",
  "&Not;": "\u2AEC",
  "&NotCongruent;": "\u2262",
  "&NotCupCap;": "\u226D",
  "&NotDoubleVerticalBar;": "\u2226",
  "&NotElement;": "\u2209",
  "&NotEqual;": "\u2260",
  "&NotEqualTilde;": "\u2242\u0338",
  "&NotExists;": "\u2204",
  "&NotGreater;": "\u226F",
  "&NotGreaterEqual;": "\u2271",
  "&NotGreaterFullEqual;": "\u2267\u0338",
  "&NotGreaterGreater;": "\u226B\u0338",
  "&NotGreaterLess;": "\u2279",
  "&NotGreaterSlantEqual;": "\u2A7E\u0338",
  "&NotGreaterTilde;": "\u2275",
  "&NotHumpDownHump;": "\u224E\u0338",
  "&NotHumpEqual;": "\u224F\u0338",
  "&NotLeftTriangle;": "\u22EA",
  "&NotLeftTriangleBar;": "\u29CF\u0338",
  "&NotLeftTriangleEqual;": "\u22EC",
  "&NotLess;": "\u226E",
  "&NotLessEqual;": "\u2270",
  "&NotLessGreater;": "\u2278",
  "&NotLessLess;": "\u226A\u0338",
  "&NotLessSlantEqual;": "\u2A7D\u0338",
  "&NotLessTilde;": "\u2274",
  "&NotNestedGreaterGreater;": "\u2AA2\u0338",
  "&NotNestedLessLess;": "\u2AA1\u0338",
  "&NotPrecedes;": "\u2280",
  "&NotPrecedesEqual;": "\u2AAF\u0338",
  "&NotPrecedesSlantEqual;": "\u22E0",
  "&NotReverseElement;": "\u220C",
  "&NotRightTriangle;": "\u22EB",
  "&NotRightTriangleBar;": "\u29D0\u0338",
  "&NotRightTriangleEqual;": "\u22ED",
  "&NotSquareSubset;": "\u228F\u0338",
  "&NotSquareSubsetEqual;": "\u22E2",
  "&NotSquareSuperset;": "\u2290\u0338",
  "&NotSquareSupersetEqual;": "\u22E3",
  "&NotSubset;": "\u2282\u20D2",
  "&NotSubsetEqual;": "\u2288",
  "&NotSucceeds;": "\u2281",
  "&NotSucceedsEqual;": "\u2AB0\u0338",
  "&NotSucceedsSlantEqual;": "\u22E1",
  "&NotSucceedsTilde;": "\u227F\u0338",
  "&NotSuperset;": "\u2283\u20D2",
  "&NotSupersetEqual;": "\u2289",
  "&NotTilde;": "\u2241",
  "&NotTildeEqual;": "\u2244",
  "&NotTildeFullEqual;": "\u2247",
  "&NotTildeTilde;": "\u2249",
  "&NotVerticalBar;": "\u2224",
  "&Nscr;": "\u{1D4A9}",
  "&Ntilde": "\xD1",
  "&Ntilde;": "\xD1",
  "&Nu;": "\u039D",
  "&OElig;": "\u0152",
  "&Oacute": "\xD3",
  "&Oacute;": "\xD3",
  "&Ocirc": "\xD4",
  "&Ocirc;": "\xD4",
  "&Ocy;": "\u041E",
  "&Odblac;": "\u0150",
  "&Ofr;": "\u{1D512}",
  "&Ograve": "\xD2",
  "&Ograve;": "\xD2",
  "&Omacr;": "\u014C",
  "&Omega;": "\u03A9",
  "&Omicron;": "\u039F",
  "&Oopf;": "\u{1D546}",
  "&OpenCurlyDoubleQuote;": "\u201C",
  "&OpenCurlyQuote;": "\u2018",
  "&Or;": "\u2A54",
  "&Oscr;": "\u{1D4AA}",
  "&Oslash": "\xD8",
  "&Oslash;": "\xD8",
  "&Otilde": "\xD5",
  "&Otilde;": "\xD5",
  "&Otimes;": "\u2A37",
  "&Ouml": "\xD6",
  "&Ouml;": "\xD6",
  "&OverBar;": "\u203E",
  "&OverBrace;": "\u23DE",
  "&OverBracket;": "\u23B4",
  "&OverParenthesis;": "\u23DC",
  "&PartialD;": "\u2202",
  "&Pcy;": "\u041F",
  "&Pfr;": "\u{1D513}",
  "&Phi;": "\u03A6",
  "&Pi;": "\u03A0",
  "&PlusMinus;": "\xB1",
  "&Poincareplane;": "\u210C",
  "&Popf;": "\u2119",
  "&Pr;": "\u2ABB",
  "&Precedes;": "\u227A",
  "&PrecedesEqual;": "\u2AAF",
  "&PrecedesSlantEqual;": "\u227C",
  "&PrecedesTilde;": "\u227E",
  "&Prime;": "\u2033",
  "&Product;": "\u220F",
  "&Proportion;": "\u2237",
  "&Proportional;": "\u221D",
  "&Pscr;": "\u{1D4AB}",
  "&Psi;": "\u03A8",
  "&QUOT": '"',
  "&QUOT;": '"',
  "&Qfr;": "\u{1D514}",
  "&Qopf;": "\u211A",
  "&Qscr;": "\u{1D4AC}",
  "&RBarr;": "\u2910",
  "&REG": "\xAE",
  "&REG;": "\xAE",
  "&Racute;": "\u0154",
  "&Rang;": "\u27EB",
  "&Rarr;": "\u21A0",
  "&Rarrtl;": "\u2916",
  "&Rcaron;": "\u0158",
  "&Rcedil;": "\u0156",
  "&Rcy;": "\u0420",
  "&Re;": "\u211C",
  "&ReverseElement;": "\u220B",
  "&ReverseEquilibrium;": "\u21CB",
  "&ReverseUpEquilibrium;": "\u296F",
  "&Rfr;": "\u211C",
  "&Rho;": "\u03A1",
  "&RightAngleBracket;": "\u27E9",
  "&RightArrow;": "\u2192",
  "&RightArrowBar;": "\u21E5",
  "&RightArrowLeftArrow;": "\u21C4",
  "&RightCeiling;": "\u2309",
  "&RightDoubleBracket;": "\u27E7",
  "&RightDownTeeVector;": "\u295D",
  "&RightDownVector;": "\u21C2",
  "&RightDownVectorBar;": "\u2955",
  "&RightFloor;": "\u230B",
  "&RightTee;": "\u22A2",
  "&RightTeeArrow;": "\u21A6",
  "&RightTeeVector;": "\u295B",
  "&RightTriangle;": "\u22B3",
  "&RightTriangleBar;": "\u29D0",
  "&RightTriangleEqual;": "\u22B5",
  "&RightUpDownVector;": "\u294F",
  "&RightUpTeeVector;": "\u295C",
  "&RightUpVector;": "\u21BE",
  "&RightUpVectorBar;": "\u2954",
  "&RightVector;": "\u21C0",
  "&RightVectorBar;": "\u2953",
  "&Rightarrow;": "\u21D2",
  "&Ropf;": "\u211D",
  "&RoundImplies;": "\u2970",
  "&Rrightarrow;": "\u21DB",
  "&Rscr;": "\u211B",
  "&Rsh;": "\u21B1",
  "&RuleDelayed;": "\u29F4",
  "&SHCHcy;": "\u0429",
  "&SHcy;": "\u0428",
  "&SOFTcy;": "\u042C",
  "&Sacute;": "\u015A",
  "&Sc;": "\u2ABC",
  "&Scaron;": "\u0160",
  "&Scedil;": "\u015E",
  "&Scirc;": "\u015C",
  "&Scy;": "\u0421",
  "&Sfr;": "\u{1D516}",
  "&ShortDownArrow;": "\u2193",
  "&ShortLeftArrow;": "\u2190",
  "&ShortRightArrow;": "\u2192",
  "&ShortUpArrow;": "\u2191",
  "&Sigma;": "\u03A3",
  "&SmallCircle;": "\u2218",
  "&Sopf;": "\u{1D54A}",
  "&Sqrt;": "\u221A",
  "&Square;": "\u25A1",
  "&SquareIntersection;": "\u2293",
  "&SquareSubset;": "\u228F",
  "&SquareSubsetEqual;": "\u2291",
  "&SquareSuperset;": "\u2290",
  "&SquareSupersetEqual;": "\u2292",
  "&SquareUnion;": "\u2294",
  "&Sscr;": "\u{1D4AE}",
  "&Star;": "\u22C6",
  "&Sub;": "\u22D0",
  "&Subset;": "\u22D0",
  "&SubsetEqual;": "\u2286",
  "&Succeeds;": "\u227B",
  "&SucceedsEqual;": "\u2AB0",
  "&SucceedsSlantEqual;": "\u227D",
  "&SucceedsTilde;": "\u227F",
  "&SuchThat;": "\u220B",
  "&Sum;": "\u2211",
  "&Sup;": "\u22D1",
  "&Superset;": "\u2283",
  "&SupersetEqual;": "\u2287",
  "&Supset;": "\u22D1",
  "&THORN": "\xDE",
  "&THORN;": "\xDE",
  "&TRADE;": "\u2122",
  "&TSHcy;": "\u040B",
  "&TScy;": "\u0426",
  "&Tab;": "	",
  "&Tau;": "\u03A4",
  "&Tcaron;": "\u0164",
  "&Tcedil;": "\u0162",
  "&Tcy;": "\u0422",
  "&Tfr;": "\u{1D517}",
  "&Therefore;": "\u2234",
  "&Theta;": "\u0398",
  "&ThickSpace;": "\u205F\u200A",
  "&ThinSpace;": "\u2009",
  "&Tilde;": "\u223C",
  "&TildeEqual;": "\u2243",
  "&TildeFullEqual;": "\u2245",
  "&TildeTilde;": "\u2248",
  "&Topf;": "\u{1D54B}",
  "&TripleDot;": "\u20DB",
  "&Tscr;": "\u{1D4AF}",
  "&Tstrok;": "\u0166",
  "&Uacute": "\xDA",
  "&Uacute;": "\xDA",
  "&Uarr;": "\u219F",
  "&Uarrocir;": "\u2949",
  "&Ubrcy;": "\u040E",
  "&Ubreve;": "\u016C",
  "&Ucirc": "\xDB",
  "&Ucirc;": "\xDB",
  "&Ucy;": "\u0423",
  "&Udblac;": "\u0170",
  "&Ufr;": "\u{1D518}",
  "&Ugrave": "\xD9",
  "&Ugrave;": "\xD9",
  "&Umacr;": "\u016A",
  "&UnderBar;": "_",
  "&UnderBrace;": "\u23DF",
  "&UnderBracket;": "\u23B5",
  "&UnderParenthesis;": "\u23DD",
  "&Union;": "\u22C3",
  "&UnionPlus;": "\u228E",
  "&Uogon;": "\u0172",
  "&Uopf;": "\u{1D54C}",
  "&UpArrow;": "\u2191",
  "&UpArrowBar;": "\u2912",
  "&UpArrowDownArrow;": "\u21C5",
  "&UpDownArrow;": "\u2195",
  "&UpEquilibrium;": "\u296E",
  "&UpTee;": "\u22A5",
  "&UpTeeArrow;": "\u21A5",
  "&Uparrow;": "\u21D1",
  "&Updownarrow;": "\u21D5",
  "&UpperLeftArrow;": "\u2196",
  "&UpperRightArrow;": "\u2197",
  "&Upsi;": "\u03D2",
  "&Upsilon;": "\u03A5",
  "&Uring;": "\u016E",
  "&Uscr;": "\u{1D4B0}",
  "&Utilde;": "\u0168",
  "&Uuml": "\xDC",
  "&Uuml;": "\xDC",
  "&VDash;": "\u22AB",
  "&Vbar;": "\u2AEB",
  "&Vcy;": "\u0412",
  "&Vdash;": "\u22A9",
  "&Vdashl;": "\u2AE6",
  "&Vee;": "\u22C1",
  "&Verbar;": "\u2016",
  "&Vert;": "\u2016",
  "&VerticalBar;": "\u2223",
  "&VerticalLine;": "|",
  "&VerticalSeparator;": "\u2758",
  "&VerticalTilde;": "\u2240",
  "&VeryThinSpace;": "\u200A",
  "&Vfr;": "\u{1D519}",
  "&Vopf;": "\u{1D54D}",
  "&Vscr;": "\u{1D4B1}",
  "&Vvdash;": "\u22AA",
  "&Wcirc;": "\u0174",
  "&Wedge;": "\u22C0",
  "&Wfr;": "\u{1D51A}",
  "&Wopf;": "\u{1D54E}",
  "&Wscr;": "\u{1D4B2}",
  "&Xfr;": "\u{1D51B}",
  "&Xi;": "\u039E",
  "&Xopf;": "\u{1D54F}",
  "&Xscr;": "\u{1D4B3}",
  "&YAcy;": "\u042F",
  "&YIcy;": "\u0407",
  "&YUcy;": "\u042E",
  "&Yacute": "\xDD",
  "&Yacute;": "\xDD",
  "&Ycirc;": "\u0176",
  "&Ycy;": "\u042B",
  "&Yfr;": "\u{1D51C}",
  "&Yopf;": "\u{1D550}",
  "&Yscr;": "\u{1D4B4}",
  "&Yuml;": "\u0178",
  "&ZHcy;": "\u0416",
  "&Zacute;": "\u0179",
  "&Zcaron;": "\u017D",
  "&Zcy;": "\u0417",
  "&Zdot;": "\u017B",
  "&ZeroWidthSpace;": "\u200B",
  "&Zeta;": "\u0396",
  "&Zfr;": "\u2128",
  "&Zopf;": "\u2124",
  "&Zscr;": "\u{1D4B5}",
  "&aacute": "\xE1",
  "&aacute;": "\xE1",
  "&abreve;": "\u0103",
  "&ac;": "\u223E",
  "&acE;": "\u223E\u0333",
  "&acd;": "\u223F",
  "&acirc": "\xE2",
  "&acirc;": "\xE2",
  "&acute": "\xB4",
  "&acute;": "\xB4",
  "&acy;": "\u0430",
  "&aelig": "\xE6",
  "&aelig;": "\xE6",
  "&af;": "\u2061",
  "&afr;": "\u{1D51E}",
  "&agrave": "\xE0",
  "&agrave;": "\xE0",
  "&alefsym;": "\u2135",
  "&aleph;": "\u2135",
  "&alpha;": "\u03B1",
  "&amacr;": "\u0101",
  "&amalg;": "\u2A3F",
  "&amp": "&",
  "&amp;": "&",
  "&and;": "\u2227",
  "&andand;": "\u2A55",
  "&andd;": "\u2A5C",
  "&andslope;": "\u2A58",
  "&andv;": "\u2A5A",
  "&ang;": "\u2220",
  "&ange;": "\u29A4",
  "&angle;": "\u2220",
  "&angmsd;": "\u2221",
  "&angmsdaa;": "\u29A8",
  "&angmsdab;": "\u29A9",
  "&angmsdac;": "\u29AA",
  "&angmsdad;": "\u29AB",
  "&angmsdae;": "\u29AC",
  "&angmsdaf;": "\u29AD",
  "&angmsdag;": "\u29AE",
  "&angmsdah;": "\u29AF",
  "&angrt;": "\u221F",
  "&angrtvb;": "\u22BE",
  "&angrtvbd;": "\u299D",
  "&angsph;": "\u2222",
  "&angst;": "\xC5",
  "&angzarr;": "\u237C",
  "&aogon;": "\u0105",
  "&aopf;": "\u{1D552}",
  "&ap;": "\u2248",
  "&apE;": "\u2A70",
  "&apacir;": "\u2A6F",
  "&ape;": "\u224A",
  "&apid;": "\u224B",
  "&apos;": "'",
  "&approx;": "\u2248",
  "&approxeq;": "\u224A",
  "&aring": "\xE5",
  "&aring;": "\xE5",
  "&ascr;": "\u{1D4B6}",
  "&ast;": "*",
  "&asymp;": "\u2248",
  "&asympeq;": "\u224D",
  "&atilde": "\xE3",
  "&atilde;": "\xE3",
  "&auml": "\xE4",
  "&auml;": "\xE4",
  "&awconint;": "\u2233",
  "&awint;": "\u2A11",
  "&bNot;": "\u2AED",
  "&backcong;": "\u224C",
  "&backepsilon;": "\u03F6",
  "&backprime;": "\u2035",
  "&backsim;": "\u223D",
  "&backsimeq;": "\u22CD",
  "&barvee;": "\u22BD",
  "&barwed;": "\u2305",
  "&barwedge;": "\u2305",
  "&bbrk;": "\u23B5",
  "&bbrktbrk;": "\u23B6",
  "&bcong;": "\u224C",
  "&bcy;": "\u0431",
  "&bdquo;": "\u201E",
  "&becaus;": "\u2235",
  "&because;": "\u2235",
  "&bemptyv;": "\u29B0",
  "&bepsi;": "\u03F6",
  "&bernou;": "\u212C",
  "&beta;": "\u03B2",
  "&beth;": "\u2136",
  "&between;": "\u226C",
  "&bfr;": "\u{1D51F}",
  "&bigcap;": "\u22C2",
  "&bigcirc;": "\u25EF",
  "&bigcup;": "\u22C3",
  "&bigodot;": "\u2A00",
  "&bigoplus;": "\u2A01",
  "&bigotimes;": "\u2A02",
  "&bigsqcup;": "\u2A06",
  "&bigstar;": "\u2605",
  "&bigtriangledown;": "\u25BD",
  "&bigtriangleup;": "\u25B3",
  "&biguplus;": "\u2A04",
  "&bigvee;": "\u22C1",
  "&bigwedge;": "\u22C0",
  "&bkarow;": "\u290D",
  "&blacklozenge;": "\u29EB",
  "&blacksquare;": "\u25AA",
  "&blacktriangle;": "\u25B4",
  "&blacktriangledown;": "\u25BE",
  "&blacktriangleleft;": "\u25C2",
  "&blacktriangleright;": "\u25B8",
  "&blank;": "\u2423",
  "&blk12;": "\u2592",
  "&blk14;": "\u2591",
  "&blk34;": "\u2593",
  "&block;": "\u2588",
  "&bne;": "=\u20E5",
  "&bnequiv;": "\u2261\u20E5",
  "&bnot;": "\u2310",
  "&bopf;": "\u{1D553}",
  "&bot;": "\u22A5",
  "&bottom;": "\u22A5",
  "&bowtie;": "\u22C8",
  "&boxDL;": "\u2557",
  "&boxDR;": "\u2554",
  "&boxDl;": "\u2556",
  "&boxDr;": "\u2553",
  "&boxH;": "\u2550",
  "&boxHD;": "\u2566",
  "&boxHU;": "\u2569",
  "&boxHd;": "\u2564",
  "&boxHu;": "\u2567",
  "&boxUL;": "\u255D",
  "&boxUR;": "\u255A",
  "&boxUl;": "\u255C",
  "&boxUr;": "\u2559",
  "&boxV;": "\u2551",
  "&boxVH;": "\u256C",
  "&boxVL;": "\u2563",
  "&boxVR;": "\u2560",
  "&boxVh;": "\u256B",
  "&boxVl;": "\u2562",
  "&boxVr;": "\u255F",
  "&boxbox;": "\u29C9",
  "&boxdL;": "\u2555",
  "&boxdR;": "\u2552",
  "&boxdl;": "\u2510",
  "&boxdr;": "\u250C",
  "&boxh;": "\u2500",
  "&boxhD;": "\u2565",
  "&boxhU;": "\u2568",
  "&boxhd;": "\u252C",
  "&boxhu;": "\u2534",
  "&boxminus;": "\u229F",
  "&boxplus;": "\u229E",
  "&boxtimes;": "\u22A0",
  "&boxuL;": "\u255B",
  "&boxuR;": "\u2558",
  "&boxul;": "\u2518",
  "&boxur;": "\u2514",
  "&boxv;": "\u2502",
  "&boxvH;": "\u256A",
  "&boxvL;": "\u2561",
  "&boxvR;": "\u255E",
  "&boxvh;": "\u253C",
  "&boxvl;": "\u2524",
  "&boxvr;": "\u251C",
  "&bprime;": "\u2035",
  "&breve;": "\u02D8",
  "&brvbar": "\xA6",
  "&brvbar;": "\xA6",
  "&bscr;": "\u{1D4B7}",
  "&bsemi;": "\u204F",
  "&bsim;": "\u223D",
  "&bsime;": "\u22CD",
  "&bsol;": "\\",
  "&bsolb;": "\u29C5",
  "&bsolhsub;": "\u27C8",
  "&bull;": "\u2022",
  "&bullet;": "\u2022",
  "&bump;": "\u224E",
  "&bumpE;": "\u2AAE",
  "&bumpe;": "\u224F",
  "&bumpeq;": "\u224F",
  "&cacute;": "\u0107",
  "&cap;": "\u2229",
  "&capand;": "\u2A44",
  "&capbrcup;": "\u2A49",
  "&capcap;": "\u2A4B",
  "&capcup;": "\u2A47",
  "&capdot;": "\u2A40",
  "&caps;": "\u2229\uFE00",
  "&caret;": "\u2041",
  "&caron;": "\u02C7",
  "&ccaps;": "\u2A4D",
  "&ccaron;": "\u010D",
  "&ccedil": "\xE7",
  "&ccedil;": "\xE7",
  "&ccirc;": "\u0109",
  "&ccups;": "\u2A4C",
  "&ccupssm;": "\u2A50",
  "&cdot;": "\u010B",
  "&cedil": "\xB8",
  "&cedil;": "\xB8",
  "&cemptyv;": "\u29B2",
  "&cent": "\xA2",
  "&cent;": "\xA2",
  "&centerdot;": "\xB7",
  "&cfr;": "\u{1D520}",
  "&chcy;": "\u0447",
  "&check;": "\u2713",
  "&checkmark;": "\u2713",
  "&chi;": "\u03C7",
  "&cir;": "\u25CB",
  "&cirE;": "\u29C3",
  "&circ;": "\u02C6",
  "&circeq;": "\u2257",
  "&circlearrowleft;": "\u21BA",
  "&circlearrowright;": "\u21BB",
  "&circledR;": "\xAE",
  "&circledS;": "\u24C8",
  "&circledast;": "\u229B",
  "&circledcirc;": "\u229A",
  "&circleddash;": "\u229D",
  "&cire;": "\u2257",
  "&cirfnint;": "\u2A10",
  "&cirmid;": "\u2AEF",
  "&cirscir;": "\u29C2",
  "&clubs;": "\u2663",
  "&clubsuit;": "\u2663",
  "&colon;": ":",
  "&colone;": "\u2254",
  "&coloneq;": "\u2254",
  "&comma;": ",",
  "&commat;": "@",
  "&comp;": "\u2201",
  "&compfn;": "\u2218",
  "&complement;": "\u2201",
  "&complexes;": "\u2102",
  "&cong;": "\u2245",
  "&congdot;": "\u2A6D",
  "&conint;": "\u222E",
  "&copf;": "\u{1D554}",
  "&coprod;": "\u2210",
  "&copy": "\xA9",
  "&copy;": "\xA9",
  "&copysr;": "\u2117",
  "&crarr;": "\u21B5",
  "&cross;": "\u2717",
  "&cscr;": "\u{1D4B8}",
  "&csub;": "\u2ACF",
  "&csube;": "\u2AD1",
  "&csup;": "\u2AD0",
  "&csupe;": "\u2AD2",
  "&ctdot;": "\u22EF",
  "&cudarrl;": "\u2938",
  "&cudarrr;": "\u2935",
  "&cuepr;": "\u22DE",
  "&cuesc;": "\u22DF",
  "&cularr;": "\u21B6",
  "&cularrp;": "\u293D",
  "&cup;": "\u222A",
  "&cupbrcap;": "\u2A48",
  "&cupcap;": "\u2A46",
  "&cupcup;": "\u2A4A",
  "&cupdot;": "\u228D",
  "&cupor;": "\u2A45",
  "&cups;": "\u222A\uFE00",
  "&curarr;": "\u21B7",
  "&curarrm;": "\u293C",
  "&curlyeqprec;": "\u22DE",
  "&curlyeqsucc;": "\u22DF",
  "&curlyvee;": "\u22CE",
  "&curlywedge;": "\u22CF",
  "&curren": "\xA4",
  "&curren;": "\xA4",
  "&curvearrowleft;": "\u21B6",
  "&curvearrowright;": "\u21B7",
  "&cuvee;": "\u22CE",
  "&cuwed;": "\u22CF",
  "&cwconint;": "\u2232",
  "&cwint;": "\u2231",
  "&cylcty;": "\u232D",
  "&dArr;": "\u21D3",
  "&dHar;": "\u2965",
  "&dagger;": "\u2020",
  "&daleth;": "\u2138",
  "&darr;": "\u2193",
  "&dash;": "\u2010",
  "&dashv;": "\u22A3",
  "&dbkarow;": "\u290F",
  "&dblac;": "\u02DD",
  "&dcaron;": "\u010F",
  "&dcy;": "\u0434",
  "&dd;": "\u2146",
  "&ddagger;": "\u2021",
  "&ddarr;": "\u21CA",
  "&ddotseq;": "\u2A77",
  "&deg": "\xB0",
  "&deg;": "\xB0",
  "&delta;": "\u03B4",
  "&demptyv;": "\u29B1",
  "&dfisht;": "\u297F",
  "&dfr;": "\u{1D521}",
  "&dharl;": "\u21C3",
  "&dharr;": "\u21C2",
  "&diam;": "\u22C4",
  "&diamond;": "\u22C4",
  "&diamondsuit;": "\u2666",
  "&diams;": "\u2666",
  "&die;": "\xA8",
  "&digamma;": "\u03DD",
  "&disin;": "\u22F2",
  "&div;": "\xF7",
  "&divide": "\xF7",
  "&divide;": "\xF7",
  "&divideontimes;": "\u22C7",
  "&divonx;": "\u22C7",
  "&djcy;": "\u0452",
  "&dlcorn;": "\u231E",
  "&dlcrop;": "\u230D",
  "&dollar;": "$",
  "&dopf;": "\u{1D555}",
  "&dot;": "\u02D9",
  "&doteq;": "\u2250",
  "&doteqdot;": "\u2251",
  "&dotminus;": "\u2238",
  "&dotplus;": "\u2214",
  "&dotsquare;": "\u22A1",
  "&doublebarwedge;": "\u2306",
  "&downarrow;": "\u2193",
  "&downdownarrows;": "\u21CA",
  "&downharpoonleft;": "\u21C3",
  "&downharpoonright;": "\u21C2",
  "&drbkarow;": "\u2910",
  "&drcorn;": "\u231F",
  "&drcrop;": "\u230C",
  "&dscr;": "\u{1D4B9}",
  "&dscy;": "\u0455",
  "&dsol;": "\u29F6",
  "&dstrok;": "\u0111",
  "&dtdot;": "\u22F1",
  "&dtri;": "\u25BF",
  "&dtrif;": "\u25BE",
  "&duarr;": "\u21F5",
  "&duhar;": "\u296F",
  "&dwangle;": "\u29A6",
  "&dzcy;": "\u045F",
  "&dzigrarr;": "\u27FF",
  "&eDDot;": "\u2A77",
  "&eDot;": "\u2251",
  "&eacute": "\xE9",
  "&eacute;": "\xE9",
  "&easter;": "\u2A6E",
  "&ecaron;": "\u011B",
  "&ecir;": "\u2256",
  "&ecirc": "\xEA",
  "&ecirc;": "\xEA",
  "&ecolon;": "\u2255",
  "&ecy;": "\u044D",
  "&edot;": "\u0117",
  "&ee;": "\u2147",
  "&efDot;": "\u2252",
  "&efr;": "\u{1D522}",
  "&eg;": "\u2A9A",
  "&egrave": "\xE8",
  "&egrave;": "\xE8",
  "&egs;": "\u2A96",
  "&egsdot;": "\u2A98",
  "&el;": "\u2A99",
  "&elinters;": "\u23E7",
  "&ell;": "\u2113",
  "&els;": "\u2A95",
  "&elsdot;": "\u2A97",
  "&emacr;": "\u0113",
  "&empty;": "\u2205",
  "&emptyset;": "\u2205",
  "&emptyv;": "\u2205",
  "&emsp13;": "\u2004",
  "&emsp14;": "\u2005",
  "&emsp;": "\u2003",
  "&eng;": "\u014B",
  "&ensp;": "\u2002",
  "&eogon;": "\u0119",
  "&eopf;": "\u{1D556}",
  "&epar;": "\u22D5",
  "&eparsl;": "\u29E3",
  "&eplus;": "\u2A71",
  "&epsi;": "\u03B5",
  "&epsilon;": "\u03B5",
  "&epsiv;": "\u03F5",
  "&eqcirc;": "\u2256",
  "&eqcolon;": "\u2255",
  "&eqsim;": "\u2242",
  "&eqslantgtr;": "\u2A96",
  "&eqslantless;": "\u2A95",
  "&equals;": "=",
  "&equest;": "\u225F",
  "&equiv;": "\u2261",
  "&equivDD;": "\u2A78",
  "&eqvparsl;": "\u29E5",
  "&erDot;": "\u2253",
  "&erarr;": "\u2971",
  "&escr;": "\u212F",
  "&esdot;": "\u2250",
  "&esim;": "\u2242",
  "&eta;": "\u03B7",
  "&eth": "\xF0",
  "&eth;": "\xF0",
  "&euml": "\xEB",
  "&euml;": "\xEB",
  "&euro;": "\u20AC",
  "&excl;": "!",
  "&exist;": "\u2203",
  "&expectation;": "\u2130",
  "&exponentiale;": "\u2147",
  "&fallingdotseq;": "\u2252",
  "&fcy;": "\u0444",
  "&female;": "\u2640",
  "&ffilig;": "\uFB03",
  "&fflig;": "\uFB00",
  "&ffllig;": "\uFB04",
  "&ffr;": "\u{1D523}",
  "&filig;": "\uFB01",
  "&fjlig;": "fj",
  "&flat;": "\u266D",
  "&fllig;": "\uFB02",
  "&fltns;": "\u25B1",
  "&fnof;": "\u0192",
  "&fopf;": "\u{1D557}",
  "&forall;": "\u2200",
  "&fork;": "\u22D4",
  "&forkv;": "\u2AD9",
  "&fpartint;": "\u2A0D",
  "&frac12": "\xBD",
  "&frac12;": "\xBD",
  "&frac13;": "\u2153",
  "&frac14": "\xBC",
  "&frac14;": "\xBC",
  "&frac15;": "\u2155",
  "&frac16;": "\u2159",
  "&frac18;": "\u215B",
  "&frac23;": "\u2154",
  "&frac25;": "\u2156",
  "&frac34": "\xBE",
  "&frac34;": "\xBE",
  "&frac35;": "\u2157",
  "&frac38;": "\u215C",
  "&frac45;": "\u2158",
  "&frac56;": "\u215A",
  "&frac58;": "\u215D",
  "&frac78;": "\u215E",
  "&frasl;": "\u2044",
  "&frown;": "\u2322",
  "&fscr;": "\u{1D4BB}",
  "&gE;": "\u2267",
  "&gEl;": "\u2A8C",
  "&gacute;": "\u01F5",
  "&gamma;": "\u03B3",
  "&gammad;": "\u03DD",
  "&gap;": "\u2A86",
  "&gbreve;": "\u011F",
  "&gcirc;": "\u011D",
  "&gcy;": "\u0433",
  "&gdot;": "\u0121",
  "&ge;": "\u2265",
  "&gel;": "\u22DB",
  "&geq;": "\u2265",
  "&geqq;": "\u2267",
  "&geqslant;": "\u2A7E",
  "&ges;": "\u2A7E",
  "&gescc;": "\u2AA9",
  "&gesdot;": "\u2A80",
  "&gesdoto;": "\u2A82",
  "&gesdotol;": "\u2A84",
  "&gesl;": "\u22DB\uFE00",
  "&gesles;": "\u2A94",
  "&gfr;": "\u{1D524}",
  "&gg;": "\u226B",
  "&ggg;": "\u22D9",
  "&gimel;": "\u2137",
  "&gjcy;": "\u0453",
  "&gl;": "\u2277",
  "&glE;": "\u2A92",
  "&gla;": "\u2AA5",
  "&glj;": "\u2AA4",
  "&gnE;": "\u2269",
  "&gnap;": "\u2A8A",
  "&gnapprox;": "\u2A8A",
  "&gne;": "\u2A88",
  "&gneq;": "\u2A88",
  "&gneqq;": "\u2269",
  "&gnsim;": "\u22E7",
  "&gopf;": "\u{1D558}",
  "&grave;": "`",
  "&gscr;": "\u210A",
  "&gsim;": "\u2273",
  "&gsime;": "\u2A8E",
  "&gsiml;": "\u2A90",
  "&gt": ">",
  "&gt;": ">",
  "&gtcc;": "\u2AA7",
  "&gtcir;": "\u2A7A",
  "&gtdot;": "\u22D7",
  "&gtlPar;": "\u2995",
  "&gtquest;": "\u2A7C",
  "&gtrapprox;": "\u2A86",
  "&gtrarr;": "\u2978",
  "&gtrdot;": "\u22D7",
  "&gtreqless;": "\u22DB",
  "&gtreqqless;": "\u2A8C",
  "&gtrless;": "\u2277",
  "&gtrsim;": "\u2273",
  "&gvertneqq;": "\u2269\uFE00",
  "&gvnE;": "\u2269\uFE00",
  "&hArr;": "\u21D4",
  "&hairsp;": "\u200A",
  "&half;": "\xBD",
  "&hamilt;": "\u210B",
  "&hardcy;": "\u044A",
  "&harr;": "\u2194",
  "&harrcir;": "\u2948",
  "&harrw;": "\u21AD",
  "&hbar;": "\u210F",
  "&hcirc;": "\u0125",
  "&hearts;": "\u2665",
  "&heartsuit;": "\u2665",
  "&hellip;": "\u2026",
  "&hercon;": "\u22B9",
  "&hfr;": "\u{1D525}",
  "&hksearow;": "\u2925",
  "&hkswarow;": "\u2926",
  "&hoarr;": "\u21FF",
  "&homtht;": "\u223B",
  "&hookleftarrow;": "\u21A9",
  "&hookrightarrow;": "\u21AA",
  "&hopf;": "\u{1D559}",
  "&horbar;": "\u2015",
  "&hscr;": "\u{1D4BD}",
  "&hslash;": "\u210F",
  "&hstrok;": "\u0127",
  "&hybull;": "\u2043",
  "&hyphen;": "\u2010",
  "&iacute": "\xED",
  "&iacute;": "\xED",
  "&ic;": "\u2063",
  "&icirc": "\xEE",
  "&icirc;": "\xEE",
  "&icy;": "\u0438",
  "&iecy;": "\u0435",
  "&iexcl": "\xA1",
  "&iexcl;": "\xA1",
  "&iff;": "\u21D4",
  "&ifr;": "\u{1D526}",
  "&igrave": "\xEC",
  "&igrave;": "\xEC",
  "&ii;": "\u2148",
  "&iiiint;": "\u2A0C",
  "&iiint;": "\u222D",
  "&iinfin;": "\u29DC",
  "&iiota;": "\u2129",
  "&ijlig;": "\u0133",
  "&imacr;": "\u012B",
  "&image;": "\u2111",
  "&imagline;": "\u2110",
  "&imagpart;": "\u2111",
  "&imath;": "\u0131",
  "&imof;": "\u22B7",
  "&imped;": "\u01B5",
  "&in;": "\u2208",
  "&incare;": "\u2105",
  "&infin;": "\u221E",
  "&infintie;": "\u29DD",
  "&inodot;": "\u0131",
  "&int;": "\u222B",
  "&intcal;": "\u22BA",
  "&integers;": "\u2124",
  "&intercal;": "\u22BA",
  "&intlarhk;": "\u2A17",
  "&intprod;": "\u2A3C",
  "&iocy;": "\u0451",
  "&iogon;": "\u012F",
  "&iopf;": "\u{1D55A}",
  "&iota;": "\u03B9",
  "&iprod;": "\u2A3C",
  "&iquest": "\xBF",
  "&iquest;": "\xBF",
  "&iscr;": "\u{1D4BE}",
  "&isin;": "\u2208",
  "&isinE;": "\u22F9",
  "&isindot;": "\u22F5",
  "&isins;": "\u22F4",
  "&isinsv;": "\u22F3",
  "&isinv;": "\u2208",
  "&it;": "\u2062",
  "&itilde;": "\u0129",
  "&iukcy;": "\u0456",
  "&iuml": "\xEF",
  "&iuml;": "\xEF",
  "&jcirc;": "\u0135",
  "&jcy;": "\u0439",
  "&jfr;": "\u{1D527}",
  "&jmath;": "\u0237",
  "&jopf;": "\u{1D55B}",
  "&jscr;": "\u{1D4BF}",
  "&jsercy;": "\u0458",
  "&jukcy;": "\u0454",
  "&kappa;": "\u03BA",
  "&kappav;": "\u03F0",
  "&kcedil;": "\u0137",
  "&kcy;": "\u043A",
  "&kfr;": "\u{1D528}",
  "&kgreen;": "\u0138",
  "&khcy;": "\u0445",
  "&kjcy;": "\u045C",
  "&kopf;": "\u{1D55C}",
  "&kscr;": "\u{1D4C0}",
  "&lAarr;": "\u21DA",
  "&lArr;": "\u21D0",
  "&lAtail;": "\u291B",
  "&lBarr;": "\u290E",
  "&lE;": "\u2266",
  "&lEg;": "\u2A8B",
  "&lHar;": "\u2962",
  "&lacute;": "\u013A",
  "&laemptyv;": "\u29B4",
  "&lagran;": "\u2112",
  "&lambda;": "\u03BB",
  "&lang;": "\u27E8",
  "&langd;": "\u2991",
  "&langle;": "\u27E8",
  "&lap;": "\u2A85",
  "&laquo": "\xAB",
  "&laquo;": "\xAB",
  "&larr;": "\u2190",
  "&larrb;": "\u21E4",
  "&larrbfs;": "\u291F",
  "&larrfs;": "\u291D",
  "&larrhk;": "\u21A9",
  "&larrlp;": "\u21AB",
  "&larrpl;": "\u2939",
  "&larrsim;": "\u2973",
  "&larrtl;": "\u21A2",
  "&lat;": "\u2AAB",
  "&latail;": "\u2919",
  "&late;": "\u2AAD",
  "&lates;": "\u2AAD\uFE00",
  "&lbarr;": "\u290C",
  "&lbbrk;": "\u2772",
  "&lbrace;": "{",
  "&lbrack;": "[",
  "&lbrke;": "\u298B",
  "&lbrksld;": "\u298F",
  "&lbrkslu;": "\u298D",
  "&lcaron;": "\u013E",
  "&lcedil;": "\u013C",
  "&lceil;": "\u2308",
  "&lcub;": "{",
  "&lcy;": "\u043B",
  "&ldca;": "\u2936",
  "&ldquo;": "\u201C",
  "&ldquor;": "\u201E",
  "&ldrdhar;": "\u2967",
  "&ldrushar;": "\u294B",
  "&ldsh;": "\u21B2",
  "&le;": "\u2264",
  "&leftarrow;": "\u2190",
  "&leftarrowtail;": "\u21A2",
  "&leftharpoondown;": "\u21BD",
  "&leftharpoonup;": "\u21BC",
  "&leftleftarrows;": "\u21C7",
  "&leftrightarrow;": "\u2194",
  "&leftrightarrows;": "\u21C6",
  "&leftrightharpoons;": "\u21CB",
  "&leftrightsquigarrow;": "\u21AD",
  "&leftthreetimes;": "\u22CB",
  "&leg;": "\u22DA",
  "&leq;": "\u2264",
  "&leqq;": "\u2266",
  "&leqslant;": "\u2A7D",
  "&les;": "\u2A7D",
  "&lescc;": "\u2AA8",
  "&lesdot;": "\u2A7F",
  "&lesdoto;": "\u2A81",
  "&lesdotor;": "\u2A83",
  "&lesg;": "\u22DA\uFE00",
  "&lesges;": "\u2A93",
  "&lessapprox;": "\u2A85",
  "&lessdot;": "\u22D6",
  "&lesseqgtr;": "\u22DA",
  "&lesseqqgtr;": "\u2A8B",
  "&lessgtr;": "\u2276",
  "&lesssim;": "\u2272",
  "&lfisht;": "\u297C",
  "&lfloor;": "\u230A",
  "&lfr;": "\u{1D529}",
  "&lg;": "\u2276",
  "&lgE;": "\u2A91",
  "&lhard;": "\u21BD",
  "&lharu;": "\u21BC",
  "&lharul;": "\u296A",
  "&lhblk;": "\u2584",
  "&ljcy;": "\u0459",
  "&ll;": "\u226A",
  "&llarr;": "\u21C7",
  "&llcorner;": "\u231E",
  "&llhard;": "\u296B",
  "&lltri;": "\u25FA",
  "&lmidot;": "\u0140",
  "&lmoust;": "\u23B0",
  "&lmoustache;": "\u23B0",
  "&lnE;": "\u2268",
  "&lnap;": "\u2A89",
  "&lnapprox;": "\u2A89",
  "&lne;": "\u2A87",
  "&lneq;": "\u2A87",
  "&lneqq;": "\u2268",
  "&lnsim;": "\u22E6",
  "&loang;": "\u27EC",
  "&loarr;": "\u21FD",
  "&lobrk;": "\u27E6",
  "&longleftarrow;": "\u27F5",
  "&longleftrightarrow;": "\u27F7",
  "&longmapsto;": "\u27FC",
  "&longrightarrow;": "\u27F6",
  "&looparrowleft;": "\u21AB",
  "&looparrowright;": "\u21AC",
  "&lopar;": "\u2985",
  "&lopf;": "\u{1D55D}",
  "&loplus;": "\u2A2D",
  "&lotimes;": "\u2A34",
  "&lowast;": "\u2217",
  "&lowbar;": "_",
  "&loz;": "\u25CA",
  "&lozenge;": "\u25CA",
  "&lozf;": "\u29EB",
  "&lpar;": "(",
  "&lparlt;": "\u2993",
  "&lrarr;": "\u21C6",
  "&lrcorner;": "\u231F",
  "&lrhar;": "\u21CB",
  "&lrhard;": "\u296D",
  "&lrm;": "\u200E",
  "&lrtri;": "\u22BF",
  "&lsaquo;": "\u2039",
  "&lscr;": "\u{1D4C1}",
  "&lsh;": "\u21B0",
  "&lsim;": "\u2272",
  "&lsime;": "\u2A8D",
  "&lsimg;": "\u2A8F",
  "&lsqb;": "[",
  "&lsquo;": "\u2018",
  "&lsquor;": "\u201A",
  "&lstrok;": "\u0142",
  "&lt": "<",
  "&lt;": "<",
  "&ltcc;": "\u2AA6",
  "&ltcir;": "\u2A79",
  "&ltdot;": "\u22D6",
  "&lthree;": "\u22CB",
  "&ltimes;": "\u22C9",
  "&ltlarr;": "\u2976",
  "&ltquest;": "\u2A7B",
  "&ltrPar;": "\u2996",
  "&ltri;": "\u25C3",
  "&ltrie;": "\u22B4",
  "&ltrif;": "\u25C2",
  "&lurdshar;": "\u294A",
  "&luruhar;": "\u2966",
  "&lvertneqq;": "\u2268\uFE00",
  "&lvnE;": "\u2268\uFE00",
  "&mDDot;": "\u223A",
  "&macr": "\xAF",
  "&macr;": "\xAF",
  "&male;": "\u2642",
  "&malt;": "\u2720",
  "&maltese;": "\u2720",
  "&map;": "\u21A6",
  "&mapsto;": "\u21A6",
  "&mapstodown;": "\u21A7",
  "&mapstoleft;": "\u21A4",
  "&mapstoup;": "\u21A5",
  "&marker;": "\u25AE",
  "&mcomma;": "\u2A29",
  "&mcy;": "\u043C",
  "&mdash;": "\u2014",
  "&measuredangle;": "\u2221",
  "&mfr;": "\u{1D52A}",
  "&mho;": "\u2127",
  "&micro": "\xB5",
  "&micro;": "\xB5",
  "&mid;": "\u2223",
  "&midast;": "*",
  "&midcir;": "\u2AF0",
  "&middot": "\xB7",
  "&middot;": "\xB7",
  "&minus;": "\u2212",
  "&minusb;": "\u229F",
  "&minusd;": "\u2238",
  "&minusdu;": "\u2A2A",
  "&mlcp;": "\u2ADB",
  "&mldr;": "\u2026",
  "&mnplus;": "\u2213",
  "&models;": "\u22A7",
  "&mopf;": "\u{1D55E}",
  "&mp;": "\u2213",
  "&mscr;": "\u{1D4C2}",
  "&mstpos;": "\u223E",
  "&mu;": "\u03BC",
  "&multimap;": "\u22B8",
  "&mumap;": "\u22B8",
  "&nGg;": "\u22D9\u0338",
  "&nGt;": "\u226B\u20D2",
  "&nGtv;": "\u226B\u0338",
  "&nLeftarrow;": "\u21CD",
  "&nLeftrightarrow;": "\u21CE",
  "&nLl;": "\u22D8\u0338",
  "&nLt;": "\u226A\u20D2",
  "&nLtv;": "\u226A\u0338",
  "&nRightarrow;": "\u21CF",
  "&nVDash;": "\u22AF",
  "&nVdash;": "\u22AE",
  "&nabla;": "\u2207",
  "&nacute;": "\u0144",
  "&nang;": "\u2220\u20D2",
  "&nap;": "\u2249",
  "&napE;": "\u2A70\u0338",
  "&napid;": "\u224B\u0338",
  "&napos;": "\u0149",
  "&napprox;": "\u2249",
  "&natur;": "\u266E",
  "&natural;": "\u266E",
  "&naturals;": "\u2115",
  "&nbsp": "\xA0",
  "&nbsp;": "\xA0",
  "&nbump;": "\u224E\u0338",
  "&nbumpe;": "\u224F\u0338",
  "&ncap;": "\u2A43",
  "&ncaron;": "\u0148",
  "&ncedil;": "\u0146",
  "&ncong;": "\u2247",
  "&ncongdot;": "\u2A6D\u0338",
  "&ncup;": "\u2A42",
  "&ncy;": "\u043D",
  "&ndash;": "\u2013",
  "&ne;": "\u2260",
  "&neArr;": "\u21D7",
  "&nearhk;": "\u2924",
  "&nearr;": "\u2197",
  "&nearrow;": "\u2197",
  "&nedot;": "\u2250\u0338",
  "&nequiv;": "\u2262",
  "&nesear;": "\u2928",
  "&nesim;": "\u2242\u0338",
  "&nexist;": "\u2204",
  "&nexists;": "\u2204",
  "&nfr;": "\u{1D52B}",
  "&ngE;": "\u2267\u0338",
  "&nge;": "\u2271",
  "&ngeq;": "\u2271",
  "&ngeqq;": "\u2267\u0338",
  "&ngeqslant;": "\u2A7E\u0338",
  "&nges;": "\u2A7E\u0338",
  "&ngsim;": "\u2275",
  "&ngt;": "\u226F",
  "&ngtr;": "\u226F",
  "&nhArr;": "\u21CE",
  "&nharr;": "\u21AE",
  "&nhpar;": "\u2AF2",
  "&ni;": "\u220B",
  "&nis;": "\u22FC",
  "&nisd;": "\u22FA",
  "&niv;": "\u220B",
  "&njcy;": "\u045A",
  "&nlArr;": "\u21CD",
  "&nlE;": "\u2266\u0338",
  "&nlarr;": "\u219A",
  "&nldr;": "\u2025",
  "&nle;": "\u2270",
  "&nleftarrow;": "\u219A",
  "&nleftrightarrow;": "\u21AE",
  "&nleq;": "\u2270",
  "&nleqq;": "\u2266\u0338",
  "&nleqslant;": "\u2A7D\u0338",
  "&nles;": "\u2A7D\u0338",
  "&nless;": "\u226E",
  "&nlsim;": "\u2274",
  "&nlt;": "\u226E",
  "&nltri;": "\u22EA",
  "&nltrie;": "\u22EC",
  "&nmid;": "\u2224",
  "&nopf;": "\u{1D55F}",
  "&not": "\xAC",
  "&not;": "\xAC",
  "&notin;": "\u2209",
  "&notinE;": "\u22F9\u0338",
  "&notindot;": "\u22F5\u0338",
  "&notinva;": "\u2209",
  "&notinvb;": "\u22F7",
  "&notinvc;": "\u22F6",
  "&notni;": "\u220C",
  "&notniva;": "\u220C",
  "&notnivb;": "\u22FE",
  "&notnivc;": "\u22FD",
  "&npar;": "\u2226",
  "&nparallel;": "\u2226",
  "&nparsl;": "\u2AFD\u20E5",
  "&npart;": "\u2202\u0338",
  "&npolint;": "\u2A14",
  "&npr;": "\u2280",
  "&nprcue;": "\u22E0",
  "&npre;": "\u2AAF\u0338",
  "&nprec;": "\u2280",
  "&npreceq;": "\u2AAF\u0338",
  "&nrArr;": "\u21CF",
  "&nrarr;": "\u219B",
  "&nrarrc;": "\u2933\u0338",
  "&nrarrw;": "\u219D\u0338",
  "&nrightarrow;": "\u219B",
  "&nrtri;": "\u22EB",
  "&nrtrie;": "\u22ED",
  "&nsc;": "\u2281",
  "&nsccue;": "\u22E1",
  "&nsce;": "\u2AB0\u0338",
  "&nscr;": "\u{1D4C3}",
  "&nshortmid;": "\u2224",
  "&nshortparallel;": "\u2226",
  "&nsim;": "\u2241",
  "&nsime;": "\u2244",
  "&nsimeq;": "\u2244",
  "&nsmid;": "\u2224",
  "&nspar;": "\u2226",
  "&nsqsube;": "\u22E2",
  "&nsqsupe;": "\u22E3",
  "&nsub;": "\u2284",
  "&nsubE;": "\u2AC5\u0338",
  "&nsube;": "\u2288",
  "&nsubset;": "\u2282\u20D2",
  "&nsubseteq;": "\u2288",
  "&nsubseteqq;": "\u2AC5\u0338",
  "&nsucc;": "\u2281",
  "&nsucceq;": "\u2AB0\u0338",
  "&nsup;": "\u2285",
  "&nsupE;": "\u2AC6\u0338",
  "&nsupe;": "\u2289",
  "&nsupset;": "\u2283\u20D2",
  "&nsupseteq;": "\u2289",
  "&nsupseteqq;": "\u2AC6\u0338",
  "&ntgl;": "\u2279",
  "&ntilde": "\xF1",
  "&ntilde;": "\xF1",
  "&ntlg;": "\u2278",
  "&ntriangleleft;": "\u22EA",
  "&ntrianglelefteq;": "\u22EC",
  "&ntriangleright;": "\u22EB",
  "&ntrianglerighteq;": "\u22ED",
  "&nu;": "\u03BD",
  "&num;": "#",
  "&numero;": "\u2116",
  "&numsp;": "\u2007",
  "&nvDash;": "\u22AD",
  "&nvHarr;": "\u2904",
  "&nvap;": "\u224D\u20D2",
  "&nvdash;": "\u22AC",
  "&nvge;": "\u2265\u20D2",
  "&nvgt;": ">\u20D2",
  "&nvinfin;": "\u29DE",
  "&nvlArr;": "\u2902",
  "&nvle;": "\u2264\u20D2",
  "&nvlt;": "<\u20D2",
  "&nvltrie;": "\u22B4\u20D2",
  "&nvrArr;": "\u2903",
  "&nvrtrie;": "\u22B5\u20D2",
  "&nvsim;": "\u223C\u20D2",
  "&nwArr;": "\u21D6",
  "&nwarhk;": "\u2923",
  "&nwarr;": "\u2196",
  "&nwarrow;": "\u2196",
  "&nwnear;": "\u2927",
  "&oS;": "\u24C8",
  "&oacute": "\xF3",
  "&oacute;": "\xF3",
  "&oast;": "\u229B",
  "&ocir;": "\u229A",
  "&ocirc": "\xF4",
  "&ocirc;": "\xF4",
  "&ocy;": "\u043E",
  "&odash;": "\u229D",
  "&odblac;": "\u0151",
  "&odiv;": "\u2A38",
  "&odot;": "\u2299",
  "&odsold;": "\u29BC",
  "&oelig;": "\u0153",
  "&ofcir;": "\u29BF",
  "&ofr;": "\u{1D52C}",
  "&ogon;": "\u02DB",
  "&ograve": "\xF2",
  "&ograve;": "\xF2",
  "&ogt;": "\u29C1",
  "&ohbar;": "\u29B5",
  "&ohm;": "\u03A9",
  "&oint;": "\u222E",
  "&olarr;": "\u21BA",
  "&olcir;": "\u29BE",
  "&olcross;": "\u29BB",
  "&oline;": "\u203E",
  "&olt;": "\u29C0",
  "&omacr;": "\u014D",
  "&omega;": "\u03C9",
  "&omicron;": "\u03BF",
  "&omid;": "\u29B6",
  "&ominus;": "\u2296",
  "&oopf;": "\u{1D560}",
  "&opar;": "\u29B7",
  "&operp;": "\u29B9",
  "&oplus;": "\u2295",
  "&or;": "\u2228",
  "&orarr;": "\u21BB",
  "&ord;": "\u2A5D",
  "&order;": "\u2134",
  "&orderof;": "\u2134",
  "&ordf": "\xAA",
  "&ordf;": "\xAA",
  "&ordm": "\xBA",
  "&ordm;": "\xBA",
  "&origof;": "\u22B6",
  "&oror;": "\u2A56",
  "&orslope;": "\u2A57",
  "&orv;": "\u2A5B",
  "&oscr;": "\u2134",
  "&oslash": "\xF8",
  "&oslash;": "\xF8",
  "&osol;": "\u2298",
  "&otilde": "\xF5",
  "&otilde;": "\xF5",
  "&otimes;": "\u2297",
  "&otimesas;": "\u2A36",
  "&ouml": "\xF6",
  "&ouml;": "\xF6",
  "&ovbar;": "\u233D",
  "&par;": "\u2225",
  "&para": "\xB6",
  "&para;": "\xB6",
  "&parallel;": "\u2225",
  "&parsim;": "\u2AF3",
  "&parsl;": "\u2AFD",
  "&part;": "\u2202",
  "&pcy;": "\u043F",
  "&percnt;": "%",
  "&period;": ".",
  "&permil;": "\u2030",
  "&perp;": "\u22A5",
  "&pertenk;": "\u2031",
  "&pfr;": "\u{1D52D}",
  "&phi;": "\u03C6",
  "&phiv;": "\u03D5",
  "&phmmat;": "\u2133",
  "&phone;": "\u260E",
  "&pi;": "\u03C0",
  "&pitchfork;": "\u22D4",
  "&piv;": "\u03D6",
  "&planck;": "\u210F",
  "&planckh;": "\u210E",
  "&plankv;": "\u210F",
  "&plus;": "+",
  "&plusacir;": "\u2A23",
  "&plusb;": "\u229E",
  "&pluscir;": "\u2A22",
  "&plusdo;": "\u2214",
  "&plusdu;": "\u2A25",
  "&pluse;": "\u2A72",
  "&plusmn": "\xB1",
  "&plusmn;": "\xB1",
  "&plussim;": "\u2A26",
  "&plustwo;": "\u2A27",
  "&pm;": "\xB1",
  "&pointint;": "\u2A15",
  "&popf;": "\u{1D561}",
  "&pound": "\xA3",
  "&pound;": "\xA3",
  "&pr;": "\u227A",
  "&prE;": "\u2AB3",
  "&prap;": "\u2AB7",
  "&prcue;": "\u227C",
  "&pre;": "\u2AAF",
  "&prec;": "\u227A",
  "&precapprox;": "\u2AB7",
  "&preccurlyeq;": "\u227C",
  "&preceq;": "\u2AAF",
  "&precnapprox;": "\u2AB9",
  "&precneqq;": "\u2AB5",
  "&precnsim;": "\u22E8",
  "&precsim;": "\u227E",
  "&prime;": "\u2032",
  "&primes;": "\u2119",
  "&prnE;": "\u2AB5",
  "&prnap;": "\u2AB9",
  "&prnsim;": "\u22E8",
  "&prod;": "\u220F",
  "&profalar;": "\u232E",
  "&profline;": "\u2312",
  "&profsurf;": "\u2313",
  "&prop;": "\u221D",
  "&propto;": "\u221D",
  "&prsim;": "\u227E",
  "&prurel;": "\u22B0",
  "&pscr;": "\u{1D4C5}",
  "&psi;": "\u03C8",
  "&puncsp;": "\u2008",
  "&qfr;": "\u{1D52E}",
  "&qint;": "\u2A0C",
  "&qopf;": "\u{1D562}",
  "&qprime;": "\u2057",
  "&qscr;": "\u{1D4C6}",
  "&quaternions;": "\u210D",
  "&quatint;": "\u2A16",
  "&quest;": "?",
  "&questeq;": "\u225F",
  "&quot": '"',
  "&quot;": '"',
  "&rAarr;": "\u21DB",
  "&rArr;": "\u21D2",
  "&rAtail;": "\u291C",
  "&rBarr;": "\u290F",
  "&rHar;": "\u2964",
  "&race;": "\u223D\u0331",
  "&racute;": "\u0155",
  "&radic;": "\u221A",
  "&raemptyv;": "\u29B3",
  "&rang;": "\u27E9",
  "&rangd;": "\u2992",
  "&range;": "\u29A5",
  "&rangle;": "\u27E9",
  "&raquo": "\xBB",
  "&raquo;": "\xBB",
  "&rarr;": "\u2192",
  "&rarrap;": "\u2975",
  "&rarrb;": "\u21E5",
  "&rarrbfs;": "\u2920",
  "&rarrc;": "\u2933",
  "&rarrfs;": "\u291E",
  "&rarrhk;": "\u21AA",
  "&rarrlp;": "\u21AC",
  "&rarrpl;": "\u2945",
  "&rarrsim;": "\u2974",
  "&rarrtl;": "\u21A3",
  "&rarrw;": "\u219D",
  "&ratail;": "\u291A",
  "&ratio;": "\u2236",
  "&rationals;": "\u211A",
  "&rbarr;": "\u290D",
  "&rbbrk;": "\u2773",
  "&rbrace;": "}",
  "&rbrack;": "]",
  "&rbrke;": "\u298C",
  "&rbrksld;": "\u298E",
  "&rbrkslu;": "\u2990",
  "&rcaron;": "\u0159",
  "&rcedil;": "\u0157",
  "&rceil;": "\u2309",
  "&rcub;": "}",
  "&rcy;": "\u0440",
  "&rdca;": "\u2937",
  "&rdldhar;": "\u2969",
  "&rdquo;": "\u201D",
  "&rdquor;": "\u201D",
  "&rdsh;": "\u21B3",
  "&real;": "\u211C",
  "&realine;": "\u211B",
  "&realpart;": "\u211C",
  "&reals;": "\u211D",
  "&rect;": "\u25AD",
  "&reg": "\xAE",
  "&reg;": "\xAE",
  "&rfisht;": "\u297D",
  "&rfloor;": "\u230B",
  "&rfr;": "\u{1D52F}",
  "&rhard;": "\u21C1",
  "&rharu;": "\u21C0",
  "&rharul;": "\u296C",
  "&rho;": "\u03C1",
  "&rhov;": "\u03F1",
  "&rightarrow;": "\u2192",
  "&rightarrowtail;": "\u21A3",
  "&rightharpoondown;": "\u21C1",
  "&rightharpoonup;": "\u21C0",
  "&rightleftarrows;": "\u21C4",
  "&rightleftharpoons;": "\u21CC",
  "&rightrightarrows;": "\u21C9",
  "&rightsquigarrow;": "\u219D",
  "&rightthreetimes;": "\u22CC",
  "&ring;": "\u02DA",
  "&risingdotseq;": "\u2253",
  "&rlarr;": "\u21C4",
  "&rlhar;": "\u21CC",
  "&rlm;": "\u200F",
  "&rmoust;": "\u23B1",
  "&rmoustache;": "\u23B1",
  "&rnmid;": "\u2AEE",
  "&roang;": "\u27ED",
  "&roarr;": "\u21FE",
  "&robrk;": "\u27E7",
  "&ropar;": "\u2986",
  "&ropf;": "\u{1D563}",
  "&roplus;": "\u2A2E",
  "&rotimes;": "\u2A35",
  "&rpar;": ")",
  "&rpargt;": "\u2994",
  "&rppolint;": "\u2A12",
  "&rrarr;": "\u21C9",
  "&rsaquo;": "\u203A",
  "&rscr;": "\u{1D4C7}",
  "&rsh;": "\u21B1",
  "&rsqb;": "]",
  "&rsquo;": "\u2019",
  "&rsquor;": "\u2019",
  "&rthree;": "\u22CC",
  "&rtimes;": "\u22CA",
  "&rtri;": "\u25B9",
  "&rtrie;": "\u22B5",
  "&rtrif;": "\u25B8",
  "&rtriltri;": "\u29CE",
  "&ruluhar;": "\u2968",
  "&rx;": "\u211E",
  "&sacute;": "\u015B",
  "&sbquo;": "\u201A",
  "&sc;": "\u227B",
  "&scE;": "\u2AB4",
  "&scap;": "\u2AB8",
  "&scaron;": "\u0161",
  "&sccue;": "\u227D",
  "&sce;": "\u2AB0",
  "&scedil;": "\u015F",
  "&scirc;": "\u015D",
  "&scnE;": "\u2AB6",
  "&scnap;": "\u2ABA",
  "&scnsim;": "\u22E9",
  "&scpolint;": "\u2A13",
  "&scsim;": "\u227F",
  "&scy;": "\u0441",
  "&sdot;": "\u22C5",
  "&sdotb;": "\u22A1",
  "&sdote;": "\u2A66",
  "&seArr;": "\u21D8",
  "&searhk;": "\u2925",
  "&searr;": "\u2198",
  "&searrow;": "\u2198",
  "&sect": "\xA7",
  "&sect;": "\xA7",
  "&semi;": ";",
  "&seswar;": "\u2929",
  "&setminus;": "\u2216",
  "&setmn;": "\u2216",
  "&sext;": "\u2736",
  "&sfr;": "\u{1D530}",
  "&sfrown;": "\u2322",
  "&sharp;": "\u266F",
  "&shchcy;": "\u0449",
  "&shcy;": "\u0448",
  "&shortmid;": "\u2223",
  "&shortparallel;": "\u2225",
  "&shy": "\xAD",
  "&shy;": "\xAD",
  "&sigma;": "\u03C3",
  "&sigmaf;": "\u03C2",
  "&sigmav;": "\u03C2",
  "&sim;": "\u223C",
  "&simdot;": "\u2A6A",
  "&sime;": "\u2243",
  "&simeq;": "\u2243",
  "&simg;": "\u2A9E",
  "&simgE;": "\u2AA0",
  "&siml;": "\u2A9D",
  "&simlE;": "\u2A9F",
  "&simne;": "\u2246",
  "&simplus;": "\u2A24",
  "&simrarr;": "\u2972",
  "&slarr;": "\u2190",
  "&smallsetminus;": "\u2216",
  "&smashp;": "\u2A33",
  "&smeparsl;": "\u29E4",
  "&smid;": "\u2223",
  "&smile;": "\u2323",
  "&smt;": "\u2AAA",
  "&smte;": "\u2AAC",
  "&smtes;": "\u2AAC\uFE00",
  "&softcy;": "\u044C",
  "&sol;": "/",
  "&solb;": "\u29C4",
  "&solbar;": "\u233F",
  "&sopf;": "\u{1D564}",
  "&spades;": "\u2660",
  "&spadesuit;": "\u2660",
  "&spar;": "\u2225",
  "&sqcap;": "\u2293",
  "&sqcaps;": "\u2293\uFE00",
  "&sqcup;": "\u2294",
  "&sqcups;": "\u2294\uFE00",
  "&sqsub;": "\u228F",
  "&sqsube;": "\u2291",
  "&sqsubset;": "\u228F",
  "&sqsubseteq;": "\u2291",
  "&sqsup;": "\u2290",
  "&sqsupe;": "\u2292",
  "&sqsupset;": "\u2290",
  "&sqsupseteq;": "\u2292",
  "&squ;": "\u25A1",
  "&square;": "\u25A1",
  "&squarf;": "\u25AA",
  "&squf;": "\u25AA",
  "&srarr;": "\u2192",
  "&sscr;": "\u{1D4C8}",
  "&ssetmn;": "\u2216",
  "&ssmile;": "\u2323",
  "&sstarf;": "\u22C6",
  "&star;": "\u2606",
  "&starf;": "\u2605",
  "&straightepsilon;": "\u03F5",
  "&straightphi;": "\u03D5",
  "&strns;": "\xAF",
  "&sub;": "\u2282",
  "&subE;": "\u2AC5",
  "&subdot;": "\u2ABD",
  "&sube;": "\u2286",
  "&subedot;": "\u2AC3",
  "&submult;": "\u2AC1",
  "&subnE;": "\u2ACB",
  "&subne;": "\u228A",
  "&subplus;": "\u2ABF",
  "&subrarr;": "\u2979",
  "&subset;": "\u2282",
  "&subseteq;": "\u2286",
  "&subseteqq;": "\u2AC5",
  "&subsetneq;": "\u228A",
  "&subsetneqq;": "\u2ACB",
  "&subsim;": "\u2AC7",
  "&subsub;": "\u2AD5",
  "&subsup;": "\u2AD3",
  "&succ;": "\u227B",
  "&succapprox;": "\u2AB8",
  "&succcurlyeq;": "\u227D",
  "&succeq;": "\u2AB0",
  "&succnapprox;": "\u2ABA",
  "&succneqq;": "\u2AB6",
  "&succnsim;": "\u22E9",
  "&succsim;": "\u227F",
  "&sum;": "\u2211",
  "&sung;": "\u266A",
  "&sup1": "\xB9",
  "&sup1;": "\xB9",
  "&sup2": "\xB2",
  "&sup2;": "\xB2",
  "&sup3": "\xB3",
  "&sup3;": "\xB3",
  "&sup;": "\u2283",
  "&supE;": "\u2AC6",
  "&supdot;": "\u2ABE",
  "&supdsub;": "\u2AD8",
  "&supe;": "\u2287",
  "&supedot;": "\u2AC4",
  "&suphsol;": "\u27C9",
  "&suphsub;": "\u2AD7",
  "&suplarr;": "\u297B",
  "&supmult;": "\u2AC2",
  "&supnE;": "\u2ACC",
  "&supne;": "\u228B",
  "&supplus;": "\u2AC0",
  "&supset;": "\u2283",
  "&supseteq;": "\u2287",
  "&supseteqq;": "\u2AC6",
  "&supsetneq;": "\u228B",
  "&supsetneqq;": "\u2ACC",
  "&supsim;": "\u2AC8",
  "&supsub;": "\u2AD4",
  "&supsup;": "\u2AD6",
  "&swArr;": "\u21D9",
  "&swarhk;": "\u2926",
  "&swarr;": "\u2199",
  "&swarrow;": "\u2199",
  "&swnwar;": "\u292A",
  "&szlig": "\xDF",
  "&szlig;": "\xDF",
  "&target;": "\u2316",
  "&tau;": "\u03C4",
  "&tbrk;": "\u23B4",
  "&tcaron;": "\u0165",
  "&tcedil;": "\u0163",
  "&tcy;": "\u0442",
  "&tdot;": "\u20DB",
  "&telrec;": "\u2315",
  "&tfr;": "\u{1D531}",
  "&there4;": "\u2234",
  "&therefore;": "\u2234",
  "&theta;": "\u03B8",
  "&thetasym;": "\u03D1",
  "&thetav;": "\u03D1",
  "&thickapprox;": "\u2248",
  "&thicksim;": "\u223C",
  "&thinsp;": "\u2009",
  "&thkap;": "\u2248",
  "&thksim;": "\u223C",
  "&thorn": "\xFE",
  "&thorn;": "\xFE",
  "&tilde;": "\u02DC",
  "&times": "\xD7",
  "&times;": "\xD7",
  "&timesb;": "\u22A0",
  "&timesbar;": "\u2A31",
  "&timesd;": "\u2A30",
  "&tint;": "\u222D",
  "&toea;": "\u2928",
  "&top;": "\u22A4",
  "&topbot;": "\u2336",
  "&topcir;": "\u2AF1",
  "&topf;": "\u{1D565}",
  "&topfork;": "\u2ADA",
  "&tosa;": "\u2929",
  "&tprime;": "\u2034",
  "&trade;": "\u2122",
  "&triangle;": "\u25B5",
  "&triangledown;": "\u25BF",
  "&triangleleft;": "\u25C3",
  "&trianglelefteq;": "\u22B4",
  "&triangleq;": "\u225C",
  "&triangleright;": "\u25B9",
  "&trianglerighteq;": "\u22B5",
  "&tridot;": "\u25EC",
  "&trie;": "\u225C",
  "&triminus;": "\u2A3A",
  "&triplus;": "\u2A39",
  "&trisb;": "\u29CD",
  "&tritime;": "\u2A3B",
  "&trpezium;": "\u23E2",
  "&tscr;": "\u{1D4C9}",
  "&tscy;": "\u0446",
  "&tshcy;": "\u045B",
  "&tstrok;": "\u0167",
  "&twixt;": "\u226C",
  "&twoheadleftarrow;": "\u219E",
  "&twoheadrightarrow;": "\u21A0",
  "&uArr;": "\u21D1",
  "&uHar;": "\u2963",
  "&uacute": "\xFA",
  "&uacute;": "\xFA",
  "&uarr;": "\u2191",
  "&ubrcy;": "\u045E",
  "&ubreve;": "\u016D",
  "&ucirc": "\xFB",
  "&ucirc;": "\xFB",
  "&ucy;": "\u0443",
  "&udarr;": "\u21C5",
  "&udblac;": "\u0171",
  "&udhar;": "\u296E",
  "&ufisht;": "\u297E",
  "&ufr;": "\u{1D532}",
  "&ugrave": "\xF9",
  "&ugrave;": "\xF9",
  "&uharl;": "\u21BF",
  "&uharr;": "\u21BE",
  "&uhblk;": "\u2580",
  "&ulcorn;": "\u231C",
  "&ulcorner;": "\u231C",
  "&ulcrop;": "\u230F",
  "&ultri;": "\u25F8",
  "&umacr;": "\u016B",
  "&uml": "\xA8",
  "&uml;": "\xA8",
  "&uogon;": "\u0173",
  "&uopf;": "\u{1D566}",
  "&uparrow;": "\u2191",
  "&updownarrow;": "\u2195",
  "&upharpoonleft;": "\u21BF",
  "&upharpoonright;": "\u21BE",
  "&uplus;": "\u228E",
  "&upsi;": "\u03C5",
  "&upsih;": "\u03D2",
  "&upsilon;": "\u03C5",
  "&upuparrows;": "\u21C8",
  "&urcorn;": "\u231D",
  "&urcorner;": "\u231D",
  "&urcrop;": "\u230E",
  "&uring;": "\u016F",
  "&urtri;": "\u25F9",
  "&uscr;": "\u{1D4CA}",
  "&utdot;": "\u22F0",
  "&utilde;": "\u0169",
  "&utri;": "\u25B5",
  "&utrif;": "\u25B4",
  "&uuarr;": "\u21C8",
  "&uuml": "\xFC",
  "&uuml;": "\xFC",
  "&uwangle;": "\u29A7",
  "&vArr;": "\u21D5",
  "&vBar;": "\u2AE8",
  "&vBarv;": "\u2AE9",
  "&vDash;": "\u22A8",
  "&vangrt;": "\u299C",
  "&varepsilon;": "\u03F5",
  "&varkappa;": "\u03F0",
  "&varnothing;": "\u2205",
  "&varphi;": "\u03D5",
  "&varpi;": "\u03D6",
  "&varpropto;": "\u221D",
  "&varr;": "\u2195",
  "&varrho;": "\u03F1",
  "&varsigma;": "\u03C2",
  "&varsubsetneq;": "\u228A\uFE00",
  "&varsubsetneqq;": "\u2ACB\uFE00",
  "&varsupsetneq;": "\u228B\uFE00",
  "&varsupsetneqq;": "\u2ACC\uFE00",
  "&vartheta;": "\u03D1",
  "&vartriangleleft;": "\u22B2",
  "&vartriangleright;": "\u22B3",
  "&vcy;": "\u0432",
  "&vdash;": "\u22A2",
  "&vee;": "\u2228",
  "&veebar;": "\u22BB",
  "&veeeq;": "\u225A",
  "&vellip;": "\u22EE",
  "&verbar;": "|",
  "&vert;": "|",
  "&vfr;": "\u{1D533}",
  "&vltri;": "\u22B2",
  "&vnsub;": "\u2282\u20D2",
  "&vnsup;": "\u2283\u20D2",
  "&vopf;": "\u{1D567}",
  "&vprop;": "\u221D",
  "&vrtri;": "\u22B3",
  "&vscr;": "\u{1D4CB}",
  "&vsubnE;": "\u2ACB\uFE00",
  "&vsubne;": "\u228A\uFE00",
  "&vsupnE;": "\u2ACC\uFE00",
  "&vsupne;": "\u228B\uFE00",
  "&vzigzag;": "\u299A",
  "&wcirc;": "\u0175",
  "&wedbar;": "\u2A5F",
  "&wedge;": "\u2227",
  "&wedgeq;": "\u2259",
  "&weierp;": "\u2118",
  "&wfr;": "\u{1D534}",
  "&wopf;": "\u{1D568}",
  "&wp;": "\u2118",
  "&wr;": "\u2240",
  "&wreath;": "\u2240",
  "&wscr;": "\u{1D4CC}",
  "&xcap;": "\u22C2",
  "&xcirc;": "\u25EF",
  "&xcup;": "\u22C3",
  "&xdtri;": "\u25BD",
  "&xfr;": "\u{1D535}",
  "&xhArr;": "\u27FA",
  "&xharr;": "\u27F7",
  "&xi;": "\u03BE",
  "&xlArr;": "\u27F8",
  "&xlarr;": "\u27F5",
  "&xmap;": "\u27FC",
  "&xnis;": "\u22FB",
  "&xodot;": "\u2A00",
  "&xopf;": "\u{1D569}",
  "&xoplus;": "\u2A01",
  "&xotime;": "\u2A02",
  "&xrArr;": "\u27F9",
  "&xrarr;": "\u27F6",
  "&xscr;": "\u{1D4CD}",
  "&xsqcup;": "\u2A06",
  "&xuplus;": "\u2A04",
  "&xutri;": "\u25B3",
  "&xvee;": "\u22C1",
  "&xwedge;": "\u22C0",
  "&yacute": "\xFD",
  "&yacute;": "\xFD",
  "&yacy;": "\u044F",
  "&ycirc;": "\u0177",
  "&ycy;": "\u044B",
  "&yen": "\xA5",
  "&yen;": "\xA5",
  "&yfr;": "\u{1D536}",
  "&yicy;": "\u0457",
  "&yopf;": "\u{1D56A}",
  "&yscr;": "\u{1D4CE}",
  "&yucy;": "\u044E",
  "&yuml": "\xFF",
  "&yuml;": "\xFF",
  "&zacute;": "\u017A",
  "&zcaron;": "\u017E",
  "&zcy;": "\u0437",
  "&zdot;": "\u017C",
  "&zeetrf;": "\u2128",
  "&zeta;": "\u03B6",
  "&zfr;": "\u{1D537}",
  "&zhcy;": "\u0436",
  "&zigrarr;": "\u21DD",
  "&zopf;": "\u{1D56B}",
  "&zscr;": "\u{1D4CF}",
  "&zwj;": "\u200D",
  "&zwnj;": "\u200C"
};

// node_modules/postal-mime/dist/esm/text-format.js
var C1_ENTITY_REPLACEMENTS = /* @__PURE__ */ new Map([
  [128, 8364],
  [130, 8218],
  [131, 402],
  [132, 8222],
  [133, 8230],
  [134, 8224],
  [135, 8225],
  [136, 710],
  [137, 8240],
  [138, 352],
  [139, 8249],
  [140, 338],
  [142, 381],
  [145, 8216],
  [146, 8217],
  [147, 8220],
  [148, 8221],
  [149, 8226],
  [150, 8211],
  [151, 8212],
  [152, 732],
  [153, 8482],
  [154, 353],
  [155, 8250],
  [156, 339],
  [158, 382],
  [159, 376]
]);
function decodeHTMLEntities(str) {
  return str.replace(/&(#\d+|#x[a-f0-9]+|[a-z]+\d*);?/gi, (match, entity) => {
    if (typeof htmlEntities[match] === "string") {
      return htmlEntities[match];
    }
    if (entity.charAt(0) !== "#" || match.charAt(match.length - 1) !== ";") {
      return match;
    }
    let codePoint;
    if (entity.charAt(1).toLowerCase() === "x") {
      codePoint = parseInt(entity.slice(2), 16);
    } else {
      codePoint = parseInt(entity.slice(1), 10);
    }
    let output = "";
    if (codePoint === 0 || codePoint >= 55296 && codePoint <= 57343 || codePoint > 1114111) {
      return "\uFFFD";
    }
    codePoint = C1_ENTITY_REPLACEMENTS.get(codePoint) ?? codePoint;
    if (codePoint > 65535) {
      codePoint -= 65536;
      output += String.fromCharCode(codePoint >>> 10 & 1023 | 55296);
      codePoint = 56320 | codePoint & 1023;
    }
    output += String.fromCharCode(codePoint);
    return output;
  });
}
function escapeHtml(str) {
  return str.trim().replace(/[<>"'?&]/g, (c) => {
    let hex = c.charCodeAt(0).toString(16);
    if (hex.length < 2) {
      hex = "0" + hex;
    }
    return "&#x" + hex.toUpperCase() + ";";
  });
}
function textToHtml(str) {
  let html = escapeHtml(str).replace(/\n/g, "<br />");
  return "<div>" + html + "</div>";
}
var LINE_TERMINATOR = /[\n\r\u2028\u2029]/g;
function lastLineTerminator(str) {
  return Math.max(str.lastIndexOf("\n"), str.lastIndexOf("\r"), str.lastIndexOf("\u2028"), str.lastIndexOf("\u2029"));
}
function createFinder(str, regex) {
  let searchedFrom = Infinity;
  let found = null;
  return (pos) => {
    if (pos < searchedFrom || found && pos > found.index) {
      regex.lastIndex = pos;
      found = regex.exec(str);
      searchedFrom = pos;
    }
    return found;
  };
}
function replaceClosedTags(str, pattern, replacement) {
  const end = str.lastIndexOf(">") + 1;
  return str.slice(0, end).replace(pattern, replacement) + str.slice(end);
}
function replaceTags(str, prefix, pattern, replacement) {
  const parts = [];
  let copied = 0;
  let searchFrom = 0;
  while (true) {
    prefix.lastIndex = searchFrom;
    const candidate = prefix.exec(str);
    if (!candidate) {
      break;
    }
    const start = candidate.index;
    const gt = str.indexOf(">", start);
    if (gt < 0) {
      break;
    }
    pattern.lastIndex = start;
    const match = pattern.exec(str);
    if (!match) {
      searchFrom = gt + 1;
      continue;
    }
    parts.push(str.slice(copied, start), replacement(...match));
    copied = searchFrom = start + match[0].length;
  }
  parts.push(str.slice(copied));
  return parts.join("");
}
function replaceBlocks(str, open, openToGt, close, closeToGt, replacement) {
  const nextGt = createFinder(str, />/g);
  const nextClose = createFinder(str, close);
  const nextLineEnd = createFinder(str, LINE_TERMINATOR);
  const parts = [];
  let copied = 0;
  let searchFrom = 0;
  while (true) {
    open.lastIndex = searchFrom;
    const opener = open.exec(str);
    if (!opener) {
      break;
    }
    const start = opener.index;
    searchFrom = start + 1;
    let from = start + opener[0].length;
    if (openToGt) {
      const gt = nextGt(from);
      if (!gt) {
        break;
      }
      from = gt.index + 1;
    }
    const closer = nextClose(from);
    if (!closer) {
      break;
    }
    const lineEnd = nextLineEnd(from);
    if (lineEnd && lineEnd.index < closer.index) {
      continue;
    }
    let end = closer.index + closer[0].length;
    if (closeToGt) {
      const gt = nextGt(end);
      if (!gt) {
        break;
      }
      end = gt.index + 1;
    }
    parts.push(str.slice(copied, start), replacement);
    copied = searchFrom = end;
  }
  parts.push(str.slice(copied));
  return parts.join("");
}
function stripThroughLastTag(str, prefix) {
  const lineEnd = str.search(LINE_TERMINATOR);
  const limit = Math.min(lineEnd < 0 ? str.length : lineEnd, str.lastIndexOf(">"));
  let last = -1;
  prefix.lastIndex = 0;
  let match;
  while ((match = prefix.exec(str)) && match.index < limit) {
    last = match.index;
  }
  return last < 0 ? str : str.slice(str.indexOf(">", last) + 1);
}
function stripFromFirstTag(str, prefix) {
  const lineEnd = lastLineTerminator(str);
  prefix.lastIndex = lineEnd < 0 ? 0 : str.lastIndexOf(">", lineEnd) + 1;
  const match = prefix.exec(str);
  return match && match.index < str.lastIndexOf(">") ? str.slice(0, match.index) : str;
}
function htmlToText(str) {
  str = str.replace(/\r?\n/g, "");
  str = replaceBlocks(str, /<!--/g, false, /-->/g, false, " ");
  str = replaceClosedTags(str, /<br\b[^>]*>/gi, "\n");
  str = replaceClosedTags(str, /<\/?(p|div|table|tr|td|th)\b[^>]*>/gi, "\n\n");
  str = replaceBlocks(str, /<script\b/gi, true, /<\/script\b/gi, true, " ");
  str = stripThroughLastTag(str, /<body\b/gi);
  str = stripThroughLastTag(str, /<\/head\b/gi);
  str = stripThroughLastTag(str, /<!doctype\b/gi);
  str = stripFromFirstTag(str, /<\/body\b/gi);
  str = stripFromFirstTag(str, /<\/html\b/gi);
  str = replaceTags(str, /<a\b/gi, /<a\b[^>]*href\s*=\s*["']?([^\s"']+)[^>]*>/iy, (match, href) => ` (${href}) `);
  str = replaceClosedTags(str, /<\/?(span|em|i|strong|b|u|a)\b[^>]*>/gi, "");
  str = replaceTags(str, /<li\b/gi, /<li\b[^>]*>[\n\u0001\s]*/iy, () => "* ");
  str = replaceClosedTags(str, /<hr\b[^>]*>/g, "\n-------------\n");
  str = replaceClosedTags(str, /<[^>]*>/g, " ");
  str = str.replace(/\u0001/g, "\n").replace(/[ \t]+/g, " ").replace(/^\s+$/gm, "").replace(/\n\n+/g, "\n\n").replace(/^\n+/, "\n").replace(/\n+$/, "\n");
  str = decodeHTMLEntities(str);
  return str;
}
function formatDate(date) {
  if (typeof Intl === "undefined") {
    return date;
  }
  const parsed = new Date(date);
  if (isNaN(parsed.getTime())) {
    return date;
  }
  return new Intl.DateTimeFormat("default", {
    year: "numeric",
    month: "numeric",
    day: "numeric",
    hour: "numeric",
    minute: "numeric",
    second: "numeric",
    hour12: false
  }).format(parsed);
}
function formatTextAddress(address) {
  return (address.name ? [address.name, `<${address.address}>`] : [address.address]).join(" ");
}
function formatTextAddresses(addresses) {
  let parts = [];
  let processAddress = (address, partCounter) => {
    if (partCounter) {
      parts.push(", ");
    }
    if (address.group) {
      let groupStart = `${address.name}:`;
      let groupEnd = `;`;
      parts.push(groupStart);
      address.group.forEach(processAddress);
      parts.push(groupEnd);
    } else {
      parts.push(formatTextAddress(address));
    }
  };
  addresses.forEach(processAddress);
  return parts.join("");
}
function formatHtmlAddress(address) {
  return `<a href="mailto:${escapeHtml(address.address)}" class="postal-email-address">${escapeHtml(address.name || `<${address.address}>`)}</a>`;
}
function formatHtmlAddresses(addresses) {
  let parts = [];
  let processAddress = (address, partCounter) => {
    if (partCounter) {
      parts.push('<span class="postal-email-address-separator">, </span>');
    }
    if (address.group) {
      let groupStart = `<span class="postal-email-address-group">${escapeHtml(address.name)}:</span>`;
      let groupEnd = `<span class="postal-email-address-group">;</span>`;
      parts.push(groupStart);
      address.group.forEach(processAddress);
      parts.push(groupEnd);
    } else {
      parts.push(formatHtmlAddress(address));
    }
  };
  addresses.forEach(processAddress);
  return parts.join(" ");
}
function foldLines(str, lineLength, afterSpace) {
  str = (str || "").toString();
  lineLength = lineLength || 76;
  let pos = 0, len = str.length, result = "", line, match;
  while (pos < len) {
    line = str.slice(pos, pos + lineLength);
    if (line.length < lineLength) {
      result += line;
      break;
    }
    if (match = line.match(/^[^\n\r]*(\r?\n|\r)/)) {
      line = match[0];
      result += line;
      pos += line.length;
      continue;
    } else if ((match = line.match(/(\s+)[^\s]*$/)) && match[0].length - (afterSpace ? (match[1] || "").length : 0) < line.length) {
      line = line.slice(0, line.length - (match[0].length - (afterSpace ? (match[1] || "").length : 0)));
    } else if (match = str.slice(pos + line.length).match(/^[^\s]+(\s*)/)) {
      line = line + match[0].slice(0, match[0].length - (!afterSpace ? (match[1] || "").length : 0));
    }
    result += line;
    pos += line.length;
    if (pos < len) {
      result += "\r\n";
    }
  }
  return result;
}
function formatTextHeader(message2) {
  let rows = [];
  if (message2.from) {
    rows.push({ key: "From", val: formatTextAddresses([message2.from]) });
  }
  if (message2.subject) {
    rows.push({ key: "Subject", val: message2.subject });
  }
  if (message2.date) {
    rows.push({ key: "Date", val: formatDate(message2.date) });
  }
  if (message2.to && message2.to.length) {
    rows.push({ key: "To", val: formatTextAddresses(message2.to) });
  }
  if (message2.cc && message2.cc.length) {
    rows.push({ key: "Cc", val: formatTextAddresses(message2.cc) });
  }
  if (message2.bcc && message2.bcc.length) {
    rows.push({ key: "Bcc", val: formatTextAddresses(message2.bcc) });
  }
  let maxKeyLength = rows.map((r) => r.key.length).reduce((acc, cur) => {
    return cur > acc ? cur : acc;
  }, 0);
  const lines = rows.flatMap((row) => {
    let sepLen = maxKeyLength - row.key.length;
    let prefix = `${row.key}: ${" ".repeat(sepLen)}`;
    let emptyPrefix = `${" ".repeat(row.key.length + 1)} ${" ".repeat(sepLen)}`;
    let foldedLines = foldLines(row.val, 80, true).split(/\r?\n/).map((line) => line.trim());
    return foldedLines.map((line, i) => `${i ? emptyPrefix : prefix}${line}`);
  });
  let maxLineLength = lines.map((r) => r.length).reduce((acc, cur) => {
    return cur > acc ? cur : acc;
  }, 0);
  let lineMarker = "-".repeat(maxLineLength);
  let template = `
${lineMarker}
${lines.join("\n")}
${lineMarker}
`;
  return template;
}
function formatHtmlHeader(message2) {
  let rows = [];
  if (message2.from) {
    rows.push(
      // through the plural formatter, because `From:` may hold RFC 5322 group syntax
      // and a group has no address of its own
      `<div class="postal-email-header-key">From</div><div class="postal-email-header-value">${formatHtmlAddresses([message2.from])}</div>`
    );
  }
  if (message2.subject) {
    rows.push(`<div class="postal-email-header-key">Subject</div><div class="postal-email-header-value postal-email-header-subject">${escapeHtml(message2.subject)}</div>`);
  }
  if (message2.date) {
    rows.push(`<div class="postal-email-header-key">Date</div><div class="postal-email-header-value postal-email-header-date" data-date="${escapeHtml(message2.date)}">${escapeHtml(formatDate(message2.date))}</div>`);
  }
  if (message2.to && message2.to.length) {
    rows.push(`<div class="postal-email-header-key">To</div><div class="postal-email-header-value">${formatHtmlAddresses(message2.to)}</div>`);
  }
  if (message2.cc && message2.cc.length) {
    rows.push(`<div class="postal-email-header-key">Cc</div><div class="postal-email-header-value">${formatHtmlAddresses(message2.cc)}</div>`);
  }
  if (message2.bcc && message2.bcc.length) {
    rows.push(`<div class="postal-email-header-key">Bcc</div><div class="postal-email-header-value">${formatHtmlAddresses(message2.bcc)}</div>`);
  }
  let template = `<div class="postal-email-header">${rows.length ? '<div class="postal-email-header-row">' : ""}${rows.join('</div>\n<div class="postal-email-header-row">')}${rows.length ? "</div>" : ""}</div>`;
  return template;
}

// node_modules/postal-mime/dist/esm/address-parser.js
var HAS_WHITESPACE = /\s/;
var QUOTED_LOCAL_ADDR = /^("(?:[^"\\]|\\[\s\S])*"@\S+)(?:\s+([\s\S]+))?$/;
var ADDR_SPEC = /^[^@\s]+@[^@\s]+$/;
var LOOSE_ADDR_SPEC = /^[^@\s]+@\S+$/;
var LOOSE_TEXT_ADDR = /\s*\b[^@\s]+@[^\s]+\b\s*/y;
function quoteLocalPart(address) {
  const lastAt = address.lastIndexOf("@");
  if (lastAt < 0) {
    return address;
  }
  const user = address.substring(0, lastAt);
  if (/^[^\s"(),:;<>@[\\\]]+$/.test(user) || /^"(?:[^"\\]|\\[\s\S])*"$/.test(user)) {
    return address;
  }
  return '"' + user.replace(/["\\]/g, "\\$&") + '"@' + address.substring(lastAt + 1);
}
function isSpaceCode(code) {
  return code === 32 || code >= 9 && code <= 13 || code === 160 || code === 5760 || code >= 8192 && code <= 8202 || code === 8232 || code === 8233 || code === 8239 || code === 8287 || code === 12288 || code === 65279;
}
function isWordCode(code) {
  return code >= 48 && code <= 57 || code >= 65 && code <= 90 || code >= 97 && code <= 122 || code === 95;
}
function isBoundary(text, at) {
  return isWordCode(text.charCodeAt(at - 1)) !== isWordCode(text.charCodeAt(at));
}
function indexOfAt(text, from, to) {
  for (let i = from; i < to; i++) {
    if (text.charCodeAt(i) === 64) {
      return i;
    }
  }
  return -1;
}
function looseAddressStart(text) {
  const len = text.length;
  let pos = 0;
  while (pos < len) {
    while (pos < len && isSpaceCode(text.charCodeAt(pos))) {
      pos++;
    }
    if (pos >= len) {
      break;
    }
    const runStart = pos;
    let runEnd = pos;
    while (runEnd < len && !isSpaceCode(text.charCodeAt(runEnd))) {
      runEnd++;
    }
    let at = indexOfAt(text, runStart, runEnd);
    if (at >= 0) {
      let lastBoundary = -1;
      for (let k = runEnd; k > runStart; k--) {
        if (isBoundary(text, k)) {
          lastBoundary = k;
          break;
        }
      }
      let atomStart = runStart;
      while (lastBoundary >= 0 && at >= 0) {
        if (at > atomStart && runEnd > at + 1 && lastBoundary > at + 1) {
          for (let start = atomStart; start < at; start++) {
            if (isBoundary(text, start)) {
              if (start > runStart) {
                return start;
              }
              let padded = runStart;
              while (padded > 0 && isSpaceCode(text.charCodeAt(padded - 1))) {
                padded--;
              }
              return padded;
            }
          }
        }
        atomStart = at + 1;
        at = indexOfAt(text, atomStart, runEnd);
      }
    }
    pos = runEnd;
  }
  return -1;
}
function recoverAddrSpec(address, text) {
  if (!HAS_WHITESPACE.test(address)) {
    return { address, text };
  }
  let rest;
  const quoted = address.match(QUOTED_LOCAL_ADDR);
  if (quoted) {
    if (!quoted[2]) {
      return { address, text };
    }
    address = quoted[1];
    rest = [quoted[2]];
  } else {
    if (address.indexOf('"') >= 0) {
      return { address, text };
    }
    const parts = address.split(/\s+/);
    let addrIndex = parts.findIndex((part) => ADDR_SPEC.test(part));
    if (addrIndex < 0) {
      addrIndex = parts.findIndex((part) => LOOSE_ADDR_SPEC.test(part));
    }
    if (addrIndex < 0) {
      return { address, text };
    }
    address = parts.splice(addrIndex, 1)[0];
    rest = parts;
  }
  return {
    address,
    text: [text, ...rest].filter((part) => part).join(" ")
  };
}
function stripAddressComments(address) {
  const comments = [];
  let result = "";
  let comment = "";
  let depth = 0;
  let closer = "";
  let lastChar = "";
  for (let i = 0, len = address.length; i < len; i++) {
    const chr = address.charAt(i);
    if (depth) {
      if (chr === "\\" && i < len - 1) {
        comment += address.charAt(++i);
      } else if (chr === "(") {
        depth++;
        comment += chr;
      } else if (chr === ")" && !--depth) {
        comments.push(comment.trim());
        comment = "";
        if (lastChar !== "@" && address.charAt(i + 1) !== "@") {
          result += " ";
          lastChar = " ";
        }
      } else {
        comment += chr;
      }
      continue;
    }
    if (closer) {
      if (chr === "\\" && closer === '"' && i < len - 1) {
        result += chr + address.charAt(++i);
        lastChar = address.charAt(i);
        continue;
      }
      if (chr === closer) {
        closer = "";
      }
    } else if (chr === '"') {
      closer = '"';
    } else if (chr === "[") {
      closer = "]";
    } else if (chr === "(") {
      depth = 1;
      continue;
    }
    result += chr;
    lastChar = chr;
  }
  if (depth) {
    comments.push(comment.trim());
  }
  return { address: result.trim(), comments: comments.filter((text) => text) };
}
function hasAngleAddress(text) {
  let start = -1;
  let at = -1;
  for (let i = 0; i < text.length; i++) {
    const chr = text.charAt(i);
    if (chr === "<") {
      start = i + 1;
      at = -1;
    } else if (chr === ">") {
      if (at >= 0 && at < i - 1) {
        return true;
      }
      start = -1;
      at = -1;
    } else if (chr === "@" && start >= 0 && at < 0 && i > start) {
      at = i;
    }
  }
  return false;
}
function _handleAddress(tokens, depth) {
  let isGroup = false;
  let state = "text";
  let addresses = [];
  let data = {
    address: [],
    comment: [],
    group: [],
    text: [],
    textWasQuoted: []
    // Track which text tokens came from inside quotes
  };
  let i;
  let len;
  let insideQuotes = false;
  const lastChars = { address: "", comment: "", group: "", text: "" };
  for (i = 0, len = tokens.length; i < len; i++) {
    let token = tokens[i];
    let prevToken = i ? tokens[i - 1] : null;
    if (token.type === "operator") {
      switch (token.value) {
        case "<":
          state = "address";
          insideQuotes = false;
          break;
        case "(":
          state = "comment";
          insideQuotes = false;
          break;
        case ":":
          state = "group";
          isGroup = true;
          insideQuotes = false;
          break;
        case '"':
          insideQuotes = !insideQuotes;
          state = "text";
          break;
        default:
          state = "text";
          insideQuotes = false;
          break;
      }
    } else if (token.value) {
      const prevPrevToken = i > 1 ? tokens[i - 2] : null;
      const opensAfterEmptyQuotedString = prevToken?.type === "operator" && prevToken.value === '"' && !!prevToken.noBreak && prevPrevToken?.type === "operator" && prevPrevToken.value === '"';
      if (state === "address") {
        token.value = token.value.replace(/^[^<]*<\s*/, "");
      }
      const joins = prevToken && prevToken.noBreak && data[state].length && (prevToken.value !== ")" || lastChars[state] === "@" || token.value.charAt(0) === "@");
      if (joins) {
        data[state][data[state].length - 1] += token.value;
        if (state === "text" && insideQuotes) {
          data.textWasQuoted[data.textWasQuoted.length - 1] = true;
        }
      } else {
        data[state].push(token.value);
        if (state === "text") {
          data.textWasQuoted.push(insideQuotes || opensAfterEmptyQuotedString);
        }
      }
      lastChars[state] = token.value.charAt(token.value.length - 1);
    }
  }
  if (!data.text.length && data.comment.length) {
    data.text = data.comment;
    data.comment = [];
  }
  if (isGroup) {
    const text = data.text.join(" ");
    let groupMembers = [];
    if (data.group.length) {
      let parsedGroup = parseAddressList(data.group.join(","), depth + 1);
      parsedGroup.forEach((member) => {
        if (member.group) {
          for (const nestedMember of member.group) {
            groupMembers.push(nestedMember);
          }
        } else {
          groupMembers.push(member);
        }
      });
    }
    addresses.push({
      name: decodeWords(text),
      group: groupMembers
    });
  } else {
    const addressComments = [];
    const addressParts = [];
    for (const part of data.address) {
      if (part.indexOf("(") < 0) {
        addressParts.push(part);
        continue;
      }
      const stripped = stripAddressComments(part);
      for (const comment of stripped.comments) {
        addressComments.push(comment);
      }
      if (stripped.address) {
        addressParts.push(stripped.address);
      }
    }
    data.address = addressParts;
    if (!data.address.length && data.text.length) {
      for (i = data.text.length - 1; i >= 0; i--) {
        if (!data.textWasQuoted[i] && ADDR_SPEC.test(data.text[i])) {
          data.address = data.text.splice(i, 1);
          data.textWasQuoted.splice(i, 1);
          break;
        }
      }
      if (!data.address.length) {
        for (i = data.text.length - 1; i >= 0; i--) {
          if (!data.textWasQuoted[i]) {
            const part = data.text[i];
            const start = looseAddressStart(part);
            if (start >= 0) {
              LOOSE_TEXT_ADDR.lastIndex = start;
              const match = LOOSE_TEXT_ADDR.exec(part);
              if (match) {
                data.address = [match[0].trim()];
                data.text[i] = (part.slice(0, start) + " " + part.slice(start + match[0].length)).trim();
                break;
              }
            }
            data.text[i] = part.trim();
          }
        }
      }
    }
    if (!data.text.length && data.comment.length) {
      data.text = data.comment;
      data.comment = [];
    }
    if (data.address.length > 1) {
      data.text = data.text.concat(data.address.splice(1));
    }
    const addressFromQuotedText = !data.address.length && data.textWasQuoted.some((wasQuoted) => wasQuoted);
    let text = data.text.join(" ");
    let addressValue = data.address.join(" ");
    if (!addressValue && isEncodedWordsOnly(text.trim())) {
      const decodedText = decodeWords(text);
      if (!addressFromQuotedText && hasAngleAddress(decodedText)) {
        const parsedSubAddresses = parseAddressList(decodedText, depth + 1);
        if (parsedSubAddresses.length) {
          return parsedSubAddresses;
        }
      }
      return [{ address: "", name: decodedText }];
    }
    if (addressFromQuotedText && text.indexOf("@") >= 0) {
      addressValue = quoteLocalPart(text);
      text = "";
    }
    ({ address: addressValue, text } = recoverAddrSpec(addressValue, text));
    if (!text && addressComments.length) {
      text = addressComments.join(" ");
    }
    const rawName = text || addressValue || "";
    const address = {
      address: addressValue || text || "",
      name: decodeWords(rawName)
    };
    if (address.address === rawName || address.address === address.name) {
      if (address.address.match(/@/)) {
        address.name = "";
      } else {
        address.address = "";
      }
    }
    addresses.push(address);
  }
  return addresses;
}
var Tokenizer = class {
  /**
   * @param str Address field string
   */
  constructor(str) {
    this.str = (str || "").toString();
    this.operatorCurrent = "";
    this.operatorExpecting = "";
    this.node = null;
    this.escaped = false;
    this.inDomainLiteral = false;
    this.afterAt = false;
    this.list = [];
    this.operators = {
      '"': '"',
      "(": ")",
      "<": ">",
      ",": "",
      ":": ";",
      // Semicolons are not a legal delimiter per the RFC2822 grammar other
      // than for terminating a group, but they are also not valid for any
      // other use in this context.  Given that some mail clients have
      // historically allowed the semicolon as a delimiter equivalent to the
      // comma in their UI, it makes sense to treat them the same as a comma
      // when used outside of a group.
      ";": ""
    };
  }
  /**
   * Tokenizes the original input string
   *
   * @return An array of operator|text tokens
   */
  tokenize() {
    let list = [];
    for (let i = 0, len = this.str.length; i < len; i++) {
      let chr = this.str.charAt(i);
      let nextChr = i < len - 1 ? this.str.charAt(i + 1) : null;
      this.checkChar(chr, nextChr);
    }
    this.list.forEach((node) => {
      node.value = (node.value || "").toString().trim();
      if (node.value) {
        list.push(node);
      }
    });
    return list;
  }
  /**
   * Checks if a character is an operator or text and acts accordingly
   *
   * @param chr Character from the address field
   * @param nextChr The character that follows, or null at the end of the field
   */
  checkChar(chr, nextChr) {
    if (!this.escaped && !this.operatorExpecting) {
      if (!this.inDomainLiteral && chr === "[" && this.afterAt) {
        this.inDomainLiteral = true;
      } else if (this.inDomainLiteral && (chr === "]" || chr === "," || chr === ";")) {
        this.inDomainLiteral = false;
      }
    }
    if (this.escaped) {
    } else if (chr === this.operatorExpecting) {
      this.node = {
        type: "operator",
        value: chr
      };
      if (nextChr && ![" ", "	", "\r", "\n", ",", ";"].includes(nextChr)) {
        this.node.noBreak = true;
      }
      this.list.push(this.node);
      this.node = null;
      if (chr !== ")") {
        this.afterAt = false;
      }
      this.operatorExpecting = "";
      this.escaped = false;
      return;
    } else if (!this.operatorExpecting && !(this.inDomainLiteral && chr === ":") && chr in this.operators) {
      this.node = {
        type: "operator",
        value: chr
      };
      this.list.push(this.node);
      this.node = null;
      if (chr !== "(") {
        this.afterAt = false;
      }
      this.operatorExpecting = this.operators[chr];
      this.escaped = false;
      return;
    } else if ((this.operatorExpecting === '"' || this.operatorExpecting === ")") && chr === "\\") {
      this.escaped = true;
      return;
    }
    if (!this.node) {
      this.node = {
        type: "text",
        value: ""
      };
      this.list.push(this.node);
    }
    if (chr === "\n") {
      chr = " ";
    }
    if (chr.charCodeAt(0) >= 33 || [" ", "	"].includes(chr)) {
      this.node.value += chr;
      if (!this.operatorExpecting && chr !== " " && chr !== "	") {
        this.afterAt = chr === "@";
      }
    }
    this.escaped = false;
  }
};
var MAX_NESTED_GROUP_DEPTH = 50;
function parseAddressList(str, depth) {
  if (depth > MAX_NESTED_GROUP_DEPTH) {
    return [];
  }
  let tokenizer = new Tokenizer(str);
  let tokens = tokenizer.tokenize();
  let addresses = [];
  let address = [];
  const parsedAddresses = [];
  tokens.forEach((token) => {
    if (token.type === "operator" && (token.value === "," || token.value === ";")) {
      if (address.length) {
        addresses.push(address);
      }
      address = [];
    } else {
      address.push(token);
    }
  });
  if (address.length) {
    addresses.push(address);
  }
  for (const tokens2 of addresses) {
    for (const address2 of _handleAddress(tokens2, depth)) {
      parsedAddresses.push(address2);
    }
  }
  const mergedAddresses = [];
  for (let i = parsedAddresses.length - 1; i >= 0; i--) {
    const current = parsedAddresses[i];
    const next = mergedAddresses.length ? mergedAddresses[mergedAddresses.length - 1] : null;
    if (next && current.address === "" && current.name && !current.group && next.address && next.name) {
      next.name = current.name + ", " + next.name;
    } else {
      mergedAddresses.push(current);
    }
  }
  return mergedAddresses.reverse();
}
function addressParser(str, options) {
  const parsedAddresses = parseAddressList(str, 0);
  if (options && options.flatten) {
    let addresses = [];
    let walkAddressList = (list) => {
      list.forEach((address) => {
        if (address.group) {
          return walkAddressList(address.group);
        } else {
          addresses.push(address);
        }
      });
    };
    walkAddressList(parsedAddresses);
    return addresses;
  }
  return parsedAddresses;
}
var address_parser_default = addressParser;

// node_modules/postal-mime/dist/esm/base64-encoder.js
function base64ArrayBuffer(arrayBuffer) {
  let base64 = "";
  const encodings = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  const bytes2 = new Uint8Array(arrayBuffer);
  const byteLength = bytes2.byteLength;
  const byteRemainder = byteLength % 3;
  const mainLength = byteLength - byteRemainder;
  let a, b, c, d;
  let chunk;
  for (let i = 0; i < mainLength; i = i + 3) {
    chunk = bytes2[i] << 16 | bytes2[i + 1] << 8 | bytes2[i + 2];
    a = (chunk & 16515072) >> 18;
    b = (chunk & 258048) >> 12;
    c = (chunk & 4032) >> 6;
    d = chunk & 63;
    base64 += encodings[a] + encodings[b] + encodings[c] + encodings[d];
  }
  if (byteRemainder === 1) {
    chunk = bytes2[mainLength];
    a = (chunk & 252) >> 2;
    b = (chunk & 3) << 4;
    base64 += encodings[a] + encodings[b] + "==";
  } else if (byteRemainder === 2) {
    chunk = bytes2[mainLength] << 8 | bytes2[mainLength + 1];
    a = (chunk & 64512) >> 10;
    b = (chunk & 1008) >> 4;
    c = (chunk & 15) << 2;
    base64 += encodings[a] + encodings[b] + encodings[c] + "=";
  }
  return base64;
}

// node_modules/postal-mime/dist/esm/postal-mime.js
function isReadableStream(value) {
  return !!value && typeof value.getReader === "function";
}
function isBlob(value) {
  return value instanceof Blob || Object.prototype.toString.call(value) === "[object Blob]";
}
function renderEntry(textEntry, textType, convert) {
  if (textEntry.type === "subMessage") {
    return textType === "html" ? formatHtmlHeader(textEntry.value) : formatTextHeader(textEntry.value);
  }
  if (!convert) {
    return textEntry.value;
  }
  return textType === "html" ? textToHtml(textEntry.value) : htmlToText(textEntry.value);
}
var LINE_FEED = new Uint8Array([10]);
var MAX_NESTING_DEPTH = 256;
var MAX_HEADERS_SIZE = 2 * 1024 * 1024;
var MAX_PART_COUNT = 1e4;
var MAX_RFC822_NESTING_DEPTH = 10;
function parseLimitOption(value, defaultValue, name) {
  if (value === void 0 || value === null) {
    return defaultValue;
  }
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new TypeError(`${name} must be a non-negative integer`);
  }
  return value;
}
var PostalMime = class _PostalMime {
  /**
   * Parses a raw email message
   *
   * @param buf Raw email message
   * @param options Parser options
   * @returns The parsed email
   */
  // async so that an invalid option rejects the returned promise instead of throwing
  // synchronously, which would escape a `.catch()` chain
  static async parse(buf, options) {
    const parser = new _PostalMime(options);
    return parser.parse(buf);
  }
  constructor(options) {
    this.options = options || {};
    this.mimeOptions = {
      maxNestingDepth: parseLimitOption(this.options.maxNestingDepth, MAX_NESTING_DEPTH, "maxNestingDepth"),
      maxHeadersSize: parseLimitOption(this.options.maxHeadersSize, MAX_HEADERS_SIZE, "maxHeadersSize"),
      maxPartCount: parseLimitOption(this.options.maxPartCount, MAX_PART_COUNT, "maxPartCount")
    };
    this.maxRfc822NestingDepth = parseLimitOption(this.options.maxRfc822NestingDepth, MAX_RFC822_NESTING_DEPTH, "maxRfc822NestingDepth");
    this.rfc822NestingDepth = 0;
    this.headerSize = 0;
    this.partCount = 0;
    this.root = this.currentNode = new MimeNode({
      postalMime: this,
      ...this.mimeOptions
    });
    this.boundaries = [];
    this.textContent = {};
    this.textMap = /* @__PURE__ */ new Map();
    this.textTypes = /* @__PURE__ */ new Set();
    this.attachments = [];
    this.forceRfc822Attachments = false;
    this.attachmentEncoding = (this.options.attachmentEncoding || "").toString().replace(/[-_\s]/g, "").trim().toLowerCase() || "arraybuffer";
    this.started = false;
  }
  /** @internal */
  async finalize() {
    await this.root.finalize();
  }
  /** @internal */
  async processLine(line, lineBreak, isFinal) {
    let boundaries = this.boundaries;
    if (boundaries.length && line.length > 2 && line[0] === 45 && line[1] === 45) {
      for (let i = boundaries.length - 1; i >= 0; i--) {
        let boundary = boundaries[i];
        if (line.length < boundary.value.length + 2) {
          continue;
        }
        let boundaryMatches = true;
        for (let j = 0; j < boundary.value.length; j++) {
          if (line[j + 2] !== boundary.value[j]) {
            boundaryMatches = false;
            break;
          }
        }
        if (!boundaryMatches) {
          continue;
        }
        let boundaryEnd = boundary.value.length + 2;
        let isTerminator = false;
        if (line.length >= boundary.value.length + 4 && line[boundary.value.length + 2] === 45 && line[boundary.value.length + 3] === 45) {
          isTerminator = true;
          boundaryEnd = boundary.value.length + 4;
        }
        let hasValidTrailing = true;
        for (let j = boundaryEnd; j < line.length; j++) {
          if (!isWsp(line[j])) {
            hasValidTrailing = false;
            break;
          }
        }
        if (!hasValidTrailing) {
          continue;
        }
        if (isTerminator) {
          await boundary.node.finalize(true);
          this.currentNode = boundary.node.parentNode || this.root;
        } else {
          await boundary.node.finalizeChildNodes(true);
          this.currentNode = new MimeNode({
            postalMime: this,
            parentNode: boundary.node,
            parentMultipartType: boundary.node.contentType.multipart,
            ...this.mimeOptions
          });
        }
        if (isFinal) {
          return this.finalize();
        }
        return;
      }
    }
    if (this.currentNode.feed(line, lineBreak)) {
      return this.processLine(line, lineBreak, isFinal);
    }
    if (isFinal) {
      return this.finalize();
    }
  }
  // Reads the next line without its line break. The break itself, the exact bytes between
  // the line and the next one, is returned alongside, because the body decoders write it
  // back out: a part has to keep the line endings it was sent with, see PassThroughDecoder.
  /** @internal */
  readLine() {
    let startPos = this.readPos;
    let endPos = this.readPos;
    while (this.readPos < this.av.length) {
      const c = this.av[this.readPos++];
      if (c !== 13 && c !== 10) {
        endPos = this.readPos;
      }
      if (c === 10) {
        return {
          bytes: new Uint8Array(this.buf, startPos, endPos - startPos),
          lineBreak: this.av.subarray(endPos, this.readPos),
          done: this.readPos >= this.av.length
        };
      }
    }
    return {
      bytes: new Uint8Array(this.buf, startPos, endPos - startPos),
      lineBreak: LINE_FEED,
      done: this.readPos >= this.av.length
    };
  }
  // Records a text part or a nested message under the node that selects it
  /** @internal */
  addTextEntry(selector, textType, item) {
    let textEntry = this.textMap.get(selector);
    if (!textEntry) {
      textEntry = {};
      this.textMap.set(selector, textEntry);
    }
    const entries = textEntry[textType] || [];
    textEntry[textType] = entries;
    entries.push(item);
    this.textTypes.add(textType);
  }
  // Sorts every leaf of the tree into text content, nested messages and attachments.
  // `alternative` is the closest enclosing multipart/alternative, if any: its text parts
  // are collected under the alternative itself, so that the body is assembled from one
  // representation of it rather than from every one. `related` tells whether the part
  // sits inside a multipart/related, where a Content-ID makes it an inline resource
  /** @internal */
  async collectNode(node, alternative, related) {
    if (!node.contentType.multipart) {
      const inlineRfc822 = this.isInlineMessageRfc822(node);
      const rfc822DepthExceeded = inlineRfc822 && this.rfc822NestingDepth >= this.maxRfc822NestingDepth;
      if (inlineRfc822 && !rfc822DepthExceeded) {
        await this.collectSubMessage(node);
      } else if (this.isInlineTextNode(node)) {
        const textType = node.contentType.parsed.value === "text/html" ? "html" : "plain";
        this.addTextEntry(alternative || node, textType, { type: "text", value: node.getTextContent() });
      } else if (node.content) {
        this.collectAttachment(node, node.content, related, rfc822DepthExceeded);
      }
    } else if (node.contentType.multipart === "alternative") {
      alternative = node;
    } else if (node.contentType.multipart === "related") {
      related = true;
    }
    for (let childNode of node.childNodes) {
      await this.collectNode(childNode, alternative, related);
    }
  }
  // Parses an inline message/rfc822 part with a nested parser and takes over its text
  // parts and attachments
  /** @internal */
  async collectSubMessage(node) {
    const subParser = new _PostalMime({
      // Only the limits are inherited. Options that decide how a part
      // is classified stay with the parser that was configured.
      ...this.mimeOptions,
      maxRfc822NestingDepth: this.maxRfc822NestingDepth,
      // attachments are encoded by the parent parser, keep raw buffers here
      attachmentEncoding: "arraybuffer"
    });
    subParser.rfc822NestingDepth = this.rfc822NestingDepth + 1;
    const subMessage = node.subMessage = await subParser.parse(node.content || new ArrayBuffer(0));
    if (subMessage.text || !subMessage.html) {
      this.addTextEntry(node, "plain", { type: "subMessage", value: subMessage });
    }
    if (subMessage.html) {
      this.addTextEntry(node, "html", { type: "subMessage", value: subMessage });
    }
    subParser.textMap.forEach((subTextEntry, subTextNode) => {
      this.textMap.set(subTextNode, subTextEntry);
    });
    for (let attachment of subMessage.attachments) {
      this.attachments.push(attachment);
    }
  }
  /** @internal */
  collectAttachment(node, content, related, rfc822DepthExceeded) {
    const filename = node.contentDisposition.parsed.params.filename || node.contentType.parsed.params.name || null;
    const attachment = {
      // An RFC 2231 filename is percent decoded without folding, since the same decoder
      // produces the boundary, so the fold happens here, see foldLineBreaks
      filename: filename ? decodeWords(foldLineBreaks(filename)) : null,
      mimeType: node.contentType.parsed.value,
      disposition: node.contentDisposition.parsed.value || null
    };
    if (related && node.contentId && !rfc822DepthExceeded) {
      attachment.related = true;
    }
    if (rfc822DepthExceeded) {
      attachment.rfc822DepthExceeded = true;
    }
    if (node.contentDescription) {
      attachment.description = decodeWords(node.contentDescription);
    }
    if (node.contentId) {
      attachment.contentId = node.contentId;
    }
    switch (node.contentType.parsed.value) {
      // Special handling for calendar events
      case "text/calendar":
      case "application/ics": {
        if (node.contentType.parsed.params.method) {
          attachment.method = node.contentType.parsed.params.method.toString().toUpperCase().trim();
        }
        const decodedText = node.getTextContent();
        let end = decodedText.length;
        while (end > 0 && decodedText.charCodeAt(end - 1) === 10) {
          end--;
        }
        attachment.content = textEncoder.encode(decodedText.slice(0, end) + "\n");
        break;
      }
      // Regular attachments
      default:
        attachment.content = content;
    }
    this.attachments.push(attachment);
  }
  // Joins the collected text parts into the plain and html bodies
  /** @internal */
  renderTextContent() {
    const textContent = {};
    this.textMap.forEach((mapEntry) => {
      this.textTypes.forEach((textType) => {
        const output = textContent[textType] || [];
        textContent[textType] = output;
        const ownEntries = mapEntry[textType];
        const entries = ownEntries || mapEntry[textType === "html" ? "plain" : "html"] || [];
        for (const textEntry of entries) {
          output.push(renderEntry(textEntry, textType, !ownEntries));
        }
      });
    });
    Object.keys(textContent).forEach((textType) => {
      this.textContent[textType] = textContent[textType].join("\n");
    });
  }
  /** @internal */
  isInlineTextNode(node) {
    if (node.contentDisposition.parsed.value === "attachment") {
      return false;
    }
    switch (node.contentType.parsed.value) {
      case "text/html":
      case "text/plain":
        return true;
      case "text/calendar":
      case "text/csv":
      default:
        return false;
    }
  }
  /** @internal */
  isInlineMessageRfc822(node) {
    if (this.forceRfc822Attachments || node.contentType.parsed.value !== "message/rfc822") {
      return false;
    }
    let disposition = node.contentDisposition.parsed.value || (this.options.rfc822Attachments ? "attachment" : "inline");
    return disposition === "inline";
  }
  // A delivery status or feedback report carries the offending message as a part. It is
  // evidence rather than content, so it is not inlined into the body of the report
  /** @internal */
  hasReportParts() {
    let found = false;
    let walk = (node) => {
      if (!node.contentType.multipart) {
        if (["message/delivery-status", "message/feedback-report"].includes(node.contentType.parsed.value)) {
          found = true;
        }
      }
      for (let childNode of node.childNodes) {
        walk(childNode);
      }
    };
    walk(this.root);
    return found;
  }
  /** @internal */
  async resolveStream(stream) {
    let chunkLen = 0;
    let chunks = [];
    const reader = stream.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      chunks.push(value);
      chunkLen += value.length;
    }
    const result = new Uint8Array(chunkLen);
    let chunkPointer = 0;
    for (let chunk of chunks) {
      result.set(chunk, chunkPointer);
      chunkPointer += chunk.length;
    }
    return result;
  }
  /** @internal */
  async resolveInput(buf) {
    let input = buf;
    if (isReadableStream(input)) {
      input = await this.resolveStream(input);
    }
    input = input || new ArrayBuffer(0);
    if (typeof input === "string") {
      input = textEncoder.encode(input);
    }
    if (isBlob(input)) {
      input = await blobToArrayBuffer(input);
    }
    if (ArrayBuffer.isView(input)) {
      return input.buffer.slice(input.byteOffset, input.byteOffset + input.byteLength);
    }
    return input;
  }
  // Properties are added in the order the output has always had them, so the required
  // ones are filled in below rather than in the literal
  /** @internal */
  buildMessage() {
    const headers = this.root.headers;
    const message2 = {
      headers: headers.map((entry) => ({
        key: entry.key,
        originalKey: entry.originalKey,
        value: entry.value
      }))
    };
    for (const key of ["from", "sender"]) {
      const addressHeader = headers.find((line) => line.key === key);
      if (addressHeader && addressHeader.value) {
        const addresses = address_parser_default(addressHeader.value);
        if (addresses && addresses.length) {
          message2[key] = addresses[0];
        }
      }
    }
    for (const [key, camelKey] of [
      ["delivered-to", "deliveredTo"],
      ["return-path", "returnPath"]
    ]) {
      const addressHeader = headers.find((line) => line.key === key);
      if (addressHeader && addressHeader.value) {
        const addresses = address_parser_default(addressHeader.value);
        if (addresses && addresses.length && addresses[0].address) {
          message2[camelKey] = addresses[0].address;
        }
      }
    }
    for (const [key, camelKey] of [
      ["to", "to"],
      ["cc", "cc"],
      ["bcc", "bcc"],
      ["reply-to", "replyTo"]
    ]) {
      const addresses = [];
      for (const entry of headers) {
        if (entry.key === key && entry.value) {
          for (const address of address_parser_default(entry.value)) {
            addresses.push(address);
          }
        }
      }
      if (addresses.length) {
        message2[camelKey] = addresses;
      }
    }
    for (const [key, camelKey] of [
      ["subject", "subject"],
      ["message-id", "messageId"],
      ["in-reply-to", "inReplyTo"],
      ["references", "references"]
    ]) {
      const header2 = headers.find((line) => line.key === key);
      if (header2 && header2.value) {
        message2[camelKey] = decodeWords(header2.value);
      }
    }
    let dateHeader = headers.find((line) => line.key === "date");
    if (dateHeader) {
      let date = new Date(dateHeader.value);
      message2.date = date.toString() === "Invalid Date" ? dateHeader.value : date.toISOString();
    }
    if (this.textContent.html) {
      message2.html = this.textContent.html;
    }
    if (this.textContent.plain) {
      message2.text = this.textContent.plain;
    }
    message2.attachments = this.attachments;
    message2.headerLines = this.root.rawHeaderLines.slice();
    return message2;
  }
  // Every attachment holds binary content at this point, since nested parsers are
  // created with the arraybuffer encoding. The string check narrows the public content
  // type rather than handling a reachable case
  /** @internal */
  encodeAttachments() {
    switch (this.attachmentEncoding) {
      case "arraybuffer":
        break;
      case "base64":
        for (let attachment of this.attachments) {
          if (attachment.content && typeof attachment.content !== "string") {
            attachment.content = base64ArrayBuffer(attachment.content);
            attachment.encoding = "base64";
          }
        }
        break;
      case "utf8": {
        let attachmentDecoder = new TextDecoder("utf8");
        for (let attachment of this.attachments) {
          if (attachment.content && typeof attachment.content !== "string") {
            attachment.content = attachmentDecoder.decode(attachment.content);
            attachment.encoding = "utf8";
          }
        }
        break;
      }
      default:
        throw new Error("Unknown attachment encoding");
    }
  }
  /**
   * Parses a raw email message. A parser instance can be used once
   *
   * @param buf Raw email message
   * @returns The parsed email
   */
  async parse(buf) {
    if (this.started) {
      throw new Error("Can not reuse parser, create a new PostalMime object");
    }
    this.started = true;
    this.buf = await this.resolveInput(buf);
    this.av = new Uint8Array(this.buf);
    this.readPos = 0;
    while (this.readPos < this.av.length) {
      const line = this.readLine();
      await this.processLine(line.bytes, line.lineBreak, line.done);
    }
    this.forceRfc822Attachments = Boolean(this.options.forceRfc822Attachments) || this.hasReportParts();
    await this.collectNode(this.root, false, false);
    this.renderTextContent();
    const message2 = this.buildMessage();
    this.encodeAttachments();
    return message2;
  }
};

// src/archive.mjs
var RECORD_PREFIX = "mail:";
var ID_PATTERN = /^mail_\d{16}$/;
var SCAN_LIMIT = 40;
var PAGE_LIMIT = 20;
var READ_PREFIX = "cmail-read:";
var ARCHIVED_PREFIX = "cmail-archived:";
var ARCHIVED_COUNT = "cmail-archived-count";
var PURPOSE = "cloudflare-mail-archive-v1";
function header(payload, name) {
  const item = Object.entries(payload.headers || {}).find(([key]) => key.toLowerCase() === name);
  return String(item?.[1] || "");
}
function fromBase64(value) {
  const binary = atob(value);
  return Uint8Array.from(binary, (character) => character.charCodeAt(0));
}
function toBase64(bytes2) {
  let value = "";
  for (let offset = 0; offset < bytes2.length; offset += 32768) {
    value += String.fromCharCode(...bytes2.subarray(offset, offset + 32768));
  }
  return btoa(value);
}
function plainTextFromHtml(html) {
  return String(html || "").replace(/<(script|style)\b[^>]*>[\s\S]*?<\/\1\s*>/gi, "").replace(/<(?:br|\/p|\/div|\/tr|\/h[1-6])\b[^>]*>/gi, "\n").replace(/<[^>]*>/g, "").replace(/&(?:nbsp|amp|lt|gt|quot|apos);/g, (entity) => ({
    "&nbsp;": " ",
    "&amp;": "&",
    "&lt;": "<",
    "&gt;": ">",
    "&quot;": '"',
    "&apos;": "'"
  })[entity]).replace(/\n{3,}/g, "\n\n").trim();
}
var ArchiveInputError = class extends Error {
};
var MailQueue2 = class extends MailQueue {
  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (!["/archive/list", "/archive/message", "/archive/read-status", "/archive/actions", "/archive/thread-index"].includes(path)) return super.fetch(request);
    if (request.method !== "POST") return Response.json({ error: "method_not_allowed" }, { status: 405 });
    try {
      const parameters = await request.json();
      const result = path === "/archive/list" ? await this.listArchive(parameters) : path === "/archive/read-status" ? await this.setReadStatus(parameters) : path === "/archive/actions" ? await this.applyAction(parameters) : path === "/archive/thread-index" ? await this.listThreadIndex(parameters) : await this.readArchiveMessage(parameters);
      return Response.json(result, { status: result ? 200 : 404 });
    } catch (error) {
      if (error instanceof ArchiveInputError || error instanceof SyntaxError) {
        return Response.json({ error: "invalid_request" }, { status: 400 });
      }
      return Response.json({ error: "archive_read_failed" }, { status: 503 });
    }
  }
  async loadPayload(record) {
    if (record.payload) return record.payload;
    const envelope = record.payload_encrypted || (record.payload_ref ? await loadArchiveEnvelope({ storage: this.state.storage, ref: record.payload_ref }) : null);
    if (!envelope) throw new Error("missing_archive_payload");
    return decryptArchivePayload({ masterKey: this.env.LIFE_OPS_MASTER_KEY, purpose: PURPOSE, envelope });
  }
  async listThreadIndex({ cursor = "" } = {}) {
    if (typeof cursor !== "string" || cursor && !ID_PATTERN.test(cursor)) throw new ArchiveInputError();
    const options = { prefix: RECORD_PREFIX, reverse: true, limit: SCAN_LIMIT + 1 };
    if (cursor) options.end = `${RECORD_PREFIX}${cursor}`;
    const entries = [...(await this.state.storage.list(options)).values()];
    const items = [];
    let scanned = 0;
    let failed = 0;
    let lastId = null;
    for (const record of entries.slice(0, SCAN_LIMIT)) {
      scanned += 1;
      lastId = record.id;
      try {
        const payload = await this.loadPayload(record);
        const parsed = header(payload, "message-id") ? null : await PostalMime.parse(fromBase64(payload.raw_base64));
        items.push({
          id: record.id,
          received_at: record.received_at,
          date: parsed?.date || header(payload, "date") || record.received_at,
          message_id: parsed?.messageId || header(payload, "message-id"),
          in_reply_to: parsed?.inReplyTo || header(payload, "in-reply-to"),
          references: parsed?.references || header(payload, "references")
        });
      } catch {
        failed += 1;
      }
    }
    const meta = await this.state.storage.get("meta") || {};
    return { items, scanned, failed, next_cursor: scanned < entries.length ? lastId : null, total_stored: Number(meta.total_stored || 0) };
  }
  async listArchive(parameters = {}) {
    const query = String(parameters.query || "").trim().toLowerCase();
    const mailbox = String(parameters.mailbox || "").trim().toLowerCase();
    const cursor = String(parameters.cursor || "");
    const after = String(parameters.after || "");
    const before = String(parameters.before || "");
    const view = String(parameters.view || "inbox");
    const limit = parameters.limit === void 0 || parameters.limit === "" ? PAGE_LIMIT : Number(parameters.limit);
    if (query.length > 200 || mailbox.length > 254 || !["inbox", "archive", "all"].includes(view) || !Number.isSafeInteger(limit) || limit < 1 || limit > 100 || cursor && !ID_PATTERN.test(cursor) || [after, before].some((value) => value && (!/^\d{4}-\d{2}-\d{2}$/.test(value) || Number.isNaN(Date.parse(value))))) {
      throw new ArchiveInputError();
    }
    const options = { prefix: RECORD_PREFIX, reverse: true, limit: SCAN_LIMIT + 1 };
    if (cursor) options.end = `${RECORD_PREFIX}${cursor}`;
    const records = await (this.listMailRecords ? this.listMailRecords(options) : this.state.storage.list(options));
    const entries = [...records.entries()];
    const items = [];
    let scanned = 0;
    let failed = 0;
    let lastId = null;
    for (const [, record] of entries.slice(0, SCAN_LIMIT)) {
      scanned += 1;
      lastId = record.id;
      const date = String(record.received_at || "").slice(0, 10);
      if (after && date < after || before && date > before) continue;
      try {
        const archived = await this.state.storage.get(`${ARCHIVED_PREFIX}${record.id}`) === true;
        if (view === "inbox" && archived || view === "archive" && !archived) continue;
        const payload = await this.loadPayload(record);
        const item = {
          id: record.id,
          received_at: record.received_at,
          from: decodeWords(header(payload, "from")) || String(payload.envelope_from || ""),
          to: decodeWords(header(payload, "to")) || String(payload.envelope_to || ""),
          mailbox: String(payload.envelope_to || ""),
          subject: decodeWords(header(payload, "subject")) || "(No subject)",
          read: await this.state.storage.get(`${READ_PREFIX}${record.id}`) === true,
          archived
        };
        if (mailbox && item.mailbox.toLowerCase() !== mailbox) continue;
        if (query && ![item.subject, item.from, item.to, item.mailbox].some((value) => value.toLowerCase().includes(query))) {
          const parsed = await PostalMime.parse(fromBase64(payload.raw_base64));
          const text = `${parsed.text || ""}
${plainTextFromHtml(parsed.html)}`;
          if (!text.toLowerCase().includes(query)) continue;
        }
        items.push(item);
        if (items.length === limit) break;
      } catch {
        failed += 1;
      }
    }
    const meta = await this.state.storage.get("meta") || {};
    const archivedCount = Number(await this.state.storage.get(ARCHIVED_COUNT) || 0);
    const totalStored = Number(meta.total_stored || 0);
    return {
      items,
      next_cursor: scanned < entries.length ? lastId : null,
      scanned,
      failed,
      total_stored: totalStored,
      total_view: view === "all" ? totalStored : view === "archive" ? archivedCount : Math.max(0, totalStored - archivedCount),
      read_only: true
    };
  }
  async setReadStatus({ id, read } = {}) {
    if (!ID_PATTERN.test(String(id || "")) || typeof read !== "boolean") throw new ArchiveInputError();
    if (!await this.state.storage.get(`${RECORD_PREFIX}${id}`)) return null;
    await this.state.storage.put(`${READ_PREFIX}${id}`, read);
    return { id, read };
  }
  async applyAction({ ids, action } = {}) {
    if (!Array.isArray(ids) || !ids.length || ids.length > 100 || ids.some((id) => typeof id !== "string" || !ID_PATTERN.test(id)) || new Set(ids).size !== ids.length || !["read", "unread", "archive", "unarchive"].includes(action)) {
      throw new ArchiveInputError();
    }
    return this.state.storage.transaction(async (tx) => {
      for (const id of ids) if (!await tx.get(`${RECORD_PREFIX}${id}`)) return null;
      let archivedCount = Number(await tx.get(ARCHIVED_COUNT) || 0);
      const items = [];
      for (const id of ids) {
        let read = await tx.get(`${READ_PREFIX}${id}`) === true;
        let archived = await tx.get(`${ARCHIVED_PREFIX}${id}`) === true;
        if (action === "read" || action === "unread") {
          read = action === "read";
          await tx.put(`${READ_PREFIX}${id}`, read);
        } else {
          const next = action === "archive";
          if (next !== archived) archivedCount += next ? 1 : -1;
          archived = next;
          await tx.put(`${ARCHIVED_PREFIX}${id}`, archived);
        }
        items.push({ id, read, archived });
      }
      if (action === "archive" || action === "unarchive") await tx.put(ARCHIVED_COUNT, Math.max(0, archivedCount));
      return { items };
    });
  }
  async readArchiveMessage({ id, attachment, raw = false } = {}) {
    if (!ID_PATTERN.test(String(id || ""))) throw new ArchiveInputError();
    const record = await this.state.storage.get(`${RECORD_PREFIX}${id}`);
    if (!record) return null;
    const payload = await this.loadPayload(record);
    if (raw) return { content_base64: payload.raw_base64, filename: `${id}.eml` };
    const parsed = await PostalMime.parse(fromBase64(payload.raw_base64));
    const attachments = parsed.attachments || [];
    if (attachment !== void 0) {
      if (!Number.isSafeInteger(attachment) || attachment < 0 || attachment >= attachments.length) throw new ArchiveInputError();
      const file = attachments[attachment];
      return {
        filename: file.filename || `attachment-${attachment + 1}`,
        content_base64: toBase64(new Uint8Array(file.content))
      };
    }
    return {
      id,
      read: await this.state.storage.get(`${READ_PREFIX}${id}`) === true,
      archived: await this.state.storage.get(`${ARCHIVED_PREFIX}${id}`) === true,
      received_at: record.received_at,
      date: parsed.date || record.received_at,
      subject: parsed.subject || "(No subject)",
      from: parsed.from,
      to: parsed.to || [],
      cc: parsed.cc || [],
      reply_to: parsed.replyTo || [],
      mailbox: String(payload.envelope_to || ""),
      text: parsed.text || plainTextFromHtml(parsed.html) || "(This message has no text body.)",
      message_id: parsed.messageId || "",
      in_reply_to: parsed.inReplyTo || "",
      references: parsed.references || "",
      attachments: attachments.map((file, index) => ({
        index,
        filename: file.filename || `attachment-${index + 1}`,
        mime_type: file.mimeType || "application/octet-stream",
        size: file.content.byteLength
      }))
    };
  }
};

// web/index.html
var web_default = '<!doctype html>\n<html lang="en">\n<head>\n  <meta charset="utf-8">\n  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">\n  <meta name="theme-color" content="#0d1411">\n  <meta name="apple-mobile-web-app-capable" content="yes">\n  <meta name="apple-mobile-web-app-title" content="C-mail">\n  <title>C-mail</title>\n  <link rel="manifest" href="/cmail/manifest.webmanifest">\n  <link rel="apple-touch-icon" href="/cmail/icon-192.png">\n  <link rel="icon" href="/cmail/icon-192.png">\n  <link rel="stylesheet" href="/cmail/style.css">\n  <script src="/cmail/app.js" defer><\/script>\n</head>\n<body>\n  <header class="topbar">\n    <a class="brand" href="/cmail/" aria-label="C-mail inbox"><span class="brandmark">C</span><span>C-mail<span class="brand-dot">.</span></span></a>\n    <select id="mailbox" class="mailbox-select" aria-label="Choose inbox"><option value="">All inboxes</option></select>\n    <select id="mailView" class="view-select" aria-label="Mail view"><option value="inbox">Inbox</option><option value="archive">Archive</option><option value="all">All mail</option></select>\n    <form id="searchForm" class="search-form"><label class="search-label" for="search">\u2315</label><input type="search" id="search" placeholder="Search all inboxes" aria-label="Search messages, senders, and subjects"><button type="submit" aria-label="Search">\u21B5</button></form>\n    <details id="dateFilters" class="date-filters"><summary id="dateSummary">Dates</summary><div class="date-panel"><label>From<input type="date" id="after"></label><label>Through<input type="date" id="before"></label><div class="date-buttons"><button id="clearDates" type="button">Clear</button><button id="applyDates" type="button">Apply</button></div></div></details>\n    <div class="bulk-menu"><button id="actionsButton" class="control" type="button" aria-expanded="false" aria-controls="actionsMenu" disabled>Actions <span aria-hidden="true">\u2304</span></button><div id="actionsMenu" class="actions-menu" hidden><button type="button" data-action="archive">Archive</button><button type="button" data-action="read">Mark read</button><button type="button" data-action="unread">Mark unread</button><button type="button" data-action="unarchive" hidden>Move to inbox</button></div></div>\n    <span class="connection" id="connection" role="status" title="Connecting"><span class="status-dot"></span><span id="connectionText" class="sr-only">Connecting</span></span>\n    <button id="refresh" class="quiet" type="button" aria-label="Refresh mail" title="Refresh mail">\u21BB</button>\n    <a id="signOut" class="sign-out" href="/cdn-cgi/access/logout">Sign out</a>\n  </header>\n  <div class="workspace">\n    <main class="inbox" aria-label="Inbox">\n      <div class="list-toolbar"><label class="select-page" title="Select this page"><input id="selectPage" type="checkbox" aria-label="Select this page"></label><span id="selectionCount" role="status" hidden></span><button id="clearSelection" class="clear-selection" type="button" hidden>Clear</button><h1 id="inboxTitle">All inboxes</h1><span id="listLabel" role="status">Latest messages</span><span class="unread-legend"><span class="unread-dot"></span> Unread</span><span id="resultCount"></span><button id="stopSearch" type="button" hidden>Stop</button></div>\n      <div id="notice" class="notice" role="status" hidden></div>\n      <div id="messages" class="messages" aria-live="polite"><div class="empty-list">Opening your inbox\u2026</div></div>\n      <nav class="pagination" aria-label="Message pages"><label class="page-size">Per page <select id="pageSize" aria-label="Messages per page"><option value="20">20</option><option value="50" selected>50</option><option value="100">100</option></select></label><span id="pageLabel" role="status">Page 1</span><button id="previousPage" type="button" disabled aria-label="Previous page">\u2190 <span>Previous</span></button><button id="nextPage" type="button" disabled aria-label="Next page"><span>Next</span> \u2192</button><button id="continueLoad" type="button" hidden>Continue</button></nav>\n    </main>\n    <section class="reader" aria-label="Message reader">\n      <div id="readerPlaceholder" class="reader-placeholder"><div class="envelope">\u2709</div><p class="eyebrow">A LITTLE SPACE TO FOCUS</p><h2>Your mail, wherever you are.</h2><p>Select a message to read it.<br>Your inbox no longer needs your home computer.</p><span class="private-badge">PRIVATE \xB7 SIGN-IN REQUIRED</span></div>\n      <article id="message" hidden>\n        <div class="reader-toolbar"><button id="back" class="quiet" type="button" aria-label="Close message">\u2190 <span>Inbox</span></button><button id="copyMessage" class="quiet" type="button" title="Copy this message, including its headers and quoted history">Copy message</button><button id="copyThread" class="quiet" type="button" title="Copy all available messages in this conversation, oldest first" disabled>Copy thread</button><button id="toggleRead" class="quiet" type="button">Mark unread</button><a id="downloadRaw" class="quiet" href="#">Download email \u2197</a></div>\n        <div class="conversation-status"><span id="threadStatus" role="status"></span><button id="retryThread" class="quiet" type="button" hidden>Retry</button></div>\n        <div id="readerNotice" class="notice" role="status" hidden></div>\n        <div class="message-content"><p class="eyebrow" id="messageMailbox"></p><h2 id="messageSubject"></h2><div class="message-sender"><span class="avatar" id="senderAvatar">M</span><div><strong id="messageFrom"></strong><span id="messageTo"></span></div><time id="messageDate"></time></div><div id="attachments" class="attachments"></div><div id="messageBody" class="message-body"></div><div id="threadMessages"></div></div>\n      </article>\n    </section>\n  </div>\n  <div id="actionStatus" class="action-status" role="status" hidden></div>\n  <dialog id="copyFallback" class="copy-dialog"><h2>Copy email text</h2><p>Automatic copying is unavailable. Select the text below, then use Copy on your device.</p><textarea id="copyText" readonly aria-label="Email text to copy"></textarea><div><button id="selectCopyText" class="control" type="button">Select text</button><button id="closeCopy" class="control" type="button">Close</button></div></dialog>\n</body>\n</html>\n';

// web/style.css
var style_default = ':root{color-scheme:dark;--bg:#0d1411;--panel:#111b16;--panel2:#15221b;--line:#27362c;--text:#ecf1e9;--muted:#93a599;--accent:#d5edb5;font-family:ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;font-size:14px}\n*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);height:100dvh;display:flex;flex-direction:column;overflow:hidden}button,input,select,a{-webkit-tap-highlight-color:transparent}button,input,select{font:inherit}button,a,summary{touch-action:manipulation}button,select,summary{cursor:pointer}button:disabled{cursor:default;opacity:.4}a{color:inherit;text-decoration:none}button:focus-visible,a:focus-visible,input:focus-visible,select:focus-visible,summary:focus-visible{outline:2px solid var(--accent);outline-offset:2px}[hidden]{display:none!important}.sr-only{position:absolute;width:1px;height:1px;margin:-1px;padding:0;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}\n/* One compact control bar; the mail list owns the rest of the screen. */\n.topbar{position:relative;z-index:3;display:flex;align-items:center;gap:10px;padding:10px 16px;border-bottom:1px solid var(--line);flex-shrink:0;background:var(--bg)}.brand{display:flex;align-items:center;gap:8px;font-size:23px;font-weight:650;letter-spacing:-1px;white-space:nowrap;margin-right:6px}.brandmark{flex-shrink:0;height:29px;width:29px;border:1px solid var(--accent);border-radius:8px;font-size:19px;font-weight:500;display:grid;place-items:center;color:var(--accent)}.brand-dot{color:var(--accent)}\nselect{appearance:none;-webkit-appearance:none;min-width:0;border:1px solid #40563d;border-radius:6px;background-color:var(--panel2);color:var(--text);padding:8px 32px 8px 11px;font-size:12px;height:36px;background-image:linear-gradient(45deg,transparent 50%,#adbdac 50%),linear-gradient(135deg,#adbdac 50%,transparent 50%);background-position:calc(100% - 17px) 15px,calc(100% - 12px) 15px;background-size:5px 5px;background-repeat:no-repeat}.mailbox-select{width:195px}.view-select{width:106px}.search-form{display:flex;align-items:center;flex:1;min-width:150px;background:#0e1712;border:1px solid var(--line);border-radius:6px;height:36px;padding:0 8px}.search-label{font-size:20px;color:var(--muted)}.search-form input{background:transparent;border:0;color:var(--text);min-width:0;width:100%;padding:7px;font-size:12px;outline-offset:0}.search-form input::placeholder{color:#7d9182}.search-form button{border:0;color:var(--muted);background:transparent;padding:6px}.control,.sign-out,.date-filters summary{border:1px solid var(--line);border-radius:6px;padding:8px 10px;background:var(--panel2);color:var(--text);font-size:12px;white-space:nowrap;min-height:36px}.sign-out{background:transparent}.control:hover:not(:disabled),.sign-out:hover,.date-filters summary:hover{border-color:#62765e}.quiet{border:0;background:transparent;color:var(--muted);padding:9px;font-size:13px}.quiet:hover{color:var(--text);background:var(--panel2);border-radius:6px}#refresh{font-size:18px;padding:5px 8px}.connection{display:flex;align-items:center}.status-dot,.unread-dot,.read-indicator{height:6px;width:6px;background:var(--accent);border-radius:50%;display:inline-block;flex-shrink:0}\n.date-filters{position:relative}.date-filters summary{list-style:none}.date-filters summary::-webkit-details-marker{display:none}.date-filters[open] summary{border-color:var(--accent)}.date-panel,.actions-menu{position:absolute;right:0;top:calc(100% + 8px);background:var(--panel2);border:1px solid #40563d;border-radius:8px;box-shadow:0 12px 32px #0008;z-index:5}.date-panel{padding:14px;width:270px;display:grid;grid-template-columns:1fr 1fr;gap:10px}.date-panel label{min-width:0;display:block;color:var(--muted);font-size:11px}.date-panel input{display:block;width:100%;background:var(--bg);border:1px solid var(--line);color:var(--text);font-size:12px;margin-top:6px;padding:7px 4px;border-radius:4px}.date-buttons{grid-column:1/-1;display:flex;justify-content:flex-end;gap:8px}.date-buttons button{border:1px solid var(--line);border-radius:4px;background:var(--bg);color:var(--accent);padding:7px 12px;font-size:12px}.bulk-menu{position:relative}.actions-menu{min-width:170px;padding:5px}.actions-menu button{display:block;text-align:left;width:100%;padding:10px 12px;border:0;background:transparent;color:var(--text);border-radius:4px;font-size:12px}.actions-menu button:hover{background:#2a3b28}\n.workspace{display:grid;grid-template-columns:minmax(0,1fr);flex:1;min-height:0}.inbox{background:var(--panel);display:flex;flex-direction:column;min-width:0;min-height:0}.reader{display:none;min-width:0;overflow:auto;background:var(--bg)}.reading .workspace{grid-template-columns:390px minmax(0,1fr)}.reading .inbox{border-right:1px solid var(--line)}.reading .reader{display:block}.action-status{position:fixed;bottom:62px;left:16px;right:auto;max-width:calc(100vw - 32px);z-index:6;background:#2a3b28;border:1px solid #5e7652;border-radius:7px;box-shadow:0 5px 20px #0006;color:var(--accent);padding:10px 14px;font-size:12px;pointer-events:none}\n.list-toolbar{display:flex;align-items:center;gap:10px;padding:4px 16px;min-height:38px;flex-shrink:0;color:var(--muted);font-size:10px}.list-toolbar h1{font-size:12px;font-weight:550;margin:0;overflow:hidden;white-space:nowrap;text-overflow:ellipsis}.select-page,.row-check{display:flex;align-items:center;justify-content:center;flex-shrink:0;cursor:pointer}.select-page{width:18px;height:28px}input[type=checkbox]{width:15px;height:15px;margin:0;accent-color:var(--accent);cursor:pointer}.unread-legend{display:flex;align-items:center;gap:6px;margin-left:auto;white-space:nowrap}#resultCount{white-space:nowrap}#selectionCount{color:var(--accent);white-space:nowrap}.clear-selection,#stopSearch{border:1px solid var(--line);border-radius:4px;background:transparent;color:var(--accent);padding:4px 7px;font-size:10px}.has-selection .list-toolbar h1,.has-selection #listLabel{display:none}.reading .unread-legend{display:none}.reading #listLabel{display:none}.reading #resultCount{margin-left:auto}.reading .list-toolbar{gap:7px}\n.messages{overflow:auto;flex:1;min-height:0}.message-row{display:flex;width:100%;border-top:1px solid #27372a;background:#101a14;position:relative;color:var(--text)}.message-row.unread{background:#1a291d}.message-row:hover{background:#233226}.message-row.selected{background:#2b3b26}.message-row.selected:before{content:"";position:absolute;left:0;top:0;bottom:0;width:3px;background:var(--accent)}.message-row.checked{background:#304129}.row-check{width:43px;padding-left:5px}.message-open{display:grid;grid-template-areas:"indicator sender subject mailbox date";grid-template-columns:6px minmax(130px,210px) minmax(150px,1fr) minmax(130px,180px) 55px;align-items:center;gap:14px;padding:10px 16px 10px 0;flex:1;min-width:0;border:0;background:transparent;text-align:left;color:inherit}.read-indicator{grid-area:indicator;opacity:0}.unread .read-indicator{opacity:1}.row-sender{grid-area:sender;font-size:12px;font-weight:450;color:#a9b9ad;line-height:18px;overflow:hidden;white-space:nowrap;text-overflow:ellipsis}.unread .row-sender{color:#f0f5ec;font-weight:700}.row-subject{grid-area:subject;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-size:12px;line-height:18px;color:#96aa9b}.unread .row-subject{color:#dce8d3;font-weight:550}.row-mailbox{grid-area:mailbox;color:#849888;font-size:10px;line-height:14px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.row-date{grid-area:date;color:var(--muted);font-size:10px;white-space:nowrap;text-align:right;line-height:18px}.reading .message-open{grid-template-areas:"indicator sender date" ". subject subject" ". mailbox mailbox";grid-template-columns:6px minmax(0,1fr) auto;gap:3px 8px;padding:9px 14px 9px 0}.reading .row-check{width:40px}.reading .row-mailbox{font-size:9px}\n.pagination{display:flex;align-items:center;gap:8px;padding:7px 16px;border-top:1px solid var(--line);flex-shrink:0;color:var(--muted);font-size:10px;flex-wrap:wrap}.page-size{display:flex;align-items:center;gap:6px;white-space:nowrap}.page-size select{font-size:11px;padding-left:8px;height:32px;width:65px;background-position:calc(100% - 16px) 13px,calc(100% - 11px) 13px}.pagination button{font-size:11px;border:1px solid var(--line);border-radius:5px;background:var(--panel2);color:var(--accent);padding:7px;min-height:32px}#pageLabel{margin-left:auto;white-space:nowrap}.reading .pagination button span{display:none}.notice{padding:9px 16px;color:#eed5a8;background:#2c291b;font-size:12px;line-height:1.5}.empty-list{padding:28px 20px;color:var(--muted);line-height:1.8;font-size:12px}\n.reader-toolbar{min-height:48px;display:flex;gap:5px;align-items:center;border-bottom:1px solid var(--line);padding:5px 16px}.reader-toolbar .quiet{font-size:11px;white-space:nowrap}#toggleRead{margin-left:auto;color:var(--accent)}.message-content{padding:32px clamp(24px,5vw,72px) 50px;max-width:960px;margin:auto}.eyebrow{font-size:9px;font-weight:650;letter-spacing:1px;color:var(--muted);margin:0 0 18px;overflow-wrap:anywhere}.message-content h2{font-size:clamp(23px,2.2vw,31px);font-weight:550;line-height:1.4;letter-spacing:-.6px;margin:0 0 25px;overflow-wrap:anywhere}.avatar{flex-shrink:0;display:grid;place-items:center;background:#26342a;color:#c5d8ba;border:1px solid #344635;border-radius:50%;height:34px;width:34px;font-size:12px}.message-sender{display:flex;align-items:center;gap:12px;padding-bottom:24px;border-bottom:1px solid var(--line)}.message-sender>div{min-width:0;flex:1}.message-sender strong{display:block;font-size:12px;overflow-wrap:anywhere;font-weight:550}.message-sender div>span{font-size:10px;line-height:1.6;display:block;color:var(--muted);margin-top:5px;overflow-wrap:anywhere}.message-sender time{font-size:10px;line-height:1.7;max-width:110px;text-align:right;color:var(--muted)}.message-body{white-space:pre-wrap;overflow-wrap:anywhere;font-size:14px;line-height:1.9;color:#d0dacf;margin-top:25px}.message-body a{color:var(--accent);text-decoration:underline;text-decoration-thickness:1px;text-underline-offset:3px}.attachments{display:flex;flex-wrap:wrap;gap:8px;margin-top:20px}.attachment{display:block;max-width:100%;border:1px solid var(--line);background:var(--panel2);border-radius:6px;padding:10px 14px;color:#d8e5cd;font-size:11px;overflow-wrap:anywhere}.attachment span{display:block;font-size:9px;margin-top:5px;color:var(--muted)}\n@media(max-width:1100px){.topbar{gap:8px;padding:9px 12px}.brand{font-size:21px;margin-right:0}.mailbox-select{width:175px}.view-select{width:96px}.connection{display:none}.reading .workspace{grid-template-columns:340px minmax(0,1fr)}.message-open{grid-template-columns:6px 155px minmax(130px,1fr) 150px 50px;gap:10px}.message-sender{flex-wrap:wrap}.message-sender time{margin-left:46px;max-width:none;text-align:left}.message-content{padding:28px 24px}}\n@media(max-width:900px){.topbar{display:grid;grid-template-columns:auto minmax(0,1fr) auto auto auto;gap:8px}.brand{grid-row:1;grid-column:1}.mailbox-select{grid-row:1;grid-column:2;width:100%}.view-select{grid-row:1;grid-column:3;width:98px}#refresh{grid-row:1;grid-column:4}#signOut{grid-row:1;grid-column:5}.search-form{grid-row:2;grid-column:1/4;min-width:0}.date-filters{grid-row:2;grid-column:4}.bulk-menu{grid-row:2;grid-column:5}.bulk-menu>.control{width:100%}.message-open{grid-template-columns:6px 150px minmax(100px,1fr) 50px;grid-template-areas:"indicator sender subject date"}.row-mailbox{display:none}.reading .row-mailbox{display:block}.reading .workspace{grid-template-columns:310px minmax(0,1fr)}.reader-toolbar{padding:5px 8px;flex-wrap:wrap}.reader-toolbar .quiet{padding:8px}.list-toolbar #listLabel{display:none}}\n@media(max-width:620px){.topbar{grid-template-columns:minmax(0,1fr) auto auto auto;padding:8px 12px;gap:7px}.brand{grid-column:1;font-size:21px}.brandmark{width:27px;height:27px}#refresh{grid-column:3}#signOut{grid-column:4;font-size:11px;min-height:34px}.bulk-menu{grid-row:1;grid-column:2}.mailbox-select{grid-row:2;grid-column:1/3;width:100%;font-size:16px}.view-select{grid-row:2;grid-column:3/5;width:100%;font-size:16px}.search-form{grid-row:3;grid-column:1/4}.search-form input{font-size:16px}.date-filters{grid-row:3;grid-column:4}.date-filters summary{text-align:center}.date-panel{width:min(270px,calc(100vw - 24px))}.actions-menu{right:-40px}.connection{display:none}.workspace,.reading .workspace{display:flex;flex-direction:column}.inbox{flex:1}.reading .inbox{display:none}.reading .reader{flex:1}.reading .mailbox-select,.reading .view-select,.reading .search-form,.reading .date-filters,.reading .bulk-menu{display:none}.reading .topbar{display:flex}.reading #refresh{margin-left:auto}.list-toolbar{padding:3px 12px;gap:8px;min-height:34px}.list-toolbar h1{font-size:11px;max-width:95px}.unread-legend{font-size:9px}.has-selection .unread-legend{display:none}#resultCount{margin-left:auto;font-size:9px}.message-open{grid-template-areas:"indicator sender date" ". subject subject" ". mailbox mailbox";grid-template-columns:6px minmax(0,1fr) auto;gap:3px 7px;padding:8px 12px 8px 0}.row-check{width:37px;padding-left:1px}.row-mailbox{display:block;font-size:9px}.row-sender,.row-subject{font-size:13px}.pagination{padding:6px 12px calc(6px + env(safe-area-inset-bottom));gap:6px}.pagination button{min-height:36px}.pagination button span{display:none}.page-size select{font-size:16px;width:76px;min-height:36px;background-position:calc(100% - 16px) 15px,calc(100% - 11px) 15px}.reader-toolbar .quiet{font-size:10px}.message-content{padding:24px 22px calc(40px + env(safe-area-inset-bottom))}.message-sender>div{flex-basis:calc(100% - 46px)}.message-sender time{flex-basis:100%;margin-top:-4px}}\n\n@media(max-width:359px){.topbar .brand{font-size:19px;gap:6px}.topbar .brandmark{width:25px;height:25px;font-size:17px}}\n/* Copy controls stay with the reader and never take space from the inbox. */\n.reader-toolbar{flex-wrap:wrap;position:sticky;top:0;z-index:2;background:var(--bg)}#copyMessage,#copyThread{color:var(--accent)}.conversation-status{display:flex;gap:8px;align-items:center;padding:8px 20px;color:var(--muted);font-size:10px;line-height:1.5;border-bottom:1px solid var(--line)}.conversation-status .quiet{font-size:11px;padding:5px 8px;color:var(--accent)}.thread-heading{font-size:13px;font-weight:600;color:var(--muted);margin:32px 0 14px}.thread-message{border:1px solid var(--line);background:var(--panel);border-radius:7px;padding:14px;margin-top:10px}.thread-message-heading{display:flex;align-items:flex-start;gap:10px}.thread-message-heading>div{min-width:0;flex:1}.thread-message-heading strong{display:block;font-size:12px;overflow-wrap:anywhere}.thread-message-heading time{display:block;color:var(--muted);font-size:10px;margin-top:5px}.thread-message-heading .quiet{flex-shrink:0;padding:6px 8px;font-size:11px;color:var(--accent)}.thread-message summary{color:var(--muted);font-size:12px;line-height:1.5;margin-top:10px;overflow-wrap:anywhere}.thread-message .message-body{font-size:13px;margin-top:14px}.copy-dialog{color:var(--text);background:var(--panel);border:1px solid #40563d;border-radius:10px;width:min(720px,calc(100vw - 24px));padding:20px;max-height:calc(100dvh - 30px)}.copy-dialog::backdrop{background:#000a}.copy-dialog h2{font-size:18px;margin:0 0 10px}.copy-dialog p{font-size:12px;line-height:1.6;color:var(--muted)}.copy-dialog textarea{display:block;width:100%;height:min(45dvh,400px);resize:vertical;background:var(--bg);color:var(--text);border:1px solid var(--line);padding:12px;font:12px/1.5 ui-monospace,monospace;white-space:pre-wrap}.copy-dialog>div{display:flex;justify-content:flex-end;gap:10px;margin-top:12px}@media(max-width:620px){.reader-toolbar{gap:2px;padding:5px 8px}.reader-toolbar .quiet{font-size:11px;min-height:32px;padding:7px}.conversation-status{padding:7px 14px}.thread-message{padding:12px}}\n';

// raw:/Users/codymitchell/Documents/code/life-ops/cloud/cmail/web/app.js
var app_default = '(() => {\n  // web/thread.mjs\n  function messageIdTokens(value) {\n    const text = Array.isArray(value) ? value.join(" ") : String(value || "");\n    const bracketed = text.match(/<[^<>\\s]+>/g);\n    return [...new Set(bracketed || text.split(/[\\s,]+/).filter((token) => token.includes("@")).map((token) => `<${token.replace(/^<|>$/g, "")}>`))];\n  }\n  function conversationNodes(nodes, selectedId2) {\n    const byToken = /* @__PURE__ */ new Map();\n    const tokensFor = (node) => [.../* @__PURE__ */ new Set([\n      ...messageIdTokens(node.message_id),\n      ...messageIdTokens(node.in_reply_to),\n      ...messageIdTokens(node.references)\n    ])];\n    for (const node of nodes) for (const token of tokensFor(node)) {\n      if (!byToken.has(token)) byToken.set(token, []);\n      byToken.get(token).push(node);\n    }\n    const seed = nodes.find((node) => node.id === selectedId2);\n    if (!seed) return { nodes: [], missing: 0 };\n    const found = /* @__PURE__ */ new Map([[seed.id, seed]]), visited = /* @__PURE__ */ new Set(), queue = [seed];\n    for (let index = 0; index < queue.length; index++) for (const token of tokensFor(queue[index])) {\n      if (visited.has(token)) continue;\n      visited.add(token);\n      for (const node of byToken.get(token) || []) if (!found.has(node.id)) {\n        found.set(node.id, node);\n        queue.push(node);\n      }\n    }\n    const unique = /* @__PURE__ */ new Map();\n    for (const node of found.values()) {\n      const key = messageIdTokens(node.message_id)[0] || node.id;\n      if (!unique.has(key) || node.id === selectedId2) unique.set(key, node);\n    }\n    const timestamp = (node) => Date.parse(node.date) || Date.parse(node.received_at) || 0;\n    const ordered = [...unique.values()].sort((a, b) => timestamp(a) - timestamp(b) || a.id.localeCompare(b.id));\n    const available = new Set([...found.values()].flatMap((node) => messageIdTokens(node.message_id)));\n    return { nodes: ordered, missing: [...visited].filter((token) => !available.has(token)).length };\n  }\n  function address(value) {\n    if (value?.group) return value.group.map(address).filter(Boolean).join(", ");\n    return value?.name ? `${value.name} <${value.address || ""}>` : value?.address || "";\n  }\n  function formatMessage(message) {\n    return [\n      `Subject: ${message.subject || "(No subject)"}`,\n      `From: ${address(message.from)}`,\n      `To: ${(message.to || []).map(address).join(", ")}`,\n      ...message.cc?.length ? [`Cc: ${message.cc.map(address).join(", ")}`] : [],\n      `Date: ${message.date || message.received_at || ""}`,\n      "",\n      message.text || ""\n    ].join("\\n");\n  }\n  function formatThread(messages) {\n    return messages.map((message, index) => `----- Message ${index + 1} of ${messages.length} -----\n${formatMessage(message)}`).join("\\n\\n");\n  }\n\n  // web/app.js\n  var $ = (id) => document.getElementById(id);\n  var cursor = null;\n  var selectedId = null;\n  var requestGeneration = 0;\n  var readingGeneration = 0;\n  var loading = false;\n  var items = [];\n  var pageIndex = 0;\n  var pageSize = 50;\n  try {\n    const saved = Number(localStorage.getItem("cmail.page-size"));\n    if ([20, 50, 100].includes(saved)) pageSize = saved;\n  } catch {\n  }\n  var exhausted = false;\n  var interrupted = false;\n  var activeFilters = null;\n  var totalStored = 0;\n  var totalView = null;\n  var bulkBusy = false;\n  var actionStatusTimer;\n  var checkedIds = /* @__PURE__ */ new Set();\n  var selectedMessage = null;\n  var renderedIds = "";\n  var pendingReadIds = /* @__PURE__ */ new Set();\n  var readStateOverrides = /* @__PURE__ */ new Map();\n  var scanned = 0;\n  var failed = 0;\n  var loadController = null;\n  var signingOut = false;\n  var knownMailboxes = /* @__PURE__ */ new Set();\n  var conversationGeneration = 0;\n  var conversationController = null;\n  var conversationMessages = [];\n  var threadIndexCache = null;\n  function resetConversation() {\n    conversationGeneration += 1;\n    conversationController?.abort();\n    conversationController = null;\n    conversationMessages = [];\n    $("threadMessages").replaceChildren();\n    $("copyThread").disabled = true;\n    $("threadStatus").textContent = "";\n    $("retryThread").hidden = true;\n    $("copyFallback").close();\n    $("copyText").value = "";\n  }\n  function addMailboxes(addresses) {\n    for (const value of addresses) {\n      const address2 = String(value || "").trim().toLowerCase();\n      if (!address2 || knownMailboxes.has(address2)) continue;\n      knownMailboxes.add(address2);\n      const option = document.createElement("option");\n      option.value = address2;\n      option.textContent = address2;\n      $("mailbox").append(option);\n    }\n    updateInboxLabel();\n  }\n  function updateInboxLabel() {\n    const mailbox = $("mailbox").value;\n    const view = $("mailView").value;\n    $("inboxTitle").textContent = view === "archive" ? "Archive" : view === "all" ? "All mail" : mailbox ? "Inbox" : "All inboxes";\n    $("search").placeholder = view === "archive" ? "Search archive" : view === "all" ? "Search all mail" : mailbox ? "Search this inbox" : "Search all inboxes";\n    $("inboxTitle").title = mailbox || "All connected addresses";\n  }\n  function closeMessage() {\n    resetConversation();\n    readingGeneration += 1;\n    selectedId = null;\n    selectedMessage = null;\n    document.body.classList.remove("reading");\n    window.history.replaceState(null, "", window.location.pathname + window.location.search);\n    $("message").hidden = true;\n    $("readerPlaceholder").hidden = false;\n    document.querySelectorAll(".message-row").forEach((row) => row.classList.remove("selected"));\n  }\n  function switchInbox() {\n    if (bulkBusy) return;\n    const url = new URL(window.location.href);\n    if ($("mailbox").value) url.searchParams.set("mailbox", $("mailbox").value);\n    else url.searchParams.delete("mailbox");\n    if ($("mailView").value !== "inbox") url.searchParams.set("view", $("mailView").value);\n    else url.searchParams.delete("view");\n    url.hash = "";\n    window.history.replaceState(null, "", url);\n    closeMessage();\n    updateInboxLabel();\n    load(true);\n  }\n  function notice(message) {\n    $("notice").textContent = message;\n    $("notice").hidden = !message;\n  }\n  function actionStatus(message) {\n    clearTimeout(actionStatusTimer);\n    $("actionStatus").textContent = message;\n    $("actionStatus").hidden = !message;\n    if (message) actionStatusTimer = setTimeout(() => {\n      $("actionStatus").hidden = true;\n    }, 5e3);\n  }\n  function connected(value) {\n    $("connectionText").textContent = value;\n    $("connection").title = value;\n  }\n  async function api(path, signal, options = {}) {\n    const result = await fetch(`/cmail/api/${path}`, { ...options, cache: "no-store", redirect: "manual", signal });\n    if (result.type === "opaqueredirect" || [401, 403].includes(result.status)) {\n      throw new Error("Your session has expired. Reload this page to sign in again.");\n    }\n    if (!result.ok || !result.headers.get("content-type")?.includes("application/json")) {\n      throw new Error("Your mail is temporarily unavailable. Please try Refresh.");\n    }\n    return result.json();\n  }\n  function dateLabel(value, full = false) {\n    const date = new Date(value);\n    if (Number.isNaN(date.getTime())) return value || "";\n    return new Intl.DateTimeFormat(void 0, full ? { dateStyle: "medium", timeStyle: "short" } : { month: "short", day: "numeric" }).format(date);\n  }\n  function senderLabel(value) {\n    return String(value).replace(/<[^>]*>/g, "").trim().replace(/^"|"$/g, "").trim() || value;\n  }\n  function mailboxAddress(value) {\n    if (!value) return "";\n    if (value.group) return value.group.map(mailboxAddress).join(", ");\n    return value.name ? `${value.name} <${value.address}>` : value.address || "";\n  }\n  function addText(parent, tag, className, value) {\n    const element = document.createElement(tag);\n    element.className = className;\n    element.textContent = value;\n    parent.append(element);\n    return element;\n  }\n  function applyRowState(row, item) {\n    row.classList.toggle("unread", !item.read);\n    row.classList.toggle("selected", item.id === selectedId);\n    row.dataset.read = String(Boolean(item.read));\n    row.classList.toggle("checked", checkedIds.has(item.id));\n    const checkbox = row.querySelector("input");\n    checkbox.checked = checkedIds.has(item.id);\n    checkbox.disabled = bulkBusy;\n    row.querySelector(".message-open").setAttribute("aria-label", `${item.read ? "Read" : "Unread"}: ${senderLabel(item.from)} \\u2014 ${item.subject}`);\n  }\n  function appendRow(item) {\n    const row = document.createElement("div");\n    row.className = "message-row";\n    row.dataset.id = item.id;\n    const label = addText(row, "label", "row-check", "");\n    const checkbox = document.createElement("input");\n    checkbox.type = "checkbox";\n    checkbox.setAttribute("aria-label", `Select: ${senderLabel(item.from)} \\u2014 ${item.subject}`);\n    label.append(checkbox);\n    checkbox.addEventListener("change", () => {\n      if (checkbox.checked) checkedIds.add(item.id);\n      else checkedIds.delete(item.id);\n      renderList();\n    });\n    const open = addText(row, "button", "message-open", "");\n    open.type = "button";\n    const indicator = addText(open, "span", "read-indicator", "");\n    indicator.setAttribute("aria-hidden", "true");\n    addText(open, "span", "row-sender", senderLabel(item.from));\n    const subject = addText(open, "span", "row-subject", item.subject);\n    subject.title = item.subject;\n    addText(open, "span", "row-mailbox", item.mailbox);\n    addText(open, "time", "row-date", dateLabel(item.received_at));\n    applyRowState(row, item);\n    open.addEventListener("click", () => openMessage(item.id));\n    $("messages").append(row);\n  }\n  function closeActions() {\n    $("actionsMenu").hidden = true;\n    $("actionsButton").setAttribute("aria-expanded", "false");\n  }\n  function updateSelection() {\n    const visible = items.slice(pageIndex * pageSize, (pageIndex + 1) * pageSize);\n    const count = checkedIds.size;\n    $("selectPage").checked = visible.length > 0 && visible.every((item) => checkedIds.has(item.id));\n    $("selectPage").indeterminate = count > 0 && !$("selectPage").checked;\n    $("selectPage").disabled = !visible.length || bulkBusy;\n    $("selectionCount").textContent = `${count} selected`;\n    $("selectionCount").hidden = !count;\n    $("clearSelection").hidden = !count;\n    $("clearSelection").disabled = bulkBusy;\n    document.body.classList.toggle("has-selection", count > 0);\n    $("actionsButton").disabled = !count || loading || bulkBusy || [...checkedIds].some((id) => pendingReadIds.has(id));\n    $("actionsButton").textContent = bulkBusy ? "Saving\\u2026" : "Actions \\u2304";\n    $("actionsButton").title = loading ? "Wait for loading to finish, or choose Stop." : "Actions for checked messages";\n    document.querySelector(\'[data-action="archive"]\').hidden = $("mailView").value === "archive";\n    document.querySelector(\'[data-action="unarchive"]\').hidden = $("mailView").value === "inbox";\n    for (const id of ["mailbox", "mailView", "pageSize", "search", "after", "before", "applyDates", "clearDates"]) $(id).disabled = bulkBusy;\n    $("refresh").disabled = bulkBusy || loading;\n  }\n  function clearSelection() {\n    checkedIds.clear();\n    closeActions();\n    renderList();\n  }\n  function isSearching() {\n    return Boolean(activeFilters?.query || activeFilters?.after || activeFilters?.before);\n  }\n  function renderList() {\n    const start = pageIndex * pageSize;\n    const visible = items.slice(start, start + pageSize);\n    const ids = visible.map((item) => item.id).join(",");\n    if (ids !== renderedIds || !visible.length) {\n      $("messages").replaceChildren();\n      renderedIds = ids;\n      visible.forEach(appendRow);\n      if (!visible.length) {\n        const text = loading ? isSearching() ? "Searching your mail\\u2026 Results will appear as they are found." : "Loading messages for this inbox\\u2026" : interrupted ? "Loading has not finished. Choose Continue to keep looking." : failed ? "Some messages could not be loaded. Results may be incomplete." : isSearching() ? "No messages match these filters." : $("mailView").value === "archive" ? "No archived messages in this view." : "No messages in this inbox.";\n        addText($("messages"), "div", "empty-list", text);\n      }\n    } else {\n      document.querySelectorAll(".message-row").forEach((row, index) => applyRowState(row, visible[index]));\n    }\n    const knownTotal = exhausted ? items.length : !isSearching() && !activeFilters?.mailbox && !failed ? totalView : null;\n    $("resultCount").textContent = visible.length ? `${(start + 1).toLocaleString()}\\u2013${(start + visible.length).toLocaleString()}${knownTotal === null ? " shown" : ` of ${knownTotal.toLocaleString()}`}` : "0 shown";\n    const pageCount = knownTotal === null ? null : Math.max(1, Math.ceil(knownTotal / pageSize));\n    $("pageLabel").textContent = `Page ${pageIndex + 1}${pageCount === null ? "" : ` of ${pageCount}`}`;\n    $("previousPage").disabled = pageIndex === 0 || bulkBusy;\n    const cachedNext = items.length > start + pageSize;\n    $("nextPage").disabled = bulkBusy || !cachedNext && (exhausted || loading || interrupted);\n    $("continueLoad").hidden = !interrupted || loading;\n    $("stopSearch").hidden = !loading;\n    updateSelection();\n  }\n  async function load(reset = false) {\n    if (signingOut || bulkBusy || loading && !reset) return;\n    if (reset) {\n      requestGeneration += 1;\n      loadController?.abort();\n      cursor = null;\n      items = [];\n      pageIndex = 0;\n      scanned = 0;\n      failed = 0;\n      exhausted = false;\n      interrupted = false;\n      totalView = null;\n      checkedIds.clear();\n      closeActions();\n      actionStatus("");\n      readStateOverrides.clear();\n      activeFilters = { query: $("search").value.trim(), mailbox: $("mailbox").value, view: $("mailView").value, after: $("after").value, before: $("before").value };\n      renderedIds = "";\n      $("messages").replaceChildren();\n      $("messages").scrollTop = 0;\n      notice("");\n    }\n    if (exhausted) {\n      renderList();\n      return;\n    }\n    const generation = requestGeneration;\n    loadController = new AbortController();\n    const { signal } = loadController;\n    const searching = isSearching();\n    const parameters = new URLSearchParams(activeFilters);\n    loading = true;\n    interrupted = false;\n    $("refresh").disabled = true;\n    connected(searching ? "Searching mail" : "Loading mail");\n    $("listLabel").textContent = searching ? "Searching your mail\\u2026" : "Loading messages\\u2026";\n    renderList();\n    try {\n      do {\n        if (cursor) parameters.set("cursor", cursor);\n        else parameters.delete("cursor");\n        const target = (pageIndex + 1) * pageSize + 1;\n        parameters.set("limit", String(searching ? pageSize : Math.min(100, Math.max(1, target - items.length))));\n        const result = await api(`messages?${parameters}`, signal);\n        if (generation !== requestGeneration || signingOut) return;\n        addMailboxes(result.items.map((item) => item.mailbox));\n        for (const item of result.items) if (readStateOverrides.has(item.id)) item.read = readStateOverrides.get(item.id);\n        items.push(...result.items);\n        cursor = result.next_cursor;\n        exhausted = !cursor;\n        scanned += result.scanned;\n        failed += result.failed;\n        totalStored = Number(result.total_stored);\n        totalView = Number(result.total_view);\n        $("listLabel").textContent = searching ? `Searching ${scanned.toLocaleString()} / ${totalStored.toLocaleString()}` : "Latest messages";\n        if (failed) notice("Some messages could not be loaded. Results may be incomplete.");\n        renderList();\n      } while (cursor && (searching || items.length < (pageIndex + 1) * pageSize + 1));\n      if (exhausted) pageIndex = Math.min(pageIndex, Math.max(0, Math.ceil(items.length / pageSize) - 1));\n      $("listLabel").textContent = searching ? failed ? "Search finished with errors" : "Search complete" : "Latest messages";\n      connected("Cloud inbox");\n    } catch (error) {\n      if (generation !== requestGeneration || signingOut) return;\n      interrupted = true;\n      if (error.name === "AbortError") {\n        $("listLabel").textContent = searching ? "Search paused" : "Loading paused";\n        connected("Paused");\n      } else {\n        notice(error.message);\n        connected("Connection interrupted");\n        $("listLabel").textContent = "Loading interrupted";\n      }\n    } finally {\n      if (generation === requestGeneration && !signingOut) {\n        loading = false;\n        $("refresh").disabled = false;\n        renderList();\n      }\n    }\n  }\n  function changePage(delta) {\n    if (bulkBusy) return;\n    checkedIds.clear();\n    closeActions();\n    pageIndex = Math.max(0, pageIndex + delta);\n    closeMessage();\n    $("messages").scrollTop = 0;\n    renderList();\n    if (!exhausted && items.length < (pageIndex + 1) * pageSize + 1) load();\n  }\n  function updateReadControl() {\n    if (!selectedMessage) return;\n    $("toggleRead").disabled = bulkBusy || pendingReadIds.has(selectedMessage.id);\n    $("toggleRead").textContent = pendingReadIds.has(selectedMessage.id) ? "Saving\\u2026" : selectedMessage.read ? "Mark unread" : "Mark read";\n  }\n  async function setReadState(id, read) {\n    if (pendingReadIds.has(id) || signingOut || bulkBusy) return;\n    pendingReadIds.add(id);\n    updateReadControl();\n    updateSelection();\n    if (selectedId === id) {\n      $("readerNotice").hidden = true;\n      $("readerNotice").textContent = "";\n    }\n    try {\n      const result = await api(`messages/${id}/read-status`, void 0, {\n        method: "POST",\n        headers: { "Content-Type": "application/json" },\n        body: JSON.stringify({ read })\n      });\n      if (signingOut) return;\n      readStateOverrides.set(id, result.read);\n      const item = items.find((item2) => item2.id === id);\n      if (item) item.read = result.read;\n      if (selectedMessage?.id === id) selectedMessage.read = result.read;\n      renderList();\n    } catch {\n      if (!signingOut && selectedId === id) {\n        $("readerNotice").textContent = "Read status could not be saved. Use the read/unread button to try again.";\n        $("readerNotice").hidden = false;\n      }\n    } finally {\n      pendingReadIds.delete(id);\n      if (!signingOut) {\n        updateReadControl();\n        updateSelection();\n      }\n    }\n  }\n  async function applyBulkAction(action) {\n    if (!checkedIds.size || $("actionsButton").disabled || signingOut) return;\n    const ids = [...checkedIds];\n    bulkBusy = true;\n    readingGeneration += 1;\n    closeActions();\n    renderList();\n    updateReadControl();\n    notice("");\n    let succeeded = false;\n    try {\n      const result = await api("messages/actions", void 0, {\n        method: "POST",\n        headers: { "Content-Type": "application/json" },\n        body: JSON.stringify({ ids, action })\n      });\n      if (signingOut) return;\n      for (const state of result.items) {\n        const item = items.find((item2) => item2.id === state.id);\n        if (item) {\n          const archiveDelta = Number(state.archived) - Number(Boolean(item.archived));\n          if (totalView !== null && activeFilters.view !== "all") totalView += activeFilters.view === "archive" ? archiveDelta : -archiveDelta;\n          Object.assign(item, state);\n        }\n        readStateOverrides.set(state.id, state.read);\n        if (selectedMessage?.id === state.id) Object.assign(selectedMessage, state);\n      }\n      if (action === "archive" || action === "unarchive") {\n        items = items.filter((item) => activeFilters.view === "all" || item.archived === (activeFilters.view === "archive"));\n        if (ids.includes(selectedId)) closeMessage();\n        pageIndex = Math.min(pageIndex, Math.max(0, Math.ceil(items.length / pageSize) - 1));\n      }\n      checkedIds.clear();\n      succeeded = true;\n    } catch (error) {\n      const detail = error.message.startsWith("Your ") ? error.message : "Check your connection and try again.";\n      if (!signingOut) notice(`Could not confirm the action. Your selection is kept so you can retry. ${detail}`);\n    } finally {\n      bulkBusy = false;\n      if (!signingOut) {\n        renderList();\n        updateReadControl();\n        if (succeeded) {\n          const label = { read: "Marked read", unread: "Marked unread", archive: "Archived", unarchive: "Moved to inbox" }[action];\n          actionStatus(`${label}: ${ids.length} ${ids.length === 1 ? "message" : "messages"}.`);\n          if (!exhausted && !interrupted && items.length < (pageIndex + 1) * pageSize + 1) load();\n        }\n      }\n    }\n  }\n  function renderBody(text, target = $("messageBody")) {\n    target.replaceChildren();\n    const matches = String(text).matchAll(/https?:\\/\\/[^\\s<>"\\u0000-\\u001f]+/g);\n    let offset = 0;\n    for (const match of matches) {\n      target.append(document.createTextNode(text.slice(offset, match.index)));\n      const link = document.createElement("a");\n      link.href = match[0];\n      link.textContent = match[0];\n      link.target = "_blank";\n      link.rel = "noopener noreferrer";\n      target.append(link);\n      offset = match.index + match[0].length;\n    }\n    target.append(document.createTextNode(text.slice(offset)));\n  }\n  async function copyText(text, button, label) {\n    const id = selectedId;\n    const original = button.textContent;\n    button.disabled = true;\n    button.textContent = "Copying\\u2026";\n    try {\n      await navigator.clipboard.writeText(text);\n      if (!signingOut && selectedId === id) actionStatus(`${label} copied.`);\n    } catch {\n      if (!signingOut && selectedId === id) {\n        $("copyText").value = text;\n        $("copyFallback").showModal();\n        $("copyText").focus();\n        $("copyText").select();\n      }\n    } finally {\n      button.textContent = original;\n      button.disabled = button.id === "copyThread" ? !conversationMessages.length : false;\n    }\n  }\n  function renderConversation() {\n    $("threadMessages").replaceChildren();\n    const others = conversationMessages.filter((message) => message.id !== selectedId);\n    if (!others.length) return;\n    addText($("threadMessages"), "h3", "thread-heading", "Other messages in this conversation");\n    for (const message of others) {\n      const card = addText($("threadMessages"), "article", "thread-message", "");\n      const heading = addText(card, "div", "thread-message-heading", "");\n      const sender = addText(heading, "div", "", "");\n      addText(sender, "strong", "", mailboxAddress(message.from));\n      addText(sender, "time", "", dateLabel(message.date, true));\n      const copy = addText(heading, "button", "quiet copy-message", "Copy message");\n      copy.type = "button";\n      copy.setAttribute("aria-label", `Copy message from ${mailboxAddress(message.from)}`);\n      copy.addEventListener("click", () => copyText(formatMessage(message), copy, "Message"));\n      const detail = addText(card, "details", "", "");\n      addText(detail, "summary", "", message.subject);\n      const body = addText(detail, "div", "message-body", "");\n      renderBody(message.text, body);\n    }\n  }\n  async function loadConversation(message) {\n    resetConversation();\n    const generation = conversationGeneration;\n    conversationController = new AbortController();\n    const { signal } = conversationController;\n    const current = () => !signingOut && selectedId === message.id && generation === conversationGeneration;\n    $("threadStatus").textContent = "Finding conversation\\u2026";\n    try {\n      let index = threadIndexCache;\n      if (!index || index.total !== totalStored) {\n        const nodes2 = [];\n        let next = null;\n        let scanned2 = 0;\n        let failed2 = 0;\n        let total = totalStored;\n        do {\n          const page = await api(`thread-index${next ? `?cursor=${encodeURIComponent(next)}` : ""}`, signal);\n          if (!current()) return;\n          nodes2.push(...page.items);\n          scanned2 += page.scanned;\n          failed2 += page.failed;\n          total = Number(page.total_stored);\n          next = page.next_cursor;\n          $("threadStatus").textContent = `Finding conversation\\u2026 ${scanned2.toLocaleString()} / ${total.toLocaleString()}`;\n        } while (next);\n        if (failed2) throw new Error("Some mail could not be checked. Copy message is available; retry to prepare the complete conversation.");\n        index = { nodes: nodes2, total };\n        threadIndexCache = index;\n      }\n      const nodes = index.nodes.filter((node) => node.id !== message.id);\n      nodes.push(message);\n      const conversation = conversationNodes(nodes, message.id);\n      const messages = /* @__PURE__ */ new Map([[message.id, message]]);\n      const needed = conversation.nodes.filter((node) => node.id !== message.id);\n      for (let offset = 0; offset < needed.length; offset += 3) {\n        const group = await Promise.all(needed.slice(offset, offset + 3).map((node) => api(`messages/${node.id}`, signal)));\n        if (!current()) return;\n        for (const item of group) messages.set(item.id, item);\n        $("threadStatus").textContent = `Loading conversation\\u2026 ${messages.size} / ${conversation.nodes.length}`;\n      }\n      if (!current()) return;\n      conversationMessages = conversation.nodes.map((node) => messages.get(node.id));\n      renderConversation();\n      $("copyThread").disabled = false;\n      const count = conversationMessages.length;\n      $("threadStatus").textContent = `${count} ${count === 1 ? "message" : "messages"} available in C-mail${count > 1 ? " \\xB7 Copied oldest first" : ""}.${conversation.missing ? " Some referenced messages are not in C-mail; quoted history is kept." : ""}`;\n    } catch (error) {\n      if (!current() || error.name === "AbortError") return;\n      $("threadStatus").textContent = "The complete conversation could not be loaded. You can still copy this message.";\n      $("retryThread").hidden = false;\n      $("copyThread").disabled = true;\n    }\n  }\n  async function openMessage(id) {\n    if (signingOut || bulkBusy) return;\n    const generation = ++readingGeneration;\n    notice("");\n    connected("Opening message");\n    try {\n      const message = await api(`messages/${encodeURIComponent(id)}`);\n      if (generation !== readingGeneration) return;\n      selectedId = id;\n      selectedMessage = message;\n      $("readerNotice").hidden = true;\n      $("readerNotice").textContent = "";\n      window.history.replaceState(null, "", `#${id}`);\n      document.querySelectorAll(".message-row").forEach((row) => row.classList.toggle("selected", row.dataset.id === id));\n      $("messageSubject").textContent = message.subject;\n      $("messageMailbox").textContent = message.mailbox;\n      $("messageFrom").textContent = mailboxAddress(message.from);\n      $("senderAvatar").textContent = (message.from?.name || message.from?.address || "M").slice(0, 1).toUpperCase();\n      $("messageTo").textContent = `To: ${message.to.map(mailboxAddress).join(", ")}${message.cc.length ? ` \\xB7 Cc: ${message.cc.map(mailboxAddress).join(", ")}` : ""}`;\n      $("messageDate").textContent = dateLabel(message.date, true);\n      $("downloadRaw").href = `/cmail/api/messages/${id}/raw`;\n      $("attachments").replaceChildren();\n      message.attachments.forEach((file) => {\n        const link = document.createElement("a");\n        link.className = "attachment";\n        link.href = `/cmail/api/messages/${id}/attachments/${file.index}`;\n        link.textContent = `\\u2193 ${file.filename}`;\n        addText(link, "span", "", `${Math.max(1, Math.round(file.size / 1024))} KB`);\n        $("attachments").append(link);\n      });\n      renderBody(message.text);\n      $("readerPlaceholder").hidden = true;\n      $("message").hidden = false;\n      document.body.classList.add("reading");\n      document.querySelector(".reader").scrollTop = 0;\n      connected("Cloud inbox");\n      const item = items.find((item2) => item2.id === id);\n      if (item) item.read = message.read;\n      renderList();\n      updateReadControl();\n      loadConversation(message);\n      if (!message.read) await setReadState(id, true);\n    } catch (error) {\n      if (generation === readingGeneration) {\n        notice(error.message);\n        connected("Connection interrupted");\n      }\n    }\n  }\n  $("searchForm").addEventListener("submit", (event) => {\n    event.preventDefault();\n    load(true);\n  });\n  $("refresh").addEventListener("click", () => {\n    threadIndexCache = null;\n    load(true);\n    if (selectedMessage) loadConversation(selectedMessage);\n  });\n  $("mailbox").addEventListener("change", switchInbox);\n  $("mailView").addEventListener("change", switchInbox);\n  function applyDates() {\n    if (bulkBusy) return;\n    $("dateFilters").open = false;\n    $("dateSummary").textContent = $("after").value || $("before").value ? "Dates \\u2022" : "Dates";\n    closeMessage();\n    load(true);\n  }\n  $("applyDates").addEventListener("click", applyDates);\n  $("clearDates").addEventListener("click", () => {\n    $("after").value = "";\n    $("before").value = "";\n    applyDates();\n  });\n  $("selectPage").addEventListener("change", () => {\n    const checked = $("selectPage").checked;\n    for (const item of items.slice(pageIndex * pageSize, (pageIndex + 1) * pageSize)) if (checked) checkedIds.add(item.id);\n    else checkedIds.delete(item.id);\n    renderList();\n  });\n  $("clearSelection").addEventListener("click", clearSelection);\n  $("actionsButton").addEventListener("click", () => {\n    const open = $("actionsMenu").hidden;\n    $("actionsMenu").hidden = !open;\n    $("actionsButton").setAttribute("aria-expanded", String(open));\n    if (open) $("dateFilters").open = false;\n  });\n  document.querySelectorAll("[data-action]").forEach((button) => button.addEventListener("click", () => applyBulkAction(button.dataset.action)));\n  document.addEventListener("click", (event) => {\n    if (!event.target.closest(".bulk-menu")) closeActions();\n    if (!event.target.closest(".date-filters")) $("dateFilters").open = false;\n  });\n  document.addEventListener("keydown", (event) => {\n    if (event.key === "Escape") {\n      closeActions();\n      $("dateFilters").open = false;\n    }\n  });\n  $("previousPage").addEventListener("click", () => changePage(-1));\n  $("nextPage").addEventListener("click", () => changePage(1));\n  $("continueLoad").addEventListener("click", () => load());\n  $("pageSize").value = String(pageSize);\n  $("pageSize").addEventListener("change", () => {\n    if (bulkBusy) return;\n    checkedIds.clear();\n    closeActions();\n    pageSize = Number($("pageSize").value);\n    pageIndex = 0;\n    try {\n      localStorage.setItem("cmail.page-size", String(pageSize));\n    } catch {\n    }\n    closeMessage();\n    $("messages").scrollTop = 0;\n    renderList();\n    if (!exhausted && items.length < pageSize + 1) load();\n  });\n  $("toggleRead").addEventListener("click", () => {\n    if (selectedMessage) setReadState(selectedMessage.id, !selectedMessage.read);\n  });\n  $("copyMessage").addEventListener("click", () => {\n    if (selectedMessage) copyText(formatMessage(selectedMessage), $("copyMessage"), "Message");\n  });\n  $("copyThread").addEventListener("click", () => {\n    if (conversationMessages.length) copyText(formatThread(conversationMessages), $("copyThread"), "Thread");\n  });\n  $("retryThread").addEventListener("click", () => {\n    threadIndexCache = null;\n    if (selectedMessage) loadConversation(selectedMessage);\n  });\n  $("closeCopy").addEventListener("click", () => $("copyFallback").close());\n  $("selectCopyText").addEventListener("click", () => {\n    $("copyText").focus();\n    $("copyText").select();\n  });\n  $("stopSearch").addEventListener("click", () => loadController?.abort());\n  $("signOut").addEventListener("click", (event) => {\n    if (event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;\n    signingOut = true;\n    requestGeneration += 1;\n    readingGeneration += 1;\n    loadController?.abort();\n    clearTimeout(actionStatusTimer);\n    conversationController?.abort();\n    conversationGeneration += 1;\n    conversationMessages = [];\n    threadIndexCache = null;\n    document.body.replaceChildren();\n    document.title = "Signing out \\xB7 C-mail";\n  });\n  window.addEventListener("pageshow", (event) => {\n    if (event.persisted) window.location.reload();\n  });\n  $("back").addEventListener("click", closeMessage);\n  api("session").then((session) => {\n    if (signingOut) return;\n    $("signOut").title = `Sign out ${session.email}`;\n    addMailboxes(session.mailboxes || []);\n  }).catch((error) => {\n    if (!signingOut) notice(error.message);\n  });\n  function openLinkedMessage() {\n    if (signingOut) return;\n    const linked = window.location.hash.match(/^#(mail_\\d{16})$/);\n    if (linked) return openMessage(linked[1]);\n  }\n  window.addEventListener("hashchange", openLinkedMessage);\n  var initialMailbox = new URLSearchParams(window.location.search).get("mailbox") || "";\n  var initialView = new URLSearchParams(window.location.search).get("view");\n  if (["inbox", "archive", "all"].includes(initialView)) $("mailView").value = initialView;\n  if (initialMailbox) {\n    addMailboxes([initialMailbox]);\n    $("mailbox").value = initialMailbox.trim().toLowerCase();\n  }\n  updateInboxLabel();\n  load(true).then(openLinkedMessage);\n})();\n';

// web/icon-192.png
var icon_192_default = "iVBORw0KGgoAAAANSUhEUgAAAMAAAADACAYAAABS3GwHAABm4klEQVR42u29d5wb1dUG/Jx7ZySttMVb3Lux6ZhiesehhxJKaCkkIYF0kjchCamkkoS8aaS+CZAQCJAQaiD03hzAgIMx2Li3Xa+3aqWVNHPv+f6QtDuSZkYzWq1Nvu9bfott1SnPOec5nRD+h2p8PQGQABiA5Xg+Go/H27RBRwnwfCY6CoACsIiAKQCYKr6T6nd0Yc+W6ndByn845Odw0Ae5jteDQz3JDBADnQBeBiCJ+WkNelvY/HQ6ne4FkHW83ixcHuX4MK7nEY7XvfN7vSj85ooPNjU1tVukjyfg3QAvIqYpEGgDHFBn9vgyGl/gU7DHaJzlLuxd3uHCEEIQGAARjT7KDGj0MnEnQC8zcJ/J4pFkMtnjeFsEgC78jpsQ0DiCvwh8CwAmT0ZiaDhxGjOfw8DxQojW/LXg4iePnCiNvp92CPBrAP2OBnyYO75DhYEDPcEANFcqRlDx5mvdR8AjRPSPxobUP7u6kHJYhaIg1F0IaJwERQCwASDS1LSbQeoygN5NRLuOgL5o5mgE6OT+ReMEfAr2eN1BT/U04DUIA4/T93LwJ3j0DwZDF+kxERUMBK8E+D6b5e9zyeRbhbcZNVgDHuvtCHvPpQP4uxrgz4D4w0SUcIB+VLtX1e9UX+BTsOeolq+iel7GAPePxygQPE5CGFAQXASTHVpeEhGYOQWmG2zQtblkcqVDEFS9jorqrfVjrbGZwjauAPFHHMC3HZQogEtbZ60f4CxDaXrCTvYAfO5rCJnhHS4IXJ0oYYQOawCGQxCu14Z9TaYvs7Ge1oDqAP4RrZ9oTrwPwNUgmukAviz5PHrnAD+QI/uOAXuN9zmgjDCPAzWrxRpUPqWKggDmjQCuTA2mbq7RGnCN7p/njwkgl9f64ockxEWewK8F/OMAfKKwoH+nAz7EPQ8gDDtOEKpSIk9BYK3/qg39lYI1iJSF1UMdTa3RvOLrdKIlcRxrXEeC5jKzqoze1Eh5aHyB743zHQR4GsdYfUiB8MIn11MQxm4NRqJJRCRZ81oSuCQ1kHp8hFoHPyp20hc4ozBhwB9rSvwPAX8FobWg9Y1qWr/ySRfpqCXbQN7AJ6+vKP6FqMYv9jgOcnhFVIdfuPy7HgdZ+kclRSSfrwt7LBSMY1J1TZmPMBLaAVwsI5GknbOeqwHDNJbTUA3N8b8IEu8vaH1ySOGOozzVXAaq9jiNXYPTO4ThcJ0+iL3pSIhIZ30pkfvnawBMRFKzvml4MP0BR6UB10F3BgJ/rsDBsEPBX60Sgvweq5OWH8caEt6pwvDOEQQO9tk5IorUKgRBb6cb+K2CExwC/DtW69cF+CE1Pe34wOc4CYOHIHBNua/xtgYWEZm1CEEQARjh/DsV/GHpzliAH1AtjEcavRYs8bgKBLtHh/gdLwSBHGMZIE0kAKChOX5jUPDTDgC/lxcz6vSG5CrljmsAP9fL96U6+qxBPjfQd9FYUtxUGUzwc6BrJde1O8dFLFuCxP5G1JhvZ627Xf3SkAJgArBiTYnPSyGuCML568r3qcrbqTJqEerKu0VrqgAQ1QBOdYwGwR/5NQWMqFZhGBUEt7R5wCDPmDLsAYUgJ0jsLyPmgJ2znnUU04U29gYAK9bYeLQU/BgXUtN+GK8K/jpofVeqRQiv7UO0FQS6sTu7FqhKkitQTJ/DHRt7vHfMtChAVqxKrsAmQChNizNDQ08VhMAOIwACAOIT4xORoWdBmFv4YDHu4A/D9Ufi1FQX4FOYCgh6h9UChRCIQADlEGLAIZxk3iERonyFKWMtYnxEujvd7Xi8EugeAqA4g1+SoHmFNPROAT+5gX8kSRPCW6XgNIf86EJF4iw4+Cngf7UltagqD6p61G7vD3JOHpRqTJSoypur6B8BQJGgeZzBLz3x63GaEoAda06cL4lu9XR6dxD43bV+/TR+PTV9gPruOip7rotl4DpaBC9rsBMtgUVEpmK+IDOYus2teK4cA/mIT1vDVNjiRQImumZ5dwb4w9AdCmFVqr6HagM77YAEAFcw8pqFgYMAlcfBN+BxFQJdiOB2w9AHDfcOby2nQsLVdFj0NUE0xZX3jzP4aSzgr2LCK6KcXhTHJ9iYN/lUPcKDcSie84kuEZGDEnoESb2e8osAh6ZFheOoJyUKQ4cq8cyCaApZ9DU3KuQ8JQFAR1ta5ki2/wMgVnFNgia5ajwp8qAvgSgPhdD4gbU9uQrjO7oalN3oBwezCnW2CFza/LjjLEFl0EgDyCgy9skODKwrYr3cAggALLV1BRElis38Oxz8Tq0UBPw+MW1Pp9ZX2zs1ffU8gdfxULXfcoVe5fWhi1aEW7CA/K1CmUUIRxU9bUHFh9XXElStCiDki+YSUltXlLMa5+XS0ZaWuZLtZQAaSg55R4I/LOUJEz3yrQ2i0tqhEBRupwZBwzqT7MWe2fehuliDIH5BTZagqj9QPIVhRcbC7MDA2iLmhSPyw5KtTxe0v9654K+i7qpofQGXMgnPCnCHtg+i6V00edXTptp+Ayn7cisRwDJQiYIpu0AeD4k6WINAfkFNOKoaHqVCI01CsvXpgjDIklqf1tbWpqydfQVEc0rMBO0M8I+31nfIdsByQAoA8nGLBHH4Ht5A1oG9LAJ7Ro2qWoN3riUoJMd4XdSI7t/X15cs8n4JQA+r3CkkxNwST3mcwE91Bn84rU+juSzhPxbAT8uXaGuXSJArz6/11+VBEv7WwmkdfEcfuFoE76hRVWtQ1XiTZ5Ju3CxBSXJMzB1WuVMKAiFHzkUwn1MiOOMU7agp0hPA0XUN8VUAn6oCPxDohTtOAhehBS2Eq5atDigQhCq0KoggBKkCDUiJyKOCbtyEoPQ1XMD6qOFuampqt6FWEVFrYVob1YX31wv8PiIdhO4EoTpEVR4PkuSlHcSBOOAwQh+6xByUGnFtTnIAShQ4TBqaDnkUz+W/kJi5z4BckEwmewQAWKSLszpt/2hSnWbKBQ1zVqE87uAv1V6EYBq/mqZ3Vc7kxjUqX1nh4AqfX1en1o1fkW+tj9vLq513hUWohzWg6rVRdbEEwSoqCYAthGi1SB8/okQlcIp/ppfGrNjIBfyoEfz+XN+F54cEfmDQuwHdBdShssTlwHURkArBCCgMbhTJVxAqaBF8fYO6+AW1CkE4f2AE88bkyZMTg8PJgyif9hKoR38f7QDwuzb+Om6VCDEfiKqUeXg0HAdrQ6A6ZILZ+3uLVIWdB19KV0Ze6nhP8ZyLVKj83xVaXue5celzjogO5f/pOnmKXf7u8jXO43N9OVW5Tj5voNHR44LBYPBBkydPTlBDQ8M0GPQGCWoZGeU+Hrx/jJzfn/IE4PpuoxADAZ98/YFqzdBUT/ZfrTGW3UDM3i6Eh5/gGUL18g2C+AUcxIcJ4BOMwR9wfDqx5gHYvKchpTxECzQBrADIdxr4yTuMXwn+gFrfF/hUyQWpmk2m8WuLcTu2EZ1WfqOdFtZpGcqsgpdFYC6V+8DWwKn6qfKhoJaAQKNC4KXtqZYdMCVWgArh0CYhcYihhd4TEKJyyOg7GfxllCeg1q8OfHLpMfYuhR1LF2ToWjj2vraVAsElgsvsQpFcBIFQSYtcrYEosB8uv2ZVKFFQISh+jgOx40GFtNB7GgwsJirVDGPdIlBX8NdIeVy1fgDgk1fpaIhCrlomshBqT3w6v89NGEbOyQnagILgZw1GP4JG1f9/gRA4Lt9ig4iSgWb4jMGG71DwB9X6ZX/xA34gyk/e3GvMLfHMrsfBQYShhOc46BFXItShHStpkZs1IKfiH0chQL2c4vJrRElqaElsJWAKlTK4uml/34jPWMAv6qP1QwGffKJJRGPt+w9X2Fmmkpmr5M3cpIWd72N3x9PFsXZNounqzrGnY8wBaodqdYrdzSYj78p0UrwlwSGG+P9XgN9f6/sA39cprhwM5QV5AsalJZID9uJytSkNXPmEmyDULgSulXZ1E4LQmWKfqJABL80/xkQX3gHg99T6IoDGJ3ef2w30RHUg+NVunBsF4spSYy6L1nNlkCjPilyoEWuUUJjS17tQIjfnWDvdzRCOcZDoTl38gRKHmA3PSt9abmoQp5cCBplqBX9grV8F+ORPb4hCln1TDRqfqkSDqIowMI8muMoFwcVHIFG0BpVOsquDvIOEIJBTjJpCo2TUpKaqFLmFTY6KKqFOV/C7Obt+4BeVb64GfCpPCVINzvAY1jxRkLVG7CMMo2tHAwqCvzVwc5BdneOAQuCqxUM4xaiDQ2yM1WSTa7SQxtQUEwb8VSkPuUyKrgL+qsD38Qt8S39rmU5OXHnuHEwYvASBXSJH5bSIhCN/UIUSkeMxdyGAZ3SIQ0VvaPR61EKFXL7EGKvjW1feXzfwe1CeOgHfD/REVNcUcDn5YmbP28UBBaGI2hHQeliD0ZcXLhxzmcAEFQL3ECnBJzyKGv0BhLMCRn2pz04Gf0jKQx4+ixfA3XsDqKY+g8CGgH0EjEstRLmG9i58c9RKlFGYCmtAPEqJHO+rixCM1R/AWEslqIwCjZn6BN/E7m0s6gl+8nidu9Ynqh4FqgZ6ovqGgco/j9lD6TiEgbi8TMJNEKgA3ALAy6wBl5F+Eh5+QU1C4E7pQgtBLVSo7DljTLn6MPH+QBEfn2hPjeAn8tL65Fde5LF8I0gIlNwd9Zp8AKpwMIk8EmAOYWCXztYKQSgq4XJaVO4g1yoE8IoOBXSKA8dxxkaFjJq1f5i+AAqRL/Cp6wkFfh/KU1XrhwZ+tcrRsfgA5bX/VMbzK1cYOZ1e5+eU+AiltB5EVOokV5RAhBQCr2pS9ogMeYXrPZFdSqXAtVEhA3VZQE21UZ9y3u82z9Ov1icI+D0oj6vWr/g3+YM+COCDdNgFHfNXfl5FgRihN+wZ/RmhGuRvDfwpEflGiKoKQZnQjd0fqCxNCmsFjLFo/5qojx/vp+pJrprB76Q8VbR+Ocd3DaNSlfh9tdl25E55XB1at6nLZRbHWfJcbhWooMZHfAQfa+CkRGXJ4lIhoLKCujBCwP7+AIdAIxHnP79GK2DUpU43DPUJyvvrAX4qtu96d3X5af2gwHcqglCcn1DRSOJ5+ZlKQ5XgMtpTRRAKPkI1a1AqNFWEoAod8gRiGH8gCBUq83nC+BLGuGj/UNTHv6SZyLs+yA/8vnzfTet70h13aubU9D7jRkNHyDxbFysasApRnDLLUFr7PyoIFbQI7tbATQjKneNqQlBOd/yFolIIPPMDXlSIUJsVoGoWIEjNe01jz8mf+rhRoLK/jQX8gbU+ebVTku8IxmCj2KtngakM/K4dggXLwFRmFcgBDKYRH6Hc6a2wBi5CUO4c+wpBGbnxjAxV0J1KfyAMFar1vQZq+Amr/b04kBv1qe70Vqc9fs5uRfLLY9skCR/g+4E+aLFflfvJLjTJKRBuiq4IdC4Beik1qrAGRd+grK7HzTlGEDpECO4P+OUHQkaFfKmPz5NGYE0etA92jNSnGu8v5+V+mt8X/B5an1xn3rgDn3zOu+bUMLOnz1TOGCoyuTRqFUClfQIjgqDJ3TdApRA4tCkTkSqocsmFBEV5p21eCEL6A15UqFonWRWHOKgVMIJr/PC1vVRtOqQf9XH7TgpQmuAHfk/K4631K4rZvCxOgLHSFPBis0c7V0ndD5Vq29JepzwAihnTEme5CFIX36DCLxiNEBGDjHxJRJklKEuWjaDezx8IGvnxei5E70Bdq0HDan+vhFcg6uPG+8m75prKtHoo8JNbUw25NMNURpi8AD+WNcJU4fxSRTcXudAlcksdjNIiHgG7AHlRInb0H+eXo5MwDHp52ryGXwLA5tWpz1oWFjFYgx1TxCsaZXz8AV8qFDBB5qlYOcgcruAWoH7an3wmVFWjPi7OaTn18XN4Q4K/fJlDVeBTgBAohewJZpf3jFAeqqRL3oKgCoIgUaj9Kc/w+ggBMwAjIta//VLXjSCgcfKEM23LXlQsEg3rDwTT0FQSCQhlBQJOovMWgHHT/t4yVI36EJXV97vx/rqA30PruzbbULWNS6HHzJQ0wJBbG3e5MFDJ5IiSkv6iIiVIMEASKSmQ1poiyuaW/N6sAs32EoIRQeFIT+clrQAwa+HtERDlTYhfZIiKZdRhqZB7cQ+FKJv2lAH3cugw/loN2p/8tX8Q6uMlXBVOL7kPtap4HlTK910cXaLSdjXymgBBPqCnGnzg0fbvUbEvD4W6TWAr8ApHCT+RQGb+Pon/TUyI9Cw6peWNk06e0fmpU1/84raN2fcXKIz0co5LfAICA3PswuVgJ5+pCI9WlHiGoELlaK0W2nQVhEqh8xMWgbEWv1XT/lUc37DUp/J95BvnJ5e4qhP8RIAomzPk3A5J5VqfKmcSlSzVII/nRo7FdysGkyASggQJopFpfeQz4dl53UafIAIUMyJT5zWsvfsXn7lt/6Pbew/B1NTM3RIvEaBFSUStGACgyvHlPgqKiHxzHUQ+hX0BykgCrdENG4AhPwGoJWkTWPsjkPZ3j/r4835PbexBe1z5PlVSHjfgkwu43UFPLoD3ndGuSQqKxuRdU+cmPiwEdQkpJBFpImIi71WsJSudSo9XrF+R3ncr/t2STtryWWxuPPiUlhUkkAVBElw+1wn6ksNuH0UMlQoBuYKcKu+lX4VvtQbzUNs4yX+GUxALUBm8oTFof4TT/i61/H68392JrgQ/UemFd3V2RSX4y/MZbrt7K7V85X4iqvgdeU9R82/+2C/2+fxTt66+5cQPTz8hGhe3520BEUDK+R4Ug0OUZz/lpeJFQRjozu05FXFbGsQZKLH7IRP6jSh1Ft/v1PIjS0HK8y0CAHpGhcBLmbkZBLe8TT2tAGFMZWyinnNsxsr960J9yoBNLjkEX/CjEvzlFMRN23vxnhLQehmEEeLOTX/+yuv/A3yu4VeXHrzp9cfe+7Epc2OXkqStJIUEYBd6FLUQpOGsJqz8DiIAw2med7+1oaW5PWL39WTNs1rm9sUb5eqR3hlyoTpOheFjtCpKTshn+p4PAwhsBcbSauHBbkSwGp7qoaGqJfEUMOwZxPF1sQBUpVONyHtnbVXwuzS7uGv7IujLgFL9N481jeb0oP7M/if+48d39KxNAAvsp295/C9HnD75TDOC54QpDBCRVhDKhojE5IAZEUOj/ZMl14dAYDurZzx0w9a5M5HIKpspC0XNHZEVfgO/yqNuQe47kQvz8ahlqTo+xssKUBgrQOUMKqAFCBDJCPa4j/ZHDdqfqowaIS+nF2VOr0vLYomzSxXgJ6rk/uXAL6cnXszfIRyaCIocFIYIzJrV8JD62NfOeO2pA0758feewJcm3HDlR1f8/clTTzcMulYIGmyfHl2x+H2Tf/2zpw/83N5HT7iHNTEJR/xk9BQ0COaa5UNz90BrFgB6kZXz900shYBNVDrHQLjkUsgZ+XE7fXJBp9s9E1WsQAiPty5WoFoYtKYm95BVpFW1v8vwq6rUp5z3C484P8G7+d5D61cKEHloPQTZKiMcccDibmYigmRmxcDc9KB1+aeOffbAg05+8/I/feX0Lbc/efTNS9ZsXzZ7Xjw3EdGMBmjfY9veeOWRPgYTkSPR5cjmomttdmEW6i5pEK/EQOyIszpWP33n9pRS3OIo1iyUNjhyACIfxmRQJP+hMzURrJFKU5QXuxV6COA2YaI0LumaHAuSF3BLE1TpFfCKloLLFmKPuSOGqu8lCsX9fZdVBGjSr4j4uER7yjV/Vcoz6thSlS2JJQvyyvJaTROMx5vbjAeJkAWRLLxKF+6YBKCZ2daKj3jmzu57Lrnmn5faULvPmBdXw4rF6uRQcx9ykZb2yHC82egcyUW5nEMmrWYUvzwGqQd7rAhrjlRQHnLs/3OchxmRG4Cp+gXc0KgUT4X7yCKfHQuogy9QY+FZFZou3PBKtdKfEOUSobh/AMe32o7fEsfYNdTp8nc3ykNUEiUhF9CPbHN0j5oxBFmLTmq//sV7v/yh498/9UMTJhr/NKNiA4iKKQlVeLWhFdtCYNqLD2z/wj/u3jhpOGV1CwmOxKVKK2UcdGBb50Vfm/MrIckazRGXCoEwaTgKyQAwBEvO3rNxyDCpy1lmRPC4VgRMmBJZAnzAuulXqyapnF7IzkBRteFiYX2BcXKGKVAeoAZpo2pyQsGl1Iv7e4GJPIWjjPqUR4XIPdTpah3K/uG2opeoKuhBgvLhTiIhBNu7HtC09X7rvhkf+cS8N5bcdeLnv3vX3u+dOD16szTEAAmSGE2ESRJQwynd9MTfut7dmog1IqelkMRCEvflctFjF09e39JubGCAIIgdpd0EAOmk2t0ZCTq3fUFPQ5O5unBluCSKNBL5HD0ZZWsTAMyo0CCyKKi2p2DDxaoPXwvoDNeAY1G3pTBhnd9qCyiqaX8Xx5dcB9xWOTRf8FMF+CtkWnhENAglsX8n/TEjYvOZR83obDEj9qqugfhtXes72lpi1l1/O+Z7H7t6l7P3Pqz5imiU1pAhZMFx0UTQg725BQrabIyYOa3yVT9aMzVC2hOnNyxHsTzHMfyIiNjO6ekP/2nrnNFIUE5MmGQuc7OIbmBlxQYAlpJ4JGlOfsktCpzBJa/cTt3Sv+Trkwr/pXQUjlMFfc7XVwiQ/fMdR0KV4b0ycJOfE+s1Ht0t9Crgu8WbPLxiIUXyu99bduRzT/R0KJtp0eSOZGdXOvr0wNbmgw9v6739mqPu+fYde184bV70R0IgQxAmCRKpATXpxv9dc/iSZ7ZPT/XZpgBYmMRdyDR84ucL/h5vFAOF+h7tMF0aIHPlK8nd9kBrVtl5wWmbGtlEDuRVJD7zSTIB4uG5ezctA66LZdJKUvmseApjBQLWRASsr6I6yIcYq7MxZv/Zv5ykxCtzH2dCPtJObkMoXGuTPMOcRJWL2MnLYasMh5ZfbwLrbFrt/+8H+3930/fXXnvLjzaecOuD62aeP3nBtmiL1H2WHbmjf2Njc0vE/vufjrjzhA9O/kBLh/ynYdI2ImG89EDve6ctSPS3dkQytmIhJHEmo+XMhoTed3HbtVpzWohCAZsD3JGEOZxFmgAgCsm5tB0HcSnFqPSLiEC5PQ5q3QZcknvxoe6DAWoE530Uv4FhflagKg0aM/CCf5yoNfQZOvrj+XRl6LMmOaLKP8klLu3ZzF4F/O7lOyX83jdSRaNz9wVrQGtGdljv/varQ9++7ccbbjjtMw9/JDegOG6KVKxRTMgqfvfz2d6PnvexWWrJXad+5qKvzvzQxBmRm6yMbl//RqqjCaZVpEEyJtSWTLrxG9/Y+/G9j2g7BTySWxhZEdnflZsBANIgXobe2B4Ht7xBhOECC2A/KjS4PWcCYKU5AvIYgE0ImIEld4YfmAahNhrk8bQYs1cNj46vwA02VUKffnH/csfXb+eX28YXchu6W8n5PW0u+Y9Dd7gCWmtg+q7xJ9/1gSk/nzI3tmTq3NiSRIuxkYhz2bSe8taLQ1/72SdX/WTDmuH3RAx5OhjThSmsgZx96O29a6e968QZm5+57T3f2uPQ5s++vXSwg8AkRgakgGRM5FakBubd/fPz3kg0mz8Q+QPTeSEA+jsz+xcFoA9ZY8/Dm7eRIGs0mkPut5PZVEVBM0W2vGCu6At4RYQ8mTSFYwnkw+mphsBNgLlAhPo4xeF2DlQLBVPAPICf4+tNRysdAE/a48NjXXgpEYEXHd/27Kcv3O3F5R8beK1FmrltmXTsrSVDE5c92beoY0YsfuCJbf3tU6Kzczm2CGTld/hQc0OLcXR3auihv2dXTP7TNQc/+FSmd1q3snYxI2RrxzKjWELiXuvl3Zc+uOGqI987p7d7U+5nDM4yyCApLMdN56F+yyxJgTjHbBbSYgQiGaX/vP/TC7YBfzEHurKH+uauCO7L82poahnjDqTAGA23IcaXhtHYVygFEZSyrK+7Jqnk/lTFi6IyDkXkD35X/uv+D2YGNTSJbQed1LHu+d6eiQLAoLJNwyReeMyELYcd0xHT4I60rfstSycIHCMJIg1AQ0kh5lKUpyVMrH+wd1t7S5uR1EBaAXEB1gxiE8S9vTmje1M2umHhJ1ue+fsnr9vr6F+coRSOY1WJJCFpJPxZjsriRBQiwIyIdXsgYR3/4W8fb+X0bGeKt9hF5rbDy79Fq/JFI730Ph1jo+8j75HQIQVHhDIjgRpiKFyHWRj6E6ZWJEBkgvz4ug/n9+t4dn4v54fMMgOYt7DpyfltjQN2VgsGWGumTFbLni252Iat6aEt23Ibh4YUa80NWhNrBVIKpBiwNbPW4iAZoaZ4mxy2Uqy0bScJnANETismDaamFiMnScz/4S+XzAPWGZ/55T4fiTcZv6HSgLfncbtd58YO45kWHJP93g0HvcDMTUGLc4LQoLCjtMcc93eBrECNya96paipVrqD6un0SieY3AWB4M/7PWcTuX8Pj2ozJgYJAfvgUyYufe71nqnKYpHst6NWFhKQiMRM0rboJ4ZBzDNVDrAtFpalpVYs2GahLCgzIiavWZa+YP2r6emRhGixLfTZWR4gQAlJrAGKSsOasksDr/h336d++MAbLR/fb8/Bta90fcqM0t3a0pOjaLFc+bVbiKZwPoYUWeAD1o2/eWuyyvG+hSlxVFOVQNhoENUY4gyxpESMGcj1ei0FyGd40B8in/lBHty/srSRKg7F32F3/0ymEgvCzKDGCeZ6Imn98/ebj8lapB/9a9dCJcjeujYTe+PfQ+0sRDQ5qHszaXTZik3bZsE2cnYOtmIQM5Od03ai2WxomxKZaylMb2yIcCwqO+ycair26iZTdmy/eOuGthnRdX/+3spnzvif+w7u63qiYY9DWv63odF46W1sj0cbpB7xBMg3hQIAULaOAOvEhAmxHIhy5dEyKpuoTUF6oeu8RJxqjlYCBtWI4KA8fqwrVAPRHwqQGCHfjJ9rbVC1K+sK/tLudgEiTqf01Idv2vKR5omR/lxK773wmHYtINuzKbSlB9W0SEw2ExCBoIyV4y1EMJVgFoKlzEKwJK01i7apEZvAExg0tHplcrIZQXrynJjBOS11RFA0JqzVSE458YNT3vrPk/3TVr04+Pt/42eHXPfjY19/Bhu+/6bV2xGNm8p3SvWoN0sgziXazK62ybtnTrhkfjsBUU8/123aebWm9zIeH84PqI/gGIHMR72K3ypG2Xl4DuRdNho0Ve7nG1ON0/DIx0q5gL8Q/yRqiEo+9wtzX2qdEh0e7FONkyfEdCbLe3TMa1KT50Nl02pXIaVJYK00GmybNxgmEoKQYwMQzMIwSOeyDCEwrSGqu4b6rF1Yo2/WnKatuYhq14oNIYnTOUWL9mzbFI3TMjtH+156wnM/f/Xhb35EDWwSZouoPj6TRkZHC2i2T7po1qtz9mhctPTh3pNA1ABmXZJA5QCjehwl1u5jS2oBtdvsxODOcPFpUYeegkAOcFiKRFXkzDdpEpBWOSM/RH7UkXx9i5LF6uyoLmOClIKT/TrxyhMDM4az0sxkZHwoLRptZcYyWRFTCvNsLSmnZCZnCYtJzhdS7pbLCShbRGyLhLKJbJuFUoAGjNSgnr7nIc1b9zqslVMZNa1/W66xOAKLNBsmzNb5+zffrLXm7LA+/7Czr77kjJa9+qxByNGzsEcy1y4XiEkQpCleP/ekeQN7Hzeh1zCQKvr15Wkszz1sqD9tpjox77FVg44TlwsWaaJgnUJeliFAyItE8Fk+Ts3PpeWfgJD5OYRCwrKFZJgJEsYsMsypGsZkQxoLNMyptoqwUkJa2hA5W2YZNB0kZ2SzYDsHadsgZTPZFgtlMYNpQrJXNw315wgCqq/TFgLEJJnMGNnrh5MT7vzJoffudkDT15nBvZ3DP/rktQ/sP7c5kc6mlfCciUbOaYsEIyLWLsCi4S8e8tTm1JDat3pNcg2REtq5eBPjDtwg9CXMylEK7kT78Xki+Hi13vF+8qE9pR2+In95hQSkAY0ohrPRoVQ6ti49HOvO2LFhmyMRW0XYUqawrIhQtkG2LWXOMiwSci7DaM9Zgm2LDdsmwZpJK0AzIqyZAKFY8+y5CxPNSkHaCjYUEGmQ6s5k1+R7rj3+1l0PavwaSDS8cH/3N/fBhHR2WAkbTGZE6OJgOS8Bb2mNLgFm6SdwXpud0/Oq7gIP4/xWCyePFfQ0jvsBgjYHhI7bkmOuTeij8NndG8AqUJjF1uQ+zm00TFXsipHQLCg7TIjGI5OkIaNmDK3CEBIgU5Nhaw3OF14yiDUJaGZWgLIJBnaxs3gVFtkMVRxYwgIMkGjTjD5SzFZGNRqGmMDgHi3ldgKrpiYj/fftq1vv+fmpt5zwkX+2pget6VnkBAA0ImpvXbctDiBSqBYlx5jpAjXmwbmLGpcA2+Q/b1rfpiy9cGSEBJd7rsGcKiquXOJgOOCqO4DLB7SHXxVv7BzTQ+P3XT6xTApTVEj+FKuC+hQbA4qanySUEmifmcjNntSQtW3ZQkJGNCSBpVYsbA2SRAQBrQhaEzRptklAECvSQiAhTdpVK1qvbM6A2KL82Fk2ojxLCOpgIEbAIGsWQmAXrXkiBDIK2L2lI/Li3am39O+uX3RjJ7LGk+htamiI2FkMxxqaTLZy3CMETSdiJoIuTFbUJEgaJpbcetWXXwNWy0f/uvk9RCLKmvMNOu553JDQG0t0ZwyRoLK3inoq/1D7bymk61xtL0CYZAy5dcvUuAKK4TKnME99hGlycgBy7n4dwwsOn9qZHDJsJWIyZUebLIqZRkMsLWPRdA4xM201NNuIGoojUGySYlMoFdFMYqIRpf1kVO4jiKYwMAWCpmqFiNaIaQYTUzOAOINygI6T5naAZzJIRhJQK9L98RRsCQDRuNTbczzn+OMmN7/3ilnXdkyPLAfBVjaE1qNnkmg2ngO2CqBfpHrVYpA/VogQqkKNArQr7oiMsFHXHECNS1GCtoKWd2IQgifRQubifI/b2/EdBT+TBBkGWbaUj922fZJoaFS7HDRFkcTAXnOth4H+gb6NfTkAaN21NQI0tqxeFzkslzVaTZFN52c3A6RZKQaRwU3CEHsICRaSBTQvAyOlAakLzfQEJqFJs2ACRD+gBcEwInFWNjQZIJuhJscionlrbpjPvWD2prMumPmDZ57eNuOlh3v2ePul5CF9XbldiaDjLeYa4Dzru/+8eIqVU/sWhtkKLhs6Wx7rL07or3PIftwMgrGjHOBx+SwKkbqoR3Gr727fUe3PxQiQMCDMCCBM/citvVPbZzQ8ffjhA4/99pNLd3vp/s6jezcOTweAtpkNmw9697SlF/9onzu2Z2Yf2tMT3SsikM7PF2dBYK1Ya6FYAwQpOMsKgww2hCZmYtKCIEdXVkgCJwFqZxAJ2BsEDAJ01AImS7CORITuymUiBKYjjpq88aCjOrZ87yPLpvV25vYg0uk9jm5ZBjxqvvZc9xQCGtmPio83gMdRkIx3EuKpXl9RlZpROFPga6HKtD+J/JSTggCQMLQSDWKXPSOvTpFvPvH+qcsu7escmmq0TkfzcUcBALYvfXrXe3+zetdn7uja8v179/1j+5wD0bs9sjcJnRLQ0FBESkCTJskgW8EiDUUMYsVgQUwaYAlAMAGUVRBbJZQioM1GxDZgbchBzjLyRabMAJkRYoB4ENnElae8+o3+ruwsaQpIg1665FN7bAXeZW1cftVCEMVQ4P9jBfOOMgq1CQDhv/SHxkEoKeg2Uxf+P2IBmMmkxhaj76LL9D+/cPjrl/Z12ZOnffArquOiLwm0tObfOtCH7X/9sd5y08+nfe2M1z7+x7cn/S4ZmzNN58yEZm0TFAkSzJqgpZBQnGIFW0qSAJeUNrAGQYAJrDUEifzOyA4bQgJozSd58ylUpQBYOjIpFn9rOGmtlYaYAYaYMDn6t0Nx9cALuLKlt8s6r2xFmXMZ0jsT1WHyAPROw+w7/DuZqnn2o34ACcE2R+jId0cf+ddvV83bvnFw8rQPfF5NveJqyZFWaheK2oUijrTStCuultPe/3nVtzU16forXttt1zn2czlbNGgIMASYBWkWxBratrCdmYUGoAsOgIOSEcCp4sRFLuyMZIiJ+dVgNAJVUwLNscjgQTj9pbYpsRtJQoA5M3XXpuXAo+aNf3xrsp3TB3Ox6D//VeSxjuyde993WiJsDBeE6nxhXVIuPLKAopYvZI+pV47tGkyCjAjZ02Zx19N/23yw0TadJ77va5TbrrDnDI1D95A4dA+JPWdoZLcrTHzf18hsm8Yv39d5kEJyUJpGBkIUXG7BDDKUpkFboY+EkFAazCDWyNdDAxLgQWZ+UwNJcoirzNc+OJ1VPdCXbfz77zdMfBr3xv98++J7DYMelya9edcPP/gyMEkte6j/ICIRKVwnxYBomxq9Y+8jW670uxK1Fk7+f1MAds5PPngpSNbFeLuNjCAwQ1AshkxTLJvevjY1rXm/o4ibEzQxDsydInB2rA9nx/owd4rAxDjAzQlq3v8o6tuUntq7vseKxGhIa5LMBF34AqWQLJZZl98+1gCT2MyKJClOOLW9LoObUjoyu7Vx1XP3bjvo2m+umL8A1/fvftiEb0+YErvxG3+/e9oDuKGle0P6g4WmeQXAmDDR/NdfHzz8W/f99szbdj+k6ZtjsgT/vwXYieAvRFBjjfLG/24GW5j8oPPkjJlXKkulhaQMg1IMSOFqq5hY6VQ7jM0nXjT1ju2bMrsDX4lf9OVd3v7qTfveduC7O5J/+MrbC6wcH8bMNjMiLR3mv+555LgvbkE68pf1S6c+9Mezbtrr8Javjswz/f8F4L8C+sVxIZvnH5B4z38e3nhJpEHcTzQKo5rtiXORb6EklKB1JoNYMhONd8xNbBl89WmmwRR3p4G1nRp3ZFpxR6YVazs1utMADaZ48JWnuXVGfGvb7HYzl+FGIVgRMYrTz6VE08hea+hCIQMLIZDVzEMkhGbWQ4K5P98yWar58xtMmUSEMm9aqeh7Lpq67rIfz3/9jvTKpontcdW7JSPPjN+9beWL2y8gQQYzjAkd5r/uefi4L76NgdjgQM6YNjuRWYYNjZ1rhw/4bw6dvDMEgEM9HPpzykP2+WZvampuNvuBq+LHnjvxa0S80eHkBftkcpeCwgbq/Gh+1rBzbHRupElHnTf933bvZuq++fsc6ZB4Y5PACysUXlih8MYmgWiHRPfN32erdwsteveUFyWampVlx6B1IQSvicC2FNxsSLSy1gqFgBOIlLKxiiRZGoAwqAMkpmogxw4/oGgNBCCjMBokKNILJSZ3RDgRtxpTKWCP+W25q584dk56UJ3JGtQ2ybz79oePvqII/uaWiL0nWjLvO+7ZH/VszZ0bBEdc4z3bIQLA/58yAfk0JTOa33hp8KPARP3rT313ZUtH9Lfk3As6+qMryq8qL5geyYUWwa81wIoJmgXn8Ngd6VNOvHTB2o6ZzZ1b/vIzufWaKxXl+rhHS+7RkinXx1uuuVJtuelnsnVqYttHrtn3rZXrjMMjhh4W0MhPOtQsSDMJCMNER355HhiCpGbeJASloCAFAFYkNJhkvjyiOMafFUgS2MxmxMYXHu3e2mqaaSttyxwUG0C8fzAVPRT7px+5YfN+0hDY54jmLz304PFfWYPkCPjnoyVz8uLHfzKw3ToFecf6v5pFyEhD5CoKvrjXc34o+fXiUvk8Tyr9e3kU0TkNIsAgLOfOr/Kx60QVs0EJACuLd31t6O17zzws0p/bN73q3/dsPx0C7YWtKoWhUiQMk7ZojWZy+A7ODDQXSBUJ0sXOGgYhEjNzh587+/nJ8xo7c3Yk9vZyPfWjX2289YX7uhd0P/tYS899N1F2xYs0+Ng/aNOvvkIDS/4lWifHtnz/3n3/bzixx2GpQT3boFxWkgVJNguyIaVmQ2gWkiGJt0mTlMgvVO2TgpIaEEIS59MQxKUdcmwCOjNs4fUzo6e//OmP33NlhlTvKYumb+22MqaWWkQjxEmjP9s0W3bvdVjLY7/+9IEvPZre2qwspsYmU81HS+a0xY//ZKDbOgVgeySPxHWw1OOhiXmHUiAe/0/g8B/ErlaAGUQNS/7Z9fnP/O7xOUfuOX147l6NV2B0rIMGkWhqNx/4++NHnpRoMZ4siJZyfCYzAxNnRpZOmBh5S1ssAK3AGswayrLkjF0btk+fl+g59bLZT7ftMt3u4j2O/uuWk/5w2ifm3trR3Lty6PFbU0OP35rqaO5defond7n1D6uOv07MOfCwnh7sZcpcKr/FSEFAs5CaBTGDoA0JUwhIAjMJwDCpEVAgk1jKyhuswaSB7RJ4LbkhtxHYh3MpvecTt3ZdNh+TU9m0EhJSm1GKb8fQhL0OSDQsPr6j/86BDRPyE+GIJ6LBOuP4x38yOKr5jZqp6DuJEjS2NXJVCyA8LIBA2XxH9/2srhaA3PfSkpsFGNnj5dDyzs8ozrMnl/n/5GZhAIAUCSFm7d5w0aN/eu+/BrBdHnbM/fdqhcNZs26ZZP7rz3cd9pVBWHL5W71N13529bWZtN6vwG1EoeAme9E35n/n+Xu7D+vZas3q2WLtKwwJzZIT7Q1DJ31s16dELK5gxpiId22d3tSkNQ8umGc/BwyNFsPNLBbDmYdZWd1qimxaskWSLJZkaUkWDMMm09CQUrNpsoDWyyBoq2kiIgX6YPCbQkgUBcCQpHU+ESYAvX4C5LbuAT28W0tj/y33b2y75Ttvv6AJiQu+NPOc9541Z/36dDIWjUutoASsvDcDAO1mzDoEs5NHnX/nZze8mb4cDBsMQ2O0Ooh1IcJQYIBceG7k3w6NwezUIKPNFCV/R/n7KgMOo2yVHY+VCZj2EEH/cuj6yyjzGCyB43hLKTqXBF58j957DxVtWZP5NAC0YIY9c7fET8GAGREbr7lr/29sQ8Zc15VseNduk3pPvXTKN6WB7YXsudIKNGOPxDOJZpnd64gJr33hhr1+OWv3+OOGyUPKUrTnwc0rZu4W62lq4dykidbUtnYxhFw6DWXFl79Bp7+5csJ7+3nemf0878w3V0547/I35Ols5RIRGk5LzpEkiwVsFsKS0lBMxElt6xXa1m9oxa8DSEoJIQSUMKkBEEYRtIYkzWBBIKFttWEhWjde/+M1h/38s8uP2g0/GBramokUyjUjD/2p68KFaB62LU35hJnU0syPTplqxqynn9vevu8Jf/vVxrfSl5dofna/t8W7wjtS3fMOjgIFBp7Pmznse9lFoNhbaAIIpGSltbL0EUef/7f3fv6W+2d+9OrdXhCSV1oWz/7qea99/hjM7ZMG8bJ0MnHGOTM3HPHu9i/mGQWENJB714XTHtnt4KaeRce1bujbmm348h/3uaF9amRZ+1Rjy8GndqzSVpZjUctQyuqFzuRMkY2bnLUTRjZpcsayhzNxlcnGI8hYcXN40EDWlpTDiOaXOUGsu+0sv6qy6j+auZOYu8C8BYKGhZHPAxAgSMIQI7gjyYAN2GuH+7P909GWW/pE38XppG4C5mQbp8ZyTHn92DIl+tB2ZKRpjl4f29LUYAp+c2vS/P3n3vpTweHVfsWTFVo6BGY5JIi4jvpa7CB6H+T06/vRXHlnXLQSsQZ3bcp8L5uy5Xvbv9a73+K2SwC2tm/Ove/0z97/vmPbOwayw0psSw9Hfv/FRc/tdmDzN5kh5u6VeOy4Yyat79uai+VsLdo6ZObRf2xeMHV+fPlnfr3nDbEGrYktIlhDUHaXhJUS2u6SlLMEZWGKjBGRGWVi2DaQYYksSWRhkMUGWdqQFglSKWWplQIqJQ2tTUNLYZIUkk3DzN88aZAGYAggDmgmyaShsgxalcnp/okdcfX7N5bNymVU06QZkZUv4MJJrz/TM5k1tcca6YW//v7gF55P9zRHnPOCTCCTE9beUzs2ao1eItJOfsFBAFJ62ZXvjQ5tLrgu0GP3KJBH36DbGH9yiQT5TGQun+BWMpKwYiwQhRuJ7hYJchl5WOYHEECaBDVtXJ6adeKHBv516bHzu+5+ZuOyZI91ak9n7qS3upNLrzxhrzdf7e1rXKWGYt8+c5+XHl7W1b3ohI7O+bs19SQztikFsbJZxOIys/cRE7ax1ruQ1oYwSBEDRFoyY8i20G1bvE0p9GjmAYPsmCTbFKRYwoZRjPYImw1DQ1v6LUEqbZpKmCa0YbA2DLBhCC1lPqknBDFJkBS0HYLSQpDJoAEJ7s6mrNgp0ckD3796+clb12QvaGw176Vm2rbm1eRu8Sax7Vt/2+vq7TJnajBpnf8VksDQYqhTpY9rvmvLX+75RdNwUh/PxVlBHMr0q8I6WMFcGJ/LLokzdqHpXsIWytTv7EQYVzkerpIM4wAn5UOH2DVTW/7ZLLVi27bw7vef9dhHgVOyj9zw9h0NTeJvRKAlD/R8+zv3v77g6MntA8pmunNgY9tvf37A7ZtWDnU98kjn/LkTE0mS+WG3bZPMbM5S2s7xKpLYrDMWEVlGQ0RxxLBULJpNNMYzs+OxzMSYkWkwKJczZI5MaWnTzGlp2GwYSkVM22St1hLsnoipyTDJNgzWUhBLCSajkNE2iImIxUi922ipA4FJgxhoULkMNxJh3UN/fPjew45tN79+7a5v/evPR/wyYZoqnc63Ss4wG7PRAvdnEFNUNK3GB1pP+fgudzDrVL7QrgD/6qHM/FxrIsmM7KRZsc+1Top8t6CGtJ/W5xqVPdf4KvFfU7rGIemTh6bwuHlCK60HtuUu+8BVV+53zWNHzJq9e+NtrNm2czzzth9vvO66B9fPPqK9bVDZTEvSPc0f/+L8p1b9u6/3dz9569j2NjM3cWosPTysjIiELSX3C81r+7uzK+20vUFK3W9Km0ypMkRqs9BWp4S9TSt7lSC11TByZAilTWlr07AjBN5MrDZFIiyMCJRpsBaStDTBJIllfisNCwILMeJ8inzsv/xnWEYiNNQ+M/Yg8J1pvb2pjl7LjjyU7m7pHciY8+PNw7uaLcN/uXnNvC3rUzHb0pSAqY7r6Egt3d7d+t3Tvrg53mT+vVD+r6t6V1xc+k0iFpe3n3zx9COWPfrRG958/s6rzRg9BCJREp8ZL+bDNSfC/CmQ/zI9qhg85RoKRemiabdQaJEWlS99rhibTu5UinwmP1dugizO2aPW7k2ZKTd/+71/ueDkM9f/5e4ndC7Lx2jFTW88P/guayqePHOfqdtWbUvFW5ujqn/Qsp6/r2fRc/dtO8K2kDv58Gmrtw9nG5TSEoJEY1skLSQP5LK6jwhDrJESpHNErARxVgjFBO6PmDzVkGxIyQTFm5TSq0wTMCRYmqylJGEYREKSMmQB/DLfeUkgsCBiUDcJShd2PgwLoF9GBa3FcHT/d7Wued8Fs5cstXqb4o2mZUjB2WHL2KelNX3Tvetnfv/zy7/6ymN9X1m3PN33vXMufuixt5ZO+dYX/nNmJC6XL97jmt5b//XL+GCP/Z6ReadlyqTw5wjdEQatnLd/y8eeeeTMX59/zF7bgWY+74pvL1z/ZvpMbfN07/XhY8M115oJhmubX4jdwFS6CsVrI2Tl5hYX8FLZbPkQfgCVb4UszweU9a+XjUckMBQzdvvrff/Z9tHzFy89+sLo0jtuXrW3VligFUaE4KP7z9nyYk9vk2yA/s0nDr33zqc2mEse6Pnk6k1DdPApHW+3tpi55JBtQmshpSBpEKCRYVsPKo3tkjBsRHQLMfeZJjeaJkdJcIYI3WBeFYmAhKEhDWhTkiSDO0lQvxlBkyTSJAApiQvg10KQIIFeoFQANCAECEIS+q2cETUNrcDU32tHz56wa9dFn3jy5Cdv23ZdJqX2AEMO9dmzn9+4fNvffrLxZ92bs2dGEvK28941advzm1+0335l8MPEFCnAnRw0RhfAbzAh0zol8p0v/2nfL/z6E6etiCKu/2/5kgnnnXrXt1e9MvgLrTAbcEwgGgEt10YLxiANxaelWbAAgawAAjrCHosbS7a1+DjChColEeUJMZRtjvFYi+oUzlJhI+diaR4eUsc/v+nlhy479pMb1uGVl1a+MnAaMxq1QvOK5wff9cbg0PLLj9tl9QZkIm8M98cvPGvuil6VXfLMnds+9ditnWdYijcdfFT7JrNB6OFhZZBmGIawjRhFYg16ZrSBppomDCkZwqCcIGwX4G1E3CMNNoTMO7qmKUwIrNdMqyUQNSI0GQSWEgKAYEEk8qtMTQJ6AEq5CYBtaTJMwTaUtNJMbc2J3l/e8Fb8+Xu6fgegtZDcIq3QsvGt9Gna5glCkM5l1dJLL/z6y5MOfc26+7r1h2uN2cwjAqALlEiCSJgRevbAk9ovfuqWK+5aNElaALDopD9d8M//2/SHXFqfCMAovF74Zu25mj/AdSutYACUKGSC6xoJClMTJFymRbtlhKtZAeFdF+RmBVzXouYtjiaCEILWfP2efY95f/tvt5/6yeNPXvNa+g4GWwBFpEl9R5898TO/++z+/76ta1N7JCb0Li0tw6sHBhp+9JE3v9DXZZ3XNsVcceolM24+7JSOjRFI1dWXmWVbUFJghiBqYMDW0BIaShAJXWhvEZI0C7ABksy8QTM6hdAsDRklyQkIoaXDeyOJQssjDQlQRoNNDdVnAmtssBGF1NMQz21EKmplOW1pc+g9DYf373H0z2+zsvoU1qzAkKNZXORrOYgMw6Sn/rLkuPMOxdUDB5161CXbNmV/pTVbhfY0mb/OvKZ1cvT//u/Ro/50KA4bBlrVWV/43v5LH+z5XjaLY8AMZlacr+imkgywI4vsmgEuB7J2zwC7uonsH1Xh0ALgVRJRTQCq+AElWpjKNLObH+AhAE5aI4hcqI6LFSD3HWFEUEKQjMTo9v889oGPAnvZh5719c/3b7e+y8wWmAwhKbXXIU3fvv2aI++6vWdDu7KZWidHrRMxvf+8rz51xrKnB75LRIkJk8zXFh7b+u8T3zeNZASDKocBSJ4GTUxiZPpfsdyDJRGDOEKaO5Xmt4WEhJQgIbSA1vn9Xo49XxgpW5CFzajEgE3gtySklRocjjx26/YZZ106Z0lvN9TZE6/o3/u4c2/MpNW5xUlvrJ3hx5FADzFz5sQPTT/sD5/72ttn/c83F7308PaHAEQKCuetqXPiPz798nlPfn3xyduABH/kp3/c5fG/bL4qk1InAdTAugBvZuEQsJH/jZRFcGn2eMwlEOxhPrjyJcJPYMJHaOBSUewX1mTPtDI7LwyHM3HsPC638CdXsZ4MqTWrXIbPPfyc2z4EbBUP3XnMbzumxz5DIBNgxZoTr7+QvOaMy5++8PT2KX1zJjcNb9+cidzes7bj2z9Y+PB5X5h+oRGhtX1d1r6P3Nj5sUdu3jJ3UiJGJDHRlGQbUbCUeUfXkGDDJBWR+RgJMa9lA2tEREpICSGIDckkJYz8L0kpSVJ+wYkhwOao7iDNoIgGTWiBtNYszzUte2agcQEah8+eOCd72NkXXpZN63NZsw0UNH8Z8Apl44pIRFa80L8nMEOd8clZa4iwWQha3z41+uUv37D3SUvuvvbWIvjPu/JX+zx8/abrsmn9HjAaCsIlmFl45rlCJsACpwA4OGQp0dbI/pEdbxpEruagYAWEhx8wRhpUQmsC0CDfEmkfKwCAhYAGgw88tePov37z3W8Ae9lHnf+ti7s3Z36dn2BLICLZ3G7cf+GXp//vJw+fv+murq1tRUq0oqsvceN3152+fnn6fWZUzP7qLXv/obHFyCorP9FBjLZoggimVpxmYA0zDQhJ0rnRURaqOt1DnaUdX/lSCJUEaLUA507E/E1PYEvs1VcHY7+69JXlABoKYCcX7V9UtBZAZqRB/G3l85dd+gCWJW76ztoZey9u7/nKkX/cAvw8BsR40ak3v7+3M3tKNq0WA9TAakSwyLcAzoP+FF/jTn9KTEE4+uMs3isRgNZGdmEt9fUDQlSGjoUGOcEs3EBeNSJUsShbE4GEwMbjzp/xnh99do8NzTghc+R53/pQ9+bhX4DBJIhAJKVB2/Y4sPGa26858q47Bza2AUCixVQnYuLgrdvWT7z+yvWLDzixfc+Tzp/Wk8ralIc3cSHBZIO5W4G6wUiRZLM4wkRACwYJytf3M0OzHG1uZq/sJoMEQ60iUPeGVfbQxxYcMnjSJTefsPr1wb+yhlHg8U48MZh14QMlIT+G14jgsa/dst+H5s5vsXZFqz0Np6V/vfRnE5f8q3vis3dt+3Impc8FM1gzmDEym86tApRLeE6ICtB68H83AWCHAFRkxcLkA1ykIUw+oGQpBVHlgIVyZ1h4RIpCWIEwQpBvOuGNx50/48y8ENyYPPiMWV9N9qlvaa1zRJAASRBh1wMav3ntL/b9x3JrKA4A6UFbNrdH7BMxvf+2rvVzm9vN07RgIw9wSAHKaNbrCdjOiiIkmRiUTydJNqREWoFSAiQK/D4OAFoxCQjhnNUm8lOyDAHuEUBSQw5rcP+2jWrg4pmX9R113mffv3X98O9Zs2KGLCBBFxxfg4oLA5j7Yo3mHbN2j9/1qz8ctWQzhuRAOiuicck/OO+Vo9e9mfqmsvQCIjK11lz4DFEcXVECfkdZcgn/16W8uBr/Zy7T/gjD/0s/0DlHSdSlIYWrx2i5jINzRXloiGwGe58fu5lRrrx2YPeLxmW2mBlCa1bMNPPx2zbd/eVfrpgFfC7+ke/M+5M08BQRRViDmFmzZrVy6dB33n/hi1ccabYls2klWtoNZVnA3emNrR2TI9uEwRuloBgJ2CSwAYJfgUSfkBQRkbzWV5aS0Qhxg5RbGLQmClrDsNYSeJ2CXjk8kHtrkoyuNmQh7zrqfwkC5wSoU4C6BThnQBVWo3bwYG92YeEUVeGdDCJBRAbAw5EY3T1pTvTjZ3x69tErnr788w/+YdVDC3BcuhVRDQAnY0pmw8rU57XiPQEYBSeXAEjXGh/XxH1lJj9Y+HMsGWD2fFqM9Qt4rOlorl7gwD7OMFeoCneHnF1sZTnv9BECqdSoEFx1/8uTPr7fE1sfevy0c5raje/mR/znu+CZWQ0n1dwoBCtorSEsw4SMx0V7VvHeDJliYBuBVgO0CSApQZJBrHNMcWlYB8Tau3p6c+k3l/UPNsJMa9gRCSnyNftkn9UypefpJdvmdXfmGiNSKJRQIkoxtJWFHQEs5MC5SINk4O5IJq0OBIOIKUKCJIFICKycMNH8xmGnT3nXm8994/1L7tjwp19c8uLKLVgdveib+yza7YgvXXvOwQ//piUe1cAp2Y6psT8XLp1isPBWROwtBx7KjrlewAuOO8PtM2kkS8CVNIhLF0JTDc+58zPnpjkaaUVnt8Q0Oz6OvDIcxY8qDAdEyceO2GomGjlN5ykXxoGP3hkiqRTbUtLMv/9ozR/eeGafb/7PD/Z+88W7N3x/r+OmH6UVjtWWZs0gO6cQhWSSogkQCUC3KAgtJUUZnBHgNxlgAZj5oyNtZbScFYsP33zL+n2vuKXzXbEELX3X+6bcfcBCDA5agLKUiMcNNQfx3OIPPv2ZTEotnrNP09Kjzu54dY+FE3oySkuSJBS0aRbGIdowVbKX9IUdJwwe98GfvVspcYhmbZkReq5tesMtiYSx/bwr5rx46V5/2AbMYeArscuvO2jOv+/bdmj3hswHcll9GEAmg/GbLyz/+dk3v/eZvRdP24Z1bIFHG2OYfawyB7EE/laDg8pBDcWhhhMsY5rwwiWSUwbqMlxy6U7YIAKUb+OlUmA6Qhajz3F+xS05sesSaSUXISif/Z/fil6UCE1CGAysbJ8e/cuK53s/e93/rvzBrC9cpL74h70/+NPLln9FC7qwfWr07QNPaX/iaQzsG2uItuSjPaRHbAkAhhBaMUEBFMlHbaIxycs7B1Vq0F4TbxaNl/9i95vmtzelNllDUWUxzY43ZXZDLHvkmU/8b7LHPu38K2ZfdfKZ01b1qVw0kx+Zm487gnsUWAtIGQXnIlFpb8Gy2Ky9GtdaWfXx6XMbVvzhh4uXN+NXg62To9Yjf7qx4aaeT3Rcf/mqwzevTr0vO6yOAlMrF4FNnAOTsXxp30knXTrPmrv7hOWrXxlQDIoULAFVanYOSEDC0h+uzQj4ZJspMWHUCa4I3gQIh5Y4zzS24rjSDY3hMsPOMCYRlfUDjPYMu0aFfJxih2OsSJCMJeSfNy7b9qEnu8+fdP+DG5unHdg43BI3xIKG+Oz/dPXsuevk5kEbJLpVpsGUsBVc9oOrfNtiAhGVRDZmKx5qkOhc8/Zwbu78WHoxZiWfxOaWlGVJAJhixnN///v6Off+buuPc2m9mzSQuvBLc344YaqZnjovPtTaFsnYigVLEgb0aoYeEBAmg/sFmgaH0ml5ZnxuGvj5YP4Afh3/5G+um/faE70HpAatGYPb7Q+yjQVcDIUylGYe7akHiY6p0S+98vBJ1wN/HJqzcOKjOYuPhNOZ9nN+nbQzjPPrDJW6eL06iPPrEf0pPmC4afEKZR5U+ftRJxfzWGoFyr88gBUoGhk3vsU0Sp+cVMhJdeDOvMqpV0FvE+WNhT7nir0P3Bcnrtj3pKMGgaclMIEfwh8PaZscVZtzmQYJwIyQrV3Ym1ZMphTYuiGTeOulztb9j2v7T3trZGsqZ9tz5jeIDJR558DqjkSLqQxTsG1psqAp2iDtRLN8wzTEutl7xXvPOH362i7kIgzOT3qTAEPZGmwxhFAAsrCtKQCGAFz94KsdLzy4+/69m605vZ2Zw4eHrHOZaULxBLUuTPNiCMZIpLV4n2iwL3v0URc8tn7L25PPtW2ela98HhlYV6n92U/7B+wbZv9M7ljbeEctACprd6iWsoggSbHAVoDKngtQH1Su5UuSbIXq0nItX75lprwCtfT0WUgiaGTMBvEoCcq2Tok+prLo/+gPd+mfuWvDFEtBlY8mKcpWoUlFC3CmZ7O1YeVLveZ+J7SmmuIR1lAik2ayLU2CmDQXGtzNfCJ1drwpsxDNw1koWotMy9vZ5DRhUKOUIpqXZy0A3RsBrVbQJiOW2box13fxzMv6DjjlU58f7Ml9USluICBaTHpxIYbEDEJh+iKXObCjkx1GtU/BSrgEDqqEPsOUPnglv8KGP515BpfwqqcAlGK/Og2qZD8BaRCCJ8ZGewKqF8l5CYGg0lR2hRCWf2flcm1NBEHFThQCtCJMmRN95tq/HfjrzlymIR/OLIKeSOfDkwRQN0GvIzAZIG6EQf2wZTathG1p2qWlZXg3xLJRNKgsciIKIAtFr2KoocfKmMrKC4iC1s0tERsgqUCNEqJNg1sY3COhN9pgg3NkaxaZ/aJtyROOeOieXIYPYM0MjCS7RMkkxyKtcAd/ETCK86pfMkDlFGcE/G6JryqZX//Yf6Vl0bWA3yXcbvjzmaC8p8oLyl5b4gyTCwUbebw0IlQWonLhSKMOcQXdotEbPCIEjmgS02gRTGGAaElEyGmgmKFZa5UfpkgMzbJnS+agVV3JlomTo8PDis1CwZrKR32QtKEHCZwWkNIAIWMp0TuYltEGqYva/YqbXtnn9WeHZg8PqlSkQQgjisZ4s6Cjzpr4yp4HNw1khiFjcWm3mA32oJU1M2lNZGIoaohBGRERCWILQhhgpog2slaueSNSDdIQYG0XwS+9ojdu4C8lMSwrhw64xLM52CQPF1zX1/kNEAYy/PA6Suc90O6GcRc+zqDSj3ADbhBfgEc3D1KZr1ISLi0RpDJ/oHhE5aFWlHxtvizAIQTFbqgC0yJwoZSgsBBG2Zxc+mjfjHMvmvF6SuWyUnI/gZMaepggdRRa6Py0ckqnlZgUb8id2t4ysALp6E3/WDP3c/f0LN60MvNRMBoh8pZM2xqxJuNPsYTx0m4HtyVPbW7OvYVU4umXejsOP7Cj94CWyekkUsY6DEU2I2vJwloOAGBLcJNpWq++0tU8nLR2L9xF4Qp+LsuZcBltcQMzVyY42Y/7cxXu7xr65GBFcT4JWq5pSV6NIVFfZ7jcCjhWbAa2AiXPuzjEI9rc4fA6hABOp5Z8hYAIkMUQqbOFiSQNAJDSpO3xZvnmhDbzzXn7Nb128Ekda2JxkgPIDZkRKAHWApqURTIznIsIQ+poHDqXtuSCeMvw7fdtnP79h5fvu3556vThpDqQQdH89dAZAvXHEvLeQ8+Y+rvff/rDK4GNEgBO/NgdizetTP6PleV9fxcXKxtbzeXTd2lYuvuipjWHnt7RKeL5pnaCpuQwG4eZHf0PrN40vXAn2A/87BKn53IB8aAurmqcyyM5PtqfA09Gq7V21HNukREI1OyTcEKNVsAvF1b4AKdweGp7N0tUjPoUP62YH9Cj4VEPIWACyDDRlZhgLoXmSCQuOqftklhqWzrSOiW6ffF5HW+mk7YxZW58+Nz2BT1Z5EQvsnINBqMWgJRlS20pqW1lWhYwsb0hd6g5fXAjhswXe3oaD27v6P3Rr97c48m/bv2z1oUoTJ4/63izcd3C4yZet8vuzX1XnbVPNxBhIEV3p1ckvn7qiz8dHtbnFzn5cFItGk6qRUO91p4TZzb8tL87a06bnchYALJZHp7ZHFG/e3p1xxO3bj9dMxqK3Vh+4Gd2B4wb+N1q6920P3P4yE8o7T/GQoRRJ9hnI/vYnGFHgZzLHArviFBlqXTJ86KKQ4xgTvFI0Vz+KQVAtkw0H1j26Pc/vBWPtbYhOlJqUAS6AcFDsOT2rkxE2UzSIC7+trXErEmIWfPQnIsioh/C+parP/D6xaYp3nro+jW/+3PPaVNSWy3xj/9dc+jmlalvC4EBMyFemjEv/sB9v7n8SWCVAZyfBd4Wv37jho4/f2X1BX3dmctY09xig0lhTrXNGubeRzZ/+Z5fXHzLvelnp8TixIRIdrBbbD974sLULgf+5gGt6bCRxpcyh7dc81c4vRjJC3iUi/hHfcod350V+fHLJhsYq79bJytQkRcYGxXSRND54pgRp7hYfk+sR4WAC9ZCOBwArdhcj0fanlq/ZYI0RlcoFEEOAM3tEXvh5LahPRDPFp7lLHLi3p7O1vse2jLzjWf79upcmzl4qN9eZNuYyZrVoefM0S/84z233N2wNv7oDd+8/+evXvficfvNTO+L7w4C9xlAM/+95+3Ejy4+7zPDQ/b+VoYXasYuyLuvemSfQ77s2DRj4u0Pf2feE/dbz02OxqW2oURmMJc9e+KczEXf+cdeSuHAAkJlOZfX5XF1N6e3CvirUh/2dAPG3uceQvv7vc+omqyicAmuWn0B9/ezMzzjSYVchEAwkSgPvxbHd5TToUIjLEhAEROIKDcbc1K7zk6bh2DiECAdl00RIPlZbGn+/R9X7brmP6n5navTB2QzepJWnEgP6QNH5mZTPu4iJHWaDXJpLEZDqzBodq1NG0/sdW/TcfvNTC99e1t03bTL256+o3PCQzdsujw9ZJ8DUHtJrJ4LaydLL5KOxeU6AIiaUmehRAwyt3E7ZdD8m8FXH552GYFMzfme3xItyuwK0grND3/ws2eHnYfjG5j7czDuX2PlsL8AVA/4BI96VtQZuVuB8hohlAndaG1PIaJDnv4AM0DRmHhACAwzKCIEFBNMK6MOBiPKJBrL6/4d75dMgJ3VU3/z8tPzVr462PH155btozWbpkG5bRszB2SH9RSSsLJDajfb5jbWhS0pRCQl3jBj4p7mjshj0agxpMFyxtzYyr2PnLj1G6cdtg34YRr4S2TBXkh/4+7fzZiyIJr51J7n9Nydfqh59oKW4Zb2bS9bOZ6ZTauTC7zMKjSYkJNGMyCEge0zd294eOnjAx1nvWfGpi1WMpYcNjOXzju354hzZ38knbQvYl2Y2uDh7JYC2wX8Xk6vF+/3oT4IUPPDXuXBobQ/hxq8XOkDuI1A2dG+AKrXCXn4AxqA2m9x+6kv3rP+kZ7OK5uBmQxsF1/9691T+7szkZce7jlEMwsiYmYmQaRZ60jvdut4YjZAYCnIlgaltYZh5fQkMEQhvigiDfKlhkaxQQiBRJux5aT3zXolOZQzAODD5+zeswDXDzjO2Mr/uc4Enpbvv+qafVYsGTgxPWQdksuoI4SgzrkLE1c+9IfTHwc+lwHmKAB0xLlzL+7amPkKK56X59cjWdtCbQ7Lplbj8WWPXf7BJ/HcxJRlScMUnOzj7ee0Ht2/y6KfPqls7MsO7e8N/vLHy8Cv3RxkL97vn/H11f514f5u0St/wXEXAA9nuKJjyu31XkVyNTjEFUJHHpq7uKMaIEEYfNel846bPCOSPv2iGf3H4pghIEXAl3IAuG1KLNfbmRH5Gcjll+QvJrC6UMiwTeajMIssYELhdafnnF5P25SY3duZiY4asefFtUu+NWXNm/3xluao9eqz3dO2b8jt0r8te5Bt84xcWi0mQQ0jdU2FczIieLJtSsMtM/eML//b9y5+DUjwd//5t6kP/HnjMd0b0++3svpwEmQWcrgWAPPgk1s/9f2rD3jyPwO9icYWqZMpYZ2dOGX1uz768+PefnHgzgI4ZUWIswbwl5YScEVvBbuB380R9aE+rh1fKNLToPNk2TOSNTYBqLsVcC+R8Gye9yqTcI/+sCAMQQhtGFhhmLSVmQwIULxRvNzYbG6atGvi7f2PaN861G8Z+dWjxEoxnf+xXbYfilOHgK0CSHBecEbLgL58453TcqxFJCp0sjdnvvJIz8Fas5SmyKUH7DmZlFqYy+q9WfMkAhQzmkiQUeZUFqJKJEYXdhdOTOvheIvx5zsfP/mbC3D9IADegouaP//VZbutWNJ3/tCAfZbWmNGQkC899vTxH3wp3d0YjUtN0NSzjfsumPTJrbsfcdlvM0n1Qc1aMUYrNQOB38k+PMDv6RNUifp4Nrtz+WfsOO0fWAAq52kGtwL1pUJVQqPl9ULO4jenZWG2SWAIoGJVBIOITBNvSENuZbA5mlIFEZFmsGln+RDON4IwiCUV/AmUZZlROSJ8tCrb3ZXShV+DBEEa+Hdjh3nniefPuPtHHzx7U956raNP/+rcBU/e3fmJEy6Y9OjHLlnw1lorGSOTiXPRzC6R+Na/Pby59Y9fWPEyMzexW62+W7eWI8nl1sTiq/kDhjxroj5OgQmr/Tl4W6W3APhYgZqokJcVqLc/UCi7xKhPwM6JJ4XnpfMNrrNIy4ZtOVx456mpsnNlUKFcAnD7f4BV3tAkiiNRuC/eZN4xc/fEnQ/8/uxnABIZbG15CdvbBwvjDgmaOjeq/g/MfHD9/ifO+GLP1tx3CuMLzQpA+1Aeb83uofnLeb9HnX+1qA+XZ9e8tH+1Ufp+USMO6wSPxQqMExVyo19VhKB01k+pQHFlxScVNXGpwJQen0CFfJD38mxCCOyX6sVCAFiC8kstG5uN333xhn1+8OH5h6U2Y03HegwkUrCknTNy8yJTt/zmt0un3PW79fczYyZzYc7DyNQhNz/AJdJTK/iD8P56UJ+A2j9M1EhGopGrfO+Q2/2tgQqhGhVyAY/rUF1UFu24CgFcBl45DAhGp607+sMgKD+/XhQqp0Vx1n0xHFkcU+SM9JbsPvbcJO9zbd3jcKKQ8tIAyMrxQU/dvvXMX1+/dN/Hnu0cnHVAY38iborccG4wHktkO3t7DRvy4cHu3LCyuUVrdLiBn90aVnzA70173MEfpA3RN+aPsOB3L0gNGgelREsjBxWA0FbAjwrVyR+oZgnKewgqpr9XDOqqnHBNVKnSibyvV9nCqEDXtXqPeL6coVgGG282f/Xmsxd+9wlsEGvfHjA/PP+MPuBTWeAvkV8vvaHjrl+v3/utlwa/py3sy3kkC4a31q8X+MNTn0rBcaU+deb+pXOBAuef3TY9hqzPY08GV71akMsvDFdGIthduTCP2tyKaQXOCsmRhFFpJrT8hrH2nlla7K3VhVFpOv+Jjv/Kvm/kpo/+V3yfLnRgcb6cQTFzjgTWHXF2x/UDSFvdPWn53fNe/fOuh3zv9yd8dJcTXsNDiU8d8NiWh6+79Blt8RRmpvx+rtJhsih3dp0Z3h0Cfu9SiDBZXw6A0+oWoJrJrkJlKIQV8IsKuXRf1ccSBPELPKxB+ev9rILXbpE6/iiA5NyFjec+9dc1/7xxzQmTv/GeV+9XCnsV0Scj4vmWduMuKUVm24bhn3Ix8hRA64+UN5S1XI0P+Mca9ani+AZU7KMCMAYhGG8qVLMQuADcVQhcjpPchNdXENwdprEKhMP6KSKSkZi4Z/XLp30YmGEffdGNJ6x5NXk7M9sOv4E8IzweARcvZ7cSxGMAf5WQ51ipT+j9wRyiGtSzGKi88ypwrA8uHWI8WulbuUekso2SyytHCz0EuigEoz0FKKsdGvmgkc1Io11m5ReTXLagO32EcoCUziAidyBQ2MItLlREcObYi6ZetQVSpLAhun558oujJ5rP+iI/9pALNUDEfu2J5Vrfh/JUVIz6gR9jBH8N1CdUm6RnOXRwDFcUvHm+1WXw1gi4XN/E7kLgbA8OKQQVrxvtWC8ZflXS+1t+Yck5SmX0BpY7zJ4CUWNT60gFqyCjZaL5veu+cMoa4IfJfRbP+JJt4VDWWjkL3lAsXa0GfGcjTBXKU9LVFQT8HKLMOXTI00F9eOzb8ozaUO/V0cXu/kAQIeDSf9QsBAVVXioEKK0irbAGIynfkpvpFISRymyHIJQLgxtN4rHOsAQUERnRBvGPR544/VdATh73vrmn93UOX856dBy5+/xTj2FT5ckqDgh+ZtfXVQV/LSFPBOD9GJv2H9tUCM+y5yr11OMtBI7umpKaf8ewLLhYg3JaVCEI5a9HpTAUG3RK2U6N2TAuJMQIgojX/PSpQz87Dbtkz7nixoVvvTx4RwH4+TMNOmzWg+6EoTx1BT8H7WJBoKHK4SOaxQGnY7ACgalQUKNSJkQMhBKCiuG+XGiU8aJEZReTHCD2FYSyfh3POb3Etc7yyB+shjrktKkfPDN+RPLu9LNNS+7bemNhkJWCS7GbH/AxRvCzaxi8NvD7cvcdQH2KLzaCjXYePypUzSkuKjhXIUDZZAnX0Y4Ov4BKZyJWWAOnVfIRBFdhQMkGXYRNCrtcKQ0Ak+bEP/+Paz7x2iq83PC5w5/7pbJpHphHeb8P1WL2Gw/vDvwK4FYDv08uBGGc3jAhzzpQn9JtOmOUJreCJa7mvrvxQAa87hhXjANzWYjBHoswCk8wlzlzjlU9FfeLy9f8cFm/bOlrKk6GR2HsfG3AX8Ug2dBkXv/qgxv/AABnHnn/pzJpPgf5kKf0PgZ2HK/jeV02u4rdtT47u9arlEvsOPCPkfr4INYobcmtHxUK1F9ZLTIUxBKU9RVXpUQl1qCSFpW8viwK5JRSZ+6jHAhUGT8Nl+wiMkxJ/37gpVO/Btwrj77gJ+/q67K/zg7wV20D9FA0qIPWHxfwB9kKXyv18Sn3NBjoJGCKK1pDUiEuBw9VmSYRWgh88gQeQlBBiQpTrpgdxXTOUdGOAjfy4vtUabKoTI34luB6XxINIikIm449b8qnFmDR8Aev+uqeq15J/pKZReluD/YuM3GjCh6ocwtv7hTwh+D9Y6M+7HQtOwUBSzBaCly/VTRchQr5LDDwpkOOdJR2j3owvCkRu9ArV1pUyQJKn3bSGpTSJOdvtT21buvLCJxceEzbh2+86ksrVuHlhkdv2/prVjyLmXV+4TSXhDErKE75tWT3FYqj583eM/RdnF1Xyqd3IvhrkwZdKGRfYjBzk7NGuG5UKIxTjDCWgEqnS8BtQG5Qa1BKi0brgEpXybBbbb9HJKikOyxsEiDfejCYHrQS7VM/lmqe2vxz2+KDC7zf4ADA4SpjFCoEYoxav3LhYZ3BjzGAv0qFLTM3yUjUnAmixQUCIPyLf0LWCiFk0Vy1wju4F+54liKRx0e7heYJFRJYtc6n1pl85FpVUtzY1Lx9c/aCprbY7NSgunhkJhA74v21gj4g8KuGOF0pD+oI/jo4vT5v4OJ2TObrDKHFG7ow6Z39rMBY/AEaD0swiibmwkPCY5memzWAm9PrmILLjjoiclkwVu4gu+Uzwk9AG7FVqaT+cPgSYPZeUueCJD/g+2ax3fg+UEfwV3F6xzAX3XHYWmjxBjU0NEyDQW+QoBa4LmWk2rqc3LQuwb9VhLy/1qtrrKK/mAJaAwrQulj2peT12T7WksZU+lwyki5Y1sglLOpJJTyH4frU11ejPOMA/rprf4BY8wBs3lM0NzcPgLA50EfzGIWQQ9g+rswKVazjdHGORxxC+DdilMTP2W8EB1c4zCPNMOzh1nJlPqHit/xdla8pHWbLXu4zKo+rvNMG/g6u17Q410af8vP1CESEBT92DPgdcWxsbm5uHhBdXV0pAXqR8h6irrqPI+QBsK8GCTPVNEjCrCxp5lEmUJsgoCKsxI7OMF+BcI35lP0GeQ/K3loAPdxAXwPwPR1dtyiPG9+vVpfPAYrcxhf8AEMTCAL0Yh77eVv7rypschyEgINZi6BCUCYIvtYgpCAEEQZwpSZ2xXZQS1kuI+WWJyDoawZ+Na3vwfd3GvhDEpci5gkAmpqa2m2oVUTUOjJ93pXD1skfCOoTwH/CBPn6DlQZJfL7GqryWJBTJ4y9Cii0Rgk2QJarVYoG4fo+tf6M8JWddeP8wbV/nv8z9xmQC5LJZI8AEEkmkz0EPEr5ti5Vj7HTQS2BxzQ19zoRN78grDXgYBahotbGneJ7WAcXYo+Qv34Og8fXVbAlDq/xR5J8QbR+tdGF/I4Cf7G3ggl4NJlM9gCIjBTDaaJ/VERBx0qF6i0EQSkR3J3GMILACCYMbgLBIdK/fnS/2ttdXQQ30I8F+FWiPLVRnh0M/jKzXMD6SEBbAEBra2tT1s6+AqI5hfcKb7MfMkkWhA5VS5ZVSZhVD5f6JNACJPl85wVQyKKfepDYKlQm0MQaV3xXpzuokfLsNPDzyPxVAvO6qBHdv6+vLwlHObTR19c3AMJdlK/q0v4LDcbBEqAG59jPGnCVeu2AFqEi8uJnNdjdmR7zL7zDqW5RW+YAnVF+Gr8K1/ed1lzN2d0B4Pd4WFN+rt9dfX19A4VemJFmQAFAR1ta5kq2lwFoqPQi30GWoK7WoAaLEMI6jOdPyLlkHs5tlZqDOmh9txk+Y472BOf9TjEdVmQszA4MrC1iXmI0/W6obLbHjJgzSIiDC5lIUb3+ZScKQQBBQA2CAIZ7oR2CnxuNB9hrCQeyR3a4XsB/54O/6PxKMF+XGRy6CfnJ2apc3xWtwBzJ9n8AxFA+z34chaDmMGkYa+ArCD5WoRbLEPb8uc7Swn6smKuCOdCcnkCbGtmTro4b+Csf1gAyiox9sgMD60ZXTRWGJzleWLQCM12tgKcQ1NgIG/RtdbAG7Kf4Ue1JF8swHmp+rObBV9NX6UQPA/ygWt+rD3nHgX9krAyYry/X/m56TQBAQ1vDVNjiRQImOnwEhPIH6mkJahWCYJucQliFimEngWlY/bmQF7B8kLoDgB+K8ow/+IslZN0w9EHDvcNbHY+j3AIU3y7sYbvfiEY2CaLzAFT2odZbCEL4BYEoUQBQcnCsBzgZKi2r5nECPgeMdAcEfYCVW+GBX60feceBHwBsIjI08NFMf/r5Ao5LimOkBxs17ay1zIgYewkh9ikIgdjRQjBmSlSrIISmSMFOiGq932GREmBUeD2B76f1dzL4Ta313zPJ9FXl1KeaaycAID4xPhEZehaEuSPJsZ0tBLVYgzDRotDCsDOdgeCaflyBv0O0fijwF5JeWIsYH5HuTneXU58gsQ0DgBVrbDxaCn6ssK7VCF4fV18h8PcNQnx4QL5OVSxQuHOjcUgBh5ukXGsJ+jtD64cCPwOwCRBK0+LM0NBTBe1vu32q9Lk7GkDEzuXWyEhkSAg6pbCkWQbaIzaWEGlYaxCWFtWIz5r1/Djgv4a+ovoCP4zW33HgBwCLiCKKcUVmKHUrgIgX+KsJQPErpJ2zXjCixjxBYv93ghD40XHfZR1jBCuPY7nPWEATCsc1gt5zlVW9KE8dwa9Z35RJpq8s4FT5faUMeg/trHWHETXm100Ixt0ajJ8gIPxCnXGVB667lNQB+GMGv0/1mTf4Tc36puHB9AccFQ5cDwEgAMLOWv94pwmBnyAQjXE87Y6kSfU1CjXF7z1zuTsU+HUBvwx6JDLkPSsXgpz7rMpxco5rpUU1hTDrj3KqN8jrquWrAD/oOqIdD/5ckfY4wB84GyNruJ3lQqCCKPwdZQ12iCC8k8ohuF61RBxocQXXS2rHDn6NfImzWSv44dDeoXWwnbXukBEzSYSTC++3Xfc7jqcQBBEECvI41WncyzgLBYeJbSJ08swLk3XT+AElKIDWL1YnCM34QiaZ/pIDe6FsEI0xUq8TLYnjWOM6EjSX84sbRLhe+vpslKhZEOpNj8bKgxjYYWajypYWrvdxjl3rc0HrS9a8lgQuSQ2kHq8B/FxuAWqFX8TKWqtlQt5BCpNJiH0d1qA0yVZ1qsLOEQR6R2Z4d1zWmHca8EOBn4uT8ohIsNZ/1YZ+/3D/8GuFOL+q9Wjq0c0qi4mGRHPifQCuBtHMwmRku8LP2BHWIMD7iMJONaH/VwB+h2r8WrR+JRtSAAzK76zaCODK1GDqZoxWK6ixHI2sU9BCAJBW1npNxuXtpIQBwt5EFHNYBFRvsQwa66wj5XBpadzxc392wtygoGtc6wr8wFpfOzU+gBSYfq8NdcnwwPAzjt12eqxHRHUO+I1Yg0hT064G+DMg/jARJQoWQY0U21E1/VpnQQiYuQpd2k87ORXGNaYEdjLwXcDvnOMniQjMnALTDTbo2lwyubJGre97VDQOke9iA01REHYzSF0G0LuJaFfH4ghVmEJXdJqJas5+1V8QAvbW7JisGI/t5YF3cvF4HSd79+BwoXKzAPoCPlYCfJ/N8ve5ZPItB/B1PTIN4xB5h9cGSoF8xhiTJyMxNJw4jZnPYeB4IURr2UAs7VjPhdJoEo0fwEJ4QTuy8avuOTAex6hTdeCPaHcuVZIjREBr3UfAI0T0j8aG1D+7upAqvNcsxvzHQ23QOOs0cghCrvhgU1NTu0X6eALeDfAiYpoCgbaSVU3MO8YajFEY3tF5sPEEfdisGDkW3DADGr1M3AnQywzcZ7J4pDCusPgTcQCfx8tm1jnQWLW0vpipsxzPR+PxeJs26CgBns9ERxXo0SLhtb0y6PS4nWAZxuNwQuF4vEEfrhZiZBsjgJcBSGJ+WoPeFjY/nU6newFkHa83C5dMjSHHHer1/w9RJ6CkUnQcKAAAAABJRU5ErkJggg==";

// web/icon-512.png
var icon_512_default = "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";

// src/index.mjs
var PREFIX = "/cmail";
var PUBLIC_HOSTNAME = "cmail.frg.earth";
var securityHeaders = {
  "Cache-Control": "private, no-store",
  "X-Content-Type-Options": "nosniff",
  "Referrer-Policy": "no-referrer",
  "X-Frame-Options": "DENY",
  "Content-Security-Policy": "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; font-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
};
function response(body, type = "application/json; charset=utf-8", status = 200, extra = {}) {
  return new Response(body, { status, headers: { ...securityHeaders, "Content-Type": type, ...extra } });
}
function json(value, status = 200) {
  return response(JSON.stringify(value), void 0, status);
}
function bytes(base64) {
  return Uint8Array.from(atob(base64), (character) => character.charCodeAt(0));
}
function createWorker(checkAccess = authorize) {
  return {
    email: legacy_worker_default.email,
    async fetch(request, env, context) {
      const url = new URL(request.url);
      const publicHostname = url.hostname === PUBLIC_HOSTNAME;
      const publicRoot = publicHostname && url.pathname === "/";
      if (!publicRoot && url.pathname !== PREFIX && !url.pathname.startsWith(`${PREFIX}/`)) {
        return legacy_worker_default.fetch(request, env, context);
      }
      const access = await checkAccess(request, env);
      if (!access.ok) return json({ error: access.error }, access.status);
      const readStatusMatch = url.pathname.match(/^\/cmail\/api\/messages\/(mail_\d{16})\/read-status$/);
      const bulkAction = url.pathname === `${PREFIX}/api/messages/actions`;
      if (readStatusMatch || bulkAction) {
        if (request.method !== "POST") return json({ error: "method_not_allowed" }, 405);
        if (request.headers.get("Origin") !== url.origin || request.headers.get("Sec-Fetch-Site") === "cross-site") {
          return json({ error: "invalid_origin" }, 403);
        }
        if (request.headers.get("Content-Type")?.split(";")[0].trim().toLowerCase() !== "application/json") {
          return json({ error: "invalid_request" }, 400);
        }
        let body;
        try {
          const text = await request.text();
          if (text.length > (bulkAction ? 8192 : 128)) return json({ error: "invalid_request" }, 400);
          body = JSON.parse(text);
        } catch {
          return json({ error: "invalid_request" }, 400);
        }
        if (readStatusMatch && typeof body?.read !== "boolean") return json({ error: "invalid_request" }, 400);
        try {
          const result = await env.MAIL_QUEUE.getByName("primary").fetch(`https://mail-queue.internal/archive/${bulkAction ? "actions" : "read-status"}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(bulkAction ? { ids: body?.ids, action: body?.action } : { id: readStatusMatch[1], read: body.read })
          });
          return json(await result.json(), result.status);
        } catch {
          return json({ error: "mail_state_unavailable" }, 503);
        }
      }
      if (request.method !== "GET") return json({ error: "read_only" }, 405);
      if (url.pathname === PREFIX) return response("", void 0, 302, { Location: `${PREFIX}/` });
      if (publicRoot || url.pathname === `${PREFIX}/`) {
        return response(publicHostname ? web_default.replace('href="/cmail/"', 'href="/"') : web_default, "text/html; charset=utf-8");
      }
      if (url.pathname === `${PREFIX}/style.css`) return response(style_default, "text/css; charset=utf-8");
      if (url.pathname === `${PREFIX}/app.js`) return response(app_default, "text/javascript; charset=utf-8");
      if (url.pathname === `${PREFIX}/icon-192.png`) return response(bytes(icon_192_default), "image/png");
      if (url.pathname === `${PREFIX}/icon-512.png`) return response(bytes(icon_512_default), "image/png");
      if (url.pathname === `${PREFIX}/manifest.webmanifest`) {
        const home = publicHostname ? "/" : `${PREFIX}/`;
        return response(JSON.stringify({
          id: home,
          name: "C-mail",
          short_name: "C-mail",
          description: "Your connected mail, available wherever you are.",
          start_url: home,
          scope: home,
          display: "standalone",
          background_color: "#0d1411",
          theme_color: "#0d1411",
          icons: [192, 512].map((size) => ({ src: `${PREFIX}/icon-${size}.png`, sizes: `${size}x${size}`, type: "image/png", purpose: "any" }))
        }), "application/manifest+json");
      }
      if (url.pathname === `${PREFIX}/api/session`) {
        const mailboxes = [...new Set(String(env.CMAIL_MAILBOXES || "").split(/[,\n]/).map((value) => value.trim().toLowerCase()).filter((value) => value.length <= 254 && /^[^\s<>@]+@[^\s<>@]+\.[^\s<>@]+$/.test(value)))];
        return json({ email: access.email, mailboxes, read_only: true });
      }
      let path;
      let parameters;
      let download = false;
      if (url.pathname === `${PREFIX}/api/thread-index`) {
        path = "/archive/thread-index";
        parameters = { cursor: url.searchParams.get("cursor") || "" };
      } else if (url.pathname === `${PREFIX}/api/messages`) {
        path = "/archive/list";
        parameters = Object.fromEntries(["query", "mailbox", "cursor", "after", "before", "limit", "view"].map((key) => [key, url.searchParams.get(key) || ""]));
      } else {
        const match = url.pathname.match(/^\/cmail\/api\/messages\/(mail_\d{16})(?:\/(raw|attachments\/\d+))?$/);
        if (!match) return json({ error: "not_found" }, 404);
        path = "/archive/message";
        parameters = { id: match[1] };
        if (match[2] === "raw") {
          parameters.raw = true;
          download = true;
        }
        if (match[2]?.startsWith("attachments/")) {
          parameters.attachment = Number(match[2].split("/")[1]);
          download = true;
        }
      }
      try {
        const queue = env.MAIL_QUEUE.getByName("primary");
        const result = await queue.fetch(`https://mail-queue.internal${path}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(parameters)
        });
        const payload = await result.json();
        if (!result.ok) return json(payload || { error: "not_found" }, result.status);
        if (download) {
          const filename = String(payload.filename || "message").replace(/[\r\n"\\/\u0000-\u001f]/g, "_");
          return response(bytes(payload.content_base64), "application/octet-stream", 200, {
            "Content-Disposition": `attachment; filename="${filename.replace(/[^\x20-\x7e]/g, "_")}"; filename*=UTF-8''${encodeURIComponent(filename)}`
          });
        }
        return json(payload);
      } catch {
        return json({ error: "mail_temporarily_unavailable" }, 503);
      }
    }
  };
}
var index_default = createWorker();
export {
  MailQueue2 as MailQueue,
  createWorker,
  index_default as default
};

// Recovery adapter: expose existing primitives without altering the deployed application.
export { sha256Hex, encryptArchivePayload, decryptArchivePayload, storeArchiveEnvelope, verifyLifeOpsRequest, PostalMime, authorize, plainTextFromHtml };
