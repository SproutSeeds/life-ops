from __future__ import annotations

import fcntl
import json
import os
import time as time_module
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib import error, request

from life_ops import credentials
from life_ops import store
from life_ops.mail_ingest import (
    MAIL_INGEST_SECRET_NAME,
    MAIL_INGEST_SIGNATURE_HEADER,
    MAIL_INGEST_TIMESTAMP_HEADER,
    ingest_cloudflare_email_payload,
    sign_mail_ingest_payload,
)
from life_ops.vault_crypto import CLOUDFLARE_MAIL_ARCHIVE_PURPOSE, MASTER_KEY_NAME

DEFAULT_CLOUDFLARE_WORKER_NAME = "life-ops-email-ingest"
DEFAULT_CLOUDFLARE_WORKER_COMPATIBILITY_DATE = "2026-03-27"
DEFAULT_CLOUDFLARE_QUEUE_PULL_LIMIT = 25
MAX_CLOUDFLARE_QUEUE_PULL_LIMIT = 100
CLOUDFLARE_WORKER_USER_AGENT = "life-ops/0.2 (+https://frg.earth)"
DEFAULT_CLOUDFLARE_SYNC_LOCK_TIMEOUT_SECONDS = 0.0
DEFAULT_CLOUDFLARE_SYNC_REQUEST_TIMEOUT_SECONDS = 60.0


def default_cloudflare_mail_config_path() -> Path:
    return store.config_root() / "cloudflare_mail.json"


def default_cloudflare_worker_output_dir() -> Path:
    return store.package_root() / "config" / "cloudflare_email_worker"


def default_cloudflare_sync_lock_path() -> Path:
    return store.data_root() / ".cloudflare_mail_sync.lock"


class CloudflareMailSyncBusy(RuntimeError):
    pass


def cloudflare_mail_config_template() -> dict[str, Any]:
    return {
        "zone_name": "frg.earth",
        "route_address": "cody",
        "forward_to": "",
        "worker_name": DEFAULT_CLOUDFLARE_WORKER_NAME,
        "worker_public_url": "",
        "ingest_secret_env": MAIL_INGEST_SECRET_NAME,
        "archive_key_env": MASTER_KEY_NAME,
        "notes": (
            "Cloudflare Email Routing is inbound only. "
            "This stack stores mail durably in an encrypted Cloudflare Worker queue, "
            "then syncs it down into the local life-ops SQLite database. "
            "Use Resend or SES for outbound mail."
        ),
    }


def write_cloudflare_mail_config_template(path: Path, *, force: bool = False) -> dict[str, Any]:
    if path.exists() and not force:
        return {"path": str(path), "created": False, "already_exists": True}

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cloudflare_mail_config_template(), indent=2) + "\n")
    return {"path": str(path), "created": True, "already_exists": False}


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text() or "{}")
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _strip_string(value: Any) -> str:
    return str(value or "").strip()


def _route_full_address(route_address: str, zone_name: str) -> str:
    if not route_address or not zone_name:
        return ""
    return f"{route_address}@{zone_name}"


def _normalize_base_url(value: str) -> str:
    clean = _strip_string(value).rstrip("/")
    if not clean:
        return ""
    if clean.endswith("/api/mail/inbound"):
        return clean[: -len("/api/mail/inbound")]
    return clean


def _utc_now_string() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _acquire_cloudflare_sync_lock(*, timeout_seconds: float = DEFAULT_CLOUDFLARE_SYNC_LOCK_TIMEOUT_SECONDS):
    lock_path = default_cloudflare_sync_lock_path()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(lock_path, "a+b")
    deadline = time_module.monotonic() + max(0.0, float(timeout_seconds))
    while True:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except BlockingIOError:
            if time_module.monotonic() >= deadline:
                handle.close()
                raise CloudflareMailSyncBusy(f"another cloudflare mail sync is already running ({lock_path})")
            time_module.sleep(0.1)
    try:
        os.chmod(lock_path, 0o600)
    except OSError:
        pass
    return handle


def _release_cloudflare_sync_lock(handle) -> None:
    if not handle:
        return
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    except OSError:
        pass
    try:
        handle.close()
    except OSError:
        pass


def _coalesce_number(*values: Any) -> int:
    for value in values:
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return 0


def _load_cloudflare_mail_config(config_path: Path) -> dict[str, Any]:
    config = _load_json(config_path)
    if not config:
        raise FileNotFoundError(
            f"Cloudflare mail config not found at {config_path}. "
            "Run `zsh ./bin/life-ops cloudflare-mail-init-config` first."
        )

    ingest_secret_env = _strip_string(config.get("ingest_secret_env")) or MAIL_INGEST_SECRET_NAME
    archive_key_env = _strip_string(config.get("archive_key_env")) or MASTER_KEY_NAME
    config["zone_name"] = _strip_string(config.get("zone_name"))
    config["route_address"] = _strip_string(config.get("route_address"))
    config["forward_to"] = _strip_string(config.get("forward_to"))
    config["worker_name"] = _strip_string(config.get("worker_name")) or DEFAULT_CLOUDFLARE_WORKER_NAME
    config["worker_public_url"] = _normalize_base_url(
        _strip_string(config.get("worker_public_url")) or _strip_string(config.get("ingest_url"))
    )
    config["ingest_secret_env"] = ingest_secret_env
    config["archive_key_env"] = archive_key_env
    config["ingest_secret"] = (
        _strip_string(os.getenv(ingest_secret_env))
        or _strip_string(credentials.resolve_secret(name=ingest_secret_env) or "")
    )
    config["archive_key_present"] = bool(
        _strip_string(os.getenv(archive_key_env))
        or _strip_string(credentials.resolve_secret(name=archive_key_env) or "")
    )
    return config


def cloudflare_mail_status(*, config_path: Path | None = None) -> dict[str, Any]:
    target = config_path or default_cloudflare_mail_config_path()
    config_present = target.exists()
    config = _load_json(target) if config_present else {}

    zone_name = _strip_string(config.get("zone_name"))
    route_address = _strip_string(config.get("route_address"))
    forward_to = _strip_string(config.get("forward_to"))
    worker_name = _strip_string(config.get("worker_name")) or DEFAULT_CLOUDFLARE_WORKER_NAME
    worker_public_url = _normalize_base_url(
        _strip_string(config.get("worker_public_url")) or _strip_string(config.get("ingest_url"))
    )
    ingest_secret_env = _strip_string(config.get("ingest_secret_env")) or MAIL_INGEST_SECRET_NAME
    archive_key_env = _strip_string(config.get("archive_key_env")) or MASTER_KEY_NAME
    ingest_secret_present = bool(
        _strip_string(os.getenv(ingest_secret_env))
        or _strip_string(credentials.resolve_secret(name=ingest_secret_env) or "")
    )
    archive_key_present = bool(
        _strip_string(os.getenv(archive_key_env))
        or _strip_string(credentials.resolve_secret(name=archive_key_env) or "")
    )

    next_steps: list[str] = []
    if not config_present:
        next_steps.append("Run `zsh ./bin/life-ops cloudflare-mail-init-config` to create a local Cloudflare mail config.")
    if not zone_name:
        next_steps.append("Set zone_name in config/cloudflare_mail.json.")
    if not route_address:
        next_steps.append("Set route_address in config/cloudflare_mail.json.")
    if not worker_public_url:
        next_steps.append("Set worker_public_url in config/cloudflare_mail.json to your deployed worker URL.")
    if not ingest_secret_present:
        next_steps.append("Register LIFE_OPS_MAIL_INGEST_SECRET in the global key registry before deploying or syncing.")
    if not archive_key_present:
        next_steps.append(
            f"Register {archive_key_env} before deploying the Cloudflare worker so cloud mail copies are encrypted at rest."
        )
    if not forward_to:
        next_steps.append("forward_to is optional. Leave it blank if local SQLite + cloud durability should be the primary inbox flow.")

    return {
        "config_present": config_present,
        "config_path": str(target),
        "zone_name": zone_name or None,
        "route_address": route_address or None,
        "route_full_address": _route_full_address(route_address, zone_name) or None,
        "forward_to": forward_to or None,
        "forwarding_enabled": bool(forward_to),
        "worker_name": worker_name,
        "worker_public_url": worker_public_url or None,
        "ingest_secret_env": ingest_secret_env,
        "ingest_secret_present": ingest_secret_present,
        "archive_key_env": archive_key_env,
        "archive_key_present": archive_key_present,
        "cloud_backup_mode": "encrypted_durable_queue",
        "ready_for_worker": config_present and bool(zone_name and route_address and ingest_secret_present and archive_key_present),
        "ready_for_local_sync": config_present and bool(zone_name and route_address and worker_public_url and ingest_secret_present and archive_key_present),
        "next_steps": next_steps,
    }


def _worker_source(config: dict[str, Any]) -> str:
    worker_name = _strip_string(config.get("worker_name")) or DEFAULT_CLOUDFLARE_WORKER_NAME
    return f"""const MAIL_QUEUE_OBJECT = "primary";
const MAX_SIGNATURE_SKEW_SECONDS = 300;
const DEFAULT_PULL_LIMIT = {DEFAULT_CLOUDFLARE_QUEUE_PULL_LIMIT};
const MAX_PULL_LIMIT = {MAX_CLOUDFLARE_QUEUE_PULL_LIMIT};
const MAX_ARCHIVE_CHUNK_CHARS = 90000;

export default {{
  async email(message, env, ctx) {{
    const headers = Object.fromEntries(message.headers);
    const rawBytes = new Uint8Array(await new Response(message.raw).arrayBuffer());
    const payloadHash = await sha256Hex(rawBytes);
    let binary = "";
    const chunkSize = 0x8000;
    for (let index = 0; index < rawBytes.length; index += chunkSize) {{
      binary += String.fromCharCode(...rawBytes.subarray(index, index + chunkSize));
    }}

    const payload = {{
      provider: "cloudflare-email-routing",
      worker: "{worker_name}",
      received_at: new Date().toISOString(),
      envelope_from: message.from,
      envelope_to: message.to,
      headers,
      raw_base64: btoa(binary),
      raw_size: message.rawSize,
      payload_hash: payloadHash,
    }};

    const queue = mailQueueStub(env);
    const enqueueResponse = await queue.fetch("https://mail-queue.internal/enqueue", {{
      method: "POST",
      headers: {{
        "content-type": "application/json",
      }},
      body: JSON.stringify(payload),
    }});

    if (!enqueueResponse.ok) {{
      const reason = await enqueueResponse.text();
      throw new Error(`mail enqueue failed: ${{enqueueResponse.status}} ${{reason}}`);
    }}

    if (env.FORWARD_TO) {{
      await message.forward(env.FORWARD_TO, new Headers({{
        "X-Life-Ops-Envelope-To": message.to,
        "X-Life-Ops-Worker": "{worker_name}",
      }}));
    }}
  }},

  async fetch(request, env, ctx) {{
    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/healthz") {{
      return jsonResponse({{
        ok: true,
        worker: "{worker_name}",
        queue_mode: "encrypted_durable_queue",
        forwarding_enabled: Boolean(env.FORWARD_TO),
        archive_encryption_enabled: Boolean(env.LIFE_OPS_MASTER_KEY),
      }});
    }}

    if (!url.pathname.startsWith("/api/mail/queue/")) {{
      return jsonResponse({{ error: "not_found" }}, 404);
    }}
    if (request.method !== "POST") {{
      return jsonResponse({{ error: "method_not_allowed" }}, 405);
    }}
    if (!env.LIFE_OPS_MAIL_INGEST_SECRET) {{
      return jsonResponse({{ error: "missing_ingest_secret" }}, 500);
    }}
    if (!env.LIFE_OPS_MASTER_KEY) {{
      return jsonResponse({{ error: "missing_master_key" }}, 500);
    }}

    const rawBody = await request.text();
    const verified = await verifyLifeOpsRequest({{
      secret: env.LIFE_OPS_MAIL_INGEST_SECRET,
      timestamp: request.headers.get("X-Life-Ops-Timestamp") || "",
      signature: request.headers.get("X-Life-Ops-Signature") || "",
      body: rawBody,
      maxSkewSeconds: MAX_SIGNATURE_SKEW_SECONDS,
    }});
    if (!verified.ok) {{
      return jsonResponse({{ error: verified.reason }}, 401);
    }}

    const queue = mailQueueStub(env);
    let queuePath = "";
    if (url.pathname === "/api/mail/queue/inject") {{
      queuePath = "/enqueue";
    }} else if (url.pathname === "/api/mail/queue/pull") {{
      queuePath = "/pull";
    }} else if (url.pathname === "/api/mail/queue/ack") {{
      queuePath = "/ack";
    }} else if (url.pathname === "/api/mail/queue/status") {{
      queuePath = "/status";
    }} else {{
      return jsonResponse({{ error: "not_found" }}, 404);
    }}

    return queue.fetch(`https://mail-queue.internal${{queuePath}}`, {{
      method: "POST",
      headers: {{
        "content-type": "application/json",
      }},
      body: rawBody,
    }});
  }},
}};

export class MailQueue {{
  constructor(state, env) {{
    this.state = state;
    this.env = env;
  }}

  async fetch(request) {{
    const url = new URL(request.url);
    if (request.method !== "POST") {{
      return jsonResponse({{ error: "method_not_allowed" }}, 405);
    }}
    const payload = await request.json().catch(() => ({{}}));
    if (url.pathname === "/enqueue") {{
      return jsonResponse(await this.enqueue(payload));
    }}
    if (url.pathname === "/pull") {{
      return jsonResponse(await this.pull(payload));
    }}
    if (url.pathname === "/ack") {{
      return jsonResponse(await this.ack(payload));
    }}
    if (url.pathname === "/status") {{
      return jsonResponse(await this.status());
    }}
    return jsonResponse({{ error: "not_found" }}, 404);
  }}

  async enqueue(payload) {{
    const fingerprint = await fingerprintPayload(payload);
    const dedupeKey = `dedupe:${{fingerprint}}`;
    const existingId = await this.state.storage.get(dedupeKey);
    if (existingId) {{
      return {{
        stored: false,
        duplicate: true,
        id: existingId,
        pending_count: await this.pendingCount(),
      }};
    }}

    const meta = (await this.state.storage.get("meta")) || {{
      next_seq: 0,
      total_stored: 0,
      total_acknowledged: 0,
    }};
    const seq = Number(meta.next_seq || 0) + 1;
    const id = `mail_${{String(seq).padStart(16, "0")}}`;
    const receivedAt = cleanString(payload.received_at) || new Date().toISOString();
    const encryptedPayload = await encryptArchivePayload({{
      masterKey: this.env.LIFE_OPS_MASTER_KEY,
      purpose: "{CLOUDFLARE_MAIL_ARCHIVE_PURPOSE}",
      payload,
      metadata: {{
        id,
        seq,
        fingerprint,
        received_at: receivedAt,
      }},
    }});
    const payloadRef = await storeArchiveEnvelope({{
      storage: this.state.storage,
      id,
      envelope: encryptedPayload,
    }});
    const record = {{
      id,
      seq,
      fingerprint,
      received_at: receivedAt,
      acknowledged: false,
      acknowledged_at: null,
      payload_ref: payloadRef,
    }};

    await this.state.storage.put(`mail:${{id}}`, record);
    await this.state.storage.put(`queue:${{padSeq(seq)}}`, id);
    await this.state.storage.put(dedupeKey, id);

    meta.next_seq = seq;
    meta.total_stored = Number(meta.total_stored || 0) + 1;
    await this.state.storage.put("meta", meta);

    return {{
      stored: true,
      duplicate: false,
      id,
      seq,
      pending_count: await this.pendingCount(),
      total_stored: meta.total_stored,
    }};
  }}

  async pull(payload) {{
    const limit = clampLimit(payload.limit);
    const entries = await this.state.storage.list({{ prefix: "queue:", limit }});
    const items = [];
    for (const [queueKey, itemId] of entries) {{
      const record = await this.state.storage.get(`mail:${{itemId}}`);
      if (!record) {{
        await this.state.storage.delete(queueKey);
        continue;
      }}
      let payload = record.payload || null;
      if (!payload && record.payload_encrypted) {{
        payload = await decryptArchivePayload({{
          masterKey: this.env.LIFE_OPS_MASTER_KEY,
          purpose: "{CLOUDFLARE_MAIL_ARCHIVE_PURPOSE}",
          envelope: record.payload_encrypted,
        }});
      }}
      if (!payload && record.payload_ref) {{
        const archivedEnvelope = await loadArchiveEnvelope({{
          storage: this.state.storage,
          ref: record.payload_ref,
        }});
        payload = await decryptArchivePayload({{
          masterKey: this.env.LIFE_OPS_MASTER_KEY,
          purpose: "{CLOUDFLARE_MAIL_ARCHIVE_PURPOSE}",
          envelope: archivedEnvelope,
        }});
      }}
      items.push({{
        id: record.id,
        seq: record.seq,
        received_at: record.received_at,
        payload_hash: record.fingerprint,
        payload,
      }});
    }}
    const meta = (await this.state.storage.get("meta")) || {{}};
    return {{
      items,
      pulled_count: items.length,
      pending_count: await this.pendingCount(),
      total_stored: Number(meta.total_stored || 0),
      total_acknowledged: Number(meta.total_acknowledged || 0),
      forwarding_enabled: Boolean(this.env.FORWARD_TO),
    }};
  }}

  async ack(payload) {{
    const ids = Array.isArray(payload.ids)
      ? payload.ids.map((value) => cleanString(value)).filter(Boolean)
      : [];
    const meta = (await this.state.storage.get("meta")) || {{
      next_seq: 0,
      total_stored: 0,
      total_acknowledged: 0,
    }};
    let acknowledgedCount = 0;
    const missingIds = [];

    for (const id of ids) {{
      const recordKey = `mail:${{id}}`;
      const record = await this.state.storage.get(recordKey);
      if (!record) {{
        missingIds.push(id);
        continue;
      }}
      if (!record.acknowledged) {{
        record.acknowledged = true;
        record.acknowledged_at = new Date().toISOString();
        await this.state.storage.put(recordKey, record);
        await this.state.storage.delete(`queue:${{padSeq(record.seq)}}`);
        acknowledgedCount += 1;
      }}
    }}

    meta.total_acknowledged = Number(meta.total_acknowledged || 0) + acknowledgedCount;
    await this.state.storage.put("meta", meta);

    return {{
      acknowledged_count: acknowledgedCount,
      missing_ids: missingIds,
      pending_count: await this.pendingCount(),
      total_stored: Number(meta.total_stored || 0),
      total_acknowledged: Number(meta.total_acknowledged || 0),
    }};
  }}

  async status() {{
    const meta = (await this.state.storage.get("meta")) || {{
      next_seq: 0,
      total_stored: 0,
      total_acknowledged: 0,
    }};
    return {{
      pending_count: await this.pendingCount(),
      total_stored: Number(meta.total_stored || 0),
      total_acknowledged: Number(meta.total_acknowledged || 0),
      worker: "{worker_name}",
      forwarding_enabled: Boolean(this.env.FORWARD_TO),
      cloud_backup_mode: "encrypted_durable_queue",
      archive_encryption_enabled: Boolean(this.env.LIFE_OPS_MASTER_KEY),
    }};
  }}

  async pendingCount() {{
    const entries = await this.state.storage.list({{ prefix: "queue:" }});
    return entries.size;
  }}
}}

function mailQueueStub(env) {{
  return env.MAIL_QUEUE.getByName(MAIL_QUEUE_OBJECT);
}}

function cleanString(value) {{
  return String(value || "").trim();
}}

function padSeq(value) {{
  return String(value).padStart(16, "0");
}}

function clampLimit(rawValue) {{
  const numeric = Number(rawValue || DEFAULT_PULL_LIMIT);
  if (!Number.isFinite(numeric)) {{
    return DEFAULT_PULL_LIMIT;
  }}
  return Math.max(1, Math.min(MAX_PULL_LIMIT, Math.trunc(numeric)));
}}

async function fingerprintPayload(payload) {{
  const direct = cleanString(payload.payload_hash);
  if (direct) {{
    return direct;
  }}
  const fallback = cleanString(payload.raw_base64) || JSON.stringify(payload || {{}});
  return sha256Hex(new TextEncoder().encode(fallback));
}}

async function sha256Hex(bytes) {{
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return [...new Uint8Array(digest)]
    .map((value) => value.toString(16).padStart(2, "0"))
    .join("");
}}

async function verifyLifeOpsRequest({{ secret, timestamp, signature, body, maxSkewSeconds }}) {{
  if (!cleanString(timestamp)) {{
    return {{ ok: false, reason: "missing_timestamp" }};
  }}
  if (!cleanString(signature)) {{
    return {{ ok: false, reason: "missing_signature" }};
  }}
  const signedAt = new Date(timestamp);
  if (Number.isNaN(signedAt.getTime())) {{
    return {{ ok: false, reason: "invalid_timestamp" }};
  }}
  const skewSeconds = Math.abs(Date.now() - signedAt.getTime()) / 1000;
  if (skewSeconds > maxSkewSeconds) {{
    return {{ ok: false, reason: "stale_timestamp" }};
  }}
  const expected = await signLifeOpsPayload(secret, timestamp, body);
  if (!(await timingSafeEqual(expected, signature))) {{
    return {{ ok: false, reason: "invalid_signature" }};
  }}
  return {{ ok: true, reason: "ok" }};
}}

async function signLifeOpsPayload(secret, timestamp, body) {{
  const encoder = new TextEncoder();
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    {{ name: "HMAC", hash: "SHA-256" }},
    false,
    ["sign"]
  );
  const bytes = encoder.encode(`${{timestamp}}.${{body}}`);
  const signature = await crypto.subtle.sign("HMAC", key, bytes);
  const digest = [...new Uint8Array(signature)]
    .map((value) => value.toString(16).padStart(2, "0"))
    .join("");
  return `sha256=${{digest}}`;
}}

async function timingSafeEqual(left, right) {{
  const encoder = new TextEncoder();
  const [leftDigest, rightDigest] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(String(left || ""))),
    crypto.subtle.digest("SHA-256", encoder.encode(String(right || ""))),
  ]);
  const a = new Uint8Array(leftDigest);
  const b = new Uint8Array(rightDigest);
  let mismatch = 0;
  for (let index = 0; index < a.length; index += 1) {{
    mismatch |= a[index] ^ b[index];
  }}
  return mismatch === 0;
}}

function b64urlEncode(bytes) {{
  let binary = "";
  const chunkSize = 0x8000;
  for (let index = 0; index < bytes.length; index += chunkSize) {{
    binary += String.fromCharCode(...bytes.subarray(index, index + chunkSize));
  }}
  return btoa(binary).replace(/\\+/g, "-").replace(/\\//g, "_").replace(/=+$/g, "");
}}

function b64urlDecode(value) {{
  const clean = cleanString(value);
  const padded = clean + "=".repeat((4 - (clean.length % 4)) % 4);
  const binary = atob(padded.replace(/-/g, "+").replace(/_/g, "/"));
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {{
    bytes[index] = binary.charCodeAt(index);
  }}
  return bytes;
}}

async function deriveArchiveKey(masterKey, purpose) {{
  const material = await crypto.subtle.importKey(
    "raw",
    b64urlDecode(masterKey),
    "HKDF",
    false,
    ["deriveKey"]
  );
  return crypto.subtle.deriveKey(
    {{
      name: "HKDF",
      hash: "SHA-256",
      salt: new TextEncoder().encode("life-ops-hkdf-salt-v1"),
      info: new TextEncoder().encode(cleanString(purpose)),
    }},
    material,
    {{ name: "AES-GCM", length: 256 }},
    false,
    ["encrypt", "decrypt"]
  );
}}

async function encryptArchivePayload({{ masterKey, purpose, payload, metadata }}) {{
  const aad = {{
    purpose: cleanString(purpose),
    version: 1,
    created_at: new Date().toISOString(),
    metadata: metadata || {{}},
  }};
  const aadBytes = new TextEncoder().encode(JSON.stringify(aad));
  const plaintext = new TextEncoder().encode(JSON.stringify(payload || {{}}));
  const nonce = crypto.getRandomValues(new Uint8Array(12));
  const key = await deriveArchiveKey(masterKey, purpose);
  const ciphertext = new Uint8Array(
    await crypto.subtle.encrypt({{ name: "AES-GCM", iv: nonce, additionalData: aadBytes }}, key, plaintext)
  );
  return {{
    version: 1,
    alg: "AES-256-GCM",
    kdf: "HKDF-SHA256",
    purpose: cleanString(purpose),
    nonce_b64: b64urlEncode(nonce),
    aad_b64: b64urlEncode(aadBytes),
    ciphertext_b64: b64urlEncode(ciphertext),
  }};
}}

async function decryptArchivePayload({{ masterKey, purpose, envelope }}) {{
  const key = await deriveArchiveKey(masterKey, purpose);
  const plaintext = await crypto.subtle.decrypt(
    {{
      name: "AES-GCM",
      iv: b64urlDecode(envelope.nonce_b64),
      additionalData: b64urlDecode(envelope.aad_b64),
    }},
    key,
    b64urlDecode(envelope.ciphertext_b64)
  );
  return JSON.parse(new TextDecoder().decode(plaintext));
}}

async function storeArchiveEnvelope({{ storage, id, envelope }}) {{
  const serialized = JSON.stringify(envelope || {{}});
  const chunkCount = Math.max(1, Math.ceil(serialized.length / MAX_ARCHIVE_CHUNK_CHARS));
  const prefix = `archive:${{id}}:chunk:`;
  for (let index = 0; index < chunkCount; index += 1) {{
    const start = index * MAX_ARCHIVE_CHUNK_CHARS;
    const end = start + MAX_ARCHIVE_CHUNK_CHARS;
    await storage.put(`${{prefix}}${{String(index).padStart(4, "0")}}`, serialized.slice(start, end));
  }}
  return {{
    version: 1,
    kind: "chunked_json",
    prefix,
    chunk_count: chunkCount,
  }};
}}

async function loadArchiveEnvelope({{ storage, ref }}) {{
  if (!ref || ref.kind !== "chunked_json") {{
    return ref || {{}};
  }}
  const chunkCount = Number(ref.chunk_count || 0);
  if (!Number.isFinite(chunkCount) || chunkCount < 1) {{
    throw new Error("invalid_archive_chunk_count");
  }}
  let serialized = "";
  for (let index = 0; index < chunkCount; index += 1) {{
    const chunkKey = `${{ref.prefix}}${{String(index).padStart(4, "0")}}`;
    const chunk = await storage.get(chunkKey);
    if (typeof chunk !== "string") {{
      throw new Error(`missing_archive_chunk:${{chunkKey}}`);
    }}
    serialized += chunk;
  }}
  return JSON.parse(serialized || "{{}}");
}}

function jsonResponse(payload, status = 200) {{
  return new Response(JSON.stringify(payload), {{
    status,
    headers: {{
      "content-type": "application/json",
    }},
  }});
}}
"""


def _worker_wrangler_toml(config: dict[str, Any]) -> str:
    worker_name = _strip_string(config.get("worker_name")) or DEFAULT_CLOUDFLARE_WORKER_NAME
    compatibility_date = DEFAULT_CLOUDFLARE_WORKER_COMPATIBILITY_DATE
    forward_to = _strip_string(config.get("forward_to"))
    return f"""name = "{worker_name}"
main = "src/index.mjs"
compatibility_date = "{compatibility_date}"

[[durable_objects.bindings]]
name = "MAIL_QUEUE"
class_name = "MailQueue"

[[migrations]]
tag = "v1"
new_sqlite_classes = ["MailQueue"]

[vars]
FORWARD_TO = "{forward_to}"
"""


def _worker_readme(config: dict[str, Any]) -> str:
    route_address = _strip_string(config.get("route_address")) or "cody"
    zone_name = _strip_string(config.get("zone_name")) or "example.com"
    worker_name = _strip_string(config.get("worker_name")) or DEFAULT_CLOUDFLARE_WORKER_NAME
    worker_public_url = _normalize_base_url(
        _strip_string(config.get("worker_public_url")) or "https://example.workers.dev"
    )
    return f"""# Cloudflare Email Worker

This worker is the inbound half of the sovereign mail stack for `life-ops`.

## What it does

- receives inbound mail for `{route_address}@{zone_name}`
- stores each message durably inside a Cloudflare Durable Object queue
- encrypts each stored raw message before writing it to Durable Object storage
- exposes signed control endpoints for local sync:
  - `POST /api/mail/queue/inject`
  - `POST /api/mail/queue/pull`
  - `POST /api/mail/queue/ack`
  - `POST /api/mail/queue/status`
- optionally forwards mirrored mail to `FORWARD_TO`
- keeps cloud copies after local sync for redundancy

## Deploy

```bash
npm install -g wrangler
zsh ./bin/life-ops mail-ingest-generate-secret
cd {worker_name}
wrangler secret put LIFE_OPS_MAIL_INGEST_SECRET
wrangler secret put {MASTER_KEY_NAME}
wrangler deploy
```

Then bind the deployed worker inside Cloudflare Email Routing so mail for `{route_address}@{zone_name}` hits this worker.

## Local sync

Set `worker_public_url` in `config/cloudflare_mail.json` to something like:

```text
{worker_public_url}
```

Then sync queued mail into local SQLite:

```bash
zsh ./bin/life-ops cloudflare-mail-sync
```

Requests to the worker queue endpoints are authenticated with timestamped HMAC headers using `LIFE_OPS_MAIL_INGEST_SECRET`, and stored cloud copies are encrypted with `{MASTER_KEY_NAME}` before they are written to Durable Object storage.
"""


def _cloudflare_worker_request_json(
    *,
    worker_public_url: str,
    secret: str,
    path: str,
    payload: Optional[dict[str, Any]] = None,
    timeout_seconds: float = 60,
) -> dict[str, Any]:
    body_dict = payload or {}
    body_bytes = json.dumps(body_dict).encode("utf-8")
    timestamp = _utc_now_string()
    signature = sign_mail_ingest_payload(
        body_bytes=body_bytes,
        secret=secret,
        timestamp=timestamp,
    )
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": CLOUDFLARE_WORKER_USER_AGENT,
        MAIL_INGEST_TIMESTAMP_HEADER: timestamp,
        MAIL_INGEST_SIGNATURE_HEADER: signature,
    }
    req = request.Request(
        f"{_normalize_base_url(worker_public_url)}{path}",
        data=body_bytes,
        method="POST",
        headers=headers,
    )
    try:
        with request.urlopen(req, timeout=max(0.1, float(timeout_seconds))) as response:
            raw = response.read().decode("utf-8")
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="ignore")
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"raw": raw}
        message = payload.get("error") or payload.get("message") or raw or str(exc)
        raise RuntimeError(f"Cloudflare worker request failed ({exc.code}): {message}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Cloudflare worker request failed: {exc.reason}") from exc

    try:
        return json.loads(raw) if raw else {}
    except json.JSONDecodeError as exc:
        raise RuntimeError("Cloudflare worker returned non-JSON output.") from exc


def cloudflare_mail_queue_status(
    *,
    config_path: Path | None = None,
    timeout_seconds: float = 60,
) -> dict[str, Any]:
    config = _load_cloudflare_mail_config(config_path or default_cloudflare_mail_config_path())
    if not config["worker_public_url"]:
        raise RuntimeError("Set worker_public_url in config/cloudflare_mail.json before querying queue status.")
    if not config["ingest_secret"]:
        raise RuntimeError(f"Set {config['ingest_secret_env']} before querying queue status.")
    result = _cloudflare_worker_request_json(
        worker_public_url=str(config["worker_public_url"]),
        secret=str(config["ingest_secret"]),
        path="/api/mail/queue/status",
        payload={},
        timeout_seconds=timeout_seconds,
    )
    return {
        **result,
        "worker_public_url": config["worker_public_url"],
        "route_full_address": _route_full_address(str(config["route_address"]), str(config["zone_name"])),
        "forward_to": config["forward_to"] or None,
        "forwarding_enabled": bool(config["forward_to"]),
        "archive_encryption_enabled": bool(result.get("archive_encryption_enabled")),
    }


def enqueue_cloudflare_mail_payload(
    *,
    payload: dict[str, Any],
    config_path: Path | None = None,
) -> dict[str, Any]:
    config = _load_cloudflare_mail_config(config_path or default_cloudflare_mail_config_path())
    if not config["worker_public_url"]:
        raise RuntimeError("Set worker_public_url in config/cloudflare_mail.json before enqueuing mail payloads.")
    if not config["ingest_secret"]:
        raise RuntimeError(f"Set {config['ingest_secret_env']} before enqueuing mail payloads.")
    result = _cloudflare_worker_request_json(
        worker_public_url=str(config["worker_public_url"]),
        secret=str(config["ingest_secret"]),
        path="/api/mail/queue/inject",
        payload=payload,
    )
    return {
        **result,
        "worker_public_url": config["worker_public_url"],
        "route_full_address": _route_full_address(str(config["route_address"]), str(config["zone_name"])),
    }


def sync_cloudflare_mail_queue(
    *,
    db_path: Path,
    config_path: Path | None = None,
    limit: int = DEFAULT_CLOUDFLARE_QUEUE_PULL_LIMIT,
    request_timeout_seconds: float = DEFAULT_CLOUDFLARE_SYNC_REQUEST_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    config = _load_cloudflare_mail_config(config_path or default_cloudflare_mail_config_path())
    if not config["worker_public_url"]:
        raise RuntimeError("Set worker_public_url in config/cloudflare_mail.json before syncing queued mail.")
    if not config["ingest_secret"]:
        raise RuntimeError(f"Set {config['ingest_secret_env']} before syncing queued mail.")
    sync_started_at = _utc_now_string()
    alert_key = "cloudflare_mail_sync"
    try:
        sync_lock_handle = _acquire_cloudflare_sync_lock()
    except CloudflareMailSyncBusy as exc:
        return {
            "route_full_address": _route_full_address(str(config["route_address"]), str(config["zone_name"])),
            "worker_public_url": config["worker_public_url"],
            "forward_to": config["forward_to"] or None,
            "forwarding_enabled": bool(config["forward_to"]),
            "archive_encryption_enabled": bool(config["archive_key_present"]),
            "skipped": True,
            "skip_reason": str(exc),
            "sync_lock_path": str(default_cloudflare_sync_lock_path()),
            "pulled_count": 0,
            "ingested_count": 0,
            "failed_count": 0,
            "acked_count": 0,
            "pending_count": None,
            "total_stored": None,
            "total_acknowledged": None,
            "ingested": [],
            "errors": [],
        }
    try:
        queue_response = _cloudflare_worker_request_json(
            worker_public_url=str(config["worker_public_url"]),
            secret=str(config["ingest_secret"]),
            path="/api/mail/queue/pull",
            payload={"limit": max(1, min(MAX_CLOUDFLARE_QUEUE_PULL_LIMIT, int(limit)))},
            timeout_seconds=request_timeout_seconds,
        )
        items = list(queue_response.get("items") or [])

        ingested: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        ack_ids: list[str] = []
        for item in items:
            queue_id = _strip_string(item.get("id"))
            payload = item.get("payload")
            if not queue_id or not isinstance(payload, dict):
                errors.append({"id": queue_id or None, "error": "invalid_queue_item"})
                continue
            try:
                result = ingest_cloudflare_email_payload(payload, db_path=db_path)
                ingested.append(
                    {
                        "queue_id": queue_id,
                        "communication_id": result["communication_id"],
                        "subject": result["subject"],
                        "status": result["status"],
                        "category": result["category"],
                    }
                )
                ack_ids.append(queue_id)
            except Exception as exc:
                errors.append({"id": queue_id, "error": str(exc)})

        ack_result: dict[str, Any] = {"acknowledged_count": 0, "pending_count": queue_response.get("pending_count", 0)}
        if ack_ids:
            ack_result = _cloudflare_worker_request_json(
                worker_public_url=str(config["worker_public_url"]),
                secret=str(config["ingest_secret"]),
                path="/api/mail/queue/ack",
                payload={"ids": ack_ids},
                timeout_seconds=request_timeout_seconds,
            )

        result = {
            "route_full_address": _route_full_address(str(config["route_address"]), str(config["zone_name"])),
            "worker_public_url": config["worker_public_url"],
            "forward_to": config["forward_to"] or None,
            "forwarding_enabled": bool(config["forward_to"]),
            "archive_encryption_enabled": True,
            "skipped": False,
            "skip_reason": "",
            "sync_lock_path": str(default_cloudflare_sync_lock_path()),
            "pulled_count": len(items),
            "ingested_count": len(ingested),
            "failed_count": len(errors),
            "acked_count": _coalesce_number(ack_result.get("acknowledged_count"), 0),
            "pending_count": _coalesce_number(ack_result.get("pending_count"), queue_response.get("pending_count"), 0),
            "total_stored": _coalesce_number(ack_result.get("total_stored"), queue_response.get("total_stored"), 0),
            "total_acknowledged": _coalesce_number(
                ack_result.get("total_acknowledged"),
                queue_response.get("total_acknowledged"),
                0,
            ),
            "ingested": ingested,
            "errors": errors,
        }
        sync_completed_at = _utc_now_string()
        with store.open_db(db_path) as connection:
            store.set_sync_state(connection, "cloudflare_mail:last_sync_at", sync_completed_at)
            store.set_sync_state(connection, "cloudflare_mail:pending_count", str(result["pending_count"]))
            store.set_sync_state(connection, "cloudflare_mail:total_stored", str(result["total_stored"]))
            store.set_sync_state(connection, "cloudflare_mail:total_acknowledged", str(result["total_acknowledged"]))
            store.set_sync_state(connection, "cloudflare_mail:forwarding_enabled", "1" if result["forwarding_enabled"] else "0")
            store.set_sync_state(connection, "cloudflare_mail:archive_encryption_enabled", "1" if result["archive_encryption_enabled"] else "0")
            if errors:
                store.set_sync_state(connection, "cloudflare_mail:last_failure_at", sync_completed_at)
                store.upsert_system_alert(
                    connection,
                    alert_key=alert_key,
                    source="cloudflare_email",
                    severity="warning",
                    title="Cloudflare mail sync ingested with errors",
                    message=f"{len(errors)} queued mail item(s) failed during local ingest.",
                    details={"errors": errors[:10], "pulled_count": len(items), "ingested_count": len(ingested)},
                )
            else:
                store.set_sync_state(connection, "cloudflare_mail:last_success_at", sync_completed_at)
                store.clear_system_alert(connection, alert_key)
            purge_result = store.purge_deleted_communications(connection)
        result["purged_deleted_count"] = int(purge_result.get("purged_count", 0))
        result["purged_artifact_count"] = int(purge_result.get("artifact_count", 0))
        return result
    except Exception as exc:
        sync_failed_at = _utc_now_string()
        with store.open_db(db_path) as connection:
            store.set_sync_state(connection, "cloudflare_mail:last_failure_at", sync_failed_at)
            store.upsert_system_alert(
                connection,
                alert_key=alert_key,
                source="cloudflare_email",
                severity="error",
                title="Cloudflare mail sync failed",
                message=str(exc),
                details={"worker_public_url": config["worker_public_url"], "route_full_address": _route_full_address(str(config["route_address"]), str(config["zone_name"]))},
            )
        raise
    finally:
        _release_cloudflare_sync_lock(locals().get("sync_lock_handle"))


def write_cloudflare_worker_template(
    output_dir: Path,
    *,
    config_path: Path | None = None,
    force: bool = False,
) -> dict[str, Any]:
    config_source = _load_json(config_path or default_cloudflare_mail_config_path())
    output_dir.mkdir(parents=True, exist_ok=True)
    index_path = output_dir / "src" / "index.mjs"
    wrangler_path = output_dir / "wrangler.toml"
    readme_path = output_dir / "README.md"

    if not force:
        for path in (index_path, wrangler_path, readme_path):
            if path.exists():
                return {
                    "output_dir": str(output_dir),
                    "created": False,
                    "already_exists": True,
                }

    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text(_worker_source(config_source))
    wrangler_path.write_text(_worker_wrangler_toml(config_source))
    readme_path.write_text(_worker_readme(config_source))

    return {
        "output_dir": str(output_dir),
        "created": True,
        "already_exists": False,
        "files": [str(index_path), str(wrangler_path), str(readme_path)],
    }
