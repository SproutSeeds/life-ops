from __future__ import annotations

import base64
import json
import os
import secrets
from datetime import datetime, timezone
from typing import Any, Optional

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from life_ops import credentials

MASTER_KEY_NAME = "LIFE_OPS_MASTER_KEY"
HKDF_SALT = b"life-ops-hkdf-salt-v1"
LOCAL_DB_STORAGE_PURPOSE = "local-db-storage-v1"
LOCAL_DB_BACKUP_PURPOSE = "local-db-backup-v1"
CLOUDFLARE_MAIL_ARCHIVE_PURPOSE = "cloudflare-mail-archive-v1"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _strip_string(value: Any) -> str:
    return str(value or "").strip()


def _b64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64url_decode(value: str) -> bytes:
    clean = _strip_string(value)
    if not clean:
        raise ValueError("base64url value is required")
    padding = "=" * ((4 - (len(clean) % 4)) % 4)
    return base64.urlsafe_b64decode((clean + padding).encode("ascii"))


def _canonical_json(value: dict[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def generate_master_key(
    *,
    backend: str = "auto",
    allow_insecure_file_backend: bool = False,
) -> dict[str, Any]:
    value = _b64url_encode(secrets.token_bytes(32))
    result = credentials.set_secret(
        name=MASTER_KEY_NAME,
        value=value,
        backend=backend,
        allow_insecure_file_backend=allow_insecure_file_backend,
    )
    return {
        **result,
        "secret_name": MASTER_KEY_NAME,
        "generated": True,
    }


def master_key_status() -> dict[str, Any]:
    value = resolve_master_key()
    return {
        "secret_name": MASTER_KEY_NAME,
        "present": bool(value),
        "length_bytes": len(_b64url_decode(value)) if value else 0,
    }


def resolve_master_key() -> Optional[str]:
    return _strip_string(os.getenv(MASTER_KEY_NAME)) or _strip_string(
        credentials.resolve_secret(name=MASTER_KEY_NAME) or ""
    ) or None


def derive_purpose_key(*, purpose: str, master_key: Optional[str] = None) -> bytes:
    master = _strip_string(master_key) or resolve_master_key()
    if not master:
        raise ValueError(f"{MASTER_KEY_NAME} is not configured")
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=HKDF_SALT,
        info=_strip_string(purpose).encode("utf-8"),
    )
    return hkdf.derive(_b64url_decode(master))


def encrypt_bytes(
    plaintext: bytes,
    *,
    purpose: str,
    metadata: Optional[dict[str, Any]] = None,
    master_key: Optional[str] = None,
) -> dict[str, Any]:
    clean_purpose = _strip_string(purpose)
    if not clean_purpose:
        raise ValueError("purpose is required")
    aad = {
        "purpose": clean_purpose,
        "version": 1,
        "created_at": _utc_now_iso(),
    }
    if metadata:
        aad["metadata"] = metadata
    aad_bytes = _canonical_json(aad)
    nonce = secrets.token_bytes(12)
    key = derive_purpose_key(purpose=clean_purpose, master_key=master_key)
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, aad_bytes)
    return {
        "version": 1,
        "alg": "AES-256-GCM",
        "kdf": "HKDF-SHA256",
        "purpose": clean_purpose,
        "nonce_b64": _b64url_encode(nonce),
        "aad_b64": _b64url_encode(aad_bytes),
        "ciphertext_b64": _b64url_encode(ciphertext),
    }


def decrypt_bytes(
    envelope: dict[str, Any],
    *,
    purpose: Optional[str] = None,
    master_key: Optional[str] = None,
) -> bytes:
    if not isinstance(envelope, dict):
        raise ValueError("envelope must be a dict")
    clean_purpose = _strip_string(envelope.get("purpose"))
    if purpose and clean_purpose != _strip_string(purpose):
        raise ValueError("envelope purpose does not match requested purpose")
    aad = _b64url_decode(_strip_string(envelope.get("aad_b64")))
    nonce = _b64url_decode(_strip_string(envelope.get("nonce_b64")))
    ciphertext = _b64url_decode(_strip_string(envelope.get("ciphertext_b64")))
    key = derive_purpose_key(purpose=clean_purpose, master_key=master_key)
    return AESGCM(key).decrypt(nonce, ciphertext, aad)
