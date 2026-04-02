from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Optional

from life_ops import vault_crypto

LOCAL_MAIL_VAULT_PURPOSE = "local-mail-vault-v1"
ENCRYPTED_VAULT_SUFFIX = ".enc.json"


def _strip_string(value: Any) -> str:
    return str(value or "").strip()


def _resolve_vault_path(*, vault_root: Path, relative_path: str | Path) -> Path:
    resolved_root = vault_root.expanduser().resolve(strict=False)
    clean_relative = Path(str(relative_path or "").strip())
    resolved_path = (resolved_root / clean_relative).resolve(strict=False)
    try:
        resolved_path.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError("path traversal detected") from exc
    return resolved_path


def encrypted_vault_filename(logical_filename: str) -> str:
    clean_name = _strip_string(logical_filename) or "mail-artifact.bin"
    return f"{clean_name}{ENCRYPTED_VAULT_SUFFIX}"


def write_encrypted_vault_file(
    *,
    vault_root: Path,
    relative_dir: Path,
    logical_filename: str,
    raw_bytes: bytes,
    metadata: Optional[dict[str, Any]] = None,
) -> tuple[str, str]:
    plaintext_sha256 = hashlib.sha256(raw_bytes).hexdigest()
    envelope = vault_crypto.encrypt_bytes(
        raw_bytes,
        purpose=LOCAL_MAIL_VAULT_PURPOSE,
        metadata={
            "logical_filename": _strip_string(logical_filename),
            "plaintext_bytes": len(raw_bytes),
            "plaintext_sha256": plaintext_sha256,
            **(metadata or {}),
        },
    )
    absolute_dir = _resolve_vault_path(vault_root=vault_root, relative_path=relative_dir)
    absolute_dir.mkdir(parents=True, exist_ok=True)
    target_path = absolute_dir / encrypted_vault_filename(logical_filename)
    target_path.write_text(json.dumps(envelope, indent=2, sort_keys=True) + "\n")
    resolved_root = vault_root.expanduser().resolve(strict=False)
    return str(target_path.relative_to(resolved_root).as_posix()), plaintext_sha256


def read_encrypted_vault_file(*, vault_root: Path, relative_path: str) -> bytes:
    payload_path = _resolve_vault_path(vault_root=vault_root, relative_path=_strip_string(relative_path))
    envelope = json.loads(payload_path.read_text() or "{}")
    if not isinstance(envelope, dict):
        raise ValueError(f"encrypted vault payload at {payload_path} is invalid")
    return vault_crypto.decrypt_bytes(envelope, purpose=LOCAL_MAIL_VAULT_PURPOSE)


def delete_encrypted_vault_file(*, vault_root: Path, relative_path: str) -> bool:
    payload_path = _resolve_vault_path(vault_root=vault_root, relative_path=_strip_string(relative_path))
    if not payload_path.exists():
        return False
    payload_path.unlink()
    resolved_root = vault_root.expanduser().resolve(strict=False)
    current = payload_path.parent
    while current != resolved_root and current.exists():
        try:
            current.rmdir()
        except OSError:
            break
        current = current.parent
    return True
