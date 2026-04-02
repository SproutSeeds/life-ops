from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from life_ops import store
from life_ops import vault_crypto


def _utc_now_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def backup_root() -> Path:
    return store.data_root() / "backups"


def _manifest_path(backup_id: str, root: Path) -> Path:
    return root / f"{backup_id}.backup.json"


def _read_manifest(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text() or "{}")
    if not isinstance(payload, dict):
        raise ValueError(f"backup manifest at {path} is invalid")
    return payload


def _list_manifest_paths(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return sorted(root.glob("*.backup.json"), reverse=True)


def create_encrypted_db_backup(
    *,
    db_path: Optional[Path] = None,
    output_dir: Optional[Path] = None,
) -> dict[str, Any]:
    source_db = db_path or store.default_db_path()
    if not store.db_storage_exists(source_db):
        raise FileNotFoundError(f"database not found at {source_db}")

    root = output_dir or backup_root()
    root.mkdir(parents=True, exist_ok=True)

    backup_id = f"life-ops-db-{_utc_now_slug()}"
    manifest_path = _manifest_path(backup_id, root)

    if store.encrypted_db_enabled(source_db):
        plaintext = store.read_db_bytes(source_db)
    else:
        with tempfile.NamedTemporaryFile(prefix="life-ops-backup-", suffix=".sqlite3", delete=False) as handle:
            temp_path = Path(handle.name)
        try:
            with store.open_db(source_db) as source_connection:
                dest_connection = sqlite3.connect(str(temp_path))
                try:
                    source_connection.backup(dest_connection)
                finally:
                    dest_connection.close()

            plaintext = temp_path.read_bytes()
        finally:
            temp_path.unlink(missing_ok=True)

    compressed = gzip.compress(plaintext, mtime=0)
    envelope = vault_crypto.encrypt_bytes(
        compressed,
        purpose=vault_crypto.LOCAL_DB_BACKUP_PURPOSE,
        metadata={
            "backup_id": backup_id,
            "kind": "sqlite-db",
            "source_path": str(source_db),
            "compression": "gzip",
            "created_at": _utc_now_iso(),
            "plaintext_sha256": _sha256_hex(plaintext),
            "compressed_sha256": _sha256_hex(compressed),
        },
    )
    manifest = {
        "backup_id": backup_id,
        "kind": "sqlite-db",
        "created_at": _utc_now_iso(),
        "source_path": str(source_db),
        "manifest_path": str(manifest_path),
        "plaintext_bytes": len(plaintext),
        "compressed_bytes": len(compressed),
        "plaintext_sha256": _sha256_hex(plaintext),
        "compressed_sha256": _sha256_hex(compressed),
        "envelope": envelope,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    return {
        "backup_id": backup_id,
        "manifest_path": str(manifest_path),
        "plaintext_bytes": len(plaintext),
        "compressed_bytes": len(compressed),
        "plaintext_sha256": manifest["plaintext_sha256"],
        "compressed_sha256": manifest["compressed_sha256"],
    }


def list_backups(*, output_dir: Optional[Path] = None) -> list[dict[str, Any]]:
    root = output_dir or backup_root()
    records: list[dict[str, Any]] = []
    for path in _list_manifest_paths(root):
        try:
            manifest = _read_manifest(path)
        except Exception:
            records.append(
                {
                    "backup_id": path.stem,
                    "manifest_path": str(path),
                    "valid": False,
                }
            )
            continue
        records.append(
            {
                "backup_id": str(manifest.get("backup_id") or path.stem),
                "created_at": str(manifest.get("created_at") or ""),
                "kind": str(manifest.get("kind") or ""),
                "manifest_path": str(path),
                "plaintext_bytes": int(manifest.get("plaintext_bytes") or 0),
                "compressed_bytes": int(manifest.get("compressed_bytes") or 0),
                "valid": True,
            }
        )
    return records


def restore_encrypted_db_backup(
    *,
    manifest_path: Path,
    output_path: Path,
) -> dict[str, Any]:
    manifest = _read_manifest(manifest_path)
    envelope = manifest.get("envelope")
    if not isinstance(envelope, dict):
        raise ValueError("backup manifest is missing envelope")
    compressed = vault_crypto.decrypt_bytes(
        envelope,
        purpose=vault_crypto.LOCAL_DB_BACKUP_PURPOSE,
    )
    plaintext = gzip.decompress(compressed)
    stored_path = store.write_db_bytes(output_path, plaintext)
    return {
        "backup_id": str(manifest.get("backup_id") or ""),
        "manifest_path": str(manifest_path),
        "output_path": str(output_path),
        "stored_path": str(stored_path),
        "encrypted_storage": store.encrypted_db_enabled(output_path),
        "restored_bytes": len(plaintext),
        "restored_sha256": _sha256_hex(plaintext),
        "matches_manifest_sha256": _sha256_hex(plaintext) == str(manifest.get("plaintext_sha256") or ""),
    }


def backup_status(*, output_dir: Optional[Path] = None) -> dict[str, Any]:
    root = output_dir or backup_root()
    rows = list_backups(output_dir=root)
    latest = rows[0] if rows else None
    return {
        "backup_root": str(root),
        "count": len(rows),
        "latest": latest,
        "master_key": vault_crypto.master_key_status(),
    }
