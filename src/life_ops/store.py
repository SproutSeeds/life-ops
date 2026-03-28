from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sqlite3
import tempfile
import time as time_module
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Optional

from life_ops import vault_crypto

LOW_SIGNAL_ATTACHMENT_NAME_FRAGMENTS = {
    "bar",
    "banner",
    "corner",
    "footer",
    "header",
    "icon",
    "logo",
    "pixel",
    "spacer",
}
LOW_SIGNAL_ATTACHMENT_NAMES = {
    "",
    "attachment",
    "attachment-1",
    "attachment-2",
    "image",
    "logo",
}

SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS organizations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    category TEXT NOT NULL DEFAULT 'general',
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    start_at TEXT NOT NULL,
    end_at TEXT NOT NULL,
    all_day INTEGER NOT NULL DEFAULT 0,
    location TEXT NOT NULL DEFAULT '',
    organization_id INTEGER REFERENCES organizations(id),
    kind TEXT NOT NULL DEFAULT 'event',
    status TEXT NOT NULL DEFAULT 'confirmed',
    source TEXT NOT NULL DEFAULT 'manual',
    notes TEXT NOT NULL DEFAULT '',
    external_id TEXT,
    external_calendar_id TEXT,
    external_etag TEXT,
    html_link TEXT
);

CREATE TABLE IF NOT EXISTS communications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subject TEXT NOT NULL,
    channel TEXT NOT NULL,
    direction TEXT NOT NULL DEFAULT 'inbound',
    person TEXT NOT NULL DEFAULT '',
    organization_id INTEGER REFERENCES organizations(id),
    happened_at TEXT NOT NULL,
    follow_up_at TEXT,
    status TEXT NOT NULL DEFAULT 'open',
    notes TEXT NOT NULL DEFAULT '',
    source TEXT NOT NULL DEFAULT 'manual',
    external_id TEXT,
    external_thread_id TEXT,
    external_from TEXT,
    external_to TEXT NOT NULL DEFAULT '',
    external_cc TEXT NOT NULL DEFAULT '',
    external_bcc TEXT NOT NULL DEFAULT '',
    external_reply_to TEXT NOT NULL DEFAULT '',
    from_json TEXT NOT NULL DEFAULT '{}',
    to_json TEXT NOT NULL DEFAULT '[]',
    cc_json TEXT NOT NULL DEFAULT '[]',
    bcc_json TEXT NOT NULL DEFAULT '[]',
    reply_to_json TEXT NOT NULL DEFAULT '[]',
    message_id TEXT NOT NULL DEFAULT '',
    in_reply_to TEXT NOT NULL DEFAULT '',
    references_json TEXT NOT NULL DEFAULT '[]',
    headers_json TEXT NOT NULL DEFAULT '{}',
    thread_key TEXT NOT NULL DEFAULT '',
    snippet TEXT NOT NULL DEFAULT '',
    body_text TEXT NOT NULL DEFAULT '',
    html_body TEXT NOT NULL DEFAULT '',
    attachments_json TEXT NOT NULL DEFAULT '[]',
    raw_relative_path TEXT NOT NULL DEFAULT '',
    raw_sha256 TEXT NOT NULL DEFAULT '',
    category TEXT NOT NULL DEFAULT '',
    categories_json TEXT NOT NULL DEFAULT '[]',
    priority_level TEXT NOT NULL DEFAULT '',
    priority_score INTEGER NOT NULL DEFAULT 0,
    retention_bucket TEXT NOT NULL DEFAULT '',
    classifier_version TEXT NOT NULL DEFAULT '',
    classification_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS profile_context_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_key TEXT NOT NULL UNIQUE,
    subject_key TEXT NOT NULL DEFAULT 'self',
    item_type TEXT NOT NULL,
    title TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'gmail',
    communication_id INTEGER REFERENCES communications(id) ON DELETE CASCADE,
    happened_at TEXT NOT NULL,
    confidence INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'candidate',
    details_json TEXT NOT NULL DEFAULT '{}',
    evidence_json TEXT NOT NULL DEFAULT '[]',
    review_notes TEXT NOT NULL DEFAULT '',
    reviewed_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS profile_subjects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_key TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL DEFAULT '',
    relationship TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'active',
    notes TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS profile_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subject_id INTEGER NOT NULL REFERENCES profile_subjects(id) ON DELETE CASCADE,
    item_type TEXT NOT NULL,
    record_kind TEXT NOT NULL DEFAULT 'record',
    title TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    source TEXT NOT NULL DEFAULT 'profile_context',
    happened_at TEXT NOT NULL,
    confidence INTEGER NOT NULL DEFAULT 0,
    notes TEXT NOT NULL DEFAULT '',
    details_json TEXT NOT NULL DEFAULT '{}',
    evidence_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS profile_record_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    record_id INTEGER NOT NULL REFERENCES profile_records(id) ON DELETE CASCADE,
    profile_item_id INTEGER NOT NULL REFERENCES profile_context_items(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(record_id, profile_item_id)
);

CREATE TABLE IF NOT EXISTS profile_record_attachments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    record_id INTEGER NOT NULL REFERENCES profile_records(id) ON DELETE CASCADE,
    attachment_id INTEGER NOT NULL REFERENCES communication_attachments(id) ON DELETE CASCADE,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(record_id, attachment_id)
);

CREATE TABLE IF NOT EXISTS communication_attachments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    external_key TEXT NOT NULL UNIQUE,
    communication_id INTEGER NOT NULL REFERENCES communications(id) ON DELETE CASCADE,
    source TEXT NOT NULL DEFAULT 'gmail',
    external_message_id TEXT NOT NULL DEFAULT '',
    external_attachment_id TEXT NOT NULL DEFAULT '',
    part_id TEXT NOT NULL DEFAULT '',
    filename TEXT NOT NULL DEFAULT '',
    mime_type TEXT NOT NULL DEFAULT '',
    size INTEGER NOT NULL DEFAULT 0,
    relative_path TEXT NOT NULL DEFAULT '',
    extracted_text TEXT NOT NULL DEFAULT '',
    extracted_text_path TEXT NOT NULL DEFAULT '',
    extraction_method TEXT NOT NULL DEFAULT '',
    ingest_status TEXT NOT NULL DEFAULT 'pending',
    error_text TEXT NOT NULL DEFAULT '',
    sha256 TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS x_content_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL DEFAULT 'x',
    kind TEXT NOT NULL DEFAULT 'post',
    title TEXT NOT NULL DEFAULT '',
    summary TEXT NOT NULL DEFAULT '',
    body_text TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'draft',
    parent_id INTEGER REFERENCES x_content_items(id) ON DELETE CASCADE,
    sequence_index INTEGER,
    tags_json TEXT NOT NULL DEFAULT '[]',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS x_media_assets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content_item_id INTEGER REFERENCES x_content_items(id) ON DELETE CASCADE,
    asset_kind TEXT NOT NULL DEFAULT 'image',
    title TEXT NOT NULL DEFAULT '',
    prompt_text TEXT NOT NULL DEFAULT '',
    alt_text TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'planned',
    model_name TEXT NOT NULL DEFAULT '',
    relative_path TEXT NOT NULL DEFAULT '',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    error_text TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS routines (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    cadence TEXT NOT NULL CHECK (cadence IN ('daily', 'weekly')),
    day_of_week INTEGER,
    start_time TEXT NOT NULL,
    duration_minutes INTEGER NOT NULL DEFAULT 30,
    notes TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS sync_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mail_delivery_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_key TEXT NOT NULL UNIQUE,
    provider TEXT NOT NULL DEFAULT 'resend',
    communication_id INTEGER NOT NULL REFERENCES communications(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'queued',
    payload_json TEXT NOT NULL DEFAULT '{}',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 8,
    next_attempt_at TEXT NOT NULL DEFAULT '',
    last_attempt_at TEXT,
    provider_message_id TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS system_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alert_key TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'error',
    status TEXT NOT NULL DEFAULT 'active',
    title TEXT NOT NULL,
    message TEXT NOT NULL DEFAULT '',
    details_json TEXT NOT NULL DEFAULT '{}',
    first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    cleared_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS trace_runs (
    id TEXT PRIMARY KEY,
    trace_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    metadata_json TEXT NOT NULL DEFAULT '{}',
    summary_json TEXT NOT NULL DEFAULT '{}',
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT
);

CREATE TABLE IF NOT EXISTS trace_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES trace_runs(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    entity_key TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL DEFAULT '{}',
    happened_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""

DAY_TO_INDEX = {
    "monday": 0,
    "tuesday": 1,
    "wednesday": 2,
    "thursday": 3,
    "friday": 4,
    "saturday": 5,
    "sunday": 6,
}

FORCE_ENCRYPTED_DB_ENV = "LIFE_OPS_FORCE_ENCRYPTED_DB"
ENCRYPTED_DB_SUFFIX = ".enc.json"
DB_LOCK_SUFFIX = ".lock"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_db_path() -> Path:
    return repo_root() / "data" / "life_ops.db"


def attachment_vault_root() -> Path:
    return repo_root() / "data" / "attachments"


def x_media_root() -> Path:
    return repo_root() / "data" / "x_media"


def _normalized_path(path: Path) -> Path:
    return path.expanduser().resolve(strict=False)


def encrypted_db_enabled(db_path: Path) -> bool:
    forced = str(os.getenv(FORCE_ENCRYPTED_DB_ENV) or "").strip().lower()
    if forced in {"1", "true", "yes", "on"}:
        return True
    return _normalized_path(db_path) == _normalized_path(default_db_path())


def encrypted_db_manifest_path(db_path: Path) -> Path:
    return db_path.with_name(f"{db_path.name}{ENCRYPTED_DB_SUFFIX}")


def encrypted_db_lock_path(db_path: Path) -> Path:
    return db_path.with_name(f".{db_path.name}{DB_LOCK_SUFFIX}")


def _db_sidecar_paths(db_path: Path) -> list[Path]:
    return [
        db_path,
        db_path.with_name(f"{db_path.name}-wal"),
        db_path.with_name(f"{db_path.name}-shm"),
    ]


def remove_plaintext_db_artifacts(db_path: Path) -> None:
    for path in _db_sidecar_paths(db_path):
        path.unlink(missing_ok=True)


def db_storage_exists(db_path: Path) -> bool:
    return db_path.exists() or encrypted_db_manifest_path(db_path).exists()


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _write_text_atomic(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        prefix=f"{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
        delete=False,
        mode="w",
        encoding="utf-8",
    ) as handle:
        temp_path = Path(handle.name)
        handle.write(payload)
    try:
        os.chmod(temp_path, 0o600)
    except OSError:
        pass
    temp_path.replace(path)


def read_db_bytes(db_path: Path) -> bytes:
    manifest_path = encrypted_db_manifest_path(db_path)
    if manifest_path.exists():
        payload = json.loads(manifest_path.read_text() or "{}")
        envelope = payload.get("envelope")
        if not isinstance(envelope, dict):
            raise ValueError(f"encrypted DB manifest at {manifest_path} is missing envelope")
        return vault_crypto.decrypt_bytes(
            envelope,
            purpose=vault_crypto.LOCAL_DB_STORAGE_PURPOSE,
        )
    if db_path.exists():
        return db_path.read_bytes()
    raise FileNotFoundError(f"database not found at {db_path}")


def write_db_bytes(db_path: Path, plaintext: bytes) -> Path:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if encrypted_db_enabled(db_path):
        manifest_path = encrypted_db_manifest_path(db_path)
        envelope = vault_crypto.encrypt_bytes(
            plaintext,
            purpose=vault_crypto.LOCAL_DB_STORAGE_PURPOSE,
            metadata={
                "logical_path": str(db_path),
                "plaintext_bytes": len(plaintext),
                "plaintext_sha256": _sha256_hex(plaintext),
                "stored_at": _utc_now_iso(),
            },
        )
        payload = {
            "logical_path": str(db_path),
            "stored_at": _utc_now_iso(),
            "plaintext_bytes": len(plaintext),
            "plaintext_sha256": _sha256_hex(plaintext),
            "envelope": envelope,
        }
        _write_text_atomic(manifest_path, json.dumps(payload, indent=2) + "\n")
        remove_plaintext_db_artifacts(db_path)
        return manifest_path
    db_path.write_bytes(plaintext)
    return db_path


def _secure_temp_db_path(prefix: str = "life-ops-db-") -> Path:
    with tempfile.NamedTemporaryFile(prefix=prefix, suffix=".sqlite3", delete=False) as handle:
        temp_path = Path(handle.name)
    try:
        os.chmod(temp_path, 0o600)
    except OSError:
        pass
    return temp_path


def _hydrate_connection_from_bytes(connection: sqlite3.Connection, plaintext: bytes) -> None:
    if not plaintext:
        return
    temp_path = _secure_temp_db_path(prefix="life-ops-hydrate-")
    try:
        temp_path.write_bytes(plaintext)
        source = sqlite3.connect(str(temp_path))
        try:
            source.backup(connection)
        finally:
            source.close()
    finally:
        temp_path.unlink(missing_ok=True)


def _persist_connection_snapshot(connection: sqlite3.Connection, db_path: Path) -> dict[str, object]:
    temp_path = _secure_temp_db_path(prefix="life-ops-snapshot-")
    try:
        dest = sqlite3.connect(str(temp_path))
        try:
            connection.backup(dest)
        finally:
            dest.close()
        plaintext = temp_path.read_bytes()
    finally:
        temp_path.unlink(missing_ok=True)
    stored_path = write_db_bytes(db_path, plaintext)
    return {
        "stored_path": stored_path,
        "plaintext_bytes": len(plaintext),
        "plaintext_sha256": _sha256_hex(plaintext),
    }


def _acquire_db_lock(db_path: Path, *, timeout: float = 30.0):
    lock_path = encrypted_db_lock_path(db_path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(lock_path, "a+b")
    deadline = time_module.monotonic() + timeout
    while True:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except BlockingIOError:
            if time_module.monotonic() >= deadline:
                handle.close()
                raise TimeoutError(f"timed out waiting for DB lock at {lock_path}")
            time_module.sleep(0.1)
    try:
        os.chmod(lock_path, 0o600)
    except OSError:
        pass
    return handle


def _release_db_lock(handle) -> None:
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


class LifeOpsConnection(sqlite3.Connection):
    def commit(self) -> None:
        super().commit()
        if (
            getattr(self, "_life_ops_encrypted_storage", False)
            and not getattr(self, "_life_ops_closed", False)
            and self.total_changes != getattr(self, "_life_ops_last_persisted_changes", 0)
        ):
            _persist_connection_snapshot(self, getattr(self, "_life_ops_logical_path"))
            self._life_ops_last_persisted_changes = self.total_changes

    def close(self) -> None:
        if getattr(self, "_life_ops_closed", False):
            return
        seal_error = None
        if (
            getattr(self, "_life_ops_encrypted_storage", False)
            and self.total_changes != getattr(self, "_life_ops_last_persisted_changes", 0)
        ):
            try:
                _persist_connection_snapshot(self, getattr(self, "_life_ops_logical_path"))
                self._life_ops_last_persisted_changes = self.total_changes
            except Exception as exc:  # pragma: no cover - surfaced to caller
                seal_error = exc
        try:
            super().close()
        finally:
            _release_db_lock(getattr(self, "_life_ops_lock_handle", None))
            self._life_ops_closed = True
            self._life_ops_lock_handle = None
        if seal_error is not None:
            raise seal_error

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        if exc_type is None:
            try:
                self.commit()
            except Exception:
                self.rollback()
                self.close()
                raise
        else:
            self.rollback()
        self.close()
        return False


def attachment_filename_is_low_signal(filename: str) -> bool:
    collapsed = str(filename or "").strip().lower()
    if collapsed in LOW_SIGNAL_ATTACHMENT_NAMES:
        return True
    return any(fragment in collapsed for fragment in LOW_SIGNAL_ATTACHMENT_NAME_FRAGMENTS)


def _configure_connection(connection: sqlite3.Connection, *, encrypted_storage: bool) -> None:
    connection.row_factory = sqlite3.Row
    if encrypted_storage:
        connection.execute("PRAGMA journal_mode = MEMORY")
    else:
        connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA busy_timeout = 30000")
    connection.execute("PRAGMA foreign_keys = ON")


def open_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if encrypted_db_enabled(db_path):
        if not vault_crypto.resolve_master_key():
            raise ValueError(
                "Encrypted local DB storage requires LIFE_OPS_MASTER_KEY. "
                "Run `zsh ./bin/life-ops vault-generate-master-key` first."
            )
        lock_handle = _acquire_db_lock(db_path)
        connection = sqlite3.connect(":memory:", timeout=30.0, factory=LifeOpsConnection)
        connection._life_ops_encrypted_storage = True
        connection._life_ops_logical_path = db_path
        connection._life_ops_lock_handle = lock_handle
        connection._life_ops_last_persisted_changes = 0
        _configure_connection(connection, encrypted_storage=True)

        manifest_path = encrypted_db_manifest_path(db_path)
        if manifest_path.exists():
            _hydrate_connection_from_bytes(connection, read_db_bytes(db_path))
        elif db_path.exists():
            _hydrate_connection_from_bytes(connection, db_path.read_bytes())

        connection.executescript(SCHEMA)
        _apply_migrations(connection)

        if not manifest_path.exists():
            _persist_connection_snapshot(connection, db_path)
            connection._life_ops_last_persisted_changes = connection.total_changes
        remove_plaintext_db_artifacts(db_path)
        return connection

    connection = sqlite3.connect(db_path, timeout=30.0, factory=LifeOpsConnection)
    connection._life_ops_encrypted_storage = False
    connection._life_ops_last_persisted_changes = 0
    _configure_connection(connection, encrypted_storage=False)
    connection.executescript(SCHEMA)
    _apply_migrations(connection)
    return connection


def initialize(db_path: Path) -> Path:
    with open_db(db_path) as connection:
        connection.commit()
    return db_path


def _json_text(value, fallback) -> str:
    return json.dumps(value if value is not None else fallback)


def _json_value(value: str, fallback):
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def _iso_minute(value: datetime) -> str:
    return _local_naive(value).replace(second=0, microsecond=0).isoformat(timespec="minutes")


def _local_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value

    local_tz = datetime.now().astimezone().tzinfo
    return value.astimezone(local_tz).replace(tzinfo=None)


def parse_datetime(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        parsed_date = date.fromisoformat(value)
        return datetime.combine(parsed_date, time(0, 0))

    return _local_naive(parsed)


def _column_names(connection: sqlite3.Connection, table_name: str) -> set[str]:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"]) for row in rows}


def _ensure_column(connection: sqlite3.Connection, table_name: str, column_name: str, definition: str) -> None:
    if column_name in _column_names(connection, table_name):
        return
    connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def _apply_migrations(connection: sqlite3.Connection) -> None:
    _ensure_column(connection, "events", "external_id", "TEXT")
    _ensure_column(connection, "events", "external_calendar_id", "TEXT")
    _ensure_column(connection, "events", "external_etag", "TEXT")
    _ensure_column(connection, "events", "html_link", "TEXT")

    _ensure_column(connection, "communications", "source", "TEXT NOT NULL DEFAULT 'manual'")
    _ensure_column(connection, "communications", "external_id", "TEXT")
    _ensure_column(connection, "communications", "external_thread_id", "TEXT")
    _ensure_column(connection, "communications", "external_from", "TEXT")
    _ensure_column(connection, "communications", "direction", "TEXT NOT NULL DEFAULT 'inbound'")
    _ensure_column(connection, "communications", "external_to", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communications", "external_cc", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communications", "external_bcc", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communications", "external_reply_to", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communications", "from_json", "TEXT NOT NULL DEFAULT '{}'")
    _ensure_column(connection, "communications", "to_json", "TEXT NOT NULL DEFAULT '[]'")
    _ensure_column(connection, "communications", "cc_json", "TEXT NOT NULL DEFAULT '[]'")
    _ensure_column(connection, "communications", "bcc_json", "TEXT NOT NULL DEFAULT '[]'")
    _ensure_column(connection, "communications", "reply_to_json", "TEXT NOT NULL DEFAULT '[]'")
    _ensure_column(connection, "communications", "message_id", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communications", "in_reply_to", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communications", "references_json", "TEXT NOT NULL DEFAULT '[]'")
    _ensure_column(connection, "communications", "headers_json", "TEXT NOT NULL DEFAULT '{}'")
    _ensure_column(connection, "communications", "thread_key", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communications", "snippet", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communications", "body_text", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communications", "html_body", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communications", "attachments_json", "TEXT NOT NULL DEFAULT '[]'")
    _ensure_column(connection, "communications", "raw_relative_path", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communications", "raw_sha256", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communications", "category", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communications", "categories_json", "TEXT NOT NULL DEFAULT '[]'")
    _ensure_column(connection, "communications", "priority_level", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communications", "priority_score", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "communications", "retention_bucket", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communications", "classifier_version", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communications", "classification_json", "TEXT NOT NULL DEFAULT '{}'")

    _ensure_column(connection, "profile_context_items", "external_key", "TEXT")
    _ensure_column(connection, "profile_context_items", "subject_key", "TEXT NOT NULL DEFAULT 'self'")
    _ensure_column(connection, "profile_context_items", "item_type", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "profile_context_items", "title", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "profile_context_items", "source", "TEXT NOT NULL DEFAULT 'gmail'")
    _ensure_column(connection, "profile_context_items", "communication_id", "INTEGER")
    _ensure_column(connection, "profile_context_items", "happened_at", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "profile_context_items", "confidence", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "profile_context_items", "status", "TEXT NOT NULL DEFAULT 'candidate'")
    _ensure_column(connection, "profile_context_items", "details_json", "TEXT NOT NULL DEFAULT '{}'")
    _ensure_column(connection, "profile_context_items", "evidence_json", "TEXT NOT NULL DEFAULT '[]'")
    _ensure_column(connection, "profile_context_items", "review_notes", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "profile_context_items", "reviewed_at", "TEXT")
    _ensure_column(connection, "profile_context_items", "created_at", "TEXT")
    _ensure_column(connection, "profile_context_items", "updated_at", "TEXT")

    _ensure_column(connection, "communication_attachments", "external_key", "TEXT")
    _ensure_column(connection, "communication_attachments", "communication_id", "INTEGER")
    _ensure_column(connection, "communication_attachments", "source", "TEXT NOT NULL DEFAULT 'gmail'")
    _ensure_column(connection, "communication_attachments", "external_message_id", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communication_attachments", "external_attachment_id", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communication_attachments", "part_id", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communication_attachments", "filename", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communication_attachments", "mime_type", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communication_attachments", "size", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "communication_attachments", "relative_path", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communication_attachments", "extracted_text", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communication_attachments", "extracted_text_path", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communication_attachments", "extraction_method", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communication_attachments", "ingest_status", "TEXT NOT NULL DEFAULT 'pending'")
    _ensure_column(connection, "communication_attachments", "error_text", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communication_attachments", "sha256", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "communication_attachments", "created_at", "TEXT")
    _ensure_column(connection, "communication_attachments", "updated_at", "TEXT")

    _ensure_column(connection, "mail_delivery_queue", "queue_key", "TEXT")
    _ensure_column(connection, "mail_delivery_queue", "provider", "TEXT NOT NULL DEFAULT 'resend'")
    _ensure_column(connection, "mail_delivery_queue", "communication_id", "INTEGER")
    _ensure_column(connection, "mail_delivery_queue", "status", "TEXT NOT NULL DEFAULT 'queued'")
    _ensure_column(connection, "mail_delivery_queue", "payload_json", "TEXT NOT NULL DEFAULT '{}'")
    _ensure_column(connection, "mail_delivery_queue", "metadata_json", "TEXT NOT NULL DEFAULT '{}'")
    _ensure_column(connection, "mail_delivery_queue", "attempt_count", "INTEGER NOT NULL DEFAULT 0")
    _ensure_column(connection, "mail_delivery_queue", "max_attempts", "INTEGER NOT NULL DEFAULT 8")
    _ensure_column(connection, "mail_delivery_queue", "next_attempt_at", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "mail_delivery_queue", "last_attempt_at", "TEXT")
    _ensure_column(connection, "mail_delivery_queue", "provider_message_id", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "mail_delivery_queue", "last_error", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "mail_delivery_queue", "created_at", "TEXT")
    _ensure_column(connection, "mail_delivery_queue", "updated_at", "TEXT")

    _ensure_column(connection, "system_alerts", "alert_key", "TEXT")
    _ensure_column(connection, "system_alerts", "source", "TEXT")
    _ensure_column(connection, "system_alerts", "severity", "TEXT NOT NULL DEFAULT 'error'")
    _ensure_column(connection, "system_alerts", "status", "TEXT NOT NULL DEFAULT 'active'")
    _ensure_column(connection, "system_alerts", "title", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "system_alerts", "message", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "system_alerts", "details_json", "TEXT NOT NULL DEFAULT '{}'")
    _ensure_column(connection, "system_alerts", "first_seen_at", "TEXT")
    _ensure_column(connection, "system_alerts", "last_seen_at", "TEXT")
    _ensure_column(connection, "system_alerts", "cleared_at", "TEXT")
    _ensure_column(connection, "system_alerts", "created_at", "TEXT")
    _ensure_column(connection, "system_alerts", "updated_at", "TEXT")

    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_events_source_external_id
        ON events(source, external_id)
        WHERE external_id IS NOT NULL
        """
    )
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_communications_source_external_id
        ON communications(source, external_id)
        WHERE external_id IS NOT NULL
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_trace_runs_type_started_at
        ON trace_runs(trace_type, started_at DESC)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_trace_events_run_id
        ON trace_events(run_id, id)
        """
    )
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_profile_context_external_key
        ON profile_context_items(external_key)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_profile_context_subject_item
        ON profile_context_items(subject_key, item_type, happened_at DESC)
        """
    )
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_profile_subjects_subject_key
        ON profile_subjects(subject_key)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_profile_records_subject_item_status
        ON profile_records(subject_id, item_type, status, happened_at DESC)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_profile_record_items_record
        ON profile_record_items(record_id, profile_item_id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_profile_record_attachments_record
        ON profile_record_attachments(record_id, attachment_id)
        """
    )
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_communication_attachments_external_key
        ON communication_attachments(external_key)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_communication_attachments_comm_id
        ON communication_attachments(communication_id, ingest_status, created_at DESC)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_x_content_platform_kind_status
        ON x_content_items(platform, kind, status, created_at DESC)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_x_content_parent_sequence
        ON x_content_items(parent_id, sequence_index, id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_x_media_content_status
        ON x_media_assets(content_item_id, status, id)
        """
    )
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_mail_delivery_queue_key
        ON mail_delivery_queue(queue_key)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mail_delivery_queue_due
        ON mail_delivery_queue(provider, status, next_attempt_at, id)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mail_delivery_queue_comm
        ON mail_delivery_queue(communication_id, provider, status)
        """
    )
    connection.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_system_alerts_key
        ON system_alerts(alert_key)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_system_alerts_status_source
        ON system_alerts(status, source, last_seen_at DESC)
        """
    )
    connection.commit()


def parse_day_name(value: str) -> int:
    lowered = value.strip().lower()
    if lowered not in DAY_TO_INDEX:
        valid = ", ".join(DAY_TO_INDEX)
        raise ValueError(f"invalid weekday '{value}'. Use one of: {valid}")
    return DAY_TO_INDEX[lowered]


def ensure_organization(connection: sqlite3.Connection, name: Optional[str], category: str = "general", notes: str = "") -> Optional[int]:
    if not name:
        return None

    existing = connection.execute(
        "SELECT id FROM organizations WHERE name = ?",
        (name,),
    ).fetchone()
    if existing:
        return int(existing["id"])

    cursor = connection.execute(
        "INSERT INTO organizations (name, category, notes) VALUES (?, ?, ?)",
        (name, category, notes),
    )
    connection.commit()
    return int(cursor.lastrowid)


def add_organization(connection: sqlite3.Connection, name: str, category: str = "general", notes: str = "") -> int:
    organization_id = ensure_organization(connection, name, category=category, notes=notes)
    if organization_id is None:
        raise ValueError("organization name is required")
    return organization_id


def add_event(
    connection: sqlite3.Connection,
    title: str,
    start_at: datetime,
    end_at: datetime,
    organization_name: Optional[str] = None,
    location: str = "",
    kind: str = "event",
    status: str = "confirmed",
    source: str = "manual",
    notes: str = "",
    all_day: bool = False,
) -> int:
    if end_at < start_at:
        raise ValueError("event end must be after the start")

    organization_id = ensure_organization(connection, organization_name)
    cursor = connection.execute(
        """
        INSERT INTO events (
            title, start_at, end_at, all_day, location, organization_id, kind, status, source, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            title,
            _iso_minute(start_at),
            _iso_minute(end_at),
            1 if all_day else 0,
            location,
            organization_id,
            kind,
            status,
            source,
            notes,
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def add_communication(
    connection: sqlite3.Connection,
    subject: str,
    channel: str,
    happened_at: datetime,
    follow_up_at: Optional[datetime] = None,
    person: str = "",
    organization_name: Optional[str] = None,
    notes: str = "",
) -> int:
    organization_id = ensure_organization(connection, organization_name)
    cursor = connection.execute(
        """
        INSERT INTO communications (
            subject, channel, person, organization_id, happened_at, follow_up_at, status, notes, source, snippet
        ) VALUES (?, ?, ?, ?, ?, ?, 'open', ?, 'manual', '')
        """,
        (
            subject,
            channel,
            person,
            organization_id,
            _iso_minute(happened_at),
            _iso_minute(follow_up_at) if follow_up_at else None,
            notes,
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def upsert_event_from_sync(
    connection: sqlite3.Connection,
    *,
    source: str,
    external_id: str,
    title: str,
    start_at: datetime,
    end_at: datetime,
    all_day: bool,
    organization_name: Optional[str] = None,
    location: str = "",
    kind: str = "event",
    status: str = "confirmed",
    notes: str = "",
    external_calendar_id: Optional[str] = None,
    external_etag: Optional[str] = None,
    html_link: Optional[str] = None,
) -> int:
    organization_id = ensure_organization(connection, organization_name)
    existing = connection.execute(
        "SELECT id FROM events WHERE source = ? AND external_id = ?",
        (source, external_id),
    ).fetchone()

    values = (
        title,
        _iso_minute(start_at),
        _iso_minute(end_at),
        1 if all_day else 0,
        location,
        organization_id,
        kind,
        status,
        notes,
        external_calendar_id,
        external_etag,
        html_link,
    )

    if existing:
        connection.execute(
            """
            UPDATE events
            SET title = ?, start_at = ?, end_at = ?, all_day = ?, location = ?, organization_id = ?,
                kind = ?, status = ?, notes = ?, external_calendar_id = ?, external_etag = ?, html_link = ?
            WHERE id = ?
            """,
            values + (int(existing["id"]),),
        )
        connection.commit()
        return int(existing["id"])

    cursor = connection.execute(
        """
        INSERT INTO events (
            title, start_at, end_at, all_day, location, organization_id, kind, status, source, notes,
            external_id, external_calendar_id, external_etag, html_link
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        values[:8]
        + (
            source,
            notes,
            external_id,
            external_calendar_id,
            external_etag,
            html_link,
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def upsert_communication_from_sync(
    connection: sqlite3.Connection,
    *,
    source: str,
    external_id: str,
    subject: str,
    channel: str,
    happened_at: datetime,
    follow_up_at: Optional[datetime],
    direction: str = "inbound",
    person: str = "",
    organization_name: Optional[str] = None,
    notes: str = "",
    status: str = "open",
    external_thread_id: Optional[str] = None,
    external_from: Optional[str] = None,
    external_to: str = "",
    external_cc: str = "",
    external_bcc: str = "",
    external_reply_to: str = "",
    from_value: Optional[dict] = None,
    to_recipients: Optional[list[dict]] = None,
    cc_recipients: Optional[list[dict]] = None,
    bcc_recipients: Optional[list[dict]] = None,
    reply_to_recipients: Optional[list[dict]] = None,
    message_id: str = "",
    in_reply_to: str = "",
    references: Optional[list[str]] = None,
    headers: Optional[dict] = None,
    thread_key: str = "",
    snippet: str = "",
    body_text: str = "",
    html_body: str = "",
    attachments: Optional[list[dict]] = None,
    raw_relative_path: str = "",
    raw_sha256: str = "",
    category: str = "",
    categories: Optional[list[str]] = None,
    priority_level: str = "",
    priority_score: int = 0,
    retention_bucket: str = "",
    classifier_version: str = "",
    classification: Optional[dict] = None,
) -> int:
    organization_id = ensure_organization(connection, organization_name)
    existing = connection.execute(
        "SELECT id, status, follow_up_at FROM communications WHERE source = ? AND external_id = ?",
        (source, external_id),
    ).fetchone()

    follow_up_text = _iso_minute(follow_up_at) if follow_up_at else None
    if existing and existing["status"] == "done":
        persisted_status = "done"
        persisted_follow_up = existing["follow_up_at"]
    else:
        persisted_status = status
        persisted_follow_up = follow_up_text

    values = (
        subject,
        channel,
        direction,
        person,
        organization_id,
        _iso_minute(happened_at),
        persisted_follow_up,
        persisted_status,
        notes,
        external_thread_id,
        external_from,
        external_to,
        external_cc,
        external_bcc,
        external_reply_to,
        _json_text(from_value, {}),
        _json_text(to_recipients, []),
        _json_text(cc_recipients, []),
        _json_text(bcc_recipients, []),
        _json_text(reply_to_recipients, []),
        message_id,
        in_reply_to,
        _json_text(references, []),
        _json_text(headers, {}),
        thread_key,
        snippet,
        body_text,
        html_body,
        _json_text(attachments, []),
        raw_relative_path,
        raw_sha256,
        category,
        _json_text(categories, []),
        priority_level,
        priority_score,
        retention_bucket,
        classifier_version,
        _json_text(classification, {}),
    )

    if existing:
        connection.execute(
            """
            UPDATE communications
            SET subject = ?, channel = ?, direction = ?, person = ?, organization_id = ?, happened_at = ?,
                follow_up_at = ?, status = ?, notes = ?, external_thread_id = ?, external_from = ?, external_to = ?,
                external_cc = ?, external_bcc = ?, external_reply_to = ?, from_json = ?, to_json = ?, cc_json = ?,
                bcc_json = ?, reply_to_json = ?, message_id = ?, in_reply_to = ?, references_json = ?, headers_json = ?,
                thread_key = ?, snippet = ?, body_text = ?, html_body = ?, attachments_json = ?, raw_relative_path = ?,
                raw_sha256 = ?, category = ?, categories_json = ?, priority_level = ?, priority_score = ?,
                retention_bucket = ?, classifier_version = ?, classification_json = ?
            WHERE id = ?
            """,
            values + (int(existing["id"]),),
        )
        connection.commit()
        return int(existing["id"])

    cursor = connection.execute(
        """
        INSERT INTO communications (
            subject, channel, direction, person, organization_id, happened_at, follow_up_at, status, notes,
            source, external_id, external_thread_id, external_from, external_to, external_cc, external_bcc,
            external_reply_to, from_json, to_json, cc_json, bcc_json, reply_to_json, message_id, in_reply_to,
            references_json, headers_json, thread_key, snippet, body_text, html_body, attachments_json,
            raw_relative_path, raw_sha256, category, categories_json, priority_level, priority_score,
            retention_bucket, classifier_version, classification_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        values[:9]
        + (
            source,
            external_id,
            external_thread_id,
            external_from,
            external_to,
            external_cc,
            external_bcc,
            external_reply_to,
            _json_text(from_value, {}),
            _json_text(to_recipients, []),
            _json_text(cc_recipients, []),
            _json_text(bcc_recipients, []),
            _json_text(reply_to_recipients, []),
            message_id,
            in_reply_to,
            _json_text(references, []),
            _json_text(headers, {}),
            thread_key,
            snippet,
            body_text,
            html_body,
            _json_text(attachments, []),
            raw_relative_path,
            raw_sha256,
            category,
            _json_text(categories, []),
            priority_level,
            priority_score,
            retention_bucket,
            classifier_version,
            _json_text(classification, {}),
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def replace_profile_context_items(
    connection: sqlite3.Connection,
    *,
    source: Optional[str] = None,
    statuses: Optional[list[str]] = None,
) -> int:
    clauses = []
    params: list = []

    if source:
        clauses.append("source = ?")
        params.append(source)
    if statuses:
        placeholders = ", ".join("?" for _ in statuses)
        clauses.append(f"status IN ({placeholders})")
        params.extend(statuses)

    where_clause = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    cursor = connection.execute(f"DELETE FROM profile_context_items{where_clause}", params)
    connection.commit()
    return int(cursor.rowcount)


def upsert_profile_context_item(
    connection: sqlite3.Connection,
    *,
    external_key: str,
    subject_key: str,
    item_type: str,
    title: str,
    source: str,
    communication_id: Optional[int],
    happened_at: datetime,
    confidence: int = 0,
    status: str = "candidate",
    details: Optional[dict] = None,
    evidence: Optional[list[dict]] = None,
) -> int:
    existing = connection.execute(
        "SELECT id, status, review_notes, reviewed_at FROM profile_context_items WHERE external_key = ?",
        (external_key,),
    ).fetchone()

    stored_status = status
    review_notes = ""
    reviewed_at = None
    if existing and status == "candidate" and str(existing["status"] or "") in {"approved", "rejected"}:
        stored_status = str(existing["status"])
        review_notes = str(existing["review_notes"] or "")
        reviewed_at = str(existing["reviewed_at"]) if existing["reviewed_at"] else None

    values = (
        subject_key,
        item_type,
        title,
        source,
        communication_id,
        _iso_minute(happened_at),
        confidence,
        stored_status,
        _json_text(details, {}),
        _json_text(evidence, []),
        review_notes,
        reviewed_at,
    )

    if existing:
        connection.execute(
            """
            UPDATE profile_context_items
            SET subject_key = ?, item_type = ?, title = ?, source = ?, communication_id = ?,
                happened_at = ?, confidence = ?, status = ?, details_json = ?, evidence_json = ?,
                review_notes = ?, reviewed_at = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            values + (int(existing["id"]),),
        )
        connection.commit()
        return int(existing["id"])

    cursor = connection.execute(
        """
        INSERT INTO profile_context_items (
            external_key, subject_key, item_type, title, source, communication_id, happened_at,
            confidence, status, details_json, evidence_json, review_notes, reviewed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            external_key,
            subject_key,
            item_type,
            title,
            source,
            communication_id,
            _iso_minute(happened_at),
            confidence,
            stored_status,
            _json_text(details, {}),
            _json_text(evidence, []),
            review_notes,
            reviewed_at,
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def next_profile_context_review_item(
    connection: sqlite3.Connection,
    *,
    subject_key: Optional[str] = None,
    item_type: Optional[str] = None,
    source: Optional[str] = None,
) -> Optional[sqlite3.Row]:
    clauses = ["profile_context_items.status = 'candidate'"]
    params: list = []

    if subject_key and subject_key != "all":
        clauses.append("profile_context_items.subject_key = ?")
        params.append(subject_key)
    if item_type and item_type != "all":
        clauses.append("profile_context_items.item_type = ?")
        params.append(item_type)
    if source and source != "all":
        clauses.append("profile_context_items.source = ?")
        params.append(source)

    where_clause = " AND ".join(clauses)
    return connection.execute(
        f"""
        SELECT profile_context_items.*, communications.subject AS communication_subject
        FROM profile_context_items
        LEFT JOIN communications ON communications.id = profile_context_items.communication_id
        WHERE {where_clause}
        ORDER BY profile_context_items.confidence DESC, profile_context_items.happened_at DESC, profile_context_items.id DESC
        LIMIT 1
        """,
        params,
    ).fetchone()


def update_profile_context_item_status(
    connection: sqlite3.Connection,
    *,
    item_id: int,
    status: str,
    review_notes: str = "",
) -> None:
    connection.execute(
        """
        UPDATE profile_context_items
        SET status = ?, review_notes = ?, reviewed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (status, review_notes, item_id),
    )
    connection.commit()


def get_profile_context_item(connection: sqlite3.Connection, item_id: int) -> Optional[sqlite3.Row]:
    return connection.execute(
        """
        SELECT profile_context_items.*, communications.subject AS communication_subject
        FROM profile_context_items
        LEFT JOIN communications ON communications.id = profile_context_items.communication_id
        WHERE profile_context_items.id = ?
        """,
        (item_id,),
    ).fetchone()


def ensure_profile_subject(
    connection: sqlite3.Connection,
    *,
    subject_key: str,
    display_name: str,
    relationship: str = "",
    status: str = "active",
    notes: str = "",
) -> int:
    existing = connection.execute(
        "SELECT id FROM profile_subjects WHERE subject_key = ?",
        (subject_key,),
    ).fetchone()

    if existing:
        connection.execute(
            """
            UPDATE profile_subjects
            SET display_name = ?, relationship = ?, status = ?, notes = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (display_name, relationship, status, notes, int(existing["id"])),
        )
        connection.commit()
        return int(existing["id"])

    cursor = connection.execute(
        """
        INSERT INTO profile_subjects (
            subject_key, display_name, relationship, status, notes
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (subject_key, display_name, relationship, status, notes),
    )
    connection.commit()
    return int(cursor.lastrowid)


def get_profile_subject_by_key(connection: sqlite3.Connection, subject_key: str) -> Optional[sqlite3.Row]:
    return connection.execute(
        """
        SELECT *
        FROM profile_subjects
        WHERE subject_key = ?
        """,
        (subject_key,),
    ).fetchone()


def create_profile_record(
    connection: sqlite3.Connection,
    *,
    subject_id: int,
    item_type: str,
    record_kind: str,
    title: str,
    status: str,
    source: str,
    happened_at: datetime,
    confidence: int = 0,
    notes: str = "",
    details: Optional[dict] = None,
    evidence: Optional[list[dict]] = None,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO profile_records (
            subject_id, item_type, record_kind, title, status, source, happened_at,
            confidence, notes, details_json, evidence_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            subject_id,
            item_type,
            record_kind,
            title,
            status,
            source,
            _iso_minute(happened_at),
            confidence,
            notes,
            _json_text(details, {}),
            _json_text(evidence, []),
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def update_profile_record(
    connection: sqlite3.Connection,
    *,
    record_id: int,
    title: str,
    status: str,
    confidence: int,
    notes: str,
    details: Optional[dict] = None,
    evidence: Optional[list[dict]] = None,
) -> None:
    connection.execute(
        """
        UPDATE profile_records
        SET title = ?, status = ?, confidence = ?, notes = ?,
            details_json = ?, evidence_json = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (
            title,
            status,
            confidence,
            notes,
            _json_text(details, {}),
            _json_text(evidence, []),
            record_id,
        ),
    )
    connection.commit()


def get_profile_record(connection: sqlite3.Connection, record_id: int) -> Optional[sqlite3.Row]:
    return connection.execute(
        """
        SELECT profile_records.*, profile_subjects.subject_key, profile_subjects.display_name, profile_subjects.relationship
        FROM profile_records
        JOIN profile_subjects ON profile_subjects.id = profile_records.subject_id
        WHERE profile_records.id = ?
        """,
        (record_id,),
    ).fetchone()


def list_profile_records(
    connection: sqlite3.Connection,
    *,
    subject_key: Optional[str] = None,
    item_type: Optional[str] = None,
    status: Optional[str] = None,
    limit: Optional[int] = 100,
) -> list[sqlite3.Row]:
    clauses = []
    params: list = []

    if subject_key and subject_key != "all":
        clauses.append("profile_subjects.subject_key = ?")
        params.append(subject_key)
    if item_type and item_type != "all":
        clauses.append("profile_records.item_type = ?")
        params.append(item_type)
    if status and status != "all":
        clauses.append("profile_records.status = ?")
        params.append(status)

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    limit_clause = ""
    if limit is not None:
        params.append(limit)
        limit_clause = "LIMIT ?"

    return connection.execute(
        f"""
        SELECT profile_records.*, profile_subjects.subject_key, profile_subjects.display_name, profile_subjects.relationship
        FROM profile_records
        JOIN profile_subjects ON profile_subjects.id = profile_records.subject_id
        {where_clause}
        ORDER BY profile_records.happened_at DESC, profile_records.id DESC
        {limit_clause}
        """,
        params,
    ).fetchall()


def summarize_profile_records(
    connection: sqlite3.Connection,
    *,
    subject_key: Optional[str] = None,
    item_type: Optional[str] = None,
    status: Optional[str] = None,
) -> dict:
    clauses = []
    params: list = []

    if subject_key and subject_key != "all":
        clauses.append("profile_subjects.subject_key = ?")
        params.append(subject_key)
    if item_type and item_type != "all":
        clauses.append("profile_records.item_type = ?")
        params.append(item_type)
    if status and status != "all":
        clauses.append("profile_records.status = ?")
        params.append(status)

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    total = connection.execute(
        f"""
        SELECT COUNT(*) AS c
        FROM profile_records
        JOIN profile_subjects ON profile_subjects.id = profile_records.subject_id
        {where_clause}
        """,
        params,
    ).fetchone()
    by_subject = connection.execute(
        f"""
        SELECT profile_subjects.subject_key, COUNT(*) AS c
        FROM profile_records
        JOIN profile_subjects ON profile_subjects.id = profile_records.subject_id
        {where_clause}
        GROUP BY profile_subjects.subject_key
        ORDER BY c DESC, profile_subjects.subject_key
        """,
        params,
    ).fetchall()
    by_item_type = connection.execute(
        f"""
        SELECT profile_records.item_type, COUNT(*) AS c
        FROM profile_records
        JOIN profile_subjects ON profile_subjects.id = profile_records.subject_id
        {where_clause}
        GROUP BY profile_records.item_type
        ORDER BY c DESC, profile_records.item_type
        """,
        params,
    ).fetchall()
    by_status = connection.execute(
        f"""
        SELECT profile_records.status, COUNT(*) AS c
        FROM profile_records
        JOIN profile_subjects ON profile_subjects.id = profile_records.subject_id
        {where_clause}
        GROUP BY profile_records.status
        ORDER BY c DESC, profile_records.status
        """,
        params,
    ).fetchall()

    return {
        "total": int(total["c"]) if total else 0,
        "by_subject": [{"subject_key": str(row["subject_key"]), "count": int(row["c"])} for row in by_subject],
        "by_item_type": [{"item_type": str(row["item_type"]), "count": int(row["c"])} for row in by_item_type],
        "by_status": [{"status": str(row["status"]), "count": int(row["c"])} for row in by_status],
    }


def link_profile_record_item(connection: sqlite3.Connection, *, record_id: int, profile_item_id: int) -> None:
    connection.execute(
        """
        INSERT OR IGNORE INTO profile_record_items (record_id, profile_item_id)
        VALUES (?, ?)
        """,
        (record_id, profile_item_id),
    )
    connection.commit()


def list_profile_record_items(connection: sqlite3.Connection, *, record_id: int) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT profile_context_items.*, communications.subject AS communication_subject
        FROM profile_record_items
        JOIN profile_context_items ON profile_context_items.id = profile_record_items.profile_item_id
        LEFT JOIN communications ON communications.id = profile_context_items.communication_id
        WHERE profile_record_items.record_id = ?
        ORDER BY profile_context_items.happened_at DESC, profile_context_items.id DESC
        """,
        (record_id,),
    ).fetchall()


def link_profile_record_attachment(connection: sqlite3.Connection, *, record_id: int, attachment_id: int) -> None:
    connection.execute(
        """
        INSERT OR IGNORE INTO profile_record_attachments (record_id, attachment_id)
        VALUES (?, ?)
        """,
        (record_id, attachment_id),
    )
    connection.commit()


def list_profile_record_attachments(connection: sqlite3.Connection, *, record_id: int) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT communication_attachments.*, communications.subject AS communication_subject
        FROM profile_record_attachments
        JOIN communication_attachments ON communication_attachments.id = profile_record_attachments.attachment_id
        LEFT JOIN communications ON communications.id = communication_attachments.communication_id
        WHERE profile_record_attachments.record_id = ?
        ORDER BY communication_attachments.created_at DESC, communication_attachments.id DESC
        """,
        (record_id,),
    ).fetchall()


def upsert_communication_attachment(
    connection: sqlite3.Connection,
    *,
    external_key: str,
    communication_id: int,
    source: str,
    external_message_id: str,
    external_attachment_id: str,
    part_id: str,
    filename: str,
    mime_type: str,
    size: int,
    relative_path: str,
    extracted_text: str = "",
    extracted_text_path: str = "",
    extraction_method: str = "",
    ingest_status: str = "pending",
    error_text: str = "",
    sha256: str = "",
) -> int:
    existing = connection.execute(
        "SELECT id FROM communication_attachments WHERE external_key = ?",
        (external_key,),
    ).fetchone()

    values = (
        communication_id,
        source,
        external_message_id,
        external_attachment_id,
        part_id,
        filename,
        mime_type,
        size,
        relative_path,
        extracted_text,
        extracted_text_path,
        extraction_method,
        ingest_status,
        error_text,
        sha256,
    )

    if existing:
        connection.execute(
            """
            UPDATE communication_attachments
            SET communication_id = ?, source = ?, external_message_id = ?, external_attachment_id = ?,
                part_id = ?, filename = ?, mime_type = ?, size = ?, relative_path = ?, extracted_text = ?,
                extracted_text_path = ?, extraction_method = ?, ingest_status = ?, error_text = ?, sha256 = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            values + (int(existing["id"]),),
        )
        connection.commit()
        return int(existing["id"])

    cursor = connection.execute(
        """
        INSERT INTO communication_attachments (
            external_key, communication_id, source, external_message_id, external_attachment_id, part_id,
            filename, mime_type, size, relative_path, extracted_text, extracted_text_path, extraction_method,
            ingest_status, error_text, sha256
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            external_key,
            communication_id,
            source,
            external_message_id,
            external_attachment_id,
            part_id,
            filename,
            mime_type,
            size,
            relative_path,
            extracted_text,
            extracted_text_path,
            extraction_method,
            ingest_status,
            error_text,
            sha256,
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def add_routine(
    connection: sqlite3.Connection,
    name: str,
    cadence: str,
    start_time: str,
    duration_minutes: int,
    day_of_week: Optional[int] = None,
    notes: str = "",
) -> int:
    if cadence == "weekly" and day_of_week is None:
        raise ValueError("weekly routines require a day_of_week")
    if cadence == "daily":
        day_of_week = None

    cursor = connection.execute(
        """
        INSERT INTO routines (
            name, cadence, day_of_week, start_time, duration_minutes, notes
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            name,
            cadence,
            day_of_week,
            start_time,
            duration_minutes,
            notes,
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def mark_communication_done(connection: sqlite3.Connection, communication_id: int) -> None:
    connection.execute(
        "UPDATE communications SET status = 'done' WHERE id = ?",
        (communication_id,),
    )
    connection.commit()


def list_routines(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        "SELECT * FROM routines ORDER BY cadence, day_of_week, start_time, name"
    ).fetchall()


def list_events_between(connection: sqlite3.Connection, start_at: datetime, end_at: datetime) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT events.*, organizations.name AS organization_name
        FROM events
        LEFT JOIN organizations ON organizations.id = events.organization_id
        WHERE events.end_at >= ? AND events.start_at <= ?
          AND events.status != 'cancelled'
        ORDER BY events.start_at, events.end_at, events.title
        """,
        (_iso_minute(start_at), _iso_minute(end_at)),
    ).fetchall()


def delete_events_for_calendar(connection: sqlite3.Connection, *, source: str, external_calendar_id: str) -> int:
    cursor = connection.execute(
        "DELETE FROM events WHERE source = ? AND external_calendar_id = ?",
        (source, external_calendar_id),
    )
    connection.commit()
    return int(cursor.rowcount)


def delete_open_communications_not_in_ids(
    connection: sqlite3.Connection,
    *,
    source: str,
    keep_external_ids: set[str],
) -> int:
    if keep_external_ids:
        placeholders = ", ".join("?" for _ in keep_external_ids)
        cursor = connection.execute(
            f"""
            DELETE FROM communications
            WHERE source = ?
              AND status = 'open'
              AND external_id IS NOT NULL
              AND external_id NOT IN ({placeholders})
            """,
            (source, *sorted(keep_external_ids)),
        )
    else:
        cursor = connection.execute(
            """
            DELETE FROM communications
            WHERE source = ?
              AND status = 'open'
              AND external_id IS NOT NULL
            """,
            (source,),
        )

    connection.commit()
    return int(cursor.rowcount)


def delete_open_communications_without_prefix(
    connection: sqlite3.Connection,
    *,
    source: str,
    external_id_prefix: str,
) -> int:
    cursor = connection.execute(
        """
        DELETE FROM communications
        WHERE source = ?
          AND status = 'open'
          AND external_id IS NOT NULL
          AND external_id NOT LIKE ?
        """,
        (source, f"{external_id_prefix}%"),
    )
    connection.commit()
    return int(cursor.rowcount)


def delete_communications_by_statuses(
    connection: sqlite3.Connection,
    *,
    source: str,
    statuses: list[str],
) -> int:
    if not statuses:
        return 0
    placeholders = ", ".join("?" for _ in statuses)
    cursor = connection.execute(
        f"""
        DELETE FROM communications
        WHERE source = ?
          AND status IN ({placeholders})
        """,
        (source, *statuses),
    )
    connection.commit()
    return int(cursor.rowcount)


def list_followups_between(connection: sqlite3.Connection, start_at: datetime, end_at: datetime) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT communications.*, organizations.name AS organization_name
        FROM communications
        LEFT JOIN organizations ON organizations.id = communications.organization_id
        WHERE communications.status = 'open'
          AND communications.follow_up_at IS NOT NULL
          AND communications.follow_up_at >= ?
          AND communications.follow_up_at <= ?
        ORDER BY communications.follow_up_at, communications.subject
        """,
        (_iso_minute(start_at), _iso_minute(end_at)),
    ).fetchall()


def list_communications(
    connection: sqlite3.Connection,
    *,
    status: Optional[str] = None,
    source: Optional[str] = None,
    category: Optional[str] = None,
    limit: Optional[int] = 100,
) -> list[sqlite3.Row]:
    clauses = []
    params: list = []

    if status and status != "all":
        clauses.append("communications.status = ?")
        params.append(status)
    if source:
        clauses.append("communications.source = ?")
        params.append(source)
    if category and category != "all":
        clauses.append("communications.category = ?")
        params.append(category)

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    limit_clause = ""
    if limit is not None:
        params.append(limit)
        limit_clause = "LIMIT ?"

    return connection.execute(
        f"""
        SELECT communications.*, organizations.name AS organization_name
        FROM communications
        LEFT JOIN organizations ON organizations.id = communications.organization_id
        {where_clause}
        ORDER BY communications.happened_at DESC, communications.id DESC
        {limit_clause}
        """,
        params,
    ).fetchall()


def get_communication_by_external_id(
    connection: sqlite3.Connection,
    *,
    source: str,
    external_id: str,
) -> Optional[sqlite3.Row]:
    return connection.execute(
        """
        SELECT communications.*, organizations.name AS organization_name
        FROM communications
        LEFT JOIN organizations ON organizations.id = communications.organization_id
        WHERE communications.source = ?
          AND communications.external_id = ?
        """,
        (source, external_id),
    ).fetchone()


def get_communication_by_id(connection: sqlite3.Connection, communication_id: int) -> Optional[sqlite3.Row]:
    return connection.execute(
        """
        SELECT communications.*, organizations.name AS organization_name
        FROM communications
        LEFT JOIN organizations ON organizations.id = communications.organization_id
        WHERE communications.id = ?
        """,
        (communication_id,),
    ).fetchone()


def list_profile_context_communications(
    connection: sqlite3.Connection,
    *,
    subject_key: Optional[str] = None,
    item_type: Optional[str] = None,
    status: Optional[str] = None,
    source: Optional[str] = None,
    limit: Optional[int] = 100,
) -> list[sqlite3.Row]:
    clauses = ["profile_context_items.communication_id IS NOT NULL", "communications.attachments_json != '[]'"]
    params: list = []

    if subject_key and subject_key != "all":
        clauses.append("profile_context_items.subject_key = ?")
        params.append(subject_key)
    if item_type and item_type != "all":
        clauses.append("profile_context_items.item_type = ?")
        params.append(item_type)
    if status and status != "all":
        clauses.append("profile_context_items.status = ?")
        params.append(status)
    if source and source != "all":
        clauses.append("profile_context_items.source = ?")
        params.append(source)

    where_clause = f"WHERE {' AND '.join(clauses)}"
    limit_clause = ""
    if limit is not None:
        params.append(limit)
        limit_clause = "LIMIT ?"

    return connection.execute(
        f"""
        SELECT communications.*, organizations.name AS organization_name,
               MAX(profile_context_items.confidence) AS profile_confidence
        FROM profile_context_items
        JOIN communications ON communications.id = profile_context_items.communication_id
        LEFT JOIN organizations ON organizations.id = communications.organization_id
        {where_clause}
        GROUP BY communications.id
        ORDER BY profile_confidence DESC, communications.happened_at DESC, communications.id DESC
        {limit_clause}
        """,
        params,
    ).fetchall()


def list_communication_attachments(
    connection: sqlite3.Connection,
    *,
    communication_id: Optional[int] = None,
    ingest_status: Optional[str] = None,
    source: Optional[str] = None,
    limit: Optional[int] = 100,
) -> list[sqlite3.Row]:
    clauses = []
    params: list = []

    if communication_id is not None:
        clauses.append("communication_attachments.communication_id = ?")
        params.append(communication_id)
    if ingest_status and ingest_status != "all":
        clauses.append("communication_attachments.ingest_status = ?")
        params.append(ingest_status)
    if source and source != "all":
        clauses.append("communication_attachments.source = ?")
        params.append(source)

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    limit_clause = ""
    if limit is not None:
        params.append(limit)
        limit_clause = "LIMIT ?"

    return connection.execute(
        f"""
        SELECT communication_attachments.*, communications.subject AS communication_subject
        FROM communication_attachments
        LEFT JOIN communications ON communications.id = communication_attachments.communication_id
        {where_clause}
        ORDER BY communication_attachments.created_at DESC, communication_attachments.id DESC
        {limit_clause}
        """,
        params,
    ).fetchall()


def summarize_communication_attachments(
    connection: sqlite3.Connection,
    *,
    ingest_status: Optional[str] = None,
    source: Optional[str] = None,
) -> dict:
    clauses = []
    params: list = []

    if ingest_status and ingest_status != "all":
        clauses.append("ingest_status = ?")
        params.append(ingest_status)
    if source and source != "all":
        clauses.append("source = ?")
        params.append(source)

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    total = connection.execute(
        f"SELECT COUNT(*) AS c FROM communication_attachments {where_clause}",
        params,
    ).fetchone()
    by_status = connection.execute(
        f"""
        SELECT ingest_status, COUNT(*) AS c
        FROM communication_attachments
        {where_clause}
        GROUP BY ingest_status
        ORDER BY c DESC, ingest_status
        """,
        params,
    ).fetchall()
    by_method = connection.execute(
        f"""
        SELECT extraction_method, COUNT(*) AS c
        FROM communication_attachments
        {where_clause}
        GROUP BY extraction_method
        ORDER BY c DESC, extraction_method
        """,
        params,
    ).fetchall()

    return {
        "total": int(total["c"]) if total else 0,
        "by_status": [{"ingest_status": str(row["ingest_status"]), "count": int(row["c"])} for row in by_status],
        "by_method": [{"extraction_method": str(row["extraction_method"]), "count": int(row["c"])} for row in by_method],
    }


def combined_attachment_text(
    connection: sqlite3.Connection,
    *,
    communication_id: int,
    limit_chars: int = 12000,
) -> str:
    rows = connection.execute(
        """
        SELECT filename, extracted_text
        FROM communication_attachments
        WHERE communication_id = ?
          AND ingest_status = 'extracted'
          AND extracted_text != ''
        ORDER BY id
        """,
        (communication_id,),
    ).fetchall()
    combined = "\n\n".join(
        str(row["extracted_text"] or "").strip()
        for row in rows
        if row["extracted_text"] and not attachment_filename_is_low_signal(str(row["filename"] or ""))
    )
    if len(combined) <= limit_chars:
        return combined
    return combined[:limit_chars]


def summarize_communications(
    connection: sqlite3.Connection,
    *,
    source: Optional[str] = None,
    status: Optional[str] = None,
    category: Optional[str] = None,
) -> dict:
    clauses = []
    params: list = []

    if source:
        clauses.append("source = ?")
        params.append(source)
    if status and status != "all":
        clauses.append("status = ?")
        params.append(status)
    if category and category != "all":
        clauses.append("category = ?")
        params.append(category)

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    total = connection.execute(
        f"SELECT COUNT(*) AS c FROM communications {where_clause}",
        params,
    ).fetchone()

    by_status = connection.execute(
        f"""
        SELECT status, COUNT(*) AS c
        FROM communications
        {where_clause}
        GROUP BY status
        ORDER BY status
        """,
        params,
    ).fetchall()

    by_category = connection.execute(
        f"""
        SELECT category, COUNT(*) AS c
        FROM communications
        {where_clause}
        GROUP BY category
        ORDER BY c DESC, category
        """,
        params,
    ).fetchall()

    by_priority = connection.execute(
        f"""
        SELECT priority_level, COUNT(*) AS c
        FROM communications
        {where_clause}
        GROUP BY priority_level
        ORDER BY c DESC, priority_level
        """,
        params,
    ).fetchall()

    return {
        "total": int(total["c"]) if total else 0,
        "by_status": [{"status": str(row["status"]), "count": int(row["c"])} for row in by_status],
        "by_category": [{"category": str(row["category"]), "count": int(row["c"])} for row in by_category],
        "by_priority": [{"priority_level": str(row["priority_level"]), "count": int(row["c"])} for row in by_priority],
    }


def list_profile_context_items(
    connection: sqlite3.Connection,
    *,
    subject_key: Optional[str] = None,
    item_type: Optional[str] = None,
    status: Optional[str] = None,
    source: Optional[str] = None,
    limit: Optional[int] = 100,
) -> list[sqlite3.Row]:
    clauses = []
    params: list = []

    if subject_key and subject_key != "all":
        clauses.append("subject_key = ?")
        params.append(subject_key)
    if item_type and item_type != "all":
        clauses.append("item_type = ?")
        params.append(item_type)
    if status and status != "all":
        clauses.append("status = ?")
        params.append(status)
    if source and source != "all":
        clauses.append("source = ?")
        params.append(source)

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    limit_clause = ""
    if limit is not None:
        params.append(limit)
        limit_clause = "LIMIT ?"

    return connection.execute(
        f"""
        SELECT profile_context_items.*, communications.subject AS communication_subject
        FROM profile_context_items
        LEFT JOIN communications ON communications.id = profile_context_items.communication_id
        {where_clause}
        ORDER BY profile_context_items.happened_at DESC, profile_context_items.id DESC
        {limit_clause}
        """,
        params,
    ).fetchall()


def summarize_profile_context(
    connection: sqlite3.Connection,
    *,
    subject_key: Optional[str] = None,
    item_type: Optional[str] = None,
    status: Optional[str] = None,
    source: Optional[str] = None,
) -> dict:
    clauses = []
    params: list = []

    if subject_key and subject_key != "all":
        clauses.append("subject_key = ?")
        params.append(subject_key)
    if item_type and item_type != "all":
        clauses.append("item_type = ?")
        params.append(item_type)
    if status and status != "all":
        clauses.append("status = ?")
        params.append(status)
    if source and source != "all":
        clauses.append("source = ?")
        params.append(source)

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    total = connection.execute(
        f"SELECT COUNT(*) AS c FROM profile_context_items {where_clause}",
        params,
    ).fetchone()

    by_subject = connection.execute(
        f"""
        SELECT subject_key, COUNT(*) AS c
        FROM profile_context_items
        {where_clause}
        GROUP BY subject_key
        ORDER BY c DESC, subject_key
        """,
        params,
    ).fetchall()

    by_item_type = connection.execute(
        f"""
        SELECT item_type, COUNT(*) AS c
        FROM profile_context_items
        {where_clause}
        GROUP BY item_type
        ORDER BY c DESC, item_type
        """,
        params,
    ).fetchall()

    by_status = connection.execute(
        f"""
        SELECT status, COUNT(*) AS c
        FROM profile_context_items
        {where_clause}
        GROUP BY status
        ORDER BY c DESC, status
        """,
        params,
    ).fetchall()

    return {
        "total": int(total["c"]) if total else 0,
        "by_subject": [{"subject_key": str(row["subject_key"]), "count": int(row["c"])} for row in by_subject],
        "by_item_type": [{"item_type": str(row["item_type"]), "count": int(row["c"])} for row in by_item_type],
        "by_status": [{"status": str(row["status"]), "count": int(row["c"])} for row in by_status],
    }


def add_x_content_item(
    connection: sqlite3.Connection,
    *,
    platform: str = "x",
    kind: str,
    title: str,
    summary: str = "",
    body_text: str = "",
    status: str = "draft",
    parent_id: Optional[int] = None,
    sequence_index: Optional[int] = None,
    tags: Optional[list[str]] = None,
    metadata: Optional[dict] = None,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO x_content_items (
            platform, kind, title, summary, body_text, status, parent_id, sequence_index, tags_json, metadata_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            platform,
            kind,
            title,
            summary,
            body_text,
            status,
            parent_id,
            sequence_index,
            _json_text(tags or [], []),
            _json_text(metadata or {}, {}),
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def list_x_content_items(
    connection: sqlite3.Connection,
    *,
    platform: Optional[str] = "x",
    kind: Optional[str] = None,
    status: Optional[str] = None,
    parent_id: Optional[int] = None,
    limit: Optional[int] = 100,
) -> list[sqlite3.Row]:
    clauses = []
    params: list = []

    if platform and platform != "all":
        clauses.append("items.platform = ?")
        params.append(platform)
    if kind and kind != "all":
        clauses.append("items.kind = ?")
        params.append(kind)
    if status and status != "all":
        clauses.append("items.status = ?")
        params.append(status)
    if parent_id is None:
        clauses.append("items.parent_id IS NULL")
    elif parent_id >= 0:
        clauses.append("items.parent_id = ?")
        params.append(parent_id)

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    limit_clause = ""
    if limit is not None:
        params.append(limit)
        limit_clause = "LIMIT ?"

    return connection.execute(
        f"""
        SELECT
            items.*,
            parent.title AS parent_title
        FROM x_content_items AS items
        LEFT JOIN x_content_items AS parent ON parent.id = items.parent_id
        {where_clause}
        ORDER BY
            COALESCE(items.sequence_index, 999999),
            items.created_at DESC,
            items.id DESC
        {limit_clause}
        """,
        params,
    ).fetchall()


def get_x_content_item(connection: sqlite3.Connection, item_id: int) -> Optional[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
            items.*,
            parent.title AS parent_title
        FROM x_content_items AS items
        LEFT JOIN x_content_items AS parent ON parent.id = items.parent_id
        WHERE items.id = ?
        """,
        (item_id,),
    ).fetchone()


def list_x_content_children(connection: sqlite3.Connection, parent_id: int) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
            items.*,
            parent.title AS parent_title
        FROM x_content_items AS items
        LEFT JOIN x_content_items AS parent ON parent.id = items.parent_id
        WHERE items.parent_id = ?
        ORDER BY COALESCE(items.sequence_index, 999999), items.id
        """,
        (parent_id,),
    ).fetchall()


def add_x_media_asset(
    connection: sqlite3.Connection,
    *,
    content_item_id: Optional[int],
    asset_kind: str = "image",
    title: str,
    prompt_text: str,
    alt_text: str = "",
    status: str = "planned",
    model_name: str = "",
    relative_path: str = "",
    metadata: Optional[dict] = None,
    error_text: str = "",
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO x_media_assets (
            content_item_id, asset_kind, title, prompt_text, alt_text, status, model_name, relative_path, metadata_json, error_text
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            content_item_id,
            asset_kind,
            title,
            prompt_text,
            alt_text,
            status,
            model_name,
            relative_path,
            _json_text(metadata or {}, {}),
            error_text,
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def list_x_media_assets(
    connection: sqlite3.Connection,
    *,
    content_item_id: Optional[int] = None,
    asset_kind: Optional[str] = None,
    status: Optional[str] = None,
    limit: Optional[int] = 100,
) -> list[sqlite3.Row]:
    clauses = []
    params: list = []

    if content_item_id is not None:
        clauses.append("assets.content_item_id = ?")
        params.append(content_item_id)
    if asset_kind and asset_kind != "all":
        clauses.append("assets.asset_kind = ?")
        params.append(asset_kind)
    if status and status != "all":
        clauses.append("assets.status = ?")
        params.append(status)

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    limit_clause = ""
    if limit is not None:
        params.append(limit)
        limit_clause = "LIMIT ?"

    return connection.execute(
        f"""
        SELECT
            assets.*,
            content.title AS content_title,
            content.kind AS content_kind
        FROM x_media_assets AS assets
        LEFT JOIN x_content_items AS content ON content.id = assets.content_item_id
        {where_clause}
        ORDER BY assets.created_at DESC, assets.id DESC
        {limit_clause}
        """,
        params,
    ).fetchall()


def get_x_media_asset(connection: sqlite3.Connection, asset_id: int) -> Optional[sqlite3.Row]:
    return connection.execute(
        """
        SELECT
            assets.*,
            content.title AS content_title,
            content.kind AS content_kind
        FROM x_media_assets AS assets
        LEFT JOIN x_content_items AS content ON content.id = assets.content_item_id
        WHERE assets.id = ?
        """,
        (asset_id,),
    ).fetchone()


def update_x_media_asset(
    connection: sqlite3.Connection,
    *,
    asset_id: int,
    status: Optional[str] = None,
    model_name: Optional[str] = None,
    relative_path: Optional[str] = None,
    metadata: Optional[dict] = None,
    error_text: Optional[str] = None,
) -> None:
    existing = get_x_media_asset(connection, asset_id)
    if existing is None:
        raise ValueError(f"x media asset #{asset_id} was not found")

    merged_metadata = json.loads(str(existing["metadata_json"] or "{}"))
    if metadata:
        merged_metadata.update(metadata)

    connection.execute(
        """
        UPDATE x_media_assets
        SET
            status = ?,
            model_name = ?,
            relative_path = ?,
            metadata_json = ?,
            error_text = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (
            status if status is not None else str(existing["status"] or ""),
            model_name if model_name is not None else str(existing["model_name"] or ""),
            relative_path if relative_path is not None else str(existing["relative_path"] or ""),
            _json_text(merged_metadata, {}),
            error_text if error_text is not None else str(existing["error_text"] or ""),
            asset_id,
        ),
    )
    connection.commit()


def set_sync_state(connection: sqlite3.Connection, key: str, value: str) -> None:
    connection.execute(
        """
        INSERT INTO sync_state (key, value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            updated_at = CURRENT_TIMESTAMP
        """,
        (key, value),
    )
    connection.commit()


def get_sync_state(connection: sqlite3.Connection, key: str) -> Optional[str]:
    row = connection.execute(
        "SELECT value FROM sync_state WHERE key = ?",
        (key,),
    ).fetchone()
    if not row:
        return None
    return str(row["value"])


def enqueue_mail_delivery(
    connection: sqlite3.Connection,
    *,
    queue_key: str,
    provider: str,
    communication_id: int,
    payload: dict,
    metadata: Optional[dict] = None,
    status: str = "queued",
    attempt_count: int = 0,
    max_attempts: int = 8,
    next_attempt_at: Optional[str] = None,
    last_attempt_at: Optional[str] = None,
    provider_message_id: str = "",
    last_error: str = "",
) -> int:
    existing = connection.execute(
        "SELECT id FROM mail_delivery_queue WHERE queue_key = ?",
        (queue_key,),
    ).fetchone()
    values = (
        provider,
        communication_id,
        status,
        _json_text(payload, {}),
        _json_text(metadata, {}),
        int(attempt_count),
        int(max_attempts),
        next_attempt_at or _utc_now_iso(),
        last_attempt_at,
        provider_message_id,
        last_error,
    )
    if existing:
        connection.execute(
            """
            UPDATE mail_delivery_queue
            SET provider = ?, communication_id = ?, status = ?, payload_json = ?, metadata_json = ?,
                attempt_count = ?, max_attempts = ?, next_attempt_at = ?, last_attempt_at = ?,
                provider_message_id = ?, last_error = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            values + (int(existing["id"]),),
        )
        connection.commit()
        return int(existing["id"])

    cursor = connection.execute(
        """
        INSERT INTO mail_delivery_queue (
            queue_key, provider, communication_id, status, payload_json, metadata_json,
            attempt_count, max_attempts, next_attempt_at, last_attempt_at, provider_message_id, last_error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            queue_key,
            *values,
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def get_mail_delivery_queue_item(
    connection: sqlite3.Connection,
    *,
    queue_id: Optional[int] = None,
    queue_key: Optional[str] = None,
) -> Optional[sqlite3.Row]:
    if queue_id is None and not queue_key:
        raise ValueError("queue_id or queue_key is required")
    if queue_id is not None:
        return connection.execute(
            """
            SELECT mail_delivery_queue.*, communications.subject, communications.external_to
            FROM mail_delivery_queue
            LEFT JOIN communications ON communications.id = mail_delivery_queue.communication_id
            WHERE mail_delivery_queue.id = ?
            """,
            (queue_id,),
        ).fetchone()
    return connection.execute(
        """
        SELECT mail_delivery_queue.*, communications.subject, communications.external_to
        FROM mail_delivery_queue
        LEFT JOIN communications ON communications.id = mail_delivery_queue.communication_id
        WHERE mail_delivery_queue.queue_key = ?
        """,
        (queue_key,),
    ).fetchone()


def list_mail_delivery_queue(
    connection: sqlite3.Connection,
    *,
    provider: Optional[str] = None,
    status: Optional[str] = None,
    due_before: Optional[str] = None,
    limit: int = 100,
) -> list[sqlite3.Row]:
    clauses: list[str] = []
    params: list[object] = []
    if provider:
        clauses.append("mail_delivery_queue.provider = ?")
        params.append(provider)
    if status and status != "all":
        clauses.append("mail_delivery_queue.status = ?")
        params.append(status)
    if due_before:
        clauses.append("mail_delivery_queue.next_attempt_at <= ?")
        params.append(due_before)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(int(limit))
    return connection.execute(
        f"""
        SELECT mail_delivery_queue.*, communications.subject, communications.external_to
        FROM mail_delivery_queue
        LEFT JOIN communications ON communications.id = mail_delivery_queue.communication_id
        {where_clause}
        ORDER BY mail_delivery_queue.next_attempt_at, mail_delivery_queue.id
        LIMIT ?
        """,
        params,
    ).fetchall()


def update_mail_delivery_queue_item(
    connection: sqlite3.Connection,
    *,
    queue_id: int,
    status: Optional[str] = None,
    payload: Optional[dict] = None,
    metadata: Optional[dict] = None,
    attempt_count: Optional[int] = None,
    max_attempts: Optional[int] = None,
    next_attempt_at: Optional[str] = None,
    last_attempt_at: Optional[str] = None,
    provider_message_id: Optional[str] = None,
    last_error: Optional[str] = None,
) -> None:
    existing = get_mail_delivery_queue_item(connection, queue_id=queue_id)
    if not existing:
        raise KeyError(f"mail delivery queue item {queue_id} not found")
    merged_metadata = _json_value(str(existing["metadata_json"] or "{}"), {})
    if metadata is not None:
        merged_metadata = {**merged_metadata, **metadata}
    merged_payload = _json_value(str(existing["payload_json"] or "{}"), {})
    if payload is not None:
        merged_payload = payload
    connection.execute(
        """
        UPDATE mail_delivery_queue
        SET status = ?, payload_json = ?, metadata_json = ?, attempt_count = ?, max_attempts = ?,
            next_attempt_at = ?, last_attempt_at = ?, provider_message_id = ?, last_error = ?,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (
            status if status is not None else str(existing["status"] or ""),
            _json_text(merged_payload, {}),
            _json_text(merged_metadata, {}),
            int(attempt_count if attempt_count is not None else existing["attempt_count"]),
            int(max_attempts if max_attempts is not None else existing["max_attempts"]),
            next_attempt_at if next_attempt_at is not None else str(existing["next_attempt_at"] or ""),
            last_attempt_at if last_attempt_at is not None else existing["last_attempt_at"],
            provider_message_id if provider_message_id is not None else str(existing["provider_message_id"] or ""),
            last_error if last_error is not None else str(existing["last_error"] or ""),
            queue_id,
        ),
    )
    connection.commit()


def upsert_system_alert(
    connection: sqlite3.Connection,
    *,
    alert_key: str,
    source: str,
    severity: str,
    title: str,
    message: str = "",
    details: Optional[dict] = None,
    status: str = "active",
) -> int:
    now = _utc_now_iso()
    existing = connection.execute(
        "SELECT id, first_seen_at FROM system_alerts WHERE alert_key = ?",
        (alert_key,),
    ).fetchone()
    if existing:
        connection.execute(
            """
            UPDATE system_alerts
            SET source = ?, severity = ?, status = ?, title = ?, message = ?, details_json = ?,
                last_seen_at = ?, cleared_at = CASE WHEN ? = 'cleared' THEN ? ELSE NULL END,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                source,
                severity,
                status,
                title,
                message,
                _json_text(details, {}),
                now,
                status,
                now,
                int(existing["id"]),
            ),
        )
        connection.commit()
        return int(existing["id"])

    cursor = connection.execute(
        """
        INSERT INTO system_alerts (
            alert_key, source, severity, status, title, message, details_json,
            first_seen_at, last_seen_at, cleared_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            alert_key,
            source,
            severity,
            status,
            title,
            message,
            _json_text(details, {}),
            now,
            now,
            now if status == "cleared" else None,
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def clear_system_alert(connection: sqlite3.Connection, alert_key: str) -> None:
    existing = connection.execute(
        "SELECT id FROM system_alerts WHERE alert_key = ? AND status != 'cleared'",
        (alert_key,),
    ).fetchone()
    if not existing:
        return
    upsert_system_alert(
        connection,
        alert_key=alert_key,
        source=str(connection.execute("SELECT source FROM system_alerts WHERE id = ?", (int(existing["id"]),)).fetchone()["source"]),
        severity=str(connection.execute("SELECT severity FROM system_alerts WHERE id = ?", (int(existing["id"]),)).fetchone()["severity"]),
        title=str(connection.execute("SELECT title FROM system_alerts WHERE id = ?", (int(existing["id"]),)).fetchone()["title"]),
        message="",
        details={},
        status="cleared",
    )


def list_system_alerts(
    connection: sqlite3.Connection,
    *,
    source: Optional[str] = None,
    status: Optional[str] = "active",
    limit: int = 50,
) -> list[sqlite3.Row]:
    clauses: list[str] = []
    params: list[object] = []
    if source:
        clauses.append("source = ?")
        params.append(source)
    if status and status != "all":
        clauses.append("status = ?")
        params.append(status)
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(int(limit))
    return connection.execute(
        f"""
        SELECT *
        FROM system_alerts
        {where_clause}
        ORDER BY
            CASE severity
                WHEN 'critical' THEN 0
                WHEN 'error' THEN 1
                WHEN 'warning' THEN 2
                ELSE 3
            END,
            last_seen_at DESC,
            id DESC
        LIMIT ?
        """,
        params,
    ).fetchall()


def seed_demo(connection: sqlite3.Connection, anchor: Optional[date] = None) -> bool:
    existing_rows = connection.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM organizations) AS organization_count,
            (SELECT COUNT(*) FROM events) AS event_count,
            (SELECT COUNT(*) FROM communications) AS communication_count,
            (SELECT COUNT(*) FROM routines) AS routine_count
        """
    ).fetchone()
    if existing_rows and any(int(existing_rows[key]) > 0 for key in existing_rows.keys()):
        return False

    today = anchor or date.today()
    monday = today - timedelta(days=today.weekday())

    add_organization(connection, "Primary Work", category="work")
    add_organization(connection, "Health", category="personal")
    add_organization(connection, "Friends and Family", category="relationship")

    add_routine(
        connection,
        name="Morning planning",
        cadence="daily",
        start_time="08:30",
        duration_minutes=30,
        notes="Review the next 7 days and lock in the top priorities.",
    )
    add_routine(
        connection,
        name="Comms sweep",
        cadence="daily",
        start_time="11:30",
        duration_minutes=25,
        notes="Respond, delegate, or turn open loops into follow-up items.",
    )
    add_routine(
        connection,
        name="Shutdown ritual",
        cadence="daily",
        start_time="17:30",
        duration_minutes=20,
        notes="Check tomorrow, clear loose ends, and reset the system.",
    )
    add_routine(
        connection,
        name="Weekly review",
        cadence="weekly",
        day_of_week=6,
        start_time="18:00",
        duration_minutes=60,
        notes="Review the next 7 days and rebalance commitments.",
    )

    add_event(
        connection,
        title="Weekly planning block",
        start_at=datetime.combine(monday, time(9, 0)),
        end_at=datetime.combine(monday, time(10, 0)),
        organization_name="Primary Work",
        kind="planning",
        notes="Set priorities and prune commitments.",
    )
    add_event(
        connection,
        title="Health admin block",
        start_at=datetime.combine(monday + timedelta(days=1), time(14, 0)),
        end_at=datetime.combine(monday + timedelta(days=1), time(14, 45)),
        organization_name="Health",
        kind="admin",
        notes="Handle appointments, prescriptions, or paperwork.",
    )
    add_event(
        connection,
        title="Relationship touchpoint block",
        start_at=datetime.combine(monday + timedelta(days=3), time(16, 0)),
        end_at=datetime.combine(monday + timedelta(days=3), time(16, 30)),
        organization_name="Friends and Family",
        kind="relationship",
        notes="Reach out instead of letting the week run away.",
    )

    add_communication(
        connection,
        subject="Reply to investor follow-up",
        channel="email",
        happened_at=datetime.combine(today, time(9, 15)),
        follow_up_at=datetime.combine(today + timedelta(days=1), time(13, 0)),
        organization_name="Primary Work",
        notes="Keep the thread moving while context is still fresh.",
    )
    add_communication(
        connection,
        subject="Schedule annual physical",
        channel="phone",
        happened_at=datetime.combine(today, time(10, 5)),
        follow_up_at=datetime.combine(today + timedelta(days=2), time(15, 0)),
        organization_name="Health",
        notes="Confirm availability and paperwork requirements.",
    )
    return True
