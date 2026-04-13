from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sqlite3
import tempfile
import time as time_module
from datetime import date, datetime, time, timedelta, timezone
from email.utils import getaddresses, parseaddr
from pathlib import Path
from typing import Any, Optional

from life_ops import mail_vault
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

MAIL_UI_HIDDEN_CONTACTS_SYNC_KEY = "mail_ui:hidden_contacts"
MAIL_CONTACTS_BACKFILLED_SYNC_KEY = "mail_contacts:backfilled_at"
MAIL_CONTACTS_CLEANUP_SYNC_KEY = "mail_contacts:cleanup_v1"
LIST_ITEM_NAMES = ("personal", "professional")
LIST_ITEM_STATUSES = ("open", "done")

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
    classification_json TEXT NOT NULL DEFAULT '{}',
    deleted_at TEXT
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

CREATE TABLE IF NOT EXISTS list_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    list_name TEXT NOT NULL CHECK (list_name IN ('personal', 'professional')),
    title TEXT NOT NULL,
    notes TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'done')),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS sync_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mail_contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    contact_key TEXT NOT NULL UNIQUE,
    email TEXT NOT NULL DEFAULT '',
    display_name TEXT NOT NULL DEFAULT '',
    first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    interaction_count INTEGER NOT NULL DEFAULT 1,
    last_direction TEXT NOT NULL DEFAULT '',
    last_source TEXT NOT NULL DEFAULT '',
    last_communication_id INTEGER REFERENCES communications(id) ON DELETE SET NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
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
LIFE_OPS_HOME_ENV = "LIFE_OPS_HOME"
LIFE_OPS_PACKAGE_ROOT_ENV = "LIFE_OPS_PACKAGE_ROOT"
ENCRYPTED_DB_SUFFIX = ".enc.json"
DB_LOCK_SUFFIX = ".lock"
DELETED_COMMUNICATION_RETENTION_DAYS = 30


def package_root() -> Path:
    override = str(os.getenv(LIFE_OPS_PACKAGE_ROOT_ENV) or "").strip()
    if override:
        return Path(override).expanduser().resolve(strict=False)
    return Path(__file__).resolve().parents[2]


def repo_root() -> Path:
    return package_root()


def life_ops_home() -> Path:
    override = str(os.getenv(LIFE_OPS_HOME_ENV) or "").strip()
    if override:
        return Path(override).expanduser().resolve(strict=False)
    package = package_root()
    if (package / ".git").exists() or (package / "pyproject.toml").exists():
        return package
    return Path.home() / ".lifeops"


def data_root() -> Path:
    return life_ops_home() / "data"


def config_root() -> Path:
    return life_ops_home() / "config"


def default_db_path() -> Path:
    return data_root() / "life_ops.db"


def attachment_vault_root() -> Path:
    return data_root() / "attachments"


def x_media_root() -> Path:
    return data_root() / "x_media"


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
    remove_plaintext_db_artifacts(db_path)
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
    connection.execute("PRAGMA busy_timeout = 30000")
    if encrypted_storage:
        connection.execute("PRAGMA journal_mode = MEMORY")
    else:
        connection.execute("PRAGMA journal_mode = WAL")
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
        _ensure_mail_contacts_backfilled(connection)

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
    _ensure_mail_contacts_backfilled(connection)
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


def _utc_now_string() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
    _ensure_column(connection, "communications", "deleted_at", "TEXT")

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

    _ensure_column(connection, "mail_contacts", "contact_key", "TEXT")
    _ensure_column(connection, "mail_contacts", "email", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "mail_contacts", "display_name", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "mail_contacts", "first_seen_at", "TEXT")
    _ensure_column(connection, "mail_contacts", "last_seen_at", "TEXT")
    _ensure_column(connection, "mail_contacts", "interaction_count", "INTEGER NOT NULL DEFAULT 1")
    _ensure_column(connection, "mail_contacts", "last_direction", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "mail_contacts", "last_source", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "mail_contacts", "last_communication_id", "INTEGER")
    _ensure_column(connection, "mail_contacts", "created_at", "TEXT")
    _ensure_column(connection, "mail_contacts", "updated_at", "TEXT")

    _ensure_column(connection, "list_items", "list_name", "TEXT NOT NULL DEFAULT 'personal'")
    _ensure_column(connection, "list_items", "title", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "list_items", "notes", "TEXT NOT NULL DEFAULT ''")
    _ensure_column(connection, "list_items", "status", "TEXT NOT NULL DEFAULT 'open'")
    _ensure_column(connection, "list_items", "created_at", "TEXT")
    _ensure_column(connection, "list_items", "updated_at", "TEXT")
    _ensure_column(connection, "list_items", "completed_at", "TEXT")

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
        CREATE UNIQUE INDEX IF NOT EXISTS idx_mail_contacts_key
        ON mail_contacts(contact_key)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mail_contacts_last_seen
        ON mail_contacts(last_seen_at DESC, updated_at DESC, id DESC)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_mail_contacts_email
        ON mail_contacts(email)
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_list_items_list_status_updated
        ON list_items(list_name, status, updated_at DESC, id DESC)
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


def _normalize_list_name(value: str) -> str:
    clean = str(value or "").strip().lower()
    if clean not in LIST_ITEM_NAMES:
        valid = ", ".join(LIST_ITEM_NAMES)
        raise ValueError(f"invalid list '{value}'. Use one of: {valid}")
    return clean


def _normalize_list_item_status(value: str) -> str:
    clean = str(value or "").strip().lower()
    if clean not in LIST_ITEM_STATUSES:
        valid = ", ".join(LIST_ITEM_STATUSES)
        raise ValueError(f"invalid status '{value}'. Use one of: {valid}")
    return clean


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


def add_list_item(
    connection: sqlite3.Connection,
    *,
    list_name: str,
    title: str,
    notes: str = "",
) -> int:
    clean_list_name = _normalize_list_name(list_name)
    clean_title = " ".join(str(title or "").split()).strip()
    if not clean_title:
        raise ValueError("list item title is required")
    now = _utc_now_string()
    cursor = connection.execute(
        """
        INSERT INTO list_items (
            list_name, title, notes, status, created_at, updated_at, completed_at
        ) VALUES (?, ?, ?, 'open', ?, ?, NULL)
        """,
        (
            clean_list_name,
            clean_title,
            str(notes or ""),
            now,
            now,
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def get_list_item(connection: sqlite3.Connection, item_id: int) -> Optional[sqlite3.Row]:
    return connection.execute(
        """
        SELECT *
        FROM list_items
        WHERE id = ?
        """,
        (item_id,),
    ).fetchone()


def list_list_items(
    connection: sqlite3.Connection,
    *,
    list_name: Optional[str] = None,
    status: str = "open",
    limit: int = 200,
) -> list[sqlite3.Row]:
    clauses: list[str] = []
    params: list[Any] = []
    if list_name and str(list_name).strip().lower() != "all":
        clauses.append("list_name = ?")
        params.append(_normalize_list_name(list_name))
    clean_status = str(status or "open").strip().lower()
    if clean_status != "all":
        clauses.append("status = ?")
        params.append(_normalize_list_item_status(clean_status))

    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return connection.execute(
        f"""
        SELECT *
        FROM list_items
        {where_clause}
        ORDER BY
            CASE status WHEN 'open' THEN 0 ELSE 1 END,
            list_name,
            updated_at DESC,
            id DESC
        LIMIT ?
        """,
        [*params, max(1, int(limit))],
    ).fetchall()


def set_list_item_status(connection: sqlite3.Connection, *, item_id: int, status: str) -> None:
    clean_status = _normalize_list_item_status(status)
    now = _utc_now_string()
    completed_at = now if clean_status == "done" else None
    cursor = connection.execute(
        """
        UPDATE list_items
        SET status = ?, updated_at = ?, completed_at = ?
        WHERE id = ?
        """,
        (clean_status, now, completed_at, item_id),
    )
    if cursor.rowcount <= 0:
        raise ValueError(f"list item #{item_id} was not found")
    connection.commit()


def _mail_contact_key(*, email: str = "", display_name: str = "", fallback: str = "") -> str:
    clean_email = str(email or "").strip().lower()
    if clean_email:
        return clean_email
    clean_name = " ".join(str(display_name or fallback or "").split()).strip().lower()
    if clean_name:
        return f"person:{clean_name}"
    return ""


def _mail_contact_record(*, name: str = "", email: str = "", fallback_name: str = "") -> dict[str, str]:
    clean_name = " ".join(str(name or fallback_name or "").split()).strip()
    clean_email = str(email or "").strip().lower()
    if not clean_email and "@" in clean_name:
        return {}
    contact_key = _mail_contact_key(email=clean_email, display_name=clean_name)
    if not contact_key:
        return {}
    return {
        "contact_key": contact_key,
        "email": clean_email,
        "display_name": clean_name,
    }


def _dedupe_mail_contact_records(records: list[dict[str, str]]) -> list[dict[str, str]]:
    deduped: dict[str, dict[str, str]] = {}
    ordered_keys: list[str] = []
    for record in records:
        contact_key = str(record.get("contact_key") or "").strip()
        if not contact_key:
            continue
        if contact_key not in deduped:
            deduped[contact_key] = {
                "contact_key": contact_key,
                "email": str(record.get("email") or "").strip().lower(),
                "display_name": " ".join(str(record.get("display_name") or "").split()).strip(),
            }
            ordered_keys.append(contact_key)
            continue
        existing = deduped[contact_key]
        candidate_name = " ".join(str(record.get("display_name") or "").split()).strip()
        candidate_email = str(record.get("email") or "").strip().lower()
        if candidate_email and not existing.get("email"):
            existing["email"] = candidate_email
        if candidate_name and (
            not existing.get("display_name")
            or existing.get("display_name") == existing.get("email")
            or len(candidate_name) > len(str(existing.get("display_name") or ""))
        ):
            existing["display_name"] = candidate_name
    return [deduped[key] for key in ordered_keys]


def _mail_contact_records_from_header(value: str, *, fallback_name: str = "") -> list[dict[str, str]]:
    if not str(value or "").strip():
        return []
    return _dedupe_mail_contact_records(
        [
            _mail_contact_record(name=name, email=email, fallback_name=fallback_name)
            for name, email in getaddresses([str(value)])
        ]
    )


def _mail_contact_records_from_structured(values: Optional[list[dict]], *, fallback_name: str = "") -> list[dict[str, str]]:
    if not values:
        return []
    records: list[dict[str, str]] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        records.append(
            _mail_contact_record(
                name=str(item.get("name") or ""),
                email=str(item.get("email") or ""),
                fallback_name=fallback_name,
            )
        )
    return _dedupe_mail_contact_records(records)


def upsert_mail_contact(
    connection: sqlite3.Connection,
    *,
    email: str = "",
    display_name: str = "",
    happened_at: str = "",
    direction: str = "",
    source: str = "",
    communication_id: Optional[int] = None,
    contact_key: str = "",
) -> Optional[int]:
    clean_email = str(email or "").strip().lower()
    clean_name = " ".join(str(display_name or "").split()).strip()
    normalized_key = str(contact_key or "").strip() or _mail_contact_key(email=clean_email, display_name=clean_name)
    if not normalized_key:
        return None
    happened_text = str(happened_at or "").strip() or _utc_now_string()
    existing = connection.execute(
        """
        SELECT id, email, display_name, first_seen_at, last_seen_at, interaction_count, last_communication_id
        FROM mail_contacts
        WHERE contact_key = ?
        """,
        (normalized_key,),
    ).fetchone()

    if existing:
        existing_email = str(existing["email"] or "").strip().lower()
        existing_name = " ".join(str(existing["display_name"] or "").split()).strip()
        existing_first_seen = str(existing["first_seen_at"] or "").strip()
        existing_last_seen = str(existing["last_seen_at"] or "").strip()
        existing_last_communication_id = int(existing["last_communication_id"] or 0)
        next_email = clean_email or existing_email
        next_name = existing_name
        if clean_name and (not existing_name or existing_name == existing_email or len(clean_name) > len(existing_name)):
            next_name = clean_name
        should_increment = bool(communication_id and int(communication_id) != existing_last_communication_id)
        next_interaction_count = int(existing["interaction_count"] or 0) + (1 if should_increment else 0)
        next_first_seen = existing_first_seen or happened_text
        next_last_seen = happened_text if happened_text >= existing_last_seen else existing_last_seen
        connection.execute(
            """
            UPDATE mail_contacts
            SET email = ?, display_name = ?, first_seen_at = ?, last_seen_at = ?, interaction_count = ?,
                last_direction = ?, last_source = ?, last_communication_id = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                next_email,
                next_name,
                next_first_seen,
                next_last_seen,
                next_interaction_count,
                str(direction or ""),
                str(source or ""),
                int(communication_id) if communication_id else existing_last_communication_id or None,
                int(existing["id"]),
            ),
        )
        return int(existing["id"])

    cursor = connection.execute(
        """
        INSERT INTO mail_contacts (
            contact_key, email, display_name, first_seen_at, last_seen_at,
            interaction_count, last_direction, last_source, last_communication_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            normalized_key,
            clean_email,
            clean_name,
            happened_text,
            happened_text,
            1,
            str(direction or ""),
            str(source or ""),
            int(communication_id) if communication_id else None,
        ),
    )
    return int(cursor.lastrowid)


def touch_mail_contacts_for_communication(
    connection: sqlite3.Connection,
    *,
    communication_id: Optional[int],
    happened_at: str,
    direction: str,
    source: str,
    person: str = "",
    external_from: str = "",
    external_to: str = "",
    external_cc: str = "",
    external_bcc: str = "",
    external_reply_to: str = "",
    from_value: Optional[dict] = None,
    to_recipients: Optional[list[dict]] = None,
    cc_recipients: Optional[list[dict]] = None,
    bcc_recipients: Optional[list[dict]] = None,
    reply_to_recipients: Optional[list[dict]] = None,
) -> None:
    records: list[dict[str, str]] = []
    clean_direction = str(direction or "").strip().lower()
    if clean_direction == "inbound":
        primary_sender = _mail_contact_record(
            name=str((from_value or {}).get("name") or ""),
            email=str((from_value or {}).get("email") or ""),
            fallback_name=person,
        )
        if primary_sender:
            records.append(primary_sender)
        records.extend(_mail_contact_records_from_header(external_from, fallback_name=person))
        records.extend(_mail_contact_records_from_structured(reply_to_recipients, fallback_name=person))
        records.extend(_mail_contact_records_from_header(external_reply_to, fallback_name=person))
    elif clean_direction == "outbound":
        records.extend(_mail_contact_records_from_structured(to_recipients, fallback_name=person))
        records.extend(_mail_contact_records_from_structured(cc_recipients))
        records.extend(_mail_contact_records_from_structured(bcc_recipients))
        records.extend(_mail_contact_records_from_header(external_to, fallback_name=person))
        records.extend(_mail_contact_records_from_header(external_cc))
        records.extend(_mail_contact_records_from_header(external_bcc))
    for record in _dedupe_mail_contact_records(records):
        upsert_mail_contact(
            connection,
            email=str(record.get("email") or ""),
            display_name=str(record.get("display_name") or ""),
            happened_at=happened_at,
            direction=clean_direction,
            source=source,
            communication_id=communication_id,
            contact_key=str(record.get("contact_key") or ""),
        )


def _touch_mail_contacts_for_communication(
    connection: sqlite3.Connection,
    *,
    communication_id: Optional[int],
    happened_at_text: str,
    direction: str,
    source: str,
    person: str = "",
    external_from: str = "",
    external_to: str = "",
    external_cc: str = "",
    external_bcc: str = "",
    external_reply_to: str = "",
    from_value: Optional[dict] = None,
    to_recipients: Optional[list[dict]] = None,
    cc_recipients: Optional[list[dict]] = None,
    bcc_recipients: Optional[list[dict]] = None,
    reply_to_recipients: Optional[list[dict]] = None,
) -> None:
    touch_mail_contacts_for_communication(
        connection,
        communication_id=communication_id,
        happened_at=happened_at_text,
        direction=direction,
        source=source,
        person=person,
        external_from=external_from,
        external_to=external_to,
        external_cc=external_cc,
        external_bcc=external_bcc,
        external_reply_to=external_reply_to,
        from_value=from_value,
        to_recipients=to_recipients,
        cc_recipients=cc_recipients,
        bcc_recipients=bcc_recipients,
        reply_to_recipients=reply_to_recipients,
    )


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
    if existing and str(existing["status"] or "") in {"done", "deleted"}:
        persisted_status = str(existing["status"] or "")
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
        communication_id = int(existing["id"])
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
            values + (communication_id,),
        )
        _touch_mail_contacts_for_communication(
            connection,
            communication_id=communication_id,
            happened_at_text=_iso_minute(happened_at),
            direction=direction,
            source=source,
            person=person,
            external_from=external_from or "",
            external_to=external_to,
            external_cc=external_cc,
            external_bcc=external_bcc,
            external_reply_to=external_reply_to,
            from_value=from_value,
            to_recipients=to_recipients,
            cc_recipients=cc_recipients,
            bcc_recipients=bcc_recipients,
            reply_to_recipients=reply_to_recipients,
        )
        connection.commit()
        return communication_id

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
    communication_id = int(cursor.lastrowid)
    _touch_mail_contacts_for_communication(
        connection,
        communication_id=communication_id,
        happened_at_text=_iso_minute(happened_at),
        direction=direction,
        source=source,
        person=person,
        external_from=external_from or "",
        external_to=external_to,
        external_cc=external_cc,
        external_bcc=external_bcc,
        external_reply_to=external_reply_to,
        from_value=from_value,
        to_recipients=to_recipients,
        cc_recipients=cc_recipients,
        bcc_recipients=bcc_recipients,
        reply_to_recipients=reply_to_recipients,
    )
    connection.commit()
    return communication_id


def set_communication_status(
    connection: sqlite3.Connection,
    *,
    communication_id: int,
    status: str,
) -> bool:
    deleted_at = _utc_now_string() if str(status or "") == "deleted" else None
    cursor = connection.execute(
        "UPDATE communications SET status = ?, deleted_at = ? WHERE id = ?",
        (status, deleted_at, int(communication_id)),
    )
    connection.commit()
    return int(cursor.rowcount) > 0


def set_communications_status(
    connection: sqlite3.Connection,
    *,
    communication_ids: list[int],
    status: str,
) -> int:
    ids = [int(value) for value in communication_ids if int(value) > 0]
    if not ids:
        return 0
    deleted_at = _utc_now_string() if str(status or "") == "deleted" else None
    placeholders = ", ".join("?" for _ in ids)
    cursor = connection.execute(
        f"UPDATE communications SET status = ?, deleted_at = ? WHERE id IN ({placeholders})",
        [status, deleted_at, *ids],
    )
    connection.commit()
    return int(cursor.rowcount)


def purge_deleted_communications(
    connection: sqlite3.Connection,
    *,
    retention_days: int = DELETED_COMMUNICATION_RETENTION_DAYS,
    now: Optional[datetime] = None,
    vault_root: Optional[Path] = None,
) -> dict[str, int]:
    cutoff = (now or datetime.now(timezone.utc)) - timedelta(days=max(0, int(retention_days)))
    cutoff_text = cutoff.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    rows = connection.execute(
        """
        SELECT id, raw_relative_path
        FROM communications
        WHERE status = 'deleted'
          AND deleted_at IS NOT NULL
          AND deleted_at <= ?
        ORDER BY deleted_at ASC, id ASC
        """,
        (cutoff_text,),
    ).fetchall()
    communication_ids = [int(row["id"]) for row in rows]
    if not communication_ids:
        return {"purged_count": 0, "artifact_count": 0}

    placeholders = ", ".join("?" for _ in communication_ids)
    artifact_rows = connection.execute(
        f"""
        SELECT relative_path, extracted_text_path
        FROM communication_attachments
        WHERE communication_id IN ({placeholders})
        """,
        communication_ids,
    ).fetchall()

    artifact_paths = {
        str(row["raw_relative_path"] or "").strip()
        for row in rows
        if str(row["raw_relative_path"] or "").strip()
    }
    artifact_paths.update(
        str(row["relative_path"] or "").strip()
        for row in artifact_rows
        if str(row["relative_path"] or "").strip()
    )
    artifact_paths.update(
        str(row["extracted_text_path"] or "").strip()
        for row in artifact_rows
        if str(row["extracted_text_path"] or "").strip()
    )

    cursor = connection.execute(
        f"DELETE FROM communications WHERE id IN ({placeholders})",
        communication_ids,
    )
    purged_count = int(cursor.rowcount)
    connection.commit()

    deleted_artifacts = 0
    resolved_vault_root = vault_root or attachment_vault_root()
    for relative_path in sorted(artifact_paths):
        if mail_vault.delete_encrypted_vault_file(vault_root=resolved_vault_root, relative_path=relative_path):
            deleted_artifacts += 1
    return {"purged_count": purged_count, "artifact_count": deleted_artifacts}


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
    channel: Optional[str] = None,
    direction: Optional[str] = None,
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
    if channel:
        clauses.append("communications.channel = ?")
        params.append(channel)
    if direction:
        clauses.append("communications.direction = ?")
        params.append(direction)
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


def list_mail_contacts(
    connection: sqlite3.Connection,
    *,
    query: Optional[str] = None,
    limit: Optional[int] = 100,
) -> list[sqlite3.Row]:
    clauses = []
    params: list[Any] = []
    clean_query = str(query or "").strip().lower()
    if clean_query:
        like_value = f"%{clean_query}%"
        clauses.append(
            "(LOWER(mail_contacts.email) LIKE ? OR LOWER(mail_contacts.display_name) LIKE ? OR LOWER(mail_contacts.contact_key) LIKE ?)"
        )
        params.extend([like_value, like_value, like_value])
    where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    limit_clause = ""
    if limit is not None:
        params.append(max(1, int(limit)))
        limit_clause = "LIMIT ?"
    return connection.execute(
        f"""
        SELECT *
        FROM mail_contacts
        {where_clause}
        ORDER BY last_seen_at DESC, interaction_count DESC, id DESC
        {limit_clause}
        """,
        params,
    ).fetchall()


def _ensure_mail_contacts_backfilled(connection: sqlite3.Connection) -> None:
    existing_marker = connection.execute(
        "SELECT value FROM sync_state WHERE key = ?",
        (MAIL_CONTACTS_BACKFILLED_SYNC_KEY,),
    ).fetchone()
    cleanup_marker = connection.execute(
        "SELECT value FROM sync_state WHERE key = ?",
        (MAIL_CONTACTS_CLEANUP_SYNC_KEY,),
    ).fetchone()
    contacts_present = connection.execute(
        "SELECT 1 FROM mail_contacts LIMIT 1"
    ).fetchone()
    contacts_already_present = contacts_present is not None

    # Older runtime DBs can already have a healthy contact index but still be
    # missing the sync markers that newer builds expect. In that case, stamp the
    # markers and avoid a needless rebuild on every open.
    if contacts_already_present and (
        not existing_marker
        or not str(existing_marker["value"] or "").strip()
        or not cleanup_marker
        or not str(cleanup_marker["value"] or "").strip()
    ):
        marker_value = _utc_now_string()
        connection.execute(
            """
            INSERT INTO sync_state (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
            """,
            (MAIL_CONTACTS_BACKFILLED_SYNC_KEY, marker_value),
        )
        connection.execute(
            """
            INSERT INTO sync_state (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
            """,
            (MAIL_CONTACTS_CLEANUP_SYNC_KEY, marker_value),
        )
        connection.commit()
        return

    if not cleanup_marker or not str(cleanup_marker["value"] or "").strip():
        connection.execute(
            """
            DELETE FROM mail_contacts
            WHERE email = ''
              AND display_name LIKE '%@%'
            """
        )
        connection.execute(
            """
            INSERT INTO sync_state (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
            """,
            (MAIL_CONTACTS_CLEANUP_SYNC_KEY, _utc_now_string()),
        )
        connection.commit()
    if existing_marker and str(existing_marker["value"] or "").strip():
        return

    rows = connection.execute(
        """
        SELECT *
        FROM communications
        WHERE channel = 'email'
          AND status != 'deleted'
        ORDER BY happened_at ASC, id ASC
        """
    ).fetchall()
    if not rows:
        connection.execute(
            """
            INSERT INTO sync_state (key, value, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
            """,
            (MAIL_CONTACTS_BACKFILLED_SYNC_KEY, _utc_now_string()),
        )
        connection.commit()
        return

    connection.execute("DELETE FROM mail_contacts")
    for row in rows:
        touch_mail_contacts_for_communication(
            connection,
            communication_id=int(row["id"]) if row["id"] is not None else None,
            happened_at=str(row["happened_at"] or ""),
            direction=str(row["direction"] or ""),
            source=str(row["source"] or ""),
            person=str(row["person"] or ""),
            external_from=str(row["external_from"] or ""),
            external_to=str(row["external_to"] or ""),
            external_cc=str(row["external_cc"] or ""),
            external_bcc=str(row["external_bcc"] or ""),
            external_reply_to=str(row["external_reply_to"] or ""),
            from_value=_json_value(str(row["from_json"] or "{}"), {}),
            to_recipients=_json_value(str(row["to_json"] or "[]"), []),
            cc_recipients=_json_value(str(row["cc_json"] or "[]"), []),
            bcc_recipients=_json_value(str(row["bcc_json"] or "[]"), []),
            reply_to_recipients=_json_value(str(row["reply_to_json"] or "[]"), []),
        )
    connection.execute(
        """
        INSERT INTO sync_state (key, value, updated_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
        """,
        (MAIL_CONTACTS_BACKFILLED_SYNC_KEY, _utc_now_string()),
    )
    connection.commit()


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


def mail_contact_key(*, person: str = "", external_from: str = "", source: str = "") -> str:
    contact_name, contact_email = parseaddr(str(external_from or ""))
    contact_name = contact_name.strip() or str(person or "").strip()
    contact_email = contact_email.strip().lower()
    if contact_email:
        return contact_email
    contact_label = contact_name or str(source or "unknown sender").strip()
    return f"person:{contact_label.lower()}"


def get_hidden_mail_contacts(connection: sqlite3.Connection) -> dict[str, str]:
    raw = get_sync_state(connection, MAIL_UI_HIDDEN_CONTACTS_SYNC_KEY)
    if not raw:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    hidden: dict[str, str] = {}
    for key, value in payload.items():
        contact_key = str(key or "").strip().lower()
        hidden_at = str(value or "").strip()
        if contact_key and hidden_at:
            hidden[contact_key] = hidden_at
    return hidden


def set_hidden_mail_contacts(connection: sqlite3.Connection, hidden_contacts: dict[str, str]) -> None:
    payload = {
        str(contact_key).strip().lower(): str(hidden_at).strip()
        for contact_key, hidden_at in hidden_contacts.items()
        if str(contact_key).strip() and str(hidden_at).strip()
    }
    set_sync_state(connection, MAIL_UI_HIDDEN_CONTACTS_SYNC_KEY, json.dumps(payload, sort_keys=True))


def mark_hidden_mail_contact(
    connection: sqlite3.Connection,
    *,
    contact_key: str,
    hidden_at: Optional[str] = None,
) -> None:
    normalized_key = str(contact_key or "").strip().lower()
    if not normalized_key:
        return
    hidden_contacts = get_hidden_mail_contacts(connection)
    hidden_contacts[normalized_key] = str(hidden_at or _utc_now_string())
    set_hidden_mail_contacts(connection, hidden_contacts)


def clear_hidden_mail_contact(connection: sqlite3.Connection, *, contact_key: str) -> bool:
    normalized_key = str(contact_key or "").strip().lower()
    if not normalized_key:
        return False
    hidden_contacts = get_hidden_mail_contacts(connection)
    if normalized_key not in hidden_contacts:
        return False
    hidden_contacts.pop(normalized_key, None)
    set_hidden_mail_contacts(connection, hidden_contacts)
    return True


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
