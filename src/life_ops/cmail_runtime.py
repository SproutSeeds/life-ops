from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
import threading
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from life_ops import store
from life_ops.cloudflare_email import sync_cloudflare_mail_queue
from life_ops.mail_ui import (
    DEFAULT_MAIL_UI_HOST,
    DEFAULT_MAIL_UI_LIMIT,
    DEFAULT_MAIL_UI_PORT,
    serve_mail_ui,
)
from life_ops.resend_integration import (
    DEFAULT_RESEND_PROCESS_LIMIT,
    default_resend_config_path,
    process_resend_delivery_queue,
)

DEFAULT_CMAIL_RUNTIME_DB_NAME = "cmail_runtime.db"
DEFAULT_CMAIL_RUNTIME_STATE_NAME = "cmail_runtime_state.json"
DEFAULT_CMAIL_RUNTIME_SYNC_INTERVAL_SECONDS = 2.0
DEFAULT_CMAIL_RUNTIME_SEND_INTERVAL_SECONDS = 2.0
DEFAULT_CMAIL_RUNTIME_SEAL_INTERVAL_SECONDS = 30.0
DEFAULT_CMAIL_RUNTIME_SYNC_LIMIT = 25
DEFAULT_CMAIL_RUNTIME_ALERT_THRESHOLD = 3
DEFAULT_CMAIL_RUNTIME_LIST_ITEMS_HYDRATED_SYNC_KEY = "cmail_runtime:list_items_hydrated_at"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def default_cmail_runtime_db_path() -> Path:
    return store.data_root() / DEFAULT_CMAIL_RUNTIME_DB_NAME


def default_cmail_runtime_state_path(runtime_db_path: Path | None = None) -> Path:
    db_path = runtime_db_path or default_cmail_runtime_db_path()
    return db_path.with_name(DEFAULT_CMAIL_RUNTIME_STATE_NAME)


def resolve_cmail_db_path(db_path: Path | None) -> Path:
    requested = (db_path or store.default_db_path()).expanduser()
    if requested.resolve(strict=False) == store.default_db_path().resolve(strict=False):
        return default_cmail_runtime_db_path()
    return requested


def _write_text_atomic(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text(payload, encoding="utf-8")
    try:
        os.chmod(temp_path, 0o600)
    except OSError:
        pass
    temp_path.replace(path)


def _runtime_worker_alert_key(worker_name: str) -> str:
    return f"cmail_runtime:{worker_name}"


def _record_runtime_worker_failure(
    *,
    runtime_db_path: Path,
    worker_name: str,
    exc: Exception,
    consecutive_failures: int,
) -> None:
    failed_at = _utc_now_iso()
    print(
        f"[cmail_runtime] worker={worker_name} failure_count={consecutive_failures} at {failed_at}: {exc}",
        file=sys.stderr,
    )
    traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)
    try:
        with store.open_db(runtime_db_path) as connection:
            store.set_sync_state(connection, f"cmail_runtime:{worker_name}:last_failure_at", failed_at)
            store.set_sync_state(connection, f"cmail_runtime:{worker_name}:consecutive_failures", str(consecutive_failures))
            if consecutive_failures >= DEFAULT_CMAIL_RUNTIME_ALERT_THRESHOLD:
                store.upsert_system_alert(
                    connection,
                    alert_key=_runtime_worker_alert_key(worker_name),
                    source="cmail_runtime",
                    severity="error",
                    title=f"CMAIL runtime {worker_name} worker is failing",
                    message=str(exc),
                    details={
                        "worker_name": worker_name,
                        "consecutive_failures": consecutive_failures,
                        "runtime_db_path": str(runtime_db_path),
                        "failed_at": failed_at,
                    },
                )
    except Exception as alert_exc:  # pragma: no cover - defensive logging path
        print(
            f"[cmail_runtime] failed to persist runtime worker alert for {worker_name}: {alert_exc}",
            file=sys.stderr,
        )


def _record_runtime_worker_recovery(*, runtime_db_path: Path, worker_name: str) -> None:
    recovered_at = _utc_now_iso()
    try:
        with store.open_db(runtime_db_path) as connection:
            store.set_sync_state(connection, f"cmail_runtime:{worker_name}:last_success_at", recovered_at)
            store.set_sync_state(connection, f"cmail_runtime:{worker_name}:consecutive_failures", "0")
            store.clear_system_alert(connection, _runtime_worker_alert_key(worker_name))
    except Exception as exc:  # pragma: no cover - defensive logging path
        print(
            f"[cmail_runtime] failed to persist runtime worker recovery for {worker_name}: {exc}",
            file=sys.stderr,
        )


def _load_runtime_state(runtime_db_path: Path) -> dict[str, Any]:
    state_path = default_cmail_runtime_state_path(runtime_db_path)
    if not state_path.exists():
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8") or "{}")
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_runtime_state(runtime_db_path: Path, payload: dict[str, Any]) -> Path:
    state_path = default_cmail_runtime_state_path(runtime_db_path)
    normalized = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    _write_text_atomic(state_path, normalized)
    return state_path


def _initialize_runtime_db(runtime_db_path: Path) -> None:
    runtime_db_path.parent.mkdir(parents=True, exist_ok=True)
    with store.open_db(runtime_db_path) as connection:
        connection.commit()
    try:
        os.chmod(runtime_db_path, 0o600)
    except OSError:
        pass
    for sidecar_path in (
        runtime_db_path.with_name(f"{runtime_db_path.name}-wal"),
        runtime_db_path.with_name(f"{runtime_db_path.name}-shm"),
    ):
        if sidecar_path.exists():
            try:
                os.chmod(sidecar_path, 0o600)
            except OSError:
                pass


def ensure_cmail_runtime_db(
    *,
    runtime_db_path: Path | None = None,
    canonical_db_path: Path | None = None,
) -> dict[str, Any]:
    runtime_path = resolve_cmail_db_path(runtime_db_path)
    canonical_path = (canonical_db_path or store.default_db_path()).expanduser()

    if runtime_path.exists():
        _initialize_runtime_db(runtime_path)
        return {
            "runtime_db_path": str(runtime_path),
            "canonical_db_path": str(canonical_path),
            "created": False,
            "hydrated_from_canonical": False,
            "state_path": str(default_cmail_runtime_state_path(runtime_path)),
        }

    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    hydrated = False
    plaintext_bytes = 0
    if store.db_storage_exists(canonical_path):
        plaintext = store.read_db_bytes(canonical_path)
        plaintext_bytes = len(plaintext)
        if plaintext:
            store.write_db_bytes(runtime_path, plaintext)
            hydrated = True

    _initialize_runtime_db(runtime_path)
    state = {
        "runtime_db_path": str(runtime_path),
        "canonical_db_path": str(canonical_path),
        "created_at": _utc_now_iso(),
        "hydrated_from_canonical": hydrated,
        "hydrated_at": _utc_now_iso() if hydrated else "",
        "last_snapshot_sha256": "",
        "last_sealed_at": "",
        "plaintext_bytes": plaintext_bytes,
    }
    _save_runtime_state(runtime_path, state)
    with store.open_db(runtime_path) as connection:
        store.set_sync_state(connection, "cmail_runtime:canonical_db_path", str(canonical_path))
        store.set_sync_state(connection, "cmail_runtime:hydrated_at", state["hydrated_at"])
    return {
        "runtime_db_path": str(runtime_path),
        "canonical_db_path": str(canonical_path),
        "created": True,
        "hydrated_from_canonical": hydrated,
        "plaintext_bytes": plaintext_bytes,
        "state_path": str(default_cmail_runtime_state_path(runtime_path)),
    }


def _runtime_row_is_staler(*, runtime_updated_at: str, canonical_updated_at: str) -> bool:
    runtime_text = str(runtime_updated_at or "").strip()
    canonical_text = str(canonical_updated_at or "").strip()
    if not runtime_text:
        return True
    if not canonical_text:
        return False
    try:
        runtime_dt = store.parse_datetime(runtime_text)
        canonical_dt = store.parse_datetime(canonical_text)
    except Exception:
        return canonical_text > runtime_text
    return canonical_dt > runtime_dt


def ensure_cmail_runtime_list_items(
    *,
    runtime_db_path: Path | None = None,
    canonical_db_path: Path | None = None,
    force_merge: bool = False,
) -> dict[str, Any]:
    runtime_path = resolve_cmail_db_path(runtime_db_path)
    canonical_path = (canonical_db_path or store.default_db_path()).expanduser()
    ensure_cmail_runtime_db(runtime_db_path=runtime_path, canonical_db_path=canonical_path)

    with store.open_db(runtime_path) as runtime_connection:
        hydrated_at = store.get_sync_state(runtime_connection, DEFAULT_CMAIL_RUNTIME_LIST_ITEMS_HYDRATED_SYNC_KEY)
        if hydrated_at and not force_merge:
            row = runtime_connection.execute("SELECT COUNT(*) AS c FROM list_items").fetchone()
            return {
                "runtime_db_path": str(runtime_path),
                "canonical_db_path": str(canonical_path),
                "merged": False,
                "list_item_count": int((row["c"] if row is not None else 0) or 0),
                "hydrated_at": str(hydrated_at),
            }

        merged_count = 0
        if store.db_storage_exists(canonical_path):
            plaintext = store.read_db_bytes(canonical_path)
            temp_path = store._secure_temp_db_path(prefix="cmail-runtime-list-hydrate-")
            try:
                if plaintext:
                    temp_path.write_bytes(plaintext)
                    canonical_connection = sqlite3.connect(str(temp_path))
                    try:
                        canonical_connection.row_factory = sqlite3.Row
                        canonical_rows = canonical_connection.execute(
                            """
                            SELECT id, list_name, title, notes, status, created_at, updated_at, completed_at
                            FROM list_items
                            ORDER BY id
                            """
                        ).fetchall()
                    finally:
                        canonical_connection.close()
                else:
                    canonical_rows = []
            finally:
                temp_path.unlink(missing_ok=True)
            for row in canonical_rows:
                runtime_row = runtime_connection.execute(
                    "SELECT updated_at FROM list_items WHERE id = ?",
                    (int(row["id"]),),
                ).fetchone()
                if runtime_row is not None and not _runtime_row_is_staler(
                    runtime_updated_at=str(runtime_row["updated_at"] or ""),
                    canonical_updated_at=str(row["updated_at"] or ""),
                ):
                    continue
                runtime_connection.execute(
                    """
                    INSERT INTO list_items (
                        id, list_name, title, notes, status, created_at, updated_at, completed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        list_name = excluded.list_name,
                        title = excluded.title,
                        notes = excluded.notes,
                        status = excluded.status,
                        created_at = excluded.created_at,
                        updated_at = excluded.updated_at,
                        completed_at = excluded.completed_at
                    """,
                    (
                        int(row["id"]),
                        str(row["list_name"] or "personal"),
                        str(row["title"] or ""),
                        str(row["notes"] or ""),
                        str(row["status"] or "open"),
                        str(row["created_at"] or ""),
                        str(row["updated_at"] or ""),
                        str(row["completed_at"] or ""),
                    ),
                )
                merged_count += 1
        hydrated_now = _utc_now_iso()
        store.set_sync_state(runtime_connection, DEFAULT_CMAIL_RUNTIME_LIST_ITEMS_HYDRATED_SYNC_KEY, hydrated_now)
        runtime_connection.commit()
        row = runtime_connection.execute("SELECT COUNT(*) AS c FROM list_items").fetchone()
        return {
            "runtime_db_path": str(runtime_path),
            "canonical_db_path": str(canonical_path),
            "merged": bool(merged_count),
            "merged_count": merged_count,
            "list_item_count": int((row["c"] if row is not None else 0) or 0),
            "hydrated_at": hydrated_now,
        }


def _snapshot_plaintext_bytes(db_path: Path) -> bytes:
    temp_path = store._secure_temp_db_path(prefix="cmail-runtime-snapshot-")
    try:
        with store.open_db(db_path) as connection:
            destination = sqlite3.connect(str(temp_path))
            try:
                connection.backup(destination)
            finally:
                destination.close()
        return temp_path.read_bytes()
    finally:
        temp_path.unlink(missing_ok=True)


def seal_cmail_runtime_db(
    *,
    runtime_db_path: Path | None = None,
    canonical_db_path: Path | None = None,
) -> dict[str, Any]:
    runtime_path = resolve_cmail_db_path(runtime_db_path)
    canonical_path = (canonical_db_path or store.default_db_path()).expanduser()
    ensure_cmail_runtime_db(runtime_db_path=runtime_path, canonical_db_path=canonical_path)
    ensure_cmail_runtime_list_items(
        runtime_db_path=runtime_path,
        canonical_db_path=canonical_path,
        force_merge=True,
    )

    plaintext = _snapshot_plaintext_bytes(runtime_path)
    snapshot_sha256 = hashlib.sha256(plaintext).hexdigest()
    state = _load_runtime_state(runtime_path)
    previous_sha256 = str(state.get("last_snapshot_sha256") or "")
    if previous_sha256 == snapshot_sha256 and store.db_storage_exists(canonical_path):
        return {
            "runtime_db_path": str(runtime_path),
            "canonical_db_path": str(canonical_path),
            "sealed": False,
            "unchanged": True,
            "snapshot_sha256": snapshot_sha256,
            "last_sealed_at": str(state.get("last_sealed_at") or ""),
        }

    stored_path = store.write_db_bytes(canonical_path, plaintext)
    sealed_at = _utc_now_iso()
    updated_state = {
        **state,
        "runtime_db_path": str(runtime_path),
        "canonical_db_path": str(canonical_path),
        "last_snapshot_sha256": snapshot_sha256,
        "last_sealed_at": sealed_at,
        "last_stored_path": str(stored_path),
        "plaintext_bytes": len(plaintext),
    }
    _save_runtime_state(runtime_path, updated_state)
    with store.open_db(runtime_path) as connection:
        store.set_sync_state(connection, "cmail_runtime:last_sealed_at", sealed_at)
        store.set_sync_state(connection, "cmail_runtime:last_snapshot_sha256", snapshot_sha256)
    return {
        "runtime_db_path": str(runtime_path),
        "canonical_db_path": str(canonical_path),
        "sealed": True,
        "unchanged": False,
        "stored_path": str(stored_path),
        "snapshot_sha256": snapshot_sha256,
        "plaintext_bytes": len(plaintext),
        "sealed_at": sealed_at,
    }


def _run_periodic_loop(
    *,
    stop_event: threading.Event,
    interval_seconds: float,
    runtime_db_path: Path,
    worker_name: str,
    target,
) -> None:
    consecutive_failures = 0
    while not stop_event.is_set():
        try:
            target()
        except Exception as exc:
            consecutive_failures += 1
            _record_runtime_worker_failure(
                runtime_db_path=runtime_db_path,
                worker_name=worker_name,
                exc=exc,
                consecutive_failures=consecutive_failures,
            )
        else:
            if consecutive_failures:
                _record_runtime_worker_recovery(runtime_db_path=runtime_db_path, worker_name=worker_name)
                consecutive_failures = 0
        if stop_event.wait(max(0.25, float(interval_seconds))):
            return


def serve_cmail_service(
    *,
    runtime_db_path: Path | None = None,
    canonical_db_path: Path | None = None,
    host: str = DEFAULT_MAIL_UI_HOST,
    port: int = DEFAULT_MAIL_UI_PORT,
    limit: int = DEFAULT_MAIL_UI_LIMIT,
    sync_interval_seconds: float = DEFAULT_CMAIL_RUNTIME_SYNC_INTERVAL_SECONDS,
    send_interval_seconds: float = DEFAULT_CMAIL_RUNTIME_SEND_INTERVAL_SECONDS,
    seal_interval_seconds: float = DEFAULT_CMAIL_RUNTIME_SEAL_INTERVAL_SECONDS,
    sync_limit: int = DEFAULT_CMAIL_RUNTIME_SYNC_LIMIT,
    send_limit: int = DEFAULT_RESEND_PROCESS_LIMIT,
    resend_config_path: Path | None = None,
) -> None:
    runtime_path = resolve_cmail_db_path(runtime_db_path)
    canonical_path = (canonical_db_path or store.default_db_path()).expanduser()
    ensure_cmail_runtime_db(runtime_db_path=runtime_path, canonical_db_path=canonical_path)

    stop_event = threading.Event()
    worker_threads = [
        threading.Thread(
            target=_run_periodic_loop,
            kwargs={
                "stop_event": stop_event,
                "interval_seconds": sync_interval_seconds,
                "runtime_db_path": runtime_path,
                "worker_name": "sync",
                "target": lambda: sync_cloudflare_mail_queue(
                    db_path=runtime_path,
                    limit=sync_limit,
                ),
            },
            daemon=True,
            name="life-ops-cmail-runtime-sync",
        ),
        threading.Thread(
            target=_run_periodic_loop,
            kwargs={
                "stop_event": stop_event,
                "interval_seconds": send_interval_seconds,
                "runtime_db_path": runtime_path,
                "worker_name": "send",
                "target": lambda: process_resend_delivery_queue(
                    db_path=runtime_path,
                    config_path=resend_config_path or default_resend_config_path(),
                    limit=send_limit,
                ),
            },
            daemon=True,
            name="life-ops-cmail-runtime-send",
        ),
        threading.Thread(
            target=_run_periodic_loop,
            kwargs={
                "stop_event": stop_event,
                "interval_seconds": seal_interval_seconds,
                "runtime_db_path": runtime_path,
                "worker_name": "seal",
                "target": lambda: seal_cmail_runtime_db(
                    runtime_db_path=runtime_path,
                    canonical_db_path=canonical_path,
                ),
            },
            daemon=True,
            name="life-ops-cmail-runtime-seal",
        ),
    ]
    for thread in worker_threads:
        thread.start()
    try:
        serve_mail_ui(
            db_path=runtime_path,
            host=host,
            port=port,
            limit=limit,
            enable_background_remote_sync=False,
        )
    finally:
        stop_event.set()
        for thread in worker_threads:
            thread.join(timeout=1.0)
