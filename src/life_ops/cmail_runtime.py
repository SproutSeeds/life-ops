from __future__ import annotations

import hashlib
import gc
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error as urlerror, request as urlrequest

from life_ops import credentials
from life_ops import store
from life_ops.cloudflare_email import cloudflare_mail_queue_status, sync_cloudflare_mail_queue
from life_ops.mail_ingest import MAIL_INGEST_SECRET_NAME
from life_ops.mail_ui import (
    DEFAULT_MAIL_UI_HOST,
    DEFAULT_MAIL_UI_LIMIT,
    DEFAULT_MAIL_UI_PORT,
    cleanup_cmail_correspondence_artifacts,
    serve_mail_ui,
)
from life_ops.resend_integration import (
    DEFAULT_RESEND_PROCESS_LIMIT,
    default_resend_config_path,
    process_resend_delivery_queue,
    resend_queue_status,
)
from life_ops.vault_crypto import MASTER_KEY_NAME

DEFAULT_CMAIL_RUNTIME_DB_NAME = "cmail_runtime.db"
DEFAULT_CMAIL_RUNTIME_STATE_NAME = "cmail_runtime_state.json"
DEFAULT_CMAIL_RUNTIME_SYNC_INTERVAL_SECONDS = 60.0
DEFAULT_CMAIL_RUNTIME_SEND_INTERVAL_SECONDS = 2.0
DEFAULT_CMAIL_RUNTIME_SEAL_INTERVAL_SECONDS = 900.0
DEFAULT_CMAIL_RUNTIME_BACKUP_INTERVAL_SECONDS = 12 * 3600.0
DEFAULT_CMAIL_RUNTIME_SYNC_LIMIT = 5
DEFAULT_CMAIL_RUNTIME_ALERT_THRESHOLD = 3
DEFAULT_CMAIL_RUNTIME_LIST_ITEMS_HYDRATED_SYNC_KEY = "cmail_runtime:list_items_hydrated_at"
DEFAULT_CMAIL_EXTERNAL_BACKUP_ROOT_ENV = "LIFE_OPS_CMAIL_BACKUP_ROOT"
DEFAULT_CMAIL_EXTERNAL_BACKUP_VOLUME_NAME = "APFS_4TB_Backup"
DEFAULT_CMAIL_LEGACY_HOME_ENV = "LIFE_OPS_LEGACY_HOME"
DEFAULT_CMAIL_MIGRATED_FROM_SYNC_KEY = "cmail_runtime:migrated_from"
DEFAULT_CMAIL_SERVICE_SECRETS_NAME = credentials.DEFAULT_SERVICE_SECRETS_FILENAME


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def default_cmail_runtime_db_path() -> Path:
    return store.data_root() / DEFAULT_CMAIL_RUNTIME_DB_NAME


def default_cmail_runtime_state_path(runtime_db_path: Path | None = None) -> Path:
    db_path = runtime_db_path or default_cmail_runtime_db_path()
    return db_path.with_name(DEFAULT_CMAIL_RUNTIME_STATE_NAME)


def default_cmail_service_secrets_path() -> Path:
    return store.config_root() / DEFAULT_CMAIL_SERVICE_SECRETS_NAME


def resolve_cmail_db_path(db_path: Path | None) -> Path:
    requested = (db_path or store.default_db_path()).expanduser()
    if requested.resolve(strict=False) == store.default_db_path().resolve(strict=False):
        return default_cmail_runtime_db_path()
    return requested


def _write_text_atomic(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        prefix=f".{path.name}.",
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


def _runtime_mailbox_score(runtime_db_path: Path) -> dict[str, int]:
    if not runtime_db_path.exists():
        return {
            "communications": 0,
            "drafts": 0,
            "contacts": 0,
            "attachments": 0,
        }
    with sqlite3.connect(str(runtime_db_path)) as connection:
        metrics = {
            "communications": "SELECT COUNT(*) FROM communications",
            "drafts": "SELECT COUNT(*) FROM communications WHERE status = 'draft'",
            "contacts": "SELECT COUNT(*) FROM mail_contacts",
            "attachments": "SELECT COUNT(*) FROM communication_attachments",
        }
        result: dict[str, int] = {}
        for key, query in metrics.items():
            try:
                row = connection.execute(query).fetchone()
                result[key] = int((row[0] if row else 0) or 0)
            except sqlite3.Error:
                result[key] = 0
        return result


def _write_runtime_path_metadata(*, runtime_db_path: Path, canonical_db_path: Path, migrated_from: str = "") -> None:
    state = _load_runtime_state(runtime_db_path)
    updated = {
        **state,
        "runtime_db_path": str(runtime_db_path),
        "canonical_db_path": str(canonical_db_path),
    }
    if migrated_from:
        updated["migrated_from"] = migrated_from
        updated["migrated_at"] = _utc_now_iso()
    _save_runtime_state(runtime_db_path, updated)
    with store.open_db(runtime_db_path) as connection:
        store.set_sync_state(connection, "cmail_runtime:canonical_db_path", str(canonical_db_path))
        store.set_sync_state(connection, "cmail_runtime:runtime_db_path", str(runtime_db_path))
        if migrated_from:
            store.set_sync_state(connection, DEFAULT_CMAIL_MIGRATED_FROM_SYNC_KEY, migrated_from)


def _candidate_legacy_homes(*, current_home: Path) -> list[Path]:
    env_value = str(os.getenv(DEFAULT_CMAIL_LEGACY_HOME_ENV) or "").strip()
    explicit_candidates: list[Path] = []
    if env_value:
        for raw_path in env_value.split(os.pathsep):
            text = raw_path.strip()
            if text:
                explicit_candidates.append(Path(text).expanduser().resolve(strict=False))
    candidates: list[Path] = list(explicit_candidates)
    if explicit_candidates:
        search_default_candidates = False
    else:
        search_default_candidates = True
    if search_default_candidates:
        candidates.append((Path.home() / "code" / "life-ops").resolve(strict=False))
        volumes_root = Path("/Volumes")
        if volumes_root.exists():
            for volume in volumes_root.iterdir():
                candidates.append((volume / "code" / "life-ops").resolve(strict=False))
    unique: list[Path] = []
    seen: set[str] = set()
    normalized_current = current_home.resolve(strict=False)
    for candidate in candidates:
        normalized = candidate.resolve(strict=False)
        if normalized == normalized_current:
            continue
        marker_paths = (
            normalized / "data" / DEFAULT_CMAIL_RUNTIME_DB_NAME,
            normalized / "data" / f"{store.default_db_path().name}{store.ENCRYPTED_DB_SUFFIX}",
            normalized / "config" / "cloudflare_mail.json",
            normalized / "config" / "resend.json",
        )
        if not any(path.exists() for path in marker_paths):
            continue
        key = str(normalized)
        if key in seen:
            continue
        seen.add(key)
        unique.append(normalized)
    return unique


def _copy_file_if_present(source: Path, destination: Path) -> bool:
    if not source.exists():
        return False
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    return True


def _copy_tree_incremental(*, source_root: Path, destination_root: Path) -> int:
    if not source_root.exists():
        return 0
    copied = 0
    for source_path in source_root.rglob("*"):
        if not source_path.is_file():
            continue
        relative_path = source_path.relative_to(source_root)
        destination_path = destination_root / relative_path
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            source_stat = source_path.stat()
            destination_stat = destination_path.stat() if destination_path.exists() else None
            if destination_stat and destination_stat.st_size == source_stat.st_size and int(destination_stat.st_mtime) >= int(source_stat.st_mtime):
                continue
        except OSError:
            pass
        shutil.copy2(source_path, destination_path)
        copied += 1
    return copied


def migrate_legacy_cmail_state_if_needed(
    *,
    runtime_db_path: Path | None = None,
    canonical_db_path: Path | None = None,
) -> dict[str, Any]:
    runtime_path = resolve_cmail_db_path(runtime_db_path)
    canonical_path = (canonical_db_path or store.default_db_path()).expanduser()
    current_home = store.life_ops_home()
    current_score = _runtime_mailbox_score(runtime_path)
    if runtime_path.exists() and current_score["communications"] > 0:
        return {
            "runtime_db_path": str(runtime_path),
            "canonical_db_path": str(canonical_path),
            "migrated": False,
            "migrated_from": "",
            "attachments_copied": 0,
            "copied_configs": [],
            "runtime_score": current_score,
            "legacy_score": {"communications": 0, "drafts": 0, "contacts": 0, "attachments": 0},
            "skipped_reason": "runtime_mailbox_present",
        }
    config_root = store.config_root()
    chosen_legacy_home: Path | None = None
    chosen_legacy_score: dict[str, int] = {"communications": 0, "drafts": 0, "contacts": 0, "attachments": 0}
    for candidate_home in _candidate_legacy_homes(current_home=current_home):
        candidate_runtime = candidate_home / "data" / DEFAULT_CMAIL_RUNTIME_DB_NAME
        candidate_score = _runtime_mailbox_score(candidate_runtime)
        if chosen_legacy_home is None or tuple(candidate_score.values()) > tuple(chosen_legacy_score.values()):
            chosen_legacy_home = candidate_home
            chosen_legacy_score = candidate_score

    copied_configs: list[str] = []
    migrated = False
    attachments_copied = 0
    if chosen_legacy_home is not None:
        chosen_data_root = chosen_legacy_home / "data"
        chosen_config_root = chosen_legacy_home / "config"
        should_import_mailbox = current_score["communications"] == 0 and chosen_legacy_score["communications"] > 0
        if should_import_mailbox:
            runtime_path.parent.mkdir(parents=True, exist_ok=True)
            _copy_file_if_present(chosen_data_root / DEFAULT_CMAIL_RUNTIME_DB_NAME, runtime_path)
            _copy_file_if_present(
                chosen_data_root / DEFAULT_CMAIL_RUNTIME_STATE_NAME,
                default_cmail_runtime_state_path(runtime_path),
            )
            _copy_file_if_present(
                store.encrypted_db_manifest_path(chosen_data_root / store.default_db_path().name),
                store.encrypted_db_manifest_path(canonical_path),
            )
            attachments_copied = _copy_tree_incremental(
                source_root=chosen_data_root / "attachments",
                destination_root=store.attachment_vault_root(),
            )
            migrated = runtime_path.exists()
        for config_name in ("resend.json", "cloudflare_mail.json"):
            destination = config_root / config_name
            if destination.exists():
                continue
            if _copy_file_if_present(chosen_config_root / config_name, destination):
                copied_configs.append(config_name)

    if runtime_path.exists():
        _initialize_runtime_db(runtime_path)
        _write_runtime_path_metadata(
            runtime_db_path=runtime_path,
            canonical_db_path=canonical_path,
            migrated_from=str(chosen_legacy_home) if migrated and chosen_legacy_home is not None else "",
        )

    return {
        "runtime_db_path": str(runtime_path),
        "canonical_db_path": str(canonical_path),
        "migrated": migrated,
        "migrated_from": str(chosen_legacy_home or ""),
        "attachments_copied": attachments_copied,
        "copied_configs": copied_configs,
        "runtime_score": _runtime_mailbox_score(runtime_path),
        "legacy_score": chosen_legacy_score,
    }


def default_cmail_external_backup_root() -> Path | None:
    override = str(os.getenv(DEFAULT_CMAIL_EXTERNAL_BACKUP_ROOT_ENV) or "").strip()
    if override:
        return Path(override).expanduser().resolve(strict=False)
    volume_root = Path("/Volumes") / DEFAULT_CMAIL_EXTERNAL_BACKUP_VOLUME_NAME
    if not volume_root.exists():
        return None
    return (volume_root / "life-ops" / "cmail-backup" / Path.home().name).resolve(strict=False)


def _load_json_dict(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8") or "{}")
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def required_cmail_secret_names() -> list[str]:
    config_dir = store.config_root()
    cloudflare_config = _load_json_dict(config_dir / "cloudflare_mail.json")
    resend_config = _load_json_dict(config_dir / "resend.json")
    names = [
        str(cloudflare_config.get("ingest_secret_env") or MAIL_INGEST_SECRET_NAME).strip(),
        str(cloudflare_config.get("archive_key_env") or MASTER_KEY_NAME).strip(),
        str(resend_config.get("api_key_env") or "RESEND_API_KEY").strip(),
        "FRG_BOOKING_WEBHOOK_SECRET",
    ]
    seen: set[str] = set()
    ordered: list[str] = []
    for name in names:
        if not name or name in seen:
            continue
        seen.add(name)
        ordered.append(name)
    return ordered


def write_cmail_service_secret_snapshot(*, target: Path | None = None) -> dict[str, Any]:
    return credentials.write_service_secret_snapshot(
        names=required_cmail_secret_names(),
        target=target or default_cmail_service_secrets_path(),
    )


def backup_cmail_runtime_to_external(
    *,
    runtime_db_path: Path | None = None,
    canonical_db_path: Path | None = None,
    backup_root: Path | None = None,
) -> dict[str, Any]:
    runtime_path = resolve_cmail_db_path(runtime_db_path)
    canonical_path = (canonical_db_path or store.default_db_path()).expanduser()
    target_root = backup_root or default_cmail_external_backup_root()
    if target_root is None:
        return {
            "enabled": False,
            "reason": "no_backup_root",
            "runtime_db_path": str(runtime_path),
        }
    ensure_cmail_runtime_db(runtime_db_path=runtime_path, canonical_db_path=canonical_path)
    target_root.mkdir(parents=True, exist_ok=True)
    runtime_backup_path = target_root / "data" / DEFAULT_CMAIL_RUNTIME_DB_NAME
    runtime_backup_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_bytes = _snapshot_plaintext_bytes(runtime_path)
    store.write_db_bytes(runtime_backup_path, snapshot_bytes)
    runtime_state_path = default_cmail_runtime_state_path(runtime_path)
    if runtime_state_path.exists():
        _copy_file_if_present(runtime_state_path, target_root / "data" / runtime_state_path.name)
    _copy_file_if_present(
        store.encrypted_db_manifest_path(canonical_path),
        target_root / "data" / store.encrypted_db_manifest_path(canonical_path).name,
    )
    attachments_copied = _copy_tree_incremental(
        source_root=store.attachment_vault_root(),
        destination_root=target_root / "data" / "attachments",
    )
    completed_at = _utc_now_iso()
    manifest = {
        "completed_at": completed_at,
        "runtime_db_path": str(runtime_path),
        "canonical_db_path": str(canonical_path),
        "backup_root": str(target_root),
        "snapshot_sha256": hashlib.sha256(snapshot_bytes).hexdigest(),
        "snapshot_bytes": len(snapshot_bytes),
        "attachments_copied": attachments_copied,
    }
    _write_text_atomic(target_root / "data" / "cmail-backup-status.json", json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    with store.open_db(runtime_path) as connection:
        store.set_sync_state(connection, "cmail_runtime:last_external_backup_at", completed_at)
        store.set_sync_state(connection, "cmail_runtime:external_backup_root", str(target_root))
    return {
        **manifest,
        "enabled": True,
    }


def _check_cmail_service_http_health(*, host: str, port: int, timeout_seconds: float = 5.0) -> dict[str, Any]:
    url = f"http://{host}:{int(port)}/api/health"
    try:
        with urlrequest.urlopen(url, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8") or "{}")
        return {
            "ok": bool(payload.get("ok")),
            "url": url,
            "payload": payload,
            "error": "",
        }
    except (urlerror.URLError, json.JSONDecodeError, TimeoutError, OSError) as exc:
        return {
            "ok": False,
            "url": url,
            "payload": {},
            "error": str(exc),
        }


def run_cmail_health_check(
    *,
    runtime_db_path: Path | None = None,
    canonical_db_path: Path | None = None,
    host: str = DEFAULT_MAIL_UI_HOST,
    port: int = DEFAULT_MAIL_UI_PORT,
    repair: bool = False,
    resend_process_limit: int = DEFAULT_RESEND_PROCESS_LIMIT,
    cloudflare_sync_limit: int = DEFAULT_CMAIL_RUNTIME_SYNC_LIMIT,
    health_timeout_seconds: float = 5.0,
    backup_stale_after_seconds: float = 12 * 3600,
) -> dict[str, Any]:
    runtime_path = resolve_cmail_db_path(runtime_db_path)
    canonical_path = (canonical_db_path or store.default_db_path()).expanduser()
    ensure_cmail_runtime_db(runtime_db_path=runtime_path, canonical_db_path=canonical_path)
    ensure_cmail_runtime_list_items(runtime_db_path=runtime_path, canonical_db_path=canonical_path)

    actions: list[str] = []
    cleanup_result = {"orphaned_resend_ids": [], "superseded_draft_ids": [], "restored_draft_ids": []}
    if repair:
        cleanup_result = cleanup_cmail_correspondence_artifacts(db_path=runtime_path)
        if cleanup_result["orphaned_resend_ids"]:
            actions.append(f"cleaned orphaned resend artifacts: {cleanup_result['orphaned_resend_ids']}")
        if cleanup_result["superseded_draft_ids"]:
            actions.append(f"cleaned superseded drafts: {cleanup_result['superseded_draft_ids']}")
        if cleanup_result.get("restored_draft_ids"):
            actions.append(f"restored active draft markers: {cleanup_result['restored_draft_ids']}")

    resend_before = resend_queue_status(db_path=runtime_path, limit=max(100, int(resend_process_limit)))
    resend_processed = {
        "processed_count": 0,
        "failed_count": 0,
        "processed": [],
        "failures": [],
    }
    if repair and int(resend_before.get("due_count") or 0) > 0:
        resend_processed = process_resend_delivery_queue(
            db_path=runtime_path,
            config_path=default_resend_config_path(),
            limit=max(int(resend_process_limit), int(resend_before.get("due_count") or 0)),
        )
        if int(resend_processed.get("processed_count") or 0) > 0:
            actions.append(f"processed resend queue items: {int(resend_processed.get('processed_count') or 0)}")
    resend_after = resend_queue_status(db_path=runtime_path, limit=max(100, int(resend_process_limit)))

    cloudflare_status_result: dict[str, Any] = {}
    cloudflare_sync_result: dict[str, Any] = {}
    cloudflare_warning = ""
    try:
        cloudflare_status_result = cloudflare_mail_queue_status(timeout_seconds=health_timeout_seconds)
        pending_count = int(
            cloudflare_status_result.get("pending_count")
            or cloudflare_status_result.get("queue_count")
            or 0
        )
        if repair and pending_count > 0:
            cloudflare_sync_result = sync_cloudflare_mail_queue(
                db_path=runtime_path,
                limit=max(1, int(cloudflare_sync_limit)),
                request_timeout_seconds=health_timeout_seconds,
            )
            if int(cloudflare_sync_result.get("ingested_count") or 0) > 0:
                actions.append(f"synced inbound cloudflare mail: {int(cloudflare_sync_result.get('ingested_count') or 0)}")
    except Exception as exc:
        cloudflare_warning = str(exc)

    with store.open_db(runtime_path) as connection:
        last_backup_at = str(store.get_sync_state(connection, "cmail_runtime:last_external_backup_at") or "")
        backup_root = str(store.get_sync_state(connection, "cmail_runtime:external_backup_root") or "")
    backup_result: dict[str, Any] = {
        "last_external_backup_at": last_backup_at,
        "external_backup_root": backup_root,
        "stale": False,
    }
    backup_warning = ""
    if last_backup_at:
        try:
            last_backup_dt = datetime.fromisoformat(last_backup_at.replace("Z", "+00:00"))
            backup_result["stale"] = (datetime.now(timezone.utc) - last_backup_dt).total_seconds() > max(0.0, float(backup_stale_after_seconds))
        except ValueError:
            backup_result["stale"] = True
    else:
        backup_result["stale"] = True
    if repair and backup_result["stale"]:
        try:
            backup_result = {
                **backup_result,
                **backup_cmail_runtime_to_external(
                    runtime_db_path=runtime_path,
                    canonical_db_path=canonical_path,
                ),
                "stale": False,
            }
            actions.append("created external cmail backup")
        except Exception as exc:
            backup_warning = str(exc)

    service_health = _check_cmail_service_http_health(
        host=host,
        port=port,
        timeout_seconds=health_timeout_seconds,
    )

    warnings: list[str] = []
    if cloudflare_warning:
        warnings.append(f"cloudflare status unavailable: {cloudflare_warning}")
    if backup_warning:
        warnings.append(f"external backup failed: {backup_warning}")

    ok = bool(service_health.get("ok")) and int(resend_after.get("due_count") or 0) == 0 and int(resend_after.get("active_alert_count") or 0) == 0

    return {
        "ok": ok,
        "runtime_db_path": str(runtime_path),
        "canonical_db_path": str(canonical_path),
        "service_health": service_health,
        "cleanup": cleanup_result,
        "resend_queue_before": resend_before,
        "resend_queue_after": resend_after,
        "resend_processed": resend_processed,
        "cloudflare_status": cloudflare_status_result,
        "cloudflare_sync": cloudflare_sync_result,
        "backup": backup_result,
        "warnings": warnings,
        "actions": actions,
        "checked_at": _utc_now_iso(),
    }


def ensure_cmail_runtime_db(
    *,
    runtime_db_path: Path | None = None,
    canonical_db_path: Path | None = None,
) -> dict[str, Any]:
    runtime_path = resolve_cmail_db_path(runtime_db_path)
    canonical_path = (canonical_db_path or store.default_db_path()).expanduser()

    if runtime_path.exists():
        existing_score = _runtime_mailbox_score(runtime_path)
        if existing_score["communications"] > 0:
            return {
                "runtime_db_path": str(runtime_path),
                "canonical_db_path": str(canonical_path),
                "created": False,
                "hydrated_from_canonical": False,
                "state_path": str(default_cmail_runtime_state_path(runtime_path)),
                "runtime_score": existing_score,
                "skipped_reason": "runtime_mailbox_present",
            }
        hydrated = False
        if _runtime_mailbox_score(runtime_path)["communications"] == 0 and store.db_storage_exists(canonical_path):
            plaintext = store.read_db_bytes(canonical_path)
            if plaintext:
                store.write_db_bytes(runtime_path, plaintext)
                hydrated = True
        _initialize_runtime_db(runtime_path)
        _write_runtime_path_metadata(runtime_db_path=runtime_path, canonical_db_path=canonical_path)
        return {
            "runtime_db_path": str(runtime_path),
            "canonical_db_path": str(canonical_path),
            "created": False,
            "hydrated_from_canonical": hydrated,
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
    initial_delay_seconds: float = 0.0,
) -> None:
    consecutive_failures = 0
    if initial_delay_seconds > 0 and stop_event.wait(max(0.25, float(initial_delay_seconds))):
        return
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
        if worker_name in {"sync", "backup"}:
            gc.collect()
        if stop_event.wait(max(0.25, float(interval_seconds))):
            return


def _run_cmail_runtime_subprocess(*, worker_name: str, args: list[str], timeout_seconds: float = 1800.0) -> None:
    completed = subprocess.run(
        args,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        timeout=max(1.0, float(timeout_seconds)),
        check=False,
    )
    if completed.returncode != 0:
        stderr = str(completed.stderr or "").strip()
        raise RuntimeError(f"{worker_name} subprocess failed with exit {completed.returncode}: {stderr}")


def _seal_cmail_runtime_db_in_subprocess(*, runtime_db_path: Path, canonical_db_path: Path) -> None:
    script = (
        "import sys\n"
        "from pathlib import Path\n"
        "from life_ops.cmail_runtime import seal_cmail_runtime_db\n"
        "seal_cmail_runtime_db(runtime_db_path=Path(sys.argv[1]), canonical_db_path=Path(sys.argv[2]))\n"
    )
    _run_cmail_runtime_subprocess(
        worker_name="seal",
        args=[sys.executable, "-c", script, str(runtime_db_path), str(canonical_db_path)],
    )


def _backup_cmail_runtime_to_external_in_subprocess(*, runtime_db_path: Path, canonical_db_path: Path) -> None:
    script = (
        "import sys\n"
        "from pathlib import Path\n"
        "from life_ops.cmail_runtime import backup_cmail_runtime_to_external\n"
        "backup_cmail_runtime_to_external(runtime_db_path=Path(sys.argv[1]), canonical_db_path=Path(sys.argv[2]))\n"
    )
    _run_cmail_runtime_subprocess(
        worker_name="backup",
        args=[sys.executable, "-c", script, str(runtime_db_path), str(canonical_db_path)],
    )


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
    backup_interval_seconds: float = DEFAULT_CMAIL_RUNTIME_BACKUP_INTERVAL_SECONDS,
) -> None:
    runtime_path = resolve_cmail_db_path(runtime_db_path)
    canonical_path = (canonical_db_path or store.default_db_path()).expanduser()
    print(f"[cmail_runtime] startup runtime_db={runtime_path} canonical_db={canonical_path}", flush=True)
    runtime_present = runtime_path.exists()
    runtime_bytes = runtime_path.stat().st_size if runtime_present else 0
    if runtime_present and runtime_bytes > 0:
        print("[cmail_runtime] existing runtime db detected; skipping startup migration", flush=True)
    else:
        migrate_legacy_cmail_state_if_needed(runtime_db_path=runtime_path, canonical_db_path=canonical_path)
        print("[cmail_runtime] legacy migration check complete", flush=True)
        ensure_cmail_runtime_db(runtime_db_path=runtime_path, canonical_db_path=canonical_path)
        print("[cmail_runtime] runtime db ready", flush=True)

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
                "target": lambda: _seal_cmail_runtime_db_in_subprocess(
                    runtime_db_path=runtime_path,
                    canonical_db_path=canonical_path,
                ),
                "initial_delay_seconds": seal_interval_seconds,
            },
            daemon=True,
            name="life-ops-cmail-runtime-seal",
        ),
        threading.Thread(
            target=_run_periodic_loop,
            kwargs={
                "stop_event": stop_event,
                "interval_seconds": backup_interval_seconds,
                "runtime_db_path": runtime_path,
                "worker_name": "backup",
                "target": lambda: _backup_cmail_runtime_to_external_in_subprocess(
                    runtime_db_path=runtime_path,
                    canonical_db_path=canonical_path,
                ),
                "initial_delay_seconds": backup_interval_seconds,
            },
            daemon=True,
            name="life-ops-cmail-runtime-backup",
        ),
    ]
    for thread in worker_threads:
        thread.start()
    print("[cmail_runtime] background workers started", flush=True)
    try:
        print("[cmail_runtime] entering mail UI server", flush=True)
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
