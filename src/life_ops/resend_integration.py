from __future__ import annotations

import base64
import json
import mimetypes
import os
import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from email.utils import parseaddr
from pathlib import Path
from typing import Any, Optional
from urllib import error, request

from life_ops import credentials
from life_ops.document_ingest import extract_text_from_saved_attachment
from life_ops import mail_metadata
from life_ops import mail_vault
from life_ops import store

RESEND_API_BASE_URL = "https://api.resend.com"
RESEND_USER_AGENT = "life-ops/0.2 (+https://frg.earth)"
DEFAULT_RESEND_MAX_ATTEMPTS = 8
DEFAULT_RESEND_PROCESS_LIMIT = 25
DEFAULT_RESEND_BATCH_MAX_PER_HOUR = 5
DEFAULT_RESEND_BATCH_MIN_GAP_MINUTES = 12
DEFAULT_RESEND_BATCH_DAILY_CAP = 20
RESEND_QUEUE_ALERT_PREFIX = "resend_delivery"
RESEND_QUEUE_GLOBAL_ALERT_KEY = "resend_delivery_queue"


def default_resend_config_path() -> Path:
    return store.config_root() / "resend.json"


def resend_config_template() -> dict[str, Any]:
    return {
        "api_base_url": RESEND_API_BASE_URL,
        "api_key": "",
        "api_key_env": "RESEND_API_KEY",
        "default_from": "Cody <cody@frg.earth>",
        "default_reply_to": "cody@frg.earth",
        "default_signature_text": "",
        "default_signature_html": "",
        "notes": "Use Resend for outbound transactional/agent email. Inbound should arrive through Cloudflare Email Routing.",
    }


def write_resend_config_template(path: Path, *, force: bool = False) -> dict[str, Any]:
    if path.exists() and not force:
        return {"path": str(path), "created": False, "already_exists": True}

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(resend_config_template(), indent=2) + "\n")
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


def _sender_domain(value: str) -> str:
    _, email_address = parseaddr(value or "")
    return email_address.partition("@")[2].strip().lower()


def _load_resend_config(config_path: Path) -> dict[str, Any]:
    config = _load_json(config_path)
    if not config:
        raise FileNotFoundError(
            f"Resend config not found at {config_path}. Run `zsh ./bin/life-ops resend-init-config` first."
        )

    api_key_env = _strip_string(config.get("api_key_env")) or "RESEND_API_KEY"
    if not _strip_string(config.get("api_key")):
        config["api_key"] = (
            _strip_string(os.getenv(api_key_env))
            or _strip_string(credentials.resolve_secret(name=api_key_env) or "")
        )
    config["api_key_env"] = api_key_env
    config["api_base_url"] = _strip_string(config.get("api_base_url")) or RESEND_API_BASE_URL
    config["default_from"] = _strip_string(config.get("default_from"))
    config["default_reply_to"] = _strip_string(config.get("default_reply_to"))
    config["default_signature_text"] = _strip_string(config.get("default_signature_text"))
    config["default_signature_html"] = _strip_string(config.get("default_signature_html"))
    return config


def _append_signature_text(body: str, signature: str) -> str:
    clean_body = _strip_string(body)
    clean_signature = _strip_string(signature)
    if not clean_signature:
        return clean_body
    if not clean_body:
        return clean_signature
    if clean_body.endswith(clean_signature):
        return clean_body
    return f"{clean_body}\n\n{clean_signature}"


def _append_signature_html(body: str, signature: str) -> str:
    clean_body = _strip_string(body)
    clean_signature = _strip_string(signature)
    if not clean_signature:
        return clean_body
    if not clean_body:
        return clean_signature
    if clean_body.endswith(clean_signature):
        return clean_body
    return f"{clean_body}\n\n{clean_signature}"


def _inline_attachment_parts(spec: str) -> tuple[Path, str]:
    clean = _strip_string(spec)
    if "::" in clean:
        path_text, _, content_id = clean.partition("::")
    else:
        path_text, _, content_id = clean.partition("=")
    attachment_path = Path(_strip_string(path_text)).expanduser()
    resolved_content_id = _strip_string(content_id) or attachment_path.stem
    if not resolved_content_id:
        raise ValueError("inline attachment content id is required")
    return attachment_path, resolved_content_id


def _attachment_payload_from_path(path: Path, *, content_id: str | None = None) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"attachment not found: {path}")
    raw_bytes = path.read_bytes()
    payload: dict[str, Any] = {
        "filename": path.name,
        "content": base64.b64encode(raw_bytes).decode("ascii"),
    }
    if content_id:
        payload["contentId"] = content_id
    return payload


def _attachment_metadata_from_path(path: Path, *, content_id: str | None = None) -> dict[str, Any]:
    mime_type, _ = mimetypes.guess_type(path.name)
    raw_bytes = path.read_bytes()
    return {
        "filename": path.name,
        "mime_type": mime_type or "application/octet-stream",
        "size": len(raw_bytes),
        "inline": bool(content_id),
        "content_id": content_id or "",
        "content_disposition": "inline" if content_id else "attachment",
        "path": path,
        "sha256": hashlib.sha256(raw_bytes).hexdigest(),
    }


def _utc_now_string() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _coerce_utc_datetime(value: datetime | str | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        clean = _strip_string(value)
        if not clean:
            return None
        parsed = datetime.fromisoformat(clean.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def _utc_datetime_string(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _retry_delay_seconds(attempt_count: int) -> int:
    return min(3600, 30 * (2 ** max(0, int(attempt_count) - 1)))


def _next_retry_at(attempt_count: int) -> str:
    return (
        datetime.now(timezone.utc) + timedelta(seconds=_retry_delay_seconds(attempt_count))
    ).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def resend_get_email(*, email_id: str, config_path: Path | None = None) -> dict[str, Any]:
    clean_id = _mail_metadata_strip(email_id)
    if not clean_id:
        raise ValueError("email_id is required")
    config = _load_resend_config(config_path or default_resend_config_path())
    api_key = _strip_string(config.get("api_key"))
    if not api_key:
        raise RuntimeError(f"Set {config['api_key_env']} before using Resend commands.")
    return _resend_request_json(
        method="GET",
        path=f"/emails/{clean_id}",
        api_key=api_key,
        api_base_url=str(config["api_base_url"]),
    )


def _mail_metadata_strip(value: Any) -> str:
    return mail_metadata.strip_string(value)


def _payload_with_stored_attachments(payload_snapshot: dict[str, Any]) -> dict[str, Any]:
    request_payload = dict(payload_snapshot.get("request") or {})
    stored_attachments = list(payload_snapshot.get("stored_attachments") or [])
    if not stored_attachments:
        return request_payload

    attachments: list[dict[str, Any]] = []
    for item in stored_attachments:
        relative_path = _strip_string(item.get("relative_path"))
        if not relative_path:
            continue
        raw_bytes = mail_vault.read_encrypted_vault_file(
            vault_root=store.attachment_vault_root(),
            relative_path=relative_path,
        )
        attachment_payload: dict[str, Any] = {
            "filename": _strip_string(item.get("filename")) or "attachment.bin",
            "content": base64.b64encode(raw_bytes).decode("ascii"),
        }
        content_id = _strip_string(item.get("content_id"))
        if content_id:
            attachment_payload["contentId"] = content_id
        attachments.append(attachment_payload)
    if attachments:
        request_payload["attachments"] = attachments
    return request_payload


def _queue_outbound_mail_artifacts(
    *,
    connection,
    communication_id: int,
    queue_key: str,
    payload_snapshot: dict[str, Any],
    attachment_specs: list[dict[str, Any]],
) -> dict[str, Any]:
    token = hashlib.sha256(queue_key.encode("utf-8")).hexdigest()[:12]
    communication_root = store.attachment_vault_root() / "resend_email" / f"communication-{communication_id}" / token
    communication_root.mkdir(parents=True, exist_ok=True)

    manifest_bytes = json.dumps(payload_snapshot, indent=2, sort_keys=True).encode("utf-8")
    manifest_relative_path, manifest_sha256 = mail_vault.write_encrypted_vault_file(
        vault_root=store.attachment_vault_root(),
        relative_dir=Path("resend_email") / f"communication-{communication_id}" / token,
        logical_filename="queued-outbound.json",
        raw_bytes=manifest_bytes,
        metadata={"source": "resend_email", "queue_key": queue_key},
    )

    saved_attachment_ids: list[int] = []
    stored_attachments: list[dict[str, Any]] = []
    for index, spec in enumerate(attachment_specs, start=1):
        source_path = Path(spec["path"])
        relative_path, _ = mail_vault.write_encrypted_vault_file(
            vault_root=store.attachment_vault_root(),
            relative_dir=Path("resend_email") / f"communication-{communication_id}" / token,
            logical_filename=source_path.name,
            raw_bytes=source_path.read_bytes(),
            metadata={"source": "resend_email", "queue_key": queue_key, "attachment_index": index},
        )
        extracted_text, extraction_method = extract_text_from_saved_attachment(
            path=source_path,
            mime_type=str(spec["mime_type"]),
        )
        attachment_id = store.upsert_communication_attachment(
            connection,
            external_key=f"resend_email:{queue_key}:{index}:{source_path.name}",
            communication_id=communication_id,
            source="resend_email",
            external_message_id=queue_key,
            external_attachment_id=f"attachment-{index}",
            part_id=f"attachment-{index}",
            filename=source_path.name,
            mime_type=str(spec["mime_type"]),
            size=int(spec["size"]),
            relative_path=str(relative_path),
            extracted_text=extracted_text,
            extracted_text_path="",
            extraction_method=extraction_method,
            ingest_status="stored",
            error_text="",
            sha256=str(spec["sha256"]),
        )
        saved_attachment_ids.append(attachment_id)
        stored_attachments.append(
            {
                "filename": source_path.name,
                "mime_type": str(spec["mime_type"]),
                "size": int(spec["size"]),
                "inline": bool(spec["inline"]),
                "content_id": str(spec["content_id"]),
                "content_disposition": str(spec["content_disposition"]),
                "relative_path": str(relative_path),
                "sha256": str(spec["sha256"]),
            }
        )

    return {
        "raw_relative_path": str(manifest_relative_path),
        "raw_sha256": manifest_sha256,
        "saved_attachment_ids": saved_attachment_ids,
        "stored_attachments": stored_attachments,
        "artifact_token": token,
    }


def _save_delivery_receipt_manifest(
    *,
    communication_id: int,
    queue_key: str,
    payload_snapshot: dict[str, Any],
) -> dict[str, str]:
    token = hashlib.sha256(queue_key.encode("utf-8")).hexdigest()[:12]
    manifest_bytes = json.dumps(payload_snapshot, indent=2, sort_keys=True).encode("utf-8")
    manifest_relative_path, manifest_sha256 = mail_vault.write_encrypted_vault_file(
        vault_root=store.attachment_vault_root(),
        relative_dir=Path("resend_email") / f"communication-{communication_id}" / token,
        logical_filename="delivery.json",
        raw_bytes=manifest_bytes,
        metadata={"source": "resend_email", "queue_key": queue_key, "phase": "delivery"},
    )
    return {
        "raw_relative_path": manifest_relative_path,
        "raw_sha256": manifest_sha256,
    }


def _resend_request_json(
    *,
    method: str,
    path: str,
    api_key: str,
    api_base_url: str = RESEND_API_BASE_URL,
    payload: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "User-Agent": RESEND_USER_AGENT,
    }
    if payload is not None:
        headers["Content-Type"] = "application/json"
    req = request.Request(f"{api_base_url.rstrip('/')}{path}", data=body, method=method, headers=headers)
    try:
        with request.urlopen(req, timeout=60) as response:
            raw = response.read().decode("utf-8")
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="ignore")
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"raw": raw}
        message = payload.get("message") or payload.get("name") or raw or str(exc)
        raise RuntimeError(f"Resend API request failed ({exc.code}): {message}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Resend API request failed: {exc.reason}") from exc

    try:
        return json.loads(raw) if raw else {}
    except json.JSONDecodeError as exc:
        raise RuntimeError("Resend API returned non-JSON output.") from exc


def resend_list_domains(*, config_path: Path | None = None) -> dict[str, Any]:
    config = _load_resend_config(config_path or default_resend_config_path())
    api_key = _strip_string(config.get("api_key"))
    if not api_key:
        raise RuntimeError(f"Set {config['api_key_env']} before using Resend commands.")
    return _resend_request_json(
        method="GET",
        path="/domains",
        api_key=api_key,
        api_base_url=str(config["api_base_url"]),
    )


def resend_status(*, config_path: Path | None = None) -> dict[str, Any]:
    target = config_path or default_resend_config_path()
    config_present = target.exists()
    config = _load_json(target) if config_present else {}
    api_key_env = _strip_string(config.get("api_key_env")) or "RESEND_API_KEY"
    api_key_present = bool(
        _strip_string(config.get("api_key"))
        or _strip_string(os.getenv(api_key_env))
        or _strip_string(credentials.resolve_secret(name=api_key_env) or "")
    )
    default_from = _strip_string(config.get("default_from"))
    default_reply_to = _strip_string(config.get("default_reply_to"))
    default_signature_text = _strip_string(config.get("default_signature_text"))
    default_signature_html = _strip_string(config.get("default_signature_html"))
    sender_domain = _sender_domain(default_from)

    next_steps: list[str] = []
    if not config_present:
        next_steps.append("Run `zsh ./bin/life-ops resend-init-config` to create a local Resend config.")
    if not default_from:
        next_steps.append("Set default_from in config/resend.json.")
    if not api_key_present:
        next_steps.append("Register RESEND_API_KEY in the global key registry.")

    status = {
        "config_present": config_present,
        "config_path": str(target),
        "api_key_env": api_key_env,
        "api_key_present": api_key_present,
        "default_from": default_from or None,
        "default_reply_to": default_reply_to or None,
        "default_signature_text_present": bool(default_signature_text),
        "default_signature_html_present": bool(default_signature_html),
        "sender_domain": sender_domain or None,
        "ready": False,
        "next_steps": next_steps,
        "domains": None,
        "sender_domain_ready": False,
        "error": None,
    }
    if config_present and api_key_present:
        try:
            domains_payload = resend_list_domains(config_path=target)
            domains = domains_payload.get("data") or []
            domain_names = [str(domain.get("name")) for domain in domains if domain.get("name")]
            sender_domain_record = next(
                (domain for domain in domains if _strip_string(domain.get("name")).lower() == sender_domain),
                None,
            )
            sender_domain_ready = bool(
                sender_domain_record
                and _strip_string(sender_domain_record.get("status")).lower() == "verified"
                and _strip_string((sender_domain_record.get("capabilities") or {}).get("sending")).lower() == "enabled"
            )
            status["domains"] = {
                "count": len(domains),
                "names": domain_names,
            }
            status["sender_domain_ready"] = sender_domain_ready
            status["ready"] = config_present and api_key_present and bool(default_from) and sender_domain_ready
            if sender_domain and not sender_domain_record:
                status["next_steps"] = [*status["next_steps"], f"Create and verify `{sender_domain}` in Resend."]
            elif sender_domain_record and not sender_domain_ready:
                status["next_steps"] = [*status["next_steps"], f"Finish verifying sending for `{sender_domain}` in Resend."]
        except Exception as exc:  # pragma: no cover - defensive runtime status
            status["error"] = str(exc)
            status["next_steps"] = [*status["next_steps"], "Validate your Resend API key and domain access."]
    return status


def resend_create_domain(*, name: str, config_path: Path | None = None, region: str = "us-east-1") -> dict[str, Any]:
    clean_name = _strip_string(name)
    if not clean_name:
        raise ValueError("name is required")
    config = _load_resend_config(config_path or default_resend_config_path())
    api_key = _strip_string(config.get("api_key"))
    if not api_key:
        raise RuntimeError(f"Set {config['api_key_env']} before using Resend commands.")
    return _resend_request_json(
        method="POST",
        path="/domains",
        api_key=api_key,
        api_base_url=str(config["api_base_url"]),
        payload={"name": clean_name, "region": region},
    )


def resend_set_default_signature(
    *,
    signature_text: str | None = None,
    signature_html: str | None = None,
    config_path: Path | None = None,
) -> dict[str, Any]:
    target = config_path or default_resend_config_path()
    config = _load_json(target)
    if not config:
        raise FileNotFoundError(
            f"Resend config not found at {target}. Run `zsh ./bin/life-ops resend-init-config` first."
        )
    config["default_signature_text"] = _strip_string(signature_text)
    config["default_signature_html"] = _strip_string(signature_html)
    target.write_text(json.dumps(config, indent=2) + "\n")
    return {
        "config_path": str(target),
        "default_signature_text_present": bool(config["default_signature_text"]),
        "default_signature_html_present": bool(config["default_signature_html"]),
    }


def resend_get_default_signature(*, config_path: Path | None = None) -> dict[str, Any]:
    config = _load_resend_config(config_path or default_resend_config_path())
    return {
        "config_path": str(config_path or default_resend_config_path()),
        "default_signature_text": config.get("default_signature_text") or "",
        "default_signature_html": config.get("default_signature_html") or "",
        "default_signature_text_present": bool(config.get("default_signature_text")),
        "default_signature_html_present": bool(config.get("default_signature_html")),
    }


def resend_queue_status(*, db_path: Path | None = None, limit: int = 25) -> dict[str, Any]:
    target_db_path = db_path or store.default_db_path()
    with store.open_db(target_db_path) as connection:
        rows = store.list_mail_delivery_queue(
            connection,
            provider="resend",
            status="all",
            limit=max(1, int(limit)),
        )
        alerts = store.list_system_alerts(
            connection,
            source="resend_delivery",
            status="active",
            limit=max(1, int(limit)),
        )

    counts: dict[str, int] = {}
    for row in rows:
        status_key = str(row["status"] or "unknown")
        counts[status_key] = counts.get(status_key, 0) + 1
    due_before = _utc_now_string()
    active_queue_count = sum(
        1
        for row in rows
        if str(row["status"] or "") in {"queued", "retrying"}
    )
    due_count = sum(
        1
        for row in rows
        if str(row["status"] or "") in {"queued", "retrying"} and str(row["next_attempt_at"] or "") <= due_before
    )
    return {
        "db_path": str(target_db_path),
        "queue_count": active_queue_count,
        "retained_count": len(rows),
        "due_count": due_count,
        "counts": counts,
        "items": [
            {
                "queue_id": int(row["id"]),
                "queue_key": str(row["queue_key"] or ""),
                "communication_id": int(row["communication_id"]),
                "status": str(row["status"] or ""),
                "attempt_count": int(row["attempt_count"]),
                "max_attempts": int(row["max_attempts"]),
                "next_attempt_at": str(row["next_attempt_at"] or ""),
                "last_error": str(row["last_error"] or ""),
                "subject": str(row["subject"] or ""),
                "external_to": str(row["external_to"] or ""),
            }
            for row in rows
        ],
        "active_alert_count": len(alerts),
        "active_alerts": [
            {
                "alert_key": str(row["alert_key"] or ""),
                "severity": str(row["severity"] or ""),
                "title": str(row["title"] or ""),
                "message": str(row["message"] or ""),
            }
            for row in alerts
        ],
    }


def process_resend_delivery_queue(
    *,
    db_path: Path | None = None,
    config_path: Path | None = None,
    limit: int = DEFAULT_RESEND_PROCESS_LIMIT,
    queue_ids: Optional[list[int]] = None,
) -> dict[str, Any]:
    target_db_path = db_path or store.default_db_path()
    started_at = _utc_now_string()
    processed: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    config = _load_resend_config(config_path or default_resend_config_path())
    api_key = _strip_string(config.get("api_key"))
    if not api_key:
        raise RuntimeError(f"Set {config['api_key_env']} before using Resend commands.")

    with store.open_db(target_db_path) as connection:
        if queue_ids:
            queue_rows = []
            for queue_id in queue_ids:
                row = store.get_mail_delivery_queue_item(connection, queue_id=int(queue_id))
                if row is not None:
                    queue_rows.append(row)
        else:
            queue_rows = connection.execute(
                """
                SELECT mail_delivery_queue.*, communications.subject, communications.external_to
                FROM mail_delivery_queue
                LEFT JOIN communications ON communications.id = mail_delivery_queue.communication_id
                WHERE mail_delivery_queue.provider = ?
                  AND mail_delivery_queue.status IN ('queued', 'retrying')
                  AND mail_delivery_queue.next_attempt_at <= ?
                ORDER BY mail_delivery_queue.next_attempt_at, mail_delivery_queue.id
                LIMIT ?
                """,
                ("resend", started_at, max(1, int(limit))),
            ).fetchall()
        eligible_rows = [
            row
            for row in queue_rows
            if str(row["status"] or "") in {"queued", "retrying"}
        ]

        for row in eligible_rows:
            queue_id = int(row["id"])
            queue_key = str(row["queue_key"] or "")
            communication_id = int(row["communication_id"])
            attempt_count = int(row["attempt_count"]) + 1
            max_attempts = int(row["max_attempts"])
            payload_snapshot = json.loads(str(row["payload_json"] or "{}") or "{}")
            try:
                request_payload = _payload_with_stored_attachments(payload_snapshot)
                store.update_mail_delivery_queue_item(
                    connection,
                    queue_id=queue_id,
                    status="sending",
                    attempt_count=attempt_count,
                    last_attempt_at=started_at,
                )
                response = _resend_request_json(
                    method="POST",
                    path="/emails",
                    api_key=api_key,
                    api_base_url=str(config["api_base_url"]),
                    payload=request_payload,
                )
                provider_message_id = _strip_string(response.get("id"))
                receipt = _save_delivery_receipt_manifest(
                    communication_id=communication_id,
                    queue_key=queue_key,
                    payload_snapshot={
                        "provider": "resend",
                        "queued_at": payload_snapshot.get("queued_at"),
                        "sent_at": _utc_now_string(),
                        "request": payload_snapshot.get("request") or {},
                        "stored_attachments": payload_snapshot.get("stored_attachments") or [],
                        "response": response,
                    },
                )
                store.update_mail_delivery_queue_item(
                    connection,
                    queue_id=queue_id,
                    status="sent",
                    attempt_count=attempt_count,
                    next_attempt_at=started_at,
                    provider_message_id=provider_message_id,
                    last_error="",
                    metadata={"last_response": response},
                )
                connection.execute(
                    """
                    UPDATE communications
                    SET status = ?, notes = ?, external_id = ?, raw_relative_path = ?, raw_sha256 = ?
                    WHERE id = ?
                    """,
                    (
                        "sent",
                        "Sent through Resend.",
                        provider_message_id or str(row["queue_key"] or ""),
                        receipt["raw_relative_path"],
                        receipt["raw_sha256"],
                        communication_id,
                    ),
                )
                connection.commit()
                store.clear_system_alert(connection, f"{RESEND_QUEUE_ALERT_PREFIX}:{queue_key}")
                processed.append(
                    {
                        "queue_id": queue_id,
                        "queue_key": queue_key,
                        "communication_id": communication_id,
                        "status": "sent",
                        "provider_message_id": provider_message_id,
                        "attempt_count": attempt_count,
                    }
                )
            except Exception as exc:
                terminal = attempt_count >= max_attempts
                retry_status = "failed" if terminal else "retrying"
                next_attempt_at = started_at if terminal else _next_retry_at(attempt_count)
                error_text = str(exc)
                store.update_mail_delivery_queue_item(
                    connection,
                    queue_id=queue_id,
                    status=retry_status,
                    attempt_count=attempt_count,
                    next_attempt_at=next_attempt_at,
                    last_error=error_text,
                )
                connection.execute(
                    """
                    UPDATE communications
                    SET status = ?, notes = ?
                    WHERE id = ?
                    """,
                    (
                        retry_status,
                        (
                            f"Outbound send failed permanently after {attempt_count} attempts: {error_text}"
                            if terminal
                            else f"Outbound send retry scheduled after attempt {attempt_count}: {error_text}"
                        ),
                        communication_id,
                    ),
                )
                connection.commit()
                store.upsert_system_alert(
                    connection,
                    alert_key=f"{RESEND_QUEUE_ALERT_PREFIX}:{queue_key}",
                    source="resend_delivery",
                    severity="error" if terminal else "warning",
                    title="Outbound email delivery failed" if terminal else "Outbound email retry scheduled",
                    message=error_text,
                    details={
                        "queue_id": queue_id,
                        "queue_key": queue_key,
                        "communication_id": communication_id,
                        "attempt_count": attempt_count,
                        "max_attempts": max_attempts,
                        "next_attempt_at": next_attempt_at,
                        "subject": str(row["subject"] or ""),
                        "external_to": str(row["external_to"] or ""),
                    },
                )
                failures.append(
                    {
                        "queue_id": queue_id,
                        "queue_key": queue_key,
                        "communication_id": communication_id,
                        "status": retry_status,
                        "attempt_count": attempt_count,
                        "next_attempt_at": next_attempt_at,
                        "error": error_text,
                    }
                )

        if failures:
            store.upsert_system_alert(
                connection,
                alert_key=RESEND_QUEUE_GLOBAL_ALERT_KEY,
                source="resend_delivery",
                severity="warning",
                title="Resend delivery queue has pending failures",
                message=f"{len(failures)} outbound message(s) need retry or attention.",
                details={"failures": failures[:10], "processed_count": len(processed)},
            )
        else:
            store.clear_system_alert(connection, RESEND_QUEUE_GLOBAL_ALERT_KEY)

        store.set_sync_state(connection, "resend_queue:last_processed_at", started_at)
        if processed:
            store.set_sync_state(connection, "resend_queue:last_success_at", started_at)
        if failures:
            store.set_sync_state(connection, "resend_queue:last_failure_at", started_at)

    return {
        "db_path": str(target_db_path),
        "processed_count": len(processed),
        "failed_count": len(failures),
        "processed": processed,
        "failures": failures,
        "last_processed_at": started_at,
    }


def resend_send_email(
    *,
    to: list[str],
    subject: str,
    text: str | None = None,
    html: str | None = None,
    from_email: str | None = None,
    cc: Optional[list[str]] = None,
    bcc: Optional[list[str]] = None,
    reply_to: str | None = None,
    in_reply_to: str | None = None,
    references: Optional[list[str]] = None,
    thread_key: str | None = None,
    attachment_paths: Optional[list[str]] = None,
    inline_attachment_specs: Optional[list[str]] = None,
    apply_signature: bool = True,
    journal_db_path: Path | None = None,
    config_path: Path | None = None,
    attempt_immediately: bool = True,
    max_attempts: int = DEFAULT_RESEND_MAX_ATTEMPTS,
    scheduled_at: datetime | str | None = None,
) -> dict[str, Any]:
    recipients = [address.strip() for address in to if address and address.strip()]
    if not recipients:
        raise ValueError("at least one recipient is required")
    cc_recipients = [address.strip() for address in (cc or []) if address and address.strip()]
    bcc_recipients = [address.strip() for address in (bcc or []) if address and address.strip()]
    clean_subject = _strip_string(subject)
    if not clean_subject:
        raise ValueError("subject is required")
    clean_text = _strip_string(text)
    clean_html = _strip_string(html)
    if not clean_text and not clean_html:
        raise ValueError("either text or html is required")

    config = _load_resend_config(config_path or default_resend_config_path())
    api_key = _strip_string(config.get("api_key"))
    if not api_key:
        raise RuntimeError(f"Set {config['api_key_env']} before using Resend commands.")

    from_value = _strip_string(from_email) or _strip_string(config.get("default_from"))
    if not from_value:
        raise ValueError("from_email is required when default_from is not set")
    reply_value = _strip_string(reply_to) or _strip_string(config.get("default_reply_to"))
    sender_domain = _sender_domain(from_value) or "frg.earth"
    outbound_message_id = f"<lifeops-{secrets.token_hex(12)}@{sender_domain}>"
    if apply_signature:
        clean_text = _append_signature_text(clean_text, _strip_string(config.get("default_signature_text")))
        clean_html = _append_signature_html(clean_html, _strip_string(config.get("default_signature_html")))
    payload: dict[str, Any] = {
        "from": from_value,
        "to": recipients,
        "subject": clean_subject,
    }
    if cc_recipients:
        payload["cc"] = cc_recipients
    if bcc_recipients:
        payload["bcc"] = bcc_recipients
    if clean_text:
        payload["text"] = clean_text
    if clean_html:
        payload["html"] = clean_html
    if reply_value:
        payload["reply_to"] = [reply_value]
    clean_in_reply_to = _strip_string(in_reply_to)
    reference_tokens = mail_metadata.message_id_tokens(references or [])
    header_values: dict[str, str] = {}
    header_values["Message-ID"] = outbound_message_id
    if clean_in_reply_to:
        header_values["In-Reply-To"] = clean_in_reply_to
    if reference_tokens:
        header_values["References"] = " ".join(reference_tokens)
    if header_values:
        payload["headers"] = header_values
    all_attachments: list[dict[str, Any]] = []
    attachment_specs: list[dict[str, Any]] = []
    for item in attachment_paths or []:
        attachment_path = Path(_strip_string(item)).expanduser()
        all_attachments.append(_attachment_payload_from_path(attachment_path))
        attachment_specs.append(_attachment_metadata_from_path(attachment_path))
    for item in inline_attachment_specs or []:
        attachment_path, content_id = _inline_attachment_parts(item)
        all_attachments.append(_attachment_payload_from_path(attachment_path, content_id=content_id))
        attachment_specs.append(_attachment_metadata_from_path(attachment_path, content_id=content_id))
    if all_attachments:
        payload["attachments"] = all_attachments

    happened_at = datetime.now()
    derived_thread_key = _strip_string(thread_key) or mail_metadata.derive_thread_key(
        message_id=outbound_message_id,
        in_reply_to=clean_in_reply_to,
        references=reference_tokens,
        fallback=outbound_message_id,
    )
    from_name, from_address = parseaddr(from_value)
    from_record = {"name": _strip_string(from_name), "email": _strip_string(from_address)}
    to_recipients = mail_metadata.parse_address_values(recipients)
    cc_recipient_records = mail_metadata.parse_address_values(cc_recipients)
    bcc_recipient_records = mail_metadata.parse_address_values(bcc_recipients)
    reply_to_recipient_records = mail_metadata.parse_address_values([reply_value] if reply_value else [])

    target_db_path = journal_db_path or store.default_db_path()
    queue_key = f"resend:{outbound_message_id}"
    scheduled_dt = _coerce_utc_datetime(scheduled_at)
    next_attempt_at = _utc_datetime_string(scheduled_dt) if scheduled_dt is not None else _utc_now_string()
    with store.open_db(target_db_path) as connection:
        communication_id = store.upsert_communication_from_sync(
            connection,
            source="resend_email",
            external_id=f"queued:{outbound_message_id}",
            subject=clean_subject,
            channel="email",
            happened_at=happened_at,
            follow_up_at=None,
            direction="outbound",
            person=mail_metadata.format_addresses(to_recipients[:1]),
            notes="Queued for Resend delivery.",
            status="queued",
            external_thread_id=derived_thread_key,
            external_from=from_value,
            external_to=", ".join(recipients),
            external_cc=", ".join(cc_recipients),
            external_bcc=", ".join(bcc_recipients),
            external_reply_to=reply_value,
            from_value=from_record,
            to_recipients=to_recipients,
            cc_recipients=cc_recipient_records,
            bcc_recipients=bcc_recipient_records,
            reply_to_recipients=reply_to_recipient_records,
            message_id=outbound_message_id,
            in_reply_to=clean_in_reply_to,
            references=reference_tokens,
            headers=header_values,
            thread_key=derived_thread_key,
            snippet=(clean_text or mail_metadata.strip_string(clean_html))[:280],
            body_text=clean_text,
            html_body=clean_html,
            attachments=[
                {
                    "filename": spec["filename"],
                    "mime_type": spec["mime_type"],
                    "size": spec["size"],
                    "inline": spec["inline"],
                    "content_id": spec["content_id"],
                    "content_disposition": spec["content_disposition"],
                }
                for spec in attachment_specs
            ],
            raw_relative_path="",
            raw_sha256="",
        )
        try:
            artifact_summary = _queue_outbound_mail_artifacts(
                connection=connection,
                communication_id=communication_id,
                queue_key=queue_key,
                payload_snapshot={
                    "provider": "resend",
                    "queued_at": _utc_now_string(),
                    "request": {
                        key: value
                        for key, value in payload.items()
                        if key != "attachments"
                    },
                },
                attachment_specs=attachment_specs,
            )
            connection.execute(
                "UPDATE communications SET raw_relative_path = ?, raw_sha256 = ? WHERE id = ?",
                (
                    artifact_summary["raw_relative_path"],
                    artifact_summary["raw_sha256"],
                    communication_id,
                ),
            )
            queue_id = store.enqueue_mail_delivery(
                connection,
                queue_key=queue_key,
                provider="resend",
                communication_id=communication_id,
                payload={
                    "queued_at": _utc_now_string(),
                    "request": {
                        key: value
                        for key, value in payload.items()
                        if key != "attachments"
                    },
                    "stored_attachments": artifact_summary["stored_attachments"],
                },
                metadata={"thread_key": derived_thread_key, "message_id": outbound_message_id},
                status="queued",
                attempt_count=0,
                max_attempts=max(1, int(max_attempts)),
                next_attempt_at=next_attempt_at,
            )
            connection.commit()
        except Exception as exc:
            note = f"Suppressed orphaned outbound Resend artifact before queue creation: {exc}"
            connection.execute(
                """
                UPDATE communications
                SET status = 'deleted',
                    deleted_at = ?,
                    notes = ?
                WHERE id = ?
                """,
                (_utc_now_string(), note, communication_id),
            )
            connection.commit()
            raise

    delivery_result: dict[str, Any] = {
        "queued": True,
        "sent": False,
        "communication_id": communication_id,
        "queue_id": queue_id,
        "queue_key": queue_key,
        "thread_key": derived_thread_key,
        "message_id": outbound_message_id,
        "status": "queued",
        "next_attempt_at": next_attempt_at,
    }
    if scheduled_dt is not None:
        delivery_result["scheduled_at"] = next_attempt_at
    if attempt_immediately and next_attempt_at <= _utc_now_string():
        process_result = process_resend_delivery_queue(
            db_path=target_db_path,
            config_path=config_path,
            limit=1,
            queue_ids=[queue_id],
        )
        matched_sent = next((row for row in process_result["processed"] if int(row["queue_id"]) == int(queue_id)), None)
        matched_failure = next((row for row in process_result["failures"] if int(row["queue_id"]) == int(queue_id)), None)
        delivery_result["attempted_immediately"] = True
        delivery_result["processed_count"] = process_result["processed_count"]
        delivery_result["failed_count"] = process_result["failed_count"]
        if matched_sent:
            delivery_result.update(
                {
                    "sent": True,
                    "status": "sent",
                    "id": matched_sent.get("provider_message_id") or "",
                    "provider_message_id": matched_sent.get("provider_message_id") or "",
                    "attempt_count": matched_sent.get("attempt_count", 1),
                }
            )
        elif matched_failure:
            delivery_result.update(
                {
                    "sent": False,
                    "status": matched_failure.get("status") or "retrying",
                    "provider_message_id": "",
                    "attempt_count": matched_failure.get("attempt_count", 1),
                    "next_attempt_at": matched_failure.get("next_attempt_at") or "",
                    "error": matched_failure.get("error") or "",
                }
            )
    elif attempt_immediately:
        delivery_result["attempted_immediately"] = False
        delivery_result["deferred_until"] = next_attempt_at
    return delivery_result
