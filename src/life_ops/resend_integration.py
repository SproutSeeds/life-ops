from __future__ import annotations

import base64
import json
import mimetypes
import os
import re
import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from email.utils import getaddresses, parseaddr
from pathlib import Path
from typing import Any, Optional
from urllib import error, request
from urllib.parse import urlencode

from life_ops import credentials
from life_ops.document_ingest import extract_text_from_saved_attachment
from life_ops import mail_identities
from life_ops import mail_metadata
from life_ops import mail_vault
from life_ops import store

RESEND_API_BASE_URL = "https://api.resend.com"
RESEND_USER_AGENT = "life-ops/0.2 (+https://frg.earth)"
DEFAULT_RESEND_MAX_ATTEMPTS = 8
DEFAULT_RESEND_PROCESS_LIMIT = 25
DEFAULT_RESEND_RECEIVING_PAGE_LIMIT = 100
DEFAULT_RESEND_BATCH_MAX_PER_HOUR = 5
DEFAULT_RESEND_BATCH_MIN_GAP_MINUTES = 12
DEFAULT_RESEND_BATCH_DAILY_CAP = 20
RESEND_QUEUE_ALERT_PREFIX = "resend_delivery"
RESEND_QUEUE_GLOBAL_ALERT_KEY = "resend_delivery_queue"


def default_resend_config_path() -> Path:
    return store.config_root() / "resend.json"


def resend_config_path_for_identity(mail_identity: str = "") -> Path:
    identity_key = mail_identities.normalize_mail_identity(mail_identity)
    if identity_key == mail_identities.CODACLI_MAIL_IDENTITY:
        return store.config_root() / "resend-codacli.json"
    if identity_key == mail_identities.BIND_MY_SOUL_MAIL_IDENTITY:
        return store.config_root() / "resend-bind-my-soul.json"
    if identity_key == mail_identities.DECKFOUND_MAIL_IDENTITY:
        return store.config_root() / "resend-deckfound.json"
    return default_resend_config_path()


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


def _email_addresses(*values: Any) -> set[str]:
    candidates: list[str] = []
    pending = list(values)
    while pending:
        value = pending.pop(0)
        if isinstance(value, dict):
            candidates.extend([str(value.get("email") or ""), str(value.get("address") or "")])
        elif isinstance(value, (list, tuple, set)):
            pending.extend(value)
        else:
            candidates.append(str(value or ""))
    return {
        address.strip().lower()
        for _, address in getaddresses(candidates)
        if address.strip() and "@" in address
    }


def _known_outbound_mail_identity(*values: Any) -> str:
    addresses = _email_addresses(*values)
    for identity_key, identity in mail_identities.MAIL_IDENTITIES.items():
        if identity.address.lower() in addresses:
            return identity_key
    return ""


def _validated_outbound_mail_identity(
    *,
    explicit: str = "",
    from_value: Any = "",
    reply_to_value: Any = "",
    config: Optional[dict[str, Any]] = None,
) -> str:
    requested_identity = _strip_string(explicit).lower()
    if requested_identity not in mail_identities.MAIL_IDENTITIES:
        requested_identity = ""
    sender_identity = _known_outbound_mail_identity(from_value, reply_to_value)
    config_identity = ""
    if config:
        config_identity = _known_outbound_mail_identity(
            config.get("default_from"),
            config.get("default_reply_to"),
        )
    if requested_identity and sender_identity and requested_identity != sender_identity:
        raise ValueError(
            f"Outbound sender belongs to {sender_identity}, but mail_identity is {requested_identity}."
        )
    if sender_identity and config_identity and sender_identity != config_identity:
        sender_domain = _sender_domain(mail_identities.MAIL_IDENTITIES[sender_identity].from_email)
        config_domain = _sender_domain(mail_identities.MAIL_IDENTITIES[config_identity].from_email)
        if not sender_domain or sender_domain != config_domain:
            raise RuntimeError(
                f"Outbound sender belongs to {sender_identity}, but the selected Resend config belongs to {config_identity}."
            )
    return (
        requested_identity
        or sender_identity
        or config_identity
        or mail_identities.DEFAULT_MAIL_IDENTITY
    )


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


def _load_resend_admin_config(config_path: Path) -> dict[str, Any]:
    config = _load_resend_config(config_path)
    admin_api_key_env = _strip_string(config.get("admin_api_key_env")) or "RESEND_ADMIN_API_KEY"
    admin_api_key = (
        _strip_string(config.get("admin_api_key"))
        or _strip_string(os.getenv(admin_api_key_env))
        or _strip_string(credentials.resolve_secret(name=admin_api_key_env) or "")
    )
    config["admin_api_key_env"] = admin_api_key_env
    config["admin_api_key"] = admin_api_key
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
    idempotency_key: str = "",
) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "User-Agent": RESEND_USER_AGENT,
    }
    if payload is not None:
        headers["Content-Type"] = "application/json"
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key
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


def resend_list_received_emails(
    *,
    config_path: Path | None = None,
    limit: int = 25,
    after: str = "",
) -> dict[str, Any]:
    target = config_path or resend_config_path_for_identity(mail_identities.BIND_MY_SOUL_MAIL_IDENTITY)
    config = _load_resend_admin_config(target)
    api_key = _strip_string(config.get("admin_api_key"))
    if not api_key:
        raise RuntimeError(f"Set {config['admin_api_key_env']} before syncing received Resend mail.")
    query = {"limit": max(1, min(100, int(limit)))}
    clean_after = _strip_string(after)
    if clean_after:
        query["after"] = clean_after
    return _resend_request_json(
        method="GET",
        path=f"/emails/receiving?{urlencode(query)}",
        api_key=api_key,
        api_base_url=str(config["api_base_url"]),
    )


def resend_get_received_email(
    *,
    email_id: str,
    config_path: Path | None = None,
) -> dict[str, Any]:
    clean_id = _strip_string(email_id)
    if not clean_id:
        raise ValueError("email_id is required")
    target = config_path or resend_config_path_for_identity(mail_identities.BIND_MY_SOUL_MAIL_IDENTITY)
    config = _load_resend_admin_config(target)
    api_key = _strip_string(config.get("admin_api_key"))
    if not api_key:
        raise RuntimeError(f"Set {config['admin_api_key_env']} before syncing received Resend mail.")
    return _resend_request_json(
        method="GET",
        path=f"/emails/receiving/{clean_id}",
        api_key=api_key,
        api_base_url=str(config["api_base_url"]),
    )


def _received_email_addresses(value: Any) -> set[str]:
    if isinstance(value, (list, tuple, set)):
        values = [str(item or "") for item in value]
    else:
        values = [str(value or "")]
    return {parseaddr(item)[1].strip().lower() for item in values if parseaddr(item)[1].strip()}


def _download_received_raw_message(download_url: str) -> bytes:
    clean_url = _strip_string(download_url)
    if not clean_url:
        raise RuntimeError("Resend received email did not include a raw download URL.")
    req = request.Request(clean_url, headers={"User-Agent": RESEND_USER_AGENT})
    try:
        with request.urlopen(req, timeout=60) as response:
            return response.read()
    except error.HTTPError as exc:
        raise RuntimeError(f"Resend raw message download failed ({exc.code}).") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Resend raw message download failed: {exc.reason}") from exc


def sync_resend_received_mail(
    *,
    db_path: Path | None = None,
    config_path: Path | None = None,
    limit: int = DEFAULT_RESEND_RECEIVING_PAGE_LIMIT,
) -> dict[str, Any]:
    from life_ops.mail_ingest import ingest_cloudflare_email_payload

    target_db_path = db_path or store.default_db_path()
    target_config_path = config_path or resend_config_path_for_identity(
        mail_identities.BIND_MY_SOUL_MAIL_IDENTITY
    )
    page_limit = max(1, min(100, int(limit)))
    records: list[Any] = []
    page_count = 0
    after = ""
    while True:
        listing = resend_list_received_emails(
            config_path=target_config_path,
            limit=page_limit,
            after=after,
        )
        page_count += 1
        page_records = listing.get("data") or []
        if not isinstance(page_records, list):
            page_records = []
        records.extend(page_records)
        if not bool(listing.get("has_more")):
            break
        if not page_records:
            raise RuntimeError("Resend receiving pagination reported more mail without returning a cursor.")
        next_after = _strip_string(page_records[-1].get("id") if isinstance(page_records[-1], dict) else "")
        if not next_after or next_after == after:
            raise RuntimeError("Resend receiving pagination did not advance its cursor.")
        after = next_after
    support_address = mail_identities.mail_identity(
        mail_identities.BIND_MY_SOUL_MAIL_IDENTITY
    ).address.lower()
    ingested: list[dict[str, Any]] = []
    skipped_existing = 0
    skipped_other_identity = 0

    for record in records:
        if not isinstance(record, dict):
            continue
        email_id = _strip_string(record.get("id"))
        if not email_id:
            continue
        external_id = f"resend-receiving:{email_id}"
        with store.open_db(target_db_path) as connection:
            if store.get_communication_by_external_id(
                connection,
                source="resend_inbound",
                external_id=external_id,
            ) is not None:
                skipped_existing += 1
                continue

        listed_recipients = _received_email_addresses(record.get("to"))
        if listed_recipients and support_address not in listed_recipients:
            skipped_other_identity += 1
            continue

        detail = resend_get_received_email(email_id=email_id, config_path=target_config_path)
        detail_recipients = _received_email_addresses(detail.get("to"))
        if detail_recipients and support_address not in detail_recipients:
            skipped_other_identity += 1
            continue
        raw = detail.get("raw") or {}
        download_url = _strip_string(raw.get("download_url") if isinstance(raw, dict) else raw)
        raw_bytes = _download_received_raw_message(download_url)
        result = ingest_cloudflare_email_payload(
            {
                "provider": "resend-receiving",
                "external_id": external_id,
                "envelope_from": _strip_string(detail.get("from") or record.get("from")),
                "envelope_to": support_address,
                "received_at": _strip_string(detail.get("created_at") or record.get("created_at")),
                "raw_base64": base64.b64encode(raw_bytes).decode("ascii"),
                "raw_size": len(raw_bytes),
                "payload_hash": hashlib.sha256(raw_bytes).hexdigest(),
            },
            db_path=target_db_path,
        )
        ingested.append(result)

    return {
        "db_path": str(target_db_path),
        "config_path": str(target_config_path),
        "listed_count": len(records),
        "page_count": page_count,
        "ingested_count": len(ingested),
        "skipped_existing_count": skipped_existing,
        "skipped_other_identity_count": skipped_other_identity,
        "ingested": ingested,
    }


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


def resend_verify_domain(*, domain_id: str, config_path: Path | None = None) -> dict[str, Any]:
    clean_domain_id = _strip_string(domain_id)
    if not clean_domain_id or not all(character.isalnum() or character == "-" for character in clean_domain_id):
        raise ValueError("domain_id must contain only letters, numbers, and hyphens")
    config = _load_resend_config(config_path or default_resend_config_path())
    api_key = _strip_string(config.get("api_key"))
    if not api_key:
        raise RuntimeError(f"Set {config['api_key_env']} before using Resend commands.")
    return _resend_request_json(
        method="POST",
        path=f"/domains/{clean_domain_id}/verify",
        api_key=api_key,
        api_base_url=str(config["api_base_url"]),
        payload={},
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


def resend_queue_status_from_connection(
    connection: Any,
    *,
    limit: int = 25,
) -> dict[str, Any]:
    item_limit = max(1, int(limit))
    due_before = _utc_now_string()
    rows = connection.execute(
        """
        SELECT mail_delivery_queue.*, communications.subject, communications.external_to
        FROM mail_delivery_queue
        LEFT JOIN communications ON communications.id = mail_delivery_queue.communication_id
        WHERE mail_delivery_queue.provider = 'resend'
        ORDER BY
            CASE mail_delivery_queue.status
                WHEN 'failed' THEN 0
                WHEN 'retrying' THEN 1
                WHEN 'queued' THEN 2
                WHEN 'sending' THEN 3
                ELSE 4
            END,
            mail_delivery_queue.id DESC
        LIMIT ?
        """,
        (item_limit,),
    ).fetchall()
    count_rows = connection.execute(
        """
        SELECT status, COUNT(*) AS item_count
        FROM mail_delivery_queue
        WHERE provider = 'resend'
        GROUP BY status
        """
    ).fetchall()
    counts = {
        str(row["status"] or "unknown"): int(row["item_count"] or 0)
        for row in count_rows
    }
    retained_count = sum(counts.values())
    active_queue_count = sum(counts.get(status, 0) for status in ("queued", "retrying"))
    due_row = connection.execute(
        """
        SELECT COUNT(*) AS due_count
        FROM mail_delivery_queue
        WHERE provider = 'resend'
          AND status IN ('queued', 'retrying')
          AND next_attempt_at <= ?
        """,
        (due_before,),
    ).fetchone()
    alerts = store.list_system_alerts(
        connection,
        source="resend_delivery",
        status="active",
        limit=item_limit,
    )
    return {
        "queue_count": active_queue_count,
        "retained_count": retained_count,
        "due_count": int(due_row["due_count"] or 0) if due_row else 0,
        "counts": counts,
        "last_processed_at": str(store.get_sync_state(connection, "resend_queue:last_processed_at") or ""),
        "last_success_at": str(store.get_sync_state(connection, "resend_queue:last_success_at") or ""),
        "last_failure_at": str(store.get_sync_state(connection, "resend_queue:last_failure_at") or ""),
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


def resend_queue_status(*, db_path: Path | None = None, limit: int = 25) -> dict[str, Any]:
    target_db_path = db_path or store.default_db_path()
    with store.open_db(target_db_path) as connection:
        result = resend_queue_status_from_connection(connection, limit=limit)
    return {"db_path": str(target_db_path), **result}


def _same_superseded_delivery(failed: Any, sent: Any) -> bool:
    text_fields = ("in_reply_to", "thread_key", "snippet")
    if any(not _strip_string(failed[field]) for field in text_fields):
        return False
    if any(_strip_string(failed[field]) != _strip_string(sent[field]) for field in text_fields):
        return False
    if _strip_string(failed["subject"]).casefold() != _strip_string(sent["subject"]).casefold():
        return False
    for field in ("external_from", "external_to", "external_cc", "external_bcc", "external_reply_to"):
        if _email_addresses(failed[field]) != _email_addresses(sent[field]):
            return False
    return True


def _same_explicit_superseded_delivery(failed: Any, sent: Any) -> bool:
    if not _strip_string(failed["snippet"]):
        return False
    if _strip_string(failed["snippet"]) != _strip_string(sent["snippet"]):
        return False
    if _strip_string(failed["subject"]).casefold() != _strip_string(sent["subject"]).casefold():
        return False
    for field in ("external_from", "external_to"):
        if _email_addresses(failed[field]) != _email_addresses(sent[field]):
            return False
    return True


def _explicit_superseding_communication_id(failed: Any) -> int | None:
    marker = "suppressed after corrected resend succeeded as communication "
    evidence = f"{_strip_string(failed['last_error'])} {_strip_string(failed['notes'])}".lower()
    if marker not in evidence:
        return None
    suffix = evidence.split(marker, 1)[1]
    digits = ""
    for character in suffix:
        if not character.isdigit():
            break
        digits += character
    return int(digits) if digits else None


def _reconcile_resend_delivery_alerts_from_connection(connection: Any) -> dict[str, Any]:
    reconciled_at = _utc_now_string()
    failed_rows = connection.execute(
        """
        SELECT q.id AS queue_id, q.queue_key, q.communication_id, q.last_error,
               q.created_at AS queue_created_at,
               c.subject, c.external_from, c.external_to, c.external_cc, c.external_bcc,
               c.external_reply_to, c.in_reply_to, c.thread_key, c.snippet, c.mail_identity,
               c.notes
        FROM mail_delivery_queue AS q
        JOIN communications AS c ON c.id = q.communication_id
        WHERE q.provider = 'resend'
          AND q.status = 'failed'
        ORDER BY q.id
        """
    ).fetchall()
    reconciled: list[dict[str, Any]] = []
    for failed in failed_rows:
        explicit_communication_id = _explicit_superseding_communication_id(failed)
        explicit_sent = None
        if explicit_communication_id is not None:
            explicit_sent = connection.execute(
                """
                SELECT q.id AS queue_id, q.queue_key, q.communication_id, q.provider_message_id,
                       q.created_at AS queue_created_at,
                       c.subject, c.external_from, c.external_to, c.external_cc, c.external_bcc,
                       c.external_reply_to, c.in_reply_to, c.thread_key, c.snippet, c.mail_identity
                FROM mail_delivery_queue AS q
                JOIN communications AS c ON c.id = q.communication_id
                WHERE q.provider = 'resend'
                  AND q.status = 'sent'
                  AND q.provider_message_id != ''
                  AND q.communication_id = ?
                  AND q.id > ?
                LIMIT 1
                """,
                (explicit_communication_id, int(failed["queue_id"])),
            ).fetchone()
            if explicit_sent is not None and not _same_explicit_superseded_delivery(failed, explicit_sent):
                explicit_sent = None
        sent_rows = connection.execute(
            """
            SELECT q.id AS queue_id, q.queue_key, q.communication_id, q.provider_message_id,
                   q.created_at AS queue_created_at,
                   c.subject, c.external_from, c.external_to, c.external_cc, c.external_bcc,
                   c.external_reply_to, c.in_reply_to, c.thread_key, c.snippet, c.mail_identity
            FROM mail_delivery_queue AS q
            JOIN communications AS c ON c.id = q.communication_id
            WHERE q.provider = 'resend'
              AND q.status = 'sent'
              AND q.provider_message_id != ''
              AND q.id > ?
              AND datetime(q.created_at) <= datetime(?, '+1 day')
            ORDER BY q.id
            """,
            (int(failed["queue_id"]), str(failed["queue_created_at"] or "")),
        ).fetchall()
        sent = explicit_sent or next(
            (candidate for candidate in sent_rows if _same_superseded_delivery(failed, candidate)),
            None,
        )
        if sent is None:
            continue
        original_error = _strip_string(failed["last_error"])
        effective_identity = _validated_outbound_mail_identity(
            explicit=_strip_string(failed["mail_identity"]),
            from_value=failed["external_from"],
            reply_to_value=failed["external_reply_to"],
        )
        store.update_mail_delivery_queue_item(
            connection,
            queue_id=int(failed["queue_id"]),
            status="superseded",
            last_error="",
            metadata={
                "original_error": original_error,
                "superseded_by_queue_id": int(sent["queue_id"]),
                "superseded_by_communication_id": int(sent["communication_id"]),
                "superseded_by_provider_message_id": str(sent["provider_message_id"] or ""),
                "reconciled_at": reconciled_at,
            },
        )
        audit_note = (
            f"Superseded by corrected delivery queue {int(sent['queue_id'])} "
            f"({str(sent['provider_message_id'] or '')}) at {reconciled_at}."
        )
        if original_error:
            audit_note += f" Original error: {original_error}"
        existing_notes = _strip_string(failed["notes"])
        merged_notes = f"{existing_notes}\n\n{audit_note}" if existing_notes else audit_note
        connection.execute(
            """
            UPDATE communications
            SET status = 'superseded', notes = ?, mail_identity = ?
            WHERE id = ?
            """,
            (merged_notes, effective_identity, int(failed["communication_id"])),
        )
        connection.execute(
            """
            UPDATE communications
            SET mail_identity = ?
            WHERE id = ? AND (mail_identity IS NULL OR mail_identity = '')
            """,
            (effective_identity, int(sent["communication_id"])),
        )
        connection.commit()
        store.clear_system_alert(
            connection,
            f"{RESEND_QUEUE_ALERT_PREFIX}:{str(failed['queue_key'] or '')}",
        )
        reconciled.append(
            {
                "queue_id": int(failed["queue_id"]),
                "communication_id": int(failed["communication_id"]),
                "superseded_by_queue_id": int(sent["queue_id"]),
                "superseded_by_communication_id": int(sent["communication_id"]),
                "provider_message_id": str(sent["provider_message_id"] or ""),
                "mail_identity": effective_identity,
            }
        )

    active_item_alert = connection.execute(
        """
        SELECT 1
        FROM system_alerts
        WHERE source = 'resend_delivery'
          AND status = 'active'
          AND alert_key LIKE ?
        LIMIT 1
        """,
        (f"{RESEND_QUEUE_ALERT_PREFIX}:%",),
    ).fetchone()
    if active_item_alert is None:
        store.clear_system_alert(connection, RESEND_QUEUE_GLOBAL_ALERT_KEY)
    if reconciled:
        store.set_sync_state(connection, "resend_queue:last_reconciled_at", reconciled_at)
    return {
        "reconciled_count": len(reconciled),
        "reconciled": reconciled,
        "reconciled_at": reconciled_at,
    }


def reconcile_resend_delivery_alerts(*, db_path: Path | None = None) -> dict[str, Any]:
    target_db_path = db_path or store.default_db_path()
    with store.open_db(target_db_path) as connection:
        result = _reconcile_resend_delivery_alerts_from_connection(connection)
    return {"db_path": str(target_db_path), **result}


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
    with store.open_db(target_db_path) as connection:
        if queue_ids:
            queue_rows = []
            for queue_id in queue_ids:
                row = connection.execute(
                    """
                    SELECT mail_delivery_queue.*, communications.subject, communications.direction,
                           communications.external_from, communications.external_to,
                           communications.external_reply_to, communications.mail_identity
                    FROM mail_delivery_queue
                    LEFT JOIN communications ON communications.id = mail_delivery_queue.communication_id
                    WHERE mail_delivery_queue.id = ?
                    """,
                    (int(queue_id),),
                ).fetchone()
                if row is not None:
                    queue_rows.append(row)
        else:
            queue_rows = connection.execute(
                """
                SELECT mail_delivery_queue.*, communications.subject, communications.direction,
                       communications.external_from, communications.external_to,
                       communications.external_reply_to, communications.mail_identity
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
                request_snapshot = payload_snapshot.get("request") or {}
                if not isinstance(request_snapshot, dict):
                    request_snapshot = {}
                stored_identity = _strip_string(row["mail_identity"]).lower() if "mail_identity" in row.keys() else ""
                if stored_identity not in mail_identities.MAIL_IDENTITIES:
                    stored_identity = ""
                sender_identity = _known_outbound_mail_identity(
                    request_snapshot.get("from") or row["external_from"],
                    request_snapshot.get("reply_to") or row["external_reply_to"],
                )
                effective_identity = _validated_outbound_mail_identity(
                    explicit=stored_identity,
                    from_value=request_snapshot.get("from") or row["external_from"],
                    reply_to_value=request_snapshot.get("reply_to") or row["external_reply_to"],
                )
                row_config_path = config_path or resend_config_path_for_identity(effective_identity)
                config = _load_resend_config(row_config_path)
                effective_identity = _validated_outbound_mail_identity(
                    explicit=stored_identity or sender_identity,
                    from_value=request_snapshot.get("from") or row["external_from"],
                    reply_to_value=request_snapshot.get("reply_to") or row["external_reply_to"],
                    config=config,
                )
                api_key = _strip_string(config.get("api_key"))
                if not api_key:
                    raise RuntimeError(f"Set {config['api_key_env']} before using Resend commands.")
                if not stored_identity:
                    connection.execute(
                        "UPDATE communications SET mail_identity = ? WHERE id = ?",
                        (effective_identity, communication_id),
                    )
                    connection.commit()
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
                    **({"idempotency_key": payload_snapshot["idempotency_key"]}
                       if payload_snapshot.get("idempotency_key") else {}),
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
                _reconcile_resend_delivery_alerts_from_connection(connection)
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
    mail_identity: str = "",
    idempotency_key: str = "",
) -> dict[str, Any]:
    if idempotency_key and not re.fullmatch(r"[A-Za-z0-9:_-]{1,256}", idempotency_key):
        raise ValueError("Invalid mail idempotency key")
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

    requested_identity = _strip_string(mail_identity).lower()
    if requested_identity not in mail_identities.MAIL_IDENTITIES:
        requested_identity = ""
    sender_identity_hint = _known_outbound_mail_identity(from_email, reply_to)
    if requested_identity and sender_identity_hint and requested_identity != sender_identity_hint:
        raise ValueError(
            f"Outbound sender belongs to {sender_identity_hint}, but mail_identity is {requested_identity}."
        )
    config_identity_hint = requested_identity or sender_identity_hint or mail_identities.DEFAULT_MAIL_IDENTITY
    effective_config_path = config_path or resend_config_path_for_identity(config_identity_hint)
    config = _load_resend_config(effective_config_path)
    api_key = _strip_string(config.get("api_key"))
    if not api_key:
        raise RuntimeError(f"Set {config['api_key_env']} before using Resend commands.")

    from_value = _strip_string(from_email) or _strip_string(config.get("default_from"))
    if not from_value:
        raise ValueError("from_email is required when default_from is not set")
    reply_value = _strip_string(reply_to) or _strip_string(config.get("default_reply_to"))
    effective_mail_identity = _validated_outbound_mail_identity(
        explicit=requested_identity,
        from_value=from_value,
        reply_to_value=reply_value,
        config=config,
    )
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
            mail_identity=effective_mail_identity,
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
                    "idempotency_key": idempotency_key,
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
            config_path=effective_config_path,
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
