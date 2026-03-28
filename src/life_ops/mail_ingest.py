from __future__ import annotations

import base64
import hmac
import hashlib
import html
import json
import mimetypes
import os
import re
import secrets
import tempfile
from datetime import datetime
from datetime import timezone
from email import policy
from email.message import Message
from email.parser import BytesParser
from email.utils import parseaddr, parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Optional

from life_ops import classification
from life_ops import credentials
from life_ops.document_ingest import CODE_SUFFIXES, extract_text_from_saved_attachment
from life_ops import mail_metadata
from life_ops import mail_vault
from life_ops import store

MAIL_INGEST_SECRET_NAME = "LIFE_OPS_MAIL_INGEST_SECRET"
DEFAULT_MAIL_INGEST_HOST = "127.0.0.1"
DEFAULT_MAIL_INGEST_PORT = 8788
DEFAULT_MAIL_INGEST_PATH = "/api/mail/inbound"
MAIL_INGEST_SIGNATURE_HEADER = "X-Life-Ops-Signature"
MAIL_INGEST_TIMESTAMP_HEADER = "X-Life-Ops-Timestamp"
MAIL_INGEST_SIGNATURE_VERSION = "sha256"
DEFAULT_MAIL_INGEST_MAX_SKEW_SECONDS = 300
MAIL_ATTACHMENT_TEXT_LIMIT = 12000
MAIL_INLINE_ATTACHMENT_SUFFIX_FALLBACK = ".bin"
FORENSIC_HEADERS_ENV = "LIFE_OPS_FORENSIC_HEADERS"


def _strip_string(value: Any) -> str:
    return str(value or "").strip()


def _utc_now() -> datetime:
    return datetime.utcnow()


def _collapse_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _html_to_text(value: str) -> str:
    without_tags = re.sub(r"<[^>]+>", " ", value or "")
    return _collapse_whitespace(html.unescape(without_tags))


def _truncate_text(value: str, limit: int) -> str:
    collapsed = value.strip()
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: max(0, limit - 3)].rstrip() + "..."


def _safe_filename(value: str, fallback: str) -> str:
    collapsed = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip(".-")
    return collapsed or fallback


def _decode_bytes(value: bytes | None, charset: str | None = None) -> str:
    if not value:
        return ""
    encodings = [charset, "utf-8", "latin-1"]
    for candidate in encodings:
        if not candidate:
            continue
        try:
            return value.decode(candidate)
        except (LookupError, UnicodeDecodeError):
            continue
    return value.decode("utf-8", errors="replace")


def _message_datetime(message: Message) -> datetime:
    raw_date = _strip_string(message.get("Date"))
    if raw_date:
        try:
            parsed = parsedate_to_datetime(raw_date)
            if parsed.tzinfo is not None:
                return parsed.astimezone().replace(tzinfo=None)
            return parsed
        except (TypeError, ValueError, IndexError):
            pass
    return datetime.now()


def sign_mail_ingest_payload(*, body_bytes: bytes, secret: str, timestamp: str) -> str:
    canonical = timestamp.encode("utf-8") + b"." + body_bytes
    digest = hmac.new(secret.encode("utf-8"), canonical, hashlib.sha256).hexdigest()
    return f"{MAIL_INGEST_SIGNATURE_VERSION}={digest}"


def verify_mail_ingest_signature(
    *,
    body_bytes: bytes,
    secret: str,
    timestamp: str,
    signature: str,
    now: Optional[datetime] = None,
    max_skew_seconds: int = DEFAULT_MAIL_INGEST_MAX_SKEW_SECONDS,
) -> tuple[bool, str]:
    clean_timestamp = _strip_string(timestamp)
    clean_signature = _strip_string(signature)
    if not clean_timestamp:
        return False, "missing_timestamp"
    if not clean_signature:
        return False, "missing_signature"

    try:
        signed_at = datetime.fromisoformat(clean_timestamp.replace("Z", "+00:00"))
    except ValueError:
        return False, "invalid_timestamp"

    if signed_at.tzinfo is not None:
        signed_at = signed_at.astimezone(timezone.utc).replace(tzinfo=None)
    current = now or _utc_now()
    skew = abs((current - signed_at).total_seconds())
    if skew > max_skew_seconds:
        return False, "stale_timestamp"

    expected = sign_mail_ingest_payload(
        body_bytes=body_bytes,
        secret=secret,
        timestamp=clean_timestamp,
    )
    if not hmac.compare_digest(expected, clean_signature):
        return False, "invalid_signature"
    return True, "ok"


def _attachment_extension(mime_type: str) -> str:
    guessed = mimetypes.guess_extension(mime_type, strict=False)
    if guessed:
        return guessed
    if mime_type == "message/rfc822":
        return ".eml"
    return MAIL_INLINE_ATTACHMENT_SUFFIX_FALLBACK


def _infer_attachment_filename(
    *,
    filename: str,
    mime_type: str,
    content_id: str,
    index: int,
    inline: bool,
) -> str:
    if filename:
        return filename
    stem = f"inline-{content_id}" if content_id else f"{'inline' if inline else 'attachment'}-{index}"
    return f"{stem}{_attachment_extension(mime_type)}"


def _extract_message_content(message: Message) -> tuple[str, str, str, list[dict[str, Any]]]:
    text_parts: list[str] = []
    html_parts: list[str] = []
    attachments: list[dict[str, Any]] = []

    if message.is_multipart():
        parts = message.walk()
    else:
        parts = [message]

    for index, part in enumerate(parts, start=1):
        if part.is_multipart():
            continue

        filename = _strip_string(part.get_filename())
        content_disposition = _strip_string(part.get_content_disposition())
        mime_type = _strip_string(part.get_content_type()) or "application/octet-stream"
        payload_bytes = part.get_payload(decode=True) or b""
        content_id = _strip_string(part.get("Content-ID")).strip("<>")
        inline = content_disposition == "inline" or bool(content_id)

        if filename or content_disposition == "attachment" or inline:
            inferred_filename = _infer_attachment_filename(
                filename=filename,
                mime_type=mime_type,
                content_id=content_id,
                index=index,
                inline=inline,
            )
            attachments.append(
                {
                    "filename": inferred_filename,
                    "original_filename": filename,
                    "mime_type": mime_type,
                    "size": len(payload_bytes),
                    "inline": inline,
                    "content_id": content_id,
                    "content_disposition": content_disposition,
                    "payload_bytes": payload_bytes,
                    "index": index,
                }
            )
            continue

        if mime_type == "text/plain":
            text_parts.append(_decode_bytes(payload_bytes, part.get_content_charset()))
        elif mime_type == "text/html":
            html_parts.append(_decode_bytes(payload_bytes, part.get_content_charset()))

    body_text = _collapse_whitespace("\n\n".join(part for part in text_parts if part))
    html_body = "\n\n".join(part for part in html_parts if part).strip()
    if not body_text and html_body:
        body_text = _collapse_whitespace(_html_to_text(html_body))
    snippet = body_text[:280]
    metadata = []
    for attachment in attachments:
        metadata.append(
            {
                "filename": attachment["filename"],
                "mime_type": attachment["mime_type"],
                "size": attachment["size"],
                "inline": attachment["inline"],
                "content_id": attachment["content_id"],
                "content_disposition": attachment["content_disposition"],
            }
        )
    return body_text, snippet, html_body, metadata


def _extract_message_parts(message: Message) -> list[dict[str, Any]]:
    parts = message.walk() if message.is_multipart() else [message]
    attachments: list[dict[str, Any]] = []
    for index, part in enumerate(parts, start=1):
        if part.is_multipart():
            continue
        filename = _strip_string(part.get_filename())
        content_disposition = _strip_string(part.get_content_disposition())
        mime_type = _strip_string(part.get_content_type()) or "application/octet-stream"
        payload_bytes = part.get_payload(decode=True) or b""
        content_id = _strip_string(part.get("Content-ID")).strip("<>")
        inline = content_disposition == "inline" or bool(content_id)
        if mime_type in {"text/plain", "text/html"} and not filename and not inline and content_disposition != "attachment":
            continue
        if not payload_bytes and not filename and content_disposition != "attachment" and not inline:
            continue
        attachments.append(
            {
                "filename": _infer_attachment_filename(
                    filename=filename,
                    mime_type=mime_type,
                    content_id=content_id,
                    index=index,
                    inline=inline,
                ),
                "original_filename": filename,
                "mime_type": mime_type,
                "size": len(payload_bytes),
                "inline": inline,
                "content_id": content_id,
                "content_disposition": content_disposition,
                "payload_bytes": payload_bytes,
                "index": index,
            }
        )
    return attachments


def _mail_vault_root() -> Path:
    return store.attachment_vault_root()


def _write_mail_vault_file(*, relative_dir: Path, filename: str, raw_bytes: bytes) -> tuple[str, str]:
    return mail_vault.write_encrypted_vault_file(
        vault_root=_mail_vault_root(),
        relative_dir=relative_dir,
        logical_filename=filename,
        raw_bytes=raw_bytes,
        metadata={"storage": "local_mail_vault"},
    )


def _forensic_headers_enabled() -> bool:
    value = _strip_string(os.getenv(FORENSIC_HEADERS_ENV)).lower()
    return value in {"1", "true", "yes", "on"}


def _extract_attachment_text_from_payload(
    *,
    payload_bytes: bytes,
    filename: str,
    mime_type: str,
) -> tuple[str, str]:
    if not payload_bytes:
        return "", ""
    suffix = Path(filename).suffix or _attachment_extension(mime_type)
    with tempfile.NamedTemporaryFile(prefix="life-ops-mail-attachment-", suffix=suffix, delete=False) as handle:
        temp_path = Path(handle.name)
        handle.write(payload_bytes)
    try:
        if (
            mime_type.startswith("text/")
            or temp_path.suffix.lower() in {".csv", ".htm", ".html", ".json", ".md", ".text", ".txt"}
        ) and temp_path.suffix.lower() not in CODE_SUFFIXES:
            decoded = _decode_bytes(payload_bytes, None)
            extracted_text = (
                _html_to_text(decoded)
                if "html" in mime_type or temp_path.suffix.lower() in {".htm", ".html"}
                else decoded
            )
            extraction_method = (
                "html_text"
                if "html" in mime_type or temp_path.suffix.lower() in {".htm", ".html"}
                else "text_decode"
            )
            return extracted_text, extraction_method
        return extract_text_from_saved_attachment(temp_path, mime_type=mime_type)
    finally:
        temp_path.unlink(missing_ok=True)


def _save_cloudflare_mail_artifacts(
    *,
    connection,
    communication_id: int,
    external_id: str,
    raw_bytes: bytes,
    attachment_parts: list[dict[str, Any]],
) -> dict[str, Any]:
    message_hash = hashlib.sha256(raw_bytes).hexdigest()
    relative_dir = Path("cloudflare_email") / f"communication-{communication_id}" / message_hash[:12]
    raw_relative_path, raw_sha256 = _write_mail_vault_file(
        relative_dir=relative_dir,
        filename="message.eml",
        raw_bytes=raw_bytes,
    )

    saved_attachment_ids: list[int] = []
    for attachment in attachment_parts:
        safe_name = _safe_filename(
            str(attachment.get("filename", "")),
            f"attachment-{int(attachment.get('index', 0) or 0)}{_attachment_extension(str(attachment.get('mime_type', '')))}",
        )
        relative_path, sha256 = _write_mail_vault_file(
            relative_dir=relative_dir,
            filename=f"{int(attachment.get('index', 0) or 0):02d}-{safe_name}",
            raw_bytes=bytes(attachment.get("payload_bytes") or b""),
        )
        effective_mime = str(attachment.get("mime_type", ""))
        extracted_text = ""
        extraction_method = ""
        extracted_text_path = ""
        ingest_status = "downloaded"
        if attachment.get("payload_bytes"):
            try:
                extracted_text, extraction_method = _extract_attachment_text_from_payload(
                    payload_bytes=bytes(attachment.get("payload_bytes") or b""),
                    filename=str(attachment.get("filename", "")),
                    mime_type=effective_mime,
                )
                extracted_text = _truncate_text(extracted_text, MAIL_ATTACHMENT_TEXT_LIMIT)
                if extracted_text:
                    text_filename = f"{Path(relative_path).name}.txt"
                    text_relative_path, _ = _write_mail_vault_file(
                        relative_dir=relative_dir,
                        filename=text_filename,
                        raw_bytes=extracted_text.encode("utf-8"),
                    )
                    extracted_text_path = text_relative_path
                    ingest_status = "extracted"
            except Exception as exc:
                ingest_status = "failed"
                extraction_method = ""
                extracted_text = ""
                extracted_text_path = ""
                attachment_error_text = str(exc)
            else:
                attachment_error_text = ""
        else:
            attachment_error_text = ""

        attachment_id = store.upsert_communication_attachment(
            connection,
            external_key=f"{external_id}:attachment:{int(attachment.get('index', 0) or 0)}:{sha256}",
            communication_id=communication_id,
            source="cloudflare_email",
            external_message_id=external_id,
            external_attachment_id=str(attachment.get("content_id", "")),
            part_id=str(int(attachment.get("index", 0) or 0)),
            filename=str(attachment.get("filename", "")),
            mime_type=effective_mime,
            size=int(attachment.get("size", 0) or 0),
            relative_path=relative_path,
            extracted_text=extracted_text,
            extracted_text_path=extracted_text_path,
            extraction_method=extraction_method,
            ingest_status=ingest_status,
            error_text=attachment_error_text,
            sha256=sha256,
        )
        saved_attachment_ids.append(attachment_id)

    return {
        "raw_relative_path": raw_relative_path,
        "raw_sha256": raw_sha256,
        "saved_attachment_ids": saved_attachment_ids,
    }


def mail_ingest_status(*, db_path: Optional[Path] = None) -> dict[str, Any]:
    secret_present = bool(
        _strip_string(os.getenv(MAIL_INGEST_SECRET_NAME))
        or _strip_string(credentials.resolve_secret(name=MAIL_INGEST_SECRET_NAME) or "")
    )
    target_db = db_path or store.default_db_path()
    next_steps: list[str] = []
    if not secret_present:
        next_steps.append("Run `zsh ./bin/life-ops mail-ingest-generate-secret` to create the worker auth secret.")
    next_steps.append("Run `zsh ./bin/life-ops mail-ingest-serve` to start the local inbound receiver.")
    next_steps.append("Expose the local receiver with a tunnel or deploy it behind a public HTTPS endpoint for Cloudflare to reach.")
    next_steps.append("Keep the signing secret private. Worker requests are verified with timestamped HMAC signatures.")
    return {
        "secret_name": MAIL_INGEST_SECRET_NAME,
        "secret_present": secret_present,
        "default_host": DEFAULT_MAIL_INGEST_HOST,
        "default_port": DEFAULT_MAIL_INGEST_PORT,
        "default_path": DEFAULT_MAIL_INGEST_PATH,
        "db_path": str(target_db),
        "ready": secret_present,
        "next_steps": next_steps,
    }


def generate_mail_ingest_secret(
    *,
    backend: str = "auto",
    allow_insecure_file_backend: bool = False,
) -> dict[str, Any]:
    value = secrets.token_urlsafe(32)
    result = credentials.set_secret(
        name=MAIL_INGEST_SECRET_NAME,
        value=value,
        backend=backend,
        allow_insecure_file_backend=allow_insecure_file_backend,
    )
    return {
        **result,
        "secret_name": MAIL_INGEST_SECRET_NAME,
        "generated": True,
    }


def ingest_cloudflare_email_payload(
    payload: dict[str, Any],
    *,
    db_path: Optional[Path] = None,
) -> dict[str, Any]:
    raw_base64 = _strip_string(payload.get("raw_base64"))
    if not raw_base64:
        raise ValueError("payload.raw_base64 is required")

    try:
        raw_bytes = base64.b64decode(raw_base64, validate=True)
    except Exception as exc:  # pragma: no cover - defensive parse guard
        raise ValueError("payload.raw_base64 is not valid base64") from exc

    message = BytesParser(policy=policy.default).parsebytes(raw_bytes)
    subject = _strip_string(message.get("Subject")) or "(no subject)"
    sender = _strip_string(message.get("From")) or _strip_string(payload.get("envelope_from"))
    to = _strip_string(message.get("To")) or _strip_string(payload.get("envelope_to"))
    cc = _strip_string(message.get("Cc"))
    bcc = _strip_string(message.get("Bcc"))
    reply_to = _strip_string(message.get("Reply-To"))
    body_text, snippet, html_body, attachments = _extract_message_content(message)
    attachment_parts = _extract_message_parts(message)
    happened_at = _message_datetime(message)
    message_id = mail_metadata.primary_message_id(message.get_all("Message-ID"))
    if not message_id:
        message_id = f"<cf-{hashlib.sha256(raw_bytes).hexdigest()[:24]}@life-ops>"
    external_id = f"cloudflare:{message_id}"
    in_reply_to = mail_metadata.primary_message_id(message.get_all("In-Reply-To"))
    references = mail_metadata.message_id_tokens(message.get_all("References"))
    thread_key = mail_metadata.derive_thread_key(
        message_id=message_id,
        in_reply_to=in_reply_to,
        references=references,
        fallback=external_id,
    )
    external_thread_id = thread_key or external_id
    sender_name, sender_email = parseaddr(sender)
    person = _strip_string(sender_name) or _strip_string(sender_email)
    envelope_to = _strip_string(payload.get("envelope_to"))
    from_value = {"name": _strip_string(sender_name), "email": _strip_string(sender_email)}
    to_recipients = mail_metadata.parse_address_values(to)
    cc_recipients = mail_metadata.parse_address_values(cc)
    bcc_recipients = mail_metadata.parse_address_values(bcc)
    reply_to_recipients = mail_metadata.parse_address_values(reply_to)
    header_snapshot = mail_metadata.headers_snapshot(message, forensic=_forensic_headers_enabled())

    message_classification = classification.classify_message(
        subject=subject,
        sender=sender,
        to=to,
        cc=cc,
        snippet=snippet,
        body_text=body_text,
        attachments=attachments,
        triage={},
        user_email=envelope_to,
    )
    status = _strip_string(message_classification.get("status")) or "reference"
    follow_up_at = happened_at if status == "open" else None
    raw_sha256 = hashlib.sha256(raw_bytes).hexdigest()

    with store.open_db(db_path or store.default_db_path()) as connection:
        communication_id = store.upsert_communication_from_sync(
            connection,
            source="cloudflare_email",
            external_id=external_id,
            subject=subject,
            channel="email",
            happened_at=happened_at,
            follow_up_at=follow_up_at,
            direction="inbound",
            person=person,
            notes="Ingested from Cloudflare Email Routing.",
            status=status,
            external_thread_id=external_thread_id,
            external_from=sender,
            external_to=to,
            external_cc=cc,
            external_bcc=bcc,
            external_reply_to=reply_to,
            from_value=from_value,
            to_recipients=to_recipients,
            cc_recipients=cc_recipients,
            bcc_recipients=bcc_recipients,
            reply_to_recipients=reply_to_recipients,
            message_id=message_id,
            in_reply_to=in_reply_to,
            references=references,
            headers=header_snapshot,
            thread_key=thread_key,
            snippet=snippet,
            body_text=body_text,
            html_body=html_body,
            attachments=attachments,
            raw_relative_path="",
            raw_sha256=raw_sha256,
            category=_strip_string(message_classification.get("primary_category")),
            categories=list(message_classification.get("categories") or []),
            priority_level=_strip_string(message_classification.get("priority_level")),
            priority_score=int(message_classification.get("priority_score") or 0),
            retention_bucket=_strip_string(message_classification.get("retention_bucket")),
            classifier_version=_strip_string(message_classification.get("classifier_version")),
            classification=message_classification,
        )
        artifact_summary = _save_cloudflare_mail_artifacts(
            connection=connection,
            communication_id=communication_id,
            external_id=external_id,
            raw_bytes=raw_bytes,
            attachment_parts=attachment_parts,
        )
        connection.execute(
            "UPDATE communications SET raw_relative_path = ?, raw_sha256 = ? WHERE id = ?",
            (
                str(artifact_summary["raw_relative_path"]),
                str(artifact_summary["raw_sha256"]),
                communication_id,
            ),
        )
        connection.commit()

    return {
        "communication_id": communication_id,
        "source": "cloudflare_email",
        "external_id": external_id,
        "subject": subject,
        "sender": sender,
        "envelope_to": envelope_to or None,
        "message_id": message_id,
        "thread_key": thread_key,
        "status": status,
        "category": message_classification.get("primary_category"),
        "categories": message_classification.get("categories") or [],
        "priority_level": message_classification.get("priority_level"),
        "priority_score": message_classification.get("priority_score"),
        "attachments_count": len(attachments),
        "raw_relative_path": artifact_summary["raw_relative_path"],
        "saved_attachment_ids": artifact_summary["saved_attachment_ids"],
    }


def _make_handler(*, db_path: Path, secret_name: str, path: str):
    class MailIngestHandler(BaseHTTPRequestHandler):
        def do_POST(self):  # pragma: no cover - exercised via helper in real runtime
            if self.path != path:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"Not found.")
                return

            try:
                content_length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                content_length = 0
            raw = self.rfile.read(content_length)
            expected_secret = _strip_string(os.getenv(secret_name)) or _strip_string(
                credentials.resolve_secret(name=secret_name) or ""
            )
            if expected_secret:
                signature = _strip_string(self.headers.get(MAIL_INGEST_SIGNATURE_HEADER))
                timestamp = _strip_string(self.headers.get(MAIL_INGEST_TIMESTAMP_HEADER))
                verified, reason = verify_mail_ingest_signature(
                    body_bytes=raw,
                    secret=expected_secret,
                    timestamp=timestamp,
                    signature=signature,
                )
                if not verified:
                    self.send_response(401)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": reason}).encode("utf-8"))
                    return
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
            except json.JSONDecodeError:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "invalid_json"}).encode("utf-8"))
                return

            try:
                result = ingest_cloudflare_email_payload(payload, db_path=db_path)
            except Exception as exc:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(exc)}).encode("utf-8"))
                return

            body = json.dumps(result).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):  # pragma: no cover - suppress console noise
            return

    return MailIngestHandler


def serve_mail_ingest(
    *,
    db_path: Path,
    host: str = DEFAULT_MAIL_INGEST_HOST,
    port: int = DEFAULT_MAIL_INGEST_PORT,
    path: str = DEFAULT_MAIL_INGEST_PATH,
) -> None:
    server = HTTPServer((host, port), _make_handler(db_path=db_path, secret_name=MAIL_INGEST_SECRET_NAME, path=path))
    try:
        server.serve_forever()
    finally:  # pragma: no cover - server shutdown path
        server.server_close()
