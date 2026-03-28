from __future__ import annotations

import base64
import binascii
import hashlib
import json
import mimetypes
import re
import subprocess
import tarfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from life_ops import store
from life_ops import tracing
from life_ops.google_sync import (
    _gmail_header_map,
    _gmail_message_time,
    _gmail_service,
    _html_to_text,
    _walk_gmail_parts,
)

ATTACHMENT_TEXT_LIMIT = 12000
ATTACHMENT_PREVIEW_LIMIT = 280
IMAGE_SUFFIXES = {".gif", ".heic", ".jpeg", ".jpg", ".png", ".tif", ".tiff", ".webp"}
TEXT_SUFFIXES = {".csv", ".json", ".md", ".text", ".txt"}
TEXTUTIL_SUFFIXES = {".doc", ".docx", ".pages", ".rtf"}
CODE_SUFFIXES = {
    ".c", ".cc", ".cpp", ".cs", ".css", ".go", ".h", ".hpp", ".html", ".java", ".js", ".jsx", ".mjs",
    ".php", ".py", ".rb", ".rs", ".sh", ".sql", ".swift", ".ts", ".tsx", ".vue", ".yaml", ".yml",
}
ARCHIVE_SUFFIXES = {
    ".7z", ".bz2", ".gz", ".rar", ".tar", ".tgz", ".xz", ".zip",
}
EXECUTABLE_SUFFIXES = {
    ".app", ".bat", ".bin", ".cmd", ".com", ".dll", ".dmg", ".exe", ".msi", ".pkg", ".ps1", ".scr", ".so",
}
MEDIA_SUFFIXES = {
    ".aac", ".aif", ".aiff", ".avi", ".flac", ".m4a", ".m4v", ".mkv", ".mov", ".mp3", ".mp4", ".mpeg",
    ".mpg", ".oga", ".ogg", ".opus", ".wav", ".webm", ".wmv",
}
DECORATIVE_FILENAME_FRAGMENTS = {
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
DOCUMENT_FILENAME_HINTS = {
    "benefit",
    "birth",
    "card",
    "coverage",
    "document",
    "id",
    "insurance",
    "license",
    "member",
    "notice",
    "passport",
    "policy",
    "report",
    "statement",
    "tax",
    "uscis",
}
SENSITIVE_ATTACHMENT_CATEGORIES = {
    "benefits",
    "identity",
    "immigration",
    "insurance",
    "medical",
    "record_keeping",
    "tax",
}
DEFAULT_PROFILE_ATTACHMENT_BACKFILL_SCOPE = "sensitive"
DEFAULT_PROFILE_ATTACHMENT_BACKFILL_NAMESPACE = "profile_attachment_backfill"


def _safe_filename(value: str, fallback: str) -> str:
    collapsed = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip(".-")
    return collapsed or fallback


def _decode_gmail_bytes(data: str) -> bytes:
    if not data:
        return b""
    padded = data + ("=" * (-len(data) % 4))
    try:
        return base64.urlsafe_b64decode(padded.encode("ascii"))
    except (ValueError, binascii.Error):
        return b""


def _message_attachment_parts(message: dict[str, Any]) -> list[dict[str, Any]]:
    attachments: list[dict[str, Any]] = []
    for part in _walk_gmail_parts(message.get("payload")):
        body = part.get("body", {}) or {}
        filename = str(part.get("filename", "")).strip()
        attachment_id = str(body.get("attachmentId", "")).strip()
        inline_data = str(body.get("data", "")).strip()
        if not filename and not attachment_id and not inline_data:
            continue
        headers = {
            str(header.get("name", "")).lower(): str(header.get("value", ""))
            for header in part.get("headers", []) or []
        }
        disposition = headers.get("content-disposition", "")
        attachments.append(
            {
                "filename": filename,
                "mime_type": str(part.get("mimeType", "")).lower(),
                "size": int(body.get("size", 0) or 0),
                "attachment_id": attachment_id,
                "part_id": str(part.get("partId", "")),
                "disposition": disposition,
                "inline": disposition.lower().startswith("inline"),
                "data": inline_data,
            }
        )
    return attachments


def _attachment_overlap_score(stored_names: set[str], live_names: set[str]) -> int:
    if not stored_names or not live_names:
        return 0
    return len(stored_names & live_names) * 4


def _snippet_match_score(target_snippet: str, live_snippet: str) -> int:
    if not target_snippet or not live_snippet:
        return 0
    short_target = target_snippet[:100]
    short_live = live_snippet[:100]
    if short_target == short_live:
        return 6
    if short_target in short_live or short_live in short_target:
        return 4
    return 0


def _time_match_score(target_happened_at: Optional[str], message_happened_at) -> int:
    if not target_happened_at:
        return 0
    target = store.parse_datetime(str(target_happened_at))
    delta_seconds = abs((message_happened_at - target).total_seconds())
    if delta_seconds <= 5 * 60:
        return 6
    if delta_seconds <= 60 * 60:
        return 3
    if delta_seconds <= 24 * 60 * 60:
        return 1
    return 0


def resolve_gmail_message_for_communication(service, communication_row) -> Optional[dict[str, Any]]:
    thread_id = str(communication_row["external_thread_id"] or "")
    if not thread_id:
        external_id = str(communication_row["external_id"] or "")
        if external_id.startswith("thread:"):
            thread_id = external_id.partition(":")[2]
    if not thread_id:
        return None

    thread = service.users().threads().get(userId="me", id=thread_id, format="full").execute()
    messages = thread.get("messages", []) or []
    target_subject = str(communication_row["subject"] or "").strip()
    target_snippet = str(communication_row["snippet"] or "").strip()
    stored_attachments = json.loads(str(communication_row["attachments_json"] or "[]"))
    stored_names = {
        str(attachment.get("filename", "")).strip().lower()
        for attachment in stored_attachments
        if attachment.get("filename")
    }

    best_message: Optional[dict[str, Any]] = None
    best_rank: tuple[int, int, str] = (-1, -1, "")

    for message in messages:
        headers = _gmail_header_map(message)
        message_subject = str(headers.get("Subject", "")).strip()
        message_happened_at = _gmail_message_time(message, headers)
        attachment_names = {
            str(attachment["filename"]).strip().lower()
            for attachment in _message_attachment_parts(message)
            if attachment.get("filename")
        }

        score = 0
        if target_subject and message_subject == target_subject:
            score += 10
        elif target_subject and message_subject.lower() == target_subject.lower():
            score += 8
        elif target_subject and target_subject.lower() in message_subject.lower():
            score += 4

        score += _attachment_overlap_score(stored_names, attachment_names)
        score += _snippet_match_score(target_snippet, str(message.get("snippet", "")))
        score += _time_match_score(communication_row["happened_at"], message_happened_at)
        if attachment_names:
            score += 1

        rank = (score, len(attachment_names), str(message.get("id", "")))
        if rank > best_rank:
            best_rank = rank
            best_message = message

    return best_message


def _attachment_bytes(service, *, message_id: str, attachment: dict[str, Any]) -> bytes:
    inline_data = str(attachment.get("data", ""))
    if inline_data:
        return _decode_gmail_bytes(inline_data)

    attachment_id = str(attachment.get("attachment_id", ""))
    if not attachment_id:
        return b""

    response = service.users().messages().attachments().get(
        userId="me",
        messageId=message_id,
        id=attachment_id,
    ).execute()
    return _decode_gmail_bytes(str(response.get("data", "")))


def _read_text_file(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def _decode_text_bytes(raw_bytes: bytes, *, mime_type: str) -> str:
    decoded = raw_bytes.decode("utf-8", errors="ignore")
    if "html" in mime_type:
        return _html_to_text(decoded)
    return decoded


def _run_capture(command: list[str]) -> str:
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"command failed: {' '.join(command)}")
    return result.stdout


def _read_binary_header(path: Path, length: int = 32) -> str:
    raw = path.read_bytes()[:length]
    return raw.hex()


def _format_size(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f} KiB"
    return f"{size / (1024 * 1024):.2f} MiB"


def _summarize_zip_archive(path: Path) -> str:
    with zipfile.ZipFile(path) as archive:
        infos = archive.infolist()
        names = [info.filename for info in infos[:12]]
        total_uncompressed = sum(int(info.file_size or 0) for info in infos)
    lines = [
        "ZIP archive",
        f"entries: {len(infos)}",
        f"uncompressed_size: {_format_size(total_uncompressed)}",
    ]
    if names:
        lines.append("sample_entries:")
        lines.extend(f"- {name}" for name in names)
    return "\n".join(lines)


def _summarize_tar_archive(path: Path) -> str:
    mode = "r:*"
    with tarfile.open(path, mode) as archive:
        members = archive.getmembers()
        names = [member.name for member in members[:12]]
        total_uncompressed = sum(int(getattr(member, "size", 0) or 0) for member in members)
    lines = [
        "TAR archive",
        f"entries: {len(members)}",
        f"uncompressed_size: {_format_size(total_uncompressed)}",
    ]
    if names:
        lines.append("sample_entries:")
        lines.extend(f"- {name}" for name in names)
    return "\n".join(lines)


def _summarize_code_file(path: Path) -> str:
    content = _read_text_file(path)
    lines = content.splitlines()
    nonempty = [line for line in lines if line.strip()]
    preview_lines = nonempty[:12]
    language_hint = path.suffix.lower().lstrip(".") or "text"
    shebang = lines[0].strip() if lines and lines[0].startswith("#!") else ""
    summary = [
        f"Code file ({language_hint})",
        f"line_count: {len(lines)}",
        f"nonempty_lines: {len(nonempty)}",
    ]
    if shebang:
        summary.append(f"shebang: {shebang}")
    if preview_lines:
        summary.append("preview:")
        summary.extend(f"{index + 1}: {line}" for index, line in enumerate(preview_lines))
    return "\n".join(summary)


def _summarize_executable(path: Path, *, mime_type: str) -> str:
    raw = path.read_bytes()[:2]
    format_hint = "unknown"
    if raw.startswith(b"MZ"):
        format_hint = "PE/Windows executable"
    elif raw.startswith(b"\x7fE"):
        format_hint = "ELF binary"
    elif path.suffix.lower() == ".dmg":
        format_hint = "disk image"
    elif path.suffix.lower() == ".pkg":
        format_hint = "installer package"
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return "\n".join(
        [
            f"Executable or binary package ({format_hint})",
            f"mime_type: {mime_type or 'application/octet-stream'}",
            f"size: {_format_size(path.stat().st_size)}",
            f"sha256: {digest}",
            f"magic_header_hex: {_read_binary_header(path, 16)}",
        ]
    )


def _summarize_media_file(path: Path, *, mime_type: str) -> str:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return "\n".join(
        [
            f"Media file ({mime_type or 'application/octet-stream'})",
            f"size: {_format_size(path.stat().st_size)}",
            f"sha256: {digest}",
            f"magic_header_hex: {_read_binary_header(path, 16)}",
        ]
    )


def _summarize_binary_file(path: Path, *, mime_type: str) -> str:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return "\n".join(
        [
            f"Binary attachment ({mime_type or 'application/octet-stream'})",
            f"size: {_format_size(path.stat().st_size)}",
            f"sha256: {digest}",
            f"magic_header_hex: {_read_binary_header(path, 16)}",
        ]
    )


def extract_text_from_saved_attachment(path: Path, *, mime_type: str) -> tuple[str, str]:
    suffix = path.suffix.lower()
    effective_mime = mime_type or mimetypes.guess_type(path.name)[0] or ""

    if suffix in CODE_SUFFIXES:
        return _summarize_code_file(path), "code_summary"
    if effective_mime.startswith("text/") or suffix in TEXT_SUFFIXES:
        return _read_text_file(path), "text_decode"
    if "html" in effective_mime or suffix in {".htm", ".html"}:
        return _html_to_text(_read_text_file(path)), "html_text"
    if effective_mime == "application/pdf" or suffix == ".pdf":
        return _run_capture(["pdftotext", str(path), "-"]), "pdf_text"
    if suffix == ".zip" or (suffix in ARCHIVE_SUFFIXES and zipfile.is_zipfile(path)):
        return _summarize_zip_archive(path), "archive_summary"
    if suffix in {".tar", ".tgz", ".gz", ".bz2", ".xz"}:
        try:
            if tarfile.is_tarfile(path):
                return _summarize_tar_archive(path), "archive_summary"
        except tarfile.TarError:
            pass
    if effective_mime.startswith("image/") or suffix in IMAGE_SUFFIXES:
        return _run_capture(["tesseract", str(path), "stdout", "--psm", "6"]), "ocr_image"
    if effective_mime.startswith("audio/") or effective_mime.startswith("video/") or suffix in MEDIA_SUFFIXES:
        return _summarize_media_file(path, mime_type=effective_mime), "media_summary"
    if suffix in EXECUTABLE_SUFFIXES:
        return _summarize_executable(path, mime_type=effective_mime), "binary_summary"
    if suffix in TEXTUTIL_SUFFIXES:
        return _run_capture(["textutil", "-convert", "txt", "-stdout", str(path)]), "textutil"
    if effective_mime.startswith("application/"):
        return _summarize_binary_file(path, mime_type=effective_mime), "binary_summary"
    return "", ""


def _truncate_text(value: str, limit: int) -> str:
    collapsed = value.strip()
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: max(0, limit - 3)].rstrip() + "..."


def _attachment_external_key(
    *,
    communication_id: int,
    message_id: str,
    attachment_id: str,
    part_id: str,
    filename: str,
) -> str:
    fingerprint = hashlib.sha1(
        f"{communication_id}:{message_id}:{attachment_id}:{part_id}:{filename}".encode("utf-8")
    ).hexdigest()[:16]
    return f"attachment:{fingerprint}"


def _attachment_sync_state_key(namespace: str, suffix: str) -> str:
    return f"{namespace}:{suffix}"


def _optional_int_sync_state(connection, key: str) -> Optional[int]:
    raw_value = store.get_sync_state(connection, key)
    if raw_value in {None, ""}:
        return None
    try:
        return int(str(raw_value))
    except ValueError:
        return None


def _list_sensitive_attachment_communications_page(
    connection,
    *,
    limit: int,
    before_happened_at: Optional[str] = None,
    before_communication_id: Optional[int] = None,
) -> list[Any]:
    category_values = sorted(SENSITIVE_ATTACHMENT_CATEGORIES)
    category_placeholders = ", ".join("?" for _ in category_values)
    category_like_clauses = " OR ".join("communications.categories_json LIKE ?" for _ in category_values)

    clauses = [
        "communications.source = 'gmail'",
        "communications.attachments_json != '[]'",
        (
            f"(communications.category IN ({category_placeholders})"
            + (f" OR {category_like_clauses}" if category_like_clauses else "")
            + ")"
        ),
    ]
    params: list[Any] = [*category_values, *(f'%"{value}"%' for value in category_values)]

    if before_happened_at is not None and before_communication_id is not None:
        clauses.append(
            "(communications.happened_at < ? OR (communications.happened_at = ? AND communications.id < ?))"
        )
        params.extend([before_happened_at, before_happened_at, before_communication_id])
    elif before_happened_at is not None:
        clauses.append("communications.happened_at < ?")
        params.append(before_happened_at)

    params.append(limit)
    return connection.execute(
        f"""
        SELECT communications.*, organizations.name AS organization_name
        FROM communications
        LEFT JOIN organizations ON organizations.id = communications.organization_id
        WHERE {' AND '.join(clauses)}
        ORDER BY communications.happened_at DESC, communications.id DESC
        LIMIT ?
        """,
        params,
    ).fetchall()


def _is_sensitive_communication(row) -> bool:
    categories = set(json.loads(str(row["categories_json"] or "[]")))
    primary_category = str(row["category"] or "")
    if primary_category:
        categories.add(primary_category)
    return bool(categories & SENSITIVE_ATTACHMENT_CATEGORIES)


def _list_sensitive_attachment_communications(connection, *, limit: Optional[int]) -> list[Any]:
    rows = connection.execute(
        """
        SELECT communications.*, organizations.name AS organization_name
        FROM communications
        LEFT JOIN organizations ON organizations.id = communications.organization_id
        WHERE communications.source = 'gmail'
          AND communications.attachments_json != '[]'
        ORDER BY communications.happened_at DESC, communications.id DESC
        """
    ).fetchall()
    filtered = [row for row in rows if _is_sensitive_communication(row)]
    if limit is None:
        return filtered
    return filtered[:limit]


def _should_skip_attachment(attachment: dict[str, Any]) -> bool:
    filename = str(attachment.get("filename", "")).strip()
    filename_lower = filename.lower()
    mime_type = str(attachment.get("mime_type", "")).lower()
    size = int(attachment.get("size", 0) or 0)

    if not filename and mime_type in {"text/html", "text/plain"}:
        return True

    if store.attachment_filename_is_low_signal(filename):
        return True

    if any(fragment in filename_lower for fragment in DECORATIVE_FILENAME_FRAGMENTS) and size < 25000:
        return True

    suffix = Path(filename).suffix.lower()
    if (mime_type.startswith("image/") or mime_type == "application/octet-stream" or suffix in IMAGE_SUFFIXES) and (
        size < 2048
        and not any(hint in filename_lower for hint in DOCUMENT_FILENAME_HINTS)
    ):
        return True

    return False


def _ingest_attachment_rows(
    connection,
    *,
    service,
    communication_rows: list[Any],
    include_inline: bool,
    force: bool,
    vault_root: Path,
    trace_type: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    trace_run_id = tracing.start_trace_run(
        connection,
        trace_type=trace_type,
        metadata=metadata,
    )
    try:
        attachments_seen = 0
        attachments_saved = 0
        attachments_extracted = 0
        attachments_failed = 0
        attachments_skipped_existing = 0
        attachments_skipped_filtered = 0
        by_method: dict[str, int] = {}

        for communication_row in communication_rows:
            message = resolve_gmail_message_for_communication(service, communication_row)
            if message is None:
                tracing.append_trace_event(
                    connection,
                    run_id=trace_run_id,
                    event_type="attachment_message_not_found",
                    entity_key=str(communication_row["external_id"] or communication_row["id"]),
                    payload={
                        "communication_id": int(communication_row["id"]),
                        "subject": str(communication_row["subject"]),
                    },
                )
                continue

            message_id = str(message.get("id", ""))
            existing_records = store.list_communication_attachments(
                connection,
                communication_id=int(communication_row["id"]),
                source="gmail",
                limit=None,
            )
            existing_by_key = {str(row["external_key"]): row for row in existing_records}
            for index, attachment in enumerate(_message_attachment_parts(message), start=1):
                attachments_seen += 1
                if not include_inline and bool(attachment.get("inline")):
                    attachments_skipped_filtered += 1
                    continue
                if _should_skip_attachment(attachment):
                    attachments_skipped_filtered += 1
                    continue

                filename = str(attachment.get("filename", "")).strip() or f"attachment-{index}"
                external_key = _attachment_external_key(
                    communication_id=int(communication_row["id"]),
                    message_id=message_id,
                    attachment_id=str(attachment.get("attachment_id", "")),
                    part_id=str(attachment.get("part_id", "")),
                    filename=filename,
                )
                existing = existing_by_key.get(external_key)
                if existing and not force and str(existing["ingest_status"]) in {"downloaded", "extracted"}:
                    attachments_skipped_existing += 1
                    continue

                safe_name = _safe_filename(filename, f"attachment-{index}")
                relative_dir = Path("gmail") / f"communication-{int(communication_row['id'])}" / f"message-{message_id}"
                absolute_dir = vault_root / relative_dir
                absolute_dir.mkdir(parents=True, exist_ok=True)
                file_path = absolute_dir / safe_name
                text_path = absolute_dir / f"{safe_name}.txt"

                try:
                    raw_bytes = _attachment_bytes(service, message_id=message_id, attachment=attachment)
                    if not raw_bytes:
                        raise RuntimeError("attachment payload was empty")

                    file_path.write_bytes(raw_bytes)
                    sha256 = hashlib.sha256(raw_bytes).hexdigest()

                    extracted_text = ""
                    extraction_method = ""
                    effective_mime = str(attachment.get("mime_type", ""))
                    if effective_mime.startswith("text/") or file_path.suffix.lower() in TEXT_SUFFIXES | {".htm", ".html"}:
                        extracted_text = _decode_text_bytes(raw_bytes, mime_type=effective_mime)
                        extraction_method = "html_text" if "html" in effective_mime else "text_decode"
                    else:
                        extracted_text, extraction_method = extract_text_from_saved_attachment(
                            file_path,
                            mime_type=effective_mime,
                        )

                    normalized_text = _truncate_text(extracted_text, ATTACHMENT_TEXT_LIMIT)
                    if normalized_text:
                        text_path.write_text(normalized_text)
                        attachments_extracted += 1
                        by_method[extraction_method or "unknown"] = by_method.get(extraction_method or "unknown", 0) + 1
                        ingest_status = "extracted"
                        extracted_text_path = str((relative_dir / text_path.name).as_posix())
                    else:
                        ingest_status = "downloaded"
                        extracted_text_path = ""

                    store.upsert_communication_attachment(
                        connection,
                        external_key=external_key,
                        communication_id=int(communication_row["id"]),
                        source="gmail",
                        external_message_id=message_id,
                        external_attachment_id=str(attachment.get("attachment_id", "")),
                        part_id=str(attachment.get("part_id", "")),
                        filename=filename,
                        mime_type=effective_mime,
                        size=int(attachment.get("size", 0) or 0),
                        relative_path=str((relative_dir / file_path.name).as_posix()),
                        extracted_text=normalized_text,
                        extracted_text_path=extracted_text_path,
                        extraction_method=extraction_method,
                        ingest_status=ingest_status,
                        error_text="",
                        sha256=sha256,
                    )
                    attachments_saved += 1
                    tracing.append_trace_event(
                        connection,
                        run_id=trace_run_id,
                        event_type="attachment_ingested",
                        entity_key=external_key,
                        payload={
                            "communication_id": int(communication_row["id"]),
                            "filename": filename,
                            "mime_type": effective_mime,
                            "ingest_status": ingest_status,
                            "extraction_method": extraction_method,
                            "text_preview": _truncate_text(normalized_text, ATTACHMENT_PREVIEW_LIMIT),
                        },
                    )
                except Exception as exc:
                    attachments_failed += 1
                    store.upsert_communication_attachment(
                        connection,
                        external_key=external_key,
                        communication_id=int(communication_row["id"]),
                        source="gmail",
                        external_message_id=message_id,
                        external_attachment_id=str(attachment.get("attachment_id", "")),
                        part_id=str(attachment.get("part_id", "")),
                        filename=filename,
                        mime_type=str(attachment.get("mime_type", "")),
                        size=int(attachment.get("size", 0) or 0),
                        relative_path=str((relative_dir / safe_name).as_posix()),
                        extracted_text="",
                        extracted_text_path="",
                        extraction_method="",
                        ingest_status="failed",
                        error_text=str(exc),
                        sha256="",
                    )
                    tracing.append_trace_event(
                        connection,
                        run_id=trace_run_id,
                        event_type="attachment_ingest_failed",
                        entity_key=external_key,
                        payload={
                            "communication_id": int(communication_row["id"]),
                            "filename": filename,
                            "error": str(exc),
                        },
                    )

        summary = {
            "communications_scanned": len(communication_rows),
            "attachments_seen": attachments_seen,
            "attachments_saved": attachments_saved,
            "attachments_extracted": attachments_extracted,
            "attachments_failed": attachments_failed,
            "attachments_skipped_existing": attachments_skipped_existing,
            "attachments_skipped_filtered": attachments_skipped_filtered,
            "by_method": by_method,
            "vault_root": str(vault_root),
        }
        tracing.finish_trace_run(connection, run_id=trace_run_id, status="completed", summary=summary)
        return summary
    except Exception as exc:
        tracing.finish_trace_run(
            connection,
            run_id=trace_run_id,
            status="failed",
            summary={"error": str(exc)},
        )
        raise


def ingest_profile_attachments(
    connection,
    *,
    credentials_path: Optional[Path] = None,
    token_path: Optional[Path] = None,
    subject_key: Optional[str] = None,
    item_type: Optional[str] = None,
    status: str = "candidate",
    limit: Optional[int] = 50,
    include_inline: bool = False,
    force: bool = False,
    scope: str = "profile",
    service=None,
    vault_root: Optional[Path] = None,
) -> dict[str, Any]:
    if service is None:
        if credentials_path is None or token_path is None:
            raise ValueError("credentials_path and token_path are required when service is not provided")
        service, _ = _gmail_service(credentials_path, token_path)

    vault_root = vault_root or store.attachment_vault_root()
    vault_root.mkdir(parents=True, exist_ok=True)

    if scope == "profile":
        communication_rows = store.list_profile_context_communications(
            connection,
            subject_key=subject_key,
            item_type=item_type,
            status=status,
            source="gmail",
            limit=limit,
        )
    elif scope == "sensitive":
        communication_rows = _list_sensitive_attachment_communications(connection, limit=limit)
    else:
        raise ValueError(f"unsupported ingest scope: {scope}")

    return _ingest_attachment_rows(
        connection,
        service=service,
        communication_rows=communication_rows,
        include_inline=include_inline,
        force=force,
        vault_root=vault_root,
        trace_type="profile_attachment_ingest",
        metadata={
            "subject_key": subject_key,
            "item_type": item_type,
            "status": status,
            "limit": limit,
            "include_inline": include_inline,
            "force": force,
            "scope": scope,
        },
    )


def backfill_profile_attachments(
    connection,
    *,
    credentials_path: Optional[Path] = None,
    token_path: Optional[Path] = None,
    scope: str = DEFAULT_PROFILE_ATTACHMENT_BACKFILL_SCOPE,
    max_results: int = 100,
    reset_cursor: bool = False,
    cursor_namespace: str = DEFAULT_PROFILE_ATTACHMENT_BACKFILL_NAMESPACE,
    include_inline: bool = False,
    force: bool = False,
    service=None,
    vault_root: Optional[Path] = None,
) -> dict[str, Any]:
    if scope != "sensitive":
        raise ValueError("profile attachment backfill currently supports only scope='sensitive'")

    if service is None:
        if credentials_path is None or token_path is None:
            raise ValueError("credentials_path and token_path are required when service is not provided")
        service, _ = _gmail_service(credentials_path, token_path)

    vault_root = vault_root or store.attachment_vault_root()
    vault_root.mkdir(parents=True, exist_ok=True)

    stored_scope = store.get_sync_state(connection, _attachment_sync_state_key(cursor_namespace, "scope"))
    stored_happened_at = None if reset_cursor else store.get_sync_state(
        connection,
        _attachment_sync_state_key(cursor_namespace, "next_happened_at"),
    )
    stored_communication_id = None if reset_cursor else _optional_int_sync_state(
        connection,
        _attachment_sync_state_key(cursor_namespace, "next_communication_id"),
    )
    cursor_reused = (
        not reset_cursor
        and stored_scope == scope
        and stored_happened_at is not None
        and stored_communication_id is not None
    )

    fetched_rows = _list_sensitive_attachment_communications_page(
        connection,
        limit=max_results + 1,
        before_happened_at=stored_happened_at if cursor_reused else None,
        before_communication_id=stored_communication_id if cursor_reused else None,
    )
    communication_rows = fetched_rows[:max_results]
    store.set_sync_state(connection, _attachment_sync_state_key(cursor_namespace, "scope"), scope)
    ingest_summary = _ingest_attachment_rows(
        connection,
        service=service,
        communication_rows=communication_rows,
        include_inline=include_inline,
        force=force,
        vault_root=vault_root,
        trace_type="profile_attachment_backfill",
        metadata={
            "scope": scope,
            "max_results": max_results,
            "reset_cursor": reset_cursor,
            "cursor_namespace": cursor_namespace,
            "cursor_reused": cursor_reused,
            "include_inline": include_inline,
            "force": force,
        },
    )

    if communication_rows and len(fetched_rows) > max_results:
        oldest_row = communication_rows[-1]
        next_happened_at = str(oldest_row["happened_at"] or "")
        next_communication_id = int(oldest_row["id"])
        backfill_exhausted = False
    else:
        next_happened_at = None
        next_communication_id = None
        backfill_exhausted = True

    store.set_sync_state(
        connection,
        _attachment_sync_state_key(cursor_namespace, "last_sync_at"),
        datetime.now().isoformat(timespec="minutes"),
    )
    store.set_sync_state(
        connection,
        _attachment_sync_state_key(cursor_namespace, "next_happened_at"),
        next_happened_at or "",
    )
    store.set_sync_state(
        connection,
        _attachment_sync_state_key(cursor_namespace, "next_communication_id"),
        str(next_communication_id or ""),
    )

    return {
        **ingest_summary,
        "scope": scope,
        "max_results": max_results,
        "cursor_namespace": cursor_namespace,
        "cursor_reused": cursor_reused,
        "next_happened_at": next_happened_at,
        "next_communication_id": next_communication_id,
        "backfill_exhausted": backfill_exhausted,
    }


def backfill_profile_attachments_until_exhausted(
    connection,
    *,
    credentials_path: Optional[Path] = None,
    token_path: Optional[Path] = None,
    scope: str = DEFAULT_PROFILE_ATTACHMENT_BACKFILL_SCOPE,
    max_results: int = 100,
    reset_cursor: bool = False,
    cursor_namespace: str = DEFAULT_PROFILE_ATTACHMENT_BACKFILL_NAMESPACE,
    include_inline: bool = False,
    force: bool = False,
    service=None,
    vault_root: Optional[Path] = None,
    max_runs: Optional[int] = None,
) -> dict[str, Any]:
    if service is None:
        if credentials_path is None or token_path is None:
            raise ValueError("credentials_path and token_path are required when service is not provided")
        service, _ = _gmail_service(credentials_path, token_path)

    tracing.cancel_running_trace_runs(
        connection,
        trace_types=["profile_attachment_backfill", "profile_attachment_backfill_exhaustive"],
        summary={"error": "cancelled stale profile attachment run before restart"},
    )

    trace_run_id = tracing.start_trace_run(
        connection,
        trace_type="profile_attachment_backfill_exhaustive",
        metadata={
            "scope": scope,
            "max_results": max_results,
            "reset_cursor": reset_cursor,
            "cursor_namespace": cursor_namespace,
            "include_inline": include_inline,
            "force": force,
            "max_runs": max_runs,
        },
    )
    try:
        runs: list[dict[str, Any]] = []
        previous_cursor: tuple[Optional[str], Optional[int]] = (None, None)
        stop_reason = "backfill_exhausted"

        while True:
            run_summary = backfill_profile_attachments(
                connection,
                credentials_path=credentials_path,
                token_path=token_path,
                scope=scope,
                max_results=max_results,
                reset_cursor=reset_cursor and not runs,
                cursor_namespace=cursor_namespace,
                include_inline=include_inline,
                force=force,
                service=service,
                vault_root=vault_root,
            )
            runs.append(run_summary)
            tracing.append_trace_event(
                connection,
                run_id=trace_run_id,
                event_type="profile_attachment_backfill_run_complete",
                entity_key=str(len(runs)),
                payload=run_summary,
            )

            current_cursor = (
                run_summary.get("next_happened_at"),
                run_summary.get("next_communication_id"),
            )
            if run_summary.get("backfill_exhausted"):
                stop_reason = "backfill_exhausted"
                break
            if current_cursor[0] is None or current_cursor[1] is None:
                stop_reason = "cursor_missing"
                break
            if current_cursor == previous_cursor:
                stop_reason = "cursor_stalled"
                break
            if max_runs is not None and len(runs) >= max_runs:
                stop_reason = "max_runs_reached"
                break

            previous_cursor = current_cursor

        aggregate_methods: dict[str, int] = {}
        for run in runs:
            for method, count in run.get("by_method", {}).items():
                aggregate_methods[method] = aggregate_methods.get(method, 0) + int(count)

        summary = {
            "scope": scope,
            "max_results": max_results,
            "cursor_namespace": cursor_namespace,
            "runs_completed": len(runs),
            "communications_scanned": sum(int(run["communications_scanned"]) for run in runs),
            "attachments_seen": sum(int(run["attachments_seen"]) for run in runs),
            "attachments_saved": sum(int(run["attachments_saved"]) for run in runs),
            "attachments_extracted": sum(int(run["attachments_extracted"]) for run in runs),
            "attachments_failed": sum(int(run["attachments_failed"]) for run in runs),
            "attachments_skipped_existing": sum(int(run["attachments_skipped_existing"]) for run in runs),
            "attachments_skipped_filtered": sum(int(run["attachments_skipped_filtered"]) for run in runs),
            "backfill_exhausted": bool(runs and runs[-1].get("backfill_exhausted")),
            "stop_reason": stop_reason,
            "last_next_happened_at": runs[-1].get("next_happened_at") if runs else None,
            "last_next_communication_id": runs[-1].get("next_communication_id") if runs else None,
            "by_method": aggregate_methods,
            "vault_root": str(vault_root or store.attachment_vault_root()),
        }
        tracing.finish_trace_run(connection, run_id=trace_run_id, status="completed", summary=summary)
        return summary
    except Exception as exc:
        tracing.finish_trace_run(
            connection,
            run_id=trace_run_id,
            status="failed",
            summary={"error": str(exc)},
        )
        raise
