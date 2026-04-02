from __future__ import annotations

import base64
import binascii
import json
import re
from datetime import date, datetime, time, timedelta
from email.utils import parseaddr, parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Optional

from life_ops import classification
from life_ops import store
from life_ops import tracing

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/gmail.readonly",
]

DEFAULT_GMAIL_QUERY = "in:inbox is:unread newer_than:7d -category:promotions -category:social"
DEFAULT_GMAIL_BACKFILL_QUERY = "-in:chats"
DEFAULT_GMAIL_BACKFILL_CUTOFF_DAYS = 7
DEFAULT_GMAIL_CATEGORY_SWEEPS = [
    {"name": "identity_docs", "query": '-in:chats passport'},
    {"name": "tax_admin", "query": '-in:chats tax'},
    {"name": "benefits_support", "query": '-in:chats benefits'},
    {"name": "medical_support", "query": '-in:chats medical'},
    {"name": "insurance_policies", "query": '-in:chats insurance'},
    {"name": "finance_bills", "query": '-in:chats invoice'},
    {"name": "creative_archive", "query": '-in:chats draft'},
    {"name": "career_search", "query": '-in:chats recruiter'},
    {"name": "logistics_travel", "query": '-in:chats tracking'},
    {"name": "pets_care", "query": '-in:chats habanero'},
]
ACTIONABLE_LABEL_IDS = {"IMPORTANT", "STARRED"}
IGNORE_LABEL_IDS = {"SPAM", "TRASH", "CATEGORY_PROMOTIONS", "CATEGORY_SOCIAL"}
STRONG_ACTION_SUBJECT_KEYWORDS = {
    "issue",
    "pull request",
    "pr #",
    "comment",
    "review",
    "reply",
    "mentioned",
    "mention",
    "invoice",
    "expires",
    "expiring",
    "token",
    "interview",
    "meeting",
    "schedule",
    "confirm",
    "minimum payment due",
}
WEAK_ACTION_SUBJECT_KEYWORDS = {
    "deadline",
    "follow up",
    "follow-up",
    "reminder",
    "approval",
    "approve",
    "request",
    "action required",
}
NON_ACTIONABLE_SUBJECT_KEYWORDS = {
    "newsletter",
    "welcome",
    "achievement",
    "30% off",
    "luxe for less",
    "top post",
    "community of neighbors",
    "hiring for",
    "job alert",
    "digest",
    "run failed:",
    "continuous integration",
    "payment processed",
    "payment confirmation",
    "receipt from",
    "receipt…",
    "received your payment",
    "just scheduled",
    "security factor changed",
}
ACTIONABLE_SENDER_DOMAINS = {
    "github.com",
    "mg.gitlab.com",
    "gitlab.com",
    "meetup.com",
}
NON_ACTIONABLE_SENDER_DOMAINS = {
    "glassdoor.com",
    "is.email.nextdoor.com",
    "member.alibaba.com",
    "updates.resend.com",
}
AUTOMATED_LOCALPART_FRAGMENTS = {
    "noreply",
    "no-reply",
    "do-not-reply",
    "donotreply",
}
BODY_TEXT_LIMIT = 6000
BODY_PREVIEW_LIMIT = 280


class _HTMLTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:  # pragma: no cover - parser callback
        if tag in {"br", "div", "li", "p", "tr"}:
            self._parts.append("\n")

    def handle_data(self, data: str) -> None:  # pragma: no cover - parser callback
        if data:
            self._parts.append(data)

    def text(self) -> str:
        return "".join(self._parts)


def default_credentials_path() -> Path:
    return store.config_root() / "google_credentials.json"


def default_token_path() -> Path:
    return store.data_root() / "google_token.json"


def _import_google_clients():
    try:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from googleapiclient.discovery import build
    except ImportError as exc:
        raise RuntimeError(
            "Google sync dependencies are not installed. Create a venv and run `pip install -e .`."
        ) from exc

    return Request, Credentials, InstalledAppFlow, build


def get_google_credentials(credentials_path: Path, token_path: Path):
    Request, Credentials, InstalledAppFlow, _ = _import_google_clients()

    credentials = None
    if token_path.exists():
        credentials = Credentials.from_authorized_user_file(str(token_path), GOOGLE_SCOPES)

    if credentials and credentials.valid:
        return credentials

    if credentials and credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())
    else:
        if not credentials_path.exists():
            raise FileNotFoundError(
                f"Google OAuth credentials not found at {credentials_path}. "
                "Download the Desktop app credentials JSON from Google Cloud and place it there."
            )
        flow = InstalledAppFlow.from_client_secrets_file(str(credentials_path), GOOGLE_SCOPES)
        credentials = flow.run_local_server(port=0)

    token_path.parent.mkdir(parents=True, exist_ok=True)
    token_path.write_text(credentials.to_json())
    return credentials


def ensure_google_auth(credentials_path: Path, token_path: Path) -> dict:
    get_google_credentials(credentials_path, token_path)
    return {
        "credentials_path": str(credentials_path),
        "token_path": str(token_path),
        "scopes": GOOGLE_SCOPES,
    }


def list_google_calendars(credentials_path: Path, token_path: Path) -> list[dict]:
    _, _, _, build = _import_google_clients()
    credentials = get_google_credentials(credentials_path, token_path)
    service = build("calendar", "v3", credentials=credentials)
    return _list_google_calendars_from_service(service)


def _list_google_calendars_from_service(service) -> list[dict]:
    calendars: list[dict] = []
    request = service.calendarList().list()
    while request is not None:
        response = request.execute()
        for item in response.get("items", []):
            calendars.append(
                {
                    "id": item.get("id", ""),
                    "summary": item.get("summary", ""),
                    "primary": bool(item.get("primary")),
                    "selected": bool(item.get("selected")),
                    "access_role": item.get("accessRole", ""),
                }
            )
        request = service.calendarList().list_next(request, response)

    return calendars


def _canonicalize_calendar_ids(calendar_ids: list[str], primary_calendar_id: Optional[str]) -> tuple[list[str], list[str]]:
    canonical_ids: list[str] = []
    aliases: list[str] = []
    seen: set[str] = set()

    for calendar_id in calendar_ids:
        canonical_id = primary_calendar_id if calendar_id == "primary" and primary_calendar_id else calendar_id
        if canonical_id != calendar_id:
            aliases.append(calendar_id)
        if canonical_id in seen:
            continue
        seen.add(canonical_id)
        canonical_ids.append(canonical_id)

    return canonical_ids, aliases


def _calendar_event_times(event: dict) -> tuple[datetime, datetime, bool]:
    start_block = event.get("start", {})
    end_block = event.get("end", {})

    if "dateTime" in start_block:
        start_at = store.parse_datetime(start_block["dateTime"])
        end_at = store.parse_datetime(end_block.get("dateTime", start_block["dateTime"]))
        return start_at, end_at, False

    start_day = date.fromisoformat(start_block["date"])
    end_day_exclusive = date.fromisoformat(end_block.get("date", start_block["date"]))
    if end_day_exclusive <= start_day:
        end_day = start_day
    else:
        end_day = end_day_exclusive - timedelta(days=1)

    return (
        datetime.combine(start_day, time(0, 0)),
        datetime.combine(end_day, time(23, 59)),
        True,
    )


def sync_google_calendar(
    connection,
    *,
    credentials_path: Path,
    token_path: Path,
    calendar_ids: Optional[list[str]] = None,
    days_back: int = 7,
    days_ahead: int = 30,
) -> dict:
    trace_run_id = tracing.start_trace_run(
        connection,
        trace_type="google_calendar_sync",
        metadata={
            "requested_calendars": calendar_ids or ["primary"],
            "days_back": days_back,
            "days_ahead": days_ahead,
        },
    )
    try:
        _, _, _, build = _import_google_clients()
        credentials = get_google_credentials(credentials_path, token_path)
        service = build("calendar", "v3", credentials=credentials)

        now = datetime.now().astimezone()
        time_min = (now - timedelta(days=days_back)).isoformat()
        time_max = (now + timedelta(days=days_ahead)).isoformat()
        discovered_calendars = _list_google_calendars_from_service(service)
        primary_calendar_id = next(
            (calendar["id"] for calendar in discovered_calendars if calendar["primary"]),
            None,
        )
        requested_calendars = calendar_ids or ["primary"]
        target_calendars, aliases = _canonicalize_calendar_ids(requested_calendars, primary_calendar_id)

        for alias in aliases:
            deleted = store.delete_events_for_calendar(
                connection,
                source="google-calendar",
                external_calendar_id=alias,
            )
            tracing.append_trace_event(
                connection,
                run_id=trace_run_id,
                event_type="calendar_alias_cleanup",
                entity_key=alias,
                payload={"deleted_rows": deleted},
            )

        processed = 0
        per_calendar_counts: dict[str, int] = {}
        for calendar_id in target_calendars:
            request = service.events().list(
                calendarId=calendar_id,
                timeMin=time_min,
                timeMax=time_max,
                singleEvents=True,
                orderBy="startTime",
                maxResults=2500,
            )

            calendar_processed = 0
            while request is not None:
                response = request.execute()
                for item in response.get("items", []):
                    start_at, end_at, all_day = _calendar_event_times(item)
                    organizer = item.get("organizer", {})
                    organization_name = organizer.get("displayName") or organizer.get("email")

                    store.upsert_event_from_sync(
                        connection,
                        source="google-calendar",
                        external_id=f"{calendar_id}:{item['id']}",
                        title=item.get("summary") or "(untitled event)",
                        start_at=start_at,
                        end_at=end_at,
                        all_day=all_day,
                        organization_name=organization_name,
                        location=item.get("location", ""),
                        kind=item.get("eventType", "event"),
                        status=item.get("status", "confirmed"),
                        notes=item.get("description", ""),
                        external_calendar_id=calendar_id,
                        external_etag=item.get("etag"),
                        html_link=item.get("htmlLink"),
                    )
                    tracing.append_trace_event(
                        connection,
                        run_id=trace_run_id,
                        event_type="calendar_event_synced",
                        entity_key=f"{calendar_id}:{item['id']}",
                        payload={
                            "calendar_id": calendar_id,
                            "title": item.get("summary") or "(untitled event)",
                            "status": item.get("status", "confirmed"),
                            "all_day": all_day,
                            "start_at": start_at,
                            "end_at": end_at,
                            "organization_name": organization_name,
                        },
                    )
                    processed += 1
                    calendar_processed += 1

                request = service.events().list_next(request, response)

            per_calendar_counts[calendar_id] = calendar_processed
            store.set_sync_state(
                connection,
                key=f"google_calendar:{calendar_id}:last_sync_at",
                value=datetime.now().isoformat(timespec="minutes"),
            )
            tracing.append_trace_event(
                connection,
                run_id=trace_run_id,
                event_type="calendar_sync_window_complete",
                entity_key=calendar_id,
                payload={"events_processed": calendar_processed},
            )

        summary = {
            "requested_calendars": requested_calendars,
            "calendars": target_calendars,
            "days_back": days_back,
            "days_ahead": days_ahead,
            "events_processed": processed,
            "per_calendar_counts": per_calendar_counts,
        }
        tracing.finish_trace_run(
            connection,
            run_id=trace_run_id,
            status="completed",
            summary=summary,
        )
        return summary
    except Exception as exc:
        tracing.finish_trace_run(
            connection,
            run_id=trace_run_id,
            status="failed",
            summary={"error": str(exc)},
        )
        raise


def _gmail_header_map(message: dict) -> dict[str, str]:
    headers = message.get("payload", {}).get("headers", [])
    return {
        str(header.get("name", "")): str(header.get("value", ""))
        for header in headers
    }


def _collapse_whitespace(value: str) -> str:
    return re.sub(r"[ \t\r\f\v]+", " ", value).strip()


def _collapse_blank_lines(value: str) -> str:
    normalized = re.sub(r"\n{3,}", "\n\n", value)
    return normalized.strip()


def _truncate_text(value: str, limit: int) -> str:
    collapsed = value.strip()
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: max(0, limit - 3)].rstrip() + "..."


def _decode_gmail_body(data: str) -> str:
    if not data:
        return ""
    try:
        padded = data + ("=" * (-len(data) % 4))
        raw_bytes = base64.urlsafe_b64decode(padded.encode("ascii"))
    except (ValueError, binascii.Error):
        return ""
    return raw_bytes.decode("utf-8", errors="ignore")


def _walk_gmail_parts(part: Optional[dict]) -> list[dict]:
    if not part:
        return []
    parts = [part]
    for child in part.get("parts", []) or []:
        parts.extend(_walk_gmail_parts(child))
    return parts


def _html_to_text(value: str) -> str:
    if not value:
        return ""
    parser = _HTMLTextExtractor()
    parser.feed(value)
    return _collapse_blank_lines(_collapse_whitespace(parser.text()).replace("\n ", "\n"))


def _gmail_body_text(message: dict) -> str:
    plain_parts: list[str] = []
    html_parts: list[str] = []

    for part in _walk_gmail_parts(message.get("payload")):
        mime_type = str(part.get("mimeType", "")).lower()
        filename = str(part.get("filename", ""))
        if filename:
            continue

        body = part.get("body", {}) or {}
        data = str(body.get("data", ""))
        if not data:
            continue

        decoded = _decode_gmail_body(data)
        if not decoded:
            continue
        if mime_type == "text/plain":
            plain_parts.append(decoded)
        elif mime_type == "text/html":
            html_parts.append(_html_to_text(decoded))

    combined = "\n\n".join(part for part in plain_parts if part).strip()
    if not combined:
        combined = "\n\n".join(part for part in html_parts if part).strip()

    return _truncate_text(_collapse_blank_lines(combined), BODY_TEXT_LIMIT)


def _gmail_attachment_metadata(message: dict) -> list[dict]:
    attachments: list[dict] = []

    for part in _walk_gmail_parts(message.get("payload")):
        mime_type = str(part.get("mimeType", "")).lower()
        body = part.get("body", {}) or {}
        attachment_id = str(body.get("attachmentId", ""))
        filename = str(part.get("filename", ""))
        if not filename and not attachment_id:
            continue

        headers = {
            str(header.get("name", "")).lower(): str(header.get("value", ""))
            for header in part.get("headers", []) or []
        }
        disposition = headers.get("content-disposition", "")
        attachments.append(
            {
                "filename": filename,
                "mime_type": mime_type,
                "size": int(body.get("size", 0) or 0),
                "attachment_id": attachment_id,
                "part_id": str(part.get("partId", "")),
                "disposition": disposition,
                "inline": disposition.lower().startswith("inline"),
            }
        )

    return attachments


def _gmail_message_time(message: dict, headers: dict[str, str]) -> datetime:
    internal_date = message.get("internalDate")
    if internal_date:
        return store.parse_datetime(
            datetime.fromtimestamp(int(internal_date) / 1000).astimezone().isoformat()
        )

    raw_date = headers.get("Date")
    if raw_date:
        return store.parse_datetime(parsedate_to_datetime(raw_date).isoformat())

    return datetime.now()


def _normalized_text(value: str) -> str:
    return value.strip().lower()


def _sender_domain(sender_email: str) -> str:
    _, _, domain = sender_email.partition("@")
    return _normalized_text(domain)


def _gmail_triage_details(message: dict, headers: dict[str, str]) -> dict:
    label_ids = {str(label_id) for label_id in message.get("labelIds", [])}
    subject = _normalized_text(headers.get("Subject", ""))
    _, sender_email = parseaddr(headers.get("From", ""))
    sender_email = _normalized_text(sender_email)
    sender_domain = _sender_domain(sender_email)
    localpart = sender_email.split("@", 1)[0] if sender_email else ""
    precedence = _normalized_text(headers.get("Precedence", ""))
    auto_submitted = _normalized_text(headers.get("Auto-Submitted", ""))
    has_list_unsubscribe = bool(headers.get("List-Unsubscribe"))
    has_suppress_header = bool(headers.get("X-Auto-Response-Suppress"))

    score = 0
    reasons: list[str] = []

    if label_ids & ACTIONABLE_LABEL_IDS:
        score += 2
        reasons.append("important-label")

    if label_ids & IGNORE_LABEL_IDS:
        score -= 5
        reasons.append("ignored-category")

    if sender_domain in ACTIONABLE_SENDER_DOMAINS:
        score += 4
        reasons.append("priority-domain")

    if sender_domain in NON_ACTIONABLE_SENDER_DOMAINS:
        score -= 4
        reasons.append("newsletter-domain")

    if any(fragment in localpart for fragment in AUTOMATED_LOCALPART_FRAGMENTS):
        score -= 3
        reasons.append("automated-sender")

    if has_list_unsubscribe:
        score -= 2
        reasons.append("mailing-list")

    if precedence in {"bulk", "list", "junk"}:
        score -= 2
        reasons.append("bulk-mail")

    if auto_submitted and auto_submitted != "no":
        score -= 2
        reasons.append("auto-submitted")

    if has_suppress_header:
        score -= 1
        reasons.append("suppressed-response")

    if any(keyword in subject for keyword in STRONG_ACTION_SUBJECT_KEYWORDS):
        score += 4
        reasons.append("strong-action-subject")
    elif any(keyword in subject for keyword in WEAK_ACTION_SUBJECT_KEYWORDS):
        score += 2
        reasons.append("weak-action-subject")

    if any(keyword in subject for keyword in NON_ACTIONABLE_SUBJECT_KEYWORDS):
        score -= 5
        reasons.append("non-actionable-subject")

    actionable = score >= 2
    return {
        "actionable": actionable,
        "score": score,
        "reasons": reasons,
        "label_ids": sorted(label_ids),
        "sender_email": sender_email,
        "sender_domain": sender_domain,
        "subject": headers.get("Subject") or "(no subject)",
    }


def _is_actionable_gmail_message(message: dict, headers: dict[str, str]) -> tuple[bool, str]:
    details = _gmail_triage_details(message, headers)
    reason_text = ",".join(details["reasons"]) or "filtered"
    return bool(details["actionable"]), reason_text


def _thread_candidate_rank(candidate: dict) -> tuple[int, int, datetime]:
    return (
        2 if candidate["status"] == "open" else 1,
        int(candidate["priority_score"]),
        candidate["happened_at"],
    )


def _gmail_notes(triage: dict, classification_result: dict) -> str:
    triage_text = ",".join(triage.get("reasons", [])) or "reviewed"
    categories = ",".join(classification_result.get("categories", [])) or classification_result.get(
        "primary_category",
        "general",
    )
    return (
        "Synced from Gmail. "
        f"status={classification_result['status']} "
        f"triage={triage_text} "
        f"categories={categories} "
        f"retention={classification_result['retention_bucket']}"
    )


def _gmail_service(credentials_path: Path, token_path: Path):
    _, _, _, build = _import_google_clients()
    credentials = get_google_credentials(credentials_path, token_path)
    service = build("gmail", "v1", credentials=credentials)
    profile = service.users().getProfile(userId="me").execute()
    return service, str(profile.get("emailAddress", ""))


def _gmail_query_with_before_ts(base_query: str, *, before_ts: Optional[int] = None) -> str:
    parts = [base_query.strip()] if base_query.strip() else []
    if before_ts is not None:
        parts.append(f"before:{before_ts}")
    return " ".join(part for part in parts if part).strip()


def _gmail_default_backfill_before_ts(*, cutoff_days: int) -> int:
    return int((datetime.now().astimezone() - timedelta(days=cutoff_days)).timestamp())


def _gmail_now_before_ts() -> int:
    return int((datetime.now().astimezone() + timedelta(days=1)).timestamp())


def _gmail_next_before_ts(oldest_message_at: Optional[datetime]) -> Optional[int]:
    if oldest_message_at is None:
        return None
    return max(0, int(oldest_message_at.timestamp()) - 1)


def _sync_state_key(namespace: str, suffix: str) -> str:
    return f"{namespace}:{suffix}"


def _gmail_optional_int_sync_state(connection, key: str) -> Optional[int]:
    raw_value = store.get_sync_state(connection, key)
    if not raw_value:
        return None
    try:
        return int(raw_value)
    except ValueError:
        return None


def _scan_gmail_candidates(
    connection,
    *,
    service,
    user_email: str,
    query: str,
    max_results: int,
    trace_run_id: str,
) -> dict:
    scanned = 0
    actionable_messages = 0
    reference_messages = 0
    filtered = 0
    truncated = False
    follow_up_anchor = datetime.now().replace(second=0, microsecond=0)
    selected_threads: dict[str, dict] = {}
    oldest_message_at: Optional[datetime] = None
    newest_message_at: Optional[datetime] = None

    request = service.users().messages().list(
        userId="me",
        q=query,
        maxResults=min(max_results, 500),
    )
    while request is not None and scanned < max_results:
        response = request.execute()
        page_messages = response.get("messages", [])
        remaining = max_results - scanned
        for message_stub in page_messages[:remaining]:
            message = service.users().messages().get(
                userId="me",
                id=message_stub["id"],
                format="full",
            ).execute()

            headers = _gmail_header_map(message)
            body_text = _gmail_body_text(message)
            attachments = _gmail_attachment_metadata(message)
            triage = _gmail_triage_details(message, headers)
            classification_result = classification.classify_message(
                subject=headers.get("Subject", ""),
                sender=headers.get("From", ""),
                to=headers.get("To", ""),
                cc=headers.get("Cc", ""),
                snippet=message.get("snippet", ""),
                body_text=body_text,
                attachments=attachments,
                triage=triage,
                user_email=user_email,
            )
            happened_at = _gmail_message_time(message, headers)
            scanned += 1
            if oldest_message_at is None or happened_at < oldest_message_at:
                oldest_message_at = happened_at
            if newest_message_at is None or happened_at > newest_message_at:
                newest_message_at = happened_at

            tracing.append_trace_event(
                connection,
                run_id=trace_run_id,
                event_type="gmail_message_triaged",
                entity_key=str(message.get("id", "")),
                payload={
                    "thread_id": str(message.get("threadId") or message.get("id")),
                    "subject": headers.get("Subject") or "(no subject)",
                    "from": headers.get("From", ""),
                    "to": headers.get("To", ""),
                    "snippet": message.get("snippet", ""),
                    "triage": triage,
                    "classification": classification_result,
                    "attachments": attachments,
                    "body_preview": _truncate_text(body_text, BODY_PREVIEW_LIMIT),
                },
            )

            if classification_result["status"] == "ignore":
                filtered += 1
                continue

            sender_name, sender_email = parseaddr(headers.get("From", ""))
            person = sender_name or sender_email
            thread_id = str(message.get("threadId") or message.get("id"))
            if classification_result["status"] == "open":
                actionable_messages += 1
            else:
                reference_messages += 1

            candidate = {
                "thread_id": thread_id,
                "subject": headers.get("Subject") or "(no subject)",
                "happened_at": happened_at,
                "follow_up_at": max(happened_at, follow_up_anchor)
                if classification_result["status"] == "open"
                else None,
                "person": person,
                "notes": _gmail_notes(triage, classification_result),
                "external_from": sender_email or headers.get("From", ""),
                "snippet": message.get("snippet", ""),
                "body_text": body_text,
                "attachments": attachments,
                "status": classification_result["status"],
                "category": classification_result["primary_category"],
                "categories": classification_result["categories"],
                "priority_level": classification_result["priority_level"],
                "priority_score": classification_result["priority_score"],
                "retention_bucket": classification_result["retention_bucket"],
                "classifier_version": classification_result["classifier_version"],
                "triage": triage,
                "classification": classification_result,
            }
            existing = selected_threads.get(thread_id)
            if existing is None or _thread_candidate_rank(candidate) > _thread_candidate_rank(existing):
                selected_threads[thread_id] = candidate

        if scanned >= max_results and response.get("nextPageToken"):
            truncated = True
            break

        request = service.users().messages().list_next(request, response)

    return {
        "messages_scanned": scanned,
        "messages_actionable": actionable_messages,
        "messages_reference": reference_messages,
        "messages_filtered": filtered,
        "results_truncated": truncated,
        "selected_threads": selected_threads,
        "oldest_message_at": oldest_message_at,
        "newest_message_at": newest_message_at,
    }


def _apply_gmail_candidates(
    connection,
    *,
    trace_run_id: str,
    selected_threads: dict[str, dict],
    cleanup_missing_open_rows: bool,
) -> dict:
    replaced_open_rows = 0
    skipped_existing_newer = 0
    open_threads = 0
    reference_threads = 0

    if cleanup_missing_open_rows:
        replaced_open_rows = store.delete_open_communications_not_in_ids(
            connection,
            source="gmail",
            keep_external_ids={f"thread:{thread_id}" for thread_id in selected_threads},
        )

    for candidate in selected_threads.values():
        external_id = f"thread:{candidate['thread_id']}"
        existing = store.get_communication_by_external_id(
            connection,
            source="gmail",
            external_id=external_id,
        )
        if existing and existing["happened_at"]:
            existing_happened_at = store.parse_datetime(str(existing["happened_at"]))
            if existing_happened_at > candidate["happened_at"]:
                skipped_existing_newer += 1
                tracing.append_trace_event(
                    connection,
                    run_id=trace_run_id,
                    event_type="gmail_thread_skipped_existing_newer",
                    entity_key=external_id,
                    payload={
                        "existing_happened_at": existing_happened_at,
                        "candidate_happened_at": candidate["happened_at"],
                        "existing_status": existing["status"],
                        "candidate_status": candidate["status"],
                    },
                )
                continue

        store.upsert_communication_from_sync(
            connection,
            source="gmail",
            external_id=external_id,
            subject=candidate["subject"],
            channel="email",
            happened_at=candidate["happened_at"],
            follow_up_at=candidate["follow_up_at"],
            person=candidate["person"],
            organization_name=None,
            notes=candidate["notes"],
            status=candidate["status"],
            external_thread_id=candidate["thread_id"],
            external_from=candidate["external_from"],
            snippet=candidate["snippet"],
            body_text=candidate["body_text"],
            attachments=candidate["attachments"],
            category=candidate["category"],
            categories=candidate["categories"],
            priority_level=candidate["priority_level"],
            priority_score=candidate["priority_score"],
            retention_bucket=candidate["retention_bucket"],
            classifier_version=candidate["classifier_version"],
            classification=candidate["classification"],
        )
        if candidate["status"] == "open":
            open_threads += 1
        else:
            reference_threads += 1
        tracing.append_trace_event(
            connection,
            run_id=trace_run_id,
            event_type="gmail_thread_selected",
            entity_key=external_id,
            payload={
                "thread_id": candidate["thread_id"],
                "subject": candidate["subject"],
                "happened_at": candidate["happened_at"],
                "follow_up_at": candidate["follow_up_at"],
                "person": candidate["person"],
                "external_from": candidate["external_from"],
                "triage": candidate["triage"],
                "classification": candidate["classification"],
                "attachment_count": len(candidate["attachments"]),
            },
        )

    return {
        "threads_kept": len(selected_threads),
        "threads_open": open_threads,
        "threads_reference": reference_threads,
        "open_rows_replaced": replaced_open_rows,
        "threads_skipped_existing_newer": skipped_existing_newer,
    }


def sync_gmail(
    connection,
    *,
    credentials_path: Path,
    token_path: Path,
    query: str = DEFAULT_GMAIL_QUERY,
    max_results: int = 250,
) -> dict:
    trace_run_id = tracing.start_trace_run(
        connection,
        trace_type="gmail_sync",
        metadata={"query": query, "max_results": max_results},
    )
    try:
        service, user_email = _gmail_service(credentials_path, token_path)
        scan_summary = _scan_gmail_candidates(
            connection,
            service=service,
            user_email=user_email,
            query=query,
            max_results=max_results,
            trace_run_id=trace_run_id,
        )
        apply_summary = _apply_gmail_candidates(
            connection,
            trace_run_id=trace_run_id,
            selected_threads=scan_summary["selected_threads"],
            cleanup_missing_open_rows=True,
        )

        store.set_sync_state(
            connection,
            key="gmail:last_sync_at",
            value=datetime.now().isoformat(timespec="minutes"),
        )
        store.set_sync_state(
            connection,
            key="gmail:user_email",
            value=user_email,
        )
        summary = {
            "query": query,
            "messages_scanned": scan_summary["messages_scanned"],
            "messages_actionable": scan_summary["messages_actionable"],
            "messages_reference": scan_summary["messages_reference"],
            "threads_kept": apply_summary["threads_kept"],
            "threads_open": apply_summary["threads_open"],
            "threads_reference": apply_summary["threads_reference"],
            "messages_filtered": scan_summary["messages_filtered"],
            "open_rows_replaced": apply_summary["open_rows_replaced"],
            "threads_skipped_existing_newer": apply_summary["threads_skipped_existing_newer"],
            "results_truncated": scan_summary["results_truncated"],
            "user_email": user_email,
        }
        tracing.finish_trace_run(
            connection,
            run_id=trace_run_id,
            status="completed",
            summary=summary,
        )
        return summary
    except Exception as exc:
        tracing.finish_trace_run(
            connection,
            run_id=trace_run_id,
            status="failed",
            summary={"error": str(exc)},
        )
        raise


def backfill_gmail(
    connection,
    *,
    credentials_path: Path,
    token_path: Path,
    query: str = DEFAULT_GMAIL_BACKFILL_QUERY,
    max_results: int = 1000,
    before_ts: Optional[int] = None,
    recent_cutoff_days: int = DEFAULT_GMAIL_BACKFILL_CUTOFF_DAYS,
    reset_cursor: bool = False,
    cursor_namespace: str = "gmail_backfill",
    initial_before_ts: Optional[int] = None,
) -> dict:
    stored_query = store.get_sync_state(connection, _sync_state_key(cursor_namespace, "query"))
    stored_before_ts = None if reset_cursor else _gmail_optional_int_sync_state(
        connection,
        _sync_state_key(cursor_namespace, "next_before_ts"),
    )
    cursor_reused = before_ts is None and stored_query == query and stored_before_ts is not None
    effective_before_ts = before_ts
    if effective_before_ts is None:
        if cursor_reused:
            effective_before_ts = stored_before_ts
        elif initial_before_ts is not None:
            effective_before_ts = initial_before_ts
        else:
            effective_before_ts = _gmail_default_backfill_before_ts(cutoff_days=recent_cutoff_days)

    effective_query = _gmail_query_with_before_ts(query, before_ts=effective_before_ts)
    trace_run_id = tracing.start_trace_run(
        connection,
        trace_type="gmail_backfill",
        metadata={
            "base_query": query,
            "effective_query": effective_query,
            "max_results": max_results,
            "before_ts": effective_before_ts,
            "recent_cutoff_days": recent_cutoff_days,
            "reset_cursor": reset_cursor,
            "cursor_reused": cursor_reused,
            "cursor_namespace": cursor_namespace,
            "initial_before_ts": initial_before_ts,
        },
    )
    try:
        service, user_email = _gmail_service(credentials_path, token_path)
        scan_summary = _scan_gmail_candidates(
            connection,
            service=service,
            user_email=user_email,
            query=effective_query,
            max_results=max_results,
            trace_run_id=trace_run_id,
        )
        apply_summary = _apply_gmail_candidates(
            connection,
            trace_run_id=trace_run_id,
            selected_threads=scan_summary["selected_threads"],
            cleanup_missing_open_rows=False,
        )

        next_before_ts = (
            _gmail_next_before_ts(scan_summary["oldest_message_at"])
            if scan_summary["results_truncated"]
            else None
        )
        store.set_sync_state(
            connection,
            key=_sync_state_key(cursor_namespace, "last_sync_at"),
            value=datetime.now().isoformat(timespec="minutes"),
        )
        store.set_sync_state(
            connection,
            key="gmail:user_email",
            value=user_email,
        )
        store.set_sync_state(
            connection,
            key=_sync_state_key(cursor_namespace, "query"),
            value=query,
        )
        store.set_sync_state(
            connection,
            key=_sync_state_key(cursor_namespace, "next_before_ts"),
            value="" if next_before_ts is None else str(next_before_ts),
        )

        summary = {
            "base_query": query,
            "effective_query": effective_query,
            "messages_scanned": scan_summary["messages_scanned"],
            "messages_actionable": scan_summary["messages_actionable"],
            "messages_reference": scan_summary["messages_reference"],
            "threads_kept": apply_summary["threads_kept"],
            "threads_open": apply_summary["threads_open"],
            "threads_reference": apply_summary["threads_reference"],
            "messages_filtered": scan_summary["messages_filtered"],
            "threads_skipped_existing_newer": apply_summary["threads_skipped_existing_newer"],
            "results_truncated": scan_summary["results_truncated"],
            "backfill_exhausted": not scan_summary["results_truncated"],
            "cursor_reused": cursor_reused,
            "before_ts_used": effective_before_ts,
            "next_before_ts": next_before_ts,
            "oldest_message_at": scan_summary["oldest_message_at"],
            "newest_message_at": scan_summary["newest_message_at"],
            "user_email": user_email,
            "cursor_namespace": cursor_namespace,
        }
        tracing.finish_trace_run(
            connection,
            run_id=trace_run_id,
            status="completed",
            summary=summary,
        )
        return summary
    except Exception as exc:
        tracing.finish_trace_run(
            connection,
            run_id=trace_run_id,
            status="failed",
            summary={"error": str(exc)},
        )
        raise


def backfill_gmail_until_exhausted(
    connection,
    *,
    credentials_path: Path,
    token_path: Path,
    query: str,
    max_results: int = 1000,
    recent_cutoff_days: int = DEFAULT_GMAIL_BACKFILL_CUTOFF_DAYS,
    reset_cursor: bool = False,
    cursor_namespace: str = "gmail_backfill",
    initial_before_ts: Optional[int] = None,
    max_runs: Optional[int] = None,
) -> dict:
    trace_run_id = tracing.start_trace_run(
        connection,
        trace_type="gmail_backfill_exhaustive",
        metadata={
            "query": query,
            "max_results": max_results,
            "recent_cutoff_days": recent_cutoff_days,
            "reset_cursor": reset_cursor,
            "cursor_namespace": cursor_namespace,
            "initial_before_ts": initial_before_ts,
            "max_runs": max_runs,
        },
    )
    try:
        runs: list[dict] = []
        previous_next_before_ts = None
        stop_reason = "backfill_exhausted"

        while True:
            backfill_summary = backfill_gmail(
                connection,
                credentials_path=credentials_path,
                token_path=token_path,
                query=query,
                max_results=max_results,
                recent_cutoff_days=recent_cutoff_days,
                reset_cursor=reset_cursor and not runs,
                cursor_namespace=cursor_namespace,
                initial_before_ts=initial_before_ts if not runs else None,
            )
            runs.append(backfill_summary)
            tracing.append_trace_event(
                connection,
                run_id=trace_run_id,
                event_type="gmail_backfill_exhaustive_run_complete",
                entity_key=str(len(runs)),
                payload=backfill_summary,
            )

            next_before_ts = backfill_summary.get("next_before_ts")
            if backfill_summary.get("backfill_exhausted"):
                stop_reason = "backfill_exhausted"
                break
            if next_before_ts is None:
                stop_reason = "cursor_missing"
                break
            if previous_next_before_ts == next_before_ts:
                stop_reason = "cursor_stalled"
                break
            if max_runs is not None and len(runs) >= max_runs:
                stop_reason = "max_runs_reached"
                break

            previous_next_before_ts = next_before_ts

        summary = {
            "query": query,
            "max_results": max_results,
            "recent_cutoff_days": recent_cutoff_days,
            "cursor_namespace": cursor_namespace,
            "runs_completed": len(runs),
            "messages_scanned": sum(int(run["messages_scanned"]) for run in runs),
            "messages_actionable": sum(int(run["messages_actionable"]) for run in runs),
            "messages_reference": sum(int(run["messages_reference"]) for run in runs),
            "threads_kept": sum(int(run["threads_kept"]) for run in runs),
            "threads_open": sum(int(run["threads_open"]) for run in runs),
            "threads_reference": sum(int(run["threads_reference"]) for run in runs),
            "messages_filtered": sum(int(run["messages_filtered"]) for run in runs),
            "threads_skipped_existing_newer": sum(int(run["threads_skipped_existing_newer"]) for run in runs),
            "backfill_exhausted": bool(runs and runs[-1].get("backfill_exhausted")),
            "last_next_before_ts": runs[-1].get("next_before_ts") if runs else None,
            "stop_reason": stop_reason,
        }
        tracing.finish_trace_run(
            connection,
            run_id=trace_run_id,
            status="completed",
            summary=summary,
        )
        return summary
    except Exception as exc:
        tracing.finish_trace_run(
            connection,
            run_id=trace_run_id,
            status="failed",
            summary={"error": str(exc)},
        )
        raise


def sync_gmail_category_pass(
    connection,
    *,
    credentials_path: Path,
    token_path: Path,
    max_results: int = 250,
    reset_cursors: bool = True,
) -> dict:
    trace_run_id = tracing.start_trace_run(
        connection,
        trace_type="gmail_category_pass",
        metadata={
            "max_results": max_results,
            "reset_cursors": reset_cursors,
            "sweeps": DEFAULT_GMAIL_CATEGORY_SWEEPS,
        },
    )
    try:
        sweep_summaries: list[dict] = []
        initial_before_ts = _gmail_now_before_ts()
        for sweep in DEFAULT_GMAIL_CATEGORY_SWEEPS:
            cursor_namespace = f"gmail_backfill_category_{sweep['name']}"
            summary = backfill_gmail_until_exhausted(
                connection,
                credentials_path=credentials_path,
                token_path=token_path,
                query=sweep["query"],
                max_results=max_results,
                reset_cursor=reset_cursors,
                cursor_namespace=cursor_namespace,
                initial_before_ts=initial_before_ts,
            )
            sweep_summary = {
                "name": sweep["name"],
                "query": sweep["query"],
                **summary,
            }
            sweep_summaries.append(sweep_summary)
            tracing.append_trace_event(
                connection,
                run_id=trace_run_id,
                event_type="gmail_category_sweep_complete",
                entity_key=sweep["name"],
                payload=sweep_summary,
            )

        reclassify_summary = reclassify_gmail_records(connection)
        tracing.append_trace_event(
            connection,
            run_id=trace_run_id,
            event_type="gmail_category_reclassify_complete",
            payload=reclassify_summary,
        )
        corpus_summary = store.summarize_communications(connection, source="gmail", status="all")
        summary = {
            "category_sweeps_completed": len(sweep_summaries),
            "messages_scanned": sum(int(sweep["messages_scanned"]) for sweep in sweep_summaries),
            "threads_kept": sum(int(sweep["threads_kept"]) for sweep in sweep_summaries),
            "sweeps": sweep_summaries,
            "reclassify": reclassify_summary,
            "corpus_total": corpus_summary["total"],
            "corpus_by_status": corpus_summary["by_status"],
            "corpus_by_category": corpus_summary["by_category"],
        }
        tracing.finish_trace_run(
            connection,
            run_id=trace_run_id,
            status="completed",
            summary=summary,
        )
        return summary
    except Exception as exc:
        tracing.finish_trace_run(
            connection,
            run_id=trace_run_id,
            status="failed",
            summary={"error": str(exc)},
        )
        raise


def reclassify_gmail_records(
    connection,
    *,
    status: str = "all",
    category: Optional[str] = None,
    limit: Optional[int] = None,
    rewrite_status: bool = False,
) -> dict:
    trace_run_id = tracing.start_trace_run(
        connection,
        trace_type="gmail_reclassify",
        metadata={
            "status": status,
            "category": category,
            "limit": limit,
            "rewrite_status": rewrite_status,
        },
    )
    try:
        user_email = store.get_sync_state(connection, "gmail:user_email") or ""
        rows = store.list_communications(
            connection,
            source="gmail",
            status=status,
            category=category,
            limit=limit,
        )

        processed = 0
        preserved_status = 0
        rewritten_status = 0
        changed_category = 0
        changed_primary_category = 0
        changed_status = 0

        for row in rows:
            attachments = json.loads(str(row["attachments_json"] or "[]"))
            current_categories = json.loads(str(row["categories_json"] or "[]"))
            current_classification = json.loads(str(row["classification_json"] or "{}"))
            current_primary_category = str(row["category"] or "")
            current_status = str(row["status"])
            current_priority_level = str(row["priority_level"] or "")
            current_priority_score = int(row["priority_score"] or 0)
            current_retention_bucket = str(row["retention_bucket"] or "")

            classification_result = classification.classify_message(
                subject=str(row["subject"] or ""),
                sender=str(row["external_from"] or row["person"] or ""),
                snippet=str(row["snippet"] or ""),
                body_text=str(row["body_text"] or ""),
                attachments=attachments,
                triage={},
                user_email=user_email,
            )

            next_status = classification_result["status"] if rewrite_status else current_status
            next_priority_level = classification_result["priority_level"] if rewrite_status else current_priority_level
            next_priority_score = classification_result["priority_score"] if rewrite_status else current_priority_score
            next_retention_bucket = classification_result["retention_bucket"]
            if not rewrite_status and current_status == "open":
                next_retention_bucket = current_retention_bucket or "action_queue"
            classification_for_store = dict(classification_result)
            classification_for_store["status"] = next_status
            classification_for_store["priority_level"] = next_priority_level
            classification_for_store["priority_score"] = next_priority_score
            classification_for_store["retention_bucket"] = next_retention_bucket
            if rewrite_status:
                rewritten_status += 1
            else:
                preserved_status += 1

            if current_primary_category != classification_result["primary_category"]:
                changed_primary_category += 1
            if sorted(current_categories) != sorted(classification_result["categories"]):
                changed_category += 1
            if current_status != next_status:
                changed_status += 1

            happened_at = store.parse_datetime(str(row["happened_at"]))
            follow_up_at = (
                store.parse_datetime(str(row["follow_up_at"]))
                if row["follow_up_at"]
                else None
            )
            if next_status != "open":
                follow_up_at = None

            store.upsert_communication_from_sync(
                connection,
                source="gmail",
                external_id=str(row["external_id"]),
                subject=str(row["subject"]),
                channel=str(row["channel"]),
                happened_at=happened_at,
                follow_up_at=follow_up_at,
                person=str(row["person"] or ""),
                organization_name=str(row["organization_name"] or "") or None,
                notes=str(row["notes"] or ""),
                status=next_status,
                external_thread_id=str(row["external_thread_id"] or "") or None,
                external_from=str(row["external_from"] or "") or None,
                snippet=str(row["snippet"] or ""),
                body_text=str(row["body_text"] or ""),
                attachments=attachments,
                category=classification_result["primary_category"],
                categories=classification_result["categories"],
                priority_level=next_priority_level,
                priority_score=next_priority_score,
                retention_bucket=next_retention_bucket,
                classifier_version=classification_result["classifier_version"],
                classification=classification_for_store,
            )
            tracing.append_trace_event(
                connection,
                run_id=trace_run_id,
                event_type="gmail_record_reclassified",
                entity_key=str(row["external_id"] or row["id"]),
                payload={
                    "communication_id": int(row["id"]),
                    "subject": str(row["subject"]),
                    "previous_status": current_status,
                    "next_status": next_status,
                    "previous_primary_category": current_primary_category,
                    "next_primary_category": classification_result["primary_category"],
                    "previous_categories": current_categories,
                    "next_categories": classification_result["categories"],
                    "previous_classifier_version": current_classification.get("classifier_version"),
                    "next_classifier_version": classification_result["classifier_version"],
                },
            )
            processed += 1

        summary = {
            "processed": processed,
            "status_filter": status,
            "category_filter": category,
            "limit": limit,
            "rewrite_status": rewrite_status,
            "preserved_status": preserved_status,
            "rewritten_status": rewritten_status,
            "changed_primary_category": changed_primary_category,
            "changed_category_lists": changed_category,
            "changed_status": changed_status,
        }
        tracing.finish_trace_run(
            connection,
            run_id=trace_run_id,
            status="completed",
            summary=summary,
        )
        return summary
    except Exception as exc:
        tracing.finish_trace_run(
            connection,
            run_id=trace_run_id,
            status="failed",
            summary={"error": str(exc)},
        )
        raise


def sync_gmail_corpus(
    connection,
    *,
    credentials_path: Path,
    token_path: Path,
    recent_query: str = DEFAULT_GMAIL_QUERY,
    recent_max_results: int = 250,
    backfill_query: str = DEFAULT_GMAIL_BACKFILL_QUERY,
    backfill_max_results: int = 1000,
    backfill_max_runs: int = 50,
    recent_cutoff_days: int = DEFAULT_GMAIL_BACKFILL_CUTOFF_DAYS,
    reset_backfill_cursor: bool = False,
) -> dict:
    trace_run_id = tracing.start_trace_run(
        connection,
        trace_type="gmail_corpus_sync",
        metadata={
            "recent_query": recent_query,
            "recent_max_results": recent_max_results,
            "backfill_query": backfill_query,
            "backfill_max_results": backfill_max_results,
            "backfill_max_runs": backfill_max_runs,
            "recent_cutoff_days": recent_cutoff_days,
            "reset_backfill_cursor": reset_backfill_cursor,
        },
    )
    try:
        recent_summary = sync_gmail(
            connection,
            credentials_path=credentials_path,
            token_path=token_path,
            query=recent_query,
            max_results=recent_max_results,
        )
        tracing.append_trace_event(
            connection,
            run_id=trace_run_id,
            event_type="gmail_corpus_recent_sync_complete",
            payload=recent_summary,
        )

        backfill_summary = backfill_gmail_until_exhausted(
            connection,
            credentials_path=credentials_path,
            token_path=token_path,
            query=backfill_query,
            max_results=backfill_max_results,
            recent_cutoff_days=recent_cutoff_days,
            reset_cursor=reset_backfill_cursor,
            cursor_namespace="gmail_backfill",
            max_runs=None if backfill_max_runs <= 0 else backfill_max_runs,
        )
        tracing.append_trace_event(
            connection,
            run_id=trace_run_id,
            event_type="gmail_corpus_backfill_complete",
            payload=backfill_summary,
        )

        reclassify_summary = reclassify_gmail_records(connection)
        tracing.append_trace_event(
            connection,
            run_id=trace_run_id,
            event_type="gmail_corpus_reclassify_complete",
            payload=reclassify_summary,
        )

        corpus_summary = store.summarize_communications(connection, source="gmail", status="all")
        summary = {
            "recent": recent_summary,
            "backfill_runs_completed": backfill_summary["runs_completed"],
            "backfill_messages_scanned": backfill_summary["messages_scanned"],
            "backfill_threads_kept": backfill_summary["threads_kept"],
            "backfill_exhausted": backfill_summary["backfill_exhausted"],
            "last_backfill_next_before_ts": backfill_summary["last_next_before_ts"],
            "stop_reason": backfill_summary["stop_reason"],
            "reclassify": reclassify_summary,
            "corpus_total": corpus_summary["total"],
            "corpus_by_status": corpus_summary["by_status"],
            "corpus_by_category": corpus_summary["by_category"],
        }
        tracing.finish_trace_run(
            connection,
            run_id=trace_run_id,
            status="completed",
            summary=summary,
        )
        return summary
    except Exception as exc:
        tracing.finish_trace_run(
            connection,
            run_id=trace_run_id,
            status="failed",
            summary={"error": str(exc)},
        )
        raise
