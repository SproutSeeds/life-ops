from __future__ import annotations

import argparse
import base64
import json
import mimetypes
import socket
import uuid
from datetime import date, datetime, time
from email.message import EmailMessage
from pathlib import Path
from typing import Optional

from life_ops.agenda import build_agenda, render_agenda_json, render_agenda_text
from life_ops.backups import (
    backup_root,
    backup_status as encrypted_backup_status,
    create_encrypted_db_backup,
    list_backups as list_encrypted_backups,
    restore_encrypted_db_backup,
)
from life_ops.cloudflare_email import (
    enqueue_cloudflare_mail_payload,
    default_cloudflare_mail_config_path,
    default_cloudflare_worker_output_dir,
    cloudflare_mail_queue_status,
    cloudflare_mail_status,
    sync_cloudflare_mail_queue,
    write_cloudflare_mail_config_template,
    write_cloudflare_worker_template,
)
from life_ops import credentials
from life_ops.document_ingest import (
    DEFAULT_PROFILE_ATTACHMENT_BACKFILL_NAMESPACE,
    backfill_profile_attachments_until_exhausted,
    ingest_profile_attachments,
)
from life_ops.emma_integration import (
    emma_agents,
    emma_chat,
    emma_me,
    emma_status,
)
from life_ops.fastmail_integration import (
    default_fastmail_config_path,
    fastmail_mailboxes,
    fastmail_session,
    fastmail_status,
    write_fastmail_config_template,
)
from life_ops.google_sync import (
    DEFAULT_GMAIL_BACKFILL_CUTOFF_DAYS,
    DEFAULT_GMAIL_BACKFILL_QUERY,
    DEFAULT_GMAIL_QUERY,
    backfill_gmail,
    default_credentials_path,
    default_token_path,
    ensure_google_auth,
    list_google_calendars,
    reclassify_gmail_records,
    sync_gmail_corpus,
    sync_gmail_category_pass,
    sync_gmail,
    sync_google_calendar,
)
from life_ops.mail_ingest import (
    DEFAULT_MAIL_INGEST_HOST,
    DEFAULT_MAIL_INGEST_PATH,
    DEFAULT_MAIL_INGEST_PORT,
    generate_mail_ingest_secret,
    ingest_cloudflare_email_payload,
    mail_ingest_status,
    serve_mail_ingest,
)
from life_ops.mail_ui import (
    DEFAULT_MAIL_UI_HOST,
    DEFAULT_MAIL_UI_LIMIT,
    DEFAULT_MAIL_UI_PORT,
    list_cmail_drafts,
    save_cmail_draft,
    send_cmail_draft,
    serve_mail_ui,
)
from life_ops.cmail_runtime import (
    default_cmail_runtime_db_path,
    ensure_cmail_runtime_db,
    ensure_cmail_runtime_list_items,
    resolve_cmail_db_path,
    seal_cmail_runtime_db,
    serve_cmail_service,
)
from life_ops.profile_context import extract_profile_context_items
from life_ops.profile_memory import (
    approve_profile_context_item,
    get_profile_record_payload,
    list_profile_alerts,
    merge_profile_context_item,
    reject_profile_context_item,
)
from life_ops.resend_integration import (
    default_resend_config_path,
    process_resend_delivery_queue,
    resend_create_domain,
    resend_get_default_signature,
    resend_list_domains,
    resend_queue_status,
    resend_send_email,
    resend_set_default_signature,
    resend_status,
    write_resend_config_template,
)
from life_ops import store
from life_ops.social.post import (
    authenticate as social_authenticate,
    available_platforms as social_available_platforms,
    check_status as social_check_status,
    post_multi as social_post_multi,
    post_to_platform as social_post_to_platform,
)
from life_ops.social.browser import clear_session as social_clear_session, list_sessions as social_list_sessions
from life_ops import tracing
from life_ops.x_content import (
    DEFAULT_OPENAI_IMAGE_BACKGROUND,
    DEFAULT_OPENAI_IMAGE_MODEL,
    DEFAULT_OPENAI_IMAGE_MODERATION,
    DEFAULT_OPENAI_IMAGE_OUTPUT_FORMAT,
    DEFAULT_OPENAI_IMAGE_QUALITY,
    DEFAULT_OPENAI_IMAGE_SIZE,
    DEFAULT_XAI_IMAGE_MODEL,
    create_x_article_package,
    generate_x_media_asset,
)
from life_ops.x_integration import (
    default_x_client_path,
    default_x_token_path,
    refresh_x_token,
    write_x_client_template,
    x_auth,
    x_create_post,
    x_delete_post,
    x_get_authenticated_user,
    x_get_home_timeline,
    x_get_user_posts,
    x_lookup_user_by_username,
    x_status,
)
from life_ops.vault_crypto import generate_master_key, master_key_status


def _inline_spec_parts(spec: str) -> tuple[Path, str]:
    raw_value = str(spec or "").strip()
    if not raw_value:
        raise ValueError("inline attachment spec must not be empty")
    for separator in ("::", "="):
        if separator in raw_value:
            raw_path, raw_content_id = raw_value.split(separator, 1)
            attachment_path = Path(raw_path.strip()).expanduser()
            content_id = raw_content_id.strip()
            if not content_id:
                raise ValueError("inline attachment spec is missing a content id")
            return attachment_path, content_id
    raise ValueError("inline attachment spec must be PATH::content-id or PATH=content-id")


def _guess_mime_parts(path: Path) -> tuple[str, str]:
    mime_type, _ = mimetypes.guess_type(str(path))
    if not mime_type or "/" not in mime_type:
        return "application", "octet-stream"
    maintype, subtype = mime_type.split("/", 1)
    return maintype, subtype


def _parse_time(value: str) -> str:
    time.fromisoformat(value)
    return value


def _parse_day(value: str) -> date:
    return date.fromisoformat(value)


def _parse_datetime_or_date(value: str) -> datetime:
    return store.parse_datetime(value)


def _normalize_event_bounds(start_at: datetime, end_at: datetime, all_day: bool) -> tuple[datetime, datetime]:
    if not all_day:
        return start_at, end_at

    return (
        datetime.combine(start_at.date(), time(0, 0)),
        datetime.combine(end_at.date(), time(23, 59)),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="life-ops",
        description="Local-first agenda system for scheduling, comms, and routines.",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=store.default_db_path(),
        help="Path to the SQLite database.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init", help="Initialize the local database.")
    subparsers.add_parser("seed-demo", help="Populate the database with sample data.")

    google_auth = subparsers.add_parser("google-auth", help="Authenticate with Google and cache a token locally.")
    google_auth.add_argument("--credentials", type=Path, default=default_credentials_path())
    google_auth.add_argument("--token", type=Path, default=default_token_path())

    google_calendars = subparsers.add_parser("google-list-calendars", help="List available Google Calendar IDs.")
    google_calendars.add_argument("--credentials", type=Path, default=default_credentials_path())
    google_calendars.add_argument("--token", type=Path, default=default_token_path())

    sync_calendar = subparsers.add_parser("sync-google-calendar", help="Sync Google Calendar events into the local store.")
    sync_calendar.add_argument("--credentials", type=Path, default=default_credentials_path())
    sync_calendar.add_argument("--token", type=Path, default=default_token_path())
    sync_calendar.add_argument("--calendar-id", action="append", dest="calendar_ids", default=None)
    sync_calendar.add_argument("--days-back", type=int, default=7)
    sync_calendar.add_argument("--days-ahead", type=int, default=30)

    sync_gmail_parser = subparsers.add_parser("sync-gmail", help="Sync Gmail follow-ups into the local store.")
    sync_gmail_parser.add_argument("--credentials", type=Path, default=default_credentials_path())
    sync_gmail_parser.add_argument("--token", type=Path, default=default_token_path())
    sync_gmail_parser.add_argument("--query", default=DEFAULT_GMAIL_QUERY)
    sync_gmail_parser.add_argument("--max-results", type=int, default=250)

    backfill_gmail_parser = subparsers.add_parser("backfill-gmail", help="Backfill older Gmail history into the local store.")
    backfill_gmail_parser.add_argument("--credentials", type=Path, default=default_credentials_path())
    backfill_gmail_parser.add_argument("--token", type=Path, default=default_token_path())
    backfill_gmail_parser.add_argument("--query", default=DEFAULT_GMAIL_BACKFILL_QUERY)
    backfill_gmail_parser.add_argument("--max-results", type=int, default=1000)
    backfill_gmail_parser.add_argument("--before-ts", type=int, default=None)
    backfill_gmail_parser.add_argument("--recent-cutoff-days", type=int, default=DEFAULT_GMAIL_BACKFILL_CUTOFF_DAYS)
    backfill_gmail_parser.add_argument("--reset-cursor", action="store_true")

    sync_google = subparsers.add_parser("sync-google", help="Run both Google Calendar and Gmail sync.")
    sync_google.add_argument("--credentials", type=Path, default=default_credentials_path())
    sync_google.add_argument("--token", type=Path, default=default_token_path())
    sync_google.add_argument("--calendar-id", action="append", dest="calendar_ids", default=None)
    sync_google.add_argument("--days-back", type=int, default=7)
    sync_google.add_argument("--days-ahead", type=int, default=30)
    sync_google.add_argument("--query", default=DEFAULT_GMAIL_QUERY)
    sync_google.add_argument("--max-results", type=int, default=250)

    sync_gmail_corpus_parser = subparsers.add_parser("sync-gmail-corpus", help="Run recent Gmail sync, archive backfill, and local reclassification end to end.")
    sync_gmail_corpus_parser.add_argument("--credentials", type=Path, default=default_credentials_path())
    sync_gmail_corpus_parser.add_argument("--token", type=Path, default=default_token_path())
    sync_gmail_corpus_parser.add_argument("--recent-query", default=DEFAULT_GMAIL_QUERY)
    sync_gmail_corpus_parser.add_argument("--recent-max-results", type=int, default=250)
    sync_gmail_corpus_parser.add_argument("--backfill-query", default=DEFAULT_GMAIL_BACKFILL_QUERY)
    sync_gmail_corpus_parser.add_argument("--backfill-max-results", type=int, default=1000)
    sync_gmail_corpus_parser.add_argument("--backfill-max-runs", type=int, default=50)
    sync_gmail_corpus_parser.add_argument("--recent-cutoff-days", type=int, default=DEFAULT_GMAIL_BACKFILL_CUTOFF_DAYS)
    sync_gmail_corpus_parser.add_argument("--reset-backfill-cursor", action="store_true")

    sync_gmail_category_parser = subparsers.add_parser("sync-gmail-category-pass", help="Run focused category sweeps across Gmail history, then reclassify the local Gmail corpus.")
    sync_gmail_category_parser.add_argument("--credentials", type=Path, default=default_credentials_path())
    sync_gmail_category_parser.add_argument("--token", type=Path, default=default_token_path())
    sync_gmail_category_parser.add_argument("--max-results", type=int, default=250)
    sync_gmail_category_parser.add_argument("--reset-cursors", action="store_true")

    agenda_parser = subparsers.add_parser("agenda", help="Render the agenda window.")
    agenda_parser.add_argument("--start", type=_parse_day, default=date.today())
    agenda_parser.add_argument("--days", type=int, default=7)
    agenda_parser.add_argument("--format", choices=["text", "json"], default="text")

    trace_summary_parser = subparsers.add_parser("trace-summary", help="Summarize recorded behavior traces.")
    trace_summary_parser.add_argument("--trace-type", default=None)
    trace_summary_parser.add_argument("--limit", type=int, default=10)
    trace_summary_parser.add_argument("--format", choices=["text", "json"], default="text")

    gmail_heartbeat_parser = subparsers.add_parser("gmail-heartbeat", help="Show the current Gmail backlog heartbeat and corpus progress.")
    gmail_heartbeat_parser.add_argument("--format", choices=["text", "json"], default="text")

    profile_attachment_heartbeat_parser = subparsers.add_parser("profile-attachment-heartbeat", help="Show the current sensitive attachment backfill heartbeat and vault progress.")
    profile_attachment_heartbeat_parser.add_argument("--format", choices=["text", "json"], default="text")

    export_traces_parser = subparsers.add_parser("export-traces", help="Export recorded traces for analysis or training.")
    export_traces_parser.add_argument("--trace-type", default=None)
    export_traces_parser.add_argument("--limit", type=int, default=1000)
    export_traces_parser.add_argument("--format", choices=["jsonl", "json"], default="jsonl")
    export_traces_parser.add_argument("--output", type=Path, default=None)

    keys_set_parser = subparsers.add_parser("keys-set", help="Store a global secret for life-ops and related tooling.")
    keys_set_parser.add_argument("--name", required=True)
    keys_set_parser.add_argument("--value", default=None)
    keys_set_parser.add_argument("--from-env", action="store_true")
    keys_set_parser.add_argument("--backend", choices=["auto", "keychain", "file"], default="auto")
    keys_set_parser.add_argument("--allow-insecure-file-backend", action="store_true")
    keys_set_parser.add_argument("--format", choices=["text", "json"], default="text")

    keys_list_parser = subparsers.add_parser("keys-list", help="List globally registered life-ops secrets without printing values.")
    keys_list_parser.add_argument("--format", choices=["text", "json"], default="text")

    keys_export_parser = subparsers.add_parser("keys-export", help="Print shell export lines for registered life-ops secrets.")
    keys_export_parser.add_argument("--name", action="append", dest="names", default=[])
    keys_export_parser.add_argument("--format", choices=["shell", "json"], default="shell")

    keys_delete_parser = subparsers.add_parser("keys-delete", help="Remove a registered life-ops secret.")
    keys_delete_parser.add_argument("--name", required=True)
    keys_delete_parser.add_argument("--format", choices=["text", "json"], default="text")

    vault_generate_parser = subparsers.add_parser(
        "vault-generate-master-key",
        help="Generate and store the shared master encryption key used for encrypted backups and encrypted cloud mail copies.",
    )
    vault_generate_parser.add_argument("--backend", choices=["auto", "keychain", "file"], default="auto")
    vault_generate_parser.add_argument("--allow-insecure-file-backend", action="store_true")
    vault_generate_parser.add_argument("--format", choices=["text", "json"], default="text")

    vault_status_parser = subparsers.add_parser(
        "vault-status",
        help="Show whether the shared master encryption key is configured.",
    )
    vault_status_parser.add_argument("--format", choices=["text", "json"], default="text")

    backup_create_parser = subparsers.add_parser(
        "backup-create",
        help="Create an encrypted local backup of the life-ops SQLite database.",
    )
    backup_create_parser.add_argument("--db", type=Path, default=store.default_db_path())
    backup_create_parser.add_argument("--output-dir", type=Path, default=backup_root())
    backup_create_parser.add_argument("--format", choices=["text", "json"], default="text")

    backup_list_parser = subparsers.add_parser(
        "backup-list",
        help="List encrypted local backups stored on this rig.",
    )
    backup_list_parser.add_argument("--output-dir", type=Path, default=backup_root())
    backup_list_parser.add_argument("--format", choices=["text", "json"], default="text")

    backup_status_parser = subparsers.add_parser(
        "backup-status",
        help="Show encrypted local backup status and the latest snapshot.",
    )
    backup_status_parser.add_argument("--output-dir", type=Path, default=backup_root())
    backup_status_parser.add_argument("--format", choices=["text", "json"], default="text")

    backup_restore_parser = subparsers.add_parser(
        "backup-restore",
        help="Restore an encrypted local backup into a SQLite file.",
    )
    backup_restore_parser.add_argument("--manifest-path", type=Path, required=True)
    backup_restore_parser.add_argument("--output-path", type=Path, required=True)
    backup_restore_parser.add_argument("--format", choices=["text", "json"], default="text")

    cloudflare_mail_init_parser = subparsers.add_parser(
        "cloudflare-mail-init-config",
        help="Write a local Cloudflare Email Routing config template.",
    )
    cloudflare_mail_init_parser.add_argument("--config", type=Path, default=default_cloudflare_mail_config_path())
    cloudflare_mail_init_parser.add_argument("--force", action="store_true")

    cloudflare_mail_status_parser = subparsers.add_parser(
        "cloudflare-mail-status",
        help="Show the current Cloudflare inbound-mail readiness.",
    )
    cloudflare_mail_status_parser.add_argument("--config", type=Path, default=default_cloudflare_mail_config_path())
    cloudflare_mail_status_parser.add_argument("--format", choices=["text", "json"], default="text")

    cloudflare_mail_queue_status_parser = subparsers.add_parser(
        "cloudflare-mail-queue-status",
        help="Show the current durable Cloudflare mail queue status.",
    )
    cloudflare_mail_queue_status_parser.add_argument("--config", type=Path, default=default_cloudflare_mail_config_path())
    cloudflare_mail_queue_status_parser.add_argument("--format", choices=["text", "json"], default="text")

    cloudflare_mail_worker_parser = subparsers.add_parser(
        "cloudflare-mail-write-worker",
        help="Generate a Cloudflare Email Worker template for sovereign inbound mail.",
    )
    cloudflare_mail_worker_parser.add_argument("--config", type=Path, default=default_cloudflare_mail_config_path())
    cloudflare_mail_worker_parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_cloudflare_worker_output_dir(),
    )
    cloudflare_mail_worker_parser.add_argument("--force", action="store_true")

    cloudflare_mail_sync_parser = subparsers.add_parser(
        "cloudflare-mail-sync",
        help="Pull queued inbound mail from the Cloudflare worker into the local SQLite store.",
    )
    cloudflare_mail_sync_parser.add_argument("--config", type=Path, default=default_cloudflare_mail_config_path())
    cloudflare_mail_sync_parser.add_argument("--limit", type=int, default=25)
    cloudflare_mail_sync_parser.add_argument("--format", choices=["text", "json"], default="text")

    cloudflare_mail_inject_test_parser = subparsers.add_parser(
        "cloudflare-mail-inject-test",
        help="Enqueue a signed synthetic inbound email into the Cloudflare worker queue for testing.",
    )
    cloudflare_mail_inject_test_parser.add_argument("--config", type=Path, default=default_cloudflare_mail_config_path())
    cloudflare_mail_inject_test_parser.add_argument("--to", default="cody@frg.earth")
    cloudflare_mail_inject_test_parser.add_argument("--from", dest="from_email", default="life-ops-test@example.net")
    cloudflare_mail_inject_test_parser.add_argument("--subject", default="life-ops durable queue test")
    cloudflare_mail_inject_test_parser.add_argument("--body", default="Synthetic Cloudflare queue test for local SQLite sync.")
    cloudflare_mail_inject_test_parser.add_argument("--html", default="")
    cloudflare_mail_inject_test_parser.add_argument("--attach", action="append", dest="attachment_paths", default=[])
    cloudflare_mail_inject_test_parser.add_argument(
        "--inline",
        action="append",
        dest="inline_attachment_specs",
        default=[],
        help="Attach a local file inline for HTML email. Use PATH::content-id or PATH=content-id.",
    )
    cloudflare_mail_inject_test_parser.add_argument("--format", choices=["text", "json"], default="text")

    mail_ingest_secret_parser = subparsers.add_parser(
        "mail-ingest-generate-secret",
        help="Generate and store the shared signing secret used by the Cloudflare inbound worker.",
    )
    mail_ingest_secret_parser.add_argument("--backend", choices=["auto", "keychain", "file"], default="auto")
    mail_ingest_secret_parser.add_argument("--allow-insecure-file-backend", action="store_true")
    mail_ingest_secret_parser.add_argument("--format", choices=["text", "json"], default="text")

    mail_ingest_status_parser = subparsers.add_parser(
        "mail-ingest-status",
        help="Show the current readiness of the local inbound mail receiver.",
    )
    mail_ingest_status_parser.add_argument("--format", choices=["text", "json"], default="text")

    mail_ingest_file_parser = subparsers.add_parser(
        "mail-ingest-file",
        help="Ingest one Cloudflare Email Worker payload from disk into the local store.",
    )
    mail_ingest_file_parser.add_argument("--input", type=Path, required=True)
    mail_ingest_file_parser.add_argument("--format", choices=["text", "json"], default="text")

    mail_ingest_serve_parser = subparsers.add_parser(
        "mail-ingest-serve",
        help="Start the local HTTP receiver for sovereign inbound mail.",
    )
    mail_ingest_serve_parser.add_argument("--host", default=DEFAULT_MAIL_INGEST_HOST)
    mail_ingest_serve_parser.add_argument("--port", type=int, default=DEFAULT_MAIL_INGEST_PORT)
    mail_ingest_serve_parser.add_argument("--path", default=DEFAULT_MAIL_INGEST_PATH)

    mail_ui_parser = subparsers.add_parser(
        "mail-ui",
        help="Start a super minimal local frontend for the life-ops mail system.",
    )
    mail_ui_parser.add_argument("--db", type=Path, default=default_cmail_runtime_db_path())
    mail_ui_parser.add_argument("--host", default=DEFAULT_MAIL_UI_HOST)
    mail_ui_parser.add_argument("--port", type=int, default=DEFAULT_MAIL_UI_PORT)
    mail_ui_parser.add_argument("--limit", type=int, default=DEFAULT_MAIL_UI_LIMIT)

    cmail_serve_parser = subparsers.add_parser(
        "cmail-serve",
        help="Start the managed local CMAIL runtime service against the hot runtime mailbox DB.",
    )
    cmail_serve_parser.add_argument("--db", type=Path, default=default_cmail_runtime_db_path())
    cmail_serve_parser.add_argument("--canonical-db", type=Path, default=store.default_db_path())
    cmail_serve_parser.add_argument("--host", default=DEFAULT_MAIL_UI_HOST)
    cmail_serve_parser.add_argument("--port", type=int, default=DEFAULT_MAIL_UI_PORT)
    cmail_serve_parser.add_argument("--limit", type=int, default=DEFAULT_MAIL_UI_LIMIT)
    cmail_serve_parser.add_argument("--sync-interval", type=float, default=2.0)
    cmail_serve_parser.add_argument("--send-interval", type=float, default=2.0)
    cmail_serve_parser.add_argument("--seal-interval", type=float, default=30.0)

    cmail_runtime_seal_parser = subparsers.add_parser(
        "cmail-runtime-seal",
        help="Seal the hot runtime mailbox DB back into the encrypted canonical life-ops store.",
    )
    cmail_runtime_seal_parser.add_argument("--db", type=Path, default=default_cmail_runtime_db_path())
    cmail_runtime_seal_parser.add_argument("--canonical-db", type=Path, default=store.default_db_path())
    cmail_runtime_seal_parser.add_argument("--format", choices=["text", "json"], default="text")

    cmail_drafts_parser = subparsers.add_parser(
        "cmail-drafts",
        help="List local CMAIL drafts stored for manual review or later send.",
    )
    cmail_drafts_parser.add_argument("--db", type=Path, default=default_cmail_runtime_db_path())
    cmail_drafts_parser.add_argument("--format", choices=["text", "json"], default="text")

    cmail_draft_save_parser = subparsers.add_parser(
        "cmail-draft-save",
        help="Create or update a local CMAIL draft without sending it.",
    )
    cmail_draft_save_parser.add_argument("--db", type=Path, default=default_cmail_runtime_db_path())
    cmail_draft_save_parser.add_argument("--id", type=int, default=0)
    cmail_draft_save_parser.add_argument("--to", default="")
    cmail_draft_save_parser.add_argument("--cc", default="")
    cmail_draft_save_parser.add_argument("--bcc", default="")
    cmail_draft_save_parser.add_argument("--subject", default="")
    cmail_draft_save_parser.add_argument("--body", default="")
    cmail_draft_save_parser.add_argument("--body-file", type=Path, default=None)
    cmail_draft_save_parser.add_argument("--attach", action="append", default=[])
    cmail_draft_save_parser.add_argument("--format", choices=["text", "json"], default="text")

    cmail_draft_send_parser = subparsers.add_parser(
        "cmail-draft-send",
        help="Send a saved local CMAIL draft through the configured outbound mail path.",
    )
    cmail_draft_send_parser.add_argument("--db", type=Path, default=default_cmail_runtime_db_path())
    cmail_draft_send_parser.add_argument("--id", required=True, type=int)
    cmail_draft_send_parser.add_argument("--format", choices=["text", "json"], default="text")

    resend_init_parser = subparsers.add_parser("resend-init-config", help="Write a local Resend config template.")
    resend_init_parser.add_argument("--config", type=Path, default=default_resend_config_path())
    resend_init_parser.add_argument("--force", action="store_true")

    resend_status_parser = subparsers.add_parser("resend-status", help="Show the current Resend sending readiness.")
    resend_status_parser.add_argument("--config", type=Path, default=default_resend_config_path())
    resend_status_parser.add_argument("--format", choices=["text", "json"], default="text")

    resend_signature_show_parser = subparsers.add_parser(
        "resend-signature-show",
        help="Show the saved default outbound signature for Resend sends.",
    )
    resend_signature_show_parser.add_argument("--config", type=Path, default=default_resend_config_path())
    resend_signature_show_parser.add_argument("--format", choices=["text", "json"], default="text")

    resend_signature_set_parser = subparsers.add_parser(
        "resend-signature-set",
        help="Save the default outbound signature used for Resend sends.",
    )
    resend_signature_set_parser.add_argument("--config", type=Path, default=default_resend_config_path())
    resend_signature_set_parser.add_argument("--text", default="")
    resend_signature_set_parser.add_argument("--html", default="")
    resend_signature_set_parser.add_argument("--clear", action="store_true")
    resend_signature_set_parser.add_argument("--format", choices=["text", "json"], default="text")

    resend_domains_parser = subparsers.add_parser("resend-domains", help="List sending domains available through Resend.")
    resend_domains_parser.add_argument("--config", type=Path, default=default_resend_config_path())
    resend_domains_parser.add_argument("--format", choices=["text", "json"], default="text")

    resend_domain_create_parser = subparsers.add_parser("resend-domain-create", help="Create a sending domain in Resend.")
    resend_domain_create_parser.add_argument("--config", type=Path, default=default_resend_config_path())
    resend_domain_create_parser.add_argument("--name", required=True)
    resend_domain_create_parser.add_argument("--region", default="us-east-1")
    resend_domain_create_parser.add_argument("--format", choices=["text", "json"], default="text")

    resend_queue_status_parser = subparsers.add_parser(
        "resend-queue-status",
        help="Show the local outbound Resend delivery queue and any active delivery alerts.",
    )
    resend_queue_status_parser.add_argument("--db", type=Path, default=store.default_db_path())
    resend_queue_status_parser.add_argument("--limit", type=int, default=25)
    resend_queue_status_parser.add_argument("--format", choices=["text", "json"], default="text")

    resend_queue_process_parser = subparsers.add_parser(
        "resend-queue-process",
        help="Process queued outbound Resend messages with retry/backoff semantics.",
    )
    resend_queue_process_parser.add_argument("--db", type=Path, default=store.default_db_path())
    resend_queue_process_parser.add_argument("--config", type=Path, default=default_resend_config_path())
    resend_queue_process_parser.add_argument("--limit", type=int, default=25)
    resend_queue_process_parser.add_argument("--format", choices=["text", "json"], default="text")

    resend_send_parser = subparsers.add_parser("resend-send-email", help="Send an outbound email through Resend.")
    resend_send_parser.add_argument("--db", type=Path, default=store.default_db_path())
    resend_send_parser.add_argument("--config", type=Path, default=default_resend_config_path())
    resend_send_parser.add_argument("--to", action="append", dest="to_addresses", required=True)
    resend_send_parser.add_argument("--cc", action="append", dest="cc_addresses", default=[])
    resend_send_parser.add_argument("--bcc", action="append", dest="bcc_addresses", default=[])
    resend_send_parser.add_argument("--subject", required=True)
    resend_send_parser.add_argument("--text", default="")
    resend_send_parser.add_argument("--html", default="")
    resend_send_parser.add_argument("--from", dest="from_email", default=None)
    resend_send_parser.add_argument("--reply-to", default=None)
    resend_send_parser.add_argument("--in-reply-to", default=None)
    resend_send_parser.add_argument("--reference", action="append", dest="reference_ids", default=[])
    resend_send_parser.add_argument("--thread-key", default=None)
    resend_send_parser.add_argument("--attach", action="append", dest="attachment_paths", default=[])
    resend_send_parser.add_argument(
        "--inline",
        action="append",
        dest="inline_attachment_specs",
        default=[],
        help="Attach a local file inline for HTML email. Use PATH::content-id or PATH=content-id.",
    )
    resend_send_parser.add_argument("--no-signature", action="store_true")
    resend_send_parser.add_argument("--queue-only", action="store_true")
    resend_send_parser.add_argument("--max-attempts", type=int, default=8)
    resend_send_parser.add_argument("--format", choices=["text", "json"], default="text")

    fastmail_init_parser = subparsers.add_parser("fastmail-init-config", help="Write a local Fastmail config template.")
    fastmail_init_parser.add_argument("--config", type=Path, default=default_fastmail_config_path())
    fastmail_init_parser.add_argument("--force", action="store_true")

    fastmail_status_parser = subparsers.add_parser("fastmail-status", help="Show the current Fastmail/JMAP readiness.")
    fastmail_status_parser.add_argument("--config", type=Path, default=default_fastmail_config_path())
    fastmail_status_parser.add_argument("--format", choices=["text", "json"], default="text")

    fastmail_session_parser = subparsers.add_parser("fastmail-session", help="Fetch the current Fastmail JMAP session.")
    fastmail_session_parser.add_argument("--config", type=Path, default=default_fastmail_config_path())
    fastmail_session_parser.add_argument("--format", choices=["text", "json"], default="text")

    fastmail_mailboxes_parser = subparsers.add_parser("fastmail-mailboxes", help="List Fastmail mailboxes via JMAP.")
    fastmail_mailboxes_parser.add_argument("--config", type=Path, default=default_fastmail_config_path())
    fastmail_mailboxes_parser.add_argument("--format", choices=["text", "json"], default="text")

    emma_status_parser = subparsers.add_parser("emma-status", help="Show the current Emma API readiness.")
    emma_status_parser.add_argument("--base-url", default=None)
    emma_status_parser.add_argument("--format", choices=["text", "json"], default="text")

    emma_me_parser = subparsers.add_parser("emma-me", help="Load the authenticated Emma profile tied to the current API key.")
    emma_me_parser.add_argument("--base-url", default=None)
    emma_me_parser.add_argument("--format", choices=["text", "json"], default="text")

    emma_agents_parser = subparsers.add_parser("emma-agents", help="List the Emma agents available to the current API key.")
    emma_agents_parser.add_argument("--base-url", default=None)
    emma_agents_parser.add_argument("--format", choices=["text", "json"], default="text")

    emma_chat_parser = subparsers.add_parser("emma-chat", help="Send a chat turn to Emma or your soulbind via the Emma developer API.")
    emma_chat_parser.add_argument("--base-url", default=None)
    emma_chat_parser.add_argument("--agent", choices=["emma", "soulbind"], default="soulbind")
    emma_chat_parser.add_argument("--mode", default="listen")
    emma_chat_parser.add_argument("--message", required=True)
    emma_chat_parser.add_argument("--format", choices=["text", "json"], default="text")

    x_init_config_parser = subparsers.add_parser("x-init-config", help="Write a local X config template for future account integration.")
    x_init_config_parser.add_argument("--client-config", type=Path, default=default_x_client_path())
    x_init_config_parser.add_argument("--force", action="store_true")

    x_status_parser = subparsers.add_parser("x-status", help="Show the current local X integration readiness.")
    x_status_parser.add_argument("--client-config", type=Path, default=default_x_client_path())
    x_status_parser.add_argument("--token", type=Path, default=default_x_token_path())
    x_status_parser.add_argument("--format", choices=["text", "json"], default="text")

    x_auth_parser = subparsers.add_parser("x-auth", help="Run the X OAuth flow and store a local user token.")
    x_auth_parser.add_argument("--client-config", type=Path, default=default_x_client_path())
    x_auth_parser.add_argument("--token", type=Path, default=default_x_token_path())
    x_auth_parser.add_argument("--timeout-seconds", type=int, default=300)
    x_auth_parser.add_argument("--no-open", action="store_true")
    x_auth_parser.add_argument("--format", choices=["text", "json"], default="text")

    x_refresh_parser = subparsers.add_parser("x-refresh", help="Refresh the stored X user access token.")
    x_refresh_parser.add_argument("--client-config", type=Path, default=default_x_client_path())
    x_refresh_parser.add_argument("--token", type=Path, default=default_x_token_path())
    x_refresh_parser.add_argument("--format", choices=["text", "json"], default="text")

    x_me_parser = subparsers.add_parser("x-me", help="Show the authenticated X account.")
    x_me_parser.add_argument("--client-config", type=Path, default=default_x_client_path())
    x_me_parser.add_argument("--token", type=Path, default=default_x_token_path())
    x_me_parser.add_argument("--format", choices=["text", "json"], default="text")

    x_user_parser = subparsers.add_parser("x-user", help="Look up a public X account by username.")
    x_user_parser.add_argument("--client-config", type=Path, default=default_x_client_path())
    x_user_parser.add_argument("--token", type=Path, default=default_x_token_path())
    x_user_parser.add_argument("--username", required=True)
    x_user_parser.add_argument("--format", choices=["text", "json"], default="text")

    x_posts_parser = subparsers.add_parser("x-posts", help="List recent posts for your account or another X username.")
    x_posts_parser.add_argument("--client-config", type=Path, default=default_x_client_path())
    x_posts_parser.add_argument("--token", type=Path, default=default_x_token_path())
    x_posts_parser.add_argument("--username", default=None)
    x_posts_parser.add_argument("--limit", type=int, default=10)
    x_posts_parser.add_argument("--format", choices=["text", "json"], default="text")

    x_home_parser = subparsers.add_parser("x-home", help="Show the authenticated account's home timeline.")
    x_home_parser.add_argument("--client-config", type=Path, default=default_x_client_path())
    x_home_parser.add_argument("--token", type=Path, default=default_x_token_path())
    x_home_parser.add_argument("--limit", type=int, default=10)
    x_home_parser.add_argument("--format", choices=["text", "json"], default="text")

    x_post_parser = subparsers.add_parser("x-post", help="Publish a new X post from the authenticated account.")
    x_post_parser.add_argument("--client-config", type=Path, default=default_x_client_path())
    x_post_parser.add_argument("--token", type=Path, default=default_x_token_path())
    x_post_parser.add_argument("--text", required=True)
    x_post_parser.add_argument("--format", choices=["text", "json"], default="text")

    x_delete_post_parser = subparsers.add_parser("x-delete-post", help="Delete one of your X posts by id.")
    x_delete_post_parser.add_argument("--client-config", type=Path, default=default_x_client_path())
    x_delete_post_parser.add_argument("--token", type=Path, default=default_x_token_path())
    x_delete_post_parser.add_argument("--id", required=True)
    x_delete_post_parser.add_argument("--format", choices=["text", "json"], default="text")

    x_package_create_parser = subparsers.add_parser("x-package-create", help="Create a local X article/thread package with image briefs.")
    x_package_create_parser.add_argument("--title", required=True)
    x_package_create_parser.add_argument("--angle", default="")
    x_package_create_parser.add_argument("--audience", default="")
    x_package_create_parser.add_argument("--thesis", default="")
    x_package_create_parser.add_argument("--point", action="append", dest="points", default=[])
    x_package_create_parser.add_argument("--cta", default="")
    x_package_create_parser.add_argument("--voice", default="bold, clear, slightly playful")
    x_package_create_parser.add_argument("--visual-style", default="editorial, cinematic, tactile, high-contrast")
    x_package_create_parser.add_argument("--tag", action="append", dest="tags", default=[])
    x_package_create_parser.add_argument("--format", choices=["text", "json"], default="text")

    x_content_parser = subparsers.add_parser("x-content", help="List stored local X content drafts and article packages.")
    x_content_parser.add_argument("--kind", default=None)
    x_content_parser.add_argument("--status", default="all")
    x_content_parser.add_argument("--limit", type=int, default=50)
    x_content_parser.add_argument("--format", choices=["text", "json"], default="text")

    x_content_show_parser = subparsers.add_parser("x-content-show", help="Show one local X content package with its posts and image briefs.")
    x_content_show_parser.add_argument("--id", required=True, type=int)
    x_content_show_parser.add_argument("--format", choices=["text", "json"], default="text")

    x_media_parser = subparsers.add_parser("x-media", help="List local X media briefs and generated assets.")
    x_media_parser.add_argument("--content-id", type=int, default=None)
    x_media_parser.add_argument("--kind", default=None)
    x_media_parser.add_argument("--status", default="all")
    x_media_parser.add_argument("--limit", type=int, default=50)
    x_media_parser.add_argument("--format", choices=["text", "json"], default="text")

    x_generate_image_parser = subparsers.add_parser("x-generate-image", help="Generate an image for a stored X media brief, falling back to OpenAI when xAI is unavailable.")
    x_generate_image_parser.add_argument("--asset-id", required=True, type=int)
    x_generate_image_parser.add_argument("--provider", choices=["auto", "openai", "xai"], default="auto")
    x_generate_image_parser.add_argument("--model", default="")
    x_generate_image_parser.add_argument("--size", default=DEFAULT_OPENAI_IMAGE_SIZE)
    x_generate_image_parser.add_argument("--quality", default=DEFAULT_OPENAI_IMAGE_QUALITY)
    x_generate_image_parser.add_argument("--output-format", choices=["png", "jpeg", "webp"], default=DEFAULT_OPENAI_IMAGE_OUTPUT_FORMAT)
    x_generate_image_parser.add_argument("--background", choices=["auto", "opaque", "transparent"], default=DEFAULT_OPENAI_IMAGE_BACKGROUND)
    x_generate_image_parser.add_argument("--moderation", choices=["auto", "low"], default=DEFAULT_OPENAI_IMAGE_MODERATION)
    x_generate_image_parser.add_argument("--aspect-ratio", default="auto")
    x_generate_image_parser.add_argument("--resolution", default="1k")
    x_generate_image_parser.add_argument("--format", choices=["text", "json"], default="text")

    comms_parser = subparsers.add_parser("comms", help="List stored communications with category and priority metadata.")
    comms_parser.add_argument("--status", choices=["all", "open", "reference", "done"], default="all")
    comms_parser.add_argument("--source", default=None)
    comms_parser.add_argument("--category", default=None)
    comms_parser.add_argument("--limit", type=int, default=50)
    comms_parser.add_argument("--format", choices=["text", "json"], default="text")

    comms_summary_parser = subparsers.add_parser("comms-summary", help="Summarize communications by status, category, and priority.")
    comms_summary_parser.add_argument("--status", choices=["all", "open", "reference", "done"], default="all")
    comms_summary_parser.add_argument("--source", default=None)
    comms_summary_parser.add_argument("--category", default=None)
    comms_summary_parser.add_argument("--format", choices=["text", "json"], default="text")

    extract_profile_context_parser = subparsers.add_parser("extract-profile-context", help="Scan stored communications and build important human-profile context items.")
    extract_profile_context_parser.add_argument("--source", default="gmail")
    extract_profile_context_parser.add_argument("--status", choices=["all", "open", "reference", "done"], default="all")
    extract_profile_context_parser.add_argument("--category", default=None)
    extract_profile_context_parser.add_argument("--limit", type=int, default=None)
    extract_profile_context_parser.add_argument("--keep-existing", action="store_true")

    profile_context_parser = subparsers.add_parser("profile-context", help="List extracted profile-context items.")
    profile_context_parser.add_argument("--subject-key", default=None)
    profile_context_parser.add_argument("--item-type", default=None)
    profile_context_parser.add_argument("--status", default=None)
    profile_context_parser.add_argument("--source", default=None)
    profile_context_parser.add_argument("--limit", type=int, default=100)
    profile_context_parser.add_argument("--format", choices=["text", "json"], default="text")

    profile_context_summary_parser = subparsers.add_parser("profile-context-summary", help="Summarize extracted profile-context items.")
    profile_context_summary_parser.add_argument("--subject-key", default=None)
    profile_context_summary_parser.add_argument("--item-type", default=None)
    profile_context_summary_parser.add_argument("--status", default=None)
    profile_context_summary_parser.add_argument("--source", default=None)
    profile_context_summary_parser.add_argument("--format", choices=["text", "json"], default="text")

    profile_review_next_parser = subparsers.add_parser("profile-review-next", help="Show the highest-priority candidate profile item for review.")
    profile_review_next_parser.add_argument("--subject-key", default=None)
    profile_review_next_parser.add_argument("--item-type", default=None)
    profile_review_next_parser.add_argument("--source", default=None)
    profile_review_next_parser.add_argument("--format", choices=["text", "json"], default="text")

    profile_approve_parser = subparsers.add_parser("profile-approve", help="Approve a candidate profile item and promote it into the canonical profile layer.")
    profile_approve_parser.add_argument("--id", required=True, type=int)
    profile_approve_parser.add_argument("--title", default="")
    profile_approve_parser.add_argument("--record-status", choices=["active", "archived"], default="active")
    profile_approve_parser.add_argument("--notes", default="")
    profile_approve_parser.add_argument("--format", choices=["text", "json"], default="text")

    profile_reject_parser = subparsers.add_parser("profile-reject", help="Reject a candidate profile item.")
    profile_reject_parser.add_argument("--id", required=True, type=int)
    profile_reject_parser.add_argument("--notes", default="")
    profile_reject_parser.add_argument("--format", choices=["text", "json"], default="text")

    profile_merge_parser = subparsers.add_parser("profile-merge", help="Merge a candidate profile item into an existing canonical record.")
    profile_merge_parser.add_argument("--id", required=True, type=int)
    profile_merge_parser.add_argument("--record-id", required=True, type=int)
    profile_merge_parser.add_argument("--notes", default="")
    profile_merge_parser.add_argument("--format", choices=["text", "json"], default="text")

    profile_records_parser = subparsers.add_parser("profile-records", help="List canonical profile records.")
    profile_records_parser.add_argument("--subject-key", default=None)
    profile_records_parser.add_argument("--item-type", default=None)
    profile_records_parser.add_argument("--status", default="all")
    profile_records_parser.add_argument("--limit", type=int, default=100)
    profile_records_parser.add_argument("--format", choices=["text", "json"], default="text")

    profile_record_show_parser = subparsers.add_parser("profile-record-show", help="Show one canonical profile record with linked items and attachments.")
    profile_record_show_parser.add_argument("--id", required=True, type=int)
    profile_record_show_parser.add_argument("--format", choices=["text", "json"], default="text")

    profile_record_summary_parser = subparsers.add_parser("profile-record-summary", help="Summarize canonical profile records.")
    profile_record_summary_parser.add_argument("--subject-key", default=None)
    profile_record_summary_parser.add_argument("--item-type", default=None)
    profile_record_summary_parser.add_argument("--status", default="all")
    profile_record_summary_parser.add_argument("--format", choices=["text", "json"], default="text")

    profile_alerts_parser = subparsers.add_parser("profile-alerts", help="Show operational alerts for the canonical profile layer.")
    profile_alerts_parser.add_argument("--subject-key", default=None)
    profile_alerts_parser.add_argument("--item-type", default=None)
    profile_alerts_parser.add_argument("--status", default="active")
    profile_alerts_parser.add_argument("--limit", type=int, default=50)
    profile_alerts_parser.add_argument("--format", choices=["text", "json"], default="text")

    mail_alerts_parser = subparsers.add_parser("mail-alerts", help="Show active local mail flow alerts.")
    mail_alerts_parser.add_argument("--db", type=Path, default=store.default_db_path())
    mail_alerts_parser.add_argument("--source", default=None)
    mail_alerts_parser.add_argument("--status", default="active")
    mail_alerts_parser.add_argument("--limit", type=int, default=50)
    mail_alerts_parser.add_argument("--format", choices=["text", "json"], default="text")

    ingest_profile_attachments_parser = subparsers.add_parser("ingest-profile-attachments", help="Download and extract Gmail attachments for current profile candidates or broader sensitive Gmail records.")
    ingest_profile_attachments_parser.add_argument("--credentials", type=Path, default=default_credentials_path())
    ingest_profile_attachments_parser.add_argument("--token", type=Path, default=default_token_path())
    ingest_profile_attachments_parser.add_argument("--subject-key", default=None)
    ingest_profile_attachments_parser.add_argument("--item-type", default=None)
    ingest_profile_attachments_parser.add_argument("--status", default="candidate")
    ingest_profile_attachments_parser.add_argument("--scope", choices=["profile", "sensitive"], default="profile")
    ingest_profile_attachments_parser.add_argument("--limit", type=int, default=50)
    ingest_profile_attachments_parser.add_argument("--include-inline", action="store_true")
    ingest_profile_attachments_parser.add_argument("--force", action="store_true")

    backfill_profile_attachments_parser = subparsers.add_parser("backfill-profile-attachments", help="Walk the stored Gmail corpus and ingest sensitive/profile attachments with a resumable local cursor.")
    backfill_profile_attachments_parser.add_argument("--credentials", type=Path, default=default_credentials_path())
    backfill_profile_attachments_parser.add_argument("--token", type=Path, default=default_token_path())
    backfill_profile_attachments_parser.add_argument("--scope", choices=["sensitive"], default="sensitive")
    backfill_profile_attachments_parser.add_argument("--max-results", type=int, default=100)
    backfill_profile_attachments_parser.add_argument("--max-runs", type=int, default=20)
    backfill_profile_attachments_parser.add_argument("--reset-cursor", action="store_true")
    backfill_profile_attachments_parser.add_argument("--include-inline", action="store_true")
    backfill_profile_attachments_parser.add_argument("--force", action="store_true")

    attachments_parser = subparsers.add_parser("attachments", help="List downloaded communication attachments and extraction results.")
    attachments_parser.add_argument("--communication-id", type=int, default=None)
    attachments_parser.add_argument("--status", default=None)
    attachments_parser.add_argument("--source", default=None)
    attachments_parser.add_argument("--limit", type=int, default=100)
    attachments_parser.add_argument("--format", choices=["text", "json"], default="text")

    attachment_summary_parser = subparsers.add_parser("attachment-summary", help="Summarize downloaded communication attachments.")
    attachment_summary_parser.add_argument("--status", default=None)
    attachment_summary_parser.add_argument("--source", default=None)
    attachment_summary_parser.add_argument("--format", choices=["text", "json"], default="text")

    profile_review_set_parser = subparsers.add_parser("profile-review-set", help="Update review status for a profile-context item.")
    profile_review_set_parser.add_argument("--id", required=True, type=int)
    profile_review_set_parser.add_argument("--status", choices=["candidate", "approved", "rejected"], required=True)
    profile_review_set_parser.add_argument("--notes", default="")

    reclassify_gmail_parser = subparsers.add_parser("reclassify-gmail", help="Re-run the Gmail classifier on stored local Gmail communications.")
    reclassify_gmail_parser.add_argument("--status", choices=["all", "open", "reference", "done"], default="all")
    reclassify_gmail_parser.add_argument("--category", default=None)
    reclassify_gmail_parser.add_argument("--limit", type=int, default=None)
    reclassify_gmail_parser.add_argument("--rewrite-status", action="store_true")

    add_org = subparsers.add_parser("add-org", help="Add an organization.")
    add_org.add_argument("--name", required=True)
    add_org.add_argument("--category", default="general")
    add_org.add_argument("--notes", default="")

    add_event = subparsers.add_parser("add-event", help="Add a calendar event.")
    add_event.add_argument("--title", required=True)
    add_event.add_argument("--start", required=True, type=_parse_datetime_or_date)
    add_event.add_argument("--end", required=True, type=_parse_datetime_or_date)
    add_event.add_argument("--organization", default=None)
    add_event.add_argument("--location", default="")
    add_event.add_argument("--kind", default="event")
    add_event.add_argument("--status", default="confirmed")
    add_event.add_argument("--source", default="manual")
    add_event.add_argument("--notes", default="")
    add_event.add_argument("--all-day", action="store_true")

    add_comm = subparsers.add_parser("add-comm", help="Add a communication follow-up.")
    add_comm.add_argument("--subject", required=True)
    add_comm.add_argument("--channel", required=True)
    add_comm.add_argument("--person", default="")
    add_comm.add_argument("--organization", default=None)
    add_comm.add_argument("--happened-at", type=_parse_datetime_or_date, default=datetime.now())
    add_comm.add_argument("--follow-up-at", type=_parse_datetime_or_date, default=None)
    add_comm.add_argument("--notes", default="")

    add_item = subparsers.add_parser("add-item", help="Add a personal or professional list item.")
    add_item.add_argument("--list", dest="list_name", choices=store.LIST_ITEM_NAMES, required=True)
    add_item.add_argument("--title", required=True)
    add_item.add_argument("--notes", default="")

    list_items = subparsers.add_parser("list-items", help="Show personal and professional list items.")
    list_items.add_argument("--list", dest="list_name", choices=[*store.LIST_ITEM_NAMES, "all"], default="all")
    list_items.add_argument("--status", choices=[*store.LIST_ITEM_STATUSES, "all"], default="open")
    list_items.add_argument("--limit", type=int, default=200)
    list_items.add_argument("--format", choices=["text", "json"], default="text")

    done_item = subparsers.add_parser("done-item", help="Mark a list item done.")
    done_item.add_argument("--id", required=True, type=int)

    add_routine = subparsers.add_parser("add-routine", help="Add a daily or weekly routine.")
    add_routine.add_argument("--name", required=True)
    add_routine.add_argument("--cadence", choices=["daily", "weekly"], required=True)
    add_routine.add_argument("--day", default=None)
    add_routine.add_argument("--start-time", required=True, type=_parse_time)
    add_routine.add_argument("--duration", type=int, default=30)
    add_routine.add_argument("--notes", default="")

    done_comm = subparsers.add_parser("done-comm", help="Mark a communication follow-up done.")
    done_comm.add_argument("--id", required=True, type=int)

    # ── social ──────────────────────────────────────────────────────
    social_auth_parser = subparsers.add_parser(
        "social-auth",
        help="Authenticate with a social platform (opens a browser for manual login).",
    )
    social_auth_parser.add_argument(
        "platform",
        choices=social_available_platforms(),
        help="Platform to authenticate with.",
    )

    social_status_parser = subparsers.add_parser(
        "social-status",
        help="Show the status of stored social platform sessions.",
    )
    social_status_parser.add_argument(
        "--platform",
        choices=social_available_platforms(),
        default=None,
        help="Check a specific platform (default: all).",
    )
    social_status_parser.add_argument("--format", choices=["text", "json"], default="text")

    social_logout_parser = subparsers.add_parser(
        "social-logout",
        help="Clear the stored browser session for a social platform.",
    )
    social_logout_parser.add_argument(
        "platform",
        choices=social_available_platforms(),
        help="Platform to log out of.",
    )

    social_post_parser = subparsers.add_parser(
        "social-post",
        help="Publish a post to one or more social platforms via browser automation.",
    )
    social_post_parser.add_argument(
        "--platforms",
        required=True,
        help="Comma-separated list of platforms (e.g. linkedin,facebook).",
    )
    social_post_parser.add_argument(
        "--text",
        default=None,
        help="Default post text (used for any platform without a specific override).",
    )
    social_post_parser.add_argument(
        "--linkedin-text",
        default=None,
        help="Override text for LinkedIn.",
    )
    social_post_parser.add_argument(
        "--facebook-text",
        default=None,
        help="Override text for Facebook.",
    )
    social_post_parser.add_argument(
        "--image",
        default=None,
        help="Path to an image file to attach.",
    )
    social_post_parser.add_argument(
        "--visible",
        action="store_true",
        default=False,
        help="Run the browser in headed (visible) mode for debugging.",
    )

    return parser


def _render_sync_summary(title: str, payload: dict) -> str:
    lines = [title]
    for key, value in payload.items():
        lines.append(f"- {key}: {value}")
    return "\n".join(lines)


def _render_cmail_drafts_text(records: list[dict]) -> str:
    if not records:
        return "No CMAIL drafts stored."
    lines = [f"{len(records)} CMAIL draft{'s' if len(records) != 1 else ''}"]
    for record in records:
        label = str(record.get("label") or "(untitled draft)")
        to_value = str(record.get("to") or "no recipient yet")
        updated_at = str(record.get("updated_at") or "")
        lines.append(f"- [{record.get('id')}] {label}")
        lines.append(f"  to: {to_value}")
        if updated_at:
            lines.append(f"  updated: {updated_at}")
    return "\n".join(lines)


def _render_cmail_draft_saved_text(record: dict) -> str:
    label = str(record.get("label") or "(untitled draft)")
    to_value = str(record.get("to") or "no recipient yet")
    updated_at = str(record.get("updated_at") or "")
    lines = [
        "CMAIL draft saved",
        f"- id: {record.get('id')}",
        f"- label: {label}",
        f"- to: {to_value}",
    ]
    if updated_at:
        lines.append(f"- updated: {updated_at}")
    return "\n".join(lines)


def _list_item_record(row) -> dict:
    return {
        "id": int(row["id"]),
        "list_name": str(row["list_name"]),
        "title": str(row["title"]),
        "notes": str(row["notes"] or ""),
        "status": str(row["status"]),
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
        "completed_at": str(row["completed_at"] or ""),
    }


def _render_list_items_text(records: list[dict]) -> str:
    if not records:
        return "No list items stored."
    lines = ["Life Ops lists"]
    for list_name in store.LIST_ITEM_NAMES:
        bucket = [record for record in records if record["list_name"] == list_name]
        lines.append(f"")
        lines.append(f"{list_name.title()}")
        if not bucket:
            lines.append("- none")
            continue
        for record in bucket:
            marker = "x" if record["status"] == "done" else " "
            lines.append(f"- [{record['id']}] [{marker}] {record['title']}")
            if record["notes"]:
                lines.append(f"  notes: {record['notes']}")
    return "\n".join(lines)


def _render_vault_status_text(payload: dict) -> str:
    return "\n".join(
        [
            "Vault status",
            f"- secret_name: {payload['secret_name']}",
            f"- present: {payload['present']}",
            f"- length_bytes: {payload['length_bytes']}",
        ]
    )


def _render_backup_list_text(records: list[dict]) -> str:
    if not records:
        return "No encrypted backups found."
    lines = ["Encrypted backups"]
    for record in records:
        state = "valid" if record.get("valid", True) else "invalid"
        lines.append(
            f"- {record.get('backup_id')} [{state}] "
            f"created_at={record.get('created_at') or 'unknown'} "
            f"size={record.get('compressed_bytes') or 0}"
        )
    return "\n".join(lines)


def _render_backup_status_text(payload: dict) -> str:
    lines = ["Backup status"]
    lines.append(f"- backup_root: {payload['backup_root']}")
    lines.append(f"- count: {payload['count']}")
    master_key = payload.get("master_key") or {}
    lines.append(f"- master_key_present: {bool(master_key.get('present'))}")
    latest = payload.get("latest")
    if latest:
        lines.append(
            f"- latest: {latest.get('backup_id')} created_at={latest.get('created_at') or 'unknown'}"
        )
    else:
        lines.append("- latest: none")
    return "\n".join(lines)


def _render_trace_export(records: list[dict], output_format: str) -> str:
    if output_format == "json":
        return json.dumps(records, indent=2)
    return tracing.render_trace_records_jsonl(records)


def _load_json_field(value: str, fallback):
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def _iso_from_timestamp(raw_value: Optional[str]) -> Optional[str]:
    if not raw_value:
        return None
    try:
        return datetime.fromtimestamp(int(raw_value)).isoformat(timespec="seconds")
    except (TypeError, ValueError, OSError):
        return None


def _latest_trace_run(connection, *, trace_type: str, status: Optional[str] = None):
    clauses = ["trace_type = ?"]
    params: list = [trace_type]
    if status:
        clauses.append("status = ?")
        params.append(status)

    where_clause = " AND ".join(clauses)
    return connection.execute(
        f"""
        SELECT id, trace_type, status, metadata_json, summary_json, started_at, finished_at
        FROM trace_runs
        WHERE {where_clause}
        ORDER BY started_at DESC
        LIMIT 1
        """,
        params,
    ).fetchone()


def _gmail_heartbeat(connection) -> dict:
    corpus_summary = store.summarize_communications(connection, source="gmail", status="all")
    running_rows = connection.execute(
        """
        SELECT id, trace_type, started_at
        FROM trace_runs
        WHERE status = 'running'
          AND trace_type IN ('gmail_sync', 'gmail_backfill', 'gmail_backfill_exhaustive', 'gmail_corpus_sync', 'gmail_reclassify')
        ORDER BY started_at DESC
        """
    ).fetchall()
    latest_backfill = _latest_trace_run(connection, trace_type="gmail_backfill", status="completed")
    latest_reclassify = _latest_trace_run(connection, trace_type="gmail_reclassify", status="completed")
    latest_corpus = _latest_trace_run(connection, trace_type="gmail_corpus_sync")

    heartbeat = {
        "user_email": store.get_sync_state(connection, "gmail:user_email"),
        "last_sync_at": store.get_sync_state(connection, "gmail:last_sync_at"),
        "backfill_query": store.get_sync_state(connection, "gmail_backfill:query"),
        "backfill_next_before_ts": store.get_sync_state(connection, "gmail_backfill:next_before_ts"),
        "backfill_cursor_at": _iso_from_timestamp(store.get_sync_state(connection, "gmail_backfill:next_before_ts")),
        "backfill_last_sync_at": store.get_sync_state(connection, "gmail_backfill:last_sync_at"),
        "corpus_total": corpus_summary["total"],
        "corpus_by_status": corpus_summary["by_status"],
        "top_categories": corpus_summary["by_category"][:10],
        "running_runs": [
            {
                "run_id": str(row["id"]),
                "trace_type": str(row["trace_type"]),
                "started_at": str(row["started_at"]),
            }
            for row in running_rows
        ],
        "latest_completed_backfill": None,
        "latest_reclassify": None,
        "latest_corpus_run": None,
    }

    if latest_backfill:
        heartbeat["latest_completed_backfill"] = {
            "run_id": str(latest_backfill["id"]),
            "started_at": str(latest_backfill["started_at"]),
            "finished_at": str(latest_backfill["finished_at"]),
            "summary": _load_json_field(str(latest_backfill["summary_json"] or "{}"), {}),
        }
    if latest_reclassify:
        heartbeat["latest_reclassify"] = {
            "run_id": str(latest_reclassify["id"]),
            "started_at": str(latest_reclassify["started_at"]),
            "finished_at": str(latest_reclassify["finished_at"]),
            "summary": _load_json_field(str(latest_reclassify["summary_json"] or "{}"), {}),
        }
    if latest_corpus:
        heartbeat["latest_corpus_run"] = {
            "run_id": str(latest_corpus["id"]),
            "status": str(latest_corpus["status"]),
            "started_at": str(latest_corpus["started_at"]),
            "finished_at": str(latest_corpus["finished_at"]) if latest_corpus["finished_at"] else None,
            "summary": _load_json_field(str(latest_corpus["summary_json"] or "{}"), {}),
        }

    return heartbeat


def _render_gmail_heartbeat_text(heartbeat: dict) -> str:
    lines = ["Gmail heartbeat"]
    lines.append(f"- user_email: {heartbeat.get('user_email') or 'unknown'}")
    lines.append(f"- corpus_total: {heartbeat['corpus_total']}")
    if heartbeat["corpus_by_status"]:
        lines.append(
            "- corpus_by_status: "
            + ", ".join(f"{row['status']}={row['count']}" for row in heartbeat["corpus_by_status"])
        )
    else:
        lines.append("- corpus_by_status: none")
    if heartbeat["top_categories"]:
        lines.append(
            "- top_categories: "
            + ", ".join(f"{row['category'] or 'uncategorized'}={row['count']}" for row in heartbeat["top_categories"])
        )
    else:
        lines.append("- top_categories: none")
    lines.append(f"- backfill_query: {heartbeat.get('backfill_query') or 'none'}")
    lines.append(f"- backfill_cursor_at: {heartbeat.get('backfill_cursor_at') or 'unknown'}")
    lines.append(f"- backfill_last_sync_at: {heartbeat.get('backfill_last_sync_at') or 'unknown'}")
    if heartbeat["running_runs"]:
        lines.append("- running_runs:")
        for run in heartbeat["running_runs"]:
            lines.append(f"  - {run['run_id']} [{run['trace_type']}] since {run['started_at']}")
    else:
        lines.append("- running_runs: none")
    latest_backfill = heartbeat.get("latest_completed_backfill")
    if latest_backfill:
        summary = latest_backfill["summary"]
        lines.append(
            "- latest_completed_backfill: "
            f"{latest_backfill['run_id']} messages_scanned={summary.get('messages_scanned')} "
            f"threads_kept={summary.get('threads_kept')} next_before_ts={summary.get('next_before_ts')}"
        )
    latest_reclassify = heartbeat.get("latest_reclassify")
    if latest_reclassify:
        summary = latest_reclassify["summary"]
        lines.append(
            "- latest_reclassify: "
            f"{latest_reclassify['run_id']} processed={summary.get('processed')} "
            f"changed_primary_category={summary.get('changed_primary_category')}"
        )
    latest_corpus = heartbeat.get("latest_corpus_run")
    if latest_corpus:
        lines.append(
            "- latest_corpus_run: "
            f"{latest_corpus['run_id']} [{latest_corpus['status']}] started_at={latest_corpus['started_at']}"
        )
    return "\n".join(lines)


def _profile_attachment_heartbeat(connection) -> dict:
    attachment_summary = store.summarize_communication_attachments(connection, source="gmail")
    profile_summary = store.summarize_profile_context(connection, source="gmail")
    running_rows = connection.execute(
        """
        SELECT id, trace_type, started_at
        FROM trace_runs
        WHERE status = 'running'
          AND trace_type IN ('profile_attachment_ingest', 'profile_attachment_backfill', 'profile_attachment_backfill_exhaustive')
        ORDER BY started_at DESC
        """
    ).fetchall()
    latest_backfill = _latest_trace_run(connection, trace_type="profile_attachment_backfill", status="completed")
    latest_exhaustive = _latest_trace_run(connection, trace_type="profile_attachment_backfill_exhaustive")
    namespace = DEFAULT_PROFILE_ATTACHMENT_BACKFILL_NAMESPACE
    next_happened_at = store.get_sync_state(connection, f"{namespace}:next_happened_at")
    next_communication_id = store.get_sync_state(connection, f"{namespace}:next_communication_id")

    return {
        "scope": store.get_sync_state(connection, f"{namespace}:scope"),
        "last_sync_at": store.get_sync_state(connection, f"{namespace}:last_sync_at"),
        "next_happened_at": next_happened_at or None,
        "next_communication_id": next_communication_id or None,
        "attachment_total": attachment_summary["total"],
        "attachment_by_status": attachment_summary["by_status"],
        "attachment_by_method": attachment_summary["by_method"],
        "profile_context_total": profile_summary["total"],
        "running_runs": [
            {
                "run_id": str(row["id"]),
                "trace_type": str(row["trace_type"]),
                "started_at": str(row["started_at"]),
            }
            for row in running_rows
        ],
        "latest_completed_backfill": None if not latest_backfill else {
            "run_id": str(latest_backfill["id"]),
            "started_at": str(latest_backfill["started_at"]),
            "finished_at": str(latest_backfill["finished_at"]),
            "summary": _load_json_field(str(latest_backfill["summary_json"] or "{}"), {}),
        },
        "latest_exhaustive_run": None if not latest_exhaustive else {
            "run_id": str(latest_exhaustive["id"]),
            "status": str(latest_exhaustive["status"]),
            "started_at": str(latest_exhaustive["started_at"]),
            "finished_at": str(latest_exhaustive["finished_at"]) if latest_exhaustive["finished_at"] else None,
            "summary": _load_json_field(str(latest_exhaustive["summary_json"] or "{}"), {}),
        },
    }


def _render_profile_attachment_heartbeat_text(heartbeat: dict) -> str:
    lines = ["Profile attachment heartbeat"]
    lines.append(f"- scope: {heartbeat.get('scope') or 'unknown'}")
    lines.append(f"- attachment_total: {heartbeat['attachment_total']}")
    if heartbeat["attachment_by_status"]:
        lines.append(
            "- attachment_by_status: "
            + ", ".join(f"{row['ingest_status']}={row['count']}" for row in heartbeat["attachment_by_status"])
        )
    else:
        lines.append("- attachment_by_status: none")
    if heartbeat["attachment_by_method"]:
        lines.append(
            "- attachment_by_method: "
            + ", ".join(f"{row['extraction_method'] or 'none'}={row['count']}" for row in heartbeat["attachment_by_method"])
        )
    else:
        lines.append("- attachment_by_method: none")
    lines.append(f"- profile_context_total: {heartbeat['profile_context_total']}")
    lines.append(f"- next_happened_at: {heartbeat.get('next_happened_at') or 'exhausted_or_not_started'}")
    lines.append(f"- next_communication_id: {heartbeat.get('next_communication_id') or 'exhausted_or_not_started'}")
    lines.append(f"- last_sync_at: {heartbeat.get('last_sync_at') or 'unknown'}")
    if heartbeat["running_runs"]:
        lines.append("- running_runs:")
        for run in heartbeat["running_runs"]:
            lines.append(f"  - {run['run_id']} [{run['trace_type']}] since {run['started_at']}")
    else:
        lines.append("- running_runs: none")
    latest_backfill = heartbeat.get("latest_completed_backfill")
    if latest_backfill:
        summary = latest_backfill["summary"]
        lines.append(
            "- latest_completed_backfill: "
            f"{latest_backfill['run_id']} communications_scanned={summary.get('communications_scanned')} "
            f"attachments_saved={summary.get('attachments_saved')} next_happened_at={summary.get('next_happened_at')}"
        )
    latest_exhaustive = heartbeat.get("latest_exhaustive_run")
    if latest_exhaustive:
        summary = latest_exhaustive["summary"]
        lines.append(
            "- latest_exhaustive_run: "
            f"{latest_exhaustive['run_id']} status={latest_exhaustive['status']} "
            f"runs_completed={summary.get('runs_completed')} stop_reason={summary.get('stop_reason')}"
        )
    return "\n".join(lines)


def _render_x_status_text(status: dict) -> str:
    lines = ["X integration status"]
    lines.append(f"- client_config_present: {status['client_config_present']}")
    lines.append(f"- token_present: {status['token_present']}")
    lines.append(f"- has_client_id: {status['has_client_id']}")
    lines.append(f"- has_client_secret: {status['has_client_secret']}")
    lines.append(f"- has_bearer_token: {status['has_bearer_token']}")
    lines.append(f"- has_access_token: {status['has_access_token']}")
    lines.append(f"- has_refresh_token: {status['has_refresh_token']}")
    lines.append(f"- token_expired: {status['token_expired']}")
    lines.append(f"- ready_for_public_read: {status['ready_for_public_read']}")
    lines.append(f"- ready_for_user_actions: {status['ready_for_user_actions']}")
    lines.append(f"- ready_for_post_write: {status['ready_for_post_write']}")
    if status["scopes"]:
        lines.append("- scopes: " + ", ".join(status["scopes"]))
    else:
        lines.append("- scopes: none")
    if status["missing_required_scopes"]:
        lines.append("- missing_required_scopes: " + ", ".join(status["missing_required_scopes"]))
    else:
        lines.append("- missing_required_scopes: none")
    if status["next_steps"]:
        lines.append("- next_steps:")
        for step in status["next_steps"]:
            lines.append(f"  - {step}")
    else:
        lines.append("- next_steps: none")
    me = status.get("me") or {}
    if me:
        handle = str(me.get("username") or "").strip()
        label = f"@{handle}" if handle else str(me.get("name") or me.get("id") or "unknown")
        lines.append(f"- authenticated_account: {label}")
    return "\n".join(lines)


def _render_emma_status_text(status: dict) -> str:
    lines = ["Emma integration status"]
    lines.append(f"- base_url: {status['base_url']}")
    lines.append(f"- api_key_present: {status['api_key_present']}")
    lines.append(f"- default_agent: {status['default_agent']}")
    lines.append(f"- default_mode: {status['default_mode']}")
    lines.append(f"- ready: {status['ready']}")
    if status["next_steps"]:
        lines.append("- next_steps:")
        for step in status["next_steps"]:
            lines.append(f"  - {step}")
    else:
        lines.append("- next_steps: none")
    return "\n".join(lines)


def _render_cloudflare_mail_status_text(status: dict) -> str:
    lines = ["Cloudflare mail status"]
    lines.append(f"- config_present: {status['config_present']}")
    lines.append(f"- config_path: {status['config_path']}")
    lines.append(f"- zone_name: {status.get('zone_name') or 'not_set'}")
    lines.append(f"- route_address: {status.get('route_address') or 'not_set'}")
    lines.append(f"- route_full_address: {status.get('route_full_address') or 'not_set'}")
    lines.append(f"- forward_to: {status.get('forward_to') or 'disabled'}")
    lines.append(f"- forwarding_enabled: {status.get('forwarding_enabled', False)}")
    lines.append(f"- worker_name: {status.get('worker_name') or 'not_set'}")
    lines.append(f"- worker_public_url: {status.get('worker_public_url') or 'not_set'}")
    lines.append(f"- ingest_secret_env: {status['ingest_secret_env']}")
    lines.append(f"- ingest_secret_present: {status['ingest_secret_present']}")
    lines.append(f"- cloud_backup_mode: {status.get('cloud_backup_mode') or 'unknown'}")
    lines.append(f"- ready_for_worker: {status['ready_for_worker']}")
    lines.append(f"- ready_for_local_sync: {status.get('ready_for_local_sync', False)}")
    if status["next_steps"]:
        lines.append("- next_steps:")
        for step in status["next_steps"]:
            lines.append(f"  - {step}")
    else:
        lines.append("- next_steps: none")
    return "\n".join(lines)


def _render_cloudflare_mail_queue_status_text(status: dict) -> str:
    lines = ["Cloudflare mail queue status"]
    lines.append(f"- worker_public_url: {status.get('worker_public_url') or 'not_set'}")
    lines.append(f"- route_full_address: {status.get('route_full_address') or 'not_set'}")
    lines.append(f"- forward_to: {status.get('forward_to') or 'disabled'}")
    lines.append(f"- forwarding_enabled: {status.get('forwarding_enabled', False)}")
    lines.append(f"- pending_count: {status.get('pending_count', 0)}")
    lines.append(f"- total_stored: {status.get('total_stored', 0)}")
    lines.append(f"- total_acknowledged: {status.get('total_acknowledged', 0)}")
    lines.append(f"- cloud_backup_mode: {status.get('cloud_backup_mode') or 'unknown'}")
    return "\n".join(lines)


def _render_cloudflare_mail_sync_text(result: dict) -> str:
    lines = ["Cloudflare mail sync"]
    lines.append(f"- route_full_address: {result.get('route_full_address') or 'not_set'}")
    lines.append(f"- worker_public_url: {result.get('worker_public_url') or 'not_set'}")
    lines.append(f"- forward_to: {result.get('forward_to') or 'disabled'}")
    lines.append(f"- forwarding_enabled: {result.get('forwarding_enabled', False)}")
    lines.append(f"- skipped: {result.get('skipped', False)}")
    if result.get("skip_reason"):
        lines.append(f"- skip_reason: {result.get('skip_reason')}")
    lines.append(f"- pulled_count: {result.get('pulled_count', 0)}")
    lines.append(f"- ingested_count: {result.get('ingested_count', 0)}")
    lines.append(f"- acked_count: {result.get('acked_count', 0)}")
    lines.append(f"- failed_count: {result.get('failed_count', 0)}")
    lines.append(f"- pending_count: {result.get('pending_count', 0)}")
    lines.append(f"- total_stored: {result.get('total_stored', 0)}")
    lines.append(f"- total_acknowledged: {result.get('total_acknowledged', 0)}")
    ingested = result.get("ingested") or []
    if ingested:
        lines.append("- ingested:")
        for row in ingested[:10]:
            lines.append(
                "  - "
                + f"queue_id={row.get('queue_id')} "
                + f"communication_id={row.get('communication_id')} "
                + f"subject={row.get('subject')}"
            )
    errors = result.get("errors") or []
    if errors:
        lines.append("- errors:")
        for row in errors[:10]:
            lines.append(f"  - id={row.get('id')} error={row.get('error')}")
    outbound = result.get("outbound") or {}
    if outbound:
        lines.append("- outbound:")
        lines.append(f"  - processed_count: {outbound.get('processed_count', 0)}")
        lines.append(f"  - failed_count: {outbound.get('failed_count', 0)}")
    return "\n".join(lines)


def _render_mail_ingest_status_text(status: dict) -> str:
    lines = ["Mail ingest status"]
    lines.append(f"- secret_name: {status['secret_name']}")
    lines.append(f"- secret_present: {status['secret_present']}")
    lines.append(f"- default_host: {status['default_host']}")
    lines.append(f"- default_port: {status['default_port']}")
    lines.append(f"- default_path: {status['default_path']}")
    lines.append(f"- db_path: {status['db_path']}")
    lines.append(f"- ready: {status['ready']}")
    if status["next_steps"]:
        lines.append("- next_steps:")
        for step in status["next_steps"]:
            lines.append(f"  - {step}")
    else:
        lines.append("- next_steps: none")
    return "\n".join(lines)


def _render_resend_status_text(status: dict) -> str:
    lines = ["Resend status"]
    lines.append(f"- config_present: {status['config_present']}")
    lines.append(f"- config_path: {status['config_path']}")
    lines.append(f"- api_key_env: {status['api_key_env']}")
    lines.append(f"- api_key_present: {status['api_key_present']}")
    lines.append(f"- default_from: {status.get('default_from') or 'not_set'}")
    lines.append(f"- default_reply_to: {status.get('default_reply_to') or 'not_set'}")
    lines.append(f"- default_signature_text_present: {status.get('default_signature_text_present', False)}")
    lines.append(f"- default_signature_html_present: {status.get('default_signature_html_present', False)}")
    lines.append(f"- sender_domain: {status.get('sender_domain') or 'not_set'}")
    lines.append(f"- sender_domain_ready: {status.get('sender_domain_ready', False)}")
    lines.append(f"- ready: {status['ready']}")
    domains = status.get("domains") or {}
    if domains:
        names = domains.get("names") or []
        lines.append(f"- domains_count: {domains.get('count', len(names))}")
        lines.append("- domains: " + (", ".join(names) if names else "none"))
    if status.get("error"):
        lines.append(f"- error: {status['error']}")
    if status["next_steps"]:
        lines.append("- next_steps:")
        for step in status["next_steps"]:
            lines.append(f"  - {step}")
    else:
        lines.append("- next_steps: none")
    return "\n".join(lines)


def _render_resend_signature_text(payload: dict) -> str:
    lines = ["Resend default signature"]
    lines.append(f"- config_path: {payload['config_path']}")
    lines.append(f"- default_signature_text_present: {payload.get('default_signature_text_present', False)}")
    lines.append(f"- default_signature_html_present: {payload.get('default_signature_html_present', False)}")
    text_value = str(payload.get("default_signature_text") or "").strip()
    html_value = str(payload.get("default_signature_html") or "").strip()
    lines.append("- text:")
    lines.append(text_value if text_value else "(empty)")
    lines.append("- html:")
    lines.append(html_value if html_value else "(empty)")
    return "\n".join(lines)


def _render_resend_domains_text(payload: dict) -> str:
    domains = payload.get("data") or []
    if not domains:
        return "No Resend domains found."

    lines = ["Resend domains"]
    for domain in domains:
        label = str(domain.get("name") or "unknown")
        status = str(domain.get("status") or "unknown")
        region = str(domain.get("region") or "unknown")
        lines.append(f"- {label} [status={status}, region={region}]")
    return "\n".join(lines)


def _render_resend_queue_status_text(payload: dict) -> str:
    lines = ["Resend queue status"]
    lines.append(f"- db_path: {payload['db_path']}")
    lines.append(f"- queue_count: {payload.get('queue_count', 0)}")
    lines.append(f"- retained_count: {payload.get('retained_count', 0)}")
    lines.append(f"- due_count: {payload.get('due_count', 0)}")
    counts = payload.get("counts") or {}
    if counts:
        lines.append("- counts: " + ", ".join(f"{key}={value}" for key, value in sorted(counts.items())))
    else:
        lines.append("- counts: none")
    lines.append(f"- active_alert_count: {payload.get('active_alert_count', 0)}")
    items = payload.get("items") or []
    if items:
        lines.append("- items:")
        for row in items[:10]:
            lines.append(
                "  - "
                + f"queue_id={row.get('queue_id')} "
                + f"status={row.get('status')} "
                + f"attempts={row.get('attempt_count')}/{row.get('max_attempts')} "
                + f"subject={row.get('subject')}"
            )
    alerts = payload.get("active_alerts") or []
    if alerts:
        lines.append("- active_alerts:")
        for row in alerts[:10]:
            lines.append(f"  - severity={row.get('severity')} title={row.get('title')} message={row.get('message')}")
    return "\n".join(lines)


def _render_resend_queue_process_text(payload: dict) -> str:
    lines = ["Resend queue processing"]
    lines.append(f"- db_path: {payload['db_path']}")
    lines.append(f"- processed_count: {payload.get('processed_count', 0)}")
    lines.append(f"- failed_count: {payload.get('failed_count', 0)}")
    lines.append(f"- last_processed_at: {payload.get('last_processed_at') or 'unknown'}")
    processed = payload.get("processed") or []
    if processed:
        lines.append("- sent:")
        for row in processed[:10]:
            lines.append(
                "  - "
                + f"queue_id={row.get('queue_id')} "
                + f"communication_id={row.get('communication_id')} "
                + f"provider_message_id={row.get('provider_message_id')}"
            )
    failures = payload.get("failures") or []
    if failures:
        lines.append("- failures:")
        for row in failures[:10]:
            lines.append(
                "  - "
                + f"queue_id={row.get('queue_id')} "
                + f"status={row.get('status')} "
                + f"next_attempt_at={row.get('next_attempt_at')} "
                + f"error={row.get('error')}"
            )
    return "\n".join(lines)


def _render_mail_alerts_text(alerts: list[dict]) -> str:
    if not alerts:
        return "No mail alerts found."
    lines = ["Mail alerts"]
    for row in alerts:
        lines.append(
            f"- [{row.get('severity')}] {row.get('title')} "
            f"(source={row.get('source')}, status={row.get('status')})"
        )
        message = str(row.get("message") or "").strip()
        if message:
            lines.append(f"  {message}")
    return "\n".join(lines)


def _render_fastmail_status_text(status: dict) -> str:
    lines = ["Fastmail integration status"]
    lines.append(f"- config_present: {status['config_present']}")
    lines.append(f"- config_path: {status['config_path']}")
    lines.append(f"- account_email: {status.get('account_email') or 'not_set'}")
    lines.append(f"- api_token_env: {status['api_token_env']}")
    lines.append(f"- api_token_present: {status['api_token_present']}")
    lines.append(f"- ready: {status['ready']}")
    session = status.get("session") or {}
    if session:
        lines.append(f"- username: {session.get('username') or 'unknown'}")
        lines.append(f"- mail_account_id: {session.get('mail_account_id') or 'unknown'}")
        capabilities = session.get("capabilities") or []
        lines.append("- capabilities: " + (", ".join(capabilities) if capabilities else "none"))
    if status.get("error"):
        lines.append(f"- error: {status['error']}")
    if status["next_steps"]:
        lines.append("- next_steps:")
        for step in status["next_steps"]:
            lines.append(f"  - {step}")
    else:
        lines.append("- next_steps: none")
    return "\n".join(lines)


def _render_fastmail_session_text(payload: dict) -> str:
    lines = ["Fastmail JMAP session"]
    lines.append(f"- username: {payload.get('username') or 'unknown'}")
    lines.append(f"- api_url: {payload.get('apiUrl') or 'unknown'}")
    lines.append(f"- download_url: {payload.get('downloadUrl') or 'unknown'}")
    lines.append(f"- upload_url: {payload.get('uploadUrl') or 'unknown'}")
    capabilities = sorted((payload.get("capabilities") or {}).keys())
    lines.append("- capabilities: " + (", ".join(capabilities) if capabilities else "none"))
    return "\n".join(lines)


def _render_fastmail_mailboxes_text(payload: dict) -> str:
    mailboxes = payload.get("list") or []
    if not mailboxes:
        return "No Fastmail mailboxes found."

    lines = ["Fastmail mailboxes"]
    for mailbox in mailboxes:
        label = str(mailbox.get("name") or mailbox.get("role") or mailbox.get("id") or "unknown")
        bits = [
            f"role={mailbox.get('role') or 'custom'}",
            f"unreadEmails={mailbox.get('unreadEmails')}",
            f"totalEmails={mailbox.get('totalEmails')}",
        ]
        lines.append(f"- {label} [{', '.join(bits)}]")
    return "\n".join(lines)


def _render_emma_me_text(payload: dict) -> str:
    user = payload.get("user") or {}
    api_key = payload.get("apiKey") or {}
    budget = payload.get("budget") or {}
    agents = payload.get("agents") or []

    lines = ["Emma profile"]
    if user:
        lines.append(f"- username: {user.get('username')}")
        lines.append(f"- display_name: {user.get('displayName')}")
        counterpart = user.get("counterpart") or {}
        if counterpart:
            label = counterpart.get("label") or counterpart.get("username") or "configured"
            lines.append(f"- counterpart: {label}")
    if api_key:
        lines.append(f"- api_key_id: {api_key.get('id')}")
        lines.append(f"- api_key_name: {api_key.get('name')}")
        scopes = api_key.get("scopes") or []
        if scopes:
            lines.append("- scopes: " + ", ".join(str(scope) for scope in scopes))
    if budget:
        lines.append(
            "- budget: "
            f"spent_usd={budget.get('spentUsd')} remaining_usd={budget.get('remainingUsd')} capped={budget.get('capped')}"
        )
    if agents:
        lines.append("- agents: " + ", ".join(str(agent.get("id")) for agent in agents))
    return "\n".join(lines)


def _render_emma_agents_text(payload: dict) -> str:
    agents = payload.get("agents") or []
    if not agents:
        return "No Emma agents found."

    lines = ["Emma agents"]
    for agent in agents:
        parts = [str(agent.get("id") or "")]
        if agent.get("label"):
            parts.append(str(agent["label"]))
        if agent.get("description"):
            parts.append(str(agent["description"]))
        lines.append("- " + " | ".join(part for part in parts if part))
    return "\n".join(lines)


def _render_emma_chat_text(payload: dict) -> str:
    lines = ["Emma chat"]
    if payload.get("agent"):
        lines.append(f"- agent: {payload['agent']}")
    if payload.get("persona"):
        lines.append(f"- persona: {payload['persona']}")
    if payload.get("source"):
        lines.append(f"- source: {payload['source']}")
    risk = payload.get("risk") or {}
    if risk:
        lines.append(f"- risk_level: {risk.get('level')}")
    if payload.get("message"):
        lines.append("- message:")
        lines.extend(str(payload["message"]).splitlines())
    if payload.get("error"):
        lines.append(f"- error: {payload['error']}")
    return "\n".join(lines)


def _render_keys_list_text(records: list[dict]) -> str:
    if not records:
        return "No registered keys found."

    lines = ["Registered keys"]
    for record in records:
        status_parts = [record["backend"]]
        status_parts.append("available" if record["available"] else "missing")
        if record["env_present"]:
            status_parts.append("env-present")
        lines.append(f"- {record['name']} [{' | '.join(status_parts)}]")
    return "\n".join(lines)


def _render_x_auth_text(result: dict) -> str:
    me = result.get("me") or {}
    username = str(me.get("username") or "").strip()
    label = f"@{username}" if username else str(me.get("name") or me.get("id") or "unknown")
    lines = ["X auth complete"]
    lines.append(f"- browser_opened: {result.get('browser_opened', False)}")
    lines.append(f"- token_path: {result.get('token_path')}")
    lines.append(f"- authenticated_account: {label}")
    if result.get("scopes"):
        lines.append("- scopes: " + ", ".join(result["scopes"]))
    return "\n".join(lines)


def _render_x_user_text(payload: dict) -> str:
    user = payload.get("data", payload)
    if not user:
        return "No X user data returned."

    handle = str(user.get("username") or "").strip()
    metrics = user.get("public_metrics") or {}
    lines = ["X user"]
    lines.append(f"- handle: @{handle}" if handle else f"- id: {user.get('id')}")
    if user.get("name"):
        lines.append(f"- name: {user['name']}")
    if user.get("id"):
        lines.append(f"- id: {user['id']}")
    if user.get("created_at"):
        lines.append(f"- created_at: {user['created_at']}")
    if metrics:
        metric_parts = []
        for key in ("followers_count", "following_count", "tweet_count", "listed_count"):
            if key in metrics:
                metric_parts.append(f"{key}={metrics[key]}")
        if metric_parts:
            lines.append("- metrics: " + ", ".join(metric_parts))
    if user.get("description"):
        lines.append(f"- bio: {user['description']}")
    return "\n".join(lines)


def _render_x_posts_text(payload: dict, *, title: str) -> str:
    posts = payload.get("posts", [])
    if not posts:
        return f"{title}\n- No posts found."

    user = payload.get("user") or {}
    handle = str(user.get("username") or "").strip()
    lines = [title]
    if handle:
        lines.append(f"- account: @{handle}")
    meta = payload.get("meta") or {}
    if meta:
        meta_bits = []
        for key in ("result_count", "newest_id", "oldest_id", "next_token"):
            if meta.get(key) is not None:
                meta_bits.append(f"{key}={meta[key]}")
        if meta_bits:
            lines.append("- meta: " + ", ".join(meta_bits))
    for post in posts:
        created_at = str(post.get("created_at") or "unknown")
        text = " ".join(str(post.get("text") or "").split())
        if len(text) > 180:
            text = text[:177] + "..."
        lines.append(f"- {post.get('id')} {created_at} {text}")
    return "\n".join(lines)


def _render_x_post_action_text(payload: dict, *, action: str) -> str:
    data = payload.get("data", payload)
    lines = [f"X {action} complete"]
    if isinstance(data, dict):
        if data.get("id"):
            lines.append(f"- id: {data['id']}")
        if data.get("text"):
            lines.append(f"- text: {' '.join(str(data['text']).split())}")
        if data.get("deleted") is not None:
            lines.append(f"- deleted: {data['deleted']}")
    return "\n".join(lines)


def _x_content_record(row) -> dict:
    return {
        "id": int(row["id"]),
        "platform": str(row["platform"]),
        "kind": str(row["kind"]),
        "title": str(row["title"] or ""),
        "summary": str(row["summary"] or ""),
        "body_text": str(row["body_text"] or ""),
        "status": str(row["status"] or ""),
        "parent_id": int(row["parent_id"]) if row["parent_id"] is not None else None,
        "parent_title": str(row["parent_title"] or ""),
        "sequence_index": int(row["sequence_index"]) if row["sequence_index"] is not None else None,
        "tags": _load_json_field(str(row["tags_json"] or "[]"), []),
        "metadata": _load_json_field(str(row["metadata_json"] or "{}"), {}),
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def _x_media_record(row) -> dict:
    return {
        "id": int(row["id"]),
        "content_item_id": int(row["content_item_id"]) if row["content_item_id"] is not None else None,
        "content_title": str(row["content_title"] or ""),
        "content_kind": str(row["content_kind"] or ""),
        "asset_kind": str(row["asset_kind"] or ""),
        "title": str(row["title"] or ""),
        "prompt_text": str(row["prompt_text"] or ""),
        "alt_text": str(row["alt_text"] or ""),
        "status": str(row["status"] or ""),
        "model_name": str(row["model_name"] or ""),
        "relative_path": str(row["relative_path"] or ""),
        "metadata": _load_json_field(str(row["metadata_json"] or "{}"), {}),
        "error_text": str(row["error_text"] or ""),
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def _render_x_package_create_text(result: dict) -> str:
    lines = ["X package created"]
    lines.append(f"- article_id: {result['article_id']}")
    lines.append(f"- title: {result['title']}")
    lines.append(f"- posts_created: {len(result['post_ids'])}")
    lines.append(f"- image_briefs_created: {len(result['asset_ids'])}")
    return "\n".join(lines)


def _render_x_content_text(records: list[dict]) -> str:
    if not records:
        return "No X content found."

    lines = ["X content"]
    for record in records:
        context = [record["platform"], record["status"]]
        if record["tags"]:
            context.append(",".join(str(tag) for tag in record["tags"][:4]))
        if record["parent_title"]:
            context.append(f"parent={record['parent_title']}")
        lines.append(f"- #{record['id']} {record['kind']} {record['title']} [{' | '.join(context)}]")
    return "\n".join(lines)


def _render_x_content_detail_text(record: dict, children: list[dict], assets: list[dict]) -> str:
    lines = ["X content detail"]
    lines.append(f"- id: {record['id']}")
    lines.append(f"- kind: {record['kind']}")
    lines.append(f"- title: {record['title']}")
    lines.append(f"- status: {record['status']}")
    if record["tags"]:
        lines.append("- tags: " + ", ".join(str(tag) for tag in record["tags"]))
    if record["summary"]:
        lines.append(f"- summary: {record['summary']}")
    if children:
        lines.append("- thread_posts:")
        for child in children:
            label = child["sequence_index"] if child["sequence_index"] is not None else child["id"]
            lines.append(f"  - {label}. {child['body_text']}")
    if assets:
        lines.append("- image_briefs:")
        for asset in assets:
            lines.append(f"  - #{asset['id']} {asset['asset_kind']} {asset['title']} [{asset['status']}]")
    if record["body_text"]:
        lines.append("- article_body:")
        lines.extend(record["body_text"].splitlines())
    return "\n".join(lines)


def _render_x_media_text(records: list[dict]) -> str:
    if not records:
        return "No X media assets found."

    lines = ["X media"]
    for record in records:
        context = [record["status"], record["asset_kind"]]
        if record["content_title"]:
            context.append(record["content_title"])
        if record["relative_path"]:
            context.append(record["relative_path"])
        lines.append(f"- #{record['id']} {record['title']} [{' | '.join(context)}]")
    return "\n".join(lines)


def _render_x_image_generate_text(result: dict) -> str:
    lines = ["X image generated"]
    for key in (
        "asset_id",
        "title",
        "provider",
        "model",
        "relative_path",
        "size",
        "quality",
        "aspect_ratio",
        "resolution",
        "output_format",
        "bytes_written",
    ):
        if result.get(key) is not None:
            lines.append(f"- {key}: {result[key]}")
    return "\n".join(lines)


def _communication_record(row) -> dict:
    return {
        "id": int(row["id"]),
        "subject": str(row["subject"]),
        "channel": str(row["channel"]),
        "direction": str(row["direction"] or "inbound"),
        "person": str(row["person"] or ""),
        "organization_name": str(row["organization_name"] or ""),
        "happened_at": str(row["happened_at"]),
        "follow_up_at": str(row["follow_up_at"]) if row["follow_up_at"] else None,
        "status": str(row["status"]),
        "source": str(row["source"]),
        "external_from": str(row["external_from"] or ""),
        "external_to": str(row["external_to"] or ""),
        "external_cc": str(row["external_cc"] or ""),
        "external_bcc": str(row["external_bcc"] or ""),
        "external_reply_to": str(row["external_reply_to"] or ""),
        "message_id": str(row["message_id"] or ""),
        "in_reply_to": str(row["in_reply_to"] or ""),
        "thread_key": str(row["thread_key"] or ""),
        "from": _load_json_field(str(row["from_json"] or "{}"), {}),
        "to": _load_json_field(str(row["to_json"] or "[]"), []),
        "cc": _load_json_field(str(row["cc_json"] or "[]"), []),
        "bcc": _load_json_field(str(row["bcc_json"] or "[]"), []),
        "reply_to": _load_json_field(str(row["reply_to_json"] or "[]"), []),
        "references": _load_json_field(str(row["references_json"] or "[]"), []),
        "headers": _load_json_field(str(row["headers_json"] or "{}"), {}),
        "snippet": str(row["snippet"] or ""),
        "category": str(row["category"] or ""),
        "categories": _load_json_field(str(row["categories_json"] or "[]"), []),
        "priority_level": str(row["priority_level"] or ""),
        "priority_score": int(row["priority_score"] or 0),
        "retention_bucket": str(row["retention_bucket"] or ""),
        "attachments": _load_json_field(str(row["attachments_json"] or "[]"), []),
        "classification": _load_json_field(str(row["classification_json"] or "{}"), {}),
    }


def _render_communications_text(records: list[dict]) -> str:
    if not records:
        return "No communications found."

    lines = ["Communications"]
    for record in records:
        status_parts = [record["direction"], record["status"]]
        if record["priority_level"]:
            status_parts.append(record["priority_level"])
        if record["category"]:
            status_parts.append(record["category"])

        when = record["follow_up_at"] or record["happened_at"]
        context = [record["source"]]
        if record["organization_name"]:
            context.append(record["organization_name"])
        elif record["person"]:
            context.append(record["person"])
        elif record["external_from"]:
            context.append(record["external_from"])
        if record["external_to"]:
            context.append(f"to {record['external_to']}")
        if record["external_cc"]:
            context.append(f"cc {record['external_cc']}")
        if record["retention_bucket"]:
            context.append(record["retention_bucket"])
        if record["attachments"]:
            count = len(record["attachments"])
            context.append(f"{count} attachment" if count == 1 else f"{count} attachments")

        lines.append(
            f"- #{record['id']} {'/'.join(status_parts)} {when} {record['subject']} "
            f"[{', '.join(context)}]"
        )
    return "\n".join(lines)


def _render_communications_summary_text(summary: dict) -> str:
    lines = ["Communication summary", f"- total: {summary['total']}"]
    if summary["by_status"]:
        lines.append("- by_status: " + ", ".join(f"{row['status']}={row['count']}" for row in summary["by_status"]))
    else:
        lines.append("- by_status: none")
    if summary["by_category"]:
        lines.append("- by_category: " + ", ".join(f"{row['category'] or 'uncategorized'}={row['count']}" for row in summary["by_category"]))
    else:
        lines.append("- by_category: none")
    if summary["by_priority"]:
        lines.append(
            "- by_priority: " + ", ".join(f"{row['priority_level'] or 'unspecified'}={row['count']}" for row in summary["by_priority"])
        )
    else:
        lines.append("- by_priority: none")
    return "\n".join(lines)


def _profile_context_record(row) -> dict:
    return {
        "id": int(row["id"]),
        "external_key": str(row["external_key"]),
        "subject_key": str(row["subject_key"]),
        "item_type": str(row["item_type"]),
        "title": str(row["title"]),
        "source": str(row["source"]),
        "communication_id": int(row["communication_id"]) if row["communication_id"] is not None else None,
        "communication_subject": str(row["communication_subject"] or ""),
        "happened_at": str(row["happened_at"]),
        "confidence": int(row["confidence"] or 0),
        "status": str(row["status"]),
        "review_notes": str(row["review_notes"] or ""),
        "reviewed_at": str(row["reviewed_at"]) if row["reviewed_at"] else None,
        "details": _load_json_field(str(row["details_json"] or "{}"), {}),
        "evidence": _load_json_field(str(row["evidence_json"] or "[]"), []),
    }


def _render_profile_context_text(records: list[dict]) -> str:
    if not records:
        return "No profile-context items found."

    lines = ["Profile context"]
    for record in records:
        lines.append(
            f"- #{record['id']} {record['subject_key']} {record['item_type']} "
            f"{record['happened_at']} {record['title']} "
            f"[{record['status']}, confidence={record['confidence']}, {record['source']}]"
        )
        if record["review_notes"]:
            lines.append(f"  note: {record['review_notes']}")
    return "\n".join(lines)


def _render_profile_context_summary_text(summary: dict) -> str:
    lines = ["Profile context summary", f"- total: {summary['total']}"]
    if summary["by_subject"]:
        lines.append("- by_subject: " + ", ".join(f"{row['subject_key']}={row['count']}" for row in summary["by_subject"]))
    else:
        lines.append("- by_subject: none")
    if summary["by_item_type"]:
        lines.append("- by_item_type: " + ", ".join(f"{row['item_type']}={row['count']}" for row in summary["by_item_type"]))
    else:
        lines.append("- by_item_type: none")
    if summary["by_status"]:
        lines.append("- by_status: " + ", ".join(f"{row['status']}={row['count']}" for row in summary["by_status"]))
    else:
        lines.append("- by_status: none")
    return "\n".join(lines)


def _profile_record_record(row) -> dict:
    return {
        "id": int(row["id"]),
        "subject_key": str(row["subject_key"]),
        "display_name": str(row["display_name"] or ""),
        "relationship": str(row["relationship"] or ""),
        "item_type": str(row["item_type"]),
        "record_kind": str(row["record_kind"]),
        "title": str(row["title"]),
        "status": str(row["status"]),
        "source": str(row["source"]),
        "happened_at": str(row["happened_at"]),
        "confidence": int(row["confidence"] or 0),
        "notes": str(row["notes"] or ""),
        "details": _load_json_field(str(row["details_json"] or "{}"), {}),
        "evidence": _load_json_field(str(row["evidence_json"] or "[]"), []),
    }


def _render_profile_review_item_text(record: dict) -> str:
    lines = ["Profile review next"]
    lines.append(
        f"- #{record['id']} {record['subject_key']} {record['item_type']} {record['title']} "
        f"[confidence={record['confidence']}, {record['source']}, {record['happened_at']}]"
    )
    if record["communication_subject"]:
        lines.append(f"- communication_subject: {record['communication_subject']}")
    if record["details"]:
        matched_terms = []
        for key in (
            "matched_strong_terms",
            "matched_document_strong_terms",
            "matched_attachment_terms",
        ):
            matched_terms.extend(str(term) for term in record["details"].get(key, []))
        if matched_terms:
            lines.append("- matched_terms: " + ", ".join(matched_terms[:8]))
    if record["evidence"]:
        lines.append("- evidence:")
        for item in record["evidence"][:5]:
            lines.append(f"  - {item.get('kind')}: {item.get('text')}")
    return "\n".join(lines)


def _render_profile_records_text(records: list[dict]) -> str:
    if not records:
        return "No canonical profile records found."

    lines = ["Profile records"]
    for record in records:
        lines.append(
            f"- #{record['id']} {record['subject_key']} {record['item_type']} {record['title']} "
            f"[{record['status']}, {record['record_kind']}, confidence={record['confidence']}]"
        )
    return "\n".join(lines)


def _render_profile_record_detail_text(record: dict) -> str:
    lines = ["Profile record detail"]
    lines.append(f"- id: {record['id']}")
    lines.append(f"- subject: {record['subject_key']}")
    lines.append(f"- item_type: {record['item_type']}")
    lines.append(f"- record_kind: {record['record_kind']}")
    lines.append(f"- title: {record['title']}")
    lines.append(f"- status: {record['status']}")
    lines.append(f"- confidence: {record['confidence']}")
    lines.append(f"- happened_at: {record['happened_at']}")
    if record["notes"]:
        lines.append(f"- notes: {record['notes']}")
    if record["linked_profile_items"]:
        lines.append("- linked_profile_items:")
        for item in record["linked_profile_items"]:
            lines.append(
                f"  - #{item['id']} {item['item_type']} {item['title']} "
                f"[{item['status']}, confidence={item['confidence']}]"
            )
    if record["linked_attachments"]:
        lines.append("- linked_attachments:")
        for attachment in record["linked_attachments"]:
            lines.append(
                f"  - #{attachment['id']} {attachment['filename']} "
                f"[{attachment['ingest_status']}, {attachment['relative_path']}]"
            )
    return "\n".join(lines)


def _render_profile_record_summary_text(summary: dict) -> str:
    lines = ["Profile record summary", f"- total: {summary['total']}"]
    if summary["by_subject"]:
        lines.append("- by_subject: " + ", ".join(f"{row['subject_key']}={row['count']}" for row in summary["by_subject"]))
    else:
        lines.append("- by_subject: none")
    if summary["by_item_type"]:
        lines.append("- by_item_type: " + ", ".join(f"{row['item_type']}={row['count']}" for row in summary["by_item_type"]))
    else:
        lines.append("- by_item_type: none")
    if summary["by_status"]:
        lines.append("- by_status: " + ", ".join(f"{row['status']}={row['count']}" for row in summary["by_status"]))
    else:
        lines.append("- by_status: none")
    return "\n".join(lines)


def _render_profile_alerts_text(records: list[dict]) -> str:
    if not records:
        return "No profile alerts found."

    lines = ["Profile alerts"]
    for record in records:
        lines.append(
            f"- [{record['level']}] record #{record['record_id']} {record['item_type']} {record['title']} "
            f"({record['subject_key']}, age_days={record['age_days']})"
        )
        lines.append(f"  reason: {record['reason']}")
    return "\n".join(lines)


def _attachment_record(row) -> dict:
    return {
        "id": int(row["id"]),
        "external_key": str(row["external_key"]),
        "communication_id": int(row["communication_id"]),
        "communication_subject": str(row["communication_subject"] or ""),
        "filename": str(row["filename"] or ""),
        "mime_type": str(row["mime_type"] or ""),
        "size": int(row["size"] or 0),
        "relative_path": str(row["relative_path"] or ""),
        "extracted_text_path": str(row["extracted_text_path"] or ""),
        "extraction_method": str(row["extraction_method"] or ""),
        "ingest_status": str(row["ingest_status"] or ""),
        "error_text": str(row["error_text"] or ""),
        "text_preview": str(row["extracted_text"] or "")[:280],
    }


def _render_attachments_text(records: list[dict]) -> str:
    if not records:
        return "No downloaded attachments found."

    lines = ["Attachments"]
    for record in records:
        context = [f"comm={record['communication_id']}"]
        if record["extraction_method"]:
            context.append(record["extraction_method"])
        if record["relative_path"]:
            context.append(record["relative_path"])
        lines.append(
            f"- #{record['id']} {record['ingest_status']} {record['filename']} "
            f"[{', '.join(context)}]"
        )
    return "\n".join(lines)


def _render_attachment_summary_text(summary: dict) -> str:
    lines = ["Attachment summary", f"- total: {summary['total']}"]
    if summary["by_status"]:
        lines.append(
            "- by_status: "
            + ", ".join(f"{row['ingest_status']}={row['count']}" for row in summary["by_status"])
        )
    else:
        lines.append("- by_status: none")
    if summary["by_method"]:
        lines.append(
            "- by_method: "
            + ", ".join(f"{row['extraction_method'] or 'none'}={row['count']}" for row in summary["by_method"])
        )
    else:
        lines.append("- by_method: none")
    return "\n".join(lines)


def _should_use_hot_runtime_db(db_path: Path) -> bool:
    return db_path.expanduser().resolve(strict=False) == store.default_db_path().resolve(strict=False)


def _run_without_db_command(args: argparse.Namespace) -> str | None:
    command = args.command

    if command == "keys-set":
        if args.from_env:
            value = str(credentials.resolve_secret(name=args.name) or "")
        else:
            value = str(args.value or "")
        result = credentials.set_secret(
            name=args.name,
            value=value,
            backend=args.backend,
            allow_insecure_file_backend=args.allow_insecure_file_backend,
        )
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_sync_summary("Key stored", result)

    if command == "keys-list":
        records = credentials.list_secrets()
        if args.format == "json":
            return json.dumps(records, indent=2)
        return _render_keys_list_text(records)

    if command == "keys-export":
        result = credentials.export_secrets(names=args.names)
        if args.format == "json":
            return json.dumps(result, indent=2)
        return result["export_text"]

    if command == "keys-delete":
        result = credentials.delete_secret(name=args.name)
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_sync_summary("Key deleted", result)

    if command == "vault-generate-master-key":
        result = generate_master_key(
            backend=args.backend,
            allow_insecure_file_backend=args.allow_insecure_file_backend,
        )
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_sync_summary("Master key stored", result)

    if command == "vault-status":
        result = master_key_status()
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_vault_status_text(result)

    if command == "backup-create":
        result = create_encrypted_db_backup(
            db_path=args.db,
            output_dir=args.output_dir,
        )
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_sync_summary("Encrypted backup created", result)

    if command == "backup-list":
        records = list_encrypted_backups(output_dir=args.output_dir)
        if args.format == "json":
            return json.dumps(records, indent=2)
        return _render_backup_list_text(records)

    if command == "backup-status":
        result = encrypted_backup_status(output_dir=args.output_dir)
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_backup_status_text(result)

    if command == "backup-restore":
        result = restore_encrypted_db_backup(
            manifest_path=args.manifest_path,
            output_path=args.output_path,
        )
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_sync_summary("Encrypted backup restored", result)

    if command == "cloudflare-mail-init-config":
        result = write_cloudflare_mail_config_template(args.config, force=args.force)
        return _render_sync_summary("Cloudflare mail config template prepared", result)

    if command == "cloudflare-mail-status":
        result = cloudflare_mail_status(config_path=args.config)
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_cloudflare_mail_status_text(result)

    if command == "cloudflare-mail-queue-status":
        result = cloudflare_mail_queue_status(config_path=args.config)
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_cloudflare_mail_queue_status_text(result)

    if command == "cloudflare-mail-write-worker":
        result = write_cloudflare_worker_template(
            args.output_dir,
            config_path=args.config,
            force=args.force,
        )
        return _render_sync_summary("Cloudflare Email Worker template prepared", result)

    if command == "cloudflare-mail-sync":
        inbound_result = sync_cloudflare_mail_queue(
            db_path=args.db,
            config_path=args.config,
            limit=args.limit,
        )
        if inbound_result.get("skipped"):
            outbound_result = {
                "skipped": True,
                "processed_count": 0,
                "failed_count": 0,
                "processed": [],
                "failures": [],
            }
        else:
            try:
                outbound_result = process_resend_delivery_queue(
                    db_path=args.db,
                    config_path=default_resend_config_path(),
                    limit=args.limit,
                )
            except Exception as exc:
                with store.open_db(args.db) as connection:
                    store.upsert_system_alert(
                        connection,
                        alert_key="resend_delivery_queue",
                        source="resend_delivery",
                        severity="error",
                        title="Resend queue processing failed",
                        message=str(exc),
                        details={"command": "cloudflare-mail-sync"},
                    )
                outbound_result = {
                    "processed_count": 0,
                    "failed_count": 1,
                    "processed": [],
                    "failures": [{"error": str(exc)}],
                    "error": str(exc),
                }
        result = {**inbound_result, "outbound": outbound_result}
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_cloudflare_mail_sync_text(result)

    if command == "cloudflare-mail-inject-test":
        token = uuid.uuid4().hex[:12]
        message = EmailMessage()
        message["From"] = args.from_email
        message["To"] = args.to
        message["Subject"] = f"{args.subject} {token}"
        message["Message-ID"] = f"<lifeops-test-{token}@{socket.gethostname()}>"
        message.set_content(args.body)
        clean_html = str(args.html or "").strip()
        if clean_html:
            message.add_alternative(clean_html, subtype="html")
            html_part = message.get_payload()[-1]
        else:
            html_part = None
        for item in args.inline_attachment_specs:
            attachment_path, content_id = _inline_spec_parts(item)
            payload = attachment_path.read_bytes()
            maintype, subtype = _guess_mime_parts(attachment_path)
            if html_part is None:
                message.add_alternative(f"<p>{args.body}</p>", subtype="html")
                html_part = message.get_payload()[-1]
            html_part.add_related(
                payload,
                maintype=maintype,
                subtype=subtype,
                cid=f"<{content_id}>",
                filename=attachment_path.name,
                disposition="inline",
            )
        for item in args.attachment_paths:
            attachment_path = Path(str(item).strip()).expanduser()
            payload = attachment_path.read_bytes()
            maintype, subtype = _guess_mime_parts(attachment_path)
            message.add_attachment(payload, maintype=maintype, subtype=subtype, filename=attachment_path.name)
        raw_bytes = message.as_bytes()
        payload = {
            "provider": "cloudflare-email-routing",
            "worker": "life-ops-email-ingest",
            "received_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "envelope_from": args.from_email,
            "envelope_to": args.to,
            "headers": {
                "From": args.from_email,
                "To": args.to,
                "Subject": message["Subject"],
                "Message-ID": message["Message-ID"],
            },
            "raw_base64": base64.b64encode(raw_bytes).decode("ascii"),
            "raw_size": len(raw_bytes),
        }
        result = enqueue_cloudflare_mail_payload(payload=payload, config_path=args.config)
        result["subject"] = str(message["Subject"])
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_sync_summary("Cloudflare mail test payload enqueued", result)

    if command == "mail-ingest-generate-secret":
        result = generate_mail_ingest_secret(
            backend=args.backend,
            allow_insecure_file_backend=args.allow_insecure_file_backend,
        )
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_sync_summary("Mail ingest secret stored", result)

    if command == "mail-ingest-status":
        result = mail_ingest_status(db_path=args.db)
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_mail_ingest_status_text(result)

    if command == "mail-ingest-file":
        payload = json.loads(args.input.read_text())
        result = ingest_cloudflare_email_payload(payload, db_path=args.db)
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_sync_summary("Mail payload ingested", result)

    if command == "mail-ingest-serve":
        print(
            f"Starting mail ingest server on http://{args.host}:{args.port}{args.path}",
            flush=True,
        )
        serve_mail_ingest(
            db_path=args.db,
            host=args.host,
            port=args.port,
            path=args.path,
        )
        return ""

    if command == "mail-ui":
        target_db_path = resolve_cmail_db_path(args.db)
        ensure_cmail_runtime_db(
            runtime_db_path=target_db_path,
            canonical_db_path=store.default_db_path(),
        )
        print(
            f"Starting mail UI on http://{args.host}:{args.port}",
            flush=True,
        )
        serve_mail_ui(
            db_path=target_db_path,
            host=args.host,
            port=args.port,
            limit=args.limit,
        )
        return ""

    if command == "cmail-serve":
        target_db_path = resolve_cmail_db_path(args.db)
        ensure_cmail_runtime_db(
            runtime_db_path=target_db_path,
            canonical_db_path=args.canonical_db,
        )
        print(
            f"Starting managed CMAIL service on http://{args.host}:{args.port}",
            flush=True,
        )
        serve_cmail_service(
            runtime_db_path=target_db_path,
            canonical_db_path=args.canonical_db,
            host=args.host,
            port=args.port,
            limit=args.limit,
            sync_interval_seconds=args.sync_interval,
            send_interval_seconds=args.send_interval,
            seal_interval_seconds=args.seal_interval,
        )
        return ""

    if command == "cmail-runtime-seal":
        result = seal_cmail_runtime_db(
            runtime_db_path=resolve_cmail_db_path(args.db),
            canonical_db_path=args.canonical_db,
        )
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_sync_summary("CMAIL runtime sealed", result)

    if command == "cmail-drafts":
        target_db_path = resolve_cmail_db_path(args.db)
        ensure_cmail_runtime_db(
            runtime_db_path=target_db_path,
            canonical_db_path=store.default_db_path(),
        )
        records = list_cmail_drafts(db_path=target_db_path)
        if args.format == "json":
            return json.dumps(records, indent=2)
        return _render_cmail_drafts_text(records)

    if command == "cmail-draft-save":
        target_db_path = resolve_cmail_db_path(args.db)
        ensure_cmail_runtime_db(
            runtime_db_path=target_db_path,
            canonical_db_path=store.default_db_path(),
        )
        body_text = args.body
        if args.body_file is not None:
            body_text = args.body_file.read_text()
        record = save_cmail_draft(
            db_path=target_db_path,
            payload={
                "id": args.id,
                "to": args.to,
                "cc": args.cc,
                "bcc": args.bcc,
                "subject": args.subject,
                "body_text": body_text,
                "attachment_paths": args.attach,
            },
        )
        if args.format == "json":
            return json.dumps(record, indent=2)
        return _render_cmail_draft_saved_text(record)

    if command == "cmail-draft-send":
        target_db_path = resolve_cmail_db_path(args.db)
        ensure_cmail_runtime_db(
            runtime_db_path=target_db_path,
            canonical_db_path=store.default_db_path(),
        )
        result = send_cmail_draft(
            db_path=target_db_path,
            draft_id=args.id,
        )
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_sync_summary("CMAIL draft sent", result)

    if command == "resend-init-config":
        result = write_resend_config_template(args.config, force=args.force)
        return _render_sync_summary("Resend config template prepared", result)

    if command == "resend-status":
        result = resend_status(config_path=args.config)
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_resend_status_text(result)

    if command == "resend-signature-show":
        result = resend_get_default_signature(config_path=args.config)
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_resend_signature_text(result)

    if command == "resend-signature-set":
        text_value = "" if args.clear else args.text
        html_value = "" if args.clear else args.html
        result = resend_set_default_signature(
            signature_text=text_value,
            signature_html=html_value,
            config_path=args.config,
        )
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_sync_summary("Resend default signature updated", result)

    if command == "resend-domains":
        result = resend_list_domains(config_path=args.config)
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_resend_domains_text(result)

    if command == "resend-domain-create":
        result = resend_create_domain(
            name=args.name,
            config_path=args.config,
            region=args.region,
        )
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_sync_summary("Resend domain created", result)

    if command == "resend-queue-status":
        result = resend_queue_status(db_path=args.db, limit=args.limit)
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_resend_queue_status_text(result)

    if command == "resend-queue-process":
        result = process_resend_delivery_queue(
            db_path=args.db,
            config_path=args.config,
            limit=args.limit,
        )
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_resend_queue_process_text(result)

    if command == "resend-send-email":
        result = resend_send_email(
            to=args.to_addresses,
            cc=args.cc_addresses,
            bcc=args.bcc_addresses,
            subject=args.subject,
            text=args.text,
            html=args.html,
            from_email=args.from_email,
            reply_to=args.reply_to,
            in_reply_to=args.in_reply_to,
            references=args.reference_ids,
            thread_key=args.thread_key,
            attachment_paths=args.attachment_paths,
            inline_attachment_specs=args.inline_attachment_specs,
            apply_signature=not args.no_signature,
            journal_db_path=args.db,
            config_path=args.config,
            attempt_immediately=not args.queue_only,
            max_attempts=args.max_attempts,
        )
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_sync_summary("Resend email queued", result)

    if command == "mail-alerts":
        with store.open_db(args.db) as connection:
            alerts = store.list_system_alerts(
                connection,
                source=args.source,
                status=args.status,
                limit=args.limit,
            )
            serialized = [dict(row) for row in alerts]
        if args.format == "json":
            return json.dumps(serialized, indent=2)
        return _render_mail_alerts_text(serialized)

    if command == "fastmail-init-config":
        result = write_fastmail_config_template(args.config, force=args.force)
        return _render_sync_summary("Fastmail config template prepared", result)

    if command == "fastmail-status":
        result = fastmail_status(config_path=args.config)
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_fastmail_status_text(result)

    if command == "fastmail-session":
        result = fastmail_session(config_path=args.config)
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_fastmail_session_text(result)

    if command == "fastmail-mailboxes":
        result = fastmail_mailboxes(config_path=args.config)
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_fastmail_mailboxes_text(result)

    if command == "emma-status":
        result = emma_status(base_url=args.base_url)
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_emma_status_text(result)

    if command == "emma-me":
        result = emma_me(base_url=args.base_url)
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_emma_me_text(result)

    if command == "emma-agents":
        result = emma_agents(base_url=args.base_url)
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_emma_agents_text(result)

    if command == "emma-chat":
        result = emma_chat(
            base_url=args.base_url,
            agent=args.agent,
            mode=args.mode,
            message=args.message,
        )
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_emma_chat_text(result)

    if command == "x-init-config":
        result = write_x_client_template(args.client_config, force=args.force)
        return _render_sync_summary("X config template prepared", result)

    if command == "x-status":
        status = x_status(client_path=args.client_config, token_path=args.token)
        if args.format == "json":
            return json.dumps(status, indent=2)
        return _render_x_status_text(status)

    if command == "x-auth":
        result = x_auth(
            client_path=args.client_config,
            token_path=args.token,
            timeout_seconds=args.timeout_seconds,
            open_browser=not args.no_open,
        )
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_x_auth_text(result)

    if command == "x-refresh":
        result = refresh_x_token(
            client_path=args.client_config,
            token_path=args.token,
        )
        if args.format == "json":
            return json.dumps(result, indent=2)
        return _render_sync_summary(
            "X token refresh complete",
            {
                "token_path": args.token,
                "has_access_token": bool(result.get("access_token")),
                "has_refresh_token": bool(result.get("refresh_token")),
                "obtained_at": result.get("obtained_at"),
                "expires_in": result.get("expires_in"),
            },
        )

    if command == "x-me":
        payload = x_get_authenticated_user(
            client_path=args.client_config,
            token_path=args.token,
        )
        if args.format == "json":
            return json.dumps(payload, indent=2)
        return _render_x_user_text(payload)

    if command == "x-user":
        payload = x_lookup_user_by_username(
            username=args.username,
            client_path=args.client_config,
            token_path=args.token,
        )
        if args.format == "json":
            return json.dumps(payload, indent=2)
        return _render_x_user_text(payload)

    if command == "x-posts":
        payload = x_get_user_posts(
            client_path=args.client_config,
            token_path=args.token,
            username=args.username,
            max_results=args.limit,
        )
        if args.format == "json":
            return json.dumps(payload, indent=2)
        title = "X posts"
        if args.username:
            title = f"X posts for @{args.username.lstrip('@')}"
        return _render_x_posts_text(payload, title=title)

    if command == "x-home":
        payload = x_get_home_timeline(
            client_path=args.client_config,
            token_path=args.token,
            max_results=args.limit,
        )
        if args.format == "json":
            return json.dumps(payload, indent=2)
        return _render_x_posts_text(payload, title="X home timeline")

    if command == "x-post":
        payload = x_create_post(
            client_path=args.client_config,
            token_path=args.token,
            text=args.text,
        )
        if args.format == "json":
            return json.dumps(payload, indent=2)
        return _render_x_post_action_text(payload, action="post")

    if command == "x-delete-post":
        payload = x_delete_post(
            client_path=args.client_config,
            token_path=args.token,
            post_id=args.id,
        )
        if args.format == "json":
            return json.dumps(payload, indent=2)
        return _render_x_post_action_text(payload, action="delete")

    # ── social ──────────────────────────────────────────────────────

    if command == "social-auth":
        ok = social_authenticate(args.platform)
        if ok:
            return f"Authenticated with {args.platform}."
        return f"Authentication timed out for {args.platform}.  Try again."

    if command == "social-status":
        if args.platform:
            profile_present = args.platform in social_list_sessions()
            logged_in = social_check_status(args.platform) if profile_present else False
            result = {
                args.platform: {
                    "logged_in": logged_in,
                    "profile_present": profile_present,
                }
            }
        else:
            sessions = social_list_sessions()
            platforms = social_available_platforms()
            result = {}
            for plat in platforms:
                profile_present = plat in sessions
                logged_in = False
                if profile_present:
                    logged_in = social_check_status(plat)
                result[plat] = {
                    "logged_in": logged_in,
                    "profile_present": profile_present,
                    "cookies": sessions.get(plat, {}).get("cookies", 0),
                }
        if args.format == "json":
            return json.dumps(result, indent=2)
        lines = ["Social platform sessions:"]
        for plat, info in sorted(result.items()):
            if info.get("logged_in"):
                status_icon = "ok"
            elif info.get("profile_present"):
                status_icon = "saved profile, login expired"
            else:
                status_icon = "no session"
            lines.append(f"  {plat}: {status_icon}")
        return "\n".join(lines)

    if command == "social-logout":
        removed = social_clear_session(args.platform)
        if removed:
            return f"Cleared session for {args.platform}."
        return f"No stored session for {args.platform}."

    if command == "social-post":
        platforms = []
        seen_platforms: set[str] = set()
        for raw_name in args.platforms.split(","):
            platform_name = raw_name.strip()
            if not platform_name or platform_name in seen_platforms:
                continue
            seen_platforms.add(platform_name)
            platforms.append(platform_name)
        if not platforms:
            raise ValueError("No platforms specified.")
        invalid_platforms = [plat for plat in platforms if plat not in social_available_platforms()]
        if invalid_platforms:
            raise ValueError(
                "Unknown platform(s): "
                + ", ".join(sorted(invalid_platforms))
                + f". Available: {', '.join(social_available_platforms())}"
            )
        text = args.text or ""
        if not text and not args.linkedin_text and not args.facebook_text:
            raise ValueError("Provide --text or a platform-specific text flag.")
        overrides: dict[str, str] = {}
        if args.linkedin_text:
            overrides["linkedin"] = args.linkedin_text
        if args.facebook_text:
            overrides["facebook"] = args.facebook_text
        missing_text_platforms = [
            plat for plat in platforms if not (overrides.get(plat, text) or "").strip()
        ]
        if missing_text_platforms:
            raise ValueError(
                "Provide post text for: " + ", ".join(missing_text_platforms)
            )
        headless = not args.visible
        results = social_post_multi(
            platforms,
            text,
            platform_text=overrides if overrides else None,
            image=args.image,
            headless=headless,
        )
        lines = []
        for plat, info in results.items():
            status = "ok" if info.get("ok") else "FAILED"
            lines.append(f"  {plat}: {status} — {info.get('message', '')}")
        return "Social post results:\n" + "\n".join(lines)

    return None


def run(args: argparse.Namespace) -> str:
    command = args.command

    if command == "init":
        path = store.initialize(args.db)
        return f"Initialized database at {path}"

    without_db_result = _run_without_db_command(args)
    if without_db_result is not None:
        return without_db_result

    if command in {"add-item", "list-items", "done-item"} and _should_use_hot_runtime_db(args.db):
        target_db_path = resolve_cmail_db_path(args.db)
        ensure_cmail_runtime_list_items(
            runtime_db_path=target_db_path,
            canonical_db_path=store.default_db_path(),
        )
        with store.open_db(target_db_path) as connection:
            if command == "add-item":
                item_id = store.add_list_item(
                    connection,
                    list_name=args.list_name,
                    title=args.title,
                    notes=args.notes,
                )
                return f"Added {args.list_name} item #{item_id}: {args.title}"

            if command == "list-items":
                rows = store.list_list_items(
                    connection,
                    list_name=args.list_name,
                    status=args.status,
                    limit=args.limit,
                )
                records = [_list_item_record(row) for row in rows]
                if args.format == "json":
                    return json.dumps(records, indent=2)
                return _render_list_items_text(records)

            if command == "done-item":
                store.set_list_item_status(connection, item_id=args.id, status="done")
                row = store.get_list_item(connection, args.id)
                if row is None:
                    return f"Marked list item #{args.id} done"
                return f"Marked {row['list_name']} item #{args.id} done: {row['title']}"

    with store.open_db(args.db) as connection:
        if command == "google-auth":
            result = ensure_google_auth(args.credentials, args.token)
            return _render_sync_summary("Google auth ready", result)

        if command == "google-list-calendars":
            calendars = list_google_calendars(args.credentials, args.token)
            if not calendars:
                return "No calendars found."

            lines = ["Google calendars"]
            for calendar in calendars:
                flags = []
                if calendar["primary"]:
                    flags.append("primary")
                if calendar["selected"]:
                    flags.append("selected")
                suffix = f" [{' '.join(flags)}]" if flags else ""
                lines.append(f"- {calendar['summary']} ({calendar['id']}){suffix}")
            return "\n".join(lines)

        if command == "sync-google-calendar":
            result = sync_google_calendar(
                connection,
                credentials_path=args.credentials,
                token_path=args.token,
                calendar_ids=args.calendar_ids,
                days_back=args.days_back,
                days_ahead=args.days_ahead,
            )
            return _render_sync_summary("Google Calendar sync complete", result)

        if command == "sync-gmail":
            result = sync_gmail(
                connection,
                credentials_path=args.credentials,
                token_path=args.token,
                query=args.query,
                max_results=args.max_results,
            )
            return _render_sync_summary("Gmail sync complete", result)

        if command == "backfill-gmail":
            result = backfill_gmail(
                connection,
                credentials_path=args.credentials,
                token_path=args.token,
                query=args.query,
                max_results=args.max_results,
                before_ts=args.before_ts,
                recent_cutoff_days=args.recent_cutoff_days,
                reset_cursor=args.reset_cursor,
            )
            return _render_sync_summary("Gmail backfill complete", result)

        if command == "sync-google":
            calendar_result = sync_google_calendar(
                connection,
                credentials_path=args.credentials,
                token_path=args.token,
                calendar_ids=args.calendar_ids,
                days_back=args.days_back,
                days_ahead=args.days_ahead,
            )
            gmail_result = sync_gmail(
                connection,
                credentials_path=args.credentials,
                token_path=args.token,
                query=args.query,
                max_results=args.max_results,
            )
            return "\n".join(
                [
                    _render_sync_summary("Google Calendar sync complete", calendar_result),
                    "",
                    _render_sync_summary("Gmail sync complete", gmail_result),
                ]
            )

        if command == "sync-gmail-corpus":
            result = sync_gmail_corpus(
                connection,
                credentials_path=args.credentials,
                token_path=args.token,
                recent_query=args.recent_query,
                recent_max_results=args.recent_max_results,
                backfill_query=args.backfill_query,
                backfill_max_results=args.backfill_max_results,
                backfill_max_runs=args.backfill_max_runs,
                recent_cutoff_days=args.recent_cutoff_days,
                reset_backfill_cursor=args.reset_backfill_cursor,
            )
            return _render_sync_summary("Gmail corpus sync complete", result)

        if command == "sync-gmail-category-pass":
            result = sync_gmail_category_pass(
                connection,
                credentials_path=args.credentials,
                token_path=args.token,
                max_results=args.max_results,
                reset_cursors=args.reset_cursors,
            )
            return _render_sync_summary("Gmail category pass complete", result)

        if command == "trace-summary":
            summary = tracing.summarize_traces(
                connection,
                trace_type=args.trace_type,
                limit=args.limit,
            )
            if args.format == "json":
                return json.dumps(summary, indent=2)
            return tracing.render_trace_summary_text(summary)

        if command == "gmail-heartbeat":
            heartbeat = _gmail_heartbeat(connection)
            if args.format == "json":
                return json.dumps(heartbeat, indent=2)
            return _render_gmail_heartbeat_text(heartbeat)

        if command == "profile-attachment-heartbeat":
            heartbeat = _profile_attachment_heartbeat(connection)
            if args.format == "json":
                return json.dumps(heartbeat, indent=2)
            return _render_profile_attachment_heartbeat_text(heartbeat)

        if command == "export-traces":
            records = tracing.export_trace_records(
                connection,
                trace_type=args.trace_type,
                limit=args.limit,
            )
            rendered = _render_trace_export(records, args.format)
            if args.output:
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(rendered + ("\n" if rendered and not rendered.endswith("\n") else ""))
                return f"Exported {len(records)} trace records to {args.output}"
            return rendered

        if command == "keys-set":
            if args.from_env:
                value = str(credentials.resolve_secret(name=args.name) or "")
            else:
                value = str(args.value or "")
            result = credentials.set_secret(
                name=args.name,
                value=value,
                backend=args.backend,
                allow_insecure_file_backend=args.allow_insecure_file_backend,
            )
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_sync_summary("Key stored", result)

        if command == "keys-list":
            records = credentials.list_secrets()
            if args.format == "json":
                return json.dumps(records, indent=2)
            return _render_keys_list_text(records)

        if command == "keys-export":
            result = credentials.export_secrets(names=args.names)
            if args.format == "json":
                return json.dumps(result, indent=2)
            return result["export_text"]

        if command == "keys-delete":
            result = credentials.delete_secret(name=args.name)
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_sync_summary("Key deleted", result)

        if command == "vault-generate-master-key":
            result = generate_master_key(
                backend=args.backend,
                allow_insecure_file_backend=args.allow_insecure_file_backend,
            )
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_sync_summary("Master key stored", result)

        if command == "vault-status":
            result = master_key_status()
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_vault_status_text(result)

        if command == "backup-create":
            result = create_encrypted_db_backup(
                db_path=args.db,
                output_dir=args.output_dir,
            )
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_sync_summary("Encrypted backup created", result)

        if command == "backup-list":
            records = list_encrypted_backups(output_dir=args.output_dir)
            if args.format == "json":
                return json.dumps(records, indent=2)
            return _render_backup_list_text(records)

        if command == "backup-status":
            result = encrypted_backup_status(output_dir=args.output_dir)
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_backup_status_text(result)

        if command == "backup-restore":
            result = restore_encrypted_db_backup(
                manifest_path=args.manifest_path,
                output_path=args.output_path,
            )
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_sync_summary("Encrypted backup restored", result)

        if command == "cloudflare-mail-init-config":
            result = write_cloudflare_mail_config_template(args.config, force=args.force)
            return _render_sync_summary("Cloudflare mail config template prepared", result)

        if command == "cloudflare-mail-status":
            result = cloudflare_mail_status(config_path=args.config)
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_cloudflare_mail_status_text(result)

        if command == "cloudflare-mail-queue-status":
            result = cloudflare_mail_queue_status(config_path=args.config)
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_cloudflare_mail_queue_status_text(result)

        if command == "cloudflare-mail-write-worker":
            result = write_cloudflare_worker_template(
                args.output_dir,
                config_path=args.config,
                force=args.force,
            )
            return _render_sync_summary("Cloudflare Email Worker template prepared", result)

        if command == "cloudflare-mail-sync":
            result = sync_cloudflare_mail_queue(
                db_path=args.db,
                config_path=args.config,
                limit=args.limit,
            )
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_cloudflare_mail_sync_text(result)

        if command == "cloudflare-mail-inject-test":
            token = uuid.uuid4().hex[:12]
            message = EmailMessage()
            message["From"] = args.from_email
            message["To"] = args.to
            message["Subject"] = f"{args.subject} {token}"
            message["Message-ID"] = f"<lifeops-test-{token}@{socket.gethostname()}>"
            message.set_content(args.body)
            clean_html = str(args.html or "").strip()
            if clean_html:
                message.add_alternative(clean_html, subtype="html")
                html_part = message.get_payload()[-1]
            else:
                html_part = None
            for item in args.inline_attachment_specs:
                attachment_path, content_id = _inline_spec_parts(item)
                payload = attachment_path.read_bytes()
                maintype, subtype = _guess_mime_parts(attachment_path)
                if html_part is None:
                    message.add_alternative(f"<p>{args.body}</p>", subtype="html")
                    html_part = message.get_payload()[-1]
                html_part.add_related(
                    payload,
                    maintype=maintype,
                    subtype=subtype,
                    cid=f"<{content_id}>",
                    filename=attachment_path.name,
                    disposition="inline",
                )
            for item in args.attachment_paths:
                attachment_path = Path(str(item).strip()).expanduser()
                payload = attachment_path.read_bytes()
                maintype, subtype = _guess_mime_parts(attachment_path)
                message.add_attachment(payload, maintype=maintype, subtype=subtype, filename=attachment_path.name)
            raw_bytes = message.as_bytes()
            payload = {
                "provider": "cloudflare-email-routing",
                "worker": "life-ops-email-ingest",
                "received_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
                "envelope_from": args.from_email,
                "envelope_to": args.to,
                "headers": {
                    "From": args.from_email,
                    "To": args.to,
                    "Subject": message["Subject"],
                    "Message-ID": message["Message-ID"],
                },
                "raw_base64": base64.b64encode(raw_bytes).decode("ascii"),
                "raw_size": len(raw_bytes),
            }
            result = enqueue_cloudflare_mail_payload(payload=payload, config_path=args.config)
            result["subject"] = str(message["Subject"])
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_sync_summary("Cloudflare mail test payload enqueued", result)

        if command == "mail-ingest-generate-secret":
            result = generate_mail_ingest_secret(
                backend=args.backend,
                allow_insecure_file_backend=args.allow_insecure_file_backend,
            )
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_sync_summary("Mail ingest secret stored", result)

        if command == "mail-ingest-status":
            result = mail_ingest_status(db_path=args.db)
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_mail_ingest_status_text(result)

        if command == "mail-ingest-file":
            payload = json.loads(args.input.read_text())
            result = ingest_cloudflare_email_payload(payload, db_path=args.db)
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_sync_summary("Mail payload ingested", result)

        if command == "mail-ingest-serve":
            print(
                f"Starting mail ingest server on http://{args.host}:{args.port}{args.path}",
                flush=True,
            )
            serve_mail_ingest(
                db_path=args.db,
                host=args.host,
                port=args.port,
                path=args.path,
            )
            return ""

        if command == "resend-init-config":
            result = write_resend_config_template(args.config, force=args.force)
            return _render_sync_summary("Resend config template prepared", result)

        if command == "resend-status":
            result = resend_status(config_path=args.config)
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_resend_status_text(result)

        if command == "resend-signature-show":
            result = resend_get_default_signature(config_path=args.config)
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_resend_signature_text(result)

        if command == "resend-signature-set":
            text_value = "" if args.clear else args.text
            html_value = "" if args.clear else args.html
            result = resend_set_default_signature(
                signature_text=text_value,
                signature_html=html_value,
                config_path=args.config,
            )
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_sync_summary("Resend default signature updated", result)

        if command == "resend-domains":
            result = resend_list_domains(config_path=args.config)
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_resend_domains_text(result)

        if command == "resend-domain-create":
            result = resend_create_domain(
                name=args.name,
                config_path=args.config,
                region=args.region,
            )
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_sync_summary("Resend domain created", result)

        if command == "resend-send-email":
            result = resend_send_email(
                to=args.to_addresses,
                cc=args.cc_addresses,
                bcc=args.bcc_addresses,
                subject=args.subject,
                text=args.text,
                html=args.html,
                from_email=args.from_email,
                reply_to=args.reply_to,
                in_reply_to=args.in_reply_to,
                references=args.reference_ids,
                thread_key=args.thread_key,
                attachment_paths=args.attachment_paths,
                inline_attachment_specs=args.inline_attachment_specs,
                apply_signature=not args.no_signature,
                config_path=args.config,
            )
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_sync_summary("Resend email queued", result)

        if command == "fastmail-init-config":
            result = write_fastmail_config_template(args.config, force=args.force)
            return _render_sync_summary("Fastmail config template prepared", result)

        if command == "fastmail-status":
            result = fastmail_status(config_path=args.config)
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_fastmail_status_text(result)

        if command == "fastmail-session":
            result = fastmail_session(config_path=args.config)
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_fastmail_session_text(result)

        if command == "fastmail-mailboxes":
            result = fastmail_mailboxes(config_path=args.config)
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_fastmail_mailboxes_text(result)

        if command == "emma-status":
            result = emma_status(base_url=args.base_url)
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_emma_status_text(result)

        if command == "emma-me":
            result = emma_me(base_url=args.base_url)
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_emma_me_text(result)

        if command == "emma-agents":
            result = emma_agents(base_url=args.base_url)
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_emma_agents_text(result)

        if command == "emma-chat":
            result = emma_chat(
                base_url=args.base_url,
                agent=args.agent,
                mode=args.mode,
                message=args.message,
            )
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_emma_chat_text(result)

        if command == "x-init-config":
            result = write_x_client_template(args.client_config, force=args.force)
            return _render_sync_summary("X config template prepared", result)

        if command == "x-status":
            status = x_status(client_path=args.client_config, token_path=args.token)
            if args.format == "json":
                return json.dumps(status, indent=2)
            return _render_x_status_text(status)

        if command == "x-auth":
            result = x_auth(
                client_path=args.client_config,
                token_path=args.token,
                timeout_seconds=args.timeout_seconds,
                open_browser=not args.no_open,
            )
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_x_auth_text(result)

        if command == "x-refresh":
            result = refresh_x_token(
                client_path=args.client_config,
                token_path=args.token,
            )
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_sync_summary(
                "X token refresh complete",
                {
                    "token_path": args.token,
                    "has_access_token": bool(result.get("access_token")),
                    "has_refresh_token": bool(result.get("refresh_token")),
                    "obtained_at": result.get("obtained_at"),
                    "expires_in": result.get("expires_in"),
                },
            )

        if command == "x-me":
            payload = x_get_authenticated_user(
                client_path=args.client_config,
                token_path=args.token,
            )
            if args.format == "json":
                return json.dumps(payload, indent=2)
            return _render_x_user_text(payload)

        if command == "x-user":
            payload = x_lookup_user_by_username(
                username=args.username,
                client_path=args.client_config,
                token_path=args.token,
            )
            if args.format == "json":
                return json.dumps(payload, indent=2)
            return _render_x_user_text(payload)

        if command == "x-posts":
            payload = x_get_user_posts(
                client_path=args.client_config,
                token_path=args.token,
                username=args.username,
                max_results=args.limit,
            )
            if args.format == "json":
                return json.dumps(payload, indent=2)
            title = "X posts"
            if args.username:
                title = f"X posts for @{args.username.lstrip('@')}"
            return _render_x_posts_text(payload, title=title)

        if command == "x-home":
            payload = x_get_home_timeline(
                client_path=args.client_config,
                token_path=args.token,
                max_results=args.limit,
            )
            if args.format == "json":
                return json.dumps(payload, indent=2)
            return _render_x_posts_text(payload, title="X home timeline")

        if command == "x-post":
            payload = x_create_post(
                client_path=args.client_config,
                token_path=args.token,
                text=args.text,
            )
            if args.format == "json":
                return json.dumps(payload, indent=2)
            return _render_x_post_action_text(payload, action="post")

        if command == "x-delete-post":
            payload = x_delete_post(
                client_path=args.client_config,
                token_path=args.token,
                post_id=args.id,
            )
            if args.format == "json":
                return json.dumps(payload, indent=2)
            return _render_x_post_action_text(payload, action="delete")

        if command == "x-package-create":
            result = create_x_article_package(
                connection,
                title=args.title,
                angle=args.angle,
                audience=args.audience,
                thesis=args.thesis,
                key_points=args.points,
                cta=args.cta,
                voice=args.voice,
                visual_style=args.visual_style,
                tags=args.tags,
            )
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_x_package_create_text(result)

        if command == "x-content":
            rows = store.list_x_content_items(
                connection,
                kind=args.kind,
                status=args.status,
                limit=args.limit,
            )
            records = [_x_content_record(row) for row in rows]
            if args.format == "json":
                return json.dumps(records, indent=2)
            return _render_x_content_text(records)

        if command == "x-content-show":
            row = store.get_x_content_item(connection, args.id)
            if row is None:
                raise ValueError(f"x content item #{args.id} was not found")
            record = _x_content_record(row)
            children = [_x_content_record(child) for child in store.list_x_content_children(connection, args.id)]
            assets = [_x_media_record(asset) for asset in store.list_x_media_assets(connection, content_item_id=args.id, limit=None)]
            if args.format == "json":
                return json.dumps({"record": record, "children": children, "assets": assets}, indent=2)
            return _render_x_content_detail_text(record, children, assets)

        if command == "x-media":
            rows = store.list_x_media_assets(
                connection,
                content_item_id=args.content_id,
                asset_kind=args.kind,
                status=args.status,
                limit=args.limit,
            )
            records = [_x_media_record(row) for row in rows]
            if args.format == "json":
                return json.dumps(records, indent=2)
            return _render_x_media_text(records)

        if command == "x-generate-image":
            result = generate_x_media_asset(
                connection,
                asset_id=args.asset_id,
                provider=args.provider,
                model=args.model,
                size=args.size,
                quality=args.quality,
                output_format=args.output_format,
                background=args.background,
                moderation=args.moderation,
                aspect_ratio=args.aspect_ratio,
                resolution=args.resolution,
            )
            if args.format == "json":
                return json.dumps(result, indent=2)
            return _render_x_image_generate_text(result)

        if command == "comms":
            rows = store.list_communications(
                connection,
                status=args.status,
                source=args.source,
                category=args.category,
                limit=args.limit,
            )
            records = [_communication_record(row) for row in rows]
            if args.format == "json":
                return json.dumps(records, indent=2)
            return _render_communications_text(records)

        if command == "comms-summary":
            summary = store.summarize_communications(
                connection,
                source=args.source,
                status=args.status,
                category=args.category,
            )
            if args.format == "json":
                return json.dumps(summary, indent=2)
            return _render_communications_summary_text(summary)

        if command == "extract-profile-context":
            result = extract_profile_context_items(
                connection,
                source=args.source,
                status=args.status,
                category=args.category,
                limit=args.limit,
                replace_existing=not args.keep_existing,
            )
            return _render_sync_summary("Profile context extraction complete", result)

        if command == "profile-context":
            rows = store.list_profile_context_items(
                connection,
                subject_key=args.subject_key,
                item_type=args.item_type,
                status=args.status,
                source=args.source,
                limit=args.limit,
            )
            records = [_profile_context_record(row) for row in rows]
            if args.format == "json":
                return json.dumps(records, indent=2)
            return _render_profile_context_text(records)

        if command == "profile-context-summary":
            summary = store.summarize_profile_context(
                connection,
                subject_key=args.subject_key,
                item_type=args.item_type,
                status=args.status,
                source=args.source,
            )
            if args.format == "json":
                return json.dumps(summary, indent=2)
            return _render_profile_context_summary_text(summary)

        if command == "profile-review-next":
            row = store.next_profile_context_review_item(
                connection,
                subject_key=args.subject_key,
                item_type=args.item_type,
                source=args.source,
            )
            if row is None:
                return "No candidate profile items are waiting for review."
            record = _profile_context_record(row)
            if args.format == "json":
                return json.dumps(record, indent=2)
            return _render_profile_review_item_text(record)

        if command == "profile-approve":
            record = approve_profile_context_item(
                connection,
                item_id=args.id,
                title=args.title,
                record_status=args.record_status,
                notes=args.notes,
            )
            if args.format == "json":
                return json.dumps(record, indent=2)
            return _render_profile_record_detail_text(record)

        if command == "profile-reject":
            result = reject_profile_context_item(
                connection,
                item_id=args.id,
                notes=args.notes,
            )
            if args.format == "json":
                return json.dumps(result, indent=2)
            return (
                f"Rejected profile item #{result['id']}: "
                f"{result['item_type']} {result['title']}"
            )

        if command == "profile-merge":
            record = merge_profile_context_item(
                connection,
                item_id=args.id,
                record_id=args.record_id,
                notes=args.notes,
            )
            if args.format == "json":
                return json.dumps(record, indent=2)
            return _render_profile_record_detail_text(record)

        if command == "profile-records":
            rows = store.list_profile_records(
                connection,
                subject_key=args.subject_key,
                item_type=args.item_type,
                status=args.status,
                limit=args.limit,
            )
            records = [_profile_record_record(row) for row in rows]
            if args.format == "json":
                return json.dumps(records, indent=2)
            return _render_profile_records_text(records)

        if command == "profile-record-show":
            record = store.get_profile_record(connection, args.id)
            if record is None:
                raise ValueError(f"profile record #{args.id} was not found")
            payload = get_profile_record_payload(connection, args.id)
            if args.format == "json":
                return json.dumps(payload, indent=2)
            return _render_profile_record_detail_text(payload)

        if command == "profile-record-summary":
            summary = store.summarize_profile_records(
                connection,
                subject_key=args.subject_key,
                item_type=args.item_type,
                status=args.status,
            )
            if args.format == "json":
                return json.dumps(summary, indent=2)
            return _render_profile_record_summary_text(summary)

        if command == "profile-alerts":
            alerts = list_profile_alerts(
                connection,
                subject_key=args.subject_key,
                item_type=args.item_type,
                status=args.status,
                limit=args.limit,
            )
            if args.format == "json":
                return json.dumps(alerts, indent=2)
            return _render_profile_alerts_text(alerts)

        if command == "ingest-profile-attachments":
            result = ingest_profile_attachments(
                connection,
                credentials_path=args.credentials,
                token_path=args.token,
                subject_key=args.subject_key,
                item_type=args.item_type,
                status=args.status,
                scope=args.scope,
                limit=args.limit,
                include_inline=args.include_inline,
                force=args.force,
            )
            return _render_sync_summary("Profile attachment ingest complete", result)

        if command == "backfill-profile-attachments":
            result = backfill_profile_attachments_until_exhausted(
                connection,
                credentials_path=args.credentials,
                token_path=args.token,
                scope=args.scope,
                max_results=args.max_results,
                reset_cursor=args.reset_cursor,
                include_inline=args.include_inline,
                force=args.force,
                max_runs=None if args.max_runs <= 0 else args.max_runs,
            )
            return _render_sync_summary("Profile attachment backfill complete", result)

        if command == "attachments":
            rows = store.list_communication_attachments(
                connection,
                communication_id=args.communication_id,
                ingest_status=args.status,
                source=args.source,
                limit=args.limit,
            )
            records = [_attachment_record(row) for row in rows]
            if args.format == "json":
                return json.dumps(records, indent=2)
            return _render_attachments_text(records)

        if command == "attachment-summary":
            summary = store.summarize_communication_attachments(
                connection,
                ingest_status=args.status,
                source=args.source,
            )
            if args.format == "json":
                return json.dumps(summary, indent=2)
            return _render_attachment_summary_text(summary)

        if command == "profile-review-set":
            store.update_profile_context_item_status(
                connection,
                item_id=args.id,
                status=args.status,
                review_notes=args.notes,
            )
            row = store.get_profile_context_item(connection, args.id)
            if row is None:
                return f"Updated profile item #{args.id}"
            record = _profile_context_record(row)
            return (
                f"Updated profile item #{record['id']} to {record['status']}: "
                f"{record['item_type']} {record['title']}"
            )

        if command == "reclassify-gmail":
            result = reclassify_gmail_records(
                connection,
                status=args.status,
                category=args.category,
                limit=args.limit,
                rewrite_status=args.rewrite_status,
            )
            return _render_sync_summary("Gmail reclassify complete", result)

        if command == "seed-demo":
            seeded = store.seed_demo(connection)
            if seeded:
                return f"Seeded demo data into {args.db}"
            return f"Skipped demo seed because {args.db} already has data"

        if command == "agenda":
            trace_run_id = tracing.start_trace_run(
                connection,
                trace_type="agenda_render",
                metadata={
                    "start_date": args.start,
                    "days": args.days,
                    "format": args.format,
                },
            )
            try:
                agenda = build_agenda(connection, start_day=args.start, days=args.days)
                item_counts: dict[str, int] = {}
                for day in agenda["days"]:
                    for item in day["items"]:
                        item_counts[item["type"]] = item_counts.get(item["type"], 0) + 1
                        tracing.append_trace_event(
                            connection,
                            run_id=trace_run_id,
                            event_type="agenda_item_selected",
                            entity_key=f"{day['date']}:{item['type']}:{item['title']}",
                            payload={
                                "date": day["date"],
                                "label": day["label"],
                                "item": item,
                            },
                        )

                tracing.finish_trace_run(
                    connection,
                    run_id=trace_run_id,
                    status="completed",
                    summary={
                        "start_date": agenda["start_date"],
                        "end_date": agenda["end_date"],
                        "days": args.days,
                        "item_counts": item_counts,
                    },
                )
                if args.format == "json":
                    return render_agenda_json(agenda)
                return render_agenda_text(agenda)
            except Exception as exc:
                tracing.finish_trace_run(
                    connection,
                    run_id=trace_run_id,
                    status="failed",
                    summary={"error": str(exc)},
                )
                raise

        if command == "add-org":
            organization_id = store.add_organization(
                connection,
                name=args.name,
                category=args.category,
                notes=args.notes,
            )
            return f"Added organization #{organization_id}: {args.name}"

        if command == "add-event":
            start_at, end_at = _normalize_event_bounds(args.start, args.end, args.all_day)
            event_id = store.add_event(
                connection,
                title=args.title,
                start_at=start_at,
                end_at=end_at,
                organization_name=args.organization,
                location=args.location,
                kind=args.kind,
                status=args.status,
                source=args.source,
                notes=args.notes,
                all_day=args.all_day,
            )
            return f"Added event #{event_id}: {args.title}"

        if command == "add-comm":
            communication_id = store.add_communication(
                connection,
                subject=args.subject,
                channel=args.channel,
                happened_at=args.happened_at,
                follow_up_at=args.follow_up_at,
                person=args.person,
                organization_name=args.organization,
                notes=args.notes,
            )
            return f"Added communication #{communication_id}: {args.subject}"

        if command == "add-item":
            item_id = store.add_list_item(
                connection,
                list_name=args.list_name,
                title=args.title,
                notes=args.notes,
            )
            return f"Added {args.list_name} item #{item_id}: {args.title}"

        if command == "list-items":
            rows = store.list_list_items(
                connection,
                list_name=args.list_name,
                status=args.status,
                limit=args.limit,
            )
            records = [_list_item_record(row) for row in rows]
            if args.format == "json":
                return json.dumps(records, indent=2)
            return _render_list_items_text(records)

        if command == "done-item":
            store.set_list_item_status(connection, item_id=args.id, status="done")
            row = store.get_list_item(connection, args.id)
            if row is None:
                return f"Marked list item #{args.id} done"
            return f"Marked {row['list_name']} item #{args.id} done: {row['title']}"

        if command == "add-routine":
            day_of_week: Optional[int] = None
            if args.cadence == "weekly":
                if not args.day:
                    raise ValueError("--day is required for weekly routines")
                day_of_week = store.parse_day_name(args.day)

            routine_id = store.add_routine(
                connection,
                name=args.name,
                cadence=args.cadence,
                day_of_week=day_of_week,
                start_time=args.start_time,
                duration_minutes=args.duration,
                notes=args.notes,
            )
            return f"Added routine #{routine_id}: {args.name}"

        if command == "done-comm":
            store.mark_communication_done(connection, args.id)
            return f"Marked communication #{args.id} done"

    raise ValueError(f"unknown command: {command}")


def main() -> None:
    credentials.load_registered_secrets()
    parser = build_parser()
    args = parser.parse_args()
    try:
        print(run(args))
    except Exception as exc:
        raise SystemExit(str(exc))
