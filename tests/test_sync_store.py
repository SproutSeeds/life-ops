from __future__ import annotations

import sys
import tempfile
import unittest
from unittest.mock import patch
from datetime import datetime, time, date, timezone
from pathlib import Path
import os

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops.google_sync import (
    _apply_gmail_candidates,
    _calendar_event_times,
    _canonicalize_calendar_ids,
    _is_actionable_gmail_message,
    backfill_gmail_until_exhausted,
    DEFAULT_GMAIL_CATEGORY_SWEEPS,
    _gmail_next_before_ts,
    _gmail_query_with_before_ts,
    reclassify_gmail_records,
    sync_gmail_corpus,
    sync_gmail_category_pass,
)
from life_ops import mail_vault, store, tracing, vault_crypto


class SyncStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "sync-test.db"
        self.vault_root = Path(self.temp_dir.name) / "attachments"
        self.original_master_key = os.environ.get(vault_crypto.MASTER_KEY_NAME)
        os.environ[vault_crypto.MASTER_KEY_NAME] = vault_crypto._b64url_encode(b"c" * 32)
        self.connection = store.open_db(self.db_path)

    def tearDown(self) -> None:
        self.connection.close()
        if self.original_master_key is None:
            os.environ.pop(vault_crypto.MASTER_KEY_NAME, None)
        else:
            os.environ[vault_crypto.MASTER_KEY_NAME] = self.original_master_key
        self.temp_dir.cleanup()

    def test_upsert_event_from_sync_updates_existing_row(self) -> None:
        event_id = store.upsert_event_from_sync(
            self.connection,
            source="google-calendar",
            external_id="primary:event-1",
            title="Planning",
            start_at=datetime(2026, 3, 24, 9, 0),
            end_at=datetime(2026, 3, 24, 10, 0),
            all_day=False,
            organization_name="Primary Work",
        )

        updated_id = store.upsert_event_from_sync(
            self.connection,
            source="google-calendar",
            external_id="primary:event-1",
            title="Planning moved",
            start_at=datetime(2026, 3, 24, 10, 0),
            end_at=datetime(2026, 3, 24, 11, 0),
            all_day=False,
            organization_name="Primary Work",
        )

        self.assertEqual(event_id, updated_id)
        row = self.connection.execute("SELECT title, start_at FROM events WHERE id = ?", (event_id,)).fetchone()
        self.assertEqual("Planning moved", row["title"])
        self.assertEqual("2026-03-24T10:00", row["start_at"])

    def test_upsert_communication_preserves_done_status(self) -> None:
        communication_id = store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="msg-1",
            subject="Reply please",
            channel="email",
            happened_at=datetime(2026, 3, 24, 9, 0),
            follow_up_at=datetime(2026, 3, 24, 11, 0),
            person="Dana",
        )
        store.mark_communication_done(self.connection, communication_id)

        store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="msg-1",
            subject="Reply please updated",
            channel="email",
            happened_at=datetime(2026, 3, 24, 9, 15),
            follow_up_at=datetime(2026, 3, 24, 14, 0),
            person="Dana",
        )

        row = self.connection.execute(
            "SELECT status, follow_up_at, subject FROM communications WHERE id = ?",
            (communication_id,),
        ).fetchone()
        self.assertEqual("done", row["status"])
        self.assertEqual("2026-03-24T11:00", row["follow_up_at"])
        self.assertEqual("Reply please updated", row["subject"])

    def test_list_items_support_personal_professional_and_done_status(self) -> None:
        personal_item_id = store.add_list_item(
            self.connection,
            list_name="personal",
            title="Buy new dish sponge",
        )
        professional_item_id = store.add_list_item(
            self.connection,
            list_name="professional",
            title="Reply to partner note",
            notes="Check the partnership draft before Friday.",
        )

        open_items = store.list_list_items(self.connection, status="open", limit=20)
        self.assertEqual(
            [personal_item_id, professional_item_id],
            [int(row["id"]) for row in open_items],
        )

        store.set_list_item_status(self.connection, item_id=professional_item_id, status="done")

        professional_done = store.list_list_items(
            self.connection,
            list_name="professional",
            status="done",
            limit=20,
        )
        self.assertEqual(1, len(professional_done))
        self.assertEqual(professional_item_id, int(professional_done[0]["id"]))
        self.assertEqual("done", str(professional_done[0]["status"]))
        self.assertTrue(str(professional_done[0]["completed_at"]))

        personal_open = store.list_list_items(
            self.connection,
            list_name="personal",
            status="open",
            limit=20,
        )
        self.assertEqual(1, len(personal_open))
        self.assertEqual(personal_item_id, int(personal_open[0]["id"]))
        self.assertEqual("Buy new dish sponge", str(personal_open[0]["title"]))

    def test_life_ops_home_controls_default_data_and_config_paths(self) -> None:
        home_root = Path(self.temp_dir.name) / "lifeops-home"
        with patch.dict(os.environ, {"LIFE_OPS_HOME": str(home_root)}, clear=False):
            expected = home_root.resolve(strict=False)
            self.assertEqual(expected / "data" / "life_ops.db", store.default_db_path())
            self.assertEqual(expected / "data" / "attachments", store.attachment_vault_root())
            self.assertEqual(expected / "data" / "x_media", store.x_media_root())
            self.assertEqual(expected / "config", store.config_root())

    def test_purge_deleted_communications_removes_archived_rows_and_vault_artifacts(self) -> None:
        raw_relative_path, _ = mail_vault.write_encrypted_vault_file(
            vault_root=self.vault_root,
            relative_dir=Path("cloudflare_email") / "communication-1" / "raw",
            logical_filename="message.eml",
            raw_bytes=b"raw-message",
            metadata={"kind": "raw"},
        )
        attachment_relative_path, _ = mail_vault.write_encrypted_vault_file(
            vault_root=self.vault_root,
            relative_dir=Path("cloudflare_email") / "communication-1" / "attachments",
            logical_filename="proof.txt",
            raw_bytes=b"proof body",
            metadata={"kind": "attachment"},
        )
        extracted_text_relative_path, _ = mail_vault.write_encrypted_vault_file(
            vault_root=self.vault_root,
            relative_dir=Path("cloudflare_email") / "communication-1" / "attachments",
            logical_filename="proof.txt.extracted.txt",
            raw_bytes=b"proof body extracted",
            metadata={"kind": "extracted_text"},
        )

        communication_id = store.upsert_communication_from_sync(
            self.connection,
            source="cloudflare_email",
            external_id="msg-delete-1",
            subject="Delete me later",
            channel="email",
            happened_at=datetime(2026, 2, 1, 10, 0),
            follow_up_at=None,
            direction="inbound",
            person="Archive Test",
            external_from="archive@example.com",
            raw_relative_path=raw_relative_path,
        )
        store.upsert_communication_attachment(
            self.connection,
            external_key="att-delete-1",
            communication_id=communication_id,
            source="cloudflare_email",
            external_message_id="msg-delete-1",
            external_attachment_id="att-1",
            part_id="1",
            filename="proof.txt",
            mime_type="text/plain",
            size=10,
            relative_path=attachment_relative_path,
            extracted_text="proof body extracted",
            extracted_text_path=extracted_text_relative_path,
            extraction_method="unit_test",
            ingest_status="saved",
            sha256="abc123",
        )
        store.set_communication_status(self.connection, communication_id=communication_id, status="deleted")
        self.connection.execute(
            "UPDATE communications SET deleted_at = ? WHERE id = ?",
            ("2026-02-15T00:00:00Z", communication_id),
        )
        self.connection.commit()

        result = store.purge_deleted_communications(
            self.connection,
            now=datetime(2026, 3, 30, 12, 0, tzinfo=timezone.utc),
            vault_root=self.vault_root,
        )

        self.assertEqual(1, result["purged_count"])
        self.assertEqual(3, result["artifact_count"])
        self.assertIsNone(store.get_communication_by_id(self.connection, communication_id))
        self.assertEqual([], store.list_communication_attachments(self.connection, communication_id=communication_id))
        self.assertFalse((self.vault_root / raw_relative_path).exists())
        self.assertFalse((self.vault_root / attachment_relative_path).exists())
        self.assertFalse((self.vault_root / extracted_text_relative_path).exists())

    def test_calendar_all_day_event_uses_inclusive_local_day(self) -> None:
        start_at, end_at, all_day = _calendar_event_times(
            {
                "start": {"date": "2026-03-24"},
                "end": {"date": "2026-03-25"},
            }
        )

        self.assertTrue(all_day)
        self.assertEqual(datetime.combine(date(2026, 3, 24), time(0, 0)), start_at)
        self.assertEqual(datetime.combine(date(2026, 3, 24), time(23, 59)), end_at)

    def test_primary_alias_is_canonicalized_and_deduped(self) -> None:
        canonical_ids, aliases = _canonicalize_calendar_ids(
            ["primary", "codyshanemitchell@gmail.com", "family@example.com"],
            "codyshanemitchell@gmail.com",
        )

        self.assertEqual(
            ["codyshanemitchell@gmail.com", "family@example.com"],
            canonical_ids,
        )
        self.assertEqual(["primary"], aliases)

    def test_delete_events_for_calendar_removes_alias_rows(self) -> None:
        store.upsert_event_from_sync(
            self.connection,
            source="google-calendar",
            external_id="primary:event-1",
            title="Alias event",
            start_at=datetime(2026, 3, 24, 9, 0),
            end_at=datetime(2026, 3, 24, 10, 0),
            all_day=False,
            external_calendar_id="primary",
        )
        store.upsert_event_from_sync(
            self.connection,
            source="google-calendar",
            external_id="actual:event-1",
            title="Actual event",
            start_at=datetime(2026, 3, 24, 9, 0),
            end_at=datetime(2026, 3, 24, 10, 0),
            all_day=False,
            external_calendar_id="actual",
        )

        deleted = store.delete_events_for_calendar(
            self.connection,
            source="google-calendar",
            external_calendar_id="primary",
        )

        remaining = self.connection.execute(
            "SELECT external_calendar_id FROM events ORDER BY external_calendar_id"
        ).fetchall()

        self.assertEqual(1, deleted)
        self.assertEqual(["actual"], [row["external_calendar_id"] for row in remaining])

    def test_delete_open_communications_not_in_ids_removes_stale_gmail_rows(self) -> None:
        store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="keep-me",
            subject="Keep me",
            channel="email",
            happened_at=datetime(2026, 3, 24, 9, 0),
            follow_up_at=datetime(2026, 3, 24, 10, 0),
        )
        store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="delete-me",
            subject="Delete me",
            channel="email",
            happened_at=datetime(2026, 3, 24, 9, 30),
            follow_up_at=datetime(2026, 3, 24, 11, 0),
        )

        deleted = store.delete_open_communications_not_in_ids(
            self.connection,
            source="gmail",
            keep_external_ids={"keep-me"},
        )
        remaining = self.connection.execute(
            "SELECT external_id FROM communications ORDER BY external_id"
        ).fetchall()

        self.assertEqual(1, deleted)
        self.assertEqual(["keep-me"], [row["external_id"] for row in remaining])

    def test_delete_open_communications_without_prefix_removes_legacy_rows(self) -> None:
        store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="thread:abc",
            subject="Thread row",
            channel="email",
            happened_at=datetime(2026, 3, 24, 9, 0),
            follow_up_at=datetime(2026, 3, 24, 10, 0),
        )
        store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="msg-legacy",
            subject="Legacy row",
            channel="email",
            happened_at=datetime(2026, 3, 24, 9, 30),
            follow_up_at=datetime(2026, 3, 24, 11, 0),
        )

        deleted = store.delete_open_communications_without_prefix(
            self.connection,
            source="gmail",
            external_id_prefix="thread:",
        )
        remaining = self.connection.execute(
            "SELECT external_id FROM communications ORDER BY external_id"
        ).fetchall()

        self.assertEqual(1, deleted)
        self.assertEqual(["thread:abc"], [row["external_id"] for row in remaining])

    def test_github_notification_is_actionable(self) -> None:
        actionable, reason = _is_actionable_gmail_message(
            {"labelIds": ["INBOX", "UNREAD"]},
            {
                "Subject": "Re: [openai/codex] Issue #14593 comment",
                "From": "GitHub <notifications@github.com>",
            },
        )

        self.assertTrue(actionable)
        self.assertIn("priority-domain", reason)

    def test_marketing_noreply_message_is_filtered(self) -> None:
        actionable, reason = _is_actionable_gmail_message(
            {"labelIds": ["INBOX", "UNREAD"]},
            {
                "Subject": "Check out your new achievement!",
                "From": "Devpost <support@devpost.com>",
                "List-Unsubscribe": "<mailto:unsubscribe@example.com>",
                "Precedence": "bulk",
            },
        )

        self.assertFalse(actionable)
        self.assertIn("mailing-list", reason)

    def test_ci_run_failed_notification_is_filtered(self) -> None:
        actionable, reason = _is_actionable_gmail_message(
            {"labelIds": ["INBOX", "UNREAD"]},
            {
                "Subject": "[SproutSeeds/RigidityCore] Run failed: CI - master (4d9ea6c)",
                "From": "GitHub <notifications@github.com>",
                "Precedence": "bulk",
            },
        )

        self.assertFalse(actionable)
        self.assertIn("non-actionable-subject", reason)

    def test_gmail_backfill_query_adds_before_timestamp(self) -> None:
        query = _gmail_query_with_before_ts("-in:chats", before_ts=1710000000)
        self.assertEqual("-in:chats before:1710000000", query)

    def test_gmail_next_before_timestamp_steps_back_one_second(self) -> None:
        next_before_ts = _gmail_next_before_ts(datetime(2026, 3, 10, 12, 0))
        self.assertEqual(int(datetime(2026, 3, 10, 12, 0).timestamp()) - 1, next_before_ts)

    def test_apply_gmail_candidates_skips_older_existing_thread(self) -> None:
        store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="thread:abc",
            subject="Recent thread",
            channel="email",
            happened_at=datetime(2026, 3, 25, 9, 0),
            follow_up_at=datetime(2026, 3, 25, 10, 0),
            status="open",
            category="billing",
            categories=["billing"],
            priority_level="normal",
            priority_score=40,
            retention_bucket="action_queue",
        )
        run_id = tracing.start_trace_run(
            self.connection,
            trace_type="gmail_backfill",
            metadata={"query": "-in:chats before:1710000000"},
        )

        summary = _apply_gmail_candidates(
            self.connection,
            trace_run_id=run_id,
            selected_threads={
                "abc": {
                    "thread_id": "abc",
                    "subject": "Older thread",
                    "happened_at": datetime(2026, 3, 20, 9, 0),
                    "follow_up_at": None,
                    "person": "Archive",
                    "notes": "Synced from Gmail.",
                    "external_from": "archive@example.com",
                    "snippet": "Older record",
                    "body_text": "Older record body",
                    "attachments": [],
                    "status": "reference",
                    "category": "record_keeping",
                    "categories": ["record_keeping"],
                    "priority_level": "low",
                    "priority_score": 5,
                    "retention_bucket": "records",
                    "classifier_version": "taxonomy-v1",
                    "triage": {"score": 0, "reasons": []},
                    "classification": {"status": "reference"},
                }
            },
            cleanup_missing_open_rows=False,
        )

        row = self.connection.execute(
            "SELECT subject, status, happened_at FROM communications WHERE source = 'gmail' AND external_id = 'thread:abc'"
        ).fetchone()

        self.assertEqual(1, summary["threads_skipped_existing_newer"])
        self.assertEqual("Recent thread", row["subject"])
        self.assertEqual("open", row["status"])
        self.assertEqual("2026-03-25T09:00", row["happened_at"])

    def test_reclassify_gmail_preserves_open_status_and_priority_by_default(self) -> None:
        store.set_sync_state(self.connection, key="gmail:user_email", value="codyshanemitchell@gmail.com")
        store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="thread:verify-1",
            subject="[ORCID] Verify your email address",
            channel="email",
            happened_at=datetime(2026, 3, 25, 9, 0),
            follow_up_at=datetime(2026, 3, 25, 10, 0),
            status="open",
            external_from="DoNotReply@verify.orcid.org",
            priority_level="normal",
            priority_score=30,
            retention_bucket="action_queue",
            category="insurance",
            categories=["insurance"],
        )

        summary = reclassify_gmail_records(self.connection)
        row = self.connection.execute(
            "SELECT status, priority_level, priority_score, category, retention_bucket FROM communications WHERE source = 'gmail' AND external_id = 'thread:verify-1'"
        ).fetchone()

        self.assertEqual(1, summary["processed"])
        self.assertEqual("open", row["status"])
        self.assertEqual("normal", row["priority_level"])
        self.assertEqual(30, int(row["priority_score"]))
        self.assertEqual("security", row["category"])
        self.assertEqual("action_queue", row["retention_bucket"])

    def test_backfill_gmail_until_exhausted_runs_until_exhausted(self) -> None:
        with patch("life_ops.google_sync.backfill_gmail") as backfill_mock:
            backfill_mock.side_effect = [
                {
                    "messages_scanned": 50,
                    "messages_actionable": 3,
                    "messages_reference": 40,
                    "threads_kept": 20,
                    "threads_open": 2,
                    "threads_reference": 18,
                    "messages_filtered": 7,
                    "threads_skipped_existing_newer": 0,
                    "backfill_exhausted": False,
                    "next_before_ts": 1700000000,
                },
                {
                    "messages_scanned": 25,
                    "messages_actionable": 1,
                    "messages_reference": 21,
                    "threads_kept": 10,
                    "threads_open": 1,
                    "threads_reference": 9,
                    "messages_filtered": 3,
                    "threads_skipped_existing_newer": 0,
                    "backfill_exhausted": True,
                    "next_before_ts": None,
                },
            ]

            summary = backfill_gmail_until_exhausted(
                self.connection,
                credentials_path=Path("config/google_credentials.json"),
                token_path=Path("data/google_token.json"),
                query="-in:chats tax",
                max_runs=None,
            )

        self.assertEqual(2, backfill_mock.call_count)
        self.assertTrue(summary["backfill_exhausted"])
        self.assertEqual("backfill_exhausted", summary["stop_reason"])
        self.assertEqual(75, summary["messages_scanned"])
        self.assertEqual(30, summary["threads_kept"])

    def test_sync_gmail_corpus_runs_recent_backfill_and_reclassify(self) -> None:
        with patch("life_ops.google_sync.sync_gmail") as sync_mock, patch(
            "life_ops.google_sync.backfill_gmail_until_exhausted"
        ) as backfill_mock, patch("life_ops.google_sync.reclassify_gmail_records") as reclassify_mock:
            sync_mock.return_value = {"messages_scanned": 10, "threads_kept": 3}
            backfill_mock.return_value = {
                "runs_completed": 2,
                "messages_scanned": 75,
                "threads_kept": 30,
                "backfill_exhausted": True,
                "last_next_before_ts": None,
                "stop_reason": "backfill_exhausted",
            }
            reclassify_mock.return_value = {"processed": 42}

            summary = sync_gmail_corpus(
                self.connection,
                credentials_path=Path("config/google_credentials.json"),
                token_path=Path("data/google_token.json"),
                backfill_max_runs=5,
                reset_backfill_cursor=True,
            )

        self.assertEqual(1, sync_mock.call_count)
        self.assertEqual(1, backfill_mock.call_count)
        self.assertEqual(1, reclassify_mock.call_count)
        self.assertEqual(2, summary["backfill_runs_completed"])
        self.assertTrue(summary["backfill_exhausted"])
        self.assertEqual("backfill_exhausted", summary["stop_reason"])
        self.assertEqual(75, summary["backfill_messages_scanned"])

    def test_sync_gmail_corpus_zero_max_runs_means_until_exhausted(self) -> None:
        with patch("life_ops.google_sync.sync_gmail") as sync_mock, patch(
            "life_ops.google_sync.backfill_gmail_until_exhausted"
        ) as backfill_mock, patch("life_ops.google_sync.reclassify_gmail_records") as reclassify_mock:
            sync_mock.return_value = {"messages_scanned": 10, "threads_kept": 3}
            backfill_mock.return_value = {
                "runs_completed": 3,
                "messages_scanned": 120,
                "threads_kept": 44,
                "backfill_exhausted": True,
                "last_next_before_ts": None,
                "stop_reason": "backfill_exhausted",
            }
            reclassify_mock.return_value = {"processed": 42}

            sync_gmail_corpus(
                self.connection,
                credentials_path=Path("config/google_credentials.json"),
                token_path=Path("data/google_token.json"),
                backfill_max_runs=0,
            )

        self.assertIsNone(backfill_mock.call_args.kwargs["max_runs"])

    def test_sync_gmail_category_pass_runs_all_sweeps_and_reclassify(self) -> None:
        with patch("life_ops.google_sync.backfill_gmail_until_exhausted") as backfill_mock, patch(
            "life_ops.google_sync.reclassify_gmail_records"
        ) as reclassify_mock:
            backfill_mock.return_value = {
                "runs_completed": 1,
                "messages_scanned": 10,
                "threads_kept": 4,
                "threads_open": 0,
                "threads_reference": 4,
                "messages_actionable": 0,
                "messages_reference": 10,
                "messages_filtered": 0,
                "threads_skipped_existing_newer": 0,
                "backfill_exhausted": True,
                "last_next_before_ts": None,
                "stop_reason": "backfill_exhausted",
            }
            reclassify_mock.return_value = {"processed": 42}

            summary = sync_gmail_category_pass(
                self.connection,
                credentials_path=Path("config/google_credentials.json"),
                token_path=Path("data/google_token.json"),
            )

        self.assertEqual(len(DEFAULT_GMAIL_CATEGORY_SWEEPS), backfill_mock.call_count)
        self.assertEqual(1, reclassify_mock.call_count)
        self.assertEqual(len(DEFAULT_GMAIL_CATEGORY_SWEEPS), summary["category_sweeps_completed"])

    def test_trace_summary_tolerates_legacy_non_json_summary_payloads(self) -> None:
        self.connection.execute(
            """
            INSERT INTO trace_runs (id, trace_type, status, metadata_json, summary_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "gmail_corpus_sync:legacy",
                "gmail_corpus_sync",
                "cancelled",
                "{}",
                "{error:cancelled interactive focused run to relaunch detached}",
            ),
        )
        self.connection.commit()

        summary = tracing.summarize_traces(
            self.connection,
            trace_type="gmail_corpus_sync",
            limit=5,
        )

        self.assertEqual("cancelled", summary["recent_runs"][0]["status"])
        self.assertEqual(
            "cancelled interactive focused run to relaunch detached",
            summary["recent_runs"][0]["summary"]["error"],
        )


if __name__ == "__main__":
    unittest.main()
