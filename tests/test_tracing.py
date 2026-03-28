from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import date, datetime, time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops import store, tracing
from life_ops.cli import run


class TracingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "trace-test.db"
        self.connection = store.open_db(self.db_path)

    def tearDown(self) -> None:
        self.connection.close()
        self.temp_dir.cleanup()

    def test_trace_lifecycle_and_export(self) -> None:
        run_id = tracing.start_trace_run(
            self.connection,
            trace_type="gmail_sync",
            metadata={"query": "in:inbox", "started_for": date(2026, 3, 25)},
        )
        tracing.append_trace_event(
            self.connection,
            run_id=run_id,
            event_type="gmail_message_triaged",
            entity_key="msg-1",
            payload={"subject": "Test", "score": 4, "seen_at": datetime(2026, 3, 25, 6, 30)},
        )
        tracing.finish_trace_run(
            self.connection,
            run_id=run_id,
            status="completed",
            summary={"messages_scanned": 1, "threads_kept": 1},
        )

        summary = tracing.summarize_traces(self.connection, trace_type="gmail_sync", limit=5)
        records = tracing.export_trace_records(self.connection, trace_type="gmail_sync", limit=10)

        self.assertEqual("gmail_sync", summary["counts"][0]["trace_type"])
        self.assertEqual(1, summary["counts"][0]["run_count"])
        self.assertEqual(1, len(records))
        self.assertEqual("gmail_message_triaged", records[0]["event_type"])
        self.assertEqual("Test", records[0]["payload"]["subject"])

    def test_cancel_running_trace_runs_marks_runs_cancelled(self) -> None:
        run_id = tracing.start_trace_run(
            self.connection,
            trace_type="profile_attachment_backfill",
            metadata={"scope": "sensitive"},
        )

        cancelled = tracing.cancel_running_trace_runs(
            self.connection,
            trace_types=["profile_attachment_backfill"],
            summary={"error": "cancelled stale profile attachment run before restart"},
        )
        summary = tracing.summarize_traces(self.connection, trace_type="profile_attachment_backfill", limit=5)

        self.assertEqual(1, cancelled)
        self.assertEqual("cancelled", summary["recent_runs"][0]["status"])
        self.assertEqual(run_id, summary["recent_runs"][0]["run_id"])

    def test_agenda_command_records_trace_run_and_items(self) -> None:
        store.add_routine(
            self.connection,
            name="Morning planning",
            cadence="daily",
            start_time="08:30",
            duration_minutes=30,
        )
        store.add_event(
            self.connection,
            title="Founder sync",
            start_at=datetime.combine(date(2026, 3, 25), time(10, 0)),
            end_at=datetime.combine(date(2026, 3, 25), time(11, 0)),
            organization_name="Primary Work",
        )

        output = run(
            SimpleNamespace(
                command="agenda",
                db=self.db_path,
                start=date(2026, 3, 25),
                days=1,
                format="text",
            )
        )

        with store.open_db(self.db_path) as check_connection:
            summary = tracing.summarize_traces(
                check_connection,
                trace_type="agenda_render",
                limit=5,
            )
            records = tracing.export_trace_records(
                check_connection,
                trace_type="agenda_render",
                limit=10,
            )

        self.assertIn("Founder sync", output)
        self.assertEqual(1, summary["counts"][0]["run_count"])
        self.assertTrue(any(record["event_type"] == "agenda_item_selected" for record in records))

    def test_gmail_heartbeat_reports_cursor_and_running_trace(self) -> None:
        store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="thread:1",
            subject="Invoice reminder",
            channel="email",
            happened_at=datetime(2026, 3, 25, 9, 0),
            follow_up_at=None,
            status="reference",
            category="billing",
            categories=["billing"],
            priority_level="low",
            priority_score=5,
            retention_bucket="financial_records",
        )
        store.set_sync_state(self.connection, "gmail:user_email", "codyshanemitchell@gmail.com")
        store.set_sync_state(self.connection, "gmail:last_sync_at", "2026-03-25T14:43")
        store.set_sync_state(self.connection, "gmail_backfill:query", "-in:chats")
        store.set_sync_state(self.connection, "gmail_backfill:last_sync_at", "2026-03-25T14:45")
        store.set_sync_state(self.connection, "gmail_backfill:next_before_ts", "1667765689")

        running_run_id = tracing.start_trace_run(
            self.connection,
            trace_type="gmail_backfill",
            metadata={"query": "-in:chats before:1667765689"},
        )
        completed_reclassify_id = tracing.start_trace_run(
            self.connection,
            trace_type="gmail_reclassify",
            metadata={},
        )
        tracing.finish_trace_run(
            self.connection,
            run_id=completed_reclassify_id,
            status="completed",
            summary={"processed": 10, "changed_primary_category": 2},
        )

        output = run(
            SimpleNamespace(
                command="gmail-heartbeat",
                db=self.db_path,
                format="text",
            )
        )

        self.assertIn("codyshanemitchell@gmail.com", output)
        self.assertIn("corpus_total: 1", output)
        self.assertIn("backfill_cursor_at: 2022-11-06T14:14:49", output)
        self.assertIn(running_run_id, output)

    def test_profile_attachment_heartbeat_reports_cursor_and_running_trace(self) -> None:
        communication_id = store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="thread:attachment-1",
            subject="Insurance card attached",
            channel="email",
            happened_at=datetime(2026, 3, 25, 12, 0),
            follow_up_at=None,
            status="reference",
            category="insurance",
            categories=["insurance", "record_keeping"],
        )
        store.upsert_profile_context_item(
            self.connection,
            external_key="profile:test:1",
            subject_key="self",
            item_type="insurance_record",
            title="Insurance record",
            source="gmail",
            communication_id=communication_id,
            happened_at=datetime(2026, 3, 25, 12, 0),
            confidence=80,
            status="candidate",
            details={},
            evidence=[],
        )
        store.upsert_communication_attachment(
            self.connection,
            external_key="attachment:test:1",
            communication_id=communication_id,
            source="gmail",
            external_message_id="msg-1",
            external_attachment_id="att-1",
            part_id="2",
            filename="insurance.pdf",
            mime_type="application/pdf",
            size=123,
            relative_path="gmail/test/insurance.pdf",
            extracted_text="policy number",
            extracted_text_path="gmail/test/insurance.pdf.txt",
            extraction_method="pdf_text",
            ingest_status="extracted",
            error_text="",
            sha256="abc",
        )
        store.set_sync_state(self.connection, "profile_attachment_backfill:scope", "sensitive")
        store.set_sync_state(self.connection, "profile_attachment_backfill:last_sync_at", "2026-03-25T17:10")
        store.set_sync_state(self.connection, "profile_attachment_backfill:next_happened_at", "2024-12-26T15:25")
        store.set_sync_state(self.connection, "profile_attachment_backfill:next_communication_id", "17662")

        running_run_id = tracing.start_trace_run(
            self.connection,
            trace_type="profile_attachment_backfill",
            metadata={"scope": "sensitive"},
        )

        output = run(
            SimpleNamespace(
                command="profile-attachment-heartbeat",
                db=self.db_path,
                format="text",
            )
        )

        self.assertIn("scope: sensitive", output)
        self.assertIn("attachment_total: 1", output)
        self.assertIn("next_happened_at: 2024-12-26T15:25", output)
        self.assertIn(running_run_id, output)


if __name__ == "__main__":
    unittest.main()
