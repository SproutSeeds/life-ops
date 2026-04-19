from __future__ import annotations

import hashlib
import hmac
import os
import sys
import tempfile
import json
import threading
import unittest
import urllib.request
from datetime import date, datetime, time
from http.server import HTTPServer
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops.calendar import (
    build_calendar_day,
    build_calendar_history,
    rollover_calendar_day,
    save_calendar_day,
)
from life_ops import mail_ui
from life_ops import store


class CalendarTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "calendar.db"
        self.connection = store.open_db(self.db_path)

    def tearDown(self) -> None:
        self.connection.close()
        self.temp_dir.cleanup()

    def test_calendar_day_combines_entries_agenda_notes_and_lists(self) -> None:
        target_day = date(2026, 4, 14)
        store.update_calendar_day_note(
            self.connection,
            day=target_day,
            intention="Keep the system honest.",
            mood="focused",
        )
        entry_id = store.add_calendar_entry(
            self.connection,
            entry_date=target_day,
            title="Design homemade calendar",
            entry_type="task",
            priority="high",
            start_time="09:30",
            tags=["life-ops", "calendar"],
        )
        store.add_event(
            self.connection,
            title="Calendar planning block",
            start_at=datetime.combine(target_day, time(11, 0)),
            end_at=datetime.combine(target_day, time(12, 0)),
            organization_name="Life Ops",
        )
        store.add_list_item(
            self.connection,
            list_name="professional",
            title="Review calendar history view",
        )

        payload = build_calendar_day(self.connection, target_day=target_day)

        self.assertEqual("2026-04-14", payload["date"])
        self.assertEqual("Keep the system honest.", payload["day_note"]["intention"])
        self.assertEqual([entry_id], [entry["id"] for entry in payload["need_to_get_to"]])
        self.assertEqual("Design homemade calendar", payload["entries"][0]["title"])
        self.assertIn("Calendar planning block", [item["title"] for item in payload["agenda"]["items"]])
        self.assertIn("Review calendar history view", [item["title"] for item in payload["open_list_items"]])

    def test_save_calendar_day_creates_immutable_snapshot_used_by_history(self) -> None:
        target_day = date(2026, 4, 14)
        store.add_calendar_entry(
            self.connection,
            entry_date=target_day,
            title="Ship calendar save",
            status="done",
        )

        saved = save_calendar_day(
            self.connection,
            target_day=target_day,
            title="End of day save",
            summary="Calendar save shipped.",
        )
        history = build_calendar_history(self.connection, start_day=target_day, days=1)

        self.assertGreater(saved["snapshot_id"], 0)
        self.assertEqual("Calendar save shipped.", history["days"][0]["snapshots"][0]["summary"])
        self.assertEqual(1, history["stats"]["snapshots"])
        self.assertEqual(1, history["stats"]["done_entries"])

    def test_rollover_defers_source_entry_and_creates_carry_forward(self) -> None:
        source_day = date(2026, 4, 14)
        target_day = date(2026, 4, 15)
        original_id = store.add_calendar_entry(
            self.connection,
            entry_date=source_day,
            title="Unfinished grant note",
            status="planned",
            priority="urgent",
            notes="Needs one more pass.",
        )

        result = rollover_calendar_day(
            self.connection,
            source_day=source_day,
            target_day=target_day,
        )
        source_payload = build_calendar_day(self.connection, target_day=source_day)
        target_payload = build_calendar_day(self.connection, target_day=target_day)

        self.assertEqual([original_id], result["deferred_entry_ids"])
        self.assertEqual(1, result["rolled_count"])
        self.assertEqual("deferred", source_payload["entries"][0]["status"])
        self.assertEqual("carry_forward", target_payload["entries"][0]["type"])
        self.assertEqual(original_id, target_payload["entries"][0]["source_id"])
        self.assertEqual("Unfinished grant note", target_payload["entries"][0]["title"])

    def test_mail_ui_exposes_calendar_page_and_api(self) -> None:
        handler = mail_ui._make_handler(db_path=self.db_path, limit=20)
        server = HTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base_url = f"http://127.0.0.1:{server.server_port}"
        try:
            with urllib.request.urlopen(f"{base_url}/calendar?date=2026-04-14") as response:
                html = response.read().decode("utf-8")
            self.assertIn("Life Ops Calendar", html)
            self.assertIn("/api/calendar/day", html)

            request = urllib.request.Request(
                f"{base_url}/api/calendar/entries",
                data=json.dumps(
                    {
                        "date": "2026-04-14",
                        "title": "Track calendar from UI",
                        "type": "task",
                        "priority": "high",
                    }
                ).encode("utf-8"),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(request) as response:
                created = json.loads(response.read().decode("utf-8"))
            self.assertTrue(created["ok"])
            entry_id = int(created["day"]["entries"][0]["id"])

            request = urllib.request.Request(
                f"{base_url}/api/calendar/entries/{entry_id}/status",
                data=json.dumps({"status": "done", "date": "2026-04-14"}).encode("utf-8"),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(request) as response:
                updated = json.loads(response.read().decode("utf-8"))
            self.assertEqual("done", updated["day"]["entries"][0]["status"])

            request = urllib.request.Request(
                f"{base_url}/api/calendar/day-save",
                data=json.dumps({"date": "2026-04-14", "summary": "UI save works."}).encode("utf-8"),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(request) as response:
                saved = json.loads(response.read().decode("utf-8"))
            self.assertGreater(int(saved["snapshot_id"]), 0)
            self.assertEqual("UI save works.", saved["summary"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_signed_frg_booking_webhook_creates_calendar_hold_and_cmail_draft(self) -> None:
        secret = "test-frg-booking-secret"
        payload = {
            "event": "booking.paid",
            "booking": {
                "id": "frg-booking-test-001",
                "name": "Ada Lovelace",
                "email": "ada@example.com",
                "focus": "Research collaboration",
                "durationMinutes": 45,
                "selectedDate": "2026-04-18",
                "selectedTime": "1:00 PM",
                "selectedSlotLabel": "Apr 18, 2026, 1:00 PM",
                "timezone": "America/Chicago",
                "notes": "Discuss proof-campaign structure.",
            },
            "payment": {
                "amountTotalCents": 7500,
                "stripeCheckoutSessionId": "cs_test_booking",
            },
            "zoomUrl": "https://zoom.example/frg",
        }
        raw_body = json.dumps(payload).encode("utf-8")
        timestamp = str(int(datetime.now().timestamp()))
        signature = hmac.new(
            secret.encode("utf-8"),
            f"{timestamp}.".encode("utf-8") + raw_body,
            hashlib.sha256,
        ).hexdigest()
        handler = mail_ui._make_handler(db_path=self.db_path, limit=20)
        server = HTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base_url = f"http://127.0.0.1:{server.server_port}"
        try:
            request = urllib.request.Request(
                f"{base_url}/api/frg/bookings",
                data=raw_body,
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "X-FRG-Booking-Timestamp": timestamp,
                    "X-FRG-Booking-Signature": f"v1={signature}",
                },
            )
            with mock.patch.dict(os.environ, {"FRG_BOOKING_WEBHOOK_SECRET": secret}):
                with urllib.request.urlopen(request) as response:
                    created = json.loads(response.read().decode("utf-8"))

                self.assertTrue(created["ok"])
                self.assertFalse(created["duplicate"])
                self.assertGreater(int(created["calendar_entry_id"]), 0)
                self.assertGreater(int(created["cmail_draft_id"]), 0)

                day_payload = build_calendar_day(self.connection, target_day=date(2026, 4, 18))
                self.assertEqual(1, len(day_payload["entries"]))
                entry = day_payload["entries"][0]
                self.assertEqual("event", entry["type"])
                self.assertEqual("13:00", entry["start_time"])
                self.assertEqual("13:45", entry["end_time"])
                self.assertIn("FRG booking: Ada Lovelace", entry["title"])
                self.assertIn("Stripe session: cs_test_booking", entry["notes"])

                drafts = mail_ui.list_cmail_drafts(db_path=self.db_path)
                self.assertEqual(1, len(drafts))
                self.assertEqual("Ada Lovelace <ada@example.com>", drafts[0]["to"])
                self.assertIn("FRG booking confirmed", drafts[0]["subject"])

                duplicate_request = urllib.request.Request(
                    f"{base_url}/api/frg/bookings",
                    data=raw_body,
                    method="POST",
                    headers={
                        "Content-Type": "application/json",
                        "X-FRG-Booking-Timestamp": timestamp,
                        "X-FRG-Booking-Signature": f"v1={signature}",
                    },
                )
                with urllib.request.urlopen(duplicate_request) as response:
                    duplicate = json.loads(response.read().decode("utf-8"))
                self.assertTrue(duplicate["duplicate"])
                self.assertIsNone(duplicate["cmail_draft_id"])
                self.assertEqual(1, len(build_calendar_day(self.connection, target_day=date(2026, 4, 18))["entries"]))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
