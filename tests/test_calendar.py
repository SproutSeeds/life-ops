from __future__ import annotations

import hashlib
import hmac
import os
import sys
import tempfile
import json
import threading
import unittest
import urllib.error
import urllib.request
from datetime import date, datetime, time
from http.server import HTTPServer
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops.calendar import (
    build_calendar_day,
    build_calendar_history,
    build_calendar_range,
    build_day_sheet,
    render_day_sheet_html,
    render_day_sheet_latex,
    render_day_sheet_text,
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

    def test_day_sheet_groups_needs_by_section_and_priority_for_printing(self) -> None:
        target_day = date(2026, 4, 20)
        store.update_calendar_day_note(
            self.connection,
            day=target_day,
            intention="Keep one page honest.",
            energy="steady",
        )
        store.add_routine(
            self.connection,
            name="Morning planning",
            cadence="daily",
            start_time="08:30",
            duration_minutes=30,
        )
        store.add_event(
            self.connection,
            title="Clinic call",
            start_at=datetime.combine(target_day, time(11, 0)),
            end_at=datetime.combine(target_day, time(11, 30)),
            organization_name="Health",
        )
        store.add_calendar_entry(
            self.connection,
            entry_date=target_day,
            title="GitHub: fix red main CI",
            priority="urgent",
            list_name="professional",
            start_time="09:15",
            tags=["github", "github-morning-sweep"],
        )
        store.add_calendar_entry(
            self.connection,
            entry_date=target_day,
            title="Outline project note",
            priority="high",
            list_name="professional",
        )
        store.add_calendar_entry(
            self.connection,
            entry_date=target_day,
            title="Buy dish sponge",
            priority="normal",
            list_name="personal",
        )
        store.add_calendar_entry(
            self.connection,
            entry_date=target_day,
            title="FRG booking: Ada Lovelace · Research collaboration",
            entry_type="event",
            priority="high",
            list_name="professional",
            start_time="13:00",
            end_time="13:45",
            source="frg_site_booking",
            notes="Stripe session: cs_test_booking",
            tags=["frg", "booking"],
        )
        store.add_calendar_entry(
            self.connection,
            entry_date=date(2026, 4, 27),
            title="Prepare next-week roadmap",
            priority="high",
            list_name="professional",
        )
        store.add_event(
            self.connection,
            title="Month-ahead planning call",
            start_at=datetime(2026, 5, 8, 14, 0),
            end_at=datetime(2026, 5, 8, 14, 30),
            organization_name="FRG",
        )
        store.add_calendar_entry(
            self.connection,
            entry_date=date(2028, 1, 12),
            title="FRG booking: Grace Hopper · Long-range consultation",
            entry_type="event",
            priority="normal",
            list_name="professional",
            start_time="10:00",
            end_time="10:30",
            source="frg_site_booking",
            notes="Booked far outside the normal roadmap window.",
            tags=["frg", "booking"],
        )
        store.add_list_item(
            self.connection,
            list_name="personal",
            title="Trash bags",
        )

        sheet = build_day_sheet(self.connection, target_day=target_day)
        section_names = [section["name"] for section in sheet["sections"]]
        text = render_day_sheet_text(sheet)
        html = render_day_sheet_html(sheet)
        latex = render_day_sheet_latex(sheet)

        self.assertEqual("Hard Schedule", section_names[0])
        self.assertIn("Signups / Bookings", section_names)
        self.assertNotIn("GitHub / Notifications", section_names)
        self.assertNotIn("Professional / Projects", section_names)
        self.assertIn("Personal / Home", section_names)
        self.assertIn("Open Lists", section_names)
        self.assertIn("FRG booking: Ada Lovelace", text)
        self.assertIn("GitHub: fix red main CI", text)
        self.assertNotIn("Morning planning", text)
        self.assertIn("(urgent, planned, professional)", text)
        self.assertIn("Calendar Holds / Bookings", text)
        self.assertIn("FRG booking: Grace Hopper", text)
        self.assertIn(
            "FRG booking: Grace Hopper · Long-range consultation",
            [item["title"] for item in sheet["calendar_holds"]["items"]],
        )
        self.assertIn("2028-01-12", [item["date"] for item in sheet["meeting_bookings"]["items"]])
        self.assertNotIn("Prepare next-week roadmap", text)
        self.assertIn("Month-ahead planning call", text)
        self.assertIn("Calendar Holds / Bookings", html)
        self.assertIn("FRG booking: Grace Hopper", html)
        self.assertIn("@page", html)
        self.assertIn("LifeOps Day Sheet", html)
        self.assertIn("\\documentclass", latex)
        self.assertIn("LifeOps Day Sheet", latex)
        self.assertIn("Calendar Holds / Bookings", latex)
        self.assertIn("FRG booking: Grace Hopper", latex)
        self.assertIn("Month-ahead planning call", latex)

    def test_day_sheet_prepends_canonical_frg_first_page(self) -> None:
        store.add_calendar_entry(
            self.connection,
            entry_date=date(2026, 4, 25),
            title="FRG mainstay: build sponsor-ready ocular/controller packet",
            priority="urgent",
            list_name="professional",
            notes="Package public artifacts, missing data, honest blockers, provider lanes, and ask shapes.",
            tags=["frg", "frg-mainstay"],
        )
        store.add_calendar_entry(
            self.connection,
            entry_date=date(2026, 4, 27),
            title="FRG: Call DON McKenzie",
            entry_type="event",
            priority="urgent",
            list_name="professional",
            start_time="13:00",
            end_time="14:00",
            notes="Chris said to call around 1-2 PM after the Saturday 4:30 PM conversation.",
            tags=["frg", "don-mckenzie", "call"],
        )

        sheet = build_day_sheet(self.connection, target_day=date(2026, 4, 26))
        text = render_day_sheet_text(sheet)
        html = render_day_sheet_html(sheet)
        latex = render_day_sheet_latex(sheet)

        frg_page = sheet["frg_first_page"]
        self.assertTrue(frg_page["enabled"])
        self.assertEqual(
            ["build sponsor-ready ocular/controller packet"],
            [item["title"] for item in frg_page["mainstay_items"]],
        )
        self.assertEqual(
            ["Call DON McKenzie"],
            [item["title"] for item in frg_page["hard_commitments"]],
        )
        self.assertTrue(text.startswith("Fractal Research Group First Page"))
        self.assertIn("\f\nLifeOps Day Sheet", text)
        self.assertIn("Calendar Holds / Bookings", text)
        self.assertIn("FRG: Call DON McKenzie", text)
        self.assertIn("Fractal Research Group First Page", text)
        self.assertIn("frg-first-page", html)
        self.assertIn("Fractal Research Group First Page", latex)
        self.assertIn("\\clearpage", latex)

        no_frg_sheet = build_day_sheet(
            self.connection,
            target_day=date(2026, 4, 26),
            include_frg_first_page=False,
        )
        self.assertFalse(no_frg_sheet["frg_first_page"]["enabled"])
        self.assertTrue(render_day_sheet_text(no_frg_sheet).startswith("LifeOps Day Sheet"))

    def test_calendar_range_expands_annual_recurring_entries_from_past_anchor(self) -> None:
        store.add_calendar_entry(
            self.connection,
            entry_date=date(1991, 6, 12),
            title="Cody birthday",
            entry_type="event",
            list_name="personal",
            recurrence_frequency="yearly",
            recurrence_anchor_date=date(1991, 6, 12),
            tags=["birthday"],
        )

        range_payload = build_calendar_range(
            self.connection,
            start_day=date(2026, 4, 24),
            days=365,
        )
        birthday_items = [
            item for item in range_payload["upcoming"]
            if item["title"] == "Cody birthday"
        ]
        day_payload = build_calendar_day(self.connection, target_day=date(2026, 6, 12))

        self.assertEqual(["2026-06-12"], [item["date"] for item in birthday_items])
        self.assertTrue(birthday_items[0]["is_recurring"])
        self.assertTrue(birthday_items[0]["is_virtual"])
        self.assertEqual("1991-06-12", birthday_items[0]["recurrence_anchor_date"])
        self.assertIn("Cody birthday", [entry["title"] for entry in day_payload["entries"]])

    def test_day_sheet_collapses_recent_orp_projects_into_focus_questions(self) -> None:
        target_day = date(2026, 4, 24)
        for index in range(1, 7):
            store.add_calendar_entry(
                self.connection,
                entry_date=target_day,
                title=f"Project {index}",
                priority="high",
                list_name="professional",
                source="orp",
                notes="\n".join(
                    [
                        "Next: Open the interactive session and define the next concrete artifact, decision, or shipped change before adding more scope.",
                        "Roadmap:",
                        f"Today: Answer the project {index} thesis.",
                        f"Next: Convert the answer into artifact {index}.",
                        f"Proof: Artifact {index} exists with evidence.",
                        "Admin:",
                        f"Project: Project {index}",
                        f"Rank: {index}",
                        f"Path: /tmp/project-{index} / Resume: codex resume 019d-test-{index}",
                    ]
                ),
                tags=["orp", "orp-project-sweep", "generated", "project-priority", "orp-workspace"],
            )
        store.add_calendar_entry(
            self.connection,
            entry_date=date(2026, 4, 16),
            title="Old Project",
            priority="urgent",
            list_name="professional",
            source="orp",
            notes="\n".join(
                [
                    "Today: Old work outside the seven-day window.",
                    "Proof: Old artifact.",
                    "Project: Old Project",
                    "Rank: 1",
                    "Path: /tmp/old-project / Resume: codex resume 019d-old",
                ]
            ),
            tags=["orp", "orp-project-sweep", "generated", "project-priority", "orp-workspace"],
        )

        sheet = build_day_sheet(self.connection, target_day=target_day, featured_project_name="Project 6")
        text = render_day_sheet_text(sheet)
        html = render_day_sheet_html(sheet)
        latex = render_day_sheet_latex(sheet)

        self.assertEqual(5, sheet["focus_priorities"]["count"])
        self.assertEqual("Project 1", sheet["focus_priorities"]["items"][0]["title"])
        self.assertEqual(6, sheet["agent_project_history"]["count"])
        self.assertEqual("Project 6", sheet["featured_project"]["item"]["title"])
        self.assertNotIn("ORP / Project Priorities", [section["name"] for section in sheet["sections"]])
        self.assertNotIn("Project 6", [item["title"] for item in sheet["focus_priorities"]["items"]])
        self.assertIn("Featured Project", text)
        self.assertIn("Action items:", text)
        self.assertIn("Artifact 6 exists with evidence", text)
        self.assertNotIn("Project 1 (high", text)
        self.assertNotIn("Old Project", text)
        self.assertNotIn("Open the interactive session", text)
        self.assertIn("Featured Project", html)
        self.assertNotIn("7-Day Agent Project History", html)
        self.assertIn("Featured Project", latex)
        self.assertNotIn("7-Day Agent Project History", latex)

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
            self.assertIn("/api/calendar/range", html)
            self.assertIn("repeatFrequency", html)

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

            request = urllib.request.Request(
                f"{base_url}/api/calendar/entries",
                data=json.dumps(
                    {
                        "date": "1991-06-12",
                        "title": "Cody birthday",
                        "type": "event",
                        "recurrence_frequency": "yearly",
                        "recurrence_anchor_date": "1991-06-12",
                    }
                ).encode("utf-8"),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(request) as response:
                recurring = json.loads(response.read().decode("utf-8"))
            self.assertTrue(recurring["ok"])

            with urllib.request.urlopen(f"{base_url}/api/calendar/range?start=2026-04-14&days=365") as response:
                range_payload = json.loads(response.read().decode("utf-8"))
            self.assertIn(
                "2026-06-12",
                [
                    item["date"] for item in range_payload["range"]["upcoming"]
                    if item["title"] == "Cody birthday"
                ],
            )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_signed_frg_booking_webhook_creates_calendar_hold_without_default_cmail_draft(self) -> None:
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
            with mock.patch.dict(
                os.environ,
                {
                    "FRG_BOOKING_WEBHOOK_SECRET": secret,
                    "FRG_BOOKING_CONFIRMATION_MODE": "",
                    "FRG_BOOKING_AUTO_SEND_CONFIRMATION": "",
                },
            ):
                with urllib.request.urlopen(request) as response:
                    created = json.loads(response.read().decode("utf-8"))

                self.assertTrue(created["ok"])
                self.assertFalse(created["duplicate"])
                self.assertGreater(int(created["calendar_entry_id"]), 0)
                self.assertIsNone(created["cmail_draft_id"])

                day_payload = build_calendar_day(self.connection, target_day=date(2026, 4, 18))
                self.assertEqual(1, len(day_payload["entries"]))
                entry = day_payload["entries"][0]
                self.assertEqual("event", entry["type"])
                self.assertEqual("13:00", entry["start_time"])
                self.assertEqual("13:45", entry["end_time"])
                self.assertIn("FRG booking: Ada Lovelace", entry["title"])
                self.assertIn("Stripe session: cs_test_booking", entry["notes"])

                drafts = mail_ui.list_cmail_drafts(db_path=self.db_path)
                self.assertEqual([], drafts)

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

                availability_timestamp = str(int(datetime.now().timestamp()))
                availability_signature = hmac.new(
                    secret.encode("utf-8"),
                    f"{availability_timestamp}.".encode("utf-8"),
                    hashlib.sha256,
                ).hexdigest()
                availability_request = urllib.request.Request(
                    f"{base_url}/api/frg/bookings/availability?start=2026-04-18&end=2026-04-18",
                    method="GET",
                    headers={
                        "X-FRG-Booking-Timestamp": availability_timestamp,
                        "X-FRG-Booking-Signature": f"v1={availability_signature}",
                    },
                )
                with urllib.request.urlopen(availability_request) as response:
                    availability = json.loads(response.read().decode("utf-8"))
                self.assertTrue(availability["ok"])
                self.assertEqual(1, len(availability["heldSlots"]))
                self.assertEqual("2026-04-18", availability["heldSlots"][0]["selectedDate"])
                self.assertEqual("1:00 PM", availability["heldSlots"][0]["selectedTime"])

                conflict_payload = {
                    **payload,
                    "booking": {
                        **payload["booking"],
                        "id": "frg-booking-test-002",
                        "name": "Grace Hopper",
                        "email": "grace@example.com",
                    },
                }
                conflict_body = json.dumps(conflict_payload).encode("utf-8")
                conflict_timestamp = str(int(datetime.now().timestamp()))
                conflict_signature = hmac.new(
                    secret.encode("utf-8"),
                    f"{conflict_timestamp}.".encode("utf-8") + conflict_body,
                    hashlib.sha256,
                ).hexdigest()
                conflict_request = urllib.request.Request(
                    f"{base_url}/api/frg/bookings",
                    data=conflict_body,
                    method="POST",
                    headers={
                        "Content-Type": "application/json",
                        "X-FRG-Booking-Timestamp": conflict_timestamp,
                        "X-FRG-Booking-Signature": f"v1={conflict_signature}",
                    },
                )
                with self.assertRaises(urllib.error.HTTPError) as raised:
                    urllib.request.urlopen(conflict_request)
                self.assertEqual(400, raised.exception.code)
                conflict_error = json.loads(raised.exception.read().decode("utf-8"))
                self.assertEqual("frg_booking_slot_unavailable", conflict_error["error"])
                self.assertEqual(1, len(build_calendar_day(self.connection, target_day=date(2026, 4, 18))["entries"]))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_frg_booking_confirmation_draft_is_explicit_opt_in(self) -> None:
        payload = {
            "event": "booking.free_confirmed",
            "booking": {
                "id": "frg-booking-draft-mode-001",
                "name": "Ada Lovelace",
                "email": "ada@example.com",
                "focus": "Mentorship",
                "durationMinutes": 30,
                "selectedDate": "2026-04-19",
                "selectedTime": "2:00 PM",
                "selectedSlotLabel": "Apr 19, 2026, 2:00 PM",
                "timezone": "America/Chicago",
            },
        }

        with mock.patch.dict(
            os.environ,
            {
                "FRG_BOOKING_CONFIRMATION_MODE": "draft",
                "FRG_BOOKING_AUTO_SEND_CONFIRMATION": "",
            },
        ):
            created = mail_ui._handle_frg_booking_payload(db_path=self.db_path, payload=payload)

        self.assertFalse(created["duplicate"])
        self.assertGreater(int(created["calendar_entry_id"]), 0)
        self.assertGreater(int(created["cmail_draft_id"]), 0)
        drafts = mail_ui.list_cmail_drafts(db_path=self.db_path)
        self.assertEqual(1, len(drafts))
        self.assertEqual("Ada Lovelace <ada@example.com>", drafts[0]["to"])
        self.assertIn("FRG booking confirmed", drafts[0]["subject"])

    def test_signed_frg_forge_webhook_creates_conference_seat_and_day_sheet_section(self) -> None:
        secret = "test-frg-forge-secret"
        payload = {
            "event": "forge.checkout.completed",
            "conferenceSeat": {
                "attendeeEmail": "grace@example.com",
                "attendeeName": "Grace Hopper",
                "conferenceId": "frg-forge-pilot",
                "conferenceStartsAt": None,
                "conferenceTitle": "FRG Forge",
                "fulfillmentStatus": "needs_details",
                "locationLabel": "Pensacola, FL / livestream",
                "purchasedAt": "2026-04-29T00:00:00.000Z",
                "seatId": "frg-forge-seat-test-001",
                "seatLabel": "Founding seat",
                "status": "paid",
            },
            "forge": {
                "createdAt": "2026-04-29T00:00:00.000Z",
                "email": "grace@example.com",
                "id": "frg-forge-seat-test-001",
                "kind": "founding_seat",
                "name": "Grace Hopper",
                "source": "frg-site:forge-symbol",
            },
            "lifeOps": {
                "checklistSection": "Upcoming Conferences",
                "checklistTitle": "FRG Forge seat sold",
                "requiredAction": "Send the buyer the Forge details packet and seat confirmation.",
            },
            "payment": {
                "amountTotalCents": 10000,
                "stripeCheckoutSessionId": "cs_test_forge",
            },
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
                f"{base_url}/api/frg/forge",
                data=raw_body,
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "X-FRG-Forge-Timestamp": timestamp,
                    "X-FRG-Forge-Signature": f"v1={signature}",
                },
            )
            with mock.patch.dict(
                os.environ,
                {
                    "FRG_FORGE_WEBHOOK_SECRET": secret,
                    "FRG_FORGE_CONFIRMATION_MODE": "",
                    "FRG_FORGE_AUTO_SEND_CONFIRMATION": "",
                },
            ):
                with urllib.request.urlopen(request) as response:
                    created = json.loads(response.read().decode("utf-8"))

                self.assertTrue(created["ok"])
                self.assertFalse(created["duplicate"])
                self.assertGreater(int(created["calendar_entry_id"]), 0)
                self.assertIsNone(created["cmail_draft_id"])

                day_sheet = build_day_sheet(self.connection, target_day=date(2026, 4, 29))
                self.assertEqual(1, day_sheet["summary"]["conference_seats"])
                self.assertEqual(1, day_sheet["conference_seats"]["total_count"])
                rendered = render_day_sheet_text(day_sheet)
                self.assertIn("Upcoming Conferences (1)", rendered)
                self.assertIn("FRG Forge seat: Grace Hopper", rendered)
                self.assertIn("grace@example.com", rendered)

                drafts = mail_ui.list_cmail_drafts(db_path=self.db_path)
                self.assertEqual([], drafts)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
