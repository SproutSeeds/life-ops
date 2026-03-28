from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import date, datetime, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops.agenda import build_agenda, render_agenda_text
from life_ops import store


class AgendaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test.db"
        self.connection = store.open_db(self.db_path)

    def tearDown(self) -> None:
        self.connection.close()
        self.temp_dir.cleanup()

    def test_agenda_includes_daily_weekly_routines_events_and_followups(self) -> None:
        start_day = date(2026, 3, 23)
        store.add_routine(
            self.connection,
            name="Morning planning",
            cadence="daily",
            start_time="08:30",
            duration_minutes=30,
            notes="Start the day on purpose.",
        )
        store.add_routine(
            self.connection,
            name="Weekly review",
            cadence="weekly",
            day_of_week=6,
            start_time="18:00",
            duration_minutes=60,
            notes="Look ahead.",
        )
        store.add_event(
            self.connection,
            title="Founder sync",
            start_at=datetime.combine(start_day, time(10, 0)),
            end_at=datetime.combine(start_day, time(11, 0)),
            organization_name="Primary Work",
        )
        store.add_communication(
            self.connection,
            subject="Reply to clinic",
            channel="email",
            happened_at=datetime.combine(start_day, time(9, 0)),
            follow_up_at=datetime.combine(start_day, time(13, 0)),
            organization_name="Health",
        )

        agenda = build_agenda(self.connection, start_day=start_day, days=7)
        monday = agenda["days"][0]
        sunday = agenda["days"][6]

        monday_titles = [item["title"] for item in monday["items"]]
        sunday_titles = [item["title"] for item in sunday["items"]]

        self.assertIn("Morning planning", monday_titles)
        self.assertIn("Founder sync", monday_titles)
        self.assertIn("Reply to clinic", monday_titles)
        self.assertIn("Weekly review", sunday_titles)

    def test_text_render_mentions_followup_context(self) -> None:
        start_day = date(2026, 3, 23)
        store.add_communication(
            self.connection,
            subject="Reply to partner email",
            channel="email",
            happened_at=datetime.combine(start_day, time(8, 0)),
            follow_up_at=datetime.combine(start_day, time(12, 0)),
            organization_name="Primary Work",
        )

        agenda = build_agenda(self.connection, start_day=start_day, days=1)
        output = render_agenda_text(agenda)

        self.assertIn("Follow up: Reply to partner email [email, Primary Work]", output)


if __name__ == "__main__":
    unittest.main()
