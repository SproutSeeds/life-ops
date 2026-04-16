from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import date, datetime, time, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops import store
from life_ops.apple_calendar import _feed_url_from_webcal, sync_apple_calendar_feed
from life_ops.calendar import build_calendar_day


_ICS_WEEKDAY = ["MO", "TU", "WE", "TH", "FR", "SA", "SU"]


def _ics_date(value: date) -> str:
    return value.strftime("%Y%m%d")


def _ics_datetime(value: date, hour: int, minute: int = 0) -> str:
    return f"{value.strftime('%Y%m%d')}T{hour:02d}{minute:02d}00"


class AppleCalendarSyncTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "life_ops.db"
        self.feed_path = Path(self.temp_dir.name) / "apple.ics"
        self.connection = store.open_db(self.db_path)

    def tearDown(self) -> None:
        self.connection.close()
        self.temp_dir.cleanup()

    def test_syncs_apple_ics_feed_into_agenda_events(self) -> None:
        target_day = date.today()
        all_day = target_day + timedelta(days=1)
        skipped_recurrence_day = target_day + timedelta(days=7)
        weekday = _ICS_WEEKDAY[target_day.weekday()]
        self.feed_path.write_text(
            "\n".join(
                [
                    "BEGIN:VCALENDAR",
                    "VERSION:2.0",
                    "X-WR-CALNAME:Personal iCloud",
                    "BEGIN:VEVENT",
                    "UID:timed-apple-event",
                    f"DTSTART;TZID=America/Chicago:{_ics_datetime(target_day, 11)}",
                    f"DTEND;TZID=America/Chicago:{_ics_datetime(target_day, 12)}",
                    "SUMMARY:Apple planning block",
                    "LOCATION:Kitchen table",
                    "DESCRIPTION:Imported from Apple.",
                    "END:VEVENT",
                    "BEGIN:VEVENT",
                    "UID:all-day-apple-event",
                    f"DTSTART;VALUE=DATE:{_ics_date(all_day)}",
                    f"DTEND;VALUE=DATE:{_ics_date(all_day + timedelta(days=1))}",
                    "SUMMARY:Apple all-day marker",
                    "END:VEVENT",
                    "BEGIN:VEVENT",
                    "UID:weekly-apple-event",
                    f"DTSTART;TZID=America/Chicago:{_ics_datetime(target_day, 9)}",
                    f"DTEND;TZID=America/Chicago:{_ics_datetime(target_day, 9, 30)}",
                    f"RRULE:FREQ=WEEKLY;COUNT=4;BYDAY={weekday}",
                    f"EXDATE;TZID=America/Chicago:{_ics_datetime(skipped_recurrence_day, 9)}",
                    "SUMMARY:Apple weekly habit",
                    "END:VEVENT",
                    "END:VCALENDAR",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        summary = sync_apple_calendar_feed(
            self.connection,
            feed_path=self.feed_path,
            calendar_name="Personal iCloud",
            days_back=1,
            days_ahead=30,
        )

        self.assertEqual("Personal iCloud", summary["calendar_name"])
        self.assertEqual(3, summary["events_in_feed"])
        self.assertEqual(5, summary["events_synced"])

        day_payload = build_calendar_day(self.connection, target_day=target_day)
        agenda_titles = [item["title"] for item in day_payload["agenda"]["items"]]
        self.assertIn("Apple planning block", agenda_titles)
        self.assertIn("Apple weekly habit", agenda_titles)

        all_day_events = store.list_events_between(
            self.connection,
            datetime.combine(all_day, time(0, 0)),
            datetime.combine(all_day, time(23, 59)),
        )
        all_day_record = next(item for item in all_day_events if item["title"] == "Apple all-day marker")
        self.assertEqual(1, all_day_record["all_day"])
        self.assertEqual("Personal iCloud", all_day_record["organization_name"])

        excluded_events = store.list_events_between(
            self.connection,
            datetime.combine(skipped_recurrence_day, time(0, 0)),
            datetime.combine(skipped_recurrence_day, time(23, 59)),
        )
        self.assertNotIn("Apple weekly habit", [item["title"] for item in excluded_events])

    def test_resync_replaces_stale_feed_events_for_same_calendar(self) -> None:
        target_day = date.today()
        self.feed_path.write_text(
            "\n".join(
                [
                    "BEGIN:VCALENDAR",
                    "VERSION:2.0",
                    "X-WR-CALNAME:Personal iCloud",
                    "BEGIN:VEVENT",
                    "UID:first-event",
                    f"DTSTART:{_ics_datetime(target_day, 10)}",
                    f"DTEND:{_ics_datetime(target_day, 11)}",
                    "SUMMARY:First Apple event",
                    "END:VEVENT",
                    "END:VCALENDAR",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        first_summary = sync_apple_calendar_feed(
            self.connection,
            feed_path=self.feed_path,
            calendar_name="Personal iCloud",
            days_back=1,
            days_ahead=7,
        )
        self.assertEqual(1, first_summary["events_synced"])

        self.feed_path.write_text(
            "\n".join(
                [
                    "BEGIN:VCALENDAR",
                    "VERSION:2.0",
                    "X-WR-CALNAME:Personal iCloud",
                    "BEGIN:VEVENT",
                    "UID:replacement-event",
                    f"DTSTART:{_ics_datetime(target_day, 13)}",
                    f"DTEND:{_ics_datetime(target_day, 14)}",
                    "SUMMARY:Replacement Apple event",
                    "END:VEVENT",
                    "END:VCALENDAR",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        second_summary = sync_apple_calendar_feed(
            self.connection,
            feed_path=self.feed_path,
            calendar_name="Personal iCloud",
            days_back=1,
            days_ahead=7,
        )

        events = store.list_events_between(
            self.connection,
            datetime.combine(target_day, time(0, 0)),
            datetime.combine(target_day, time(23, 59)),
        )
        self.assertEqual(1, second_summary["events_deleted_before_sync"])
        self.assertEqual(["Replacement Apple event"], [item["title"] for item in events])

    def test_webcal_urls_are_fetched_over_https(self) -> None:
        self.assertEqual(
            "https://p123-caldav.icloud.com/published/2/example",
            _feed_url_from_webcal("webcal://p123-caldav.icloud.com/published/2/example"),
        )


if __name__ == "__main__":
    unittest.main()
