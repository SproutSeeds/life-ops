from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, time, timedelta

from life_ops import store


def _format_day(day: date) -> str:
    return day.strftime("%A, %B %d").replace(" 0", " ")


def _format_clock(value: datetime) -> str:
    return value.strftime("%H:%M")


def _bucket_template(day: date) -> dict:
    return {
        "date": day.isoformat(),
        "label": _format_day(day),
        "items": [],
    }


def _sort_key(item: dict) -> tuple[str, str, str]:
    return (
        item.get("sort_time", "23:59"),
        item.get("type", "other"),
        item.get("title", ""),
    )


def _event_time_labels(start_at: datetime, end_at: datetime, bucket_day: date, all_day: bool) -> tuple[str, str]:
    if all_day:
        return ("00:00", "All day")

    if bucket_day == start_at.date() == end_at.date():
        return (_format_clock(start_at), f"{_format_clock(start_at)}-{_format_clock(end_at)}")

    if bucket_day == start_at.date():
        return (_format_clock(start_at), f"{_format_clock(start_at)} onward")

    if bucket_day == end_at.date():
        return ("00:00", f"Until {_format_clock(end_at)}")

    return ("00:00", "All day (continues)")


def build_agenda(connection: sqlite3.Connection, start_day: date, days: int = 7) -> dict:
    end_day = start_day + timedelta(days=days - 1)
    window_start = datetime.combine(start_day, time(0, 0))
    window_end = datetime.combine(end_day, time(23, 59))

    buckets = {
        start_day + timedelta(days=offset): _bucket_template(start_day + timedelta(days=offset))
        for offset in range(days)
    }

    for routine in store.list_routines(connection):
        for bucket_day, bucket in buckets.items():
            matches_daily = routine["cadence"] == "daily"
            matches_weekly = routine["cadence"] == "weekly" and routine["day_of_week"] == bucket_day.weekday()
            if not (matches_daily or matches_weekly):
                continue

            bucket["items"].append(
                {
                    "type": "routine",
                    "title": routine["name"],
                    "sort_time": routine["start_time"],
                    "time": routine["start_time"],
                    "duration_minutes": routine["duration_minutes"],
                    "notes": routine["notes"],
                }
            )

    for event in store.list_events_between(connection, window_start, window_end):
        start_at = store.parse_datetime(event["start_at"])
        end_at = store.parse_datetime(event["end_at"])
        current_day = max(start_at.date(), start_day)
        last_day = min(end_at.date(), end_day)

        while current_day <= last_day:
            sort_time, display_time = _event_time_labels(
                start_at=start_at,
                end_at=end_at,
                bucket_day=current_day,
                all_day=bool(event["all_day"]),
            )
            buckets[current_day]["items"].append(
                {
                    "type": "event",
                    "title": event["title"],
                    "sort_time": sort_time,
                    "time": display_time,
                    "location": event["location"],
                    "kind": event["kind"],
                    "organization": event["organization_name"] or "",
                    "notes": event["notes"],
                }
            )
            current_day += timedelta(days=1)

    for follow_up in store.list_followups_between(connection, window_start, window_end):
        follow_up_at = store.parse_datetime(follow_up["follow_up_at"])
        bucket = buckets.get(follow_up_at.date())
        if bucket is None:
            continue

        bucket["items"].append(
            {
                "type": "follow_up",
                "title": follow_up["subject"],
                "sort_time": _format_clock(follow_up_at),
                "time": _format_clock(follow_up_at),
                "channel": follow_up["channel"],
                "person": follow_up["person"],
                "organization": follow_up["organization_name"] or "",
                "category": follow_up["category"] or "",
                "priority_level": follow_up["priority_level"] or "",
                "retention_bucket": follow_up["retention_bucket"] or "",
                "notes": follow_up["notes"],
            }
        )

    ordered_days = []
    for bucket_day in sorted(buckets):
        bucket = buckets[bucket_day]
        bucket["items"] = sorted(bucket["items"], key=_sort_key)
        ordered_days.append(bucket)

    return {
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "days": ordered_days,
    }


def render_agenda_text(agenda: dict) -> str:
    lines = [f"Agenda window: {agenda['start_date']} to {agenda['end_date']}", ""]

    for day in agenda["days"]:
        lines.append(day["label"])
        if not day["items"]:
            lines.append("- Open space")
            lines.append("")
            continue

        for item in day["items"]:
            if item["type"] == "routine":
                lines.append(
                    f"- {item['time']} Routine: {item['title']} ({item['duration_minutes']}m)"
                )
                continue

            if item["type"] == "event":
                context = []
                if item["organization"]:
                    context.append(item["organization"])
                if item["location"]:
                    context.append(item["location"])
                context_text = f" [{', '.join(context)}]" if context else ""
                lines.append(f"- {item['time']} Event: {item['title']}{context_text}")
                continue

            if item["type"] == "follow_up":
                context = []
                if item["channel"]:
                    context.append(item["channel"])
                if item["organization"]:
                    context.append(item["organization"])
                if item["category"]:
                    context.append(item["category"])
                if item["priority_level"]:
                    context.append(item["priority_level"])
                context_text = f" [{', '.join(context)}]" if context else ""
                lines.append(f"- {item['time']} Follow up: {item['title']}{context_text}")
                continue

        lines.append("")

    return "\n".join(lines).strip()


def render_agenda_json(agenda: dict) -> str:
    return json.dumps(agenda, indent=2)
