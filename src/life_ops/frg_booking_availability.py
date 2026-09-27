from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from life_ops import store


DEFAULT_BOOKING_TIMEZONE = "America/Chicago"
DEFAULT_LEAD_TIME_HOURS = 48
DEFAULT_RANGE_LIMIT_DAYS = 120
DEFAULT_START_TIMES = ("13:00", "15:00", "17:00")
DEFAULT_WEEKDAYS = (1, 2, 3, 4, 5)
DEFAULT_ALLOWED_DURATIONS = (15, 30, 45, 60, 75, 90, 120)
DEFAULT_FREE_DURATION_MINUTES = 30
FRG_BOOKING_INACTIVE_STATUSES = ("canceled", "cancelled", "completed", "done")

PACKAGE_DURATIONS = {
    "business-readiness-call": 30,
    "forge-clarity-call": 30,
    "codex-workflow-session": 90,
    "company-pilot-deposit": 60,
}


@dataclass(frozen=True)
class BusyInterval:
    calendar_entry_id: int
    entry_date: date
    start_minutes: int
    end_minutes: int
    source: str

    @property
    def start_time(self) -> str:
        return _format_clock(self.start_minutes)

    @property
    def end_time(self) -> str:
        return _format_clock(self.end_minutes)


def _clean_text(value: Any, limit: int = 1800) -> str:
    return " ".join(str(value or "").strip().split())[:limit]


def _env_int(name: str, default: int, *, minimum: int = 0, maximum: int = 24 * 365) -> int:
    raw_value = _clean_text(os.environ.get(name), limit=32)
    if not raw_value:
        return default
    try:
        value = int(raw_value)
    except ValueError:
        return default
    return max(minimum, min(maximum, value))


def _parse_clock(value: Any) -> int | None:
    clean = _clean_text(value, limit=16)
    parts = clean.split(":")
    if len(parts) != 2:
        return None
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return None
    if hour > 23 or minute > 59 or hour < 0 or minute < 0:
        return None
    return hour * 60 + minute


def _format_clock(minutes: int) -> str:
    bounded = max(0, min(24 * 60, int(minutes)))
    hour = bounded // 60
    minute = bounded % 60
    return f"{hour:02d}:{minute:02d}"


def _clock_label(clock: str) -> str:
    minutes = _parse_clock(clock)
    if minutes is None:
        return ""
    hour = minutes // 60
    minute = minutes % 60
    meridiem = "PM" if hour >= 12 else "AM"
    hour_12 = hour % 12 or 12
    return f"{hour_12}:{minute:02d} {meridiem}"


def _configured_timezone() -> ZoneInfo:
    timezone_name = _clean_text(os.environ.get("LIFEOPS_FRG_BOOKING_TIMEZONE"), limit=80) or DEFAULT_BOOKING_TIMEZONE
    try:
        return ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        return ZoneInfo(DEFAULT_BOOKING_TIMEZONE)


def _configured_start_times() -> tuple[str, ...]:
    raw_value = _clean_text(os.environ.get("LIFEOPS_FRG_BOOKING_START_TIMES"), limit=240)
    if not raw_value:
        return DEFAULT_START_TIMES

    start_times: list[str] = []
    for part in raw_value.split(","):
        minutes = _parse_clock(part)
        if minutes is not None:
            start_times.append(_format_clock(minutes))
    return tuple(dict.fromkeys(start_times)) or DEFAULT_START_TIMES


def _configured_weekdays() -> tuple[int, ...]:
    raw_value = _clean_text(os.environ.get("LIFEOPS_FRG_BOOKING_WEEKDAYS"), limit=80)
    if not raw_value:
        return DEFAULT_WEEKDAYS

    weekdays: list[int] = []
    for part in raw_value.split(","):
        try:
            weekday = int(part.strip())
        except ValueError:
            continue
        if 1 <= weekday <= 7:
            weekdays.append(weekday)
    return tuple(dict.fromkeys(weekdays)) or DEFAULT_WEEKDAYS


def _normalize_duration(duration_minutes: Any, package_id: Any = "") -> int:
    package_duration = PACKAGE_DURATIONS.get(_clean_text(package_id, limit=80).lower())
    if package_duration is not None:
        return package_duration

    try:
        duration = int(duration_minutes)
    except (TypeError, ValueError):
        duration = 30

    if duration in DEFAULT_ALLOWED_DURATIONS:
        return duration

    nearest = min(DEFAULT_ALLOWED_DURATIONS, key=lambda candidate: abs(candidate - duration))
    return nearest


def _date_range(start_day: date, end_day: date) -> list[date]:
    day_count = (end_day - start_day).days
    return [start_day + timedelta(days=offset) for offset in range(day_count + 1)]


def _active_status_clause() -> str:
    placeholders = ", ".join("?" for _ in FRG_BOOKING_INACTIVE_STATUSES)
    return f"LOWER(status) NOT IN ({placeholders})"


def _row_to_busy_interval(row: sqlite3.Row) -> BusyInterval | None:
    try:
        entry_day = date.fromisoformat(str(row["entry_date"] or ""))
    except ValueError:
        return None

    start_minutes = _parse_clock(row["start_time"])
    if start_minutes is None:
        return None

    end_minutes = _parse_clock(row["end_time"])
    if end_minutes is None or end_minutes <= start_minutes:
        end_minutes = start_minutes + 30

    return BusyInterval(
        calendar_entry_id=int(row["id"]),
        entry_date=entry_day,
        start_minutes=start_minutes,
        end_minutes=end_minutes,
        source=str(row["source"] or ""),
    )


def _busy_intervals(connection: sqlite3.Connection, *, start_day: date, end_day: date) -> list[BusyInterval]:
    rows = connection.execute(
        f"""
        SELECT id, entry_date, start_time, end_time, source
        FROM calendar_entries
        WHERE entry_date >= ?
          AND entry_date <= ?
          AND {_active_status_clause()}
          AND start_time != ''
        ORDER BY entry_date ASC, start_time ASC, id ASC
        """,
        (
            start_day.isoformat(),
            end_day.isoformat(),
            *FRG_BOOKING_INACTIVE_STATUSES,
        ),
    ).fetchall()
    return [interval for row in rows if (interval := _row_to_busy_interval(row)) is not None]


def _held_slot(interval: BusyInterval) -> dict[str, Any]:
    return {
        "calendarEntryId": interval.calendar_entry_id,
        "selectedDate": interval.entry_date.isoformat(),
        "selectedTime": _clock_label(interval.start_time),
        "startTime": interval.start_time,
        "endTime": interval.end_time,
    }


def _slot_payload(target_day: date, start_minutes: int, duration_minutes: int, *, status: str, reason: str = "") -> dict[str, Any]:
    start_time = _format_clock(start_minutes)
    end_time = _format_clock(start_minutes + duration_minutes)
    payload = {
        "selectedDate": target_day.isoformat(),
        "selectedTime": _clock_label(start_time),
        "startTime": start_time,
        "endTime": end_time,
        "status": status,
    }
    if reason:
        payload["reason"] = reason
    return payload


def _slot_overlaps_busy_interval(
    *,
    slot_start: int,
    slot_end: int,
    busy_interval: BusyInterval,
    buffer_minutes: int,
) -> bool:
    busy_start = max(0, busy_interval.start_minutes - buffer_minutes)
    busy_end = min(24 * 60, busy_interval.end_minutes + buffer_minutes)
    return busy_start < slot_end and slot_start < busy_end


def build_frg_booking_availability_payload(
    *,
    db_path: Path,
    start_day: date,
    end_day: date,
    duration_minutes: Any = 30,
    package_id: Any = "",
    now: datetime | None = None,
) -> dict[str, Any]:
    if end_day < start_day:
        raise ValueError("invalid_frg_booking_availability_range")
    if (end_day - start_day).days > DEFAULT_RANGE_LIMIT_DAYS:
        raise ValueError("frg_booking_availability_range_too_large")

    booking_timezone = _configured_timezone()
    clean_package_id = _clean_text(package_id, limit=80).lower()
    clean_duration = _normalize_duration(duration_minutes, clean_package_id)
    lead_time_hours = _env_int("LIFEOPS_FRG_BOOKING_LEAD_HOURS", DEFAULT_LEAD_TIME_HOURS, minimum=0, maximum=24 * 90)
    buffer_minutes = _env_int("LIFEOPS_FRG_BOOKING_BUFFER_MINUTES", 0, minimum=0, maximum=24 * 60)
    start_times = _configured_start_times()
    weekdays = _configured_weekdays()
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    local_now = now.astimezone(booking_timezone) if now is not None else datetime.now(booking_timezone)
    minimum_start = local_now + timedelta(hours=lead_time_hours)

    with store.open_db(db_path) as connection:
        intervals = _busy_intervals(connection, start_day=start_day, end_day=end_day)

    intervals_by_date: dict[str, list[BusyInterval]] = {}
    for interval in intervals:
        intervals_by_date.setdefault(interval.entry_date.isoformat(), []).append(interval)

    slots: list[dict[str, Any]] = []
    available_slots: list[dict[str, Any]] = []

    for target_day in _date_range(start_day, end_day):
        date_key = target_day.isoformat()
        if target_day.isoweekday() not in weekdays:
            for start_time in start_times:
                start_minutes = _parse_clock(start_time)
                if start_minutes is not None:
                    slots.append(_slot_payload(target_day, start_minutes, clean_duration, status="closed", reason="outside_booking_window"))
            continue

        for start_time in start_times:
            start_minutes = _parse_clock(start_time)
            if start_minutes is None:
                continue

            slot_start_at = datetime.combine(
                target_day,
                time(start_minutes // 60, start_minutes % 60),
                tzinfo=booking_timezone,
            )
            if slot_start_at < minimum_start:
                slots.append(_slot_payload(target_day, start_minutes, clean_duration, status="closed", reason="lead_time"))
                continue

            slot_end = start_minutes + clean_duration
            if slot_end > 24 * 60:
                slots.append(_slot_payload(target_day, start_minutes, clean_duration, status="closed", reason="outside_booking_window"))
                continue

            busy = next(
                (
                    interval
                    for interval in intervals_by_date.get(date_key, [])
                    if _slot_overlaps_busy_interval(
                        slot_start=start_minutes,
                        slot_end=slot_end,
                        busy_interval=interval,
                        buffer_minutes=buffer_minutes,
                    )
                ),
                None,
            )
            if busy is not None:
                slots.append(
                    {
                        **_slot_payload(target_day, start_minutes, clean_duration, status="busy", reason="calendar_hold"),
                        "calendarEntryId": busy.calendar_entry_id,
                    }
                )
                continue

            open_slot = _slot_payload(target_day, start_minutes, clean_duration, status="open")
            slots.append(open_slot)
            available_slots.append(open_slot)

    return {
        "ok": True,
        "start": start_day.isoformat(),
        "end": end_day.isoformat(),
        "generatedAt": generated_at,
        "source": "life_ops_calendar_entries",
        "rules": {
            "allowedDurations": list(DEFAULT_ALLOWED_DURATIONS),
            "bufferMinutes": buffer_minutes,
            "durationMinutes": clean_duration,
            "freeDurationMinutes": DEFAULT_FREE_DURATION_MINUTES,
            "leadTimeHours": lead_time_hours,
            "packageId": clean_package_id,
            "startTimes": list(start_times),
            "timezone": str(booking_timezone.key),
            "weekdays": list(weekdays),
        },
        "availableSlots": available_slots,
        "heldSlots": [_held_slot(interval) for interval in intervals],
        "slots": slots,
    }
