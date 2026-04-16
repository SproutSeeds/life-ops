from __future__ import annotations

import hashlib
import re
import urllib.request
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from life_ops import store
from life_ops import tracing

APPLE_CALENDAR_SOURCE = "apple-calendar-feed"
DEFAULT_APPLE_CALENDAR_DAYS_BACK = 30
DEFAULT_APPLE_CALENDAR_DAYS_AHEAD = 365
_WEEKDAY_BY_ICS = {
    "MO": 0,
    "TU": 1,
    "WE": 2,
    "TH": 3,
    "FR": 4,
    "SA": 5,
    "SU": 6,
}


def _unfold_ics_lines(raw_text: str) -> list[str]:
    lines = raw_text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    unfolded: list[str] = []
    for line in lines:
        if not line:
            continue
        if line.startswith((" ", "\t")) and unfolded:
            unfolded[-1] += line[1:]
            continue
        unfolded.append(line)
    return unfolded


def _split_property(line: str) -> tuple[str, dict[str, str], str]:
    if ":" not in line:
        return ("", {}, "")
    head, value = line.split(":", 1)
    parts = head.split(";")
    name = parts[0].upper()
    params: dict[str, str] = {}
    for part in parts[1:]:
        if "=" not in part:
            continue
        key, raw_value = part.split("=", 1)
        params[key.upper()] = raw_value.strip('"')
    return name, params, value


def _decode_ics_text(value: str) -> str:
    return (
        str(value or "")
        .replace("\\n", "\n")
        .replace("\\N", "\n")
        .replace("\\,", ",")
        .replace("\\;", ";")
        .replace("\\\\", "\\")
        .strip()
    )


def _parse_ics_properties(raw_text: str) -> tuple[dict[str, list[tuple[dict[str, str], str]]], list[dict[str, list[tuple[dict[str, str], str]]]]]:
    calendar_props: dict[str, list[tuple[dict[str, str], str]]] = {}
    events: list[dict[str, list[tuple[dict[str, str], str]]]] = []
    current_event: Optional[dict[str, list[tuple[dict[str, str], str]]]] = None

    for line in _unfold_ics_lines(raw_text):
        name, params, value = _split_property(line)
        if not name:
            continue
        if name == "BEGIN" and value.upper() == "VEVENT":
            current_event = {}
            continue
        if name == "END" and value.upper() == "VEVENT":
            if current_event is not None:
                events.append(current_event)
            current_event = None
            continue

        target = current_event if current_event is not None else calendar_props
        target.setdefault(name, []).append((params, value))

    return calendar_props, events


def _first_prop(
    props: dict[str, list[tuple[dict[str, str], str]]],
    name: str,
    default: str = "",
) -> str:
    values = props.get(name.upper()) or []
    if not values:
        return default
    return _decode_ics_text(values[0][1])


def _first_prop_with_params(
    props: dict[str, list[tuple[dict[str, str], str]]],
    name: str,
) -> tuple[dict[str, str], str] | None:
    values = props.get(name.upper()) or []
    if not values:
        return None
    params, value = values[0]
    return params, value


def _local_tz():
    return datetime.now().astimezone().tzinfo


def _parse_ics_datetime(params: dict[str, str], value: str) -> tuple[datetime, bool]:
    clean = str(value or "").strip()
    if not clean:
        raise ValueError("empty ICS date value")
    value_kind = str(params.get("VALUE") or "").upper()
    if value_kind == "DATE" or re.fullmatch(r"\d{8}", clean):
        parsed_day = date(
            int(clean[0:4]),
            int(clean[4:6]),
            int(clean[6:8]),
        )
        return datetime.combine(parsed_day, time(0, 0)), True

    is_utc = clean.endswith("Z")
    if is_utc:
        clean = clean[:-1]
    if "T" not in clean:
        raise ValueError(f"unsupported ICS datetime value: {value}")
    parsed = datetime.strptime(clean, "%Y%m%dT%H%M%S")
    if is_utc:
        return parsed.replace(tzinfo=timezone.utc).astimezone(_local_tz()).replace(tzinfo=None), False

    tzid = str(params.get("TZID") or "").strip()
    if tzid:
        try:
            return parsed.replace(tzinfo=ZoneInfo(tzid)).astimezone(_local_tz()).replace(tzinfo=None), False
        except ZoneInfoNotFoundError:
            return parsed, False

    return parsed, False


def _event_times(props: dict[str, list[tuple[dict[str, str], str]]]) -> tuple[datetime, datetime, bool]:
    start_prop = _first_prop_with_params(props, "DTSTART")
    if start_prop is None:
        raise ValueError("VEVENT missing DTSTART")
    start_at, all_day = _parse_ics_datetime(*start_prop)

    end_prop = _first_prop_with_params(props, "DTEND")
    if end_prop is None:
        if all_day:
            return start_at, datetime.combine(start_at.date(), time(23, 59)), True
        return start_at, start_at + timedelta(hours=1), False

    end_at, end_all_day = _parse_ics_datetime(*end_prop)
    if all_day or end_all_day:
        end_day_exclusive = end_at.date()
        end_day = start_at.date() if end_day_exclusive <= start_at.date() else end_day_exclusive - timedelta(days=1)
        return (
            datetime.combine(start_at.date(), time(0, 0)),
            datetime.combine(end_day, time(23, 59)),
            True,
        )
    if end_at < start_at:
        end_at = start_at
    return start_at, end_at, False


def _parse_rrule(value: str) -> dict[str, str]:
    rule: dict[str, str] = {}
    for part in str(value or "").split(";"):
        if "=" not in part:
            continue
        key, raw_value = part.split("=", 1)
        rule[key.upper()] = raw_value
    return rule


def _parse_rrule_until(value: str, all_day: bool) -> Optional[datetime]:
    clean = str(value or "").strip()
    if not clean:
        return None
    try:
        parsed, _ = _parse_ics_datetime({"VALUE": "DATE" if all_day and "T" not in clean else "DATE-TIME"}, clean)
    except ValueError:
        return None
    return parsed


def _add_months(value: datetime, months: int) -> Optional[datetime]:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    try:
        return value.replace(year=year, month=month)
    except ValueError:
        return None


def _add_years(value: datetime, years: int) -> Optional[datetime]:
    try:
        return value.replace(year=value.year + years)
    except ValueError:
        return None


def _parse_exdates(
    props: dict[str, list[tuple[dict[str, str], str]]],
) -> set[datetime]:
    excluded: set[datetime] = set()
    for params, raw_value in props.get("EXDATE", []):
        for value in str(raw_value or "").split(","):
            try:
                excluded_at, _ = _parse_ics_datetime(params, value)
            except ValueError:
                continue
            excluded.add(excluded_at)
    return excluded


def _overlaps_window(start_at: datetime, end_at: datetime, window_start: datetime, window_end: datetime) -> bool:
    return start_at <= window_end and end_at >= window_start


def _expand_weekly_byday(
    *,
    start_at: datetime,
    end_at: datetime,
    rule: dict[str, str],
    window_start: datetime,
    window_end: datetime,
    excluded_starts: set[datetime],
) -> list[tuple[datetime, datetime]]:
    interval = max(1, int(rule.get("INTERVAL") or 1))
    duration = end_at - start_at
    count_limit = int(rule.get("COUNT") or 0)
    until = _parse_rrule_until(rule.get("UNTIL", ""), False)
    weekdays = [
        _WEEKDAY_BY_ICS[day[-2:]]
        for day in str(rule.get("BYDAY") or "").split(",")
        if day[-2:] in _WEEKDAY_BY_ICS
    ]
    if not weekdays:
        return []
    weekdays = sorted(set(weekdays))
    week_start = start_at - timedelta(days=start_at.weekday())
    emitted = 0
    instances: list[tuple[datetime, datetime]] = []
    safety = 0
    current_week = week_start
    while safety < 2000:
        safety += 1
        for weekday in weekdays:
            occurrence = current_week + timedelta(days=weekday)
            occurrence = occurrence.replace(hour=start_at.hour, minute=start_at.minute, second=start_at.second)
            if occurrence < start_at:
                continue
            if until and occurrence > until:
                return instances
            emitted += 1
            if count_limit and emitted > count_limit:
                return instances
            if occurrence not in excluded_starts and _overlaps_window(occurrence, occurrence + duration, window_start, window_end):
                instances.append((occurrence, occurrence + duration))
        current_week += timedelta(weeks=interval)
        if current_week > window_end + timedelta(days=7):
            break
    return instances


def _expand_event_instances(
    props: dict[str, list[tuple[dict[str, str], str]]],
    *,
    window_start: datetime,
    window_end: datetime,
) -> list[tuple[datetime, datetime, bool, str]]:
    start_at, end_at, all_day = _event_times(props)
    duration = end_at - start_at
    uid = _first_prop(props, "UID", f"event:{start_at.isoformat()}") or f"event:{start_at.isoformat()}"
    rrule_value = _first_prop(props, "RRULE")
    excluded_starts = _parse_exdates(props)

    if not rrule_value:
        if start_at in excluded_starts or not _overlaps_window(start_at, end_at, window_start, window_end):
            return []
        return [(start_at, end_at, all_day, uid)]

    rule = _parse_rrule(rrule_value)
    freq = str(rule.get("FREQ") or "").upper()
    interval = max(1, int(rule.get("INTERVAL") or 1))
    count_limit = int(rule.get("COUNT") or 0)
    until = _parse_rrule_until(rule.get("UNTIL", ""), all_day)

    if freq == "WEEKLY" and rule.get("BYDAY"):
        return [
            (instance_start, instance_end, all_day, f"{uid}:{instance_start.isoformat()}")
            for instance_start, instance_end in _expand_weekly_byday(
                start_at=start_at,
                end_at=end_at,
                rule=rule,
                window_start=window_start,
                window_end=window_end,
                excluded_starts=excluded_starts,
            )
        ]

    instances: list[tuple[datetime, datetime, bool, str]] = []
    current = start_at
    emitted = 0
    safety = 0
    while safety < 2000:
        safety += 1
        if until and current > until:
            break
        emitted += 1
        if count_limit and emitted > count_limit:
            break
        current_end = current + duration
        if current not in excluded_starts and _overlaps_window(current, current_end, window_start, window_end):
            instances.append((current, current_end, all_day, f"{uid}:{current.isoformat()}"))
        if current > window_end and not count_limit:
            break
        if freq == "DAILY":
            current = current + timedelta(days=interval)
        elif freq == "WEEKLY":
            current = current + timedelta(weeks=interval)
        elif freq == "MONTHLY":
            next_value = _add_months(current, interval)
            if next_value is None:
                break
            current = next_value
        elif freq == "YEARLY":
            next_value = _add_years(current, interval)
            if next_value is None:
                break
            current = next_value
        else:
            break
    return instances


def _feed_url_from_webcal(value: str) -> str:
    clean = str(value or "").strip()
    if clean.startswith("webcal://"):
        return "https://" + clean.removeprefix("webcal://")
    return clean


def _read_ics_text(*, feed_url: Optional[str] = None, feed_path: Optional[Path] = None, timeout_seconds: float = 30.0) -> str:
    if feed_path is not None:
        return feed_path.expanduser().read_text(encoding="utf-8")
    clean_url = _feed_url_from_webcal(str(feed_url or ""))
    if not clean_url:
        raise ValueError("feed_url or feed_path is required")
    request = urllib.request.Request(clean_url, headers={"User-Agent": "life-ops/0.2 apple-calendar-sync"})
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        raw_bytes = response.read()
    return raw_bytes.decode("utf-8-sig")


def _calendar_id_for_feed(*, feed_url: Optional[str], feed_path: Optional[Path], calendar_name: str, calendar_props: dict) -> str:
    name = calendar_name or _first_prop(calendar_props, "X-WR-CALNAME", "Apple Calendar")
    source_value = str(feed_url or feed_path or name)
    digest = hashlib.sha256(source_value.encode("utf-8")).hexdigest()[:12]
    clean_name = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-") or "apple-calendar"
    return f"{clean_name}:{digest}"


def sync_apple_calendar_feed(
    connection,
    *,
    feed_url: Optional[str] = None,
    feed_path: Optional[Path] = None,
    calendar_name: str = "",
    days_back: int = DEFAULT_APPLE_CALENDAR_DAYS_BACK,
    days_ahead: int = DEFAULT_APPLE_CALENDAR_DAYS_AHEAD,
    timeout_seconds: float = 30.0,
) -> dict:
    trace_run_id = tracing.start_trace_run(
        connection,
        trace_type="apple_calendar_feed_sync",
        metadata={
            "feed_url_present": bool(feed_url),
            "feed_path": str(feed_path) if feed_path is not None else "",
            "calendar_name": calendar_name,
            "days_back": days_back,
            "days_ahead": days_ahead,
        },
    )
    try:
        raw_text = _read_ics_text(feed_url=feed_url, feed_path=feed_path, timeout_seconds=timeout_seconds)
        calendar_props, events = _parse_ics_properties(raw_text)
        resolved_name = calendar_name or _first_prop(calendar_props, "X-WR-CALNAME", "Apple Calendar")
        calendar_id = _calendar_id_for_feed(
            feed_url=feed_url,
            feed_path=feed_path,
            calendar_name=resolved_name,
            calendar_props=calendar_props,
        )
        today = date.today()
        window_start = datetime.combine(today - timedelta(days=days_back), time(0, 0))
        window_end = datetime.combine(today + timedelta(days=days_ahead), time(23, 59))

        deleted_rows = store.delete_events_for_calendar(
            connection,
            source=APPLE_CALENDAR_SOURCE,
            external_calendar_id=calendar_id,
        )

        instances_processed = 0
        skipped_events = 0
        for props in events:
            try:
                instances = _expand_event_instances(
                    props,
                    window_start=window_start,
                    window_end=window_end,
                )
            except ValueError:
                skipped_events += 1
                continue

            title = _first_prop(props, "SUMMARY", "(untitled event)") or "(untitled event)"
            status = _first_prop(props, "STATUS", "confirmed").lower() or "confirmed"
            location = _first_prop(props, "LOCATION")
            description = _first_prop(props, "DESCRIPTION")
            html_link = _first_prop(props, "URL")
            uid = _first_prop(props, "UID", title)

            for start_at, end_at, all_day, instance_uid in instances:
                store.upsert_event_from_sync(
                    connection,
                    source=APPLE_CALENDAR_SOURCE,
                    external_id=f"{calendar_id}:{instance_uid}",
                    title=title,
                    start_at=start_at,
                    end_at=end_at,
                    all_day=all_day,
                    organization_name=resolved_name,
                    location=location,
                    kind="apple-calendar",
                    status="cancelled" if status == "cancelled" else status,
                    notes=description,
                    external_calendar_id=calendar_id,
                    external_etag=hashlib.sha256(f"{uid}:{start_at}:{end_at}:{title}".encode("utf-8")).hexdigest(),
                    html_link=html_link,
                )
                tracing.append_trace_event(
                    connection,
                    run_id=trace_run_id,
                    event_type="apple_calendar_event_synced",
                    entity_key=f"{calendar_id}:{instance_uid}",
                    payload={
                        "calendar_id": calendar_id,
                        "title": title,
                        "status": status,
                        "all_day": all_day,
                        "start_at": start_at.isoformat(timespec="minutes"),
                        "end_at": end_at.isoformat(timespec="minutes"),
                    },
                )
                instances_processed += 1

        store.set_sync_state(
            connection,
            key=f"apple_calendar:{calendar_id}:last_sync_at",
            value=datetime.now().isoformat(timespec="minutes"),
        )
        summary = {
            "calendar_id": calendar_id,
            "calendar_name": resolved_name,
            "source": APPLE_CALENDAR_SOURCE,
            "events_in_feed": len(events),
            "events_skipped": skipped_events,
            "events_deleted_before_sync": deleted_rows,
            "events_synced": instances_processed,
            "days_back": days_back,
            "days_ahead": days_ahead,
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
