from __future__ import annotations

import calendar as calendar_lib
import json
import os
import sqlite3
from datetime import date, timedelta
from html import escape
from pathlib import Path
from typing import Optional

from life_ops.agenda import build_agenda
from life_ops import store


OPEN_CALENDAR_STATUSES = {"planned", "in_progress", "missed", "deferred"}
DONE_CALENDAR_STATUSES = {"done"}
ROLLOVER_STATUSES = {"planned", "in_progress", "missed", "deferred"}
ROLLOVER_TYPES = {"task", "habit", "carry_forward", "event"}
PRIORITY_ORDER = {"urgent": 0, "high": 1, "normal": 2, "low": 3, "": 4}
SECTION_ORDER = {
    "Hard Schedule": 0,
    "Signups / Bookings": 1,
    "GitHub / Notifications": 2,
    "ORP / Project Priorities": 3,
    "Comms / Follow-Ups": 4,
    "Professional / Projects": 5,
    "Personal / Home": 6,
    "General / Admin": 7,
    "Open Lists": 8,
    "Completed Today": 9,
}
ROADMAP_AGENDA_TYPES = {"event", "follow_up"}
CALENDAR_RANGE_DEFAULT_DAYS = 365
CALENDAR_RANGE_MAX_DAYS = 366
FOCUS_LOOKBACK_DAYS = 7
FOCUS_PRIORITY_LIMIT = 5
FEATURED_PROJECT_FILE_ENV = "LIFEOPS_DAY_SHEET_FEATURED_PROJECT_FILE"
FEATURED_PROJECT_NAME_ENV = "LIFEOPS_DAY_SHEET_FEATURED_PROJECT"
DEFAULT_FEATURED_PROJECT_FILE = Path.home() / ".lifeops" / "config" / "day-sheet-featured-project.txt"
AGENT_PROJECT_MARKERS = ("codex resume",)
FOCUS_PROJECT_SECTIONS = {
    "GitHub / Notifications",
    "ORP / Project Priorities",
    "Professional / Projects",
}
FOCUS_PRIORITY_SCORES = {"urgent": 90, "high": 65, "normal": 25, "low": 5}
FOCUS_STATUS_SCORES = {
    "in_progress": 24,
    "planned": 20,
    "done": 12,
    "deferred": 4,
    "missed": 4,
    "archived": 2,
}
GENERIC_NEXT_PREFIXES = (
    "open the interactive session and define the next concrete artifact",
    "review the project map and choose the next concrete action",
)
FRG_FIRST_PAGE_LOOKBACK_DAYS = 365
FRG_FIRST_PAGE_FORWARD_DAYS = 365
FRG_FIRST_PAGE_MAINSTAY_LIMIT = 18
FRG_FIRST_PAGE_COMMITMENT_LIMIT = 8
FRG_FIRST_PAGE_OPERATING_RULES = [
    {
        "title": "FRG first-page rule",
        "notes": "The printable day sheet starts with the Fractal Research Group priority surface before the rest of LifeOps.",
    },
    {
        "title": "Evidence over theater",
        "notes": "Tasks should cash out in calls, records, packets, docs, quotes, commits, or mailed asks.",
    },
    {
        "title": "Secrets boundary",
        "notes": "No passwords, full account numbers, SSNs, card numbers, API keys, or private medical data in repo or print artifacts.",
    },
    {
        "title": "Every live thread gets a next action",
        "notes": "Host, vendor, sponsor, public artifact, or do-not-pursue. No vague open loops.",
    },
]


def _format_short_day(day: date) -> str:
    return day.strftime("%a, %b %d").replace(" 0", " ")


def _plain_text(value: object) -> str:
    text = str(value or "")
    replacements = {
        "\u2010": "-",
        "\u2011": "-",
        "\u2012": "-",
        "\u2013": "-",
        "\u2014": "-",
        "\u2212": "-",
        "\u00b7": "/",
        "\u2022": "*",
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2026": "...",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return " ".join(text.split())


def _latex_escape(value: object) -> str:
    text = _plain_text(value)
    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    return "".join(replacements.get(char, char) for char in text)


def _json_field(value: str, fallback):
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def _row_value(row: sqlite3.Row, key: str, fallback=""):
    try:
        if key not in row.keys():
            return fallback
        return row[key]
    except (AttributeError, KeyError, IndexError):
        return fallback


def _format_day(day: date) -> str:
    return day.strftime("%A, %B %d, %Y").replace(" 0", " ")


def _entry_record(row: sqlite3.Row) -> dict:
    recurrence_frequency = str(_row_value(row, "recurrence_frequency", "") or "").strip().lower()
    recurrence_interval = int(_row_value(row, "recurrence_interval", 1) or 1)
    recurrence_anchor_date = str(_row_value(row, "recurrence_anchor_date", "") or "")
    entry_date = str(row["entry_date"])
    return {
        "id": int(row["id"]),
        "date": entry_date,
        "occurrence_date": entry_date,
        "occurrence_id": f"{int(row['id'])}:{entry_date}",
        "title": str(row["title"]),
        "type": str(row["entry_type"]),
        "status": str(row["status"]),
        "priority": str(row["priority"] or "normal"),
        "list_name": str(row["list_name"] or "personal"),
        "start_time": str(row["start_time"] or ""),
        "end_time": str(row["end_time"] or ""),
        "source": str(row["source"] or "manual"),
        "source_table": str(row["source_table"] or ""),
        "source_id": int(row["source_id"]) if row["source_id"] is not None else None,
        "notes": str(row["notes"] or ""),
        "tags": _json_field(str(row["tags_json"] or "[]"), []),
        "recurrence_frequency": recurrence_frequency,
        "recurrence_interval": recurrence_interval,
        "recurrence_until": str(_row_value(row, "recurrence_until", "") or ""),
        "recurrence_count": int(_row_value(row, "recurrence_count", 0) or 0),
        "recurrence_anchor_date": recurrence_anchor_date,
        "is_recurring": bool(recurrence_frequency and recurrence_frequency != "none"),
        "is_virtual": False,
        "recurrence_source_id": int(row["id"]) if recurrence_frequency and recurrence_frequency != "none" else None,
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
        "completed_at": str(row["completed_at"] or ""),
    }


def _day_note_record(row: Optional[sqlite3.Row], target_day: date) -> dict:
    if row is None:
        return {
            "day": target_day.isoformat(),
            "intention": "",
            "reflection": "",
            "notes": "",
            "mood": "",
            "energy": "",
            "created_at": "",
            "updated_at": "",
        }
    return {
        "day": str(row["day"]),
        "intention": str(row["intention"] or ""),
        "reflection": str(row["reflection"] or ""),
        "notes": str(row["notes"] or ""),
        "mood": str(row["mood"] or ""),
        "energy": str(row["energy"] or ""),
        "created_at": str(row["created_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def _snapshot_record(row: sqlite3.Row) -> dict:
    return {
        "id": int(row["id"]),
        "day": str(row["day"]),
        "snapshot_at": str(row["snapshot_at"] or ""),
        "title": str(row["title"] or ""),
        "summary": str(row["summary"] or ""),
        "payload": _json_field(str(row["payload_json"] or "{}"), {}),
        "created_at": str(row["created_at"] or ""),
    }


def _list_item_record(row: sqlite3.Row) -> dict:
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


def _last_day_of_month(year: int, month: int) -> int:
    return calendar_lib.monthrange(year, month)[1]


def _add_months(anchor: date, months: int) -> date:
    month_index = anchor.month - 1 + months
    year = anchor.year + month_index // 12
    month = month_index % 12 + 1
    return date(year, month, min(anchor.day, _last_day_of_month(year, month)))


def _add_years(anchor: date, years: int) -> date:
    target_year = anchor.year + years
    return date(target_year, anchor.month, min(anchor.day, _last_day_of_month(target_year, anchor.month)))


def _recurrence_occurrence_date(anchor: date, frequency: str, interval: int, index: int) -> date:
    if frequency == "daily":
        return anchor + timedelta(days=interval * index)
    if frequency == "weekly":
        return anchor + timedelta(days=7 * interval * index)
    if frequency == "monthly":
        return _add_months(anchor, interval * index)
    if frequency == "yearly":
        return _add_years(anchor, interval * index)
    return anchor


def _recurrence_dates(entry: dict, *, start_day: date, end_day: date) -> list[date]:
    frequency = str(entry.get("recurrence_frequency") or "").strip().lower()
    if not frequency or frequency == "none":
        return []
    interval = max(1, int(entry.get("recurrence_interval") or 1))
    anchor_text = str(entry.get("recurrence_anchor_date") or entry.get("date") or "")
    if not anchor_text:
        return []
    anchor = date.fromisoformat(anchor_text)
    until_text = str(entry.get("recurrence_until") or "").strip()
    recurrence_end = min(end_day, date.fromisoformat(until_text)) if until_text else end_day
    if recurrence_end < anchor or recurrence_end < start_day:
        return []
    count = int(entry.get("recurrence_count") or 0)
    dates: list[date] = []
    max_iterations = max(count, 0) if count else 200000
    for index in range(max_iterations):
        occurrence_day = _recurrence_occurrence_date(anchor, frequency, interval, index)
        if occurrence_day > recurrence_end:
            break
        if occurrence_day >= start_day:
            dates.append(occurrence_day)
    return dates


def _recurring_occurrence_record(entry: dict, occurrence_day: date) -> dict:
    occurrence_date = occurrence_day.isoformat()
    return {
        **entry,
        "date": occurrence_date,
        "occurrence_date": occurrence_date,
        "occurrence_id": f"{entry['id']}:{occurrence_date}",
        "is_recurring": True,
        "is_virtual": occurrence_date != entry.get("date"),
        "recurrence_source_id": entry["id"],
    }


def _calendar_entries_for_range(
    connection: sqlite3.Connection,
    *,
    start_day: date,
    end_day: date,
    status: str = "all",
) -> list[dict]:
    entries = [
        _entry_record(row)
        for row in store.list_calendar_entries(
            connection,
            start_day=start_day,
            end_day=end_day,
            status=status,
        )
    ]
    direct_keys = {(entry["id"], entry["date"]) for entry in entries}
    for row in store.list_recurring_calendar_entries(
        connection,
        start_day=start_day,
        end_day=end_day,
        status=status,
    ):
        master = _entry_record(row)
        for occurrence_day in _recurrence_dates(master, start_day=start_day, end_day=end_day):
            occurrence_key = (master["id"], occurrence_day.isoformat())
            if occurrence_key in direct_keys:
                continue
            entries.append(_recurring_occurrence_record(master, occurrence_day))
    entries.sort(key=lambda entry: (entry["date"], _time_rank(entry.get("start_time", "")), _priority_rank(entry.get("priority", "")), entry["title"], entry["id"]))
    return entries


def _completed_list_items_for_day(connection: sqlite3.Connection, target_day: date) -> list[dict]:
    prefix = target_day.isoformat()
    rows = connection.execute(
        """
        SELECT *
        FROM list_items
        WHERE status = 'done'
          AND completed_at LIKE ?
        ORDER BY completed_at DESC, id DESC
        """,
        (f"{prefix}%",),
    ).fetchall()
    return [_list_item_record(row) for row in rows]


def _calendar_stats(entries: list[dict], agenda_day: dict, open_list_items: list[dict], completed_list_items: list[dict]) -> dict:
    by_status: dict[str, int] = {}
    by_type: dict[str, int] = {}
    for entry in entries:
        by_status[entry["status"]] = by_status.get(entry["status"], 0) + 1
        by_type[entry["type"]] = by_type.get(entry["type"], 0) + 1
    return {
        "tracked_entries": len(entries),
        "done_entries": sum(1 for entry in entries if entry["status"] in DONE_CALENDAR_STATUSES),
        "open_entries": sum(1 for entry in entries if entry["status"] in OPEN_CALENDAR_STATUSES),
        "agenda_items": len(agenda_day.get("items") or []),
        "open_list_items": len(open_list_items),
        "completed_list_items": len(completed_list_items),
        "by_status": by_status,
        "by_type": by_type,
    }


def _priority_rank(priority: str) -> int:
    return PRIORITY_ORDER.get(str(priority or "").lower(), PRIORITY_ORDER[""])


def _time_rank(value: str) -> str:
    return str(value or "99:99")


def _section_rank(name: str) -> int:
    return SECTION_ORDER.get(name, 99)


def _entry_section(entry: dict) -> str:
    tags = {str(tag).lower() for tag in entry.get("tags") or []}
    title = str(entry.get("title") or "").lower()
    source = str(entry.get("source") or "").lower()
    if (
        "booking" in tags
        or "signup" in tags
        or "sign-up" in tags
        or "frg_site_booking" in source
        or title.startswith("frg booking:")
        or "booking:" in title
        or "signup" in title
        or "sign-up" in title
    ):
        return "Signups / Bookings"
    if "github" in tags or "github-morning-sweep" in tags or title.startswith("github:"):
        return "GitHub / Notifications"
    if "orp" in tags or "orp-project-sweep" in tags or title.startswith("orp:"):
        return "ORP / Project Priorities"
    list_name = str(entry.get("list_name") or "")
    if list_name == "professional":
        return "Professional / Projects"
    if list_name == "personal":
        return "Personal / Home"
    return "General / Admin"


def _is_meeting_booking_entry(entry: dict) -> bool:
    if _entry_section(entry) != "Signups / Bookings":
        return False
    if str(entry.get("source") or "").lower() == "frg_site_booking":
        return True
    if str(entry.get("type") or "").lower() == "event":
        return True
    if str(entry.get("start_time") or "").strip() or str(entry.get("end_time") or "").strip():
        return True
    return False


def _is_calendar_hold_entry(entry: dict) -> bool:
    if _is_meeting_booking_entry(entry):
        return True
    tags = {str(tag).lower() for tag in entry.get("tags") or []}
    title = str(entry.get("title") or "").lower()
    notes = str(entry.get("notes") or "").lower()
    if "birthday" in tags and not tags.intersection({"hold", "calendar-hold"}):
        return False
    if tags.intersection({"hold", "calendar-hold", "booking", "signup", "sign-up"}):
        return True
    if "hold" in title or "calendar hold" in notes:
        return True
    if str(entry.get("type") or "").lower() == "event":
        return True
    return bool(str(entry.get("start_time") or "").strip() or str(entry.get("end_time") or "").strip())


def _is_project_inventory_entry(entry: dict) -> bool:
    if _is_calendar_hold_entry(entry):
        return False
    section = _entry_section(entry)
    return section in FOCUS_PROJECT_SECTIONS


def _clip_text(value: object, limit: int) -> str:
    text = _plain_text(value)
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


def _safe_day(value: object) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _notes_label_values(notes: object, label: str) -> list[str]:
    values: list[str] = []
    prefix = f"{label.lower()}:"
    for raw_line in str(notes or "").splitlines():
        line = raw_line.strip()
        if line.lower().startswith(prefix):
            values.append(line.split(":", 1)[1].strip())
    return [value for value in values if value]


def _notes_label_value(notes: object, label: str) -> str:
    values = _notes_label_values(notes, label)
    return values[0] if values else ""


def _is_frg_entry(entry: dict) -> bool:
    tags = {str(tag or "").strip().lower() for tag in entry.get("tags") or []}
    title = _plain_text(entry.get("title") or "").lower()
    source = str(entry.get("source") or "").strip().lower()
    return (
        "frg" in tags
        or "frg-mainstay" in tags
        or "fractal-research-group" in tags
        or "frg" in source
        or title.startswith("frg:")
        or title.startswith("frg ")
        or title.startswith("frg mainstay:")
        or "fractal research group" in title
    )


def _is_frg_mainstay_entry(entry: dict) -> bool:
    tags = {str(tag or "").strip().lower() for tag in entry.get("tags") or []}
    title = _plain_text(entry.get("title") or "").lower()
    return "frg-mainstay" in tags or title.startswith("frg mainstay:")


def _frg_title(value: object) -> str:
    title = _plain_text(value)
    lowered = title.lower()
    for prefix in ("frg mainstay:", "frg:"):
        if lowered.startswith(prefix):
            return title[len(prefix):].strip() or title
    return title


def _frg_page_item(entry: dict, *, include_date: bool) -> dict:
    time_label = entry.get("start_time") or ""
    if entry.get("end_time"):
        time_label = f"{time_label or 'anytime'}-{entry['end_time']}"
    day_value = _safe_day(entry.get("date"))
    return {
        "id": entry.get("id"),
        "title": _frg_title(entry.get("title") or ""),
        "date": entry.get("date") or "",
        "date_label": _format_short_day(day_value) if day_value and include_date else "",
        "time": time_label,
        "priority": entry.get("priority") or "normal",
        "status": entry.get("status") or "",
        "notes": entry.get("notes") or "",
        "tags": entry.get("tags") or [],
        "list_name": entry.get("list_name") or "",
    }


def _frg_page_sort_key(item: dict) -> tuple[int, str, str, str]:
    return (
        _priority_rank(item.get("priority") or ""),
        str(item.get("date") or ""),
        _time_rank(str(item.get("time") or "")),
        str(item.get("title") or ""),
    )


def _build_frg_first_page(connection: sqlite3.Connection, *, target_day: date) -> dict:
    start_day = target_day - timedelta(days=FRG_FIRST_PAGE_LOOKBACK_DAYS)
    end_day = target_day + timedelta(days=FRG_FIRST_PAGE_FORWARD_DAYS)
    entries = _calendar_entries_for_range(
        connection,
        start_day=start_day,
        end_day=end_day,
        status="all",
    )
    open_frg_entries = [
        entry for entry in entries
        if entry["status"] in OPEN_CALENDAR_STATUSES and _is_frg_entry(entry)
    ]
    mainstay_items = [
        _frg_page_item(entry, include_date=entry.get("date") != target_day.isoformat())
        for entry in open_frg_entries
        if _is_frg_mainstay_entry(entry)
    ]
    commitment_items = [
        _frg_page_item(entry, include_date=True)
        for entry in open_frg_entries
        if not _is_frg_mainstay_entry(entry)
        and (
            (_safe_day(entry.get("date")) or target_day) >= target_day
            or str(entry.get("priority") or "") == "urgent"
        )
        and (
            str(entry.get("type") or "") == "event"
            or bool(str(entry.get("start_time") or "").strip())
            or str(entry.get("priority") or "") == "urgent"
        )
    ]
    mainstay_items = sorted(mainstay_items, key=_frg_page_sort_key)[:FRG_FIRST_PAGE_MAINSTAY_LIMIT]
    commitment_items = sorted(commitment_items, key=_frg_page_sort_key)[:FRG_FIRST_PAGE_COMMITMENT_LIMIT]
    return {
        "enabled": True,
        "date": target_day.isoformat(),
        "label": _format_day(target_day),
        "hard_commitments": commitment_items,
        "mainstay_items": mainstay_items,
        "operating_rules": FRG_FIRST_PAGE_OPERATING_RULES,
        "counts": {
            "hard_commitments": len(commitment_items),
            "mainstay_items": len(mainstay_items),
            "open_frg_entries": len(open_frg_entries),
        },
        "summary": "FRG mainstay work first, then the rest of the LifeOps schedule.",
    }


def _strip_numeric_suffix(value: str) -> str:
    text = value.strip()
    if text.endswith(")") and " (" in text:
        base, suffix = text.rsplit(" (", 1)
        suffix_value = suffix[:-1]
        if suffix_value.isdigit():
            return base.strip()
    return text


def _focus_project_title(entry: dict) -> str:
    notes_project = _notes_label_value(entry.get("notes"), "Project")
    if notes_project:
        return _strip_numeric_suffix(notes_project)
    title = _plain_text(entry.get("title") or "")
    lowered = title.lower()
    if lowered.startswith("orp workspace:"):
        title = title.split(":", 1)[1].strip()
    elif lowered.startswith("orp:"):
        title = title.split(":", 1)[1].strip()
        if " - " in title:
            title = title.split(" - ", 1)[0].strip()
    return _strip_numeric_suffix(title)


def _focus_project_key(title: str) -> str:
    return " ".join(_plain_text(title).lower().split())


def _split_featured_project_names(value: str) -> list[str]:
    names: list[str] = []
    for raw_line in value.replace("|", "\n").replace(";", "\n").splitlines():
        clean = _plain_text(raw_line)
        if clean and not clean.startswith("#"):
            names.append(clean)
    return names


def _configured_featured_project_names(explicit_name: str | None = None) -> tuple[list[str], str]:
    explicit = _plain_text(explicit_name or "")
    if explicit:
        return _split_featured_project_names(explicit), "argument"

    env_value = _plain_text(os.environ.get(FEATURED_PROJECT_NAME_ENV, ""))
    if env_value:
        return _split_featured_project_names(env_value), "environment"

    configured_path = Path(
        os.environ.get(FEATURED_PROJECT_FILE_ENV, str(DEFAULT_FEATURED_PROJECT_FILE))
    ).expanduser()
    try:
        names: list[str] = []
        for line in configured_path.read_text().splitlines():
            clean = _plain_text(line)
            if clean and not clean.startswith("#"):
                names.append(clean)
        if names:
            return names, str(configured_path)
    except OSError:
        return [], "auto"
    return [], str(configured_path)


def _configured_featured_project_name(explicit_name: str | None = None) -> tuple[str, str]:
    names, source = _configured_featured_project_names(explicit_name)
    return (names[0] if names else ""), source


def _focus_rank(entry: dict) -> int | None:
    for value in _notes_label_values(entry.get("notes"), "Rank"):
        first = value.split()[0].strip("#")
        try:
            return int(first)
        except ValueError:
            continue
    return None


def _is_generic_next(value: str) -> bool:
    lowered = _plain_text(value).lower()
    return any(lowered.startswith(prefix) for prefix in GENERIC_NEXT_PREFIXES)


def _focus_details(entry: dict) -> dict:
    notes = entry.get("notes") or ""
    next_values = _notes_label_values(notes, "Next")
    today = _notes_label_value(notes, "Today")
    roadmap_next = next_values[1] if len(next_values) > 1 else (next_values[0] if next_values else "")
    if roadmap_next and _is_generic_next(roadmap_next) and today:
        roadmap_next = ""
    proof = _notes_label_value(notes, "Proof")
    then = _notes_label_value(notes, "Then")
    return {
        "today": today,
        "next": roadmap_next,
        "then": then,
        "proof": proof,
    }


def _focus_abstract(project_title: str, details: dict) -> str:
    today = _clip_text(details.get("today") or "", 150)
    next_step = _clip_text(details.get("next") or "", 150)
    proof = _clip_text(details.get("proof") or "", 150)
    if today and proof:
        return _clip_text(f"Thread: {today} Proof target: {proof}", 260)
    if today and next_step:
        return _clip_text(f"Thread: {today} Next question: {next_step}", 260)
    if next_step and proof:
        return _clip_text(f"Thread: {next_step} Proof target: {proof}", 260)
    if today:
        return _clip_text(f"Thread: {today}", 220)
    if next_step:
        return _clip_text(f"Thread: {next_step}", 220)
    if proof:
        return _clip_text(f"Proof target: {proof}", 220)
    return _clip_text(f"Clarify what {project_title} should prove next.", 220)


def _question_fragment(value: object, limit: int = 140) -> str:
    return _clip_text(value, limit).rstrip(" .?!")


def _question_object(value: object, limit: int = 140) -> str:
    text = _question_fragment(value, limit)
    if text.startswith("A "):
        return "a " + text[2:]
    if text.startswith("An "):
        return "an " + text[3:]
    if text.startswith("The "):
        return "the " + text[4:]
    return text


def _focus_question(project_title: str, details: dict) -> str:
    today = _question_fragment(details.get("today") or "")
    next_step = _question_fragment(details.get("next") or "")
    proof = _question_object(details.get("proof") or "")
    if today and proof:
        return _clip_text(f"What would make {project_title} produce {proof}?", 220)
    if today and next_step:
        return _clip_text(
            f"What is the blocking question between '{today}' and '{next_step}' for {project_title}?",
            220,
        )
    if next_step and proof:
        return _clip_text(
            f"What decision or evidence would make {proof} the natural outcome of '{next_step}'?",
            220,
        )
    if proof:
        return _clip_text(f"What evidence would make {proof} real for {project_title}?", 220)
    return _clip_text(
        f"What is the most important unanswered question for {project_title}, and what proof would settle it?",
        220,
    )


def _focus_signal_day(entry: dict, target_day: date) -> date | None:
    candidates = [
        _safe_day(entry.get("date")),
        _safe_day(entry.get("completed_at")),
        _safe_day(entry.get("updated_at")),
        _safe_day(entry.get("created_at")),
    ]
    valid = [candidate for candidate in candidates if candidate is not None and candidate <= target_day]
    if not valid:
        return next((candidate for candidate in candidates if candidate is not None), None)
    return max(valid)


def _is_focus_priority_candidate(entry: dict) -> bool:
    status = str(entry.get("status") or "").lower()
    if status == "canceled":
        return False
    title = str(entry.get("title") or "").strip()
    if not title:
        return False
    if title.lower().startswith("orp project sweep:"):
        return False
    if _is_calendar_hold_entry(entry):
        return False
    section = _entry_section(entry)
    if section == "Signups / Bookings":
        return False
    tags = {str(tag).lower() for tag in entry.get("tags") or []}
    source = str(entry.get("source") or "").lower()
    if source == "orp" or "project-priority" in tags or "orp-workspace" in tags:
        return True
    if section in FOCUS_PROJECT_SECTIONS and str(entry.get("list_name") or "") == "professional":
        return True
    return str(entry.get("list_name") or "") == "professional" and str(entry.get("priority") or "") in {"urgent", "high"}


def _focus_entry_score(entry: dict, target_day: date) -> int:
    priority = str(entry.get("priority") or "normal").lower()
    status = str(entry.get("status") or "").lower()
    rank = _focus_rank(entry)
    signal_day = _focus_signal_day(entry, target_day)
    age = (target_day - signal_day).days if signal_day and signal_day <= target_day else 0
    recency_score = max(0, FOCUS_LOOKBACK_DAYS - max(0, age))
    tags = {str(tag).lower() for tag in entry.get("tags") or []}
    source = str(entry.get("source") or "").lower()
    rank_score = max(0, 22 - rank) * 3 if rank is not None else 0
    source_score = 15 if source == "orp" or "project-priority" in tags or "orp-workspace" in tags else 0
    return (
        FOCUS_PRIORITY_SCORES.get(priority, FOCUS_PRIORITY_SCORES["normal"])
        + FOCUS_STATUS_SCORES.get(status, 0)
        + rank_score
        + source_score
        + recency_score
    )


def _focus_entry_preference(entry: dict) -> tuple[int, int, int, str, int]:
    status_order = {"in_progress": 0, "planned": 1, "done": 2, "deferred": 3, "missed": 4, "archived": 5}
    status_rank = status_order.get(str(entry.get("status") or "").lower(), 6)
    rank = _focus_rank(entry)
    return (
        status_rank,
        _priority_rank(str(entry.get("priority") or "")),
        rank if rank is not None else 999,
        str(entry.get("date") or ""),
        -int(entry.get("id") or 0),
    )


def _focus_record(entry: dict, *, score: int, signal_count: int, signal_dates: list[str]) -> dict:
    project_title = _focus_project_title(entry)
    details = _focus_details(entry)
    rank = _focus_rank(entry)
    return {
        "kind": "focus_priority",
        "title": project_title,
        "priority": str(entry.get("priority") or "normal"),
        "status": str(entry.get("status") or ""),
        "date": str(entry.get("date") or ""),
        "date_label": _format_short_day(date.fromisoformat(str(entry.get("date")))),
        "section": _entry_section(entry),
        "source": str(entry.get("source") or ""),
        "rank": rank,
        "score": score,
        "signal_count": signal_count,
        "signal_dates": signal_dates,
        "abstract": _focus_abstract(project_title, details),
        "question": _focus_question(project_title, details),
        "roadmap": details,
    }


def _focus_project_records_from_entries(entries: list[dict], *, target_day: date) -> list[dict]:
    groups: dict[str, dict] = {}
    for entry in entries:
        if not _is_focus_priority_candidate(entry):
            continue
        project_title = _focus_project_title(entry)
        project_key = _focus_project_key(project_title)
        if not project_key:
            continue
        group = groups.setdefault(
            project_key,
            {
                "entry": entry,
                "score": 0,
                "signal_count": 0,
                "signal_dates": set(),
            },
        )
        score = _focus_entry_score(entry, target_day)
        group["score"] = max(group["score"], score)
        group["signal_count"] += 1
        signal_day = _focus_signal_day(entry, target_day)
        if signal_day is not None:
            group["signal_dates"].add(signal_day.isoformat())
        if _focus_entry_preference(entry) < _focus_entry_preference(group["entry"]):
            group["entry"] = entry

    records = []
    for group in groups.values():
        score = int(group["score"]) + min(30, max(0, int(group["signal_count"]) - 1) * 6)
        records.append(
            _focus_record(
                group["entry"],
                score=score,
                signal_count=int(group["signal_count"]),
                signal_dates=sorted(group["signal_dates"]),
            )
        )
    records.sort(
        key=lambda item: (
            -int(item.get("score") or 0),
            _priority_rank(str(item.get("priority") or "")),
            item.get("rank") if item.get("rank") is not None else 999,
            str(item.get("title") or ""),
        )
    )
    return records


def _featured_project_match(records: list[dict], selected_name: str) -> dict | None:
    selected_key = _focus_project_key(selected_name)
    if not selected_key:
        return records[0] if records else None

    exact = [
        record
        for record in records
        if _focus_project_key(str(record.get("title") or "")) == selected_key
    ]
    if exact:
        return exact[0]

    fuzzy = [
        record
        for record in records
        if selected_key in _focus_project_key(str(record.get("title") or ""))
        or _focus_project_key(str(record.get("title") or "")) in selected_key
    ]
    return fuzzy[0] if fuzzy else None


def _synthetic_featured_project(selected_name: str) -> dict:
    title_key = _focus_project_key(selected_name)
    is_options_stack = "option" in title_key and ("probability" in title_key or "financial" in title_key)
    is_futures_lab = "future" in title_key and ("lab" in title_key or "trading" in title_key)
    if is_options_stack:
        today = "Turn the options probability hunter into one checked decision path from data to risk-bounded watchlist output."
        proof = "A paper-trade-ready options rule with a checked probability table, risk boundary, and watchlist output."
    elif is_futures_lab:
        today = "Define the futures lab as a paper-trading system first: strategy menu, market data, simulator loop, risk controls, and review log."
        proof = "A paper-trading checklist with replay results, sizing rules, stop rules, max daily loss, and a no-live-trading gate."
    else:
        today = f"Clarify the next proof artifact for {selected_name}."
        proof = f"A saved decision, artifact, or checklist for {selected_name}."
    return {
        "kind": "focus_priority",
        "title": selected_name,
        "display_title": selected_name,
        "priority": "high",
        "status": "planned",
        "date": "",
        "date_label": "",
        "section": "Professional / Projects",
        "source": "configured",
        "rank": None,
        "score": 0,
        "signal_count": 0,
        "signal_dates": [],
        "abstract": today,
        "question": f"What proof would make {selected_name} safe to move into repeated paper trading?",
        "roadmap": {
            "today": today,
            "next": "",
            "then": "",
            "proof": proof,
        },
        "synthetic": True,
    }


def _agent_resume_command(entry: dict) -> str:
    notes = str(entry.get("notes") or "")
    for raw_line in notes.splitlines():
        line = raw_line.strip()
        if "Resume:" in line:
            return _plain_text(line.split("Resume:", 1)[1].strip())
    return ""


def _agent_project_path(entry: dict) -> str:
    path = _notes_label_value(entry.get("notes"), "Path")
    if " / Resume:" in path:
        path = path.split(" / Resume:", 1)[0].strip()
    return _plain_text(path)


def _is_agent_project_entry(entry: dict) -> bool:
    notes = str(entry.get("notes") or "").lower()
    return any(marker in notes for marker in AGENT_PROJECT_MARKERS)


def _agent_history_record(entry: dict, *, signal_count: int, signal_dates: list[str]) -> dict:
    rank = _focus_rank(entry)
    return {
        "kind": "agent_project_history",
        "title": _focus_project_title(entry),
        "priority": str(entry.get("priority") or "normal"),
        "status": str(entry.get("status") or ""),
        "date": str(entry.get("date") or ""),
        "date_label": _format_short_day(date.fromisoformat(str(entry.get("date")))),
        "rank": rank,
        "signal_count": signal_count,
        "signal_dates": signal_dates,
        "resume": _agent_resume_command(entry),
        "path": _agent_project_path(entry),
    }


def _agent_entry_preference(entry: dict, target_day: date) -> tuple[int, int, int, int, str, int]:
    signal_day = _focus_signal_day(entry, target_day)
    day_key = signal_day.isoformat() if signal_day else ""
    return (
        -int(day_key.replace("-", "") or 0),
        *_focus_entry_preference(entry),
    )


def _build_agent_project_history(
    connection: sqlite3.Connection,
    *,
    target_day: date,
    lookback_days: int = FOCUS_LOOKBACK_DAYS,
) -> dict:
    clean_days = max(1, int(lookback_days))
    start_day = target_day - timedelta(days=clean_days - 1)
    params = (
        start_day.isoformat(),
        target_day.isoformat(),
        start_day.isoformat(),
        target_day.isoformat(),
        start_day.isoformat(),
        target_day.isoformat(),
        start_day.isoformat(),
        target_day.isoformat(),
    )
    rows = connection.execute(
        """
        SELECT *
        FROM calendar_entries
        WHERE (
            entry_date BETWEEN ? AND ?
            OR substr(created_at, 1, 10) BETWEEN ? AND ?
            OR substr(updated_at, 1, 10) BETWEEN ? AND ?
            OR substr(COALESCE(completed_at, ''), 1, 10) BETWEEN ? AND ?
        )
          AND status != 'canceled'
          AND lower(notes) LIKE '%codex resume%'
        """,
        params,
    ).fetchall()

    groups: dict[str, dict] = {}
    for entry in (_entry_record(row) for row in rows):
        if not _is_agent_project_entry(entry):
            continue
        project_title = _focus_project_title(entry)
        project_key = _focus_project_key(project_title)
        if not project_key:
            continue
        group = groups.setdefault(
            project_key,
            {
                "entry": entry,
                "signal_count": 0,
                "signal_dates": set(),
            },
        )
        group["signal_count"] += 1
        signal_day = _focus_signal_day(entry, target_day)
        if signal_day is not None:
            group["signal_dates"].add(signal_day.isoformat())
        if _agent_entry_preference(entry, target_day) < _agent_entry_preference(group["entry"], target_day):
            group["entry"] = entry

    items = [
        _agent_history_record(
            group["entry"],
            signal_count=int(group["signal_count"]),
            signal_dates=sorted(group["signal_dates"]),
        )
        for group in groups.values()
    ]
    items.sort(
        key=lambda item: (
            _priority_rank(str(item.get("priority") or "")),
            item.get("rank") if item.get("rank") is not None else 999,
            str(item.get("title") or ""),
        )
    )
    return {
        "start_date": start_day.isoformat(),
        "end_date": target_day.isoformat(),
        "lookback_days": clean_days,
        "items": items,
        "count": len(items),
        "total_count": len(items),
        "overflow_count": 0,
    }


def _build_focus_priorities(
    connection: sqlite3.Connection,
    *,
    target_day: date,
    lookback_days: int = FOCUS_LOOKBACK_DAYS,
    limit: int = FOCUS_PRIORITY_LIMIT,
) -> dict:
    clean_days = max(1, int(lookback_days))
    clean_limit = max(1, int(limit))
    start_day = target_day - timedelta(days=clean_days - 1)
    params = (
        start_day.isoformat(),
        target_day.isoformat(),
        start_day.isoformat(),
        target_day.isoformat(),
        start_day.isoformat(),
        target_day.isoformat(),
        start_day.isoformat(),
        target_day.isoformat(),
    )
    rows = connection.execute(
        """
        SELECT *
        FROM calendar_entries
        WHERE (
            entry_date BETWEEN ? AND ?
            OR substr(created_at, 1, 10) BETWEEN ? AND ?
            OR substr(updated_at, 1, 10) BETWEEN ? AND ?
            OR substr(COALESCE(completed_at, ''), 1, 10) BETWEEN ? AND ?
        )
          AND status != 'canceled'
        """,
        params,
    ).fetchall()
    records = _focus_project_records_from_entries(
        [_entry_record(row) for row in rows],
        target_day=target_day,
    )
    items = records[:clean_limit]
    return {
        "start_date": start_day.isoformat(),
        "end_date": target_day.isoformat(),
        "lookback_days": clean_days,
        "limit": clean_limit,
        "items": items,
        "count": len(items),
        "total_count": len(records),
        "overflow_count": max(0, len(records) - len(items)),
        "abstract": _focus_summary_abstract(items, start_day=start_day, target_day=target_day),
    }


def _build_featured_project(
    connection: sqlite3.Connection,
    *,
    target_day: date,
    lookback_days: int = FOCUS_LOOKBACK_DAYS,
    featured_project_name: str | None = None,
) -> dict:
    selected_names, selected_source = _configured_featured_project_names(featured_project_name)
    if selected_names:
        rows = connection.execute(
            """
            SELECT *
            FROM calendar_entries
            WHERE status != 'canceled'
            """
        ).fetchall()
        records = _focus_project_records_from_entries(
            [_entry_record(row) for row in rows],
            target_day=target_day,
        )
        items: list[dict] = []
        missing_names: list[str] = []
        seen_keys: set[str] = set()
        for selected_name in selected_names:
            item = _featured_project_match(records, selected_name)
            if item:
                item = {**item, "display_title": selected_name}
            else:
                item = _synthetic_featured_project(selected_name)
                missing_names.append(selected_name)
            item_key = _focus_project_key(str(item.get("display_title") or item.get("title") or selected_name))
            if item_key in seen_keys:
                continue
            seen_keys.add(item_key)
            items.append(item)
    else:
        focus = _build_focus_priorities(
            connection,
            target_day=target_day,
            lookback_days=lookback_days,
            limit=1,
        )
        records = focus.get("items") or []
        items = [records[0]] if records else []
        missing_names = []
    item = items[0] if items else None

    return {
        "configured_name": selected_names[0] if selected_names else "",
        "configured_names": selected_names,
        "configured_source": selected_source,
        "item": item,
        "items": items,
        "count": len(items),
        "missing": bool(missing_names),
        "missing_names": missing_names,
    }


def _focus_summary_abstract(items: list[dict], *, start_day: date, target_day: date) -> str:
    titles = [str(item.get("title") or "") for item in items[:5] if item.get("title")]
    if not titles:
        return ""
    if len(titles) == 1:
        focus_text = titles[0]
    else:
        focus_text = ", ".join(titles[:-1]) + f", and {titles[-1]}"
    return (
        f"Recent focus from {start_day.isoformat()} to {target_day.isoformat()} clusters around "
        f"{focus_text}. Use these questions to create proof, decisions, or shipped artifacts instead of carrying a broad project inventory."
    )


def _sheet_item_sort_key(item: dict) -> tuple[int, str, str]:
    return (_priority_rank(item.get("priority", "")), _time_rank(item.get("time", "")), str(item.get("title") or ""))


def _add_section(sections: dict[str, list[dict]], name: str, item: dict) -> None:
    sections.setdefault(name, []).append(item)


def _calendar_entry_sheet_item(entry: dict) -> dict:
    time_label = entry["start_time"]
    if entry["end_time"]:
        time_label = f"{time_label or 'anytime'}-{entry['end_time']}"
    return {
        "kind": "calendar_entry",
        "id": entry["id"],
        "title": entry["title"],
        "priority": entry["priority"],
        "status": entry["status"],
        "time": time_label or "",
        "notes": entry["notes"],
        "tags": entry["tags"],
        "list_name": entry["list_name"],
    }


def _agenda_sheet_item(item: dict) -> dict:
    priority = item.get("priority_level") or "normal"
    if item.get("type") in {"routine", "event"}:
        priority = "normal"
    return {
        "kind": str(item.get("type") or "agenda"),
        "title": str(item.get("title") or ""),
        "priority": priority,
        "status": "scheduled",
        "time": str(item.get("time") or item.get("sort_time") or ""),
        "notes": str(item.get("notes") or ""),
        "tags": [],
        "list_name": "",
    }


def _list_sheet_item(item: dict) -> dict:
    return {
        "kind": "list_item",
        "id": item["id"],
        "title": item["title"],
        "priority": "normal",
        "status": item["status"],
        "time": "",
        "notes": item["notes"],
        "tags": [],
        "list_name": item["list_name"],
    }


def _roadmap_windows(target_day: date, days: int) -> list[dict]:
    if days <= 0:
        return []
    start_day = target_day + timedelta(days=1)
    end_day = target_day + timedelta(days=days)
    week_end = min(target_day + timedelta(days=6 - target_day.weekday()), end_day)
    windows: list[tuple[str, date, date]] = []
    if start_day <= week_end:
        windows.append(("Rest Of Week", start_day, week_end))

    next_week_start = max(start_day, week_end + timedelta(days=1))
    next_week_end = min(next_week_start + timedelta(days=6), end_day)
    if next_week_start <= next_week_end:
        windows.append(("Next Week", next_week_start, next_week_end))

    month_start = next_week_end + timedelta(days=1)
    month_end = min(target_day + timedelta(days=30), end_day)
    if month_start <= month_end:
        windows.append(("Month Ahead", month_start, month_end))

    year_start = month_end + timedelta(days=1)
    if year_start <= end_day:
        windows.append(("Year Ahead", year_start, end_day))

    return [
        {
            "name": name,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "start_label": _format_short_day(start),
            "end_label": _format_short_day(end),
            "items": [],
            "count": 0,
        }
        for name, start, end in windows
    ]


def _roadmap_item_sort_key(item: dict) -> tuple[str, int, str, str]:
    return (
        str(item.get("date") or ""),
        _priority_rank(item.get("priority", "")),
        _time_rank(item.get("time", "")),
        str(item.get("title") or ""),
    )


def _calendar_entry_roadmap_item(entry: dict) -> dict:
    item = _calendar_entry_sheet_item(entry)
    item.update(
        {
            "date": entry["date"],
            "date_label": _format_short_day(date.fromisoformat(entry["date"])),
            "section": _entry_section(entry),
            "source": entry.get("source") or "",
        }
    )
    return item


def _list_current_meeting_bookings(
    connection: sqlite3.Connection,
    *,
    target_day: date,
) -> dict:
    rows = connection.execute(
        """
        SELECT *
        FROM calendar_entries
        WHERE entry_date >= ?
          AND status IN ('planned', 'in_progress', 'missed', 'deferred')
          AND (
            source = 'frg_site_booking'
            OR title LIKE 'FRG booking:%'
            OR title LIKE '%booking:%'
            OR tags_json LIKE '%"booking"%'
            OR tags_json LIKE '%"signup"%'
            OR tags_json LIKE '%"sign-up"%'
          )
        ORDER BY
            entry_date ASC,
            CASE WHEN start_time = '' THEN 1 ELSE 0 END,
            start_time ASC,
            id ASC
        """,
        (target_day.isoformat(),),
    ).fetchall()
    items = [
        _calendar_entry_roadmap_item(entry)
        for entry in (_entry_record(row) for row in rows)
        if _is_meeting_booking_entry(entry)
    ]
    return {
        "start_date": target_day.isoformat(),
        "items": items,
        "count": len(items),
        "total_count": len(items),
        "overflow_count": 0,
    }


def _list_current_calendar_holds(
    connection: sqlite3.Connection,
    *,
    target_day: date,
    item_limit: int = 120,
) -> dict:
    rows = connection.execute(
        """
        SELECT *
        FROM calendar_entries
        WHERE entry_date >= ?
          AND status IN ('planned', 'in_progress', 'missed', 'deferred')
        ORDER BY
            entry_date ASC,
            CASE WHEN start_time = '' THEN 1 ELSE 0 END,
            start_time ASC,
            id ASC
        """,
        (target_day.isoformat(),),
    ).fetchall()
    items = [
        _calendar_entry_roadmap_item(entry)
        for entry in (_entry_record(row) for row in rows)
        if _is_calendar_hold_entry(entry)
    ]
    agenda = build_agenda(connection, start_day=target_day, days=CALENDAR_RANGE_DEFAULT_DAYS)
    for day in agenda.get("days") or []:
        for agenda_item in day.get("items") or []:
            if str(agenda_item.get("type") or "") == "event":
                items.append(_agenda_roadmap_item(day, agenda_item))
    items.sort(key=_roadmap_item_sort_key)
    limit = max(1, int(item_limit))
    printed_items = items[:limit]
    return {
        "start_date": target_day.isoformat(),
        "items": printed_items,
        "count": len(printed_items),
        "total_count": len(items),
        "overflow_count": max(0, len(items) - len(printed_items)),
    }


def _agenda_roadmap_item(day: dict, item: dict) -> dict:
    day_value = date.fromisoformat(str(day["date"]))
    item_type = str(item.get("type") or "agenda")
    priority = item.get("priority_level") or "normal"
    if item_type == "event":
        priority = "normal"
    context_bits = []
    for key in ("organization", "location", "channel", "category"):
        value = str(item.get(key) or "").strip()
        if value:
            context_bits.append(value)
    notes = str(item.get("notes") or "")
    if context_bits and notes:
        notes = f"{' / '.join(context_bits)}. {notes}"
    elif context_bits:
        notes = " / ".join(context_bits)
    return {
        "kind": item_type,
        "date": day["date"],
        "date_label": _format_short_day(day_value),
        "title": str(item.get("title") or ""),
        "priority": priority,
        "status": "scheduled",
        "time": str(item.get("time") or item.get("sort_time") or ""),
        "notes": notes,
        "tags": [],
        "list_name": "",
        "section": "Comms / Follow-Ups" if item_type == "follow_up" else "Hard Schedule",
        "source": "agenda",
    }


def _build_day_sheet_roadmap(
    connection: sqlite3.Connection,
    *,
    target_day: date,
    days: int = 30,
    item_limit: int = 120,
) -> dict:
    windows = _roadmap_windows(target_day, days)
    if not windows:
        return {"start_date": target_day.isoformat(), "end_date": target_day.isoformat(), "sections": [], "items": 0}

    start_day = date.fromisoformat(windows[0]["start_date"])
    end_day = date.fromisoformat(windows[-1]["end_date"])
    entries = _calendar_entries_for_range(
        connection,
        start_day=start_day,
        end_day=end_day,
        status="all",
    )
    items = [
        _calendar_entry_roadmap_item(entry)
        for entry in entries
        if entry["status"] in OPEN_CALENDAR_STATUSES
        and not _is_project_inventory_entry(entry)
    ]

    agenda = build_agenda(connection, start_day=start_day, days=(end_day - start_day).days + 1)
    for day in agenda.get("days") or []:
        for agenda_item in day.get("items") or []:
            if str(agenda_item.get("type") or "") not in ROADMAP_AGENDA_TYPES:
                continue
            items.append(_agenda_roadmap_item(day, agenda_item))

    windows_by_name = {window["name"]: window for window in windows}
    for item in sorted(items, key=_roadmap_item_sort_key):
        item_day = date.fromisoformat(str(item["date"]))
        for window in windows:
            start = date.fromisoformat(window["start_date"])
            end = date.fromisoformat(window["end_date"])
            if start <= item_day <= end:
                windows_by_name[window["name"]]["items"].append(item)
                break

    limited_total = 0
    for window in windows:
        window_items = window["items"]
        window["total_count"] = len(window_items)
        remaining_slots = max(0, int(item_limit) - limited_total)
        window["items"] = window_items[:remaining_slots]
        window["count"] = len(window["items"])
        window["overflow_count"] = max(0, window["total_count"] - window["count"])
        limited_total += window["count"]

    return {
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "sections": windows,
        "items": limited_total,
        "total_items": len(items),
        "overflow_count": max(0, len(items) - limited_total),
    }


def build_day_sheet(
    connection: sqlite3.Connection,
    *,
    target_day: date,
    max_open_list_items: int = 24,
    roadmap_days: int = 30,
    roadmap_item_limit: int = 120,
    focus_lookback_days: int = FOCUS_LOOKBACK_DAYS,
    focus_priority_limit: int = FOCUS_PRIORITY_LIMIT,
    include_frg_first_page: bool = True,
    featured_project_name: str | None = None,
) -> dict:
    day = build_calendar_day(connection, target_day=target_day)
    sections: dict[str, list[dict]] = {}

    for agenda_item in day.get("agenda", {}).get("items") or []:
        if str(agenda_item.get("type") or "") == "routine":
            continue
        item = _agenda_sheet_item(agenda_item)
        if item["kind"] == "follow_up":
            _add_section(sections, "Comms / Follow-Ups", item)
        else:
            _add_section(sections, "Hard Schedule", item)

    for entry in day.get("need_to_get_to") or []:
        if _is_project_inventory_entry(entry):
            continue
        section = _entry_section(entry)
        if _is_calendar_hold_entry(entry) and section not in {"Hard Schedule", "Signups / Bookings"}:
            section = "Hard Schedule"
        _add_section(sections, section, _calendar_entry_sheet_item(entry))

    for item in (day.get("open_list_items") or [])[:max_open_list_items]:
        _add_section(sections, "Open Lists", _list_sheet_item(item))

    for entry in day.get("done") or []:
        _add_section(sections, "Completed Today", _calendar_entry_sheet_item(entry))

    section_payloads = []
    for name, items in sorted(sections.items(), key=lambda section: _section_rank(section[0])):
        section_payloads.append(
            {
                "name": name,
                "items": sorted(items, key=_sheet_item_sort_key),
                "count": len(items),
            }
        )

    all_items = [item for section in section_payloads for item in section["items"]]
    roadmap = _build_day_sheet_roadmap(
        connection,
        target_day=target_day,
        days=roadmap_days,
        item_limit=roadmap_item_limit,
    )
    meeting_bookings = _list_current_meeting_bookings(
        connection,
        target_day=target_day,
    )
    calendar_holds = _list_current_calendar_holds(
        connection,
        target_day=target_day,
        item_limit=roadmap_item_limit,
    )
    focus_priorities = _build_focus_priorities(
        connection,
        target_day=target_day,
        lookback_days=focus_lookback_days,
        limit=focus_priority_limit,
    )
    agent_project_history = _build_agent_project_history(
        connection,
        target_day=target_day,
        lookback_days=focus_lookback_days,
    )
    featured_project = _build_featured_project(
        connection,
        target_day=target_day,
        lookback_days=focus_lookback_days,
        featured_project_name=featured_project_name,
    )
    frg_first_page = (
        _build_frg_first_page(connection, target_day=target_day)
        if include_frg_first_page
        else {"enabled": False, "hard_commitments": [], "mainstay_items": [], "operating_rules": []}
    )
    return {
        "date": day["date"],
        "label": day["label"],
        "day_note": day.get("day_note") or {},
        "frg_first_page": frg_first_page,
        "summary": {
            "sections": len(section_payloads),
            "items": len(all_items),
            "urgent": sum(1 for item in all_items if item.get("priority") == "urgent"),
            "high": sum(1 for item in all_items if item.get("priority") == "high"),
            "focus_priorities": focus_priorities["count"],
            "agent_projects": agent_project_history["count"],
            "featured_project": featured_project["count"],
            "calendar_holds": calendar_holds["total_count"],
            "open_list_items_included": min(len(day.get("open_list_items") or []), max_open_list_items),
            "open_list_items_total": len(day.get("open_list_items") or []),
        },
        "sections": section_payloads,
        "focus_priorities": focus_priorities,
        "agent_project_history": agent_project_history,
        "featured_project": featured_project,
        "calendar_holds": calendar_holds,
        "meeting_bookings": meeting_bookings,
        "roadmap": roadmap,
        "source_day": day,
    }


def build_calendar_day(
    connection: sqlite3.Connection,
    *,
    target_day: date,
    snapshot_limit: int = 5,
) -> dict:
    agenda = build_agenda(connection, start_day=target_day, days=1)
    agenda_day = agenda["days"][0]
    entries = _calendar_entries_for_range(
        connection,
        start_day=target_day,
        end_day=target_day,
        status="all",
    )
    entries = [{**entry, "section": _entry_section(entry)} for entry in entries]
    open_list_items = [
        _list_item_record(row)
        for row in store.list_list_items(connection, status="open", limit=500)
    ]
    completed_list_items = _completed_list_items_for_day(connection, target_day)
    snapshots = [
        _snapshot_record(row)
        for row in store.list_calendar_day_snapshots(
            connection,
            start_day=target_day,
            end_day=target_day,
            limit=snapshot_limit,
        )
    ]
    day_note = _day_note_record(store.get_calendar_day_note(connection, target_day), target_day)
    need_to_get_to = [
        entry for entry in entries
        if entry["status"] in OPEN_CALENDAR_STATUSES
    ]
    done = [
        entry for entry in entries
        if entry["status"] in DONE_CALENDAR_STATUSES
    ]

    return {
        "date": target_day.isoformat(),
        "label": _format_day(target_day),
        "day_note": day_note,
        "agenda": agenda_day,
        "entries": entries,
        "done": done,
        "not_done": need_to_get_to,
        "need_to_get_to": need_to_get_to,
        "open_list_items": open_list_items,
        "completed_list_items": completed_list_items,
        "snapshots": snapshots,
        "stats": _calendar_stats(entries, agenda_day, open_list_items, completed_list_items),
    }


def _calendar_range_agenda_item(day: dict, item: dict) -> dict:
    base = _agenda_roadmap_item(day, item)
    base["id"] = f"agenda:{day['date']}:{item.get('type') or 'item'}:{item.get('title') or ''}:{item.get('sort_time') or ''}"
    base["type"] = str(item.get("type") or "agenda")
    return base


def _range_item_sort_key(item: dict) -> tuple[str, str, int, str]:
    return (
        str(item.get("date") or ""),
        _time_rank(str(item.get("time") or item.get("start_time") or "")),
        _priority_rank(str(item.get("priority") or "")),
        str(item.get("title") or ""),
    )


def _month_key(day: date) -> str:
    return day.strftime("%Y-%m")


def _month_label(day: date) -> str:
    return day.strftime("%B %Y")


def build_calendar_range(
    connection: sqlite3.Connection,
    *,
    start_day: date,
    days: int = CALENDAR_RANGE_DEFAULT_DAYS,
    item_limit: int = 2000,
) -> dict:
    clean_days = max(1, min(int(days), CALENDAR_RANGE_MAX_DAYS))
    end_day = start_day + timedelta(days=clean_days - 1)
    entries = _calendar_entries_for_range(
        connection,
        start_day=start_day,
        end_day=end_day,
        status="all",
    )
    entry_items = [
        {
            **entry,
            "kind": "calendar_entry",
            "time": entry.get("start_time") or "",
            "section": _entry_section(entry),
        }
        for entry in entries
        if entry["status"] in OPEN_CALENDAR_STATUSES or entry["status"] in DONE_CALENDAR_STATUSES
    ]

    agenda_items: list[dict] = []
    agenda = build_agenda(connection, start_day=start_day, days=clean_days)
    for day_payload in agenda.get("days") or []:
        for agenda_item in day_payload.get("items") or []:
            if str(agenda_item.get("type") or "") not in ROADMAP_AGENDA_TYPES:
                continue
            agenda_items.append(_calendar_range_agenda_item(day_payload, agenda_item))

    all_items = sorted(entry_items + agenda_items, key=_range_item_sort_key)
    limited_items = all_items[: max(1, int(item_limit))]
    items_by_day: dict[str, list[dict]] = {}
    for item in limited_items:
        items_by_day.setdefault(str(item.get("date") or ""), []).append(item)

    today = date.today()
    day_payloads = []
    month_map: dict[str, dict] = {}
    for offset in range(clean_days):
        current_day = start_day + timedelta(days=offset)
        day_key = current_day.isoformat()
        month_key = _month_key(current_day)
        items = items_by_day.get(day_key, [])
        recurring_count = sum(1 for item in items if item.get("is_recurring"))
        signup_count = sum(1 for item in items if item.get("section") == "Signups / Bookings")
        urgent_count = sum(1 for item in items if item.get("priority") == "urgent")
        day_summary = {
            "date": day_key,
            "label": _format_short_day(current_day),
            "day_number": current_day.day,
            "weekday": current_day.strftime("%a"),
            "month": month_key,
            "is_today": current_day == today,
            "is_weekend": current_day.weekday() >= 5,
            "items": items,
            "item_count": len(items),
            "recurring_count": recurring_count,
            "signup_count": signup_count,
            "urgent_count": urgent_count,
        }
        day_payloads.append(day_summary)
        month = month_map.setdefault(
            month_key,
            {
                "key": month_key,
                "label": _month_label(current_day),
                "days": [],
                "item_count": 0,
                "recurring_count": 0,
                "signup_count": 0,
                "urgent_count": 0,
            },
        )
        month["days"].append(day_summary)
        month["item_count"] += len(items)
        month["recurring_count"] += recurring_count
        month["signup_count"] += signup_count
        month["urgent_count"] += urgent_count

    upcoming = [
        item for item in all_items
        if str(item.get("status") or "") in OPEN_CALENDAR_STATUSES or str(item.get("kind") or "") != "calendar_entry"
    ][:80]
    return {
        "start_date": start_day.isoformat(),
        "end_date": end_day.isoformat(),
        "days": day_payloads,
        "months": list(month_map.values()),
        "upcoming": upcoming,
        "summary": {
            "days": clean_days,
            "items": len(all_items),
            "printed_items": len(limited_items),
            "overflow_count": max(0, len(all_items) - len(limited_items)),
            "entries": len(entry_items),
            "agenda_items": len(agenda_items),
            "recurring_occurrences": sum(1 for item in all_items if item.get("is_recurring")),
            "signups": sum(1 for item in all_items if item.get("section") == "Signups / Bookings"),
            "urgent": sum(1 for item in all_items if item.get("priority") == "urgent"),
        },
    }


def build_calendar_history(
    connection: sqlite3.Connection,
    *,
    start_day: date,
    days: int,
    snapshot_limit: int = 1,
) -> dict:
    if days <= 0:
        raise ValueError("days must be positive")
    day_payloads = [
        build_calendar_day(
            connection,
            target_day=start_day + timedelta(days=offset),
            snapshot_limit=snapshot_limit,
        )
        for offset in range(days)
    ]
    return {
        "start_date": start_day.isoformat(),
        "end_date": (start_day + timedelta(days=days - 1)).isoformat(),
        "days": day_payloads,
        "stats": {
            "days": days,
            "tracked_entries": sum(day["stats"]["tracked_entries"] for day in day_payloads),
            "done_entries": sum(day["stats"]["done_entries"] for day in day_payloads),
            "open_entries": sum(day["stats"]["open_entries"] for day in day_payloads),
            "snapshots": sum(len(day["snapshots"]) for day in day_payloads),
        },
    }


def save_calendar_day(
    connection: sqlite3.Connection,
    *,
    target_day: date,
    title: str = "",
    summary: str = "",
) -> dict:
    payload = build_calendar_day(connection, target_day=target_day, snapshot_limit=5)
    stats = payload["stats"]
    clean_summary = str(summary or "").strip()
    if not clean_summary:
        clean_summary = (
            f"{stats['done_entries']} done, {stats['open_entries']} not done, "
            f"{stats['agenda_items']} agenda item(s), {stats['open_list_items']} open list item(s)."
        )
    snapshot_id = store.add_calendar_day_snapshot(
        connection,
        day=target_day,
        payload=payload,
        title=title or f"Daily save for {target_day.isoformat()}",
        summary=clean_summary,
    )
    return {
        "snapshot_id": snapshot_id,
        "date": target_day.isoformat(),
        "summary": clean_summary,
        "payload": payload,
    }


def rollover_calendar_day(
    connection: sqlite3.Connection,
    *,
    source_day: date,
    target_day: date,
) -> dict:
    source_rows = store.list_calendar_entries(
        connection,
        start_day=source_day,
        end_day=source_day,
        status="all",
    )
    created_ids: list[int] = []
    deferred_ids: list[int] = []
    for row in source_rows:
        entry = _entry_record(row)
        if entry["status"] not in ROLLOVER_STATUSES:
            continue
        if entry["type"] not in ROLLOVER_TYPES:
            continue
        if entry["source_table"] == "calendar_entries" and entry["source_id"]:
            continue
        store.set_calendar_entry_status(connection, entry_id=entry["id"], status="deferred")
        deferred_ids.append(entry["id"])
        created_ids.append(
            store.add_calendar_entry(
                connection,
                entry_date=target_day,
                title=entry["title"],
                entry_type="carry_forward",
                status="planned",
                priority=entry["priority"],
                list_name=entry["list_name"],
                start_time=entry["start_time"],
                end_time=entry["end_time"],
                source="calendar_rollover",
                source_table="calendar_entries",
                source_id=entry["id"],
                notes=entry["notes"],
                tags=[*entry["tags"], "carry-forward"],
            )
        )
    return {
        "source_date": source_day.isoformat(),
        "target_date": target_day.isoformat(),
        "deferred_entry_ids": deferred_ids,
        "created_entry_ids": created_ids,
        "rolled_count": len(created_ids),
    }


def render_calendar_day_text(payload: dict) -> str:
    lines = [f"Calendar day: {payload['label']}"]
    note = payload.get("day_note") or {}
    if note.get("intention") or note.get("mood") or note.get("energy"):
        bits = []
        if note.get("intention"):
            bits.append(f"intention: {note['intention']}")
        if note.get("mood"):
            bits.append(f"mood: {note['mood']}")
        if note.get("energy"):
            bits.append(f"energy: {note['energy']}")
        lines.append("- " + " | ".join(bits))
    if note.get("reflection"):
        lines.append(f"- reflection: {note['reflection']}")
    if note.get("notes"):
        lines.append(f"- notes: {note['notes']}")

    stats = payload["stats"]
    lines.append(
        f"- tracked: {stats['tracked_entries']} total, {stats['done_entries']} done, {stats['open_entries']} not done"
    )

    lines.append("")
    lines.append("Tracked")
    if payload["entries"]:
        for entry in payload["entries"]:
            time_label = entry["start_time"] or "anytime"
            lines.append(f"- [{entry['id']}] {time_label} {entry['status']} {entry['title']} ({entry['type']}, {entry['priority']})")
    else:
        lines.append("- none yet")

    lines.append("")
    lines.append("Agenda")
    agenda_items = payload.get("agenda", {}).get("items") or []
    if agenda_items:
        for item in agenda_items:
            lines.append(f"- {item.get('time') or item.get('sort_time') or 'anytime'} {item.get('type')}: {item.get('title')}")
    else:
        lines.append("- open space")

    lines.append("")
    lines.append("Need To Get To")
    needs = payload.get("need_to_get_to") or []
    if needs:
        for entry in needs:
            lines.append(f"- [{entry['id']}] {entry['title']} ({entry['status']})")
    else:
        lines.append("- none")

    if payload.get("open_list_items"):
        lines.append("")
        lines.append("Open Lists")
        for item in payload["open_list_items"][:12]:
            lines.append(f"- [{item['id']}] {item['list_name']} {item['title']}")
        if len(payload["open_list_items"]) > 12:
            lines.append(f"- ...and {len(payload['open_list_items']) - 12} more")

    if payload.get("snapshots"):
        latest = payload["snapshots"][0]
        lines.append("")
        lines.append("Latest Save")
        lines.append(f"- [{latest['id']}] {latest['snapshot_at']} {latest['summary']}")

    return "\n".join(lines)


def _format_sheet_item_text(item: dict) -> list[str]:
    priority = str(item.get("priority") or "normal")
    status = str(item.get("status") or "")
    time_label = f"{item['time']} " if item.get("time") else ""
    title = str(item.get("title") or "")
    bits = [priority]
    if status:
        bits.append(status)
    if item.get("list_name"):
        bits.append(str(item["list_name"]))
    lines = [f"- [ ] {time_label}{title} ({', '.join(bits)})"]
    notes = str(item.get("notes") or "").strip()
    if notes:
        first_line = notes.splitlines()[0]
        lines.append(f"  notes: {first_line}")
    return lines


def _format_roadmap_item_text(item: dict) -> list[str]:
    time_label = f"{item['time']} " if item.get("time") else ""
    meta_bits = [str(item.get("kind") or "item"), str(item.get("section") or "")]
    if item.get("priority") and item.get("priority") != "normal":
        meta_bits.append(str(item["priority"]))
    title = str(item.get("title") or "")
    lines = [f"- {item['date_label']} {time_label}{title} ({', '.join(bit for bit in meta_bits if bit)})"]
    notes = str(item.get("notes") or "").strip()
    if notes:
        lines.append(f"  notes: {notes.splitlines()[0]}")
    return lines


def _format_focus_priority_text(item: dict) -> list[str]:
    meta_bits = [str(item.get("priority") or "normal")]
    if item.get("rank") is not None:
        meta_bits.append(f"rank {item['rank']}")
    meta_bits.append(f"{item.get('signal_count', 0)} recent signal(s)")
    lines = [f"- {item.get('title') or ''} ({', '.join(meta_bits)})"]
    abstract = str(item.get("abstract") or "").strip()
    if abstract:
        lines.append(f"  abstract: {abstract}")
    question = str(item.get("question") or "").strip()
    if question:
        lines.append(f"  question: {question}")
    return lines


def _featured_project_item_display_title(item: dict) -> str:
    display_title = _plain_text(item.get("display_title") or "")
    if display_title:
        return display_title
    return _plain_text(item.get("title") or "")


def _featured_project_display_title(featured_project: dict) -> str:
    configured_name = _plain_text(featured_project.get("configured_name") or "")
    if configured_name:
        return configured_name
    item = featured_project.get("item") or {}
    return _featured_project_item_display_title(item)


def _featured_project_action_items(item: dict, *, display_title: str) -> list[str]:
    roadmap = item.get("roadmap") or {}
    today = _plain_text(roadmap.get("today") or "")
    next_step = _plain_text(roadmap.get("next") or "")
    then = _plain_text(roadmap.get("then") or "")
    proof = _plain_text(roadmap.get("proof") or "")
    title_key = _focus_project_key(display_title)
    is_options_stack = "option" in title_key and ("probability" in title_key or "financial" in title_key)
    is_futures_lab = "future" in title_key and ("lab" in title_key or "trading" in title_key)

    if is_options_stack:
        return [
            "Draw the path in plain English: the market data we read, the probability we calculate, the risk limit we respect, the watchlist result we store, and the final yes-or-no decision rule.",
            "Pick one real test case so the system has something concrete to answer. Write the ticker, date, expiration, strike or spread, expected price, maximum account risk, and the exact question.",
            next_step or "Run only the calculation, data pull, or scenario comparison needed for that one case before expanding to a second setup.",
            "Name the owner for each part of the stack: the data pull, probability calculation, storage, alerting, review, and playbook update.",
            then or "Move the rule into the reusable watchlist or playbook only after the result is checked and the risk boundary is written plainly.",
        ]
    if is_futures_lab:
        return [
            "Write the futures strategy menu as rules, not vibes: opening-range breakout, VWAP pullback, trend continuation, failed-break reversal, and no-trade chop filter.",
            "Pick one contract family for paper trading first, preferably the micro version, and define the session, tick value, maximum loss per trade, stop placement, and daily stop.",
            next_step or "Run the strategy through replay or paper trading and save the entry reason, exit reason, risk taken, maximum adverse move, and whether the setup matched the written rule.",
            "Separate research from execution: the lab can test ideas, but only the execution checklist can approve a simulated trade.",
            then or "Do not move to live trading until the paper log shows repeatability, the loss limits are enforced, and the kill switch has been tested.",
        ]

    actions = [
        today or f"Write the main question {display_title} needs to answer today.",
        next_step or "Run the smallest check, calculation, draft, or decision that would answer that question.",
        then or "Turn the result into one saved artifact, decision, or next scheduled hold.",
    ]
    if proof:
        actions.append(f"Make the proof visible: {proof}")
    return actions


def _featured_project_context_paragraph(item: dict, *, display_title: str) -> str:
    roadmap = item.get("roadmap") or {}
    proof = _plain_text(roadmap.get("proof") or "")
    title_key = _focus_project_key(display_title)
    is_options_stack = "option" in title_key and ("probability" in title_key or "financial" in title_key)
    is_futures_lab = "future" in title_key and ("lab" in title_key or "trading" in title_key)
    if is_options_stack:
        return (
            "The point of this project is to make the options probability hunter fit cleanly inside the larger "
            "financial stack. We are not trying to chase every possible trade. We are trying to prove one repeatable "
            "path from market data, to probability, to risk limits, to a watchlist decision."
        )
    if is_futures_lab:
        return (
            "The futures lab is the execution sandbox for short-horizon trading ideas. Its job is to separate a real "
            "strategy from excitement: define the setup, replay it, paper trade it, record the risk, and only promote "
            "what survives review."
        )
    if proof:
        return f"Use this project block to turn the current question into visible proof: {proof}"
    return f"Use this project block to make one clear decision or saved artifact for {display_title}."


def _featured_project_strategy_paragraph(item: dict, *, display_title: str) -> str:
    title_key = _focus_project_key(display_title)
    is_options_stack = "option" in title_key and ("probability" in title_key or "financial" in title_key)
    is_futures_lab = "future" in title_key and ("lab" in title_key or "trading" in title_key)
    if is_options_stack:
        return (
            "Use probability-led strategies first: defined-risk vertical spreads, cash-secured or covered structures, "
            "and watchlist alerts where the edge is written before the trade. The hunter should rank setups by inputs, "
            "probability, payout, liquidity, and account risk instead of producing a raw ticker list."
        )
    if is_futures_lab:
        return (
            "Use rules that are easy to replay: opening-range breakout, VWAP pullback, trend continuation after a clean "
            "higher-low or lower-high, and failed-break reversals. The lab should also have a no-trade rule for chop, "
            "news spikes, low liquidity, or unclear stop placement."
        )
    return ""


def _featured_project_readiness_paragraph(item: dict, *, display_title: str) -> str:
    title_key = _focus_project_key(display_title)
    is_options_stack = "option" in title_key and ("probability" in title_key or "financial" in title_key)
    is_futures_lab = "future" in title_key and ("lab" in title_key or "trading" in title_key)
    if is_options_stack:
        return (
            "Live-trading readiness is not there yet. We are close to paper-trade readiness once one complete example "
            "runs from data input to probability table to risk boundary to saved watchlist decision. Live trading should "
            "wait until the calculation is repeatable, the assumptions are logged, and the risk rule cannot be skipped."
        )
    if is_futures_lab:
        return (
            "Live-trading readiness is behind the options hunter. Treat this as simulator-only until the strategy menu, "
            "paper log, max daily loss, contract sizing, stop behavior, and kill switch are proven. The next milestone is "
            "paper trading with micros, not live size."
        )
    return ""


def _format_agent_project_history_text(item: dict) -> str:
    meta_bits = [str(item.get("priority") or "normal")]
    if item.get("rank") is not None:
        meta_bits.append(f"rank {item['rank']}")
    signal_count = int(item.get("signal_count") or 0)
    if signal_count > 1:
        meta_bits.append(f"{signal_count} signals")
    return f"- {item.get('title') or ''} ({', '.join(meta_bits)})"


def _format_featured_project_text(featured_project: dict) -> list[str]:
    items = featured_project.get("items") or ([featured_project.get("item")] if featured_project.get("item") else [])
    if items:
        lines: list[str] = []
        for item in items:
            display_title = _featured_project_item_display_title(item)
            roadmap = item.get("roadmap") or {}
            focus = _plain_text(roadmap.get("today") or item.get("abstract") or "")
            proof = _plain_text(roadmap.get("proof") or "")
            context = _featured_project_context_paragraph(item, display_title=display_title)
            strategy = _featured_project_strategy_paragraph(item, display_title=display_title)
            readiness = _featured_project_readiness_paragraph(item, display_title=display_title)
            if lines:
                lines.append("")
            lines.append(f"{display_title}")
            if focus:
                lines.append(f"Today's focus: {focus}")
            if context:
                lines.append(f"Why this matters: {context}")
            if strategy:
                lines.append(f"Strategies to use: {strategy}")
            if readiness:
                lines.append(f"How close to live trading: {readiness}")
            lines.append("Action items:")
            for index, action in enumerate(_featured_project_action_items(item, display_title=display_title), start=1):
                lines.append(f"{index}. {action}")
            if proof:
                lines.append(f"Proof to produce: {proof}")
        return lines
    item = featured_project.get("item")
    if item:
        display_title = _featured_project_display_title(featured_project)
        roadmap = item.get("roadmap") or {}
        focus = _plain_text(roadmap.get("today") or item.get("abstract") or "")
        proof = _plain_text(roadmap.get("proof") or "")
        context = _featured_project_context_paragraph(item, display_title=display_title)
        lines = [f"{display_title}"]
        if focus:
            lines.append(f"Today's focus: {focus}")
        if context:
            lines.append(f"Why this matters: {context}")
        lines.append("Action items:")
        for index, action in enumerate(_featured_project_action_items(item, display_title=display_title), start=1):
            lines.append(f"{index}. {action}")
        if proof:
            lines.append(f"Proof to produce: {proof}")
        return lines
    configured_name = str(featured_project.get("configured_name") or "").strip()
    if configured_name:
        return [f"Configured project not found: {configured_name}"]
    return ["No recent project selected; add one to the featured project config."]


def _format_frg_first_page_item_text(item: dict) -> list[str]:
    date_label = f"{item['date_label']} " if item.get("date_label") else ""
    time_label = f"{item['time']} " if item.get("time") else ""
    meta_bits = [str(item.get("priority") or "normal")]
    if item.get("status"):
        meta_bits.append(str(item["status"]))
    if item.get("list_name"):
        meta_bits.append(str(item["list_name"]))
    lines = [f"- [ ] {date_label}{time_label}{item.get('title') or ''} ({', '.join(meta_bits)})"]
    notes = str(item.get("notes") or "").strip()
    if notes:
        lines.append(f"  notes: {notes.splitlines()[0]}")
    return lines


def _render_frg_first_page_text(page: dict) -> list[str]:
    if not page.get("enabled"):
        return []
    lines = [
        f"Fractal Research Group First Page: {page.get('label') or page.get('date') or ''}",
        f"- {page.get('summary') or 'FRG first.'}",
        "",
        "FRG Hard Commitments",
    ]
    commitments = page.get("hard_commitments") or []
    if commitments:
        for item in commitments:
            lines.extend(_format_frg_first_page_item_text(item))
    else:
        lines.append("- [ ] No dated FRG hard commitments in the current window.")

    lines.append("")
    lines.append("FRG Mainstay Checklist")
    mainstay_items = page.get("mainstay_items") or []
    if mainstay_items:
        for item in mainstay_items:
            lines.extend(_format_frg_first_page_item_text(item))
    else:
        lines.append("- [ ] No open FRG mainstay entries yet. Add tasks tagged frg-mainstay.")

    rules = page.get("operating_rules") or []
    if rules:
        lines.append("")
        lines.append("FRG Operating Rules")
        for rule in rules:
            lines.append(f"- [ ] {rule.get('title') or ''}")
            if rule.get("notes"):
                lines.append(f"  notes: {rule['notes']}")
    lines.append("\f")
    return lines


def render_day_sheet_text(payload: dict, *, page_breaks: bool = False) -> str:
    summary = payload.get("summary") or {}
    lines = _render_frg_first_page_text(payload.get("frg_first_page") or {})
    lines.extend(
        [
        f"LifeOps Day Sheet: {payload['label']}",
        f"- schedule items: {summary.get('items', 0)} | calendar holds: {summary.get('calendar_holds', 0)} | featured project: {summary.get('featured_project', 0)} | urgent: {summary.get('urgent', 0)} | high: {summary.get('high', 0)}",
        ]
    )
    note = payload.get("day_note") or {}
    if note.get("intention"):
        lines.append(f"- intention: {note['intention']}")
    if note.get("mood") or note.get("energy"):
        mood_bits = []
        if note.get("mood"):
            mood_bits.append(f"mood: {note['mood']}")
        if note.get("energy"):
            mood_bits.append(f"energy: {note['energy']}")
        lines.append("- " + " | ".join(mood_bits))

    for index, section in enumerate(payload.get("sections") or []):
        if page_breaks and index > 0:
            lines.append("\f")
        lines.append("")
        lines.append(f"{section['name']} ({section['count']})")
        for item in section.get("items") or []:
            lines.extend(_format_sheet_item_text(item))
        if not section.get("items"):
            lines.append("- none")

    if summary.get("open_list_items_total", 0) > summary.get("open_list_items_included", 0):
        remaining = summary["open_list_items_total"] - summary["open_list_items_included"]
        lines.append("")
        lines.append(f"Open list overflow: {remaining} additional item(s) not printed.")

    calendar_holds = payload.get("calendar_holds") or {}
    hold_items = calendar_holds.get("items") or []
    if hold_items:
        lines.append("")
        lines.append(f"Calendar Holds / Bookings ({calendar_holds.get('total_count', len(hold_items))})")
        for item in hold_items:
            lines.extend(_format_roadmap_item_text(item))
        if calendar_holds.get("overflow_count"):
            lines.append(f"- ...and {calendar_holds['overflow_count']} more hold(s)")

    featured_project = payload.get("featured_project") or {}
    lines.append("")
    lines.append("Featured Project")
    lines.extend(_format_featured_project_text(featured_project))

    return "\n".join(lines)


def _html_attrs_for_priority(priority: str) -> str:
    clean = str(priority or "normal").lower()
    if clean not in PRIORITY_ORDER:
        clean = "normal"
    return f"priority-{clean}"


def _render_sheet_item_html(item: dict) -> str:
    priority = escape(str(item.get("priority") or "normal"))
    status = escape(str(item.get("status") or ""))
    time_label = escape(str(item.get("time") or ""))
    title = escape(str(item.get("title") or ""))
    notes = escape(str(item.get("notes") or "").strip().splitlines()[0] if item.get("notes") else "")
    meta = " / ".join(part for part in (priority, status, escape(str(item.get("list_name") or ""))) if part)
    time_html = f'<span class="time">{time_label}</span>' if time_label else ""
    notes_html = f'<div class="notes">{notes}</div>' if notes else ""
    return (
        f'<li class="{_html_attrs_for_priority(priority)}">'
        f'<span class="check"></span>{time_html}<span class="title">{title}</span>'
        f'<span class="meta">{meta}</span>{notes_html}</li>'
    )


def _render_roadmap_item_html(item: dict) -> str:
    date_label = escape(str(item.get("date_label") or ""))
    time_label = escape(str(item.get("time") or ""))
    title = escape(str(item.get("title") or ""))
    kind = escape(str(item.get("kind") or "item"))
    section = escape(str(item.get("section") or ""))
    notes = escape(str(item.get("notes") or "").strip().splitlines()[0] if item.get("notes") else "")
    time_html = f'<span class="time">{time_label}</span>' if time_label else ""
    notes_html = f'<div class="notes">{notes}</div>' if notes else ""
    return (
        f'<li class="{_html_attrs_for_priority(str(item.get("priority") or "normal"))}">'
        f'<span class="date">{date_label}</span>{time_html}<span class="title">{title}</span>'
        f'<span class="meta">{kind} / {section}</span>{notes_html}</li>'
    )


def _render_focus_priority_html(item: dict) -> str:
    title = escape(str(item.get("title") or ""))
    priority = escape(str(item.get("priority") or "normal"))
    signal_count = escape(str(item.get("signal_count") or 0))
    rank = item.get("rank")
    rank_text = f" / rank {escape(str(rank))}" if rank is not None else ""
    abstract = escape(str(item.get("abstract") or ""))
    question = escape(str(item.get("question") or ""))
    abstract_html = f'<div class="abstract">{abstract}</div>' if abstract else ""
    question_html = f'<div class="question">{question}</div>' if question else ""
    return (
        f'<li class="{_html_attrs_for_priority(priority)} focus-item">'
        f'<span class="title">{title}</span>'
        f'<span class="meta">{priority}{rank_text} / {signal_count} signal(s)</span>'
        f"{abstract_html}{question_html}</li>"
    )


def _render_featured_project_html(featured_project: dict) -> str:
    items = featured_project.get("items") or ([featured_project.get("item")] if featured_project.get("item") else [])
    configured_name = escape(str(featured_project.get("configured_name") or ""))
    if items:
        blocks: list[str] = []
        for item in items:
            display_title = _featured_project_item_display_title(item)
            roadmap = item.get("roadmap") or {}
            focus = escape(_plain_text(roadmap.get("today") or item.get("abstract") or ""))
            proof = escape(_plain_text(roadmap.get("proof") or ""))
            context = escape(_featured_project_context_paragraph(item, display_title=display_title))
            strategy = escape(_featured_project_strategy_paragraph(item, display_title=display_title))
            readiness = escape(_featured_project_readiness_paragraph(item, display_title=display_title))
            actions = "\n".join(
                f"<li>{escape(action)}</li>"
                for action in _featured_project_action_items(item, display_title=display_title)
            )
            focus_html = f'<p class="abstract"><strong>Today&apos;s focus:</strong> {focus}</p>' if focus else ""
            context_html = f'<p class="context"><strong>Why this matters:</strong> {context}</p>' if context else ""
            strategy_html = f'<p class="context"><strong>Strategies to use:</strong> {strategy}</p>' if strategy else ""
            readiness_html = f'<p class="context"><strong>How close to live trading:</strong> {readiness}</p>' if readiness else ""
            proof_html = f'<p class="question"><strong>Proof to produce:</strong> {proof}</p>' if proof else ""
            blocks.append(
                "<div class=\"featured-project-block\">"
                f"<h3>{escape(display_title)}</h3>"
                f"{focus_html}"
                f"{context_html}"
                f"{strategy_html}"
                f"{readiness_html}"
                "<p class=\"count action-heading\">What to do next</p>"
                f"<ol>{actions}</ol>"
                f"{proof_html}"
                "</div>"
            )
        body = "".join(blocks)
    elif configured_name:
        body = f'<p class="empty">Configured project not found: {configured_name}</p>'
    else:
        body = '<p class="empty">No recent project selected.</p>'
    return (
        '<section class="featured-project">'
        "<h2>Featured Project</h2>"
        f"{body}"
        "</section>"
    )


def _render_agent_project_history_html(item: dict) -> str:
    title = escape(str(item.get("title") or ""))
    priority = escape(str(item.get("priority") or "normal"))
    signal_count = int(item.get("signal_count") or 0)
    rank = item.get("rank")
    meta_bits = [priority]
    if rank is not None:
        meta_bits.append(f"rank {escape(str(rank))}")
    if signal_count > 1:
        meta_bits.append(f"{signal_count} signals")
    return (
        f'<li class="{_html_attrs_for_priority(priority)} agent-history-item">'
        f'<span class="title">{title}</span>'
        f'<span class="meta">{" / ".join(meta_bits)}</span>'
        "</li>"
    )


def _render_frg_first_page_item_html(item: dict) -> str:
    date_label = escape(str(item.get("date_label") or ""))
    time_label = escape(str(item.get("time") or ""))
    title = escape(str(item.get("title") or ""))
    notes = escape(str(item.get("notes") or "").strip().splitlines()[0] if item.get("notes") else "")
    meta = " / ".join(
        part for part in (
            escape(str(item.get("priority") or "normal")),
            escape(str(item.get("status") or "")),
            escape(str(item.get("list_name") or "")),
        )
        if part
    )
    date_html = f'<span class="date">{date_label}</span>' if date_label else ""
    time_html = f'<span class="time">{time_label}</span>' if time_label else ""
    notes_html = f'<div class="notes">{notes}</div>' if notes else ""
    return (
        f'<li class="{_html_attrs_for_priority(str(item.get("priority") or "normal"))}">'
        f'<span class="check"></span>{date_html}{time_html}<span class="title">{title}</span>'
        f'<span class="meta">{meta}</span>{notes_html}</li>'
    )


def _render_frg_first_page_html(page: dict) -> str:
    if not page.get("enabled"):
        return ""
    commitments = page.get("hard_commitments") or []
    if commitments:
        commitments_html = "\n".join(_render_frg_first_page_item_html(item) for item in commitments)
    else:
        commitments_html = '<li class="empty"><span class="check"></span>No dated FRG hard commitments in the current window.</li>'
    mainstay_items = page.get("mainstay_items") or []
    if mainstay_items:
        mainstay_html = "\n".join(_render_frg_first_page_item_html(item) for item in mainstay_items)
    else:
        mainstay_html = '<li class="empty"><span class="check"></span>No open FRG mainstay entries yet. Add tasks tagged frg-mainstay.</li>'
    rules = page.get("operating_rules") or []
    rules_html = "\n".join(
        "<li>"
        f"<span class=\"check\"></span><span class=\"title\">{escape(str(rule.get('title') or ''))}</span>"
        f"<div class=\"notes\">{escape(str(rule.get('notes') or ''))}</div>"
        "</li>"
        for rule in rules
    )
    return (
        '<article class="frg-first-page">'
        "<header>"
        "<h1>Fractal Research Group First Page</h1>"
        f"<p>{escape(str(page.get('label') or page.get('date') or ''))}</p>"
        f"<p>{escape(str(page.get('summary') or 'FRG first.'))}</p>"
        "</header>"
        "<section><h2>FRG Hard Commitments</h2><ul>"
        f"{commitments_html}</ul></section>"
        "<section><h2>FRG Mainstay Checklist</h2><ul>"
        f"{mainstay_html}</ul></section>"
        "<section><h2>FRG Operating Rules</h2><ul>"
        f"{rules_html}</ul></section>"
        "</article>"
    )


def render_day_sheet_html(payload: dict) -> str:
    summary = payload.get("summary") or {}
    note = payload.get("day_note") or {}
    frg_first_page_html = _render_frg_first_page_html(payload.get("frg_first_page") or {})
    note_bits = []
    if note.get("intention"):
        note_bits.append(f"Intention: {escape(str(note['intention']))}")
    if note.get("mood"):
        note_bits.append(f"Mood: {escape(str(note['mood']))}")
    if note.get("energy"):
        note_bits.append(f"Energy: {escape(str(note['energy']))}")
    note_html = "".join(f"<p>{bit}</p>" for bit in note_bits)
    sections_html = []
    for section in payload.get("sections") or []:
        items = "\n".join(_render_sheet_item_html(item) for item in section.get("items") or [])
        if not items:
            items = '<li class="empty">Nothing here.</li>'
        sections_html.append(
            "<section>"
            f"<h2>{escape(str(section['name']))}</h2>"
            f"<p class=\"count\">{section['count']} item(s)</p>"
            f"<ul>{items}</ul>"
            "</section>"
        )
    calendar_holds = payload.get("calendar_holds") or {}
    hold_items = calendar_holds.get("items") or []
    calendar_holds_html = ""
    if hold_items:
        items = "\n".join(_render_roadmap_item_html(item) for item in hold_items)
        overflow = (
            f"<p class=\"count\">{calendar_holds['overflow_count']} more hold(s) not printed</p>"
            if calendar_holds.get("overflow_count")
            else ""
        )
        calendar_holds_html = (
            "<section>"
            "<h2>Calendar Holds / Bookings</h2>"
            f"<p class=\"count\">{calendar_holds.get('total_count', len(hold_items))} active hold(s)</p>"
            f"<ul>{items}</ul>"
            f"{overflow}"
            "</section>"
        )
    featured_project_html = _render_featured_project_html(payload.get("featured_project") or {})
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>LifeOps Day Sheet {escape(str(payload['date']))}</title>
  <style>
    @page {{ size: letter; margin: 0.55in; }}
    body {{ color: #111; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; font-size: 12pt; line-height: 1.35; }}
    header {{ border-bottom: 2px solid #111; margin-bottom: 14px; padding-bottom: 8px; }}
    h1 {{ font-size: 23pt; margin: 0 0 4px; }}
    h2 {{ font-size: 16pt; margin: 0; }}
    h3 {{ font-size: 18pt; margin: 4px 0 6px; }}
    p {{ margin: 5px 0; }}
    .summary {{ display: flex; gap: 12px; flex-wrap: wrap; font-weight: 700; }}
    section {{ break-inside: avoid; border-top: 1px solid #999; padding-top: 8px; margin-top: 10px; }}
    .count {{ color: #444; font-size: 10pt; margin-bottom: 4px; }}
    ul {{ list-style: none; margin: 0; padding: 0; }}
    li {{ border: 1px solid #bbb; border-radius: 4px; margin: 4px 0; padding: 5px 6px; }}
    .check {{ display: inline-block; width: 10px; height: 10px; border: 1px solid #111; margin-right: 6px; vertical-align: -1px; }}
    .date {{ display: inline-block; min-width: 72px; font-weight: 700; }}
    .time {{ display: inline-block; min-width: 44px; font-weight: 700; }}
    .title {{ font-weight: 700; }}
    .meta {{ float: right; color: #333; font-size: 10pt; }}
    .notes {{ color: #444; font-size: 10pt; margin-left: 20px; margin-top: 2px; }}
    .abstract, .context {{ color: #111; font-size: 12pt; line-height: 1.35; margin-top: 6px; }}
    .question {{ color: #111; font-size: 11pt; font-weight: 700; margin-top: 8px; }}
    .featured-project {{ border-top: 2px solid #111; margin-top: 14px; padding-top: 10px; }}
    .featured-project-block {{ break-inside: avoid; margin-bottom: 16px; }}
    .featured-project .action-heading {{ color: #111; font-size: 12pt; font-weight: 700; margin-top: 10px; }}
    .featured-project ol {{ margin: 6px 0 0 1.25em; padding: 0; }}
    .featured-project ol li {{ border: 0; border-radius: 0; margin: 0 0 6px; padding: 0; line-height: 1.35; }}
    .focus-item .title {{ display: inline-block; max-width: 62%; }}
    .frg-first-page {{ break-after: page; }}
    .frg-first-page header {{ display: flex; justify-content: space-between; align-items: baseline; gap: 16px; }}
    .frg-first-page header p {{ margin: 0; }}
    .priority-urgent {{ border-width: 2px; }}
    .priority-high {{ border-left-width: 5px; }}
    .empty {{ color: #555; }}
  </style>
</head>
<body>
  {frg_first_page_html}
  <header>
    <h1>LifeOps Day Sheet</h1>
    <p>{escape(str(payload['label']))}</p>
    <div class="summary">
      <span>{summary.get('items', 0)} schedule item(s)</span>
      <span>{summary.get('calendar_holds', 0)} hold(s)</span>
      <span>{summary.get('featured_project', 0)} featured project</span>
      <span>{summary.get('urgent', 0)} urgent</span>
      <span>{summary.get('high', 0)} high</span>
    </div>
    {note_html}
  </header>
  {''.join(sections_html)}
  {calendar_holds_html}
  {featured_project_html}
</body>
</html>
"""


def _latex_item_meta(item: dict) -> str:
    bits = []
    for key in ("priority", "status", "list_name", "section"):
        value = _plain_text(item.get(key) or "")
        if value and value not in bits:
            bits.append(value)
    return " / ".join(bits)


def _latex_time_prefix(item: dict) -> str:
    value = str(item.get("time") or "").strip()
    if value:
        return f"{value} "
    return ""


def _latex_item_block(item: dict, *, include_date: bool = False) -> str:
    date_text = f"{_plain_text(item.get('date_label') or item.get('date') or '')} " if include_date else ""
    title = _latex_escape(f"[ ] {date_text}{_latex_time_prefix(item)}{item.get('title') or ''}")
    meta = _latex_escape(_latex_item_meta(item))
    raw_notes = str(item.get("notes") or "").strip().splitlines()[0] if item.get("notes") else ""
    if _plain_text(raw_notes).lower() in {
        "generated by the orp project sweep.",
        "generated by the github morning sweep.",
    }:
        raw_notes = ""
    if len(raw_notes) > 220:
        raw_notes = raw_notes[:217].rstrip() + "..."
    notes = _latex_escape(raw_notes)
    lines = [
        r"\item \begin{minipage}[t]{\linewidth}\textbf{" + title + "}"
    ]
    if meta:
        lines.append(r"\\[-1pt]{\small " + meta + "}")
    if notes:
        lines.append(r"\\[-1pt]{\small " + notes + "}")
    lines.append(r"\end{minipage}")
    return "".join(lines)


def _latex_frg_first_page_item_block(item: dict) -> str:
    date_text = f"{_plain_text(item.get('date_label') or '')} " if item.get("date_label") else ""
    time_text = f"{_plain_text(item.get('time') or '')} " if item.get("time") else ""
    title = _latex_escape(f"[ ] {date_text}{time_text}{item.get('title') or ''}")
    meta_bits = [str(item.get("priority") or "normal")]
    if item.get("status"):
        meta_bits.append(str(item["status"]))
    if item.get("list_name"):
        meta_bits.append(str(item["list_name"]))
    meta = _latex_escape(" / ".join(meta_bits))
    raw_notes = str(item.get("notes") or "").strip().splitlines()[0] if item.get("notes") else ""
    if len(raw_notes) > 180:
        raw_notes = raw_notes[:177].rstrip() + "..."
    notes = _latex_escape(raw_notes)
    lines = [r"\item \begin{minipage}[t]{\linewidth}\textbf{" + title + "}"]
    if meta:
        lines.append(r"\\[-1pt]{\scriptsize " + meta + "}")
    if notes:
        lines.append(r"\\[-1pt]{\scriptsize " + notes + "}")
    lines.append(r"\end{minipage}")
    return "".join(lines)


def _append_frg_first_page_latex(document: list[str], page: dict) -> None:
    if not page.get("enabled"):
        return
    title = _latex_escape("Fractal Research Group First Page")
    label = _latex_escape(page.get("label") or page.get("date") or "")
    summary = _latex_escape(page.get("summary") or "FRG first.")
    document.extend(
        [
            r"{\huge\bfseries " + title + r"}\hfill{\large\bfseries " + label + r"}",
            r"\vspace{2pt}",
            r"\hrule",
            r"\vspace{3pt}",
            r"{\footnotesize " + summary + r"}",
        ]
    )
    commitments = page.get("hard_commitments") or []
    document.append(r"\section*{FRG Hard Commitments}")
    document.append(r"\begin{itemize}")
    if commitments:
        document.extend(_latex_frg_first_page_item_block(item) for item in commitments)
    else:
        document.append(r"\item {\scriptsize [ ] No dated FRG hard commitments in the current window.}")
    document.append(r"\end{itemize}")
    document.append(r"\begin{multicols}{2}")
    document.append(r"\section*{FRG Mainstay Checklist}")
    document.append(r"\begin{itemize}")
    mainstay_items = page.get("mainstay_items") or []
    if mainstay_items:
        document.extend(_latex_frg_first_page_item_block(item) for item in mainstay_items)
    else:
        document.append(r"\item {\scriptsize [ ] No open FRG mainstay entries yet. Add tasks tagged frg-mainstay.}")
    document.append(r"\end{itemize}")
    rules = page.get("operating_rules") or []
    if rules:
        document.append(r"\section*{FRG Operating Rules}")
        document.append(r"\begin{itemize}")
        for rule in rules:
            document.append(
                _latex_frg_first_page_item_block(
                    {
                        "title": rule.get("title") or "",
                        "notes": rule.get("notes") or "",
                        "priority": "normal",
                        "status": "",
                        "list_name": "",
                    }
                )
            )
        document.append(r"\end{itemize}")
    document.extend([r"\end{multicols}", r"\clearpage"])


def _latex_focus_item_block(item: dict) -> str:
    title = _latex_escape(item.get("title") or "")
    meta_bits = [str(item.get("priority") or "normal")]
    if item.get("rank") is not None:
        meta_bits.append(f"rank {item['rank']}")
    meta_bits.append(f"{item.get('signal_count', 0)} signal(s)")
    abstract = _latex_escape(item.get("abstract") or "")
    question = _latex_escape(item.get("question") or "")
    lines = [r"\item \begin{minipage}[t]{\linewidth}\textbf{" + title + "}"]
    lines.append(r"\\[-1pt]{\scriptsize " + _latex_escape(" / ".join(meta_bits)) + "}")
    if abstract:
        lines.append(r"\\[-1pt]{\scriptsize Abstract: " + abstract + "}")
    if question:
        lines.append(r"\\[-1pt]{\scriptsize Question: " + question + "}")
    lines.append(r"\end{minipage}")
    return "".join(lines)


def _latex_agent_project_history_item_block(item: dict) -> str:
    title = _latex_escape(item.get("title") or "")
    meta_bits = [str(item.get("priority") or "normal")]
    if item.get("rank") is not None:
        meta_bits.append(f"rank {item['rank']}")
    signal_count = int(item.get("signal_count") or 0)
    if signal_count > 1:
        meta_bits.append(f"{signal_count} signals")
    meta = _latex_escape(" / ".join(meta_bits))
    return r"\item {\scriptsize \textbf{" + title + "} -- " + meta + "}"


def render_day_sheet_latex(payload: dict) -> str:
    summary = payload.get("summary") or {}
    note = payload.get("day_note") or {}
    title = _latex_escape("LifeOps Day Sheet")
    label = _latex_escape(payload.get("label") or payload.get("date") or "")
    stats = _latex_escape(
        f"{summary.get('items', 0)} schedule items / {summary.get('calendar_holds', 0)} calendar holds / "
        f"{summary.get('featured_project', 0)} featured project / {summary.get('urgent', 0)} urgent / {summary.get('high', 0)} high"
    )
    document: list[str] = [
        r"\documentclass[12pt,letterpaper]{article}",
        r"\usepackage[margin=0.55in]{geometry}",
        r"\usepackage[T1]{fontenc}",
        r"\usepackage[scaled]{helvet}",
        r"\renewcommand{\familydefault}{\sfdefault}",
        r"\usepackage{multicol}",
        r"\usepackage{enumitem}",
        r"\usepackage{titlesec}",
        r"\usepackage{needspace}",
        r"\usepackage{multicol}",
        r"\usepackage{microtype}",
        r"\usepackage{fancyhdr}",
        r"\pagestyle{fancy}",
        r"\fancyhf{}",
        r"\renewcommand{\headrulewidth}{0pt}",
        r"\renewcommand{\footrulewidth}{0.2pt}",
        r"\fancyfoot[L]{\scriptsize LifeOps CMAIL calendar}",
        r"\fancyfoot[R]{\scriptsize Page \thepage}",
        r"\setlength{\parindent}{0pt}",
        r"\setlength{\parskip}{4pt}",
        r"\setlist[itemize]{leftmargin=0pt,label={},itemsep=4pt,topsep=3pt,parsep=0pt,partopsep=0pt}",
        r"\setlist[enumerate]{leftmargin=*,itemsep=7pt,topsep=4pt,parsep=0pt,partopsep=0pt}",
        r"\titleformat{\section}{\Large\bfseries}{}{0pt}{}[\titlerule]",
        r"\titlespacing*{\section}{0pt}{10pt}{5pt}",
        r"\titleformat{\subsection}{\large\bfseries}{}{0pt}{}",
        r"\titlespacing*{\subsection}{0pt}{8pt}{3pt}",
        r"\begin{document}",
    ]
    _append_frg_first_page_latex(document, payload.get("frg_first_page") or {})
    document.extend(
        [
        r"{\LARGE\bfseries " + title + r"}\hfill{\large\bfseries " + label + r"}",
        r"\vspace{2pt}",
        r"\hrule",
        r"\vspace{5pt}",
        r"{\small " + stats + r"}",
        ]
    )
    note_bits = []
    for label_text, key in (("Intention", "intention"), ("Mood", "mood"), ("Energy", "energy")):
        if note.get(key):
            note_bits.append(f"{label_text}: {_plain_text(note[key])}")
    if note_bits:
        document.append(r"\\{\small " + _latex_escape(" / ".join(note_bits)) + r"}")
    document.append(r"\vspace{4pt}")

    for section in payload.get("sections") or []:
        document.append(r"\Needspace{6\baselineskip}")
        document.append(r"\section*{" + _latex_escape(f"{section['name']} ({section['count']})") + "}")
        items = section.get("items") or []
        if items:
            document.append(r"\begin{itemize}")
            document.extend(_latex_item_block(item) for item in items)
            document.append(r"\end{itemize}")
        else:
            document.append(r"{\small None.}")

    calendar_holds = payload.get("calendar_holds") or {}
    hold_items = calendar_holds.get("items") or []
    if hold_items:
        document.append(r"\Needspace{8\baselineskip}")
        document.append(
            r"\section*{"
            + _latex_escape(f"Calendar Holds / Bookings ({calendar_holds.get('total_count', len(hold_items))})")
            + "}"
        )
        document.append(r"\begin{itemize}")
        document.extend(_latex_item_block(item, include_date=True) for item in hold_items)
        document.append(r"\end{itemize}")
        if calendar_holds.get("overflow_count"):
            document.append(r"{\small " + _latex_escape(f"...and {calendar_holds['overflow_count']} more") + r"}")

    featured_project = payload.get("featured_project") or {}
    featured_items = featured_project.get("items") or ([featured_project.get("item")] if featured_project.get("item") else [])
    if featured_items or featured_project.get("configured_name"):
        document.append(r"\Needspace{8\baselineskip}")
        document.append(r"\section*{Featured Project}")
        if featured_items:
            for index, featured_item in enumerate(featured_items):
                if index > 0:
                    document.append(r"\vspace{8pt}")
                display_title = _featured_project_item_display_title(featured_item)
                roadmap = featured_item.get("roadmap") or {}
                focus = _plain_text(roadmap.get("today") or featured_item.get("abstract") or "")
                proof = _plain_text(roadmap.get("proof") or "")
                context = _featured_project_context_paragraph(featured_item, display_title=display_title)
                strategy = _featured_project_strategy_paragraph(featured_item, display_title=display_title)
                readiness = _featured_project_readiness_paragraph(featured_item, display_title=display_title)
                document.append(r"\Needspace{10\baselineskip}")
                document.append(r"{\Large\bfseries " + _latex_escape(display_title) + r"}\par")
                if focus:
                    document.append(r"\textbf{Today's focus.} " + _latex_escape(focus) + r"\par")
                if context:
                    document.append(r"\textbf{Why this matters.} " + _latex_escape(context) + r"\par")
                if strategy:
                    document.append(r"\textbf{Strategies to use.} " + _latex_escape(strategy) + r"\par")
                if readiness:
                    document.append(r"\textbf{How close to live trading.} " + _latex_escape(readiness) + r"\par")
                document.append(r"\subsection*{What To Do Next}")
                document.append(r"\begin{enumerate}")
                for action in _featured_project_action_items(featured_item, display_title=display_title):
                    document.append(r"\item " + _latex_escape(action))
                document.append(r"\end{enumerate}")
                if proof:
                    document.append(r"\textbf{Proof to produce.} " + _latex_escape(proof) + r"\par")
        else:
            document.append(r"\begin{itemize}")
            document.append(
                r"\item {Configured project not found: "
                + _latex_escape(featured_project.get("configured_name") or "")
                + "}"
            )
            document.append(r"\end{itemize}")

    document.extend([r"\end{document}", ""])
    return "\n".join(document)


def render_calendar_history_text(payload: dict) -> str:
    lines = [f"Calendar history: {payload['start_date']} to {payload['end_date']}"]
    stats = payload["stats"]
    lines.append(
        f"- totals: {stats['tracked_entries']} tracked, {stats['done_entries']} done, {stats['open_entries']} not done, {stats['snapshots']} save(s)"
    )
    for day in payload["days"]:
        day_stats = day["stats"]
        latest_snapshot = day["snapshots"][0] if day["snapshots"] else None
        suffix = f" | saved {latest_snapshot['snapshot_at']}" if latest_snapshot else ""
        lines.append("")
        lines.append(f"{day['label']}{suffix}")
        lines.append(
            f"- tracked {day_stats['tracked_entries']} | done {day_stats['done_entries']} | not done {day_stats['open_entries']} | agenda {day_stats['agenda_items']}"
        )
        if day["done"]:
            lines.append("- done: " + "; ".join(entry["title"] for entry in day["done"][:5]))
        if day["not_done"]:
            lines.append("- not done: " + "; ".join(entry["title"] for entry in day["not_done"][:5]))
        if latest_snapshot and latest_snapshot.get("summary"):
            lines.append(f"- save summary: {latest_snapshot['summary']}")
    return "\n".join(lines)
