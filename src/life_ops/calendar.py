from __future__ import annotations

import json
import sqlite3
from datetime import date, timedelta
from typing import Optional

from life_ops.agenda import build_agenda
from life_ops import store


OPEN_CALENDAR_STATUSES = {"planned", "in_progress", "missed", "deferred"}
DONE_CALENDAR_STATUSES = {"done"}
ROLLOVER_STATUSES = {"planned", "in_progress", "missed", "deferred"}
ROLLOVER_TYPES = {"task", "habit", "carry_forward", "event"}


def _json_field(value: str, fallback):
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def _format_day(day: date) -> str:
    return day.strftime("%A, %B %d, %Y").replace(" 0", " ")


def _entry_record(row: sqlite3.Row) -> dict:
    return {
        "id": int(row["id"]),
        "date": str(row["entry_date"]),
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


def build_calendar_day(
    connection: sqlite3.Connection,
    *,
    target_day: date,
    snapshot_limit: int = 5,
) -> dict:
    agenda = build_agenda(connection, start_day=target_day, days=1)
    agenda_day = agenda["days"][0]
    entries = [
        _entry_record(row)
        for row in store.list_calendar_entries(
            connection,
            start_day=target_day,
            end_day=target_day,
            status="all",
        )
    ]
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
