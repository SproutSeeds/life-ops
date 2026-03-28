from __future__ import annotations

import ast
import json
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4


def _json_ready(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_ready(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(item) for item in value]
    return value


def _dump_json(payload: Optional[dict[str, Any]]) -> str:
    return json.dumps(_json_ready(payload or {}), sort_keys=True)


def _load_json(payload: str) -> dict[str, Any]:
    if not payload:
        return {}
    try:
        return json.loads(payload)
    except json.JSONDecodeError:
        pass

    try:
        parsed = ast.literal_eval(payload)
    except (SyntaxError, ValueError):
        parsed = None

    if isinstance(parsed, dict):
        return {str(key): _json_ready(value) for key, value in parsed.items()}

    stripped = payload.strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        inner = stripped[1:-1].strip()
        if inner and ":" in inner and "," not in inner:
            key, value = inner.split(":", 1)
            return {
                key.strip().strip("'\""): value.strip().strip("'\""),
            }

    return {"raw_payload": payload}


def start_trace_run(
    connection: sqlite3.Connection,
    *,
    trace_type: str,
    metadata: Optional[dict[str, Any]] = None,
) -> str:
    run_id = f"{trace_type}:{uuid4().hex[:12]}"
    connection.execute(
        """
        INSERT INTO trace_runs (id, trace_type, status, metadata_json, summary_json)
        VALUES (?, ?, 'running', ?, '{}')
        """,
        (run_id, trace_type, _dump_json(metadata)),
    )
    connection.commit()
    return run_id


def append_trace_event(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    event_type: str,
    entity_key: str = "",
    payload: Optional[dict[str, Any]] = None,
) -> None:
    connection.execute(
        """
        INSERT INTO trace_events (run_id, event_type, entity_key, payload_json)
        VALUES (?, ?, ?, ?)
        """,
        (run_id, event_type, entity_key, _dump_json(payload)),
    )
    connection.commit()


def finish_trace_run(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    status: str = "completed",
    summary: Optional[dict[str, Any]] = None,
) -> None:
    connection.execute(
        """
        UPDATE trace_runs
        SET status = ?, summary_json = ?, finished_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (status, _dump_json(summary), run_id),
    )
    connection.commit()


def cancel_running_trace_runs(
    connection: sqlite3.Connection,
    *,
    trace_types: list[str],
    summary: Optional[dict[str, Any]] = None,
) -> int:
    if not trace_types:
        return 0

    placeholders = ", ".join("?" for _ in trace_types)
    cursor = connection.execute(
        f"""
        UPDATE trace_runs
        SET status = 'cancelled',
            summary_json = ?,
            finished_at = CURRENT_TIMESTAMP
        WHERE status = 'running'
          AND trace_type IN ({placeholders})
        """,
        (_dump_json(summary), *trace_types),
    )
    connection.commit()
    return int(cursor.rowcount)


def summarize_traces(
    connection: sqlite3.Connection,
    *,
    trace_type: Optional[str] = None,
    limit: int = 20,
) -> dict[str, Any]:
    counts_query = """
        SELECT trace_type, COUNT(*) AS run_count, MAX(started_at) AS last_started_at
        FROM trace_runs
        {where_clause}
        GROUP BY trace_type
        ORDER BY trace_type
    """
    recent_query = """
        SELECT id, trace_type, status, metadata_json, summary_json, started_at, finished_at
        FROM trace_runs
        {where_clause}
        ORDER BY started_at DESC
        LIMIT ?
    """

    if trace_type:
        where_clause = "WHERE trace_type = ?"
        count_rows = connection.execute(
            counts_query.format(where_clause=where_clause),
            (trace_type,),
        ).fetchall()
        recent_rows = connection.execute(
            recent_query.format(where_clause=where_clause),
            (trace_type, limit),
        ).fetchall()
    else:
        where_clause = ""
        count_rows = connection.execute(
            counts_query.format(where_clause=where_clause)
        ).fetchall()
        recent_rows = connection.execute(
            recent_query.format(where_clause=where_clause),
            (limit,),
        ).fetchall()

    counts = [
        {
            "trace_type": str(row["trace_type"]),
            "run_count": int(row["run_count"]),
            "last_started_at": str(row["last_started_at"]),
        }
        for row in count_rows
    ]
    recent_runs = [
        {
            "run_id": str(row["id"]),
            "trace_type": str(row["trace_type"]),
            "status": str(row["status"]),
            "started_at": str(row["started_at"]),
            "finished_at": str(row["finished_at"]) if row["finished_at"] else None,
            "metadata": _load_json(str(row["metadata_json"])),
            "summary": _load_json(str(row["summary_json"])),
        }
        for row in recent_rows
    ]

    return {
        "trace_type_filter": trace_type,
        "counts": counts,
        "recent_runs": recent_runs,
    }


def export_trace_records(
    connection: sqlite3.Connection,
    *,
    trace_type: Optional[str] = None,
    limit: int = 1000,
) -> list[dict[str, Any]]:
    query = """
        SELECT
            trace_runs.id AS run_id,
            trace_runs.trace_type,
            trace_runs.status AS run_status,
            trace_runs.started_at AS run_started_at,
            trace_runs.finished_at AS run_finished_at,
            trace_runs.metadata_json,
            trace_runs.summary_json,
            trace_events.id AS event_id,
            trace_events.event_type,
            trace_events.entity_key,
            trace_events.payload_json,
            trace_events.happened_at
        FROM trace_events
        INNER JOIN trace_runs ON trace_runs.id = trace_events.run_id
        {where_clause}
        ORDER BY trace_events.id DESC
        LIMIT ?
    """

    if trace_type:
        rows = connection.execute(
            query.format(where_clause="WHERE trace_runs.trace_type = ?"),
            (trace_type, limit),
        ).fetchall()
    else:
        rows = connection.execute(
            query.format(where_clause=""),
            (limit,),
        ).fetchall()

    records = []
    for row in rows:
        records.append(
            {
                "run_id": str(row["run_id"]),
                "trace_type": str(row["trace_type"]),
                "run_status": str(row["run_status"]),
                "run_started_at": str(row["run_started_at"]),
                "run_finished_at": str(row["run_finished_at"]) if row["run_finished_at"] else None,
                "metadata": _load_json(str(row["metadata_json"])),
                "summary": _load_json(str(row["summary_json"])),
                "event_id": int(row["event_id"]),
                "event_type": str(row["event_type"]),
                "entity_key": str(row["entity_key"]),
                "payload": _load_json(str(row["payload_json"])),
                "happened_at": str(row["happened_at"]),
            }
        )

    return list(reversed(records))


def render_trace_summary_text(summary: dict[str, Any]) -> str:
    lines = ["Trace summary"]
    if summary.get("trace_type_filter"):
        lines.append(f"- trace_type_filter: {summary['trace_type_filter']}")

    if summary["counts"]:
        lines.append("- counts:")
        for count in summary["counts"]:
            lines.append(
                f"  - {count['trace_type']}: {count['run_count']} runs (latest {count['last_started_at']})"
            )
    else:
        lines.append("- counts: none")

    if summary["recent_runs"]:
        lines.append("- recent_runs:")
        for run in summary["recent_runs"]:
            lines.append(
                f"  - {run['run_id']} [{run['status']}] {run['trace_type']} at {run['started_at']}"
            )
    else:
        lines.append("- recent_runs: none")

    return "\n".join(lines)


def render_trace_records_jsonl(records: list[dict[str, Any]]) -> str:
    return "\n".join(json.dumps(_json_ready(record), sort_keys=True) for record in records)
