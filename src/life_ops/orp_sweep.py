from __future__ import annotations

import json
import re
import subprocess
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable

import sqlite3

from life_ops import store
from life_ops.calendar import build_calendar_day


DEFAULT_OUTPUT_DIR = Path.home() / ".codex" / "memories" / "lifeops-orp-sweep"
SWEEP_TAG = "orp-project-sweep"
SOURCE_TAG = "orp"
GENERATED_TAG = "generated"
PRIORITY_ORDER = {"urgent": 0, "high": 1, "normal": 2, "low": 3}
DEFAULT_PINNED_IDEA_IDS = {
    "55801968-1500-41a4-92e0-800f32818022",  # Open-source collaboration operating map
    "87518203-9633-4253-b97b-d990f9468f5c",  # Longevity / biotech project execution map
}
TOKEN_RE = re.compile(r"\b(?:eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+|[A-Za-z0-9_=-]{48,})\b")


Runner = Callable[[list[str]], subprocess.CompletedProcess[str]]


def _run(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True)


def _run_json(cmd: list[str], runner: Runner = _run) -> dict[str, Any]:
    proc = runner(cmd)
    if proc.returncode != 0:
        raise RuntimeError(
            f"command failed ({proc.returncode}): {' '.join(cmd)}\n{proc.stderr.strip()}"
        )
    return json.loads(proc.stdout) if proc.stdout.strip() else {}


def _redact(value: str) -> str:
    return TOKEN_RE.sub("[redacted-token]", value)


def _clean(value: Any, *, limit: int = 260) -> str:
    text = _redact(" ".join(str(value or "").replace("```", "").split()))
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _idea_score(idea: dict[str, Any]) -> tuple[int, datetime | None, str]:
    score = 0
    if idea.get("superStarred"):
        score += 1000
    if idea.get("_lifeops_pinned"):
        score += 800
    if idea.get("starred"):
        score += 400
    score += max(0, min(200, int(-float(idea.get("position") or 0) / 1000)))
    return (score, _parse_datetime(idea.get("updatedAt")), str(idea.get("title") or ""))


def _feature_score(feature: dict[str, Any]) -> tuple[int, datetime | None, str]:
    score = 0
    if feature.get("superStarred"):
        score += 1000
    if feature.get("starred"):
        score += 350
    title = str(feature.get("title") or "").lower()
    notes = str(feature.get("notes") or "").lower()
    detail = str(feature.get("detail") or "").lower()
    haystack = f"{title}\n{notes}\n{detail}"
    for needle, weight in (
        ("current priority", 160),
        ("next", 120),
        ("action", 100),
        ("near-term", 90),
        ("build", 70),
        ("operating", 60),
        ("mission", 40),
    ):
        if needle in haystack:
            score += weight
    score += max(0, min(100, int(-float(feature.get("position") or 0) / 10_000_000_000_000)))
    return (score, _parse_datetime(feature.get("updatedAt")), str(feature.get("title") or ""))


def _priority_for(idea: dict[str, Any], feature: dict[str, Any] | None) -> str:
    if idea.get("superStarred") or (feature and feature.get("superStarred")):
        return "urgent"
    if idea.get("starred") or idea.get("_lifeops_pinned") or (feature and feature.get("starred")):
        return "high"
    return "normal"


def _project_sort_key(project: dict[str, Any]) -> tuple[int, int, str]:
    if project.get("idea_super_starred") or project.get("feature_super_starred"):
        marker_rank = 0
    elif project.get("idea_pinned"):
        marker_rank = 1
    elif project.get("idea_starred") or project.get("feature_starred"):
        marker_rank = 2
    else:
        marker_rank = 3
    return (
        PRIORITY_ORDER.get(project["priority"], 9),
        marker_rank,
        project["idea_title"],
    )


def _action_summary(idea: dict[str, Any], feature: dict[str, Any] | None) -> str:
    if feature:
        combined = "\n".join(
            str(part or "")
            for part in (
                feature.get("notes"),
                feature.get("detail"),
            )
            if part
        )
        combined = _redact(combined)
        for pattern in (
            r"Current priority:\s*(.+?)(?:\.|\n|$)",
            r"Current active[^:]*:\s*(.+?)(?:\.|\n|$)",
            r"Next use:\s*(.+?)(?:\.|\n|$)",
            r"Next(?: action| step)?:\s*(.+?)(?:\.|\n|$)",
        ):
            match = re.search(pattern, combined, re.IGNORECASE | re.DOTALL)
            if match:
                return _clean(match.group(1), limit=220)
        for line in combined.splitlines():
            clean = line.strip(" -\t")
            if clean:
                return _clean(clean, limit=220)
    notes = str(idea.get("notes") or "")
    for line in notes.splitlines():
        clean = line.strip(" -\t")
        if clean:
            return _clean(clean, limit=220)
    return "Review the project map and choose the next concrete action."


def _feature_candidates(features: list[dict[str, Any]]) -> list[dict[str, Any]]:
    active = [
        feature
        for feature in features
        if not feature.get("completed") and not feature.get("deletedAt")
    ]
    starred = [feature for feature in active if feature.get("starred") or feature.get("superStarred")]
    return starred or active


def _select_feature(features: list[dict[str, Any]]) -> dict[str, Any] | None:
    candidates = _feature_candidates(features)
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda feature: (
            _feature_score(feature)[0],
            _feature_score(feature)[1] or datetime.min,
            _feature_score(feature)[2],
        ),
    )


def build_orp_project_sweep(
    *,
    idea_limit: int = 100,
    max_projects: int = 12,
    pinned_idea_ids: set[str] | None = None,
    runner: Runner = _run,
) -> dict[str, Any]:
    ideas_payload = _run_json(["orp", "ideas", "list", "--limit", str(idea_limit), "--json"], runner)
    ideas = ideas_payload.get("ideas") or []
    pinned = set(DEFAULT_PINNED_IDEA_IDS if pinned_idea_ids is None else pinned_idea_ids)
    tracked = []
    for idea in ideas:
        idea_id = str(idea.get("id") or "")
        is_pinned = idea_id in pinned
        if idea.get("deletedAt") or not (idea.get("starred") or idea.get("superStarred") or is_pinned):
            continue
        tracked.append({**idea, "_lifeops_pinned": is_pinned})
    tracked = sorted(
        tracked,
        key=lambda idea: (
            _idea_score(idea)[0],
            _idea_score(idea)[1] or datetime.min,
            _idea_score(idea)[2],
        ),
        reverse=True,
    )[:max_projects]

    projects: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    for idea in tracked:
        features: list[dict[str, Any]] = []
        try:
            feature_payload = _run_json(["orp", "feature", "list", str(idea["id"]), "--json"], runner)
            features = feature_payload.get("features") or []
        except Exception as exc:  # noqa: BLE001
            failures.append({"idea_id": str(idea.get("id") or ""), "error": str(exc)})
        feature = _select_feature(features)
        priority = _priority_for(idea, feature)
        action_title = str(feature.get("title") if feature else "Review project direction")
        project = {
            "idea_id": str(idea.get("id") or ""),
            "idea_title": _clean(idea.get("title"), limit=120),
            "idea_updated_at": str(idea.get("updatedAt") or ""),
            "idea_starred": bool(idea.get("starred")),
            "idea_super_starred": bool(idea.get("superStarred")),
            "idea_pinned": bool(idea.get("_lifeops_pinned")),
            "feature_id": str(feature.get("id") or "") if feature else "",
            "feature_title": _clean(action_title, limit=140),
            "feature_starred": bool(feature.get("starred")) if feature else False,
            "feature_super_starred": bool(feature.get("superStarred")) if feature else False,
            "priority": priority,
            "action_summary": _action_summary(idea, feature),
            "feature_count": len(features),
        }
        projects.append(project)

    now = datetime.now().astimezone()
    return {
        "generated_at": now.isoformat(),
        "idea_count": len(ideas),
        "tracked_project_count": len(projects),
        "projects": sorted(projects, key=_project_sort_key),
        "failures": failures,
    }


def _entry_title(project: dict[str, Any]) -> str:
    title = f"ORP: {project['idea_title']} - {project['feature_title']}"
    if len(title) <= 150:
        return title
    return title[:147].rstrip() + "..."


def _entry_notes(project: dict[str, Any], report_path: Path | None) -> str:
    lines = [
        "Generated by the ORP project sweep.",
        f"Project: {project['idea_title']}",
        f"Priority: {project['priority']}",
        f"Action: {project['feature_title']}",
        f"Next: {project['action_summary']}",
        f"ORP idea id: {project['idea_id']}",
    ]
    if project.get("feature_id"):
        lines.append(f"ORP feature id: {project['feature_id']}")
    if report_path is not None:
        lines.append(f"Report: {report_path}")
    return "\n".join(lines)


def sync_orp_sweep_calendar(
    connection: sqlite3.Connection,
    *,
    sweep: dict[str, Any],
    target_day: date,
    report_path: Path | None = None,
    calendar_limit: int = 12,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    day = build_calendar_day(connection, target_day=target_day)
    existing_titles = {
        str(entry.get("title") or "")
        for entry in day.get("entries") or []
        if SWEEP_TAG in set(entry.get("tags") or [])
    }
    results: list[dict[str, Any]] = []
    projects = list(sweep.get("projects") or [])[:calendar_limit]

    review_title = "ORP project sweep: review highest-level project actions"
    review_notes = "\n".join(
        [
            "Review the ORP project sweep and choose which project gets deliberate attention today.",
            *(f"- {project['idea_title']}: {project['feature_title']}" for project in projects[:8]),
        ]
    )
    entries = [
        {
            "title": review_title,
            "priority": "high",
            "notes": review_notes,
            "tags": [SOURCE_TAG, SWEEP_TAG, GENERATED_TAG, "lifeops"],
        },
        *[
            {
                "title": _entry_title(project),
                "priority": project["priority"],
                "notes": _entry_notes(project, report_path),
                "tags": [SOURCE_TAG, SWEEP_TAG, GENERATED_TAG, "project-priority"],
            }
            for project in projects
        ],
    ]

    created_count = 0
    for entry in entries:
        if entry["title"] in existing_titles:
            results.append({"action": "skipped-existing", "title": entry["title"]})
            continue
        if dry_run:
            results.append({"action": "dry-run", "title": entry["title"]})
            continue
        entry_id = store.add_calendar_entry(
            connection,
            entry_date=target_day,
            title=entry["title"],
            entry_type="task",
            status="planned",
            priority=entry["priority"],
            list_name="professional",
            source="orp",
            source_table="idea",
            notes=entry["notes"],
            tags=entry["tags"],
            commit=False,
        )
        created_count += 1
        results.append({"action": "created", "title": entry["title"], "id": entry_id})
    if created_count:
        connection.commit()
    return results


def render_orp_sweep_markdown(sweep: dict[str, Any], calendar_results: list[dict[str, Any]] | None = None) -> str:
    lines = [
        "# ORP Project Sweep",
        "",
        f"- Generated: `{sweep.get('generated_at')}`",
        f"- Tracked projects: `{sweep.get('tracked_project_count', 0)}`",
        "",
        "## Highest-Level Actions",
        "",
    ]
    projects = sweep.get("projects") or []
    if not projects:
        lines.append("- none")
    for index, project in enumerate(projects, start=1):
        if project.get("idea_super_starred"):
            marker = "super-starred"
        elif project.get("idea_pinned"):
            marker = "pinned"
        else:
            marker = "starred"
        lines.append(f"{index}. `{project['priority']}` {project['idea_title']} ({marker})")
        lines.append(f"   - action: {project['feature_title']}")
        lines.append(f"   - next: {project['action_summary']}")
        lines.append(f"   - idea: `{project['idea_id']}`")
        if project.get("feature_id"):
            lines.append(f"   - feature: `{project['feature_id']}`")
    if calendar_results is not None:
        lines.append("")
        lines.append("## LifeOps Calendar")
        lines.append("")
        if calendar_results:
            for result in calendar_results:
                suffix = f" (entry #{result['id']})" if result.get("id") is not None else ""
                lines.append(f"- `{result['action']}` {result['title']}{suffix}")
        else:
            lines.append("- no calendar sync requested")
    if sweep.get("failures"):
        lines.append("")
        lines.append("## Fetch Failures")
        lines.append("")
        for failure in sweep["failures"]:
            lines.append(f"- `{failure.get('idea_id')}`: {failure.get('error')}")
    lines.append("")
    return "\n".join(lines)


def write_orp_sweep_reports(
    sweep: dict[str, Any],
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    calendar_results: list[dict[str, Any]] | None = None,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    md_path = output_dir / f"orp-project-sweep-{stamp}.md"
    json_path = output_dir / f"orp-project-sweep-{stamp}.json"
    payload = {**sweep, "calendar_results": calendar_results or []}
    markdown = render_orp_sweep_markdown(sweep, calendar_results)
    md_path.write_text(markdown)
    json_path.write_text(json.dumps(payload, indent=2))
    (output_dir / "latest.md").write_text(markdown)
    (output_dir / "latest.json").write_text(json.dumps(payload, indent=2))
    return md_path, json_path
