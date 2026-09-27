from __future__ import annotations

import json
import os
import threading
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

from life_ops import clawdad_client, store


CMAIL_AGENT_TITLE = "CMAIL Agent"
CMAIL_AGENT_PROJECT_ENV = "LIFE_OPS_CMAIL_AGENT_PROJECT"
CMAIL_AGENT_STATE_ENV = "LIFE_OPS_CMAIL_AGENT_STATE"
CMAIL_AGENT_STATE_SCHEMA = "life_ops.cmail_agent/1"
DEFAULT_CMAIL_AGENT_PROJECT = Path("/Volumes/Code_2TB/code/life-ops")

_launch_lock = threading.Lock()


class CmailAgentError(RuntimeError):
    """A user-readable CMAIL agent launch failure."""


class CmailAgentBusyError(CmailAgentError):
    """A launch is already in progress."""


class _ClawdadRequestError(CmailAgentError):
    def __init__(self, status: int, payload: dict[str, Any]) -> None:
        self.status = status
        self.payload = payload
        super().__init__(clawdad_client.error_text(payload))


def project_path() -> Path:
    configured = str(os.environ.get(CMAIL_AGENT_PROJECT_ENV) or "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    if DEFAULT_CMAIL_AGENT_PROJECT.is_dir():
        return DEFAULT_CMAIL_AGENT_PROJECT.resolve()
    return store.package_root().resolve()


def state_path() -> Path:
    configured = str(os.environ.get(CMAIL_AGENT_STATE_ENV) or "").strip()
    if configured:
        return Path(configured).expanduser()
    configured_home = str(os.environ.get(store.LIFE_OPS_HOME_ENV) or "").strip()
    life_ops_home = Path(configured_home).expanduser() if configured_home else Path.home() / ".lifeops"
    return life_ops_home / "state" / "cmail-agent.json"


def load_state(path: Path | None = None) -> dict[str, Any]:
    target = path or state_path()
    try:
        parsed = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def save_state(payload: dict[str, Any], path: Path | None = None) -> Path:
    target = path or state_path()
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    serialized = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    try:
        temporary.write_text(serialized, encoding="utf-8")
        temporary.chmod(0o600)
        temporary.replace(target)
        target.chmod(0o600)
    finally:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
    return target


def _request(
    path: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout: float = 8.0,
) -> dict[str, Any]:
    try:
        response = clawdad_client.read_json(
            path,
            method=method,
            payload=payload,
            timeout=timeout,
        )
    except urllib.error.HTTPError as exc:
        parsed = clawdad_client.http_error_payload(exc)
        raise _ClawdadRequestError(exc.code, parsed) from exc
    except Exception as exc:
        raise CmailAgentError(clawdad_client.exception_text(exc)) from exc
    if response.get("ok") is False:
        raise _ClawdadRequestError(int(response.get("_http_status") or 500), response)
    return response


def _project_from_catalog(catalog: dict[str, Any], project: Path) -> dict[str, Any] | None:
    expected = str(project)
    projects = catalog.get("projects") if isinstance(catalog.get("projects"), list) else []
    for candidate in projects:
        if isinstance(candidate, dict) and str(candidate.get("path") or "") == expected:
            return candidate
    return None


def _catalog(project: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    response = _request(f"/v1/projects?lean=1&syncProject={quote(str(project), safe='')}")
    project_details = _project_from_catalog(response, project)
    if project_details is None:
        raise CmailAgentError(f"ClawDad is not tracking the CMAIL project at {project}")
    return response, project_details


def _sessions(project_details: dict[str, Any]) -> list[dict[str, Any]]:
    sessions = project_details.get("sessions")
    if not isinstance(sessions, list):
        return []
    return [session for session in sessions if isinstance(session, dict)]


def _session_id(session: dict[str, Any] | None) -> str:
    return str((session or {}).get("sessionId") or "").strip()


def _matching_session(
    project_details: dict[str, Any],
    stored_session_id: str,
) -> dict[str, Any] | None:
    aliases = (
        project_details.get("sessionAliases")
        if isinstance(project_details.get("sessionAliases"), dict)
        else {}
    )
    resolved_stored_id = str(aliases.get(stored_session_id) or stored_session_id).strip()
    sessions = _sessions(project_details)
    if resolved_stored_id:
        match = next(
            (session for session in sessions if _session_id(session) == resolved_stored_id),
            None,
        )
        if match is not None:
            return match

    title_matches = [
        session
        for session in sessions
        if CMAIL_AGENT_TITLE
        in {
            str(session.get("title") or "").strip(),
            str(session.get("slug") or "").strip(),
        }
    ]
    if not title_matches:
        return None
    return max(
        title_matches,
        key=lambda session: (
            bool(session.get("providerSessionSeeded")),
            str(session.get("lastActivityAt") or session.get("trackedAt") or ""),
        ),
    )


def _create_session(project: Path, previous_active_session_id: str) -> str:
    created = _request(
        "/v1/sessions",
        method="POST",
        payload={
            "project": str(project),
            "provider": "codex",
            "title": CMAIL_AGENT_TITLE,
        },
    )
    session_id = str(created.get("sessionId") or "").strip()
    if not session_id:
        raise CmailAgentError("ClawDad created the CMAIL agent without a session ID")

    if previous_active_session_id and previous_active_session_id != session_id:
        try:
            _request(
                "/v1/active-session",
                method="POST",
                payload={
                    "project": str(project),
                    "sessionId": previous_active_session_id,
                },
            )
        except CmailAgentError as exc:
            raise CmailAgentError(
                "the CMAIL agent was created, but ClawDad could not restore the previously active "
                f"Life Ops session: {exc}"
            ) from exc
    return session_id


def _open_session_terminal(project: Path, session_id: str) -> dict[str, Any]:
    return _request(
        "/v1/session-terminal",
        method="POST",
        payload={"project": str(project), "sessionId": session_id},
        timeout=12.0,
    )


def _state_payload(
    *,
    project: Path,
    session_id: str,
    selection: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema": CMAIL_AGENT_STATE_SCHEMA,
        "project": str(project),
        "sessionId": session_id,
        "selection": selection,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
    }


def _safe_selection(selection: dict[str, Any] | None) -> dict[str, Any]:
    source = selection if isinstance(selection, dict) else {}
    safe: dict[str, Any] = {}
    for key in ("view", "identity"):
        value = str(source.get(key) or "").strip()
        if value:
            safe[key] = value[:80]
    for key in ("communicationId", "draftId"):
        try:
            value = int(source.get(key) or 0)
        except (TypeError, ValueError):
            value = 0
        if value > 0:
            safe[key] = value
    for key in ("threadKey", "messageId", "contactKey"):
        value = str(source.get(key) or "").strip()
        if value:
            safe[key] = value[:1000]
    return safe


def open_terminal(selection: dict[str, Any] | None = None) -> dict[str, Any]:
    """Open CMAIL's persistent Codex thread in Terminal without sending a prompt."""

    if not _launch_lock.acquire(blocking=False):
        raise CmailAgentBusyError("the CMAIL agent is already opening")
    try:
        project = project_path()
        if not project.is_dir():
            raise CmailAgentError(f"the CMAIL project directory is unavailable: {project}")

        safe_selection = _safe_selection(selection)
        current_state = load_state()
        _catalog_response, project_details = _catalog(project)
        previous_active_session_id = str(project_details.get("activeSessionId") or "").strip()
        session = _matching_session(
            project_details,
            str(current_state.get("sessionId") or "").strip(),
        )
        session_id = _session_id(session)
        if not session_id:
            session_id = _create_session(project, previous_active_session_id)

        save_state(
            _state_payload(
                project=project,
                session_id=session_id,
                selection=safe_selection,
            )
        )

        try:
            launch = _open_session_terminal(project, session_id)
        except _ClawdadRequestError as exc:
            should_replace = exc.status in {404, 409} and (
                exc.status == 404 or clawdad_client.requires_fresh_session(exc.payload)
            )
            if not should_replace:
                raise
            _catalog_response, refreshed_project = _catalog(project)
            refreshed_active_session_id = str(
                refreshed_project.get("activeSessionId") or previous_active_session_id
            ).strip()
            session_id = _create_session(project, refreshed_active_session_id)
            save_state(
                _state_payload(
                    project=project,
                    session_id=session_id,
                    selection=safe_selection,
                )
            )
            launch = _open_session_terminal(project, session_id)

        return {
            "ok": True,
            "project": str(project),
            "sessionId": session_id,
            "launchMode": str(launch.get("launchMode") or "resume"),
            "selection": safe_selection,
        }
    finally:
        _launch_lock.release()
