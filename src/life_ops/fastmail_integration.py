from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional
from urllib import error, request

from life_ops import credentials
from life_ops import store

FASTMAIL_JMAP_SESSION_URL = "https://api.fastmail.com/jmap/session"
FASTMAIL_JMAP_CORE = "urn:ietf:params:jmap:core"
FASTMAIL_JMAP_MAIL = "urn:ietf:params:jmap:mail"


def default_fastmail_config_path() -> Path:
    return store.config_root() / "fastmail.json"


def fastmail_config_template() -> dict[str, Any]:
    return {
        "account_email": "",
        "session_url": FASTMAIL_JMAP_SESSION_URL,
        "api_token": "",
        "api_token_env": "FASTMAIL_API_TOKEN",
        "note": (
            "For personal/self-use, create a Fastmail API token in Settings -> "
            "Privacy & Security -> Manage API tokens. Distributed/public integrations "
            "should eventually use OAuth instead."
        ),
    }


def write_fastmail_config_template(path: Path, *, force: bool = False) -> dict[str, Any]:
    if path.exists() and not force:
        return {
            "path": str(path),
            "created": False,
            "already_exists": True,
        }

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(fastmail_config_template(), indent=2) + "\n")
    return {
        "path": str(path),
        "created": True,
        "already_exists": False,
    }


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text() or "{}")
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _strip_string(value: Any) -> str:
    return str(value or "").strip()


def _load_fastmail_config(config_path: Path) -> dict[str, Any]:
    config = _load_json(config_path)
    if not config:
        raise FileNotFoundError(
            f"Fastmail config not found at {config_path}. Run `zsh ./bin/life-ops fastmail-init-config` first."
        )

    api_token_env = _strip_string(config.get("api_token_env")) or "FASTMAIL_API_TOKEN"
    if not _strip_string(config.get("api_token")):
        config["api_token"] = (
            _strip_string(os.getenv(api_token_env))
            or _strip_string(credentials.resolve_secret(name=api_token_env) or "")
        )
    config["api_token_env"] = api_token_env
    config["session_url"] = _strip_string(config.get("session_url")) or FASTMAIL_JMAP_SESSION_URL
    config["account_email"] = _strip_string(config.get("account_email"))
    return config


def _fastmail_request_json(
    *,
    method: str,
    url: str,
    api_token: str,
    payload: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Accept": "application/json",
    }
    if payload is not None:
        headers["Content-Type"] = "application/json"

    req = request.Request(url, data=body, method=method, headers=headers)
    try:
        with request.urlopen(req, timeout=60) as response:
            raw = response.read().decode("utf-8")
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="ignore")
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"raw": raw}
        message = payload.get("detail") or payload.get("error") or payload.get("title") or raw or str(exc)
        raise RuntimeError(f"Fastmail API request failed ({exc.code}): {message}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Fastmail API request failed: {exc.reason}") from exc

    try:
        return json.loads(raw) if raw else {}
    except json.JSONDecodeError as exc:
        raise RuntimeError("Fastmail API returned non-JSON output.") from exc


def _mail_account_id(session: dict[str, Any]) -> str:
    primary_accounts = session.get("primaryAccounts") or {}
    account_id = _strip_string(primary_accounts.get(FASTMAIL_JMAP_MAIL))
    if not account_id:
        raise RuntimeError("Fastmail session does not expose a primary mail account id.")
    return account_id


def fastmail_status(*, config_path: Optional[Path] = None) -> dict[str, Any]:
    target = config_path or default_fastmail_config_path()
    config_present = target.exists()
    config = _load_json(target) if config_present else {}
    api_token_env = _strip_string(config.get("api_token_env")) or "FASTMAIL_API_TOKEN"
    api_token_present = bool(
        _strip_string(config.get("api_token"))
        or _strip_string(os.getenv(api_token_env))
        or _strip_string(credentials.resolve_secret(name=api_token_env) or "")
    )
    account_email = _strip_string(config.get("account_email"))
    session_url = _strip_string(config.get("session_url")) or FASTMAIL_JMAP_SESSION_URL

    next_steps: list[str] = []
    if not config_present:
        next_steps.append("Run `zsh ./bin/life-ops fastmail-init-config` to create a local Fastmail config.")
    if not account_email:
        next_steps.append("Set account_email in config/fastmail.json once your Fastmail mailbox exists.")
    if not api_token_present:
        next_steps.append("Register FASTMAIL_API_TOKEN in the global key registry after creating a Fastmail API token.")

    status: dict[str, Any] = {
        "config_present": config_present,
        "config_path": str(target),
        "session_url": session_url,
        "account_email": account_email or None,
        "has_account_email": bool(account_email),
        "api_token_env": api_token_env,
        "api_token_present": api_token_present,
        "ready": config_present and api_token_present,
        "next_steps": next_steps,
        "session": None,
        "error": None,
    }

    if config_present and api_token_present:
        try:
            session = fastmail_session(config_path=target)
            status["session"] = {
                "username": session.get("username"),
                "api_url": session.get("apiUrl"),
                "download_url": session.get("downloadUrl"),
                "upload_url": session.get("uploadUrl"),
                "mail_account_id": _mail_account_id(session),
                "capabilities": sorted((session.get("capabilities") or {}).keys()),
            }
        except Exception as exc:  # pragma: no cover - defensive runtime status
            status["error"] = str(exc)
            status["next_steps"] = [*status["next_steps"], "Validate your Fastmail API token and session URL."]
    return status


def fastmail_session(*, config_path: Optional[Path] = None) -> dict[str, Any]:
    config = _load_fastmail_config(config_path or default_fastmail_config_path())
    api_token = _strip_string(config.get("api_token"))
    if not api_token:
        raise RuntimeError(f"Set {config['api_token_env']} before using Fastmail commands.")
    return _fastmail_request_json(
        method="GET",
        url=str(config["session_url"]),
        api_token=api_token,
    )


def fastmail_mailboxes(*, config_path: Optional[Path] = None) -> dict[str, Any]:
    config = _load_fastmail_config(config_path or default_fastmail_config_path())
    api_token = _strip_string(config.get("api_token"))
    if not api_token:
        raise RuntimeError(f"Set {config['api_token_env']} before using Fastmail commands.")

    session = fastmail_session(config_path=config_path)
    api_url = _strip_string(session.get("apiUrl"))
    if not api_url:
        raise RuntimeError("Fastmail session did not include an apiUrl.")

    account_id = _mail_account_id(session)
    payload = {
        "using": [FASTMAIL_JMAP_CORE, FASTMAIL_JMAP_MAIL],
        "methodCalls": [
            [
                "Mailbox/get",
                {
                    "accountId": account_id,
                    "ids": None,
                    "properties": [
                        "id",
                        "name",
                        "role",
                        "sortOrder",
                        "totalEmails",
                        "unreadEmails",
                        "totalThreads",
                        "unreadThreads",
                    ],
                },
                "m1",
            ]
        ],
    }
    response = _fastmail_request_json(
        method="POST",
        url=api_url,
        api_token=api_token,
        payload=payload,
    )
    method_responses = response.get("methodResponses") or []
    if not method_responses:
        raise RuntimeError("Fastmail JMAP returned no mailbox response.")
    _, data, _ = method_responses[0]
    return {
        "account_id": account_id,
        "state": data.get("state"),
        "list": data.get("list") or [],
        "not_found": data.get("notFound") or [],
    }
