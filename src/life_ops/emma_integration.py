from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Optional
from urllib import error, request

from life_ops import credentials

DEFAULT_EMMA_BASE_URL = "https://emma-sable.vercel.app"
DEFAULT_EMMA_AGENT = "soulbind"
DEFAULT_EMMA_MODE = "listen"


def resolve_emma_base_url(base_url: Optional[str] = None) -> str:
    value = str(base_url or os.getenv("EMMA_BASE_URL") or DEFAULT_EMMA_BASE_URL).strip()
    return value.rstrip("/")


def _emma_api_key() -> str:
    api_key = str(os.getenv("EMMA_API_KEY") or "").strip()
    if not api_key:
        api_key = str(credentials.resolve_secret(name="EMMA_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("Set EMMA_API_KEY before using Emma commands.")
    return api_key


def _emma_request(
    *,
    method: str,
    path: str,
    base_url: Optional[str] = None,
    payload: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    target = f"{resolve_emma_base_url(base_url)}{path}"
    body = None
    headers = {
        "Authorization": f"Bearer {_emma_api_key()}",
        "Accept": "application/json",
    }
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = request.Request(
        target,
        data=body,
        method=method,
        headers=headers,
    )
    try:
        with request.urlopen(req, timeout=60) as response:
            raw = response.read().decode("utf-8")
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="ignore")
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"raw": raw}
        message = payload.get("error") or payload.get("message") or raw or str(exc)
        raise RuntimeError(f"Emma API request failed ({exc.code}): {message}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Emma API request failed: {exc.reason}") from exc

    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Emma API returned non-JSON output.") from exc


def emma_status(*, base_url: Optional[str] = None) -> dict[str, Any]:
    has_key = bool(str(os.getenv("EMMA_API_KEY") or "").strip() or credentials.resolve_secret(name="EMMA_API_KEY"))
    return {
        "base_url": resolve_emma_base_url(base_url),
        "api_key_present": has_key,
        "default_agent": DEFAULT_EMMA_AGENT,
        "default_mode": DEFAULT_EMMA_MODE,
        "ready": has_key,
        "next_steps": [] if has_key else ["Register EMMA_API_KEY in the global key registry."],
    }


def emma_me(*, base_url: Optional[str] = None) -> dict[str, Any]:
    return _emma_request(
        method="GET",
        path="/api/v1/me",
        base_url=base_url,
    )


def emma_agents(*, base_url: Optional[str] = None) -> dict[str, Any]:
    return _emma_request(
        method="GET",
        path="/api/v1/agents",
        base_url=base_url,
    )


def emma_chat(
    *,
    message: str,
    agent: str = DEFAULT_EMMA_AGENT,
    mode: str = DEFAULT_EMMA_MODE,
    base_url: Optional[str] = None,
) -> dict[str, Any]:
    clean_message = str(message or "").strip()
    if not clean_message:
        raise ValueError("message is required")
    if agent not in {"emma", "soulbind"}:
        raise ValueError("agent must be one of: emma, soulbind")

    return _emma_request(
        method="POST",
        path="/api/v1/chat",
        base_url=base_url,
        payload={
            "agent": agent,
            "mode": mode,
            "messages": [
                {
                    "id": str(uuid.uuid4()),
                    "role": "user",
                    "content": clean_message,
                    "createdAt": datetime.now(timezone.utc).isoformat(),
                }
            ],
        },
    )
