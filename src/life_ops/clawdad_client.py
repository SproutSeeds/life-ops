from __future__ import annotations

import json
import os
import re
import urllib.error
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse
from urllib.request import Request, urlopen


DEFAULT_CLAWDAD_BASE_URL = "http://127.0.0.1:4477"
NATIVE_CLAWDAD_HOST = "127.0.0.1"
NATIVE_CLAWDAD_PORTS = tuple(range(4487, 4518))
CLAWDAD_BASE_URL_ENV = "LIFEOPS_CLAWDAD_BASE_URL"
CLAWDAD_TOKEN_FILE_ENV = "LIFEOPS_CLAWDAD_TOKEN_FILE"
CLAWDAD_TAILSCALE_USER_ENV = "LIFEOPS_CLAWDAD_TAILSCALE_USER"


def server_config(path: Path | None = None) -> dict[str, Any]:
    config_path = path or (Path.home() / ".clawdad" / "server.json")
    try:
        if not config_path.exists():
            return {}
        parsed = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def native_token_file(configured: str | None = None) -> Path:
    configured_path = str(
        configured if configured is not None else os.environ.get(CLAWDAD_TOKEN_FILE_ENV, "")
    ).strip()
    if configured_path:
        return Path(configured_path).expanduser()
    return Path.home() / "Library" / "Application Support" / "ClawDad" / "native-server.token"


def native_token(token_file: Path | None = None) -> str:
    try:
        return (token_file or native_token_file()).read_text(encoding="utf-8").strip()
    except (OSError, UnicodeError):
        return ""


def native_base_url(
    *,
    token_file: Path | None = None,
    ports: Iterable[int] = NATIVE_CLAWDAD_PORTS,
    timeout: float = 0.35,
) -> str:
    if not native_token(token_file):
        return ""
    for port in ports:
        base_url = f"http://{NATIVE_CLAWDAD_HOST}:{int(port)}"
        try:
            request = Request(f"{base_url}/healthz", headers={"Accept": "application/json"})
            with urlopen(request, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        if (
            isinstance(payload, dict)
            and payload.get("ok") is True
            and payload.get("service") == "clawdad-server"
            and str(payload.get("authMode") or "").lower() == "token"
        ):
            return base_url
    return ""


def base_url(
    *,
    configured: str | None = None,
    token_file: Path | None = None,
    native_ports: Iterable[int] = NATIVE_CLAWDAD_PORTS,
    config: dict[str, Any] | None = None,
) -> str:
    configured_url = str(
        configured if configured is not None else os.environ.get(CLAWDAD_BASE_URL_ENV, "")
    ).strip()
    if configured_url:
        return configured_url.rstrip("/")
    detected_native_url = native_base_url(token_file=token_file, ports=native_ports)
    if detected_native_url:
        return detected_native_url
    resolved_config = config if config is not None else server_config()
    host = str(resolved_config.get("host") or "127.0.0.1").strip() or "127.0.0.1"
    port = int(resolved_config.get("port") or 4477)
    return f"http://{host}:{port}"


def tailscale_headers(
    *,
    config: dict[str, Any] | None = None,
    login: str | None = None,
) -> dict[str, str]:
    resolved_config = config if config is not None else server_config()
    allowed_users = (
        resolved_config.get("allowedUsers")
        if isinstance(resolved_config.get("allowedUsers"), list)
        else []
    )
    resolved_login = (
        str(login if login is not None else os.environ.get(CLAWDAD_TAILSCALE_USER_ENV, "")).strip()
        or next((str(user).strip() for user in allowed_users if str(user).strip()), "")
        or "codyshanemitchell@gmail.com"
    )
    return {
        "Content-Type": "application/json",
        "Tailscale-User-Login": resolved_login,
        "Tailscale-User-Name": "Cody",
        "Tailscale-App-Capabilities": "{}",
    }


def request_headers(
    target_base_url: str,
    *,
    token_file: Path | None = None,
    native_ports: Iterable[int] = NATIVE_CLAWDAD_PORTS,
    config: dict[str, Any] | None = None,
) -> dict[str, str]:
    parsed = urlparse(target_base_url)
    native_port_set = {int(port) for port in native_ports}
    if parsed.hostname in {"127.0.0.1", "localhost", "::1"} and parsed.port in native_port_set:
        token = native_token(token_file)
        if token:
            return {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
    return tailscale_headers(config=config)


def read_json(
    path: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout: float = 6.0,
    configured_base_url: str | None = None,
    token_file: Path | None = None,
    native_ports: Iterable[int] = NATIVE_CLAWDAD_PORTS,
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    resolved_base_url = base_url(
        configured=configured_base_url,
        token_file=token_file,
        native_ports=native_ports,
        config=config,
    )
    body = json.dumps(payload or {}).encode("utf-8") if payload is not None else None
    request = Request(
        f"{resolved_base_url}{path}",
        data=body,
        method=method,
        headers=request_headers(
            resolved_base_url,
            token_file=token_file,
            native_ports=native_ports,
            config=config,
        ),
    )
    with urlopen(request, timeout=timeout) as response:
        parsed = json.loads(response.read().decode("utf-8"))
    return parsed if isinstance(parsed, dict) else {"ok": False, "error": "invalid_clawdad_response"}


def http_error_payload(exc: urllib.error.HTTPError) -> dict[str, Any]:
    detail = exc.read().decode("utf-8", errors="replace").strip()
    try:
        parsed = json.loads(detail) if detail else {}
    except json.JSONDecodeError:
        parsed = {"error": detail}
    if not isinstance(parsed, dict):
        parsed = {"error": str(parsed)}
    parsed["_http_status"] = exc.code
    parsed["_raw"] = detail
    return parsed


def error_text(payload: dict[str, Any], fallback: str = "Clawdad request failed") -> str:
    for key in ("message", "error", "reason", "_raw"):
        value = str(payload.get(key) or "").strip()
        if value:
            return re.sub(r"\s+", " ", value)
    return fallback


def exception_text(exc: Exception) -> str:
    if isinstance(exc, urllib.error.HTTPError):
        return error_text(http_error_payload(exc))
    if isinstance(exc, urllib.error.URLError) and isinstance(exc.reason, ConnectionRefusedError):
        return "the local ClawDad service is unavailable; open ClawDad and try again"
    return re.sub(r"\s+", " ", str(exc).strip() or exc.__class__.__name__)


def requires_fresh_session(payload: dict[str, Any]) -> bool:
    reason = str(payload.get("reason") or "").strip().lower()
    text = error_text(payload).lower()
    return (
        reason in {"invalid_session_binding", "missing_transcript"}
        or "selected session has no provider session id" in text
        or "has no saved transcript under" in text
    )
