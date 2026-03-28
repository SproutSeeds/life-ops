from __future__ import annotations

import base64
import hashlib
import json
import os
import secrets
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Optional
from urllib import error, parse, request

from life_ops import credentials
from life_ops import store

X_API_BASE_URL = "https://api.x.com/2"
X_AUTHORIZE_URL = "https://x.com/i/oauth2/authorize"
X_TOKEN_URL = "https://api.x.com/2/oauth2/token"
X_REQUIRED_USER_SCOPES = [
    "tweet.read",
    "users.read",
    "tweet.write",
    "offline.access",
]
X_OPTIONAL_SCOPES = [
    "bookmark.read",
    "bookmark.write",
    "follows.read",
    "like.read",
    "like.write",
]
X_DEFAULT_TWEET_FIELDS = [
    "author_id",
    "conversation_id",
    "created_at",
    "lang",
    "public_metrics",
    "referenced_tweets",
    "source",
]
X_DEFAULT_USER_FIELDS = [
    "created_at",
    "description",
    "id",
    "name",
    "profile_image_url",
    "protected",
    "public_metrics",
    "url",
    "username",
    "verified",
]


def default_x_client_path() -> Path:
    return store.repo_root() / "config" / "x_client.json"


def default_x_token_path() -> Path:
    return store.repo_root() / "data" / "x_token.json"


def x_client_template() -> dict[str, Any]:
    return {
        "client_id": "replace-me",
        "client_secret": "",
        "client_id_env": "",
        "client_secret_env": "",
        "redirect_uri": "http://127.0.0.1:8787/x/callback",
        "scopes": X_REQUIRED_USER_SCOPES,
        "bearer_token": "",
        "bearer_token_env": "",
        "note": (
            "Use bearer_token for public read-only access. "
            "Use OAuth 2.0 Authorization Code with PKCE for user-context reads and posting."
        ),
    }


def write_x_client_template(path: Path, *, force: bool = False) -> dict[str, Any]:
    if path.exists() and not force:
        return {
            "path": str(path),
            "created": False,
            "already_exists": True,
        }

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(x_client_template(), indent=2) + "\n")
    return {
        "path": str(path),
        "created": True,
        "already_exists": False,
    }


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text() or "{}")
    except json.JSONDecodeError:
        return {}
    return raw if isinstance(raw, dict) else {}


def _save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _strip_string(value: Any) -> str:
    return str(value or "").strip()


def _load_client_config(client_path: Path) -> dict[str, Any]:
    config = _load_json(client_path)
    if not config:
        raise FileNotFoundError(
            f"X client config not found at {client_path}. Run `zsh ./bin/life-ops x-init-config` first."
        )
    env_mappings = {
        "client_id": _strip_string(config.get("client_id_env")),
        "client_secret": _strip_string(config.get("client_secret_env")),
        "bearer_token": _strip_string(config.get("bearer_token_env")),
    }
    for field_name, env_name in env_mappings.items():
        if _strip_string(config.get(field_name)):
            continue
        if not env_name:
            continue
        config[field_name] = (
            str(os.getenv(env_name) or "").strip()
            or str(credentials.resolve_secret(name=env_name) or "").strip()
        )
    return config


def _load_token_config(token_path: Path) -> dict[str, Any]:
    return _load_json(token_path)


def _pkce_verifier() -> str:
    return secrets.token_urlsafe(72).rstrip("=")


def _pkce_challenge(verifier: str) -> str:
    digest = hashlib.sha256(verifier.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def build_x_authorize_url(
    *,
    client_id: str,
    redirect_uri: str,
    scopes: list[str],
    state: str,
    code_challenge: str,
) -> str:
    query = parse.urlencode(
        {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": " ".join(scopes),
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
    )
    return f"{X_AUTHORIZE_URL}?{query}"


def _basic_auth_header(client_id: str, client_secret: str) -> str:
    raw = f"{client_id}:{client_secret}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def _request_json(
    *,
    method: str,
    url: str,
    headers: Optional[dict[str, str]] = None,
    query: Optional[dict[str, Any]] = None,
    json_body: Optional[dict[str, Any]] = None,
    form_body: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    if query:
        encoded_query = parse.urlencode(
            {key: value for key, value in query.items() if value is not None},
            doseq=True,
        )
        url = f"{url}?{encoded_query}"

    body: Optional[bytes] = None
    request_headers = dict(headers or {})
    if json_body is not None:
        body = json.dumps(json_body).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")
    elif form_body is not None:
        body = parse.urlencode(
            {key: value for key, value in form_body.items() if value is not None}
        ).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/x-www-form-urlencoded")

    req = request.Request(url, data=body, headers=request_headers, method=method.upper())
    try:
        with request.urlopen(req, timeout=30) as response:
            raw = response.read().decode("utf-8")
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="ignore")
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"raw": raw}
        message = payload.get("error_description") or payload.get("detail") or payload.get("title") or raw or str(exc)
        raise RuntimeError(f"X API request failed ({exc.code}): {message}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"X API request failed: {exc.reason}") from exc

    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"X API returned non-JSON response from {url}") from exc


def _token_response(
    *,
    client_id: str,
    client_secret: str,
    form_body: dict[str, Any],
) -> dict[str, Any]:
    headers = {"Accept": "application/json"}
    if client_secret:
        headers["Authorization"] = _basic_auth_header(client_id, client_secret)
    else:
        form_body = {**form_body, "client_id": client_id}
    return _request_json(
        method="POST",
        url=X_TOKEN_URL,
        headers=headers,
        form_body=form_body,
    )


def _token_expired(token_config: dict[str, Any], *, skew_seconds: int = 60) -> bool:
    obtained_at = _strip_string(token_config.get("obtained_at"))
    expires_in = token_config.get("expires_in")
    if not obtained_at or not expires_in:
        return False
    try:
        obtained_dt = datetime.fromisoformat(obtained_at)
    except ValueError:
        return False
    if obtained_dt.tzinfo is None:
        obtained_dt = obtained_dt.replace(tzinfo=timezone.utc)
    return _now_utc() >= obtained_dt + timedelta(seconds=int(expires_in) - skew_seconds)


def _normalized_token_payload(payload: dict[str, Any], *, scopes: list[str]) -> dict[str, Any]:
    normalized = dict(payload)
    normalized["obtained_at"] = _now_utc().isoformat()
    scope_value = _strip_string(payload.get("scope"))
    normalized["scope"] = scope_value or " ".join(scopes)
    return normalized


def _open_url(url: str) -> bool:
    open_commands = [["open", url]] if sys.platform == "darwin" else [["xdg-open", url]]
    for command in open_commands:
        try:
            subprocess.run(command, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return True
        except OSError:
            continue
    return False


def _wait_for_callback(redirect_uri: str, *, timeout_seconds: int) -> dict[str, list[str]]:
    parsed = parse.urlparse(redirect_uri)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    expected_path = parsed.path or "/"
    result: dict[str, list[str]] = {}
    ready = threading.Event()

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self):  # pragma: no cover - network callback
            request_url = parse.urlparse(self.path)
            if request_url.path != expected_path:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"Not found.")
                return

            result.update(parse.parse_qs(request_url.query))
            ready.set()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(
                (
                    "<html><body><h1>X auth complete.</h1>"
                    "<p>You can close this tab and return to life-ops.</p></body></html>"
                ).encode("utf-8")
            )

        def log_message(self, format, *args):  # pragma: no cover - suppress server logging
            return

    server = HTTPServer((host, port), CallbackHandler)
    server.timeout = 0.5
    deadline = time.monotonic() + timeout_seconds
    try:
        while time.monotonic() < deadline and not ready.is_set():
            server.handle_request()
    finally:
        server.server_close()

    if not ready.is_set():
        raise TimeoutError("Timed out waiting for the X OAuth callback.")
    return result


def x_auth(
    *,
    client_path: Path,
    token_path: Path,
    timeout_seconds: int = 300,
    open_browser: bool = True,
) -> dict[str, Any]:
    client_config = _load_client_config(client_path)
    client_id = _strip_string(client_config.get("client_id"))
    client_secret = _strip_string(client_config.get("client_secret"))
    redirect_uri = _strip_string(client_config.get("redirect_uri"))
    scopes = [str(scope) for scope in client_config.get("scopes", []) if str(scope)]

    if not client_id or client_id == "replace-me":
        raise RuntimeError(f"Set the X client_id in {client_path} before running x-auth.")
    if not redirect_uri:
        raise RuntimeError(f"Set the X redirect_uri in {client_path} before running x-auth.")
    if not scopes:
        raise RuntimeError(f"Set at least one X scope in {client_path} before running x-auth.")

    verifier = _pkce_verifier()
    challenge = _pkce_challenge(verifier)
    state = secrets.token_urlsafe(24)
    authorize_url = build_x_authorize_url(
        client_id=client_id,
        redirect_uri=redirect_uri,
        scopes=scopes,
        state=state,
        code_challenge=challenge,
    )

    print(f"Open this X authorization URL if a browser does not launch automatically:\n{authorize_url}\n", flush=True)
    browser_opened = _open_url(authorize_url) if open_browser else False
    callback_params = _wait_for_callback(redirect_uri, timeout_seconds=timeout_seconds)

    if "error" in callback_params:
        error_text = callback_params.get("error_description", callback_params["error"])[0]
        raise RuntimeError(f"X authorization failed: {error_text}")

    callback_state = callback_params.get("state", [""])[0]
    if callback_state != state:
        raise RuntimeError("X authorization failed: returned state did not match the request.")

    code = callback_params.get("code", [""])[0]
    if not code:
        raise RuntimeError("X authorization failed: no authorization code was returned.")

    token_payload = _token_response(
        client_id=client_id,
        client_secret=client_secret,
        form_body={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "code_verifier": verifier,
        },
    )
    normalized_token = _normalized_token_payload(token_payload, scopes=scopes)
    _save_json(token_path, normalized_token)

    me_payload = x_get_authenticated_user(client_path=client_path, token_path=token_path)
    saved_token = _load_token_config(token_path)
    saved_token["me"] = me_payload.get("data", {})
    _save_json(token_path, saved_token)

    return {
        "client_path": str(client_path),
        "token_path": str(token_path),
        "browser_opened": browser_opened,
        "scopes": scopes,
        "me": me_payload.get("data", {}),
    }


def refresh_x_token(*, client_path: Path, token_path: Path) -> dict[str, Any]:
    client_config = _load_client_config(client_path)
    token_config = _load_token_config(token_path)
    refresh_token = _strip_string(token_config.get("refresh_token"))
    if not refresh_token:
        raise RuntimeError(f"No X refresh token is stored at {token_path}. Run x-auth again.")

    token_payload = _token_response(
        client_id=_strip_string(client_config.get("client_id")),
        client_secret=_strip_string(client_config.get("client_secret")),
        form_body={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
    )
    scopes = [str(scope) for scope in client_config.get("scopes", []) if str(scope)]
    normalized_token = _normalized_token_payload(
        {
            **token_config,
            **token_payload,
            "refresh_token": _strip_string(token_payload.get("refresh_token")) or refresh_token,
        },
        scopes=scopes,
    )
    _save_json(token_path, normalized_token)
    return normalized_token


def _access_token_for_user_context(*, client_path: Path, token_path: Path, allow_refresh: bool = True) -> str:
    token_config = _load_token_config(token_path)
    access_token = _strip_string(token_config.get("access_token"))
    if not access_token:
        raise RuntimeError(f"No X access token is stored at {token_path}. Run x-auth first.")
    if allow_refresh and _token_expired(token_config):
        refreshed = refresh_x_token(client_path=client_path, token_path=token_path)
        access_token = _strip_string(refreshed.get("access_token"))
    return access_token


def _access_token_for_read(*, client_path: Path, token_path: Path) -> tuple[str, str]:
    token_config = _load_token_config(token_path)
    access_token = _strip_string(token_config.get("access_token"))
    if access_token:
        if _token_expired(token_config):
            refreshed = refresh_x_token(client_path=client_path, token_path=token_path)
            access_token = _strip_string(refreshed.get("access_token"))
        return access_token, "user"

    client_config = _load_client_config(client_path)
    bearer_token = _strip_string(client_config.get("bearer_token"))
    if bearer_token:
        return bearer_token, "app"

    raise RuntimeError("No X user access token or bearer token is configured yet.")


def _x_api_request(
    *,
    method: str,
    path: str,
    client_path: Path,
    token_path: Path,
    query: Optional[dict[str, Any]] = None,
    json_body: Optional[dict[str, Any]] = None,
    require_user: bool = False,
) -> dict[str, Any]:
    if require_user:
        access_token = _access_token_for_user_context(client_path=client_path, token_path=token_path)
    else:
        access_token, _ = _access_token_for_read(client_path=client_path, token_path=token_path)

    return _request_json(
        method=method,
        url=f"{X_API_BASE_URL}{path}",
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
        },
        query=query,
        json_body=json_body,
    )


def x_get_authenticated_user(*, client_path: Path, token_path: Path) -> dict[str, Any]:
    return _x_api_request(
        method="GET",
        path="/users/me",
        client_path=client_path,
        token_path=token_path,
        query={"user.fields": ",".join(X_DEFAULT_USER_FIELDS)},
        require_user=False,
    )


def x_lookup_user_by_username(*, username: str, client_path: Path, token_path: Path) -> dict[str, Any]:
    clean_username = username.lstrip("@").strip()
    if not clean_username:
        raise ValueError("username is required")
    return _x_api_request(
        method="GET",
        path=f"/users/by/username/{parse.quote(clean_username, safe='')}",
        client_path=client_path,
        token_path=token_path,
        query={"user.fields": ",".join(X_DEFAULT_USER_FIELDS)},
        require_user=False,
    )


def x_get_user_posts(
    *,
    client_path: Path,
    token_path: Path,
    username: Optional[str] = None,
    max_results: int = 10,
) -> dict[str, Any]:
    if username:
        user_payload = x_lookup_user_by_username(
            username=username,
            client_path=client_path,
            token_path=token_path,
        )
        user = user_payload.get("data", {})
    else:
        me_payload = x_get_authenticated_user(client_path=client_path, token_path=token_path)
        user = me_payload.get("data", {})

    user_id = _strip_string(user.get("id"))
    if not user_id:
        raise RuntimeError("Could not determine the X user id for the requested posts.")

    timeline_payload = _x_api_request(
        method="GET",
        path=f"/users/{parse.quote(user_id, safe='')}/tweets",
        client_path=client_path,
        token_path=token_path,
        query={
            "max_results": max(5, min(int(max_results), 100)),
            "tweet.fields": ",".join(X_DEFAULT_TWEET_FIELDS),
        },
        require_user=False,
    )
    return {
        "user": user,
        "posts": timeline_payload.get("data", []),
        "meta": timeline_payload.get("meta", {}),
    }


def x_get_home_timeline(*, client_path: Path, token_path: Path, max_results: int = 10) -> dict[str, Any]:
    me_payload = x_get_authenticated_user(client_path=client_path, token_path=token_path)
    user = me_payload.get("data", {})
    user_id = _strip_string(user.get("id"))
    if not user_id:
        raise RuntimeError("Could not determine the authenticated X user id.")

    timeline_payload = _x_api_request(
        method="GET",
        path=f"/users/{parse.quote(user_id, safe='')}/timelines/reverse_chronological",
        client_path=client_path,
        token_path=token_path,
        query={
            "max_results": max(5, min(int(max_results), 100)),
            "tweet.fields": ",".join(X_DEFAULT_TWEET_FIELDS),
            "user.fields": ",".join(X_DEFAULT_USER_FIELDS),
            "expansions": "author_id",
        },
        require_user=True,
    )
    return {
        "user": user,
        "posts": timeline_payload.get("data", []),
        "includes": timeline_payload.get("includes", {}),
        "meta": timeline_payload.get("meta", {}),
    }


def x_create_post(*, client_path: Path, token_path: Path, text: str) -> dict[str, Any]:
    if not text.strip():
        raise ValueError("text is required to create a post")
    return _x_api_request(
        method="POST",
        path="/tweets",
        client_path=client_path,
        token_path=token_path,
        json_body={"text": text},
        require_user=True,
    )


def x_delete_post(*, client_path: Path, token_path: Path, post_id: str) -> dict[str, Any]:
    clean_post_id = _strip_string(post_id)
    if not clean_post_id:
        raise ValueError("post_id is required")
    return _x_api_request(
        method="DELETE",
        path=f"/tweets/{parse.quote(clean_post_id, safe='')}",
        client_path=client_path,
        token_path=token_path,
        require_user=True,
    )


def x_status(*, client_path: Path, token_path: Path) -> dict[str, Any]:
    client_config = _load_json(client_path)
    token_config = _load_json(token_path)

    scopes = [str(scope) for scope in client_config.get("scopes", []) if str(scope)]
    missing_required_scopes = [scope for scope in X_REQUIRED_USER_SCOPES if scope not in scopes]

    has_client_id = bool(_strip_string(client_config.get("client_id")) and _strip_string(client_config.get("client_id")) != "replace-me")
    has_client_secret = bool(_strip_string(client_config.get("client_secret")))
    has_bearer_token = bool(_strip_string(client_config.get("bearer_token")))
    has_access_token = bool(_strip_string(token_config.get("access_token")))
    has_refresh_token = bool(_strip_string(token_config.get("refresh_token")))

    ready_for_public_read = has_bearer_token or has_access_token
    ready_for_user_actions = has_access_token and not missing_required_scopes
    ready_for_post_write = ready_for_user_actions and "tweet.write" in scopes

    next_steps: list[str] = []
    if not client_path.exists():
        next_steps.append("create the local X config with `zsh ./bin/life-ops x-init-config`")
    if not has_client_id:
        next_steps.append("fill in the X app client_id in config/x_client.json")
    if missing_required_scopes:
        next_steps.append(f"add the required user scopes: {', '.join(missing_required_scopes)}")
    if not has_access_token:
        next_steps.append("run `zsh ./bin/life-ops x-auth` to store a user access token")
    if has_access_token and not has_refresh_token:
        next_steps.append("store a refresh_token too so long-lived account access can be renewed")
    if not has_bearer_token and not has_access_token:
        next_steps.append("add a bearer token only if you want app-only public read access too")

    return {
        "client_path": str(client_path),
        "token_path": str(token_path),
        "api_base_url": X_API_BASE_URL,
        "client_config_present": client_path.exists(),
        "token_present": token_path.exists(),
        "has_client_id": has_client_id,
        "has_client_secret": has_client_secret,
        "has_bearer_token": has_bearer_token,
        "has_access_token": has_access_token,
        "has_refresh_token": has_refresh_token,
        "token_expired": _token_expired(token_config) if has_access_token else False,
        "scopes": scopes,
        "missing_required_scopes": missing_required_scopes,
        "ready_for_public_read": ready_for_public_read,
        "ready_for_user_actions": ready_for_user_actions,
        "ready_for_post_write": ready_for_post_write,
        "me": token_config.get("me", {}),
        "next_steps": next_steps,
    }
