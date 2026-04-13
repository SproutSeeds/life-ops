from __future__ import annotations

import json
import os
import shlex
import stat
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

KEYCHAIN_SERVICE = "life-ops"
INSECURE_FILE_BACKEND_ENV = "LIFE_OPS_ALLOW_INSECURE_FILE_SECRETS"
LIFE_OPS_HOME_ENV = "LIFE_OPS_HOME"
SERVICE_SECRETS_PATH_ENV = "LIFE_OPS_SERVICE_SECRETS_PATH"
DEFAULT_SERVICE_SECRETS_FILENAME = "service-secrets.json"


def credentials_root() -> Path:
    return Path.home() / ".config" / "life-ops"


def registry_path() -> Path:
    return credentials_root() / "keys.json"


def default_service_secrets_path() -> Path:
    override = str(os.getenv(SERVICE_SECRETS_PATH_ENV) or "").strip()
    if override:
        return Path(override).expanduser()
    life_ops_home = str(os.getenv(LIFE_OPS_HOME_ENV) or "").strip()
    if life_ops_home:
        return Path(life_ops_home).expanduser() / "config" / DEFAULT_SERVICE_SECRETS_FILENAME
    return Path.home() / ".lifeops" / "config" / DEFAULT_SERVICE_SECRETS_FILENAME


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_private_file(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.parent.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    except OSError:
        pass
    if not path.exists():
        path.write_text("{}\n")
    try:
        path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass


def _load_registry(path: Optional[Path] = None) -> dict[str, dict[str, Any]]:
    target = path or registry_path()
    if not target.exists():
        return {}
    try:
        raw = json.loads(target.read_text() or "{}")
    except json.JSONDecodeError:
        return {}
    if not isinstance(raw, dict):
        return {}
    return {str(key): value for key, value in raw.items() if isinstance(value, dict)}


def _save_registry(registry: dict[str, dict[str, Any]], path: Optional[Path] = None) -> Path:
    target = path or registry_path()
    _ensure_private_file(target)
    target.write_text(json.dumps(registry, indent=2) + "\n")
    try:
        target.chmod(stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass
    return target


def _load_service_secrets(path: Optional[Path] = None) -> dict[str, str]:
    target = path or default_service_secrets_path()
    if not target.exists():
        return {}
    try:
        raw = json.loads(target.read_text() or "{}")
    except json.JSONDecodeError:
        return {}
    if not isinstance(raw, dict):
        return {}
    service_secrets: dict[str, str] = {}
    for key, value in raw.items():
        clean_key = str(key or "").strip()
        clean_value = str(value or "").strip()
        if clean_key and clean_value:
            service_secrets[clean_key] = clean_value
    return service_secrets


def _has_macos_keychain() -> bool:
    return sys.platform == "darwin"


def _run_security(args: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["security", *args],
        capture_output=True,
        text=True,
        check=False,
    )


def _store_keychain_secret(*, name: str, value: str) -> None:
    result = _run_security(
        [
            "add-generic-password",
            "-U",
            "-s",
            KEYCHAIN_SERVICE,
            "-a",
            name,
            "-w",
            value,
        ]
    )
    if result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout or "macOS Keychain write failed").strip())


def _read_keychain_secret(*, name: str) -> Optional[str]:
    result = _run_security(
        [
            "find-generic-password",
            "-s",
            KEYCHAIN_SERVICE,
            "-a",
            name,
            "-w",
        ]
    )
    if result.returncode != 0:
        return None
    return result.stdout.strip()


def _delete_keychain_secret(*, name: str) -> None:
    _run_security(
        [
            "delete-generic-password",
            "-s",
            KEYCHAIN_SERVICE,
            "-a",
            name,
        ]
    )


def _insecure_file_backend_allowed(*, explicit: bool = False) -> bool:
    if explicit:
        return True
    value = str(os.getenv(INSECURE_FILE_BACKEND_ENV) or "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def set_secret(
    *,
    name: str,
    value: str,
    backend: str = "auto",
    path: Optional[Path] = None,
    allow_insecure_file_backend: bool = False,
) -> dict[str, Any]:
    clean_name = str(name or "").strip()
    if not clean_name:
        raise ValueError("name is required")
    if not value:
        raise ValueError("value is required")

    selected_backend = backend
    if selected_backend == "auto":
        if _has_macos_keychain():
            selected_backend = "keychain"
        else:
            raise RuntimeError(
                "No secure secret backend is available by default on this platform. "
                "Use --backend file with --allow-insecure-file-backend only if you explicitly accept plaintext local secret storage."
            )
    if selected_backend not in {"keychain", "file"}:
        raise ValueError("backend must be one of: auto, keychain, file")
    if selected_backend == "file" and not _insecure_file_backend_allowed(explicit=allow_insecure_file_backend):
        raise RuntimeError(
            "Plaintext file-backed secrets are disabled by default. "
            "Pass --allow-insecure-file-backend or set LIFE_OPS_ALLOW_INSECURE_FILE_SECRETS=1 if you explicitly accept that risk."
        )

    registry = _load_registry(path)
    entry: dict[str, Any] = {
        "backend": selected_backend,
        "updated_at": _now_iso(),
    }

    if selected_backend == "keychain":
        _store_keychain_secret(name=clean_name, value=value)
        entry["value"] = ""
    else:
        entry["value"] = value

    registry[clean_name] = entry
    saved_path = _save_registry(registry, path)
    return {
        "name": clean_name,
        "backend": selected_backend,
        "registry_path": str(saved_path),
    }


def delete_secret(*, name: str, path: Optional[Path] = None) -> dict[str, Any]:
    clean_name = str(name or "").strip()
    if not clean_name:
        raise ValueError("name is required")

    registry = _load_registry(path)
    entry = registry.pop(clean_name, None)
    if entry and entry.get("backend") == "keychain":
        _delete_keychain_secret(name=clean_name)
    saved_path = _save_registry(registry, path)
    return {
        "name": clean_name,
        "deleted": entry is not None,
        "registry_path": str(saved_path),
    }


def resolve_secret(*, name: str, path: Optional[Path] = None) -> Optional[str]:
    clean_name = str(name or "").strip()
    if not clean_name:
        return None

    env_value = str(os.getenv(clean_name) or "").strip()
    if env_value:
        return env_value

    service_value = _load_service_secrets().get(clean_name)
    if service_value:
        return service_value

    registry = _load_registry(path)
    entry = registry.get(clean_name)
    if not entry:
        return None

    backend = str(entry.get("backend") or "")
    if backend == "keychain":
        return _read_keychain_secret(name=clean_name)
    if backend == "file":
        return str(entry.get("value") or "").strip() or None
    return None


def export_secret_values(
    *,
    names: Optional[list[str]] = None,
    path: Optional[Path] = None,
) -> dict[str, Any]:
    selected_names = [str(name).strip() for name in (names or []) if str(name).strip()]
    available_names = selected_names or [row["name"] for row in list_secrets(path=path)]
    values: dict[str, str] = {}
    missing: list[str] = []
    for name in available_names:
        value = resolve_secret(name=name, path=path)
        if value is None:
            missing.append(name)
            continue
        values[name] = value
    return {
        "names": list(values.keys()),
        "missing": missing,
        "values": values,
    }


def write_service_secret_snapshot(
    *,
    names: Optional[list[str]] = None,
    path: Optional[Path] = None,
    target: Optional[Path] = None,
) -> dict[str, Any]:
    export = export_secret_values(names=names, path=path)
    target_path = target or default_service_secrets_path()
    _ensure_private_file(target_path)
    target_path.write_text(json.dumps(export["values"], indent=2, sort_keys=True) + "\n")
    try:
        target_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass
    return {
        "path": str(target_path),
        "names": export["names"],
        "missing": export["missing"],
        "count": len(export["names"]),
    }


def load_registered_secrets(*, path: Optional[Path] = None, overwrite: bool = False) -> dict[str, str]:
    loaded: dict[str, str] = {}
    for name in _load_registry(path).keys():
        if not overwrite and str(os.getenv(name) or "").strip():
            loaded[name] = str(os.getenv(name) or "")
            continue
        value = resolve_secret(name=name, path=path)
        if value:
            os.environ[name] = value
            loaded[name] = value
    return loaded


def list_secrets(*, path: Optional[Path] = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    registry = _load_registry(path)
    for name in sorted(registry):
        entry = registry[name]
        env_present = bool(str(os.getenv(name) or "").strip())
        resolved_value = resolve_secret(name=name, path=path)
        rows.append(
            {
                "name": name,
                "backend": str(entry.get("backend") or ""),
                "registered": True,
                "available": bool(resolved_value),
                "env_present": env_present,
                "updated_at": str(entry.get("updated_at") or ""),
            }
        )
    return rows


def export_secrets(
    *,
    names: Optional[list[str]] = None,
    path: Optional[Path] = None,
    shell: str = "sh",
) -> dict[str, Any]:
    selected_names = [str(name).strip() for name in (names or []) if str(name).strip()]
    available_names = selected_names or [row["name"] for row in list_secrets(path=path)]
    exports: dict[str, str] = {}
    missing: list[str] = []

    for name in available_names:
        value = resolve_secret(name=name, path=path)
        if value is None:
            missing.append(name)
            continue
        exports[name] = value

    lines = [f"export {name}={shlex.quote(value)}" for name, value in exports.items()]
    return {
        "shell": shell,
        "names": list(exports.keys()),
        "missing": missing,
        "export_text": "\n".join(lines),
    }
