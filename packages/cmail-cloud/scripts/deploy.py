"""Deploy the recovered C-mail worker while preserving its existing namespace.

Run from the Life Ops root with PYTHONPATH=src and its Python environment.
Credentials remain in memory. --inspect is the default; --deploy and --schedule
are explicit production mutations. No resource provisioning or DNS changes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import time
import urllib.error
import urllib.request
import uuid

from life_ops import credentials, resend_integration

ACCOUNT = "fc1756d941215d08d5e3257df5324481"
WORKER = "life-ops-email-ingest"
NAMESPACE = "f47ff10b992d4614a5b7a4df118b3e8c"
ROOT = Path(__file__).resolve().parents[1]
SUPPORT = "support@bindmysoul.com"
BASE = f"https://api.cloudflare.com/client/v4/accounts/{ACCOUNT}/workers/scripts/{WORKER}"


def api(path="", *, method="GET", data=None, content_type=None):
    key = credentials.resolve_secret(name="CLOUDFLARE_API_TOKEN")
    if not key:
        raise RuntimeError("Cloudflare credential unavailable")
    headers = {"Authorization": "Bearer " + key}
    if content_type:
        headers["Content-Type"] = content_type
    request = urllib.request.Request(BASE + path, method=method, data=data, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace").replace(key, "[redacted]")
        raise RuntimeError(f"Cloudflare {method} {path}: HTTP {error.code}: {detail[:1500]}") from None
    if not payload.get("success"):
        raise RuntimeError("Cloudflare request failed: " + str(payload.get("errors")))
    return payload.get("result")


def encoded(value):
    return json.dumps(value, sort_keys=True, indent=2).encode()


def current():
    settings = api("/settings")
    deployment = api("/deployments")["deployments"][0]
    namespace = next(binding for binding in settings["bindings"] if binding["name"] == "MAIL_QUEUE")
    if namespace.get("namespace_id") != NAMESPACE or namespace.get("class_name") != "MailQueue":
        raise RuntimeError("Production namespace differs from the verified repair target")
    if len(deployment["versions"]) != 1 or deployment["versions"][0]["percentage"] != 100:
        raise RuntimeError("Production has a split deployment; inspect before changing it")
    return settings, deployment


def deploy(settings):
    bindings = []
    for binding in settings["bindings"]:
        name = binding["name"]
        if name == "RESEND_RECEIVING_API_KEY":
            continue
        if name == "CMAIL_MAILBOXES":
            addresses = list(dict.fromkeys([*binding.get("text", "").split(","), SUPPORT]))
            bindings.append({"type": "plain_text", "name": name, "text": ",".join(addresses)})
        else:
            bindings.append({"type": "inherit", "name": name})
    config = resend_integration._load_resend_admin_config(Path.home() / ".lifeops/config/resend-bind-my-soul.json")
    if not config.get("admin_api_key"):
        raise RuntimeError("Existing Resend receiving credential unavailable")
    bindings.append({"type": "secret_text", "name": "RESEND_RECEIVING_API_KEY", "text": config["admin_api_key"]})
    metadata = {
        "main_module": "index.mjs", "bindings": bindings,
        "compatibility_date": settings["compatibility_date"],
        "compatibility_flags": settings.get("compatibility_flags", []),
        "annotations": {"workers/message": "Restore Bind My Soul receiving, encrypted history and chronological archive"},
    }
    for field in ["usage_model", "placement", "tail_consumers", "logpush", "observability", "limits"]:
        if field in settings:
            metadata[field] = settings[field]
    boundary = "cmail-" + uuid.uuid4().hex
    parts = []
    def part(name, body, mime, filename=None):
        disposition = f'Content-Disposition: form-data; name="{name}"' + (f'; filename="{filename}"' if filename else "")
        parts.append(f"--{boundary}\r\n{disposition}\r\nContent-Type: {mime}\r\n\r\n".encode() + body + b"\r\n")
    part("metadata", encoded(metadata), "application/json")
    for source in sorted((ROOT / "src").glob("*.mjs")):
        part(source.name, source.read_bytes(), "application/javascript+module", source.name)
    body = b"".join(parts) + f"--{boundary}--\r\n".encode()
    result = api("?bindings_inherit=strict", method="PUT", data=body, content_type=f"multipart/form-data; boundary={boundary}")
    return {key: result.get(key) for key in ["id", "etag", "deployment_id", "startup_time_ms"] if key in result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--deploy", action="store_true")
    mode.add_argument("--schedule", action="store_true")
    parser.add_argument("--expected-version")
    parser.add_argument("--receipt-dir", type=Path, default=Path("data/qa/cmail-bindmysoul-2026-10-17"))
    args = parser.parse_args()
    settings, deployment = current()
    version = deployment["versions"][0]["version_id"]
    if (args.deploy or args.schedule) and version != args.expected_version:
        raise SystemExit(f"Production version changed or --expected-version omitted: {version}")
    schedules = api("/schedules")["schedules"]
    before = {"deployment": deployment, "settings": settings, "schedules": schedules}
    sources = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((ROOT / "src").glob("*.mjs"))}
    if not args.deploy and not args.schedule:
        print(json.dumps({"version": version, "schedules": schedules, "sources": sources}, indent=2))
        return
    args.receipt_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    before_path = args.receipt_dir / f"production-before-{stamp}.json"
    before_path.write_bytes(encoded(before) + b"\n")
    if args.deploy:
        result = deploy(settings)
    else:
        desired = list(dict.fromkeys([*(item["cron"] for item in schedules), "*/2 * * * *"]))
        result = api("/schedules", method="PUT", data=encoded([{"cron": cron} for cron in desired]), content_type="application/json")
    after_settings, after_deployment = current()
    for old in settings["bindings"]:
        if old["name"] in {"CMAIL_MAILBOXES", "RESEND_RECEIVING_API_KEY"}:
            continue
        actual = next((b for b in after_settings["bindings"] if b["name"] == old["name"]), None)
        if actual != old:
            raise RuntimeError("Existing binding changed: " + old["name"])
    receipt = {"mode": "deploy" if args.deploy else "schedule", "before_path": str(before_path), "source_sha256": sources, "result": result, "deployment": after_deployment, "settings": after_settings, "schedules": api("/schedules")["schedules"]}
    path = args.receipt_dir / f"production-{'deploy' if args.deploy else 'schedule'}-{stamp}.json"
    path.write_bytes(encoded(receipt) + b"\n")
    print(json.dumps({"receipt": str(path), "version": after_deployment["versions"][0]["version_id"], "schedules": receipt["schedules"], "existing_bindings_preserved": True}, indent=2))


if __name__ == "__main__":
    main()
