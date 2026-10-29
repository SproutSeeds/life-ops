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
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

from life_ops import credentials, resend_integration, mail_identities

ACCOUNT = "fc1756d941215d08d5e3257df5324481"
WORKER = "life-ops-email-ingest"
NAMESPACE = "f47ff10b992d4614a5b7a4df118b3e8c"
ROOT = Path(__file__).resolve().parents[1]
SUPPORT = "support@bindmysoul.com"
RITMO = {
    "hello@ritmo.coffee": {"from": "Ritmo Coffee <hello@ritmo.coffee>", "signature": "Ritmo Coffee\nhttps://ritmo.coffee", "key": "ritmo"},
    "cody@ritmo.coffee": {"from": "Cody Mitchell <cody@ritmo.coffee>", "signature": "Best,\n\nCody Mitchell\nRitmo Coffee\nhttps://ritmo.coffee", "key": "ritmo"},
}
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


def updated_signatures(settings, signatures):
    identities = json.loads(next(b["text"] for b in settings["bindings"] if b["name"] == "CMAIL_SEND_IDENTITIES"))
    if not isinstance(signatures, dict) or not signatures:
        raise RuntimeError("Signature configuration must name existing identities")
    for address, value in signatures.items():
        if address not in identities or not isinstance(value, dict) or set(value) != {"signature", "signature_logo"}:
            raise RuntimeError("Signature updates may change only text and logo for existing identities")
        if not isinstance(value["signature"], str) or len(value["signature"]) > 10000 or not isinstance(value["signature_logo"], dict):
            raise RuntimeError("Invalid signature configuration")
        identities[address] = {**identities[address], **value}
    return identities


def deploy(settings, *, enable_replies=False, enable_fast_reads=False, enable_sent=False, enable_compose=False, enable_drafts=False, enable_agent=False, enable_ritmo=False, signatures=None):
    if enable_ritmo and enable_replies:
        raise RuntimeError("Add Ritmo separately from rebuilding existing sending identities")
    if signatures and (enable_ritmo or enable_replies):
        raise RuntimeError("Update saved signatures separately from adding sending identities")
    bindings = []
    reply_names = {"CMAIL_RESEND_API_KEY", "CMAIL_SUPPORT_SEND_KEY", "CMAIL_SEND_IDENTITIES"}
    for binding in settings["bindings"]:
        name = binding["name"]
        if signatures and name == "CMAIL_SEND_IDENTITIES":
            continue
        if enable_ritmo and name in {"CMAIL_SEND_IDENTITIES", "CMAIL_RITMO_SEND_KEY"}:
            continue
        if enable_agent and name == "CMAIL_AGENT_ENABLED":
            continue
        if enable_drafts and name == "CMAIL_DRAFTS_ENABLED":
            continue
        if enable_compose and name == "CMAIL_COMPOSE_ENABLED":
            continue
        if enable_sent and name == "CMAIL_SENT_ENABLED":
            continue
        if enable_fast_reads and name == "CMAIL_FAST_READS":
            continue
        if name == "RESEND_RECEIVING_API_KEY" and enable_replies:
            continue
        if enable_replies and name in reply_names:
            continue
        if name == "CMAIL_MAILBOXES":
            addresses = list(dict.fromkeys([*binding.get("text", "").split(","), SUPPORT, *(RITMO if enable_ritmo else [])]))
            bindings.append({"type": "plain_text", "name": name, "text": ",".join(addresses)})
        else:
            bindings.append({"type": "inherit", "name": name})
    if enable_replies:
        config = resend_integration._load_resend_admin_config(Path.home() / ".lifeops/config/resend-bind-my-soul.json")
        if not config.get("admin_api_key"):
            raise RuntimeError("Existing Resend receiving credential unavailable")
        bindings.append({"type": "secret_text", "name": "RESEND_RECEIVING_API_KEY", "text": config["admin_api_key"]})
    if signatures:
        bindings.append({"type": "plain_text", "name": "CMAIL_SEND_IDENTITIES", "text": json.dumps(updated_signatures(settings, signatures))})
    if enable_ritmo:
        identities = json.loads(next(b["text"] for b in settings["bindings"] if b["name"] == "CMAIL_SEND_IDENTITIES"))
        for address, identity in RITMO.items():
            if address in identities and identities[address] != identity:
                raise RuntimeError("Existing Ritmo identity differs; inspect before replacing it")
            identities[address] = identity
        lookup = subprocess.run(["/usr/bin/security", "find-generic-password", "-s", "resend-ritmo", "-a", "ritmo.coffee", "-w"], capture_output=True, text=True)
        secret = lookup.stdout.strip()
        if lookup.returncode or not secret.startswith("re_") or len(secret) < 24:
            raise RuntimeError("Verified Ritmo sending key unavailable in Keychain")
        bindings.extend([
            {"type": "secret_text", "name": "CMAIL_RITMO_SEND_KEY", "text": secret},
            {"type": "plain_text", "name": "CMAIL_SEND_IDENTITIES", "text": json.dumps(identities)},
        ])
    if enable_replies:
        if any(b["name"] == "CMAIL_RITMO_SEND_KEY" for b in settings["bindings"]):
            raise RuntimeError("Preserve the existing Ritmo identities; do not rebuild replies with this legacy option")
        primary = resend_integration._load_resend_config(Path.home() / ".lifeops/config/resend.json")
        if not primary.get("api_key") or not config.get("api_key"):
            raise RuntimeError("Existing outbound credentials unavailable")
        bindings.extend([
            {"type": "secret_text", "name": "CMAIL_RESEND_API_KEY", "text": primary["api_key"]},
            {"type": "secret_text", "name": "CMAIL_SUPPORT_SEND_KEY", "text": config["api_key"]},
        ])
        addresses = next(b["text"] for b in bindings if b["name"] == "CMAIL_MAILBOXES").split(",")
        identities = {}
        for address in addresses:
            domain = address.split("@")[-1]
            if domain not in {"frg.earth", "codacli.com", "deckfound.com", "bindmysoul.com"}:
                raise RuntimeError("Unverified sending domain in mailbox configuration")
            identity = next((item for item in mail_identities.MAIL_IDENTITIES.values() if item.address == address), None)
            signature = identity.signature_text if identity else ""
            if address == SUPPORT:
                signature = config.get("default_signature_text") or signature
            elif domain == "frg.earth" and identity:
                signature = primary.get("default_signature_text") or signature
            identities[address] = {"from": identity.from_email if identity else address,
                                   "signature": signature, "key": "support" if domain == "bindmysoul.com" else "primary"}
        bindings.append({"type": "plain_text", "name": "CMAIL_SEND_IDENTITIES", "text": json.dumps(identities)})
    if enable_fast_reads:
        bindings.append({"type": "plain_text", "name": "CMAIL_FAST_READS", "text": "1"})
    if enable_agent:
        bindings.append({"type": "plain_text", "name": "CMAIL_AGENT_ENABLED", "text": "1"})
    if enable_sent:
        bindings.append({"type": "plain_text", "name": "CMAIL_SENT_ENABLED", "text": "1"})
    if enable_compose:
        bindings.append({"type": "plain_text", "name": "CMAIL_COMPOSE_ENABLED", "text": "1"})
    if enable_drafts:
        bindings.append({"type": "plain_text", "name": "CMAIL_DRAFTS_ENABLED", "text": "1"})
    metadata = {
        "main_module": "index.mjs", "bindings": bindings,
        "compatibility_date": settings["compatibility_date"],
        "compatibility_flags": settings.get("compatibility_flags", []),
        "annotations": {"workers/message": "Add authenticated C-mail replies with durable send receipts" if enable_replies else "Deploy verified C-mail cloud source"},
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
    parser.add_argument("--enable-replies", action="store_true", help="Reuse existing per-account sending credentials and identity signatures")
    parser.add_argument("--enable-fast-reads", action="store_true", help="Activate verified mailbox and conversation indexes after backfill")
    parser.add_argument("--enable-sent", action="store_true", help="Enable Sent after its conversation index is complete")
    parser.add_argument("--enable-compose", action="store_true", help="Enable Compose after the contact history index is complete")
    parser.add_argument("--enable-drafts", action="store_true", help="Enable encrypted saved drafts with existing Compose and Sent")
    parser.add_argument("--enable-agent", action="store_true", help="Enable signed agent access to the same cloud mailbox and drafts")
    parser.add_argument("--enable-ritmo", action="store_true", help="Add the two Ritmo identities using their domain-scoped Keychain sending key, preserving all existing credentials")
    parser.add_argument("--signature-config", type=Path, help="Private JSON of signature text and logo updates keyed by existing sending address")
    parser.add_argument("--receipt-dir", type=Path, default=Path("data/qa/cmail-bindmysoul-2026-10-17"))
    args = parser.parse_args()
    if args.deploy:
        # A cloud-only feature release is incomplete: check both interfaces
        # before credentials, production inspection or any deployment mutation.
        subprocess.run([sys.executable, str(ROOT / "scripts/check-parity.py")], check=True)
    signatures = json.loads(args.signature_config.read_text()) if args.signature_config else None
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
        if args.enable_drafts:
            for name in ["CMAIL_FAST_READS", "CMAIL_SENT_ENABLED", "CMAIL_COMPOSE_ENABLED"]:
                if next((b.get("text") for b in settings["bindings"] if b["name"] == name), None) != "1":
                    raise RuntimeError("Drafts requires indexed reads, Sent, and Compose")
        if args.enable_compose:
            from life_ops import cloudflare_email
            secret = credentials.resolve_secret(name="LIFE_OPS_MAIL_INGEST_SECRET")
            readiness = cloudflare_email._cloudflare_worker_request_json(worker_public_url="https://life-ops-email-ingest.frg-life-ops.workers.dev", secret=secret, path="/api/mail/contacts/status", payload={})
            if not readiness.get("ready"):
                raise RuntimeError("Contact index must be complete before activation")
            for name in ["CMAIL_FAST_READS", "CMAIL_SENT_ENABLED"]:
                if next((b.get("text") for b in settings["bindings"] if b["name"] == name), None) != "1":
                    raise RuntimeError("Compose requires indexed reads and Sent")
        if args.enable_fast_reads or args.enable_sent:
            from life_ops import cloudflare_email
            secret = credentials.resolve_secret(name="LIFE_OPS_MAIL_INGEST_SECRET")
            readiness = cloudflare_email._cloudflare_worker_request_json(worker_public_url="https://life-ops-email-ingest.frg-life-ops.workers.dev", secret=secret, path="/api/mail/performance/status", payload={})
            if not readiness.get("ready"):
                raise RuntimeError("Archive indexes must be complete before activation")
            if args.enable_sent:
                readiness = cloudflare_email._cloudflare_worker_request_json(worker_public_url="https://life-ops-email-ingest.frg-life-ops.workers.dev", secret=secret, path="/api/mail/sent/status", payload={})
                if not readiness.get("ready"):
                    raise RuntimeError("Sent index must be complete before activation")
                if not args.enable_fast_reads and next((b.get("text") for b in settings["bindings"] if b["name"] == "CMAIL_FAST_READS"), None) != "1":
                    raise RuntimeError("Sent requires indexed reads")
        if current()[1]["versions"][0]["version_id"] != version:
            raise RuntimeError("Production changed during preflight; inspect before deploying")
        result = deploy(settings, enable_replies=args.enable_replies, enable_fast_reads=args.enable_fast_reads, enable_sent=args.enable_sent, enable_compose=args.enable_compose, enable_drafts=args.enable_drafts, enable_agent=args.enable_agent, enable_ritmo=args.enable_ritmo, signatures=signatures)
    else:
        desired = list(dict.fromkeys([*(item["cron"] for item in schedules), "*/2 * * * *"]))
        result = api("/schedules", method="PUT", data=encoded([{"cron": cron} for cron in desired]), content_type="application/json")
    after_settings, after_deployment = current()
    for old in settings["bindings"]:
        if signatures and old["name"] == "CMAIL_SEND_IDENTITIES":
            continue
        if args.enable_ritmo and old["name"] in {"CMAIL_SEND_IDENTITIES", "CMAIL_RITMO_SEND_KEY"}:
            continue
        if old["name"] == "CMAIL_MAILBOXES" or old["name"] == "RESEND_RECEIVING_API_KEY" and args.enable_replies:
            continue
        if args.enable_replies and old["name"] in {"CMAIL_RESEND_API_KEY", "CMAIL_SUPPORT_SEND_KEY", "CMAIL_SEND_IDENTITIES"}:
            continue
        if args.enable_fast_reads and old["name"] == "CMAIL_FAST_READS":
            continue
        if args.enable_sent and old["name"] == "CMAIL_SENT_ENABLED":
            continue
        if args.enable_compose and old["name"] == "CMAIL_COMPOSE_ENABLED":
            continue
        if args.enable_drafts and old["name"] == "CMAIL_DRAFTS_ENABLED":
            continue
        if args.enable_agent and old["name"] == "CMAIL_AGENT_ENABLED":
            continue
        actual = next((b for b in after_settings["bindings"] if b["name"] == old["name"]), None)
        if actual != old:
            raise RuntimeError("Existing binding changed: " + old["name"])
    if signatures:
        actual = json.loads(next(b["text"] for b in after_settings["bindings"] if b["name"] == "CMAIL_SEND_IDENTITIES"))
        assert actual == updated_signatures(settings, signatures)
    if args.enable_ritmo:
        old_identities = json.loads(next(b["text"] for b in settings["bindings"] if b["name"] == "CMAIL_SEND_IDENTITIES"))
        new_identities = json.loads(next(b["text"] for b in after_settings["bindings"] if b["name"] == "CMAIL_SEND_IDENTITIES"))
        assert all(new_identities.get(address) == identity for address, identity in old_identities.items())
        assert all(new_identities.get(address) == identity for address, identity in RITMO.items())
        old_mailboxes = set(next(b["text"] for b in settings["bindings"] if b["name"] == "CMAIL_MAILBOXES").split(","))
        new_mailboxes = set(next(b["text"] for b in after_settings["bindings"] if b["name"] == "CMAIL_MAILBOXES").split(","))
        assert new_mailboxes == old_mailboxes | set(RITMO)
        assert next(b for b in after_settings["bindings"] if b["name"] == "CMAIL_RITMO_SEND_KEY")["type"] == "secret_text"
    receipt = {"mode": "deploy" if args.deploy else "schedule", "before_path": str(before_path), "source_sha256": sources, "result": result, "deployment": after_deployment, "settings": after_settings, "schedules": api("/schedules")["schedules"]}
    path = args.receipt_dir / f"production-{'deploy' if args.deploy else 'schedule'}-{stamp}.json"
    path.write_bytes(encoded(receipt) + b"\n")
    print(json.dumps({"receipt": str(path), "version": after_deployment["versions"][0]["version_id"], "schedules": receipt["schedules"], "existing_bindings_preserved": True}, indent=2))


if __name__ == "__main__":
    main()
