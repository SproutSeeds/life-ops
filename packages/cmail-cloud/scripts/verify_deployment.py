"""Read-only production source, namespace, authentication and sync verification."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
from email import policy
from email.parser import BytesParser
import hashlib
import json
from pathlib import Path
import urllib.error
import urllib.request

from life_ops import cloudflare_email, credentials
from deploy import BASE, ROOT, api, current, encoded


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def unauthenticated(url, method="GET"):
    request = urllib.request.Request(url, method=method, data=b"{}" if method == "POST" else None,
                                    headers={"User-Agent": cloudflare_email.CLOUDFLARE_WORKER_USER_AGENT, "Accept": "application/json"})
    try:
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=30) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--require-scheduled", action="store_true")
    parser.add_argument("--require-replies", action="store_true")
    parser.add_argument("--receipt-dir", type=Path, default=Path("data/qa/cmail-bindmysoul-2026-10-17"))
    args = parser.parse_args()
    key = credentials.resolve_secret(name="CLOUDFLARE_API_TOKEN")
    request = urllib.request.Request(BASE, headers={"Authorization": "Bearer " + key})
    with urllib.request.urlopen(request, timeout=60) as response:
        raw = response.read()
        mime = response.headers["Content-Type"]
    message = BytesParser(policy=policy.default).parsebytes(f"Content-Type: {mime}\r\nMIME-Version: 1.0\r\n\r\n".encode() + raw)
    actual = {}
    for part in message.iter_parts():
        name = part.get_filename() or part.get_param("name", header="content-disposition")
        actual[name] = hashlib.sha256(part.get_payload(decode=True)).hexdigest()
    expected = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((ROOT / "src").glob("*.mjs"))}
    if actual != expected:
        raise AssertionError("Production source differs from the verified local modules")
    settings, deployment = current()
    schedules = api("/schedules")["schedules"]
    assert "*/2 * * * *" in [item["cron"] for item in schedules]
    mailboxes = next(b["text"] for b in settings["bindings"] if b["name"] == "CMAIL_MAILBOXES").split(",")
    assert "support@bindmysoul.com" in mailboxes
    assert next(b for b in settings["bindings"] if b["name"] == "RESEND_RECEIVING_API_KEY")["type"] == "secret_text"
    if args.require_replies:
        for name in ["CMAIL_RESEND_API_KEY", "CMAIL_SUPPORT_SEND_KEY"]:
            assert next(b for b in settings["bindings"] if b["name"] == name)["type"] == "secret_text"
        identities = json.loads(next(b["text"] for b in settings["bindings"] if b["name"] == "CMAIL_SEND_IDENTITIES"))
        assert set(identities) == set(mailboxes)
        assert identities["support@bindmysoul.com"]["key"] == "support"
    protected = {"website": unauthenticated("https://cmail.frg.earth/"),
                 "worker_mail_api": unauthenticated("https://life-ops-email-ingest.frg-life-ops.workers.dev/cmail/api/session"),
                 "signed_admin": unauthenticated("https://life-ops-email-ingest.frg-life-ops.workers.dev/api/mail/resend/status", "POST")}
    assert protected == {"website": 302, "worker_mail_api": 302, "signed_admin": 401}, protected
    secret = credentials.resolve_secret(name="LIFE_OPS_MAIL_INGEST_SECRET")
    def signed(path):
        return cloudflare_email._cloudflare_worker_request_json(worker_public_url="https://life-ops-email-ingest.frg-life-ops.workers.dev", secret=secret, path=path, payload={})
    sync = signed("/api/mail/resend/status")
    queue = signed("/api/mail/queue/status")
    scheduled_at = sync.get("last_scheduled_success_at")
    scheduled_fresh = bool(scheduled_at) and (datetime.now(timezone.utc) - datetime.fromisoformat(scheduled_at.replace("Z", "+00:00"))).total_seconds() < 300
    result = {"checked_at": datetime.now(timezone.utc).isoformat(), "version": deployment["versions"][0]["version_id"],
              "source_sha256": actual, "existing_namespace_verified": True, "mailboxes": mailboxes,
              "authentication": protected, "schedules": schedules, "scheduled_verified": scheduled_fresh and sync["status"] == "healthy", "sync": sync, "queue": queue}
    target = args.receipt_dir / "production-verification.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(encoded(result) + b"\n")
    print(json.dumps(result, indent=2))
    if args.require_scheduled and not result["scheduled_verified"]:
        raise SystemExit("A healthy scheduled completion within five minutes has not yet been observed")


if __name__ == "__main__":
    main()
