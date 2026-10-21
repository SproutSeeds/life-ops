"""Canonical C-mail client. Credentials stay in Keychain; failures never fall back."""
from __future__ import annotations

import argparse
import base64
import json
import sys
import uuid
from pathlib import Path

WEBSITE = "https://cmail.frg.earth"
WORKER = "https://life-ops-email-ingest.frg-life-ops.workers.dev"
CONFIG = Path.home() / ".lifeops/config/cmail-cloud.json"
RETIRED = (
    "Local CMAIL is archived. Use `cmail` and https://cmail.frg.earth. "
    "Save with `cmail draft-save --help`; send only with `cmail draft-send "
    "<cloud-id> --revision <revision> --confirm-send` after Cody authorizes sending. "
    "There is no local/provider fallback."
)


def active() -> bool:
    if not CONFIG.exists():
        return False
    value = json.loads(CONFIG.read_text())
    if value.get("system") != "cmail-cloud" or value.get("active") is not True:
        raise RuntimeError("Invalid CMAIL cutover configuration; refusing legacy fallback")
    return True


def guard_legacy_write() -> None:
    if active():
        raise RuntimeError(RETIRED)


def retired_http(handler) -> bool:
    """Retire every old UI/API route; health remains useful to the supervisor."""
    if not active():
        return False
    from urllib.parse import parse_qs, urlparse, urlencode
    url = urlparse(handler.path)
    if handler.command == "GET" and url.path in {"/healthz", "/api/health"}:
        handler._send_json({"ok": True, "app": "cmail", "system": "cmail-cloud",
                            "public_url": WEBSITE, "legacy_ui_retired": True,
                            "role": "Life Ops mail projection"})
    elif handler.command == "GET" and not url.path.startswith("/api/"):
        target = WEBSITE + "/"
        draft = parse_qs(url.query).get("draft", [""])[0]
        if draft:
            try:
                target += "?" + urlencode({"view": "drafts", "draft": resolve_draft_id(draft)})
            except ValueError:
                target += "?view=drafts"
        handler.send_response(308)
        handler.send_header("Location", target)
        handler.send_header("Cache-Control", "no-store")
        handler.end_headers()
    else:
        handler._send_json({"error": "legacy_cmail_retired", "message": RETIRED,
                            "website": WEBSITE}, status_code=410)
    return True


def serve_redirect(*, host: str, port: int) -> None:
    """Keep existing bookmarks/health alive without opening the old mailbox."""
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    class Handler(BaseHTTPRequestHandler):
        def _send_json(self, value, *, status_code=200):
            content = json.JSONEncoder().encode(value).encode()
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def do_GET(self):
            if not retired_http(self):
                self._send_json({"error": "cloud_configuration_missing", "website": WEBSITE}, status_code=503)

        do_POST = do_GET

        def log_message(self, *_args):
            pass
    print(f"[cmail_cloud] redirect and projection health at http://{host}:{port}; legacy UI disabled", flush=True)
    with ThreadingHTTPServer((host, port), Handler) as server:
        server.serve_forever()


def sync_projection(*, runtime_db_path: Path, limit: int = 10, **_ignored) -> dict:
    """Append new cloud mail to Life Ops without changing existing dispositions.

    The cursor is stored in the target database transaction. A failed ingest
    leaves the cursor at the previous item, so restarts cannot skip mail.
    """
    from email import policy
    from email.parser import BytesParser
    from . import store
    from .mail_ingest import ingest_cloudflare_email_payload
    from .mail_metadata import primary_message_id
    config = json.loads(CONFIG.read_text())
    key = "cmail-cloud-projection-v1"
    with store.open_db(runtime_db_path) as connection:
        connection.execute("CREATE TABLE IF NOT EXISTS cmail_cloud_projection (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        row = connection.execute("SELECT value FROM cmail_cloud_projection WHERE key=?", (key,)).fetchone()
        cursor = row[0] if row else config.get("projection_start_cursor")
        if not cursor:
            raise RuntimeError("CMAIL projection requires a recorded cutover cursor")
        data = request("changes", {"cursor": cursor, "limit": min(limit, 10)})
        imported = existing = 0
        for item in data["items"]:
            payload = item["payload"]
            message = BytesParser(policy=policy.default).parsebytes(base64.b64decode(payload["raw_base64"], validate=True))
            message_id = primary_message_id(message.get_all("Message-ID", []))
            known = message_id and connection.execute(
                "SELECT id FROM communications WHERE message_id=? LIMIT 1", (message_id,)
            ).fetchone()
            if known:
                existing += 1
            else:
                # Only this signed, pinned cloud projection may set the trusted
                # cmail-cloud provider, including outbound direction.
                ingest_cloudflare_email_payload({**payload, "provider": "cmail-cloud",
                    "external_id": "cmail-cloud:" + item["id"]}, db_path=runtime_db_path, connection=connection, commit=False)
                imported += 1
            connection.execute("INSERT OR REPLACE INTO cmail_cloud_projection(key,value) VALUES(?,?)", (key, item["id"]))
            connection.commit()
        return {"source": "cmail-cloud", "imported": imported, "existing": existing,
                "cursor": data["cursor"], "has_more": data["has_more"]}


def request(action: str, parameters: dict | None = None) -> dict:
    from .cloudflare_email import _cloudflare_worker_request_json
    from .credentials import resolve_secret
    secret = resolve_secret(name="LIFE_OPS_MAIL_INGEST_SECRET")
    if not secret:
        raise RuntimeError("CMAIL cloud credential unavailable; no local fallback")
    # The destination is pinned in code, never supplied by a message or caller.
    return _cloudflare_worker_request_json(
        worker_public_url=WORKER, secret=secret, path="/api/mail/agent",
        payload={"action": action, "parameters": parameters or {}},
    )


def resolve_draft_id(value: str) -> str:
    if value.isdigit():
        config = json.loads(CONFIG.read_text())
        mapped = config.get("legacy_drafts", {}).get(value)
        if not mapped:
            raise ValueError("Legacy draft ID has no verified cloud migration")
        return mapped
    return value


def address_for(value: str | None) -> str:
    from .mail_identities import MAIL_IDENTITIES
    if not value or value == "all":
        return ""
    return MAIL_IDENTITIES[value].address if value in MAIL_IDENTITIES else value


def addresses(values: list[str] | None) -> list[str]:
    from email.utils import getaddresses
    return [address for _, address in getaddresses(values or []) if address]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="cmail", description="C-mail cloud mailbox and shared Drafts. Saving never sends.",
        epilog="Mail lives at https://cmail.frg.earth. No local or direct-provider fallback.",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ["status", "identities", "url", "open"]:
        sub.add_parser(name)
    for name in ["messages", "search", "drafts", "contacts", "spam-senders"]:
        child = sub.add_parser(name)
        child.add_argument("--mailbox", "--identity", "--mail-identity", dest="mailbox")
        child.add_argument("--query", default="")
        child.add_argument("--cursor")
        child.add_argument("--limit", type=int, default=50)
        if name in {"messages", "search"}:
            child.add_argument("--view", choices=["all", "inbox", "sent", "archive", "spam"], default="inbox")
    for name in ["message", "thread", "reply-context", "draft"]:
        child = sub.add_parser(name)
        child.add_argument("id")
        if name == "message":
            child.add_argument("--raw", action="store_true")
            child.add_argument("--attachment", type=int)
            child.add_argument("--output", type=Path)
    save = sub.add_parser("draft-save", help="Save to the website's Drafts; never sends")
    save.add_argument("--id", "--draft-id", dest="id")
    save.add_argument("--revision", type=int, help="Required when editing an existing draft")
    save.add_argument("--save-id", help="Stable UUID for this save; reuse the exact request on retry")
    save.add_argument("--json-file", type=Path, help="Exact API save envelope: id, revision, save_id, draft")
    save.add_argument("--sender", "--identity", "--mail-identity", dest="sender")
    save.add_argument("--to", action="append")
    save.add_argument("--cc", action="append")
    save.add_argument("--bcc", action="append")
    save.add_argument("--subject")
    body = save.add_mutually_exclusive_group()
    body.add_argument("--text", "--body", dest="text")
    body.add_argument("--text-file", "--body-file", dest="text_file", type=Path)
    save.add_argument("--reply-to-message", help="Cloud mail ID; derives the exact sender and reply headers")
    save.add_argument("--no-signature", action="store_true")
    for name in ["draft-send", "draft-discard"]:
        child = sub.add_parser(name)
        child.add_argument("id")
        child.add_argument("--revision", type=int, required=True)
        if name == "draft-send":
            child.add_argument("--confirm-send", action="store_true", required=True,
                               help="Use only for a user-authorized send of this exact saved revision")
    for name in ["read", "unread", "archive", "unarchive", "spam", "not-spam"]:
        child = sub.add_parser(name)
        child.add_argument("ids", nargs="+")
    args_in = list(argv if argv is not None else sys.argv[1:])
    # Existing agents often pass --format json; the cloud client is always JSON.
    for index in range(len(args_in) - 2, -1, -1):
        if args_in[index:index + 2] == ["--format", "json"]:
            del args_in[index:index + 2]
    if args_in and args_in[0] == "new-draft":
        args_in[0] = "draft-save"
    args = parser.parse_args(args_in)
    try:
        action = args.command
        p = {}
        if action in {"url", "open"}:
            if action == "open":
                import webbrowser
                webbrowser.open(WEBSITE)
            print(WEBSITE)
            return 0
        if action in {"messages", "search", "drafts", "contacts", "spam-senders"}:
            p = {"mailbox": address_for(args.mailbox), "query": args.query, "limit": args.limit}
            if args.cursor:
                p["cursor"] = args.cursor
            if action in {"messages", "search"}:
                action = "messages"
                p["view"] = args.view
        elif action in {"message", "thread", "reply-context", "draft"}:
            p = {"id": resolve_draft_id(args.id) if action == "draft" else args.id}
            if action == "thread":
                action = "conversation"
            if action == "message":
                if args.raw:
                    p["raw"] = True
                if args.attachment is not None:
                    p["attachment"] = args.attachment
        elif action == "draft-save":
            if args.json_file:
                p = json.loads(args.json_file.read_text())
                if not isinstance(p, dict) or not {"id", "revision", "save_id", "draft"} <= p.keys():
                    raise ValueError("Save file requires id, revision, save_id, draft")
            else:
                if not args.save_id:
                    raise ValueError("--save-id UUID is required. Generate it once, then reuse it and the exact content on retry.")
                save_id = str(uuid.UUID(args.save_id))
                if args.id and args.revision is None:
                    raise ValueError("--revision is required when editing; first read `cmail draft ID`")
                previous = None
                if args.id:
                    previous = request("draft", {"id": resolve_draft_id(args.id)})
                    if previous["state"] != "editing":
                        raise ValueError("Draft is closed or sending; reread it before editing")
                old = previous["draft"] if previous else {}
                sender = address_for(args.sender) if args.sender else old.get("sender") or address_for("frg")
                identities = request("identities")["identities"]
                identity = next((item for item in identities if item["address"] == sender), None)
                if not identity:
                    raise ValueError("Sender is not configured in CMAIL")
                signature = old.get("signature", identity["signature"]) if sender == old.get("sender", sender) else identity["signature"]
                text = args.text_file.read_text() if args.text_file else args.text if args.text is not None else old.get("text", "")
                value = {**old, "kind": old.get("kind", "compose"), "sender": sender,
                         "to": addresses(args.to) if args.to is not None else old.get("to", []),
                         "cc": addresses(args.cc) if args.cc is not None else old.get("cc", []),
                         "bcc": addresses(args.bcc) if args.bcc is not None else old.get("bcc", []),
                         "subject": args.subject if args.subject is not None else old.get("subject", ""),
                         "pending": old.get("pending", ""), "text": text, "signature": signature}
                if args.reply_to_message:
                    context = request("reply-context", {"id": args.reply_to_message})
                    if context.get("id") and not args.id:
                        raise ValueError("This conversation already has a draft: " + context["id"] + ". Read and edit it with its revision.")
                    context = context.get("context") or context["draft"]
                    value.update(kind="reply", reply_id=args.reply_to_message, sender=context["sender"],
                                 to=context["to"], subject=context["subject"], signature=context["signature"])
                if (not previous or args.text is not None or args.text_file) and not args.no_signature and value["signature"] and not text.rstrip().endswith(value["signature"].rstrip()):
                    value["text"] = text.rstrip() + "\n\n" + value["signature"]
                p = {"id": resolve_draft_id(args.id) if args.id else "draft_" + save_id,
                     "revision": args.revision or 0, "save_id": save_id, "draft": value}
        elif action in {"draft-send", "draft-discard"}:
            p = {"id": resolve_draft_id(args.id), "revision": args.revision}
            if action == "draft-send":
                p["confirm_send"] = args.confirm_send
        elif action in {"read", "unread", "archive", "unarchive", "spam", "not-spam"}:
            p = {"ids": args.ids, "action": action}
            action = "message-action"
        result = request(action, p)
        if action == "message" and args.output:
            content = result.get("content_base64")
            if not content:
                raise ValueError("--output requires --raw or --attachment")
            with args.output.open("xb") as stream:
                stream.write(base64.b64decode(content, validate=True))
            result = {"saved": str(args.output), "filename": result.get("filename")}
        print(json.JSONEncoder(indent=2, ensure_ascii=False).encode(result))
        return 0 if action != "draft-send" or result.get("state") == "sent" else 1
    except (RuntimeError, ValueError, OSError, KeyError) as error:
        print(json.JSONEncoder().encode({"error": str(error), "system": "cmail-cloud",
                                        "website": WEBSITE, "local_fallback": False}), file=sys.stderr)
        return 1


def legacy_main(argv: list[str]) -> int | None:
    """Intercept old mail entrypoints before their parsers can write to SQLite."""
    if not argv or not active():
        return None
    command, *rest = argv
    aliases = {"cmail-drafts": "drafts", "cmail-draft-save": "draft-save",
               "cmail-draft-send": "draft-send", "cmail-draft-discard": "draft-discard"}
    if command in aliases:
        return main([aliases[command], *rest])
    if command == "cmail-health-check":
        try:
            print(json.JSONEncoder().encode({"ok": True, **request("status")}))
            return 0
        except RuntimeError as error:
            print(str(error), file=sys.stderr)
            return 1
    if command in {"resend-send-email", "resend-process-queue", "cmail-batch-send", "cmail-agent-open"}:
        print(RETIRED, file=sys.stderr)
        return 2
    return None


if __name__ == "__main__":
    raise SystemExit(main())
