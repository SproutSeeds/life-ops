"""Inventory/import preserved Bind My Soul history without modifying local mail.

Default: read-only inventory. --apply imports and verifies every message;
--verify rechecks the latest saved import receipt; --replay proves idempotency.
Receipts contain IDs and hashes, never mail content or credentials.
"""
from __future__ import annotations

import argparse
import base64
from collections import Counter
from datetime import datetime, timezone
from email import policy
from email.message import EmailMessage
from email.parser import BytesParser
from email.utils import format_datetime, parsedate_to_datetime
import hashlib
import json
from pathlib import Path
import sqlite3
from zoneinfo import ZoneInfo

from life_ops import cloudflare_email, credentials, mail_ui, mail_vault, store

SUPPORT = "support@bindmysoul.com"
WORKER = "https://life-ops-email-ingest.frg-life-ops.workers.dev"
DEFAULT_DB = Path.home() / ".lifeops/data/cmail_runtime.db"
ROOTS = [Path.home() / ".lifeops/data/attachments", Path("data/attachments")]


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, indent=2).encode()


def vault_read(relative_path, expected_hash=""):
    for root in ROOTS:
        if (root / relative_path).is_file():
            raw = mail_vault.read_encrypted_vault_file(vault_root=root, relative_path=relative_path)
            if expected_hash and sha(raw) != expected_hash:
                raise ValueError("Local artifact hash mismatch")
            return raw
    raise FileNotFoundError("Preserved mail artifact unavailable: " + relative_path)


def timestamp(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        parsed = parsedate_to_datetime(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("America/Chicago"))
    return parsed.astimezone(timezone.utc)


def sent_mime(row, receipt):
    request = receipt["request"]
    sent_at = timestamp(receipt.get("sent_at") or row["happened_at"])
    message = EmailMessage(policy=policy.SMTP)
    for name, field in [("From", "from"), ("To", "to"), ("Cc", "cc"), ("Bcc", "bcc"), ("Reply-To", "reply_to"), ("Subject", "subject")]:
        value = request.get(field)
        if value:
            message[name] = ", ".join(value) if isinstance(value, list) else value
    for name, value in (request.get("headers") or {}).items():
        if name.lower() not in {"content-type", "mime-version", "content-transfer-encoding"} and name not in message:
            message[name] = value
    for name, value in [("Message-ID", row["message_id"]), ("In-Reply-To", row["in_reply_to"]), ("References", " ".join(json.loads(row["references_json"] or "[]"))), ("Date", format_datetime(sent_at))]:
        if value and name not in message:
            message[name] = value
    message["X-Cmail-History-Provenance"] = "reconstructed-from-saved-delivery"
    if request.get("text") is not None:
        message.set_content(request["text"])
    if request.get("html") is not None:
        if message.get_content_type() == "text/plain" and request.get("text") is not None:
            message.add_alternative(request["html"], subtype="html")
        else:
            message.set_content(request["html"], subtype="html")
    stored = receipt.get("stored_attachments") or []
    if request.get("attachments") and not stored:
        raise ValueError("Sent attachment metadata requires explicit reconstruction")
    for attachment in stored:
        content = vault_read(attachment["relative_path"], attachment.get("sha256", ""))
        main, sub = (attachment.get("mime_type") or "application/octet-stream").split("/", 1)
        message.add_attachment(content, maintype=main, subtype=sub, filename=attachment["filename"],
                               disposition="inline" if attachment.get("inline") else "attachment",
                               cid=attachment.get("content_id") or None)
    # Deterministic boundaries make retries produce identical raw hashes.
    for index, part in enumerate(message.walk()):
        if part.is_multipart():
            part.set_boundary(f"cmail-history-{row['id']}-{row['raw_sha256'][:24]}-{index}")
    return message.as_bytes(), sent_at


def facts(raw):
    parsed = BytesParser(policy=policy.default).parsebytes(raw)
    attachments = []
    for part in parsed.walk():
        if part.is_multipart():
            continue
        if part.get_content_disposition() in {"attachment", "inline"} or part.get_content_maintype() not in {"text", "multipart"}:
            content = part.get_payload(decode=True) or b""
            attachments.append({"bytes": len(content), "sha256": sha(content)})
    return {"sha256": sha(raw), "bytes": len(raw), "message_id": str(parsed.get("Message-ID", "")).strip(),
            "date": str(parsed.get("Date", "")), "attachments": attachments}


def inventory(db):
    connection = sqlite3.connect(f"file:{db.resolve()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    rows = [dict(row) for row in connection.execute("SELECT * FROM communications WHERE mail_identity='bind_my_soul' ORDER BY id")]
    viewed = store.get_viewed_mail_message_keys(connection)
    connection.close()
    snapshot = sha(encoded(rows))
    excluded, entries = [], []
    for row in rows:
        if row["deleted_at"] or row["status"] in {"deleted", "superseded"} or row["source"] not in {"resend_inbound", "resend_email"}:
            excluded.append({"local_id": row["id"], "source": row["source"], "status": row["status"], "deleted": bool(row["deleted_at"])})
            continue
        saved = vault_read(row["raw_relative_path"], row["raw_sha256"])
        inbound = row["source"] == "resend_inbound"
        if inbound:
            raw = saved
            date = BytesParser(policy=policy.default).parsebytes(raw, headersonly=True).get("Date")
            received = timestamp(str(date) if date else row["happened_at"])
            provider = "resend-receiving"
            provenance = "original-mime"
        else:
            if row["status"] != "sent":
                raise ValueError("Unsent delivery record cannot be imported as sent history")
            raw, received = sent_mime(row, json.loads(saved))
            provider = "resend-sent-history"
            provenance = "reconstructed-from-saved-delivery"
        if not row["message_id"] or not row["external_id"]:
            raise ValueError(f"Missing preserved identity for local record {row['id']}")
        read = not inbound or any(key in viewed for key in mail_ui._communication_row_read_keys(row))
        payload = {"provider": provider, "external_id": row["external_id"], "envelope_to": SUPPORT,
                   "envelope_from": row["external_from"], "direction": "inbound" if inbound else "outbound",
                   "received_at": received.isoformat().replace("+00:00", "Z"), "raw_base64": base64.b64encode(raw).decode(),
                   "payload_hash": sha(raw), "read": read, "archived": not inbound,
                   "history_provenance": provenance, "history_local_id": row["id"]}
        expected = {"local_id": row["id"], "source": row["source"], "provenance": provenance,
                    "source_sha256": row["raw_sha256"], "received_at": payload["received_at"],
                    "direction": payload["direction"], "read": read, "archived": not inbound, **facts(raw)}
        if expected["message_id"] != row["message_id"].strip():
            raise ValueError(f"Message-ID differs from preserved row {row['id']}")
        entries.append((payload, expected))
    return entries, {"local_rows_sha256": snapshot, "excluded": excluded,
                     "counts": dict(Counter(item[1]["direction"] for item in entries)),
                     "inbound_unread": sum(not e[1]["read"] for e in entries if e[1]["direction"] == "inbound"),
                     "attachments": sum(len(e[1]["attachments"]) for e in entries),
                     "items": [e[1] for e in entries]}


def remote(path, payload):
    return cloudflare_email._cloudflare_worker_request_json(worker_public_url=WORKER,
        secret=credentials.resolve_secret(name="LIFE_OPS_MAIL_INGEST_SECRET"), path=path, payload=payload, timeout_seconds=120)


def verify(entries, receipts):
    for offset in range(0, len(entries), 5):
        batch = entries[offset:offset + 5]
        ids = [receipts[str(expected["local_id"])]["id"] for _, expected in batch]
        actual = remote("/api/mail/archive/verify", {"ids": ids})["items"]
        if [item["id"] for item in actual] != ids:
            raise AssertionError("Verification response identity mismatch")
        for (_, expected), result in zip(batch, actual):
            for field in ["sha256", "bytes", "message_id", "direction", "read", "archived"]:
                if result.get(field) != expected[field]:
                    raise AssertionError(f"Cloud {field} mismatch for local record {expected['local_id']}")
            if result.get("mailbox") != SUPPORT:
                raise AssertionError("Cloud mailbox mismatch")
            if timestamp(result["date"]) != timestamp(expected["date"]):
                raise AssertionError("Cloud message date mismatch")
            if sorted(result["attachments"], key=lambda a: a["sha256"]) != sorted(expected["attachments"], key=lambda a: a["sha256"]):
                raise AssertionError("Cloud attachment integrity mismatch")
        print(f"Verified {offset + len(batch)}/{len(entries)} messages", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--verify", action="store_true")
    mode.add_argument("--replay", action="store_true")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--receipt-dir", type=Path, default=Path("data/qa/cmail-bindmysoul-2026-10-17"))
    args = parser.parse_args()
    entries, manifest = inventory(args.db)
    args.receipt_dir.mkdir(parents=True, exist_ok=True)
    if not (args.apply or args.verify or args.replay):
        (args.receipt_dir / "history-inventory.json").write_bytes(encoded(manifest) + b"\n")
        print(json.dumps({k: v for k, v in manifest.items() if k != "items"}, indent=2))
        return
    prior_manifest = json.loads((args.receipt_dir / "history-inventory.json").read_text())
    if prior_manifest != manifest:
        raise SystemExit("Preserved mail changed since inventory; review a fresh inventory before importing")
    path = args.receipt_dir / "history-import.json"
    receipts = json.loads(path.read_text()) if path.exists() else {}
    if args.apply or args.replay:
        stored = duplicates = 0
        for index, (payload, expected) in enumerate(entries, start=1):
            result = remote("/api/mail/archive/import", payload)
            local_id = str(expected["local_id"])
            if local_id in receipts and result["id"] != receipts[local_id]["id"]:
                raise AssertionError("Previously imported identity changed")
            if args.replay and (result.get("stored") or not result.get("duplicate")):
                raise AssertionError("Replay created an unexpected record")
            stored += bool(result.get("stored"))
            duplicates += bool(result.get("duplicate"))
            receipts.setdefault(local_id, result)
            path.write_bytes(encoded(receipts) + b"\n")
            if index % 10 == 0 or index == len(entries):
                print(f"Imported {index}/{len(entries)}: {stored} stored, {duplicates} duplicates", flush=True)
    verify(entries, receipts)
    after_manifest = inventory(args.db)[1]
    if after_manifest["local_rows_sha256"] != manifest["local_rows_sha256"]:
        raise AssertionError("Local mail rows changed during cloud verification; inspect concurrent activity")
    result = {"verified_at": datetime.now(timezone.utc).isoformat(), "mode": "replay" if args.replay else "apply" if args.apply else "verify",
              "messages": len(entries), "attachments": manifest["attachments"], "counts": manifest["counts"],
              "local_rows_unchanged": True, "local_rows_sha256": manifest["local_rows_sha256"]}
    if args.apply or args.replay:
        result.update(stored=stored, duplicates=duplicates)
    (args.receipt_dir / f"history-{result['mode']}-verified.json").write_bytes(encoded(result) + b"\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
