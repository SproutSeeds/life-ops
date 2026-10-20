"""Copy legacy drafts exactly into cloud Drafts; never sends or edits originals."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import uuid
from email.utils import parseaddr

from life_ops import cmail_cloud, mail_ui

NAMESPACE = uuid.UUID("cab12778-1f78-469c-9044-7cb124d58626")


def encode(value):
    return json.JSONEncoder(ensure_ascii=False, sort_keys=True, indent=2).encode(value).encode()


def collect(path):
    with sqlite3.connect("file:" + str(path.resolve()) + "?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        drafts = mail_ui._list_cmail_drafts(connection)
    result = []
    # Oldest first keeps the most recently edited original at the top after
    # migration, since cloud save timestamps reflect the migration itself.
    for old in sorted(drafts, key=lambda item: (item["updated_at"], item["id"])):
        if old["attachments"]:
            raise RuntimeError("Attachment-bearing draft requires supported attachment migration before cutover")
        value = {"kind": "compose", "sender": parseaddr(old["from_email"])[1],
                 "to": cmail_cloud.addresses([old["to"]]), "pending": "", "subject": old["subject"],
                 "text": old["body_text"], "signature": ""}
        for field in ["cc", "bcc"]:
            if old[field]:
                value[field] = cmail_cloud.addresses([old[field]])
        if old["in_reply_to"]:
            value["in_reply_to"] = old["in_reply_to"]
        if old["references"]:
            value["references"] = old["references"]
        digest = hashlib.sha256(encode(value)).hexdigest()
        # Stable across retries, process restarts, and source snapshot paths.
        identifier = uuid.uuid5(NAMESPACE, f"cmail-local-draft:{old['id']}")
        save = {"id": "draft_" + str(identifier), "revision": 0,
                "save_id": str(uuid.uuid5(identifier, digest)), "draft": value}
        result.append({"legacy_id": old["id"], "source_updated_at": old["updated_at"], "sha256": digest, "save": save})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--expected-count", type=int, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    drafts = collect(args.source)
    if len(drafts) != args.expected_count:
        raise RuntimeError(f"Draft count changed: {len(drafts)}; re-audit before migration")
    receipts = []
    # Existing receipts make recovery read-only: never overwrite a draft that
    # Cody has since edited, sent, or discarded through the website.
    prior = json.loads(args.receipt.read_text()) if args.receipt.exists() else {"drafts": []}
    known = {item["cloud_id"]: item for item in prior["drafts"]}
    for item in drafts:
        save = item["save"]
        if save["id"] in known:
            receipt = known[save["id"]]
            if receipt["sha256"] != item["sha256"]:
                raise RuntimeError("Source changed after verified migration; manual reconciliation required")
            receipts.append(receipt)
            continue
        if args.apply:
            saved = cmail_cloud.request("draft-save", save)
            reopened = cmail_cloud.request("draft", {"id": save["id"]})
            if reopened.get("state") != "editing" or reopened.get("draft") != save["draft"] or reopened["revision"] != saved["revision"]:
                raise RuntimeError("Cloud draft readback differs from original")
        receipt = {"legacy_id": item["legacy_id"], "cloud_id": save["id"], "sha256": item["sha256"],
                   "source_updated_at": item["source_updated_at"], "verified_exact": args.apply, "sent": False}
        receipts.append(receipt)
        if args.apply:
            args.receipt.parent.mkdir(parents=True, exist_ok=True)
            args.receipt.write_bytes(encode({"source": str(args.source), "drafts": receipts, "provider_sends": 0}) + b"\n")
            args.receipt.chmod(0o600)
    print(encode({"apply": args.apply, "count": len(receipts), "provider_sends": 0,
                  "legacy_drafts": {str(item["legacy_id"]): item["cloud_id"] for item in receipts}}).decode())


if __name__ == "__main__":
    main()
