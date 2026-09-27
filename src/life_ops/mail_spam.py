"""Reversible CMAIL sender rules, shared by the inbox and mail attention views.

Rules use the existing encrypted/snapshotted UI state. Original mail and work
dispositions stay intact. Legacy Hidden keys retain their original identity scope.
"""
from __future__ import annotations

import json
import re
import sqlite3
from email.utils import getaddresses

from life_ops import mail_identities, store

SENDER_PREFIX = "sender:"


def normalize_sender(value: object) -> str:
    raw = str(value or "").strip()
    if not raw or any(c in raw for c in "\r\n\x00"):
        raise ValueError("Enter one complete email address.")
    addresses = getaddresses([raw])
    if len(addresses) != 1:
        raise ValueError("Enter one complete email address.")
    email = addresses[0][1].strip().lower()
    if not re.fullmatch(r"[^\s@<>,;:\\\"()]+@[^\s@<>,;:\\\"()]+\.[^\s@<>,;:\\\"()]+", email) or "*" in email:
        raise ValueError("Enter one complete email address; domains and wildcards are not supported.")
    return email


def rules(connection: sqlite3.Connection) -> dict[str, str]:
    return store.get_hidden_mail_contacts(connection)


def list_senders(connection: sqlite3.Connection) -> list[dict]:
    result = []
    for rule_id, created_at in rules(connection).items():
        global_rule = rule_id.startswith(SENDER_PREFIX)
        sender = rule_id[len(SENDER_PREFIX):] if global_rule else rule_id.rsplit("::", 1)[-1]
        result.append({"id": rule_id, "sender": sender, "created_at": created_at,
                       "scope": "All CMAIL inboxes" if global_rule else "Existing hidden correspondence"})
    return sorted(result, key=lambda item: (item["sender"], item["id"]))


def _lock_rules(connection: sqlite3.Connection) -> None:
    # Serialize read/modify/write even on the plaintext live runtime database.
    if not connection.in_transaction:
        connection.execute("BEGIN IMMEDIATE")


def add_sender(connection: sqlite3.Connection, sender: object) -> str:
    email = normalize_sender(sender)
    _lock_rules(connection)
    current = rules(connection)
    # A deliberate all-inbox rule replaces narrower legacy rules for this address.
    for rule_id in list(current):
        if not rule_id.startswith(SENDER_PREFIX) and rule_id.rsplit("::", 1)[-1] == email:
            current.pop(rule_id)
    current.setdefault(SENDER_PREFIX + email, store._utc_now_string())
    store.set_hidden_mail_contacts(connection, current)
    return email


def remove_rule(connection: sqlite3.Connection, rule_id: str) -> bool:
    _lock_rules(connection)
    current = rules(connection)
    existed = current.pop(str(rule_id).strip().lower(), None) is not None
    store.set_hidden_mail_contacts(connection, current)
    return existed


def restore_contact(connection: sqlite3.Connection, contact_key: str) -> bool:
    _lock_rules(connection)
    current = rules(connection)
    contact_key = contact_key.strip().lower()
    email = contact_key.rsplit("::", 1)[-1]
    removed = False
    for rule_id in (contact_key, SENDER_PREFIX + email):
        removed = current.pop(rule_id, None) is not None or removed
    store.set_hidden_mail_contacts(connection, current)
    return removed


def is_spam(message, sender_rules: dict[str, str]) -> bool:
    record = dict(message)
    if not sender_rules or record.get("channel", "email") != "email" or record.get("source") == "gmail":
        return False
    contact_key = str(record.get("contact_key") or "").lower()
    direction = str(record.get("direction") or "inbound").lower()
    if not contact_key:
        address_field = "external_to" if direction == "outbound" else "external_from"
        address = str(record.get(address_field) or "")
        if not address and direction != "outbound":
            try:
                sender = json.loads(record.get("from_json") or "{}")
                address = sender.get("email", "") if isinstance(sender, dict) else ""
            except (ValueError, TypeError):
                pass
        contact_key = store.mail_contact_key(person=record.get("person", ""), external_from=address, source=record.get("source", ""))
        identity = mail_identities.infer_mail_identity(
            explicit=record.get("mail_identity", ""),
            direction=direction,
            external_from=record.get("external_from", ""),
            external_to=record.get("external_to", ""),
            external_cc=record.get("external_cc", ""),
            external_bcc=record.get("external_bcc", ""),
        )
        if identity != mail_identities.DEFAULT_MAIL_IDENTITY:
            contact_key = f"{identity}::{contact_key}"
    email = contact_key.rsplit("::", 1)[-1]
    return contact_key in sender_rules or SENDER_PREFIX + email in sender_rules


def suppressed_ids(connection: sqlite3.Connection) -> set[int]:
    sender_rules = rules(connection)
    if not sender_rules:
        return set()
    return {int(row["id"]) for row in connection.execute(
        """SELECT id, channel, source, direction, person, mail_identity,
                  external_from, external_to, external_cc, external_bcc, from_json
           FROM communications WHERE channel = 'email' AND source != 'gmail'"""
    ) if is_spam(row, sender_rules)}


def suppresses_all(communication_ids, suppressed: set[int]) -> bool:
    ids = {int(value) for value in communication_ids}
    return bool(ids) and ids.issubset(suppressed)
