"""CMAIL labels: named views over correspondence.

A label combines rules that match on their own with conversations Cody adds or
removes by hand. Rules match a whole word or phrase in the subject, sender name
or body; an exact address in any address field, including the delivered-to
envelope recipient; or a correspondent's domain. One matching message labels
its whole conversation, linked only through Message-ID, In-Reply-To and
References headers. Manual choices are stored against a Message-ID and take
precedence over rules: a removed conversation stays out even when a rule
matches. Labels organize the mailbox view only. Mail status, inbox identity,
Spam rules, reminders and authorization decisions stay unchanged.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.utils import getaddresses
from typing import Any, Iterable

from life_ops import mail_identities, store

MAX_LABELS = 50
MAX_RULES_PER_LABEL = 50
MAX_LABEL_NAME_CHARS = 40
MAX_RULE_CHARS = 120
MIN_SUGGESTION_NAME_CHARS = 3
MAX_SUGGESTIONS = 25
RULE_KINDS = ("address", "domain", "keyword")
CONVERSATION_ACTIONS = ("add", "remove")
REASON_ADDED = "Added by you"
REASON_CONVERSATION = "Same conversation (reply headers)"

_CONTROL_RE = re.compile(r"[\x00-\x1f\x7f]")
_EMAIL_RE = re.compile(r"[^\s@<>,;:\\\"()*]+@[^\s@<>,;:\\\"()*]+\.[^\s@<>,;:\\\"()*]+")
_DOMAIN_RE = re.compile(r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z][a-z0-9-]{1,62}")
_WORD_CHAR_RE = re.compile(r"\w")
_QUOTES = "\"'“”‘’"
_ADDRESS_FIELDS = (
    ("external_from", "From"),
    ("external_to", "Sent to"),
    ("external_cc", "Cc"),
    ("external_bcc", "Bcc"),
    ("external_reply_to", "Reply-To"),
    ("envelope_to", "Delivered to"),
)
_KEYWORD_FIELDS = ("subject", "person", "snippet", "body_text")


def normalize_label_name(value: object) -> str:
    name = " ".join(str(value or "").split())
    if not name:
        raise ValueError("Give the label a name.")
    if _CONTROL_RE.search(name):
        raise ValueError("Label names can't contain control characters.")
    if len(name) > MAX_LABEL_NAME_CHARS:
        raise ValueError(f"Keep label names to {MAX_LABEL_NAME_CHARS} characters.")
    return name


def label_slug(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return slug or "label-" + hashlib.sha256(name.encode("utf-8")).hexdigest()[:8]


def parse_rule(value: object) -> tuple[str, str]:
    """Return (kind, value) for a keyword, exact address or @domain rule."""
    raw = str(value or "").strip()
    if not raw:
        raise ValueError("Enter a keyword, an email address, or an @domain.")
    if _CONTROL_RE.search(raw):
        raise ValueError("Enter one keyword, email address, or @domain per rule.")
    if len(raw) > MAX_RULE_CHARS:
        raise ValueError(f"Keep rules to {MAX_RULE_CHARS} characters.")
    if raw.startswith("@"):
        domain = raw[1:].strip().lower().rstrip(".")
        if not _DOMAIN_RE.fullmatch(domain):
            raise ValueError("Enter a domain like @example.com.")
        return "domain", domain
    if "@" in raw:
        addresses = [address for _, address in getaddresses([raw]) if address]
        if len(addresses) != 1 or not _EMAIL_RE.fullmatch(addresses[0]):
            raise ValueError("Enter one complete email address.")
        return "address", addresses[0].strip().lower()
    keyword = " ".join(raw.strip(_QUOTES).split()).lower()
    prefix = keyword.endswith("*")
    core = keyword[:-1].rstrip() if prefix else keyword
    if "*" in core:
        raise ValueError("Use * only at the end of a keyword, like grant*.")
    if len(_WORD_CHAR_RE.findall(core)) < 2:
        raise ValueError("Keywords need at least two letters or numbers.")
    return "keyword", core + ("*" if prefix else "")


def rule_text(kind: str, value: str) -> str:
    return f"@{value}" if kind == "domain" else value


def _rule_record(row: Any) -> dict[str, Any]:
    kind = str(row["kind"])
    value = str(row["value"])
    return {
        "id": int(row["id"]),
        "kind": kind,
        "value": value,
        "text": rule_text(kind, value),
        "created_at": str(row["created_at"] or ""),
    }


def list_labels(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    try:
        label_rows = connection.execute(
            """
            SELECT id, name, slug, color, sort_order, created_at, updated_at
            FROM mail_labels
            ORDER BY sort_order, name COLLATE NOCASE, id
            """
        ).fetchall()
        rule_rows = connection.execute(
            "SELECT id, label_id, kind, value, created_at FROM mail_label_rules ORDER BY id"
        ).fetchall()
    except sqlite3.OperationalError:
        # Read-only snapshots can predate the label tables until the first write.
        return []
    rules_by_label: dict[int, list[dict[str, Any]]] = {}
    for row in rule_rows:
        rules_by_label.setdefault(int(row["label_id"]), []).append(_rule_record(row))
    return [
        {
            "id": int(row["id"]),
            "name": str(row["name"]),
            "slug": str(row["slug"]),
            "color": str(row["color"] or ""),
            "created_at": str(row["created_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
            "rules": rules_by_label.get(int(row["id"]), []),
        }
        for row in label_rows
    ]


def find_label(labels: Iterable[dict[str, Any]], ref: object) -> dict[str, Any] | None:
    text = str(ref or "").strip().lower()
    if not text:
        return None
    for label in labels:
        if str(label["id"]) == text or label["slug"] == text:
            return label
    return None


def get_label(connection: sqlite3.Connection, ref: object) -> dict[str, Any] | None:
    return find_label(list_labels(connection), ref)


def has_labels(connection: sqlite3.Connection) -> bool:
    try:
        return connection.execute("SELECT 1 FROM mail_labels LIMIT 1").fetchone() is not None
    except sqlite3.OperationalError:
        return False


def state_digest(connection: sqlite3.Connection) -> str:
    """Fingerprint label definitions and manual choices for mailbox refresh."""
    try:
        labels = connection.execute("SELECT id, name, slug, color, sort_order FROM mail_labels ORDER BY id").fetchall()
        rules = connection.execute("SELECT id, label_id, kind, value FROM mail_label_rules ORDER BY id").fetchall()
        actions = connection.execute(
            "SELECT label_id, anchor, action, updated_at FROM mail_label_conversations ORDER BY label_id, anchor"
        ).fetchall()
    except sqlite3.OperationalError:
        return ""
    if not labels:
        return ""
    payload = [[list(row) for row in labels], [list(row) for row in rules], [list(row) for row in actions]]
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()[:16]


def _begin(connection: sqlite3.Connection) -> None:
    # Serialize read/modify/write even on the plaintext live runtime database.
    if not connection.in_transaction:
        connection.execute("BEGIN IMMEDIATE")


def _require_label(connection: sqlite3.Connection, ref: object) -> dict[str, Any]:
    label = get_label(connection, ref)
    if label is None:
        raise LookupError("That label no longer exists.")
    return label


def _touch_label(connection: sqlite3.Connection, label_id: int) -> None:
    connection.execute("UPDATE mail_labels SET updated_at = ? WHERE id = ?", (store._utc_now_string(), label_id))


def create_label(connection: sqlite3.Connection, name: object, rules: Iterable[object] = ()) -> dict[str, Any]:
    clean_name = normalize_label_name(name)
    parsed_rules = list(dict.fromkeys(parse_rule(rule) for rule in rules if str(rule or "").strip()))
    if len(parsed_rules) > MAX_RULES_PER_LABEL:
        raise ValueError(f"A label can have up to {MAX_RULES_PER_LABEL} rules.")
    slug = label_slug(clean_name)
    _begin(connection)
    try:
        count = int(connection.execute("SELECT COUNT(*) FROM mail_labels").fetchone()[0])
        if count >= MAX_LABELS:
            raise ValueError(f"CMAIL keeps up to {MAX_LABELS} labels.")
        if connection.execute("SELECT 1 FROM mail_labels WHERE slug = ?", (slug,)).fetchone():
            raise ValueError(f"A label named {clean_name} already exists.")
        now = store._utc_now_string()
        cursor = connection.execute(
            "INSERT INTO mail_labels (name, slug, color, sort_order, created_at, updated_at) VALUES (?, ?, '', ?, ?, ?)",
            (clean_name, slug, count, now, now),
        )
        label_id = int(cursor.lastrowid)
        for kind, value in parsed_rules:
            connection.execute(
                "INSERT OR IGNORE INTO mail_label_rules (label_id, kind, value, created_at) VALUES (?, ?, ?, ?)",
                (label_id, kind, value, now),
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return _require_label(connection, label_id)


def rename_label(connection: sqlite3.Connection, ref: object, name: object) -> dict[str, Any]:
    clean_name = normalize_label_name(name)
    slug = label_slug(clean_name)
    _begin(connection)
    try:
        label = _require_label(connection, ref)
        duplicate = connection.execute(
            "SELECT 1 FROM mail_labels WHERE slug = ? AND id != ?", (slug, label["id"])
        ).fetchone()
        if duplicate:
            raise ValueError(f"A label named {clean_name} already exists.")
        connection.execute(
            "UPDATE mail_labels SET name = ?, slug = ?, updated_at = ? WHERE id = ?",
            (clean_name, slug, store._utc_now_string(), label["id"]),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return _require_label(connection, label["id"])


def delete_label(connection: sqlite3.Connection, ref: object) -> bool:
    _begin(connection)
    try:
        label = get_label(connection, ref)
        if label is None:
            connection.rollback()
            return False
        connection.execute("DELETE FROM mail_label_rules WHERE label_id = ?", (label["id"],))
        connection.execute("DELETE FROM mail_label_conversations WHERE label_id = ?", (label["id"],))
        connection.execute("DELETE FROM mail_labels WHERE id = ?", (label["id"],))
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return True


def add_rule(connection: sqlite3.Connection, ref: object, value: object) -> dict[str, Any]:
    kind, clean_value = parse_rule(value)
    _begin(connection)
    try:
        label = _require_label(connection, ref)
        existing = connection.execute(
            "SELECT id, label_id, kind, value, created_at FROM mail_label_rules WHERE label_id = ? AND kind = ? AND value = ?",
            (label["id"], kind, clean_value),
        ).fetchone()
        if existing is None:
            if len(label["rules"]) >= MAX_RULES_PER_LABEL:
                raise ValueError(f"A label can have up to {MAX_RULES_PER_LABEL} rules.")
            connection.execute(
                "INSERT INTO mail_label_rules (label_id, kind, value, created_at) VALUES (?, ?, ?, ?)",
                (label["id"], kind, clean_value, store._utc_now_string()),
            )
            _touch_label(connection, label["id"])
            existing = connection.execute(
                "SELECT id, label_id, kind, value, created_at FROM mail_label_rules WHERE label_id = ? AND kind = ? AND value = ?",
                (label["id"], kind, clean_value),
            ).fetchone()
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return _rule_record(existing)


def remove_rule(connection: sqlite3.Connection, ref: object, rule_id: object) -> bool:
    _begin(connection)
    try:
        label = _require_label(connection, ref)
        cursor = connection.execute(
            "DELETE FROM mail_label_rules WHERE id = ? AND label_id = ?", (int(rule_id), label["id"])
        )
        if cursor.rowcount:
            _touch_label(connection, label["id"])
        connection.commit()
    except (TypeError, ValueError):
        connection.rollback()
        raise ValueError("Choose a rule to remove.")
    except Exception:
        connection.rollback()
        raise
    return bool(cursor.rowcount)


def message_token(value: object) -> str:
    return str(value or "").strip().strip("<>").strip().lower()


def conversation_anchor(row: Any) -> str:
    token = message_token(_value(row, "message_id"))
    return token or f"communication:{int(_value(row, 'id') or 0)}"


def set_conversation(
    connection: sqlite3.Connection,
    ref: object,
    *,
    communication_id: object,
    action: object,
) -> dict[str, Any]:
    clean_action = str(action or "").strip().lower()
    if clean_action not in CONVERSATION_ACTIONS:
        raise ValueError("Choose add or remove.")
    try:
        clean_id = int(communication_id)
    except (TypeError, ValueError):
        raise ValueError("Choose a message in the conversation.")
    _begin(connection)
    try:
        label = _require_label(connection, ref)
        row = connection.execute(
            "SELECT id, message_id FROM communications WHERE id = ? AND channel = 'email'", (clean_id,)
        ).fetchone()
        if row is None:
            raise LookupError("That message is no longer in CMAIL.")
        anchor = conversation_anchor(row)
        # Sub-second precision keeps "latest choice wins" exact for quick successive clicks.
        now = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
        connection.execute(
            """
            INSERT INTO mail_label_conversations (label_id, anchor, action, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(label_id, anchor) DO UPDATE SET
                action = excluded.action,
                updated_at = excluded.updated_at
            """,
            (label["id"], anchor, clean_action, now, now),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return {"label_id": label["id"], "anchor": anchor, "action": clean_action}


def _value(row: Any, key: str) -> Any:
    try:
        return row[key]
    except (IndexError, KeyError):
        return None


class _Conversations:
    """Union-find over messages and the Message-ID tokens that link them."""

    def __init__(self) -> None:
        self._parent: dict[str, str] = {}

    def __contains__(self, node: str) -> bool:
        return node in self._parent

    def find(self, node: str) -> str:
        parent = self._parent
        parent.setdefault(node, node)
        root = node
        while parent[root] != root:
            root = parent[root]
        while parent[node] != root:
            parent[node], node = root, parent[node]
        return root

    def union(self, left: str, right: str) -> None:
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root != right_root:
            self._parent[left_root] = right_root


def _reference_tokens(record: dict[str, Any]) -> list[str]:
    raw = record.get("references_json") or "[]"
    try:
        values = json.loads(raw) if isinstance(raw, str) else list(raw)
    except (TypeError, ValueError):
        values = []
    return [str(value) for value in values if value] if isinstance(values, list) else []


def _link_tokens(record: dict[str, Any]) -> list[str]:
    values = [record.get("message_id"), record.get("in_reply_to"), *_reference_tokens(record)]
    thread_key = str(record.get("thread_key") or "")
    if "@" in thread_key:
        values.append(thread_key)
    return [token for token in (message_token(value) for value in values) if token]


def _addresses(value: Any) -> list[str]:
    text = str(value or "")
    if "@" not in text:
        return []
    return [address.strip().lower() for _, address in getaddresses([text]) if "@" in address]


def _own_addresses() -> set[str]:
    return {identity.address.lower() for identity in mail_identities.MAIL_IDENTITIES.values() if identity.address}


@dataclass(frozen=True)
class _Rule:
    kind: str
    value: str
    literal: str
    pattern: re.Pattern[str] | None = None


def _compile_rule(rule: dict[str, Any]) -> _Rule:
    kind = str(rule["kind"])
    value = str(rule["value"])
    if kind != "keyword":
        return _Rule(kind=kind, value=value, literal=value)
    prefix = value.endswith("*")
    core = value[:-1] if prefix else value
    words = core.split()
    body = r"\s+".join(re.escape(word) for word in words)
    pattern = re.compile(r"(?<!\w)" + body + ("" if prefix else r"(?!\w)"))
    return _Rule(kind=kind, value=value, literal=max(words, key=len), pattern=pattern)


def _suggestion_pattern(name: str) -> re.Pattern[str] | None:
    words = name.lower().split()
    if len("".join(words)) < MIN_SUGGESTION_NAME_CHARS:
        return None
    return re.compile(r"(?<!\w)" + r"\s+".join(re.escape(word) for word in words) + r"(?!\w)")


class _Record:
    """Lazily normalized text for one correspondence row."""

    __slots__ = ("data", "_address_blob", "_participants", "_haystack", "_subject")

    def __init__(self, data: dict[str, Any]) -> None:
        self.data = data
        self._address_blob: str | None = None
        self._participants: list[tuple[str, str]] | None = None
        self._haystack: str | None = None
        self._subject: str | None = None

    @property
    def id(self) -> int:
        return int(self.data["id"])

    @property
    def address_blob(self) -> str:
        if self._address_blob is None:
            self._address_blob = " ".join(str(self.data.get(key) or "") for key, _ in _ADDRESS_FIELDS).lower()
        return self._address_blob

    @property
    def participants(self) -> list[tuple[str, str]]:
        if self._participants is None:
            self._participants = [
                (label, address)
                for key, label in _ADDRESS_FIELDS
                for address in _addresses(self.data.get(key))
            ]
        return self._participants

    @property
    def haystack(self) -> str:
        if self._haystack is None:
            self._haystack = " ".join(str(self.data.get(key) or "") for key in _KEYWORD_FIELDS).lower()
        return self._haystack

    @property
    def subject(self) -> str:
        if self._subject is None:
            self._subject = str(self.data.get("subject") or "").lower()
        return self._subject


def _match_reason(record: _Record, rules: list[_Rule], own_addresses: set[str]) -> str:
    for rule in rules:
        if rule.kind == "address":
            if rule.value not in record.address_blob:
                continue
            for field_label, address in record.participants:
                if address == rule.value:
                    return f"{field_label} {address}"
        elif rule.kind == "domain":
            if f"@{rule.value}" not in record.address_blob and f".{rule.value}" not in record.address_blob:
                continue
            for _, address in record.participants:
                if address in own_addresses:
                    continue
                domain = address.rsplit("@", 1)[-1]
                if domain == rule.value or domain.endswith(f".{rule.value}"):
                    return f"Correspondent at @{rule.value}"
    for rule in rules:
        if rule.kind == "keyword" and rule.literal in record.haystack and rule.pattern.search(record.haystack):
            return f"Mentions “{rule.value}”"
    return ""


@dataclass
class LabelEvaluation:
    labels: list[dict[str, Any]]
    conversation: dict[int, str] = field(default_factory=dict)
    message_labels: dict[int, dict[int, str]] = field(default_factory=dict)
    suggested: dict[int, set[str]] = field(default_factory=dict)

    def label(self, ref: object) -> dict[str, Any] | None:
        return find_label(self.labels, ref)


def _conversation_actions(connection: sqlite3.Connection) -> dict[int, list[tuple[str, str, str]]]:
    try:
        rows = connection.execute(
            "SELECT label_id, anchor, action, updated_at FROM mail_label_conversations ORDER BY updated_at, id"
        ).fetchall()
    except sqlite3.OperationalError:
        return {}
    actions: dict[int, list[tuple[str, str, str]]] = {}
    for row in rows:
        actions.setdefault(int(row["label_id"]), []).append(
            (str(row["anchor"]), str(row["action"]), str(row["updated_at"] or ""))
        )
    return actions


def evaluate(connection: sqlite3.Connection, rows: Iterable[Any]) -> LabelEvaluation | None:
    """Label every correspondence row; None when no labels exist."""
    labels = list_labels(connection)
    if not labels:
        return None
    records = [_Record(dict(row)) for row in rows]
    graph = _Conversations()
    for record in records:
        node = f"row:{record.id}"
        graph.find(node)
        for token in _link_tokens(record.data):
            graph.union(node, f"mid:{token}")
    evaluation = LabelEvaluation(labels=labels)
    for record in records:
        evaluation.conversation[record.id] = graph.find(f"row:{record.id}")
    actions = _conversation_actions(connection)
    own_addresses = _own_addresses()

    for label in labels:
        label_id = int(label["id"])
        rules = [_compile_rule(rule) for rule in label["rules"]]
        direct: dict[int, str] = {}
        if rules:
            for record in records:
                reason = _match_reason(record, rules, own_addresses)
                if reason:
                    direct[record.id] = reason
        matched = {evaluation.conversation[record_id] for record_id in direct}

        # Later choices win within a conversation; unknown anchors are ignored.
        manual: dict[str, str] = {}
        for anchor, action, _ in actions.get(label_id, []):
            node = f"row:{anchor.split(':', 1)[1]}" if anchor.startswith("communication:") else f"mid:{anchor}"
            if node in graph:
                manual[graph.find(node)] = action
        added = {root for root, action in manual.items() if action == "add"}
        removed = {root for root, action in manual.items() if action == "remove"}
        labeled = (matched | added) - removed

        for record in records:
            root = evaluation.conversation[record.id]
            if root not in labeled:
                continue
            reason = direct.get(record.id) or (REASON_ADDED if root not in matched else REASON_CONVERSATION)
            evaluation.message_labels.setdefault(record.id, {})[label_id] = reason

        pattern = _suggestion_pattern(label["name"])
        suggested: set[str] = set()
        if pattern is not None:
            for record in records:
                root = evaluation.conversation[record.id]
                if root in labeled or root in removed or root in suggested:
                    continue
                if pattern.search(record.subject):
                    suggested.add(root)
        evaluation.suggested[label_id] = suggested
    return evaluation


def annotate_messages(messages: Iterable[dict[str, Any]], evaluation: LabelEvaluation | None) -> None:
    if evaluation is None:
        return
    for message in messages:
        labels = evaluation.message_labels.get(int(message["id"]), {})
        message["label_ids"] = sorted(labels)
        message["label_reasons"] = {str(label_id): reason for label_id, reason in labels.items()}


def summarize(
    evaluation: LabelEvaluation | None,
    messages: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Per-label counts for the messages visible in the current inbox scope."""
    if evaluation is None:
        return []
    scoped = list(messages)
    contacts: dict[int, set[str]] = {}
    conversations: dict[int, set[str]] = {}
    message_counts: dict[int, int] = {}
    scoped_roots: set[str] = set()
    for message in scoped:
        message_id = int(message["id"])
        root = evaluation.conversation.get(message_id, f"row:{message_id}")
        scoped_roots.add(root)
        for label_id in evaluation.message_labels.get(message_id, {}):
            contacts.setdefault(label_id, set()).add(str(message.get("contact_key") or ""))
            conversations.setdefault(label_id, set()).add(root)
            message_counts[label_id] = message_counts.get(label_id, 0) + 1
    summary = []
    for label in evaluation.labels:
        label_id = int(label["id"])
        summary.append(
            {
                "id": label_id,
                "name": label["name"],
                "slug": label["slug"],
                "color": label["color"],
                "contact_count": len(contacts.get(label_id, ())),
                "conversation_count": len(conversations.get(label_id, ())),
                "message_count": message_counts.get(label_id, 0),
                "suggestion_count": len(evaluation.suggested.get(label_id, set()) & scoped_roots),
            }
        )
    return summary


def suggestions(
    evaluation: LabelEvaluation | None,
    messages: Iterable[dict[str, Any]],
    label_id: int,
    *,
    limit: int = MAX_SUGGESTIONS,
) -> list[dict[str, Any]]:
    """Conversations whose subject names the label but that no rule matched."""
    if evaluation is None:
        return []
    roots = evaluation.suggested.get(int(label_id), set())
    grouped: dict[str, list[dict[str, Any]]] = {}
    for message in messages:
        root = evaluation.conversation.get(int(message["id"]))
        if root in roots:
            grouped.setdefault(root, []).append(message)
    results = []
    for conversation_messages in grouped.values():
        latest = max(conversation_messages, key=lambda item: (str(item.get("happened_at") or ""), int(item["id"])))
        results.append(
            {
                "communication_id": int(latest["id"]),
                "contact_key": str(latest.get("contact_key") or ""),
                "contact_label": str(latest.get("contact_label") or ""),
                "subject": str(latest.get("subject") or ""),
                "happened_at": str(latest.get("happened_at") or ""),
                "mail_identity_short_label": str(latest.get("mail_identity_short_label") or ""),
                "message_count": len(conversation_messages),
            }
        )
    results.sort(key=lambda item: (item["happened_at"], item["communication_id"]), reverse=True)
    return results[: max(0, int(limit))]
