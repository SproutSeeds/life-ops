from __future__ import annotations

import re
from email.message import Message
from email.utils import getaddresses
from typing import Any, Iterable

MESSAGE_ID_PATTERN = re.compile(r"<[^>\s]+>")
OPERATIONAL_HEADER_ALLOWLIST = {
    "auto-submitted",
    "bcc",
    "cc",
    "content-type",
    "date",
    "from",
    "in-reply-to",
    "list-id",
    "list-unsubscribe",
    "message-id",
    "mime-version",
    "precedence",
    "reply-to",
    "references",
    "subject",
    "to",
    "x-priority",
}


def strip_string(value: Any) -> str:
    return str(value or "").strip()


def parse_address_values(values: str | Iterable[str] | None) -> list[dict[str, str]]:
    if values is None:
        return []
    if isinstance(values, str):
        candidates = [values]
    else:
        candidates = [strip_string(item) for item in values if strip_string(item)]

    results: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for name, email in getaddresses(candidates):
        clean_name = strip_string(name)
        clean_email = strip_string(email)
        if not clean_name and not clean_email:
            continue
        key = (clean_name, clean_email.lower())
        if key in seen:
            continue
        seen.add(key)
        results.append({"name": clean_name, "email": clean_email})
    return results


def format_addresses(addresses: list[dict[str, str]] | None) -> str:
    values: list[str] = []
    for address in addresses or []:
        name = strip_string(address.get("name"))
        email = strip_string(address.get("email"))
        if name and email:
            values.append(f"{name} <{email}>")
        elif email:
            values.append(email)
        elif name:
            values.append(name)
    return ", ".join(values)


def message_id_tokens(values: str | Iterable[str] | None) -> list[str]:
    if values is None:
        return []
    if isinstance(values, str):
        raw_values = [values]
    else:
        raw_values = [strip_string(item) for item in values if strip_string(item)]

    tokens: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        for token in MESSAGE_ID_PATTERN.findall(raw):
            if token not in seen:
                seen.add(token)
                tokens.append(token)
    if tokens:
        return tokens

    for raw in raw_values:
        for part in re.split(r"[\s,]+", raw):
            clean = strip_string(part)
            if clean and clean not in seen:
                seen.add(clean)
                tokens.append(clean)
    return tokens


def primary_message_id(value: str | Iterable[str] | None) -> str:
    tokens = message_id_tokens(value)
    return tokens[0] if tokens else ""


def derive_thread_key(
    *,
    message_id: str = "",
    in_reply_to: str = "",
    references: Iterable[str] | None = None,
    fallback: str = "",
) -> str:
    reference_tokens = message_id_tokens(list(references or []))
    if reference_tokens:
        return reference_tokens[0]
    clean_in_reply_to = strip_string(in_reply_to)
    if clean_in_reply_to:
        return clean_in_reply_to
    clean_message_id = strip_string(message_id)
    if clean_message_id:
        return clean_message_id
    return strip_string(fallback)


def headers_snapshot(message: Message, *, forensic: bool = False) -> dict[str, list[str]]:
    headers: dict[str, list[str]] = {}
    for name, value in message.raw_items():
        clean_name = strip_string(name)
        clean_value = strip_string(value)
        if not clean_name or not clean_value:
            continue
        if not forensic and clean_name.lower() not in OPERATIONAL_HEADER_ALLOWLIST:
            continue
        headers.setdefault(clean_name, []).append(clean_value)
    return headers
