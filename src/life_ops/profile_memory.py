from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional

from life_ops import store

SUBJECT_DEFAULTS = {
    "self": {
        "display_name": "Self",
        "relationship": "self",
    },
    "wife_sisy": {
        "display_name": "Sisy",
        "relationship": "spouse",
    },
}

RECORD_KIND_BY_ITEM_TYPE = {
    "identity_document": "document",
    "insurance_record": "policy",
    "immigration_record": "case",
    "benefits_record": "case",
    "medical_record": "record",
    "tax_record": "record",
}

ALERT_RULES = {
    "immigration_record": {
        "level": "high",
        "max_age_days": 365,
        "keywords": {
            "biometrics",
            "interview notice",
            "notice of action",
            "priority date",
            "receipt notice",
            "uscis",
            "visa interview",
        },
        "message": "Recent immigration movement deserves a visible follow-up trail.",
    },
    "benefits_record": {
        "level": "high",
        "max_age_days": 270,
        "keywords": {
            "benefit renewal",
            "case number",
            "eligibility review",
            "medicaid",
            "recertification",
            "snap",
            "ssi",
            "ssdi",
        },
        "message": "Benefits records should stay easy to reach for renewal and recertification work.",
    },
    "tax_record": {
        "level": "medium",
        "max_age_days": 365,
        "keywords": {
            "1040",
            "1099",
            "irs",
            "tax return",
            "tax transcript",
            "w 2",
            "w2",
        },
        "message": "Recent tax records should stay packaged and reachable for filing or verification.",
    },
    "insurance_record": {
        "level": "medium",
        "max_age_days": 365,
        "keywords": {
            "dependent update",
            "insurance card",
            "member id",
            "policy documents",
            "policy number",
            "proof of insurance",
            "subscriber id",
        },
        "message": "Insurance records should stay linked to the current card, member ID, and coverage proof.",
    },
    "identity_document": {
        "level": "medium",
        "max_age_days": 730,
        "keywords": {
            "birth certificate",
            "driver license",
            "green card",
            "id card",
            "passport",
            "social security card",
            "state id",
        },
        "message": "Identity records should stay linked to an actual attachment so the canonical copy is usable.",
    },
}


def _json_field(value: str, fallback):
    if not value:
        return fallback
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return fallback


def _subject_defaults(subject_key: str) -> dict[str, str]:
    defaults = SUBJECT_DEFAULTS.get(subject_key, {})
    return {
        "display_name": str(defaults.get("display_name") or subject_key.replace("_", " ").title()),
        "relationship": str(defaults.get("relationship") or "related_person"),
    }


def _record_kind(item_type: str) -> str:
    return RECORD_KIND_BY_ITEM_TYPE.get(item_type, "record")


def _record_details_from_item(row) -> dict[str, Any]:
    details = _json_field(str(row["details_json"] or "{}"), {})
    details["source_profile_item_ids"] = [int(row["id"])]
    return details


def _record_evidence_from_item(row) -> list[dict[str, Any]]:
    return _json_field(str(row["evidence_json"] or "[]"), [])


def _merge_string_lists(existing: list[str], incoming: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in existing + incoming:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def _merge_evidence(existing: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for record in existing + incoming:
        kind = str(record.get("kind") or "").strip()
        text = str(record.get("text") or "").strip()
        key = (kind, text)
        if not kind or not text or key in seen:
            continue
        seen.add(key)
        merged.append({"kind": kind, "text": text})
    return merged


def _merge_details(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    for key, value in incoming.items():
        if isinstance(value, list):
            merged[key] = _merge_string_lists(
                [str(item) for item in merged.get(key, [])],
                [str(item) for item in value],
            )
        elif value not in (None, "", []):
            merged[key] = value
    merged["source_profile_item_ids"] = _merge_string_lists(
        [str(item) for item in merged.get("source_profile_item_ids", [])],
        [str(item) for item in incoming.get("source_profile_item_ids", [])],
    )
    return merged


def get_profile_record_payload(connection, record_id: int) -> dict[str, Any]:
    row = store.get_profile_record(connection, record_id)
    if row is None:
        raise ValueError(f"profile record #{record_id} was not found")

    items = store.list_profile_record_items(connection, record_id=record_id)
    attachments = store.list_profile_record_attachments(connection, record_id=record_id)
    return {
        "id": int(row["id"]),
        "subject_key": str(row["subject_key"]),
        "display_name": str(row["display_name"] or ""),
        "relationship": str(row["relationship"] or ""),
        "item_type": str(row["item_type"]),
        "record_kind": str(row["record_kind"]),
        "title": str(row["title"]),
        "status": str(row["status"]),
        "source": str(row["source"]),
        "happened_at": str(row["happened_at"]),
        "confidence": int(row["confidence"] or 0),
        "notes": str(row["notes"] or ""),
        "details": _json_field(str(row["details_json"] or "{}"), {}),
        "evidence": _json_field(str(row["evidence_json"] or "[]"), []),
        "linked_profile_items": [
            {
                "id": int(item["id"]),
                "item_type": str(item["item_type"]),
                "title": str(item["title"]),
                "status": str(item["status"]),
                "confidence": int(item["confidence"] or 0),
                "happened_at": str(item["happened_at"]),
            }
            for item in items
        ],
        "linked_attachments": [
            {
                "id": int(attachment["id"]),
                "filename": str(attachment["filename"] or ""),
                "mime_type": str(attachment["mime_type"] or ""),
                "relative_path": str(attachment["relative_path"] or ""),
                "ingest_status": str(attachment["ingest_status"] or ""),
            }
            for attachment in attachments
        ],
    }


def list_profile_alerts(
    connection,
    *,
    subject_key: Optional[str] = None,
    item_type: Optional[str] = None,
    status: str = "active",
    limit: Optional[int] = 50,
) -> list[dict[str, Any]]:
    rows = store.list_profile_records(
        connection,
        subject_key=subject_key,
        item_type=item_type,
        status=status,
        limit=None,
    )
    now = datetime.now()
    alerts: list[dict[str, Any]] = []

    for row in rows:
        record_id = int(row["id"])
        details = _json_field(str(row["details_json"] or "{}"), {})
        attachment_rows = store.list_profile_record_attachments(connection, record_id=record_id)
        happened_at = store.parse_datetime(str(row["happened_at"]))
        age_days = max(0, (now - happened_at).days)
        record_item_type = str(row["item_type"])
        linked_terms = set()
        for key in (
            "matched_strong_terms",
            "matched_document_strong_terms",
            "matched_attachment_terms",
            "matched_context_terms",
            "matched_document_context_terms",
        ):
            linked_terms.update(str(term) for term in details.get(key, []))

        if not attachment_rows and record_item_type in ALERT_RULES:
            alerts.append(
                {
                    "record_id": record_id,
                    "subject_key": str(row["subject_key"]),
                    "item_type": record_item_type,
                    "title": str(row["title"]),
                    "level": "medium",
                    "reason": "No extracted attachment is linked yet, so the canonical record is harder to use.",
                    "age_days": age_days,
                }
            )

        rule = ALERT_RULES.get(record_item_type)
        if not rule:
            continue
        matched_keywords = sorted(term for term in linked_terms if term in rule["keywords"])
        if age_days <= int(rule["max_age_days"]) or matched_keywords:
            reason = str(rule["message"])
            if matched_keywords:
                reason += " Matched terms: " + ", ".join(matched_keywords[:4]) + "."
            alerts.append(
                {
                    "record_id": record_id,
                    "subject_key": str(row["subject_key"]),
                    "item_type": record_item_type,
                    "title": str(row["title"]),
                    "level": str(rule["level"]),
                    "reason": reason,
                    "age_days": age_days,
                }
            )

    level_order = {"high": 0, "medium": 1, "low": 2}
    alerts.sort(key=lambda alert: (level_order.get(str(alert["level"]), 9), int(alert["age_days"]), str(alert["title"])))
    if limit is None:
        return alerts
    return alerts[:limit]


def approve_profile_context_item(
    connection,
    *,
    item_id: int,
    title: str = "",
    record_status: str = "active",
    notes: str = "",
) -> dict[str, Any]:
    row = store.get_profile_context_item(connection, item_id)
    if row is None:
        raise ValueError(f"profile context item #{item_id} was not found")

    subject_key = str(row["subject_key"])
    subject_defaults = _subject_defaults(subject_key)
    subject_id = store.ensure_profile_subject(
        connection,
        subject_key=subject_key,
        display_name=subject_defaults["display_name"],
        relationship=subject_defaults["relationship"],
    )

    record_id = store.create_profile_record(
        connection,
        subject_id=subject_id,
        item_type=str(row["item_type"]),
        record_kind=_record_kind(str(row["item_type"])),
        title=title or str(row["title"]),
        status=record_status,
        source=str(row["source"]),
        happened_at=store.parse_datetime(str(row["happened_at"])),
        confidence=int(row["confidence"] or 0),
        notes=notes,
        details=_record_details_from_item(row),
        evidence=_record_evidence_from_item(row),
    )

    store.link_profile_record_item(connection, record_id=record_id, profile_item_id=item_id)
    if row["communication_id"] is not None:
        attachment_rows = store.list_communication_attachments(
            connection,
            communication_id=int(row["communication_id"]),
            limit=None,
        )
        for attachment in attachment_rows:
            store.link_profile_record_attachment(connection, record_id=record_id, attachment_id=int(attachment["id"]))

    review_notes = notes or f"Promoted to profile record #{record_id}."
    store.update_profile_context_item_status(
        connection,
        item_id=item_id,
        status="approved",
        review_notes=review_notes,
    )
    return get_profile_record_payload(connection, record_id)


def merge_profile_context_item(
    connection,
    *,
    item_id: int,
    record_id: int,
    notes: str = "",
) -> dict[str, Any]:
    row = store.get_profile_context_item(connection, item_id)
    if row is None:
        raise ValueError(f"profile context item #{item_id} was not found")

    record = store.get_profile_record(connection, record_id)
    if record is None:
        raise ValueError(f"profile record #{record_id} was not found")
    if str(record["subject_key"]) != str(row["subject_key"]):
        raise ValueError("profile item subject does not match the target record subject")
    if str(record["item_type"]) != str(row["item_type"]):
        raise ValueError("profile item type does not match the target record type")

    existing_details = _json_field(str(record["details_json"] or "{}"), {})
    existing_evidence = _json_field(str(record["evidence_json"] or "[]"), [])
    merged_details = _merge_details(existing_details, _record_details_from_item(row))
    merged_evidence = _merge_evidence(existing_evidence, _record_evidence_from_item(row))
    existing_notes = str(record["notes"] or "").strip()
    merged_notes = existing_notes
    if notes.strip():
        merged_notes = notes.strip() if not existing_notes else f"{existing_notes}\n{notes.strip()}"

    store.update_profile_record(
        connection,
        record_id=record_id,
        title=str(record["title"]),
        status=str(record["status"]),
        confidence=max(int(record["confidence"] or 0), int(row["confidence"] or 0)),
        notes=merged_notes,
        details=merged_details,
        evidence=merged_evidence,
    )
    store.link_profile_record_item(connection, record_id=record_id, profile_item_id=item_id)
    if row["communication_id"] is not None:
        attachment_rows = store.list_communication_attachments(
            connection,
            communication_id=int(row["communication_id"]),
            limit=None,
        )
        for attachment in attachment_rows:
            store.link_profile_record_attachment(connection, record_id=record_id, attachment_id=int(attachment["id"]))

    review_notes = notes or f"Merged into profile record #{record_id}."
    store.update_profile_context_item_status(
        connection,
        item_id=item_id,
        status="approved",
        review_notes=review_notes,
    )
    return get_profile_record_payload(connection, record_id)


def reject_profile_context_item(connection, *, item_id: int, notes: str = "") -> dict[str, Any]:
    row = store.get_profile_context_item(connection, item_id)
    if row is None:
        raise ValueError(f"profile context item #{item_id} was not found")
    store.update_profile_context_item_status(
        connection,
        item_id=item_id,
        status="rejected",
        review_notes=notes,
    )
    return {
        "id": int(row["id"]),
        "item_type": str(row["item_type"]),
        "title": str(row["title"]),
        "status": "rejected",
        "review_notes": notes,
    }
