from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from email.utils import parseaddr
from typing import Any, Optional

from life_ops import store
from life_ops import tracing

PROFILE_SUBJECT_NAME_ALIASES = {
    "wife_sisy": {
        "sisy",
        "wife sisy",
        "my wife sisy",
    },
}

PROFILE_ITEM_RULES = [
    {
        "item_type": "identity_document",
        "label": "Identity document",
        "categories": {"identity"},
        "strong_terms": {
            "birth certificate",
            "driver license",
            "driver s license",
            "driver permit",
            "green card",
            "id card",
            "id photo",
            "identification card",
            "passport",
            "passport card",
            "permanent resident card",
            "social security card",
            "state id",
        },
        "context_terms": {
            "copy of",
            "driver s license",
            "identification",
            "keep this",
            "record keeping",
        },
        "attachment_terms": {
            "birth_certificate",
            "drivers_license",
            "green_card",
            "id_card",
            "id_front",
            "id_back",
            "license",
            "passport",
            "resident_card",
            "ssn",
            "social_security",
            "state_id",
        },
        "trusted_domains": set(),
        "excluded_terms": {
            "everything photo",
            "passport photo",
            "photo cards",
            "photo prints",
            "same day canvas",
            "walgreens photo",
        },
        "min_score": 40,
    },
    {
        "item_type": "insurance_record",
        "label": "Insurance record",
        "categories": {"insurance"},
        "strong_terms": {
            "claim number",
            "confirmation of coverage",
            "dependent update",
            "eob",
            "explanation of benefits",
            "id cards",
            "insurance card",
            "insurance id",
            "member id",
            "policy number",
            "policy documents",
            "prior authorization",
            "proof of insurance",
            "subscriber id",
        },
        "context_terms": {
            "coverage",
            "insurance",
            "policy",
        },
        "attachment_terms": {
            "ambetter",
            "eob",
            "insurance_card",
            "member_id",
            "policy",
            "proof_of_insurance",
            "subscriber_id",
        },
        "trusted_domains": {
            "allianzassistance.com",
            "ambetterhealth.softheon.com",
            "mymemberinfo.com",
            "progressive.com",
            "softheon.com",
        },
        "excluded_terms": {
            "apply now",
            "get a quote",
            "is hiring",
            "payment has been processed",
            "privacy policy",
            "reasons to buy life insurance",
        },
        "min_score": 45,
    },
    {
        "item_type": "immigration_record",
        "label": "Immigration record",
        "categories": {"immigration"},
        "strong_terms": {
            "adjustment of status",
            "affidavit of support",
            "biometrics",
            "biometrics appointment",
            "case was received",
            "consular",
            "ds 160",
            "ds 260",
            "ead",
            "employment authorization",
            "form i 130",
            "form i 485",
            "form i 765",
            "form i 864",
            "green card",
            "immigrant visa",
            "interview notice",
            "nvc",
            "notice of action",
            "petition",
            "priority date",
            "receipt notice",
            "travel document",
            "uscis",
            "visa appointment",
            "visa application",
            "visa interview",
            "visa case",
            "work authorization",
        },
        "context_terms": {
            "immigration",
            "visa",
        },
        "attachment_terms": {
            "i130",
            "i485",
            "i765",
            "i864",
            "immigration",
            "uscis",
            "visa",
        },
        "trusted_domains": {
            "nvc.state.gov",
            "state.gov",
            "travel.state.gov",
            "uscis.dhs.gov",
            "uscis.gov",
        },
        "excluded_terms": {
            "amazon rewards visa",
            "best buy visa",
            "credit card",
            "minimum payment",
            "payment received",
            "payment scheduled",
            "statement is available",
            "visa card",
            "visa platinum",
        },
        "min_score": 50,
    },
    {
        "item_type": "benefits_record",
        "label": "Benefits record",
        "categories": {"benefits"},
        "strong_terms": {
            "benefits card",
            "benefit amount",
            "benefit renewal",
            "case number",
            "ebt",
            "eligibility review",
            "food stamps",
            "medicaid",
            "public benefits",
            "recertification",
            "snap",
            "social security administration",
            "social security benefits",
            "ssi",
            "ssdi",
            "unemployment claim",
        },
        "context_terms": {
            "benefits",
            "eligibility",
        },
        "attachment_terms": {
            "benefits",
            "medicaid",
            "snap",
            "ssi",
        },
        "trusted_domains": {
            "ssa.gov",
        },
        "excluded_terms": {
            "business loan",
            "loan eligibility",
        },
        "min_score": 45,
    },
    {
        "item_type": "medical_record",
        "label": "Medical record",
        "categories": {"medical"},
        "strong_terms": {
            "after visit summary",
            "appointment confirmation",
            "appointment reminder",
            "doctor visit",
            "lab result",
            "medical record",
            "patient portal",
            "prescription ready",
            "prescription refill",
            "prescription",
            "referral",
            "test result",
            "visit summary",
        },
        "context_terms": {
            "clinic",
            "doctor",
            "medical",
            "patient",
            "pharmacy",
        },
        "attachment_terms": {
            "after_visit_summary",
            "lab",
            "medical",
            "medical_record",
            "prescription",
            "referral",
            "summary",
            "visit",
        },
        "trusted_domains": {
            "covetruspharmacy.com",
            "marijuanadoctor.com",
            "patient-message.com",
            "practicemailer.com",
            "vetsource.com",
            "walgreens.com",
        },
        "excluded_terms": {
            "clearance",
            "deals",
            "everything photo",
            "loyalty offer",
            "weekly deals",
        },
        "min_score": 45,
    },
    {
        "item_type": "tax_record",
        "label": "Tax record",
        "categories": {"tax"},
        "strong_terms": {
            "1040",
            "1099",
            "irs",
            "return is ready",
            "tax return",
            "tax transcript",
            "w 2",
            "w2",
        },
        "context_terms": {
            "refund",
            "tax",
        },
        "attachment_terms": {
            "1040",
            "1099",
            "tax",
            "w2",
        },
        "trusted_domains": {
            "freetaxusa.com",
            "hrblock.com",
            "irs.gov",
            "turbotax.intuit.com",
        },
        "excluded_terms": {
            "amazon return",
            "return label",
        },
        "min_score": 45,
    },
]


def _normalize_text(*parts: str) -> str:
    value = " ".join(part for part in parts if part).lower()
    return re.sub(r"[^a-z0-9]+", " ", value).strip()


def _contains_phrase(text: str, phrase: str) -> bool:
    padded_text = f" {text} "
    padded_phrase = f" {phrase.strip()} "
    return padded_phrase in padded_text


def _sender_domain(sender: str) -> str:
    _, sender_email = parseaddr(sender)
    return sender_email.lower().partition("@")[2]


def _domain_matches(sender_domain: str, domains: set[str]) -> bool:
    return any(sender_domain == domain or sender_domain.endswith(f".{domain}") for domain in domains)


def _attachment_names(attachments: list[dict[str, Any]]) -> list[str]:
    return [
        str(attachment.get("filename", "")).strip()
        for attachment in attachments
        if attachment.get("filename")
        and not store.attachment_filename_is_low_signal(str(attachment.get("filename", "")))
    ]


def _attachment_text(attachments: list[dict[str, Any]]) -> str:
    return _normalize_text(
        " ".join(_attachment_names(attachments)),
        " ".join(str(attachment.get("mime_type", "")) for attachment in attachments),
    )


def _subject_key_for_row(
    rule: dict[str, Any],
    *,
    subject: str,
    sender: str,
    snippet: str,
    body_text: str,
    attachment_document_text: str,
    attachments: list[dict[str, Any]],
    matched_strong_terms: list[str],
    matched_document_strong_terms: list[str],
    matched_attachment_terms: list[str],
    matched_trusted_domain: bool,
) -> str:
    combined_text = _normalize_text(
        subject,
        sender,
        snippet,
        body_text,
        attachment_document_text,
        " ".join(_attachment_names(attachments)),
    )
    for subject_key, aliases in PROFILE_SUBJECT_NAME_ALIASES.items():
        if any(_contains_phrase(combined_text, alias) for alias in aliases):
            return subject_key
    return "self"


def _profile_title(rule: dict[str, Any], subject: str, attachments: list[dict[str, Any]]) -> str:
    attachment_names = _attachment_names(attachments)
    if attachment_names:
        return f"{rule['label']}: {attachment_names[0]}"
    if subject:
        return subject
    return rule["label"]


def _external_key(*, source: str, communication_id: int, subject_key: str, item_type: str, title: str) -> str:
    fingerprint = hashlib.sha1(
        f"{source}:{communication_id}:{subject_key}:{item_type}:{title}".encode("utf-8")
    ).hexdigest()[:16]
    return f"profile:{source}:{fingerprint}"


def extract_profile_candidates_from_communication(row, *, attachment_document_text: str = "") -> list[dict[str, Any]]:
    attachments = json.loads(str(row["attachments_json"] or "[]"))
    categories = set(json.loads(str(row["categories_json"] or "[]")))
    primary_category = str(row["category"] or "")
    if primary_category:
        categories.add(primary_category)

    subject = str(row["subject"] or "")
    sender = str(row["external_from"] or row["person"] or "")
    snippet = str(row["snippet"] or "")
    body_text = str(row["body_text"] or "")
    search_text = _normalize_text(subject, sender, snippet)
    attachment_text = _attachment_text(attachments)
    document_text = _normalize_text(attachment_document_text)
    sender_domain = _sender_domain(sender)

    candidates: list[dict[str, Any]] = []
    for rule in PROFILE_ITEM_RULES:
        matched_strong_terms = [term for term in sorted(rule["strong_terms"]) if _contains_phrase(search_text, term)]
        matched_context_terms = [term for term in sorted(rule["context_terms"]) if _contains_phrase(search_text, term)]
        matched_document_strong_terms = [term for term in sorted(rule["strong_terms"]) if _contains_phrase(document_text, term)]
        matched_document_context_terms = [term for term in sorted(rule["context_terms"]) if _contains_phrase(document_text, term)]
        matched_attachment_terms = [
            term for term in sorted(rule["attachment_terms"]) if _contains_phrase(attachment_text, term)
        ]
        matched_categories = sorted(category for category in rule["categories"] if category in categories)
        matched_trusted_domain = _domain_matches(sender_domain, rule["trusted_domains"])
        matched_excluded_terms = [term for term in sorted(rule["excluded_terms"]) if _contains_phrase(search_text, term)]

        if matched_excluded_terms and not matched_attachment_terms and not matched_trusted_domain:
            continue
        if not (
            matched_strong_terms
            or matched_document_strong_terms
            or matched_attachment_terms
            or matched_trusted_domain
        ):
            continue
        if (
            rule["item_type"] == "insurance_record"
            and matched_trusted_domain
            and not (
                matched_strong_terms
                or matched_document_strong_terms
                or matched_context_terms
                or matched_document_context_terms
                or matched_attachment_terms
            )
        ):
            continue

        score = 0
        if matched_trusted_domain:
            score += 35
        if matched_strong_terms:
            score += min(45, 18 * len(matched_strong_terms))
        if matched_document_strong_terms:
            score += min(36, 14 * len(matched_document_strong_terms))
        if matched_attachment_terms:
            score += min(30, 12 * len(matched_attachment_terms))
        if matched_categories:
            score += 10
        if matched_context_terms:
            score += min(18, 6 * len(matched_context_terms))
        if matched_document_context_terms and matched_document_strong_terms:
            score += min(12, 4 * len(matched_document_context_terms))
        if attachments and (
            matched_strong_terms
            or matched_document_strong_terms
            or matched_attachment_terms
            or matched_trusted_domain
        ):
            score += 10
        subject_key = _subject_key_for_row(
            rule=rule,
            subject=subject,
            sender=sender,
            snippet=snippet,
            body_text=body_text,
            attachment_document_text=attachment_document_text,
            attachments=attachments,
            matched_strong_terms=matched_strong_terms,
            matched_document_strong_terms=matched_document_strong_terms,
            matched_attachment_terms=matched_attachment_terms,
            matched_trusted_domain=matched_trusted_domain,
        )
        if subject_key != "self":
            score += 5

        if score < int(rule["min_score"]):
            continue

        title = _profile_title(rule, subject, attachments)
        communication_id = int(row["id"])
        happened_at = store.parse_datetime(str(row["happened_at"]))
        evidence = [
            {"kind": "subject", "text": subject},
            {"kind": "sender", "text": sender},
            {"kind": "snippet", "text": snippet[:280]},
        ]
        if attachment_document_text.strip():
            evidence.append({"kind": "attachment_text", "text": attachment_document_text[:280]})
        for attachment_name in _attachment_names(attachments)[:5]:
            evidence.append({"kind": "attachment", "text": attachment_name})

        candidates.append(
            {
                "external_key": _external_key(
                    source=str(row["source"]),
                    communication_id=communication_id,
                    subject_key=subject_key,
                    item_type=rule["item_type"],
                    title=title,
                ),
                "subject_key": subject_key,
                "item_type": rule["item_type"],
                "title": title,
                "source": str(row["source"]),
                "communication_id": communication_id,
                "happened_at": happened_at,
                "confidence": min(100, score),
                "details": {
                    "categories": sorted(categories),
                    "matched_categories": matched_categories,
                    "matched_strong_terms": matched_strong_terms,
                    "matched_context_terms": matched_context_terms,
                    "matched_document_strong_terms": matched_document_strong_terms,
                    "matched_document_context_terms": matched_document_context_terms,
                    "matched_attachment_terms": matched_attachment_terms,
                    "matched_trusted_domain": sender_domain if matched_trusted_domain else "",
                    "communication_subject": subject,
                    "communication_external_id": str(row["external_id"] or ""),
                },
                "evidence": evidence,
            }
        )

    return candidates


def extract_profile_context_items(
    connection,
    *,
    source: str = "gmail",
    status: str = "all",
    category: Optional[str] = None,
    limit: Optional[int] = None,
    replace_existing: bool = True,
) -> dict[str, Any]:
    trace_run_id = tracing.start_trace_run(
        connection,
        trace_type="profile_context_extract",
        metadata={
            "source": source,
            "status": status,
            "category": category,
            "limit": limit,
            "replace_existing": replace_existing,
        },
    )
    try:
        if replace_existing:
            deleted = store.replace_profile_context_items(connection, source=source, statuses=["candidate"])
        else:
            deleted = 0

        rows = store.list_communications(
            connection,
            source=source,
            status=status,
            category=category,
            limit=limit,
        )

        inserted = 0
        by_item_type: dict[str, int] = {}
        by_subject: dict[str, int] = {}

        for row in rows:
            attachment_document_text = store.combined_attachment_text(
                connection,
                communication_id=int(row["id"]),
            )
            for candidate in extract_profile_candidates_from_communication(
                row,
                attachment_document_text=attachment_document_text,
            ):
                store.upsert_profile_context_item(
                    connection,
                    external_key=candidate["external_key"],
                    subject_key=candidate["subject_key"],
                    item_type=candidate["item_type"],
                    title=candidate["title"],
                    source=candidate["source"],
                    communication_id=candidate["communication_id"],
                    happened_at=candidate["happened_at"],
                    confidence=candidate["confidence"],
                    status="candidate",
                    details=candidate["details"],
                    evidence=candidate["evidence"],
                )
                inserted += 1
                by_item_type[candidate["item_type"]] = by_item_type.get(candidate["item_type"], 0) + 1
                by_subject[candidate["subject_key"]] = by_subject.get(candidate["subject_key"], 0) + 1
                tracing.append_trace_event(
                    connection,
                    run_id=trace_run_id,
                    event_type="profile_context_item_extracted",
                    entity_key=candidate["external_key"],
                    payload={
                        "subject_key": candidate["subject_key"],
                        "item_type": candidate["item_type"],
                        "title": candidate["title"],
                        "confidence": candidate["confidence"],
                        "communication_id": candidate["communication_id"],
                    },
                )

        summary = store.summarize_profile_context(connection, source=source)
        result = {
            "source": source,
            "communications_scanned": len(rows),
            "items_extracted": inserted,
            "items_replaced": deleted,
            "by_item_type": by_item_type,
            "by_subject": by_subject,
            "profile_context_total": summary["total"],
        }
        tracing.finish_trace_run(
            connection,
            run_id=trace_run_id,
            status="completed",
            summary=result,
        )
        return result
    except Exception as exc:
        tracing.finish_trace_run(
            connection,
            run_id=trace_run_id,
            status="failed",
            summary={"error": str(exc)},
        )
        raise
