from __future__ import annotations

import re
from email.utils import getaddresses, parseaddr
from typing import Optional

CLASSIFIER_VERSION = "taxonomy-v5"

OPEN_BIASED_CATEGORIES = {"billing", "collaboration", "scheduling", "security"}
RECORD_BIASED_CATEGORIES = {
    "benefits",
    "career",
    "community",
    "creative",
    "developer",
    "entertainment",
    "finance",
    "home",
    "identity",
    "immigration",
    "insurance",
    "logistics",
    "medical",
    "pets",
    "record_keeping",
    "shopping",
    "tax",
    "travel",
}
TRUSTED_COLLABORATION_DOMAINS = {"github.com", "gitlab.com", "mg.gitlab.com", "slack.com"}
TRUSTED_SCHEDULING_DOMAINS = {"calendly.com", "email.meetup.com", "google.com", "meetup.com"}
ARCHIVE_TEXT_CATEGORIES = {"creative", "identity", "record_keeping"}

ACTION_TERMS = {
    "action required",
    "approve",
    "asap",
    "claim denied",
    "confirm",
    "deadline",
    "due",
    "expires",
    "expiring",
    "follow up",
    "minimum payment",
    "overdue",
    "pay now",
    "please respond",
    "reply",
    "respond",
    "response needed",
    "review",
    "schedule",
    "time sensitive",
    "update needed",
    "verify",
}

URGENT_TERMS = {
    "action required",
    "claim denied",
    "deadline",
    "due today",
    "expires today",
    "final notice",
    "immediately",
    "overdue",
    "urgent",
    "verify now",
}

REFERENCE_TERMS = {
    "archive",
    "attached",
    "attachment",
    "copy of",
    "document",
    "for reference",
    "keep this",
    "pdf",
    "receipt",
    "record",
    "scan",
    "statement",
}

CATEGORY_RULES = {
    "benefits": {
        "keywords": {
            "ebt",
            "eligibility",
            "food stamps",
            "medicaid",
            "public benefits",
            "recertification",
            "snap",
            "social security",
            "ssi",
            "ssdi",
            "unemployment",
        },
        "domains": {
            "ssa.gov",
        },
    },
    "billing": {
        "keywords": {
            "amount due",
            "autopay",
            "balance",
            "bill",
            "billing",
            "charge",
            "invoice",
            "minimum payment",
            "past due",
            "payment",
            "receipt",
            "renewal",
            "statement",
            "subscription",
        },
        "domains": {
            "americanexpress.com",
            "apple.com",
            "bankofamerica.com",
            "billing.fpl.com",
            "citi.com",
            "hypeddit.com",
            "paypal.com",
            "rocketmoney.com",
            "softheon.com",
            "stripe.com",
            "taxamo.com",
            "tm.openai.com",
            "verizonwireless.com",
            "zoom.us",
        },
    },
    "career": {
        "keywords": {
            "application",
            "candidate",
            "career",
            "hiring",
            "interview",
            "job alert",
            "job application",
            "job recommendation",
            "jobs in",
            "recruiter",
            "resume",
            "talent",
        },
        "domains": {
            "devpost.com",
            "glassdoor.com",
            "indeed.com",
            "linkedin.com",
        },
    },
    "collaboration": {
        "keywords": {
            "comment",
            "github",
            "gitlab",
            "issue",
            "merge request",
            "mentioned you",
            "pull request",
            "review requested",
            "slack",
            "thread reply",
        },
        "domains": {
            "github.com",
            "gitlab.com",
            "mg.gitlab.com",
            "slack.com",
        },
    },
    "community": {
        "keywords": {
            "community",
            "neighbors",
            "posted",
            "top post",
            "weekly roundup",
        },
        "domains": {
            "is.email.nextdoor.com",
            "notifications.soundcloud.com",
            "service.tiktok.com",
            "substack.com",
        },
    },
    "creative": {
        "keywords": {
            "art",
            "beat",
            "chapter",
            "demo",
            "draft",
            "essay",
            "idea",
            "journal",
            "lyrics",
            "manuscript",
            "notes to self",
            "painting",
            "poem",
            "script",
            "sketch",
            "song",
            "story",
            "writing",
        },
        "domains": {
            "beehiiv.com",
            "lalal.ai",
            "mastermind.com",
            "homestudiocorner.com",
            "patreon.com",
            "substack.com",
            "theguitarinstitute.co",
        },
    },
    "developer": {
        "keywords": {
            "agent features",
            "api",
            "cli",
            "developer account",
            "developer program",
            "editor",
            "release notes",
            "sdk",
        },
        "domains": {
            "nvidia.com",
            "resend.com",
        },
    },
    "entertainment": {
        "keywords": {
            "in your wishlist",
            "movie",
            "playlist",
            "posted a track",
            "theatres",
            "ticket",
            "watchlist",
        },
        "domains": {
            "amctheatres.com",
            "soundcloud.com",
            "steampowered.com",
            "store.steampowered.com",
        },
    },
    "finance": {
        "keywords": {
            "contribution limits",
            "credit score",
            "markets",
            "retirement",
            "transaction history",
            "yield",
        },
        "domains": {
            "chime.com",
            "creditkarma.com",
            "gemini.com",
            "one.app",
            "schwab.com",
            "venmo.com",
        },
    },
    "home": {
        "keywords": {
            "control app",
            "customer service experience",
            "home security",
            "smart home",
        },
        "domains": {
            "adt.com",
            "medallia.com",
        },
    },
    "identity": {
        "keywords": {
            "birth certificate",
            "driver license",
            "driver's license",
            "id photo",
            "identification",
            "passport",
            "social security card",
            "state id",
        },
        "domains": set(),
    },
    "immigration": {
        "keywords": {
            "adjustment of status",
            "affidavit of support",
            "biometrics",
            "case was received",
            "consular",
            "ds-160",
            "ds-260",
            "ead",
            "employment authorization",
            "form i-130",
            "form i-485",
            "form i-765",
            "green card",
            "immigrant visa",
            "immigration",
            "interview notice",
            "nvc",
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
        "domains": {
            "nvc.state.gov",
            "state.gov",
            "travel.state.gov",
            "uscis.dhs.gov",
            "uscis.gov",
        },
    },
    "insurance": {
        "keywords": {
            "deductible",
            "insurance claim",
            "member id",
            "policy",
            "prior authorization",
            "insurance",
        },
        "domains": {
            "ambetterhealth.softheon.com",
            "softheon.com",
        },
    },
    "logistics": {
        "keywords": {
            "delivery manager",
            "delivery",
            "package",
            "shipment",
            "tracking",
            "user id is ready",
        },
        "domains": {
            "fedex.com",
        },
    },
    "medical": {
        "keywords": {
            "clinic",
            "doctor",
            "lab result",
            "medical",
            "patient",
            "pharmacy",
            "prescription",
            "referral",
            "test result",
            "visit summary",
        },
        "domains": {
            "hims.com",
            "marijuanadoctor.com",
            "walgreens.com",
        },
    },
    "pets": {
        "keywords": {
            "canine",
            "feline",
            "flea",
            "habanero",
            "heartworm",
            "pet",
            "rabies",
            "vet",
            "veterinary",
        },
        "domains": {
            "vetcove.com",
            "vetsource.com",
        },
    },
    "record_keeping": {
        "keywords": REFERENCE_TERMS,
        "domains": set(),
    },
    "shopping": {
        "keywords": {
            "collection",
            "credit awarded",
            "hot picks",
            "lowest price",
            "offer",
            "order summary",
            "sale",
            "trade in",
            "wishlist",
        },
        "domains": {
            "alibaba.com",
            "amazon.com",
            "lenovo.com",
            "namecheap.com",
            "sandcloud.com",
            "samsungusa.com",
            "sunnyside.shop",
        },
    },
    "scheduling": {
        "keywords": {
            "appointment",
            "availability",
            "calendar invite",
            "call",
            "invite",
            "meeting",
            "rescheduled",
            "schedule",
            "time change",
            "tomorrow at",
        },
        "domains": {
            "calendar.google.com",
            "calendly.com",
            "email.meetup.com",
            "google.com",
        },
    },
    "security": {
        "keywords": {
            "2fa",
            "code",
            "login",
            "one time password",
            "password",
            "security alert",
            "sign in",
            "suspicious",
            "token",
            "verification",
            "verify account",
            "verify email",
            "verify your account",
            "verify your email",
        },
        "domains": {
            "account.canva.com",
            "elevenlabs.io",
            "verify.orcid.org",
        },
    },
    "tax": {
        "keywords": {
            "1040",
            "1099",
            "irs",
            "refund",
            "tax return",
            "tax transcript",
            "w-2",
            "w2",
        },
        "domains": {
            "irs.gov",
            "hrblock.com",
        },
    },
    "travel": {
        "keywords": {
            "rental car",
            "road trip",
            "spring break",
            "travel",
            "trip",
        },
        "domains": {
            "turo.com",
        },
    },
}


def _normalize_text(*parts: str) -> str:
    value = " ".join(part for part in parts if part).lower()
    return re.sub(r"[^a-z0-9]+", " ", value).strip()


def _addresses(value: str) -> list[str]:
    return [address.lower() for _, address in getaddresses([value]) if address]


def _domain_matches(sender_domain: str, domains: set[str]) -> bool:
    return any(sender_domain == domain or sender_domain.endswith(f".{domain}") for domain in domains)


def _contains_phrase(text: str, phrase: str) -> bool:
    padded_text = f" {text} "
    padded_phrase = f" {phrase.strip()} "
    return padded_phrase in padded_text


def _priority_level(priority_score: int) -> str:
    if priority_score >= 75:
        return "urgent"
    if priority_score >= 55:
        return "high"
    if priority_score >= 30:
        return "normal"
    return "low"


def classify_message(
    *,
    subject: str,
    sender: str,
    to: str = "",
    cc: str = "",
    snippet: str = "",
    body_text: str = "",
    attachments: Optional[list[dict]] = None,
    triage: Optional[dict] = None,
    user_email: str = "",
) -> dict:
    attachments = attachments or []
    triage = triage or {}

    _, sender_email = parseaddr(sender)
    sender_email = sender_email.lower()
    sender_domain = sender_email.partition("@")[2]
    recipients = set(_addresses(to) + _addresses(cc))
    user_email = user_email.lower().strip()
    self_sent = bool(user_email and sender_email == user_email and user_email in recipients)

    attachment_text = _normalize_text(
        " ".join(str(attachment.get("filename", "")) for attachment in attachments),
        " ".join(str(attachment.get("mime_type", "")) for attachment in attachments),
    )
    subject_text = _normalize_text(subject)
    summary_text = _normalize_text(subject, sender, snippet, attachment_text)
    archive_text = _normalize_text(subject, sender, snippet, body_text, attachment_text)

    category_scores: dict[str, int] = {}
    reasons: list[str] = []
    for category, rule in CATEGORY_RULES.items():
        score = 0
        search_text = archive_text if category in ARCHIVE_TEXT_CATEGORIES else summary_text
        matched_keywords = [keyword for keyword in sorted(rule["keywords"]) if _contains_phrase(search_text, keyword)]
        if matched_keywords:
            score += min(6, 2 * len(matched_keywords))
            reasons.append(f"{category}:keyword")
        if _domain_matches(sender_domain, rule["domains"]):
            score += 4
            reasons.append(f"{category}:domain")
        if category == "record_keeping" and attachments:
            score += 2
            reasons.append("record_keeping:attachment")
        if category == "identity" and attachments and any(
            _contains_phrase(attachment_text, token) for token in {"id", "passport", "license", "front", "back"}
        ):
            score += 3
            reasons.append("identity:attachment-name")
        if category == "creative" and self_sent and any(
            _contains_phrase(summary_text, token) for token in {"draft", "idea", "notes", "poem", "song", "writing"}
        ):
            score += 2
            reasons.append("creative:self-sent")
        if score:
            category_scores[category] = score

    if self_sent and attachments:
        category_scores["record_keeping"] = category_scores.get("record_keeping", 0) + 3
        reasons.append("record_keeping:self-sent-attachment")

    if attachments and any(category in category_scores for category in {"benefits", "identity", "insurance", "medical", "tax"}):
        category_scores["record_keeping"] = category_scores.get("record_keeping", 0) + 2
        reasons.append("record_keeping:sensitive-document")

    selected_categories = sorted(
        [category for category, score in category_scores.items() if score >= 2],
        key=lambda category: (-category_scores[category], category),
    )

    if not selected_categories and triage.get("actionable"):
        fallback_category = ""
        if sender_domain in TRUSTED_COLLABORATION_DOMAINS:
            fallback_category = "collaboration"
        elif sender_domain in TRUSTED_SCHEDULING_DOMAINS or any(
            _contains_phrase(subject_text, phrase) for phrase in {"appointment", "calendar invite", "meeting", "scheduled"}
        ):
            fallback_category = "scheduling"

        if fallback_category:
            selected_categories = [fallback_category]
            category_scores[fallback_category] = max(2, int(triage.get("score", 0)))
            reasons.append(f"{fallback_category}:triage-fallback")

    triage_score = max(0, int(triage.get("score", 0)))
    triage_reasons = set(str(reason) for reason in triage.get("reasons", []))
    has_action_terms = any(_contains_phrase(subject_text, term) for term in ACTION_TERMS)
    has_urgent_terms = any(_contains_phrase(subject_text, term) for term in URGENT_TERMS)
    has_reference_terms = any(_contains_phrase(archive_text, term) for term in REFERENCE_TERMS)
    bulkish = bool(triage_reasons & {"bulk-mail", "mailing-list", "newsletter-domain", "non-actionable-subject"})
    strong_triage_action = bool(triage.get("actionable")) and not bulkish

    priority_score = triage_score * 8
    if has_action_terms:
        priority_score += 18
        reasons.append("priority:action-term")
    if has_urgent_terms:
        priority_score += 20
        reasons.append("priority:urgent-term")
    if any(category in OPEN_BIASED_CATEGORIES for category in selected_categories):
        priority_score += 12
        reasons.append("priority:open-category")
    if any(category in {"benefits", "insurance", "medical", "tax"} for category in selected_categories) and has_action_terms:
        priority_score += 18
        reasons.append("priority:sensitive-action")
    if self_sent and attachments and not has_action_terms:
        priority_score -= 20
        reasons.append("priority:self-sent-record")
    if has_reference_terms and not triage.get("actionable"):
        priority_score -= 6
        reasons.append("priority:reference-bias")

    priority_score = max(0, min(100, priority_score))
    priority_level = _priority_level(priority_score)

    open_signal = priority_score >= 80
    if "collaboration" in selected_categories and (strong_triage_action or has_action_terms or has_urgent_terms):
        open_signal = True
    if any(category in {"billing", "scheduling", "security"} for category in selected_categories) and (
        strong_triage_action or has_action_terms or has_urgent_terms
    ):
        open_signal = True
    if any(category in {"benefits", "identity", "insurance", "medical", "tax"} for category in selected_categories) and (
        has_action_terms or has_urgent_terms
    ):
        open_signal = True
    if "creative" in selected_categories and not self_sent:
        open_signal = False

    if self_sent and attachments and all(
        category in {"creative", "identity", "record_keeping"} for category in selected_categories
    ) and priority_score < 60:
        open_signal = False

    if open_signal:
        status = "open"
    elif selected_categories or attachments or has_reference_terms:
        status = "reference"
    else:
        status = "ignore"

    if selected_categories:
        specific_categories = [category for category in selected_categories if category != "record_keeping"]
        primary_pool = specific_categories or selected_categories
        primary_category = max(primary_pool, key=lambda category: (category_scores[category], category))
    else:
        primary_category = "general"

    if status == "open":
        retention_bucket = "action_queue"
    elif "creative" in selected_categories:
        retention_bucket = "creative_archive"
    elif any(category in {"billing", "finance"} for category in selected_categories):
        retention_bucket = "financial_records"
    elif any(category in RECORD_BIASED_CATEGORIES for category in selected_categories):
        retention_bucket = "records"
    elif status == "reference":
        retention_bucket = "general_reference"
    else:
        retention_bucket = "none"

    return {
        "status": status,
        "primary_category": primary_category,
        "categories": selected_categories,
        "priority_level": priority_level,
        "priority_score": priority_score,
        "retention_bucket": retention_bucket,
        "reasons": sorted(set(reasons)),
        "classifier_version": CLASSIFIER_VERSION,
        "signals": {
            "attachments_present": bool(attachments),
            "has_action_terms": has_action_terms,
            "has_reference_terms": has_reference_terms,
            "has_urgent_terms": has_urgent_terms,
            "self_sent": self_sent,
            "sender_domain": sender_domain,
        },
        "category_scores": category_scores,
        "summary": {
            "subject": subject or "(no subject)",
            "sender_email": sender_email,
        },
    }
