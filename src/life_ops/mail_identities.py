from __future__ import annotations

from dataclasses import dataclass
from email.utils import getaddresses, parseaddr
from typing import Any, Iterable


ALL_MAIL_IDENTITIES = "all"
DEFAULT_MAIL_IDENTITY = "frg"
FRG_INFO_MAIL_IDENTITY = "frg_info"
CODACLI_MAIL_IDENTITY = "codacli"
BIND_MY_SOUL_MAIL_IDENTITY = "bind_my_soul"
DECKFOUND_MAIL_IDENTITY = "deckfound"


@dataclass(frozen=True)
class MailIdentity:
    key: str
    label: str
    short_label: str
    address: str
    from_email: str
    signature_text: str
    signature_email_html: str
    signature_preview_html: str


MAIL_IDENTITIES: dict[str, MailIdentity] = {
    DEFAULT_MAIL_IDENTITY: MailIdentity(
        key=DEFAULT_MAIL_IDENTITY,
        label="Cody / FRG",
        short_label="FRG",
        address="cody@frg.earth",
        from_email="Cody <cody@frg.earth>",
        signature_text=(
            "Best,\n\n"
            "Cody Mitchell\n"
            "Fractal Research Group LLC\n"
            "https://frg.earth\n"
            "(850) 529-8452"
        ),
        signature_email_html="""
<div style="margin-top:24px;">
  <p style="margin:0 0 24px 0; color:#111111; font:500 15px/1.6 -apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;">Best,</p>
  <p style="margin:0; color:#111111; font:700 17px/1.45 Georgia,'Times New Roman',serif;">Cody Mitchell</p>
  <p style="margin:2px 0 0 0; color:#444444; font:600 13px/1.45 -apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;">Fractal Research Group LLC</p>
  <p style="margin:8px 0 0 0; font:500 13px/1.55 -apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;">
    <a href="https://frg.earth" style="color:#4f621f; text-decoration:none;">frg.earth</a><br>
    <a href="tel:+18505298452" style="color:#4f621f; text-decoration:none;">(850) 529-8452</a>
  </p>
</div>
""".strip(),
        signature_preview_html="""
<div class="signature-preview-card">
  <p class="signature-preview-closing">Best,</p>
  <p class="signature-preview-name">Cody Mitchell</p>
  <p class="signature-preview-company">Fractal Research Group LLC</p>
  <p class="signature-preview-links">frg.earth<br>(850) 529-8452</p>
</div>
""".strip(),
    ),
    FRG_INFO_MAIL_IDENTITY: MailIdentity(
        key=FRG_INFO_MAIL_IDENTITY,
        label="FRG Info",
        short_label="FRG Info",
        address="info@frg.earth",
        from_email="Fractal Research Group <info@frg.earth>",
        signature_text=(
            "Best,\n\n"
            "Cody Mitchell\n"
            "Fractal Research Group LLC\n"
            "https://frg.earth\n"
            "(850) 529-8452"
        ),
        signature_email_html="""
<div style="margin-top:24px;">
  <p style="margin:0 0 24px 0; color:#111111; font:500 15px/1.6 -apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;">Best,</p>
  <p style="margin:0; color:#111111; font:700 17px/1.45 Georgia,'Times New Roman',serif;">Cody Mitchell</p>
  <p style="margin:2px 0 0 0; color:#444444; font:600 13px/1.45 -apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;">Fractal Research Group LLC</p>
  <p style="margin:8px 0 0 0; font:500 13px/1.55 -apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;">
    <a href="https://frg.earth" style="color:#4f621f; text-decoration:none;">frg.earth</a><br>
    <a href="tel:+18505298452" style="color:#4f621f; text-decoration:none;">(850) 529-8452</a>
  </p>
</div>
""".strip(),
        signature_preview_html="""
<div class="signature-preview-card">
  <p class="signature-preview-closing">Best,</p>
  <p class="signature-preview-name">Cody Mitchell</p>
  <p class="signature-preview-company">Fractal Research Group LLC</p>
  <p class="signature-preview-links">frg.earth<br>(850) 529-8452</p>
</div>
""".strip(),
    ),
    CODACLI_MAIL_IDENTITY: MailIdentity(
        key=CODACLI_MAIL_IDENTITY,
        label="Cody / CodaCLI",
        short_label="CodaCLI",
        address="cody@codacli.com",
        from_email="Cody <cody@codacli.com>",
        signature_text="Best,\n\nCody Mitchell\nCodaCLI\nhttps://codacli.com",
        signature_email_html="""
<div style="margin-top:24px;">
  <p style="margin:0 0 24px 0; color:#111111; font:500 15px/1.6 -apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;">Best,</p>
  <p style="margin:0; color:#111111; font:700 17px/1.45 Georgia,'Times New Roman',serif;">Cody Mitchell</p>
  <p style="margin:2px 0 0 0; color:#444444; font:600 13px/1.45 -apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;">CodaCLI</p>
  <p style="margin:8px 0 0 0; font:500 13px/1.55 -apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;">
    <a href="https://codacli.com" style="color:#3f5f85; text-decoration:none;">codacli.com</a>
  </p>
</div>
""".strip(),
        signature_preview_html="""
<div class="signature-preview-card">
  <p class="signature-preview-closing">Best,</p>
  <p class="signature-preview-name">Cody Mitchell</p>
  <p class="signature-preview-company">CodaCLI</p>
  <p class="signature-preview-links">codacli.com</p>
</div>
""".strip(),
    ),
    BIND_MY_SOUL_MAIL_IDENTITY: MailIdentity(
        key=BIND_MY_SOUL_MAIL_IDENTITY,
        label="Bind My Soul Support",
        short_label="Bind My Soul",
        address="support@bindmysoul.com",
        from_email="Bind My Soul Support <support@bindmysoul.com>",
        signature_text="Bind My Soul Support\nhttps://bindmysoul.com",
        signature_email_html="""
<div style="margin-top:24px; padding-top:18px; border-top:1px solid #c9a95d;">
  <p style="margin:0; color:#17120a; font:700 17px/1.45 Georgia,'Times New Roman',serif;">Bind My Soul Support</p>
  <p style="margin:6px 0 0 0; font:600 13px/1.55 -apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;">
    <a href="https://bindmysoul.com" style="color:#8a5f13; text-decoration:none;">bindmysoul.com</a>
  </p>
</div>
""".strip(),
        signature_preview_html="""
<div class="signature-preview-card signature-preview-card-bms">
  <p class="signature-preview-name">Bind My Soul Support</p>
  <p class="signature-preview-links">bindmysoul.com</p>
</div>
""".strip(),
    ),
    DECKFOUND_MAIL_IDENTITY: MailIdentity(
        key=DECKFOUND_MAIL_IDENTITY,
        label="DeckFound Support",
        short_label="DeckFound",
        address="support@deckfound.com",
        from_email="DeckFound Support <support@deckfound.com>",
        signature_text="Best,\n\nDeckFound Support\nhttps://deckfound.com\nsupport@deckfound.com",
        signature_email_html="""
<div style="margin-top:24px;">
  <p style="margin:0 0 24px 0; color:#111111; font:500 15px/1.6 -apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;">Best,</p>
  <table role="presentation" cellpadding="0" cellspacing="0" border="0">
    <tr>
      <td style="padding:0 14px 0 0; vertical-align:top;">
        <img
          src="https://deckfound.com/assets/favicon-192.png"
          alt="DeckFound mark"
          width="44"
          height="44"
          style="display:block; width:44px; height:44px; border:0; border-radius:9px;"
        >
      </td>
      <td style="vertical-align:top; color:#111111; font:500 14px/1.6 -apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;">
        <div style="font-weight:700;">DeckFound Support</div>
        <div><a href="https://deckfound.com" style="color:#1155cc; text-decoration:none;">deckfound.com</a></div>
        <div><a href="mailto:support@deckfound.com" style="color:#111111; text-decoration:none;">support@deckfound.com</a></div>
      </td>
    </tr>
  </table>
</div>
""".strip(),
        signature_preview_html="""
<div class="signature-preview-card">
  <p class="signature-preview-closing">Best,</p>
  <p class="signature-preview-name">DeckFound Support</p>
  <p class="signature-preview-links">deckfound.com<br>support@deckfound.com</p>
</div>
""".strip(),
    ),
}


def normalize_mail_identity(value: Any, *, allow_all: bool = False) -> str:
    clean = str(value or "").strip().lower()
    if allow_all and clean == ALL_MAIL_IDENTITIES:
        return ALL_MAIL_IDENTITIES
    return clean if clean in MAIL_IDENTITIES else DEFAULT_MAIL_IDENTITY


def mail_identity(value: Any) -> MailIdentity:
    return MAIL_IDENTITIES[normalize_mail_identity(value)]


def public_mail_identities() -> list[dict[str, str]]:
    return [
        {
            "key": identity.key,
            "label": identity.label,
            "short_label": identity.short_label,
            "address": identity.address,
            "from_email": identity.from_email,
            "signature_text": identity.signature_text,
            "signature_preview_html": identity.signature_preview_html,
        }
        for identity in MAIL_IDENTITIES.values()
    ]


def _addresses(values: Iterable[Any]) -> set[str]:
    candidates: list[str] = []
    pending = list(values)
    while pending:
        value = pending.pop(0)
        if isinstance(value, dict):
            candidates.extend([str(value.get("email") or ""), str(value.get("address") or "")])
        elif isinstance(value, (list, tuple, set)):
            pending.extend(value)
        else:
            candidates.append(str(value or ""))
    parsed = {address.strip().lower() for _, address in getaddresses(candidates) if address.strip()}
    for candidate in candidates:
        _, address = parseaddr(candidate)
        if address.strip():
            parsed.add(address.strip().lower())
    return parsed


def infer_mail_identity(
    *,
    explicit: Any = "",
    direction: str = "inbound",
    external_from: Any = "",
    external_to: Any = "",
    external_cc: Any = "",
    external_bcc: Any = "",
    external_reply_to: Any = "",
    envelope_to: Any = "",
    to_recipients: Any = None,
    cc_recipients: Any = None,
    bcc_recipients: Any = None,
) -> str:
    clean_explicit = str(explicit or "").strip().lower()
    if clean_explicit in MAIL_IDENTITIES:
        return clean_explicit

    inbound_addresses = _addresses(
        [external_to, external_cc, external_bcc, envelope_to, to_recipients or [], cc_recipients or [], bcc_recipients or []]
    )
    outbound_addresses = _addresses([external_from, external_reply_to])
    candidates = inbound_addresses if str(direction or "").lower() == "inbound" else outbound_addresses
    for key in (
        CODACLI_MAIL_IDENTITY,
        BIND_MY_SOUL_MAIL_IDENTITY,
        DECKFOUND_MAIL_IDENTITY,
        FRG_INFO_MAIL_IDENTITY,
        DEFAULT_MAIL_IDENTITY,
    ):
        identity = MAIL_IDENTITIES[key]
        if identity.address.lower() in candidates:
            return identity.key
    return DEFAULT_MAIL_IDENTITY
