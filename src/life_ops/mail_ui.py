from __future__ import annotations

import base64
import binascii
import hashlib
import json
import mimetypes
import re
import shutil
import sqlite3
import tempfile
import threading
from datetime import date, datetime, timedelta, timezone
from email.utils import formataddr, getaddresses, parseaddr
from html import escape as html_escape
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from life_ops.calendar import build_calendar_day, rollover_calendar_day, save_calendar_day
from life_ops.document_ingest import extract_text_from_saved_attachment
from life_ops import mail_metadata
from life_ops import mail_vault
from life_ops.resend_integration import resend_send_email
from life_ops import store
from life_ops.cloudflare_email import cloudflare_mail_queue_status, sync_cloudflare_mail_queue

DEFAULT_MAIL_UI_HOST = "127.0.0.1"
DEFAULT_MAIL_UI_PORT = 4311
DEFAULT_MAIL_UI_LIMIT = 80
DEFAULT_MAIL_UI_SOURCE = "cloudflare_email"
DEFAULT_MAIL_UI_OUTBOUND_SOURCE = "resend_email"
DEFAULT_MAIL_UI_CORRESPONDENCE_SOURCE = "correspondence"
DEFAULT_MAIL_UI_CONTACT_LIMIT = 200
DEFAULT_MAIL_UI_FAVICON_PATH = "/static/favicon.svg"
DEFAULT_MAIL_UI_OG_IMAGE_PATH = "/static/og-image.svg"
MAIL_UI_BACKGROUND_SYNC_INTERVAL_SECONDS = 2.0
MAIL_UI_CLIENT_REFRESH_INTERVAL_MS = 2000
MAIL_UI_SYNC_REQUEST_TIMEOUT_SECONDS = 10.0
MAIL_UI_HEARTBEAT_REQUEST_TIMEOUT_SECONDS = 3.0
MAX_DRAFT_ATTACHMENT_BYTES = 20 * 1024 * 1024
MAX_DRAFT_ATTACHMENT_COUNT = 12
_SUBJECT_PREFIX_RE = re.compile(r"^(?:(?:re|fwd?|aw|sv)\s*:\s*)+", re.IGNORECASE)
_QUOTED_REPLY_HEADER_RE = re.compile(r"(?P<header>\bOn [^\n]{0,500}? wrote:)", re.IGNORECASE)
_SIGNATURE_TAIL_RE = re.compile(r"(?:\s|^)--\s+[^\n]+$", re.DOTALL)
_GMAIL_QUOTE_RE = re.compile(r"""<div\b[^>]*\bclass\s*=\s*(["'])[^"']*\bgmail_quote\b[^"']*\1[^>]*>""", re.IGNORECASE)
_PLAIN_TEXT_URL_RE = re.compile(r"(?P<url>\bhttps?://[^\s<>'\"]+|\bmailto:[^\s<>'\"]+|\bwww\.[^\s<>'\"]+)", re.IGNORECASE)
_TRAILING_URL_PUNCTUATION = ".,;:!?)]}"
_CMAIL_SIGNATURE_TEXT = (
    "Best,\n\n"
    "Cody Mitchell\n"
    "Fractal Research Group\n"
    "https://frg.earth\n"
    "https://www.npmjs.com/~sproutseeds\n"
    "https://github.com/SproutSeeds\n"
    "cody@frg.earth"
)
_CMAIL_LEGACY_SIGNATURE_TEXT_WITH_SITE_SPACED = "Best,\n\nCody Mitchell\nFractal Research Group\nhttps://frg.earth\ncody@frg.earth"
_CMAIL_LEGACY_SIGNATURE_TEXT_SPACED = "Best,\n\nCody Mitchell\nFractal Research Group\ncody@frg.earth"
_CMAIL_LEGACY_SIGNATURE_TEXT_WITH_SITE = "Best,\nCody Mitchell\nFractal Research Group\nhttps://frg.earth\ncody@frg.earth"
_CMAIL_LEGACY_SIGNATURE_TEXT = "Best,\nCody Mitchell\nFractal Research Group\ncody@frg.earth"
_CMAIL_KNOWN_SIGNATURE_TEXTS = (
    _CMAIL_SIGNATURE_TEXT,
    _CMAIL_LEGACY_SIGNATURE_TEXT_WITH_SITE_SPACED,
    _CMAIL_LEGACY_SIGNATURE_TEXT_SPACED,
    _CMAIL_LEGACY_SIGNATURE_TEXT_WITH_SITE,
    _CMAIL_LEGACY_SIGNATURE_TEXT,
)
_CMAIL_SIGNATURE_EMAIL_HTML = """
<div style="margin-top:24px;">
  <p style="margin:0 0 24px 0; color:#111111; font:500 15px/1.6 -apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;">Best,</p>
  <table role="presentation" cellpadding="0" cellspacing="0" border="0">
    <tr>
      <td style="padding:0 14px 0 0; vertical-align:top;">
        <img
          src="https://frg.earth/branding/frg-bimi-iris-floating.png"
          alt="Fractal Research Group iris mark"
          width="44"
          height="44"
          style="display:block; width:44px; height:44px; border:0;"
        >
      </td>
      <td style="vertical-align:top; color:#111111; font:500 14px/1.6 -apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;">
        <div style="font-weight:700;">Cody Mitchell</div>
        <div>Fractal Research Group</div>
        <div><a href="https://frg.earth" style="color:#1155cc; text-decoration:none;">frg.earth</a></div>
        <div><a href="https://www.npmjs.com/~sproutseeds" style="color:#1155cc; text-decoration:none;">npmjs.com/~sproutseeds</a></div>
        <div><a href="https://github.com/SproutSeeds" style="color:#1155cc; text-decoration:none;">github.com/SproutSeeds</a></div>
        <div><a href="mailto:cody@frg.earth" style="color:#111111; text-decoration:none;">cody@frg.earth</a></div>
      </td>
    </tr>
  </table>
</div>
""".strip()

_CMAIL_SIGNATURE_PREVIEW_HTML = """
<div style="margin-top:24px;">
  <p style="margin:0 0 24px 0; color:#edf2eb; font:500 15px/1.6 -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">Best,</p>
  <table role="presentation" cellpadding="0" cellspacing="0" border="0">
    <tr>
      <td style="padding:0 14px 0 0; vertical-align:top;">
        <img
          src="https://frg.earth/branding/frg-bimi-iris-floating.png"
          alt="Fractal Research Group iris mark"
          width="44"
          height="44"
          style="display:block; width:44px; height:44px; border:0;"
        >
      </td>
      <td style="vertical-align:top; color:#edf2eb; font:500 14px/1.6 -apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;">
        <div style="font-weight:700;">Cody Mitchell</div>
        <div>Fractal Research Group</div>
        <div><a href="https://frg.earth" style="color:#b8ff4d; text-decoration:none;">frg.earth</a></div>
        <div><a href="https://www.npmjs.com/~sproutseeds" style="color:#b8ff4d; text-decoration:none;">npmjs.com/~sproutseeds</a></div>
        <div><a href="https://github.com/SproutSeeds" style="color:#b8ff4d; text-decoration:none;">github.com/SproutSeeds</a></div>
        <div><a href="mailto:cody@frg.earth" style="color:#edf2eb; text-decoration:none;">cody@frg.earth</a></div>
      </td>
    </tr>
  </table>
</div>
""".strip()


def _mail_ui_static_root() -> Path:
    return store.package_root() / "static"


def _resolve_mail_ui_static_asset(relative_path: str) -> Path:
    static_root = _mail_ui_static_root().expanduser().resolve(strict=False)
    candidate = (static_root / relative_path.lstrip("/")).resolve(strict=False)
    try:
        candidate.relative_to(static_root)
    except ValueError as exc:
        raise ValueError("path traversal detected") from exc
    return candidate

_SAFE_HTML_TAGS: dict[str, set[str]] = {
    "a": {"href", "title"},
    "b": set(),
    "blockquote": set(),
    "br": set(),
    "code": set(),
    "div": set(),
    "em": set(),
    "hr": set(),
    "i": set(),
    "img": {"src", "alt", "width", "height"},
    "li": set(),
    "ol": set(),
    "p": set(),
    "pre": set(),
    "span": set(),
    "strong": set(),
    "table": set(),
    "tbody": set(),
    "td": {"colspan", "rowspan"},
    "th": {"colspan", "rowspan"},
    "thead": set(),
    "tr": set(),
    "ul": set(),
}
_VOID_HTML_TAGS = {"br", "hr", "img"}


class _SafeHtmlFragmentSanitizer(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=False)
        self._parts: list[str] = []
        self._open_tags: list[str] = []
        self._skip_content_depth = 0

    @staticmethod
    def _safe_href(value: str) -> str:
        candidate = str(value or "").strip()
        if not candidate:
            return ""
        parsed = urlparse(candidate)
        scheme = (parsed.scheme or "").lower()
        if scheme in {"http", "https", "mailto"}:
            return candidate
        if not scheme and candidate.startswith(("/", "#")):
            return candidate
        return ""

    @staticmethod
    def _safe_img_src(value: str) -> str:
        candidate = str(value or "").strip()
        if not candidate:
            return ""
        parsed = urlparse(candidate)
        scheme = (parsed.scheme or "").lower()
        if scheme == "data":
            return candidate
        if scheme in {"http", "https"} and (parsed.netloc or "").lower() == "frg.earth":
            return candidate
        return ""

    @staticmethod
    def _safe_span(value: str, minimum: int = 1, maximum: int = 2000) -> str:
        clean = str(value or "").strip()
        if not clean.isdigit():
            return ""
        numeric = int(clean)
        if numeric < minimum or numeric > maximum:
            return ""
        return str(numeric)

    def _emit_attrs(self, tag: str, attrs: list[tuple[str, str | None]]) -> str:
        allowed_attrs = _SAFE_HTML_TAGS.get(tag, set())
        rendered: list[str] = []
        for name, raw_value in attrs:
            attr_name = str(name or "").lower()
            if attr_name not in allowed_attrs:
                continue
            value = str(raw_value or "")
            if tag == "a" and attr_name == "href":
                safe = self._safe_href(value)
                if not safe:
                    continue
                rendered.append(f' href="{html_escape(safe, quote=True)}"')
                rendered.append(' target="_blank" rel="noreferrer noopener"')
                continue
            if tag == "img" and attr_name == "src":
                safe = self._safe_img_src(value)
                if not safe:
                    continue
                rendered.append(f' src="{html_escape(safe, quote=True)}"')
                continue
            if tag == "img" and attr_name == "alt":
                rendered.append(f' alt="{html_escape(value, quote=True)}"')
                continue
            if attr_name in {"width", "height", "colspan", "rowspan"}:
                safe = self._safe_span(value)
                if not safe:
                    continue
                rendered.append(f' {attr_name}="{safe}"')
                continue
            rendered.append(f' {attr_name}="{html_escape(value, quote=True)}"')
        return "".join(rendered)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        clean_tag = str(tag or "").lower()
        if clean_tag in {"script", "style"}:
            self._skip_content_depth += 1
            return
        if self._skip_content_depth:
            return
        if clean_tag not in _SAFE_HTML_TAGS:
            return
        attr_markup = self._emit_attrs(clean_tag, attrs)
        if clean_tag == "img" and ' src="' not in attr_markup:
            return
        self._parts.append(f"<{clean_tag}{attr_markup}>")
        if clean_tag not in _VOID_HTML_TAGS:
            self._open_tags.append(clean_tag)

    def handle_endtag(self, tag: str) -> None:
        clean_tag = str(tag or "").lower()
        if clean_tag in {"script", "style"}:
            if self._skip_content_depth:
                self._skip_content_depth -= 1
            return
        if self._skip_content_depth:
            return
        if clean_tag not in _SAFE_HTML_TAGS or clean_tag in _VOID_HTML_TAGS:
            return
        if clean_tag in self._open_tags:
            while self._open_tags:
                current = self._open_tags.pop()
                self._parts.append(f"</{current}>")
                if current == clean_tag:
                    break

    def handle_data(self, data: str) -> None:
        if self._skip_content_depth:
            return
        self._parts.append(html_escape(str(data or "")))

    def handle_entityref(self, name: str) -> None:
        if self._skip_content_depth:
            return
        self._parts.append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        if self._skip_content_depth:
            return
        self._parts.append(f"&#{name};")

    def get_html(self) -> str:
        while self._open_tags:
            self._parts.append(f"</{self._open_tags.pop()}>")
        return "".join(self._parts).strip()

_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>CMAIL</title>
  <link rel="icon" href="/static/favicon.svg" type="image/svg+xml">
  <meta property="og:title" content="CMAIL">
  <meta property="og:image" content="/static/og-image.svg">
  <style>
    :root {
      color-scheme: dark;
      --bg: #0a0d0b;
      --panel: #111612;
      --panel-2: #171d18;
      --line: #283229;
      --text: #edf2eb;
      --muted: #8c988d;
      --accent: #b8ff4d;
      --accent-soft: rgba(184, 255, 77, 0.14);
      --mono: "SFMono-Regular", "JetBrains Mono", "Menlo", monospace;
      --sans: "SF Pro Display", "Inter", system-ui, sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: radial-gradient(circle at top, #111913 0%, var(--bg) 52%);
      color: var(--text);
      font-family: var(--sans);
    }
    .shell {
      min-height: 100vh;
      display: grid;
      grid-template-rows: auto 1fr;
      gap: 16px;
      padding: 18px;
    }
    .topbar, .main {
      width: min(1440px, 100%);
      margin: 0 auto;
    }
    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      flex-wrap: wrap;
    }
    .topbar-actions {
      display: flex;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
    }
    .brand-lockup {
      display: flex;
      align-items: center;
    }
    .eyebrow {
      color: var(--accent);
      font: 600 12px/1 var(--mono);
      letter-spacing: 0.18em;
      text-transform: uppercase;
    }
    h1 {
      margin: 0;
      font-size: clamp(34px, 5vw, 56px);
      line-height: 0.95;
      font-weight: 780;
      letter-spacing: -0.05em;
      color: #f5f8f3;
      text-shadow: 0 0 18px rgba(184, 255, 77, 0.08);
    }
    .hero-title {
      display: inline-flex;
      align-items: center;
      gap: 12px;
    }
    .hero-title::before {
      content: "";
      width: 12px;
      height: 12px;
      border-radius: 999px;
      background: radial-gradient(circle at 35% 35%, #efffd8 0%, #b8ff4d 46%, #6ca81c 100%);
      box-shadow:
        0 0 16px rgba(184, 255, 77, 0.34),
        0 0 40px rgba(184, 255, 77, 0.1);
      transform: translateY(1px);
    }
    .tabbar {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 4px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: rgba(255,255,255,0.03);
    }
    .tab-button {
      border: none;
      background: transparent;
      color: var(--muted);
      padding: 8px 14px;
      border-radius: 999px;
      font: 700 13px/1 var(--mono);
      letter-spacing: 0.08em;
      text-transform: uppercase;
      cursor: pointer;
      text-decoration: none;
    }
    .tab-button.active {
      background: var(--accent-soft);
      color: var(--accent);
    }
    .toolbar {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
    }
    select, button, input, textarea {
      appearance: none;
      border: 1px solid var(--line);
      background: var(--panel);
      color: var(--text);
      padding: 10px 12px;
      border-radius: 12px;
      font: 500 14px/1.3 var(--sans);
    }
    textarea {
      min-height: 150px;
      resize: vertical;
      width: 100%;
    }
    input, textarea {
      background: #0d120f;
    }
    button.primary {
      border: none;
      background: linear-gradient(180deg, #c9ff72 0%, #89c72a 100%);
      color: #091008;
      font-weight: 700;
      cursor: pointer;
    }
    button.secondary {
      cursor: pointer;
      background: var(--panel-2);
    }
    .stats {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
    }
    .card, .panel {
      background: linear-gradient(180deg, rgba(255,255,255,0.02) 0%, rgba(255,255,255,0.01) 100%), var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
    }
    .card {
      padding: 14px 16px;
      min-height: 92px;
    }
    .card .label {
      color: var(--muted);
      font: 600 11px/1 var(--mono);
      letter-spacing: 0.12em;
      text-transform: uppercase;
    }
    .card .value {
      margin-top: 10px;
      font-size: 30px;
      font-weight: 700;
    }
    .card .meta {
      margin-top: 8px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.4;
    }
    .alerts {
      display: none;
      gap: 12px;
    }
    .alerts.visible {
      display: grid;
    }
    .alert {
      padding: 12px 14px;
      border-radius: 14px;
      border: 1px solid rgba(255, 137, 118, 0.3);
      background: rgba(255, 137, 118, 0.07);
    }
    .alert .title { font-weight: 700; }
    .alert .message { color: var(--muted); margin-top: 4px; font-size: 14px; }
    .main {
      display: grid;
      grid-template-columns: 380px 1fr;
      gap: 14px;
      min-height: 72vh;
    }
    .panel { overflow: hidden; }
    .panel-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
      background: rgba(255,255,255,0.02);
    }
    .panel-head h2 {
      margin: 0;
      font-size: 15px;
      font-weight: 700;
      letter-spacing: 0.02em;
    }
    .panel-actions {
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .panel-search {
      padding: 10px 14px 12px;
      border-bottom: 1px solid rgba(255,255,255,0.04);
    }
    .panel-search.hidden {
      display: none;
    }
    .panel-search input {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(255,255,255,0.018);
      color: var(--text);
      padding: 10px 12px;
      font-size: 14px;
      line-height: 1.4;
      outline: none;
      transition: border-color 120ms ease, background 120ms ease;
    }
    .panel-search input::placeholder {
      color: rgba(226, 230, 224, 0.42);
    }
    .panel-search input:focus {
      border-color: rgba(184, 255, 77, 0.34);
      background: rgba(184, 255, 77, 0.035);
    }
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 3px 8px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: var(--panel-2);
      color: var(--muted);
      font: 600 11px/1 var(--mono);
      text-transform: uppercase;
    }
    .status-pill {
      color: var(--accent);
      border-color: rgba(184, 255, 77, 0.2);
      background: rgba(184, 255, 77, 0.08);
    }
    .thread-list {
      display: flex;
      flex-direction: column;
      max-height: calc(72vh - 54px);
      overflow: auto;
    }
    .thread {
      padding: 14px 16px;
      border-bottom: 1px solid var(--line);
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }
    .thread.active {
      background: var(--accent-soft);
    }
    .thread:hover { background: rgba(255,255,255,0.03); }
    .thread-main {
      min-width: 0;
      display: flex;
      align-items: center;
      gap: 10px;
    }
    .thread .subject {
      font-size: 16px;
      font-weight: 650;
      line-height: 1.35;
    }
    .unread-orb {
      width: 8px;
      height: 8px;
      min-width: 8px;
      border-radius: 999px;
      background: rgba(246, 248, 244, 0.96);
      box-shadow: 0 0 0 1px rgba(255,255,255,0.08);
      flex: 0 0 auto;
    }
    .unread-orb.hidden {
      display: none;
    }
    .delete-button {
      appearance: none;
      border: 1px solid rgba(255,255,255,0.12);
      background: transparent;
      color: var(--muted);
      width: 24px;
      height: 24px;
      min-width: 24px;
      border-radius: 999px;
      padding: 0;
      font: 700 13px/1 var(--mono);
      display: inline-flex;
      align-items: center;
      justify-content: center;
      cursor: pointer;
    }
    .delete-button:hover {
      color: #ffb4a8;
      border-color: rgba(255, 180, 168, 0.28);
      background: rgba(255, 137, 118, 0.08);
    }
    .confirm-layer {
      position: fixed;
      inset: 0;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 24px;
      background: rgba(6, 10, 8, 0.38);
      backdrop-filter: blur(16px);
      -webkit-backdrop-filter: blur(16px);
      z-index: 40;
    }
    .confirm-layer.hidden {
      display: none;
    }
    .confirm-card {
      width: min(420px, calc(100vw - 48px));
      border-radius: 20px;
      border: 1px solid rgba(255, 255, 255, 0.12);
      background:
        linear-gradient(180deg, rgba(255,255,255,0.08) 0%, rgba(255,255,255,0.03) 100%),
        rgba(14, 19, 16, 0.92);
      box-shadow:
        0 30px 80px rgba(0, 0, 0, 0.45),
        inset 0 1px 0 rgba(255, 255, 255, 0.06);
      padding: 18px 18px 16px;
      display: grid;
      gap: 12px;
    }
    .confirm-title {
      font-size: 24px;
      line-height: 1.1;
      font-weight: 750;
      color: var(--text);
    }
    .confirm-copy {
      color: var(--muted);
      font-size: 14px;
      line-height: 1.55;
    }
    .confirm-copy strong {
      color: var(--text);
      font-weight: 650;
    }
    .confirm-actions {
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 10px;
      flex-wrap: wrap;
    }
    .confirm-actions button {
      min-width: 108px;
    }
    button.danger {
      border: 1px solid rgba(255, 180, 168, 0.22);
      background: linear-gradient(180deg, rgba(255, 180, 168, 0.18) 0%, rgba(255, 137, 118, 0.12) 100%);
      color: #ffd7d0;
      font-weight: 700;
      cursor: pointer;
    }
    button.danger:hover {
      border-color: rgba(255, 180, 168, 0.34);
      background: linear-gradient(180deg, rgba(255, 180, 168, 0.24) 0%, rgba(255, 137, 118, 0.18) 100%);
    }
    .detail {
      display: grid;
      grid-template-rows: auto auto 1fr;
      min-height: 100%;
    }
    .detail.draft-mode {
      grid-template-rows: auto 1fr;
    }
    .detail-head {
      padding: 18px 18px 12px;
      border-bottom: 1px solid var(--line);
      display: block;
    }
    .detail-head h3 {
      margin: 0;
      font-size: 28px;
      line-height: 1.2;
    }
    .detail-grid {
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(0, 1fr) auto;
      gap: 8px 14px;
      padding: 16px 18px;
      border-bottom: 1px solid var(--line);
    }
    .detail-grid .label, .block .label {
      color: var(--muted);
      font: 600 11px/1 var(--mono);
      text-transform: uppercase;
      letter-spacing: 0.1em;
      margin-bottom: 6px;
    }
    .detail-grid .value {
      font-size: 14px;
      line-height: 1.45;
      word-break: break-word;
    }
    .detail-grid .item { min-width: 0; }
    .detail-grid .item-action {
      display: flex;
      align-items: end;
      justify-content: flex-end;
    }
    .detail-action-buttons {
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 8px;
      flex-wrap: wrap;
    }
    .inline-reply {
      min-width: 72px;
      height: 28px;
      padding: 0 12px;
      font-size: 13px;
      line-height: 1;
      border-radius: 999px;
    }
    .inline-delete {
      width: 28px;
      height: 28px;
      min-width: 28px;
      font-size: 14px;
    }
    .detail-body {
      padding: 18px;
      display: grid;
      gap: 18px;
      align-content: start;
      overflow: auto;
    }
    .draft-detail-body {
      display: flex;
      flex-direction: column;
      align-content: stretch;
      min-height: 0;
      overflow: hidden;
    }
    .attachments, .message-list {
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #0d120f;
      padding: 14px;
    }
    .thread-groups {
      display: grid;
      gap: 8px;
    }
    .thread-group {
      display: grid;
      gap: 4px;
      padding: 12px 14px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: rgba(255,255,255,0.015);
      cursor: pointer;
    }
    .thread-group.active {
      border-color: rgba(184, 255, 77, 0.28);
      background: rgba(184, 255, 77, 0.08);
    }
    .thread-group-title {
      font-size: 15px;
      line-height: 1.4;
      font-weight: 700;
    }
    .thread-group-meta {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.45;
    }
    .block { display: grid; gap: 8px; }
    .body-text {
      white-space: pre-wrap;
      line-height: 1.75;
      font-size: 18px;
      padding: 0;
      background: transparent;
      border: none;
    }
    .quote-block {
      display: grid;
      gap: 8px;
      padding-left: 16px;
      border-left: 2px solid rgba(255,255,255,0.12);
    }
    .quote-details {
      border-left: 2px solid rgba(255,255,255,0.12);
      padding-left: 16px;
    }
    .quote-details[open] {
      display: grid;
      gap: 8px;
    }
    .quote-summary {
      list-style: none;
      cursor: pointer;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.5;
      user-select: none;
    }
    .quote-summary::-webkit-details-marker {
      display: none;
    }
    .quote-summary::before {
      content: "+";
      display: inline-block;
      margin-right: 8px;
      color: var(--accent);
      font: 700 12px/1 var(--mono);
    }
    .quote-details[open] .quote-summary::before {
      content: "−";
    }
    .quote-header {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.5;
    }
    .quote-text {
      color: #c9d1c7;
      white-space: pre-wrap;
      line-height: 1.72;
      font-size: 16px;
    }
    .body-rich,
    .quote-rich {
      color: #d7ddd3;
      line-height: 1.7;
      font-size: 15px;
      display: grid;
      gap: 12px;
      overflow-wrap: anywhere;
    }
    .body-rich p,
    .body-rich div,
    .body-rich blockquote,
    .body-rich table,
    .quote-rich p,
    .quote-rich div,
    .quote-rich blockquote,
    .quote-rich table {
      margin: 0;
    }
    .body-rich :where(div, p, blockquote, table, ul, ol, pre) + :where(div, p, blockquote, table, ul, ol, pre),
    .quote-rich :where(div, p, blockquote, table, ul, ol, pre) + :where(div, p, blockquote, table, ul, ol, pre) {
      margin-top: 0.92em;
    }
    .body-rich td > :where(div, p) + :where(div, p),
    .quote-rich td > :where(div, p) + :where(div, p) {
      margin-top: 0.28em;
    }
    .body-rich table,
    .quote-rich table {
      width: 100%;
      border-collapse: collapse;
    }
    .body-rich td,
    .body-rich th,
    .quote-rich td,
    .quote-rich th {
      vertical-align: top;
      padding: 4px 8px 4px 0;
    }
    .body-rich blockquote,
    .quote-rich blockquote {
      margin: 0;
      padding-left: 14px;
      border-left: 2px solid rgba(184, 255, 77, 0.28);
      color: #c9d1c7;
    }
    .body-rich a,
    .quote-rich a {
      color: var(--accent);
      text-decoration: none;
    }
    .body-rich a:hover,
    .quote-rich a:hover {
      text-decoration: underline;
    }
    .body-rich img,
    .quote-rich img {
      display: block;
      max-width: min(100%, 240px);
      height: auto;
      border-radius: 12px;
      border: 1px solid rgba(184, 255, 77, 0.18);
      background: rgba(255, 255, 255, 0.02);
      padding: 6px;
    }
    .message-list {
      display: grid;
      gap: 8px;
    }
    .thread-item {
      display: grid;
      gap: 4px;
      padding: 10px 0;
      border-bottom: 1px solid var(--line);
      cursor: pointer;
    }
    .thread-item.active {
      color: var(--accent);
    }
    .thread-item:last-child, .attachment-item:last-child { border-bottom: none; padding-bottom: 0; }
    .thread-item:first-child, .attachment-item:first-child { padding-top: 0; }
    .thread-item .subject-line {
      font-size: 14px;
      font-weight: 650;
      line-height: 1.5;
    }
    .thread-item .meta-line {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.4;
    }
    .attachment-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 12px;
    }
    .attachment-item {
      display: grid;
      gap: 8px;
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: var(--panel-2);
    }
    .attachment-preview {
      width: 100%;
      aspect-ratio: 16 / 10;
      border-radius: 10px;
      overflow: hidden;
      background: #0a0d0b;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    .attachment-preview img {
      width: 100%;
      height: 100%;
      object-fit: cover;
      display: block;
    }
    .attachment-fallback {
      color: var(--muted);
      font: 700 12px/1 var(--mono);
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .attachment-text {
      color: var(--muted);
      white-space: pre-wrap;
      font-size: 12px;
      line-height: 1.45;
      max-height: 120px;
      overflow: auto;
    }
    .message-header {
      display: grid;
      gap: 4px;
    }
    .detail-actions {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .message-title {
      font-size: 24px;
      line-height: 1.3;
      font-weight: 700;
    }
    .message-subtitle {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
    }
    .empty { color: var(--muted); padding: 18px; }
    .hidden { display: none !important; }
    .draft-item {
      padding: 16px;
      border-bottom: 1px solid var(--line);
      cursor: pointer;
    }
    .draft-item.active {
      background: rgba(184, 255, 77, 0.08);
    }
    .draft-item-title {
      font-size: 18px;
      font-weight: 700;
      line-height: 1.2;
    }
    .draft-item-meta {
      margin-top: 6px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.4;
    }
    .draft-form {
      flex: 1;
      min-height: 0;
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    .draft-field {
      display: grid;
      gap: 8px;
    }
    .draft-body-field {
      flex: 1;
      display: flex;
      flex-direction: column;
      min-height: 0;
    }
    .draft-body-field textarea {
      flex: 1;
      min-height: 320px;
      resize: none;
      line-height: 1.7;
    }
    .draft-preview-shell {
      display: grid;
      gap: 8px;
      min-height: 0;
    }
    .draft-preview {
      min-height: 180px;
      border: 1px solid var(--line);
      border-radius: 16px;
      background: #0d120f;
      padding: 18px;
      overflow: auto;
    }
    .draft-preview p {
      margin: 0 0 16px 0;
      color: var(--text);
      font-size: 15px;
      line-height: 1.7;
    }
    .draft-field label {
      color: var(--muted);
      font: 700 11px/1 var(--mono);
      letter-spacing: 0.12em;
      text-transform: uppercase;
    }
    .draft-recipient-details {
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #0d120f;
      overflow: hidden;
    }
    .draft-recipient-summary {
      list-style: none;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 14px;
      padding: 12px 14px;
      cursor: pointer;
      user-select: none;
    }
    .draft-recipient-summary::-webkit-details-marker {
      display: none;
    }
    .draft-recipient-summary::after {
      content: "+";
      color: var(--accent);
      font: 700 14px/1 var(--mono);
      flex: 0 0 auto;
    }
    .draft-recipient-details[open] .draft-recipient-summary::after {
      content: "−";
    }
    .draft-recipient-copy {
      display: grid;
      gap: 4px;
      min-width: 0;
    }
    .draft-recipient-kicker {
      color: var(--muted);
      font: 700 11px/1 var(--mono);
      letter-spacing: 0.12em;
      text-transform: uppercase;
    }
    .draft-recipient-preview {
      color: var(--text);
      font-size: 14px;
      line-height: 1.45;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .draft-recipient-fields {
      display: grid;
      gap: 12px;
      padding: 0 14px 14px;
      border-top: 1px solid rgba(255,255,255,0.06);
    }
    .draft-attachments-shell {
      gap: 10px;
    }
    .draft-attachments-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }
    .draft-attachment-list {
      display: grid;
      gap: 10px;
    }
    .draft-attachment-item {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 14px;
      padding: 12px 14px;
      border: 1px solid var(--line);
      border-radius: 14px;
      background: #0d120f;
    }
    .draft-attachment-copy {
      display: grid;
      gap: 4px;
      min-width: 0;
    }
    .draft-attachment-link {
      color: var(--text);
      font-weight: 600;
      text-decoration: none;
      word-break: break-word;
    }
    .draft-attachment-link:hover {
      color: var(--accent);
    }
    .draft-attachment-meta, .draft-attachment-empty {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.4;
    }
    .draft-actions {
      display: flex;
      align-items: center;
      justify-content: flex-start;
      gap: 10px;
    }
    .draft-status {
      color: var(--muted);
      font-size: 13px;
    }
    @media (max-width: 1080px) {
      .stats { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .main { grid-template-columns: 1fr; }
      .thread-list { max-height: 320px; }
      .detail-grid { grid-template-columns: 1fr; }
      .detail-grid .item-action { justify-content: flex-start; }
      .draft-body-field textarea { min-height: 240px; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <div class="topbar">
      <div class="brand-lockup">
        <h1 class="hero-title">CMAIL</h1>
      </div>
      <div class="topbar-actions">
        <div class="tabbar" role="tablist" aria-label="Mail views">
          <button class="tab-button active" type="button" id="tabInbox" role="tab" aria-selected="true">Correspondence</button>
          <button class="tab-button" type="button" id="tabDrafts" role="tab" aria-selected="false">Drafts</button>
          <a class="tab-button" href="/calendar">Calendar</a>
        </div>
        <span class="badge status-pill" id="syncBadge">syncing…</span>
      </div>
    </div>
    <section class="main">
      <section class="panel">
        <div class="panel-head">
          <h2 id="panelTitle">correspondence</h2>
          <div class="panel-actions">
            <button class="secondary hidden" type="button" id="newDraftButton">new draft</button>
            <span class="badge" id="threadCount">0 contacts</span>
          </div>
        </div>
        <div class="panel-search" id="correspondenceSearchWrap">
          <input id="correspondenceSearch" type="search" placeholder="search name or email" autocomplete="off" spellcheck="false">
        </div>
        <div class="thread-list" id="threadList"></div>
      </section>
      <section class="panel detail" id="detailPanel">
        <div class="empty">No correspondence selected yet.</div>
      </section>
    </section>
  </div>
  <div class="confirm-layer hidden" id="confirmLayer" aria-hidden="true"></div>
  <script>
    const OVERVIEW_CACHE_STORAGE_KEY = "lifeops.mail.overviewCache";
    const DRAFTS_CACHE_STORAGE_KEY = "lifeops.mail.draftsCache";
    const CONTACTS_CACHE_STORAGE_KEY = "lifeops.mail.contactsCache";
    const SELECTED_CONTACT_STORAGE_KEY = "lifeops.mail.selectedContact";
    const SELECTED_MESSAGE_STORAGE_KEY = "lifeops.mail.selectedMessage";
    const ACTIVE_VIEW_STORAGE_KEY = "lifeops.mail.activeView";
    const CORRESPONDENCE_QUERY_STORAGE_KEY = "lifeops.mail.correspondenceQuery";
    const SELECTED_DRAFT_STORAGE_KEY = "lifeops.mail.selectedDraft";
    const EXPANDED_QUOTED_MESSAGES_STORAGE_KEY = "lifeops.mail.expandedQuotedMessages";
    const VIEWED_MESSAGE_IDS_STORAGE_KEY = "lifeops.mail.viewedMessageIds";
    const VIEWED_MESSAGE_IDS_SEEDED_STORAGE_KEY = "lifeops.mail.viewedMessageIdsSeeded";
    const PENDING_DELETE_CONTACTS_STORAGE_KEY = "lifeops.mail.pendingDeletedContacts";
    const PENDING_DELETE_MESSAGES_STORAGE_KEY = "lifeops.mail.pendingDeletedMessages";
    const DELETE_QUEUE_STORAGE_KEY = "lifeops.mail.deleteQueue";
    const DRAFT_SAVE_QUEUE_STORAGE_KEY = "lifeops.mail.draftSaveQueue";
    const SERVER_BOOTSTRAP_OVERVIEW = __INITIAL_OVERVIEW_JSON__;
    const CMAIL_SIGNATURE_TEXT = __CMAIL_SIGNATURE_TEXT_JSON__;
    const CMAIL_KNOWN_SIGNATURE_TEXTS = __CMAIL_KNOWN_SIGNATURE_TEXTS_JSON__;
    const CMAIL_SIGNATURE_PREVIEW_HTML = __CMAIL_SIGNATURE_PREVIEW_HTML_JSON__;
    const CORRESPONDENCE_SOURCE = "correspondence";

    function loadStoredSet(storageKey) {
      try {
        const raw = window.localStorage.getItem(storageKey);
        if (!raw) return new Set();
        const values = JSON.parse(raw);
        if (!Array.isArray(values)) return new Set();
        return new Set(values.map((value) => String(value || "")).filter(Boolean));
      } catch (_error) {
        return new Set();
      }
    }

    function loadStoredJson(storageKey) {
      try {
        const raw = window.localStorage.getItem(storageKey);
        if (!raw) return null;
        return JSON.parse(raw);
      } catch (_error) {
        return null;
      }
    }

    function loadDeleteQueue() {
      const payload = loadStoredJson(DELETE_QUEUE_STORAGE_KEY);
      if (!Array.isArray(payload)) return [];
      return payload
        .map((entry) => (entry && typeof entry === "object" ? entry : null))
        .filter(Boolean)
        .map((entry) => ({
          id: String(entry.id || ""),
          kind: String(entry.kind || ""),
          contactKey: String(entry.contactKey || ""),
          messageId: Number(entry.messageId || 0) || null,
          queuedAt: String(entry.queuedAt || ""),
        }))
        .filter((entry) => entry.id && (entry.kind === "contact" || entry.kind === "message"));
    }

    function loadDraftSaveQueue() {
      const payload = loadStoredJson(DRAFT_SAVE_QUEUE_STORAGE_KEY);
      if (!Array.isArray(payload)) return [];
      return payload
        .map((entry) => (entry && typeof entry === "object" ? entry : null))
        .filter(Boolean)
        .map((entry) => ({
          localId: Number(entry.localId || 0) || 0,
          remoteId: Number(entry.remoteId || 0) || 0,
          payload: entry.payload && typeof entry.payload === "object" ? entry.payload : {},
          queuedAt: String(entry.queuedAt || ""),
        }))
        .filter((entry) => entry.localId);
    }

    function loadStoredText(storageKey) {
      try {
        const raw = window.localStorage.getItem(storageKey);
        return raw ? String(raw) : null;
      } catch (_error) {
        return null;
      }
    }

    function normalizeDraftPreviewBody(text) {
      let clean = String(text || "").trimEnd();
      let changed = true;
      while (changed) {
        changed = false;
        for (const signature of CMAIL_KNOWN_SIGNATURE_TEXTS) {
          const marker = String(signature || "").trim();
          if (marker && clean.endsWith(marker)) {
            clean = clean.slice(0, clean.length - marker.length).trimEnd();
            changed = true;
            break;
          }
        }
      }
      return clean;
    }

    function paragraphHtml(text) {
      const clean = String(text || "").trim();
      if (!clean) return "";
      return clean
        .split(/\\n{2,}/)
        .map((paragraph) => paragraph.trim())
        .filter(Boolean)
        .map((paragraph) => `<p>${escapeHtml(paragraph).replace(/\\n/g, "<br>")}</p>`)
        .join("");
    }

    function composeDraftPreviewHtml(bodyText) {
      const unsigned = normalizeDraftPreviewBody(bodyText);
      const bodyMarkup = paragraphHtml(unsigned);
      return bodyMarkup ? `${bodyMarkup}${CMAIL_SIGNATURE_PREVIEW_HTML}` : CMAIL_SIGNATURE_PREVIEW_HTML;
    }

    function persistPendingDeleteState() {
      try {
        window.localStorage.setItem(
          PENDING_DELETE_CONTACTS_STORAGE_KEY,
          JSON.stringify(Array.from(state.pendingDeletedContactKeys)),
        );
        window.localStorage.setItem(
          PENDING_DELETE_MESSAGES_STORAGE_KEY,
          JSON.stringify(Array.from(state.pendingDeletedMessageIds)),
        );
      } catch (_error) {
        // Ignore local storage failures; the optimistic UI can still work in-memory.
      }
    }

    function persistDeleteQueue() {
      try {
        window.localStorage.setItem(DELETE_QUEUE_STORAGE_KEY, JSON.stringify(state.deleteQueue));
      } catch (_error) {
        // Ignore local storage failures; in-memory queueing still works for this session.
      }
    }

    function persistDraftSaveQueue() {
      try {
        window.localStorage.setItem(DRAFT_SAVE_QUEUE_STORAGE_KEY, JSON.stringify(state.draftSaveQueue));
      } catch (_error) {
        // Ignore local storage failures; in-memory queueing still works for this session.
      }
    }

    function persistOverviewCache(payload) {
      try {
        const minimalPayload = {
          message_count: payload?.message_count || 0,
          contact_count: payload?.contact_count || 0,
          messages: payload?.messages || [],
          contacts: payload?.contacts || [],
          cloudflare_queue: payload?.cloudflare_queue || {},
          cloudflare_sync: payload?.cloudflare_sync || {},
        };
        window.localStorage.setItem(OVERVIEW_CACHE_STORAGE_KEY, JSON.stringify(minimalPayload));
      } catch (_error) {
        // Ignore local storage failures; the UI can still use the live response.
      }
    }

    function loadCachedDrafts() {
      const payload = loadStoredJson(DRAFTS_CACHE_STORAGE_KEY);
      if (!Array.isArray(payload)) return [];
      return payload
        .map((draft) => (draft && typeof draft === "object" ? draft : null))
        .filter(Boolean);
    }

    function loadCachedContacts() {
      const payload = loadStoredJson(CONTACTS_CACHE_STORAGE_KEY);
      if (!Array.isArray(payload)) return [];
      return payload
        .map((contact) => (contact && typeof contact === "object" ? contact : null))
        .filter(Boolean);
    }

    function persistDraftsCache(drafts) {
      try {
        window.localStorage.setItem(DRAFTS_CACHE_STORAGE_KEY, JSON.stringify(drafts || []));
      } catch (_error) {
        // Ignore local storage failures; in-memory drafts still work for the current session.
      }
    }

    function persistContactsCache(contacts) {
      try {
        window.localStorage.setItem(CONTACTS_CACHE_STORAGE_KEY, JSON.stringify(contacts || []));
      } catch (_error) {
        // Ignore local storage failures; in-memory contacts still work for the current session.
      }
    }

    function persistExpandedQuotedMessages() {
      try {
        window.localStorage.setItem(
          EXPANDED_QUOTED_MESSAGES_STORAGE_KEY,
          JSON.stringify(Array.from(state.expandedQuotedMessageIds)),
        );
      } catch (_error) {
        // Ignore local storage failures.
      }
    }

    function persistViewedMessageIds() {
      try {
        window.localStorage.setItem(
          VIEWED_MESSAGE_IDS_STORAGE_KEY,
          JSON.stringify(Array.from(state.viewedMessageIds)),
        );
        window.localStorage.setItem(VIEWED_MESSAGE_IDS_SEEDED_STORAGE_KEY, "1");
      } catch (_error) {
        // Ignore local storage failures.
      }
    }

    function persistSelectionState() {
      try {
        if (state.selectedContactKey) {
          window.localStorage.setItem(SELECTED_CONTACT_STORAGE_KEY, String(state.selectedContactKey));
        } else {
          window.localStorage.removeItem(SELECTED_CONTACT_STORAGE_KEY);
        }
        if (state.selectedId) {
          window.localStorage.setItem(SELECTED_MESSAGE_STORAGE_KEY, String(state.selectedId));
        } else {
          window.localStorage.removeItem(SELECTED_MESSAGE_STORAGE_KEY);
        }
        if (state.selectedDraftId) {
          window.localStorage.setItem(SELECTED_DRAFT_STORAGE_KEY, String(state.selectedDraftId));
        } else {
          window.localStorage.removeItem(SELECTED_DRAFT_STORAGE_KEY);
        }
        window.localStorage.setItem(ACTIVE_VIEW_STORAGE_KEY, state.activeView);
        window.localStorage.setItem(CORRESPONDENCE_QUERY_STORAGE_KEY, state.correspondenceQuery || "");
      } catch (_error) {
        // Ignore local storage failures.
      }
    }

    function normalizedSearchText(value) {
      return String(value || "").trim().toLowerCase();
    }

    function filterContacts(contacts) {
      const query = normalizedSearchText(state.correspondenceQuery);
      if (!query) return contacts || [];
      return (contacts || []).filter((contact) => {
        const haystack = [
          contact?.contact_label,
          contact?.contact_name,
          contact?.contact_email,
          contact?.contact_key,
        ].map((value) => normalizedSearchText(value)).join(" ");
        return haystack.includes(query);
      });
    }

    function contactLatestTimestamp(contact) {
      const directValue = timestampValue(contact?.happened_at || "");
      if (directValue > 0) return directValue;
      const threadValues = Array.isArray(contact?.threads)
        ? contact.threads.map((thread) => timestampValue(thread?.latest_happened_at || "")).filter((value) => value > 0)
        : [];
      if (threadValues.length) return Math.max(...threadValues);
      const messageValues = messagesForContact(String(contact?.contact_key || ""))
        .map((message) => timestampValue(message?.happened_at || ""))
        .filter((value) => value > 0);
      return messageValues.length ? Math.max(...messageValues) : 0;
    }

    function displayedContacts(contacts) {
      return filterContacts(contacts || [])
        .slice()
        .sort((left, right) => {
          const leftUnread = contactHasUnread(String(left?.contact_key || "")) ? 1 : 0;
          const rightUnread = contactHasUnread(String(right?.contact_key || "")) ? 1 : 0;
          if (leftUnread !== rightUnread) {
            return rightUnread - leftUnread;
          }
          const happenedDelta = contactLatestTimestamp(right) - contactLatestTimestamp(left);
          if (happenedDelta !== 0) {
            return happenedDelta;
          }
          return String(left?.contact_label || left?.contact_email || left?.contact_key || "")
            .localeCompare(
              String(right?.contact_label || right?.contact_email || right?.contact_key || ""),
              undefined,
              { sensitivity: "base" },
            );
        });
    }

    const state = {
      activeView: loadStoredText(ACTIVE_VIEW_STORAGE_KEY) === "drafts" ? "drafts" : "inbox",
      correspondenceQuery: loadStoredText(CORRESPONDENCE_QUERY_STORAGE_KEY)?.slice(0, 200) || "",
      selectedContactKey: loadStoredText(SELECTED_CONTACT_STORAGE_KEY),
      selectedThreadKey: null,
      selectedId: Number(loadStoredText(SELECTED_MESSAGE_STORAGE_KEY) || "") || null,
      selectedDraftId: Number(loadStoredText(SELECTED_DRAFT_STORAGE_KEY) || "") || null,
      overview: null,
      detail: null,
      drafts: loadCachedDrafts(),
      contacts: loadCachedContacts(),
      listSignature: "",
      mailboxVersionSignature: "",
      pendingDelete: null,
      pendingDeletedContactKeys: loadStoredSet(PENDING_DELETE_CONTACTS_STORAGE_KEY),
      pendingDeletedMessageIds: loadStoredSet(PENDING_DELETE_MESSAGES_STORAGE_KEY),
      deleteQueue: loadDeleteQueue(),
      draftSaveQueue: loadDraftSaveQueue(),
      expandedQuotedMessageIds: loadStoredSet(EXPANDED_QUOTED_MESSAGES_STORAGE_KEY),
      viewedMessageIds: loadStoredSet(VIEWED_MESSAGE_IDS_STORAGE_KEY),
      viewedMessageIdsSeeded: loadStoredText(VIEWED_MESSAGE_IDS_SEEDED_STORAGE_KEY) === "1",
      detailCache: new Map(),
      pendingDetailLoads: new Map(),
      prefetchScheduled: false,
      deleteFlushInFlight: false,
      lastRefreshStartedAt: 0,
      draftStatus: "",
      draftSaveInFlight: false,
      draftSaveFlushInFlight: false,
      contactLookupInFlight: null,
      draftComposerSeed: null,
    };

    function reconcilePendingDeleteState() {
      const queuedContactKeys = new Set(
        state.deleteQueue
          .filter((entry) => entry.kind === "contact" && entry.contactKey)
          .map((entry) => String(entry.contactKey)),
      );
      const queuedMessageIds = new Set(
        state.deleteQueue
          .filter((entry) => entry.kind === "message" && entry.messageId)
          .map((entry) => String(entry.messageId)),
      );
      let changed = false;
      for (const contactKey of Array.from(state.pendingDeletedContactKeys)) {
        if (!queuedContactKeys.has(contactKey)) {
          state.pendingDeletedContactKeys.delete(contactKey);
          changed = true;
        }
      }
      for (const messageId of Array.from(state.pendingDeletedMessageIds)) {
        if (!queuedMessageIds.has(messageId)) {
          state.pendingDeletedMessageIds.delete(messageId);
          changed = true;
        }
      }
      if (changed) {
        persistPendingDeleteState();
      }
    }

    reconcilePendingDeleteState();

    const $ = (id) => document.getElementById(id);

    function escapeHtml(value) {
      return String(value || "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
    }

    async function fetchJson(path) {
      const response = await fetch(path);
      if (!response.ok) throw new Error(await response.text());
      return await response.json();
    }

    async function postJson(path, payload = {}, options = {}) {
      const response = await fetch(path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        keepalive: Boolean(options.keepalive),
      });
      if (!response.ok) throw new Error(await response.text());
      return await response.json();
    }

    function normalizedContacts(payload) {
      const contacts = Array.isArray(payload?.contacts) ? payload.contacts : [];
      return contacts
        .map((contact) => (contact && typeof contact === "object" ? contact : null))
        .filter(Boolean);
    }

    function contactOptionMarkup(contacts) {
      return (contacts || [])
        .map((contact) => {
          const address = String(contact.address || contact.email || "");
          const label = String(contact.label || contact.display_name || contact.email || address);
          if (!address) return "";
          return `<option value="${escapeHtml(address)}">${escapeHtml(label)}</option>`;
        })
        .filter(Boolean)
        .join("");
    }

    function updateDraftContactSuggestions(contacts = state.contacts) {
      const list = $("draftContactSuggestions");
      if (!list) return;
      list.innerHTML = contactOptionMarkup(contacts);
    }

    async function loadContacts({ query = "", limit = 200 } = {}) {
      const params = new URLSearchParams({ limit: String(limit) });
      const cleanQuery = String(query || "").trim();
      if (cleanQuery) params.set("query", cleanQuery);
      const payload = await fetchJson(`/api/contacts?${params.toString()}`);
      const contacts = normalizedContacts(payload);
      if (!cleanQuery) {
        state.contacts = contacts;
        persistContactsCache(contacts);
      }
      updateDraftContactSuggestions(cleanQuery ? contacts : state.contacts);
      return contacts;
    }

    async function getDetail(id) {
      const key = String(id || "");
      if (!key) return null;
      if (state.detailCache.has(key)) {
        return state.detailCache.get(key);
      }
      if (state.pendingDetailLoads.has(key)) {
        return await state.pendingDetailLoads.get(key);
      }
      const request = fetchJson(`/api/communications/${id}`)
        .then((payload) => {
          state.detailCache.set(key, payload);
          return payload;
        })
        .finally(() => {
          state.pendingDetailLoads.delete(key);
        });
      state.pendingDetailLoads.set(key, request);
      return await request;
    }

    function pruneDetailCache() {
      const validIds = new Set((state.overview?.messages || []).map((message) => String(message.id)));
      for (const key of Array.from(state.detailCache.keys())) {
        if (!validIds.has(key)) {
          state.detailCache.delete(key);
        }
      }
      for (const key of Array.from(state.pendingDetailLoads.keys())) {
        if (!validIds.has(key)) {
          state.pendingDetailLoads.delete(key);
        }
      }
    }

    function messageIsUnread(message) {
      if (!message) return false;
      if (String(message.direction || "").toLowerCase() === "outbound") return false;
      const readKeys = messageReadKeys(message);
      if (!readKeys.length) return false;
      return !readKeys.some((key) => state.viewedMessageIds.has(key));
    }

    function contactHasUnread(contactKey) {
      return messagesForContact(contactKey).some((message) => messageIsUnread(message));
    }

    function primeViewedMessagesFromOverview(payload) {
      const messages = Array.isArray(payload?.messages) ? payload.messages : [];
      let changed = false;
      let seededNow = false;
      if (!state.viewedMessageIdsSeeded) {
        for (const message of messages) {
          for (const readKey of messageReadKeys(message)) {
            if (state.viewedMessageIds.has(readKey)) continue;
            state.viewedMessageIds.add(readKey);
            changed = true;
          }
        }
        state.viewedMessageIdsSeeded = true;
        seededNow = true;
      } else {
        for (const message of messages) {
          if (String(message?.direction || "").toLowerCase() !== "outbound") continue;
          for (const readKey of messageReadKeys(message)) {
            if (state.viewedMessageIds.has(readKey)) continue;
            state.viewedMessageIds.add(readKey);
            changed = true;
          }
        }
      }
      if (changed || seededNow) {
        persistViewedMessageIds();
      }
    }

    function markMessageViewed(messageOrId) {
      const readKeys = typeof messageOrId === "object"
        ? messageReadKeys(messageOrId)
        : [`local-id:${String(messageOrId || "").trim()}`].filter((key) => key !== "local-id:");
      let changed = false;
      for (const readKey of readKeys) {
        if (!readKey || state.viewedMessageIds.has(readKey)) continue;
        state.viewedMessageIds.add(readKey);
        changed = true;
      }
      if (!changed) return false;
      state.viewedMessageIdsSeeded = true;
      persistViewedMessageIds();
      return true;
    }

    function normalizedSubject(subject) {
      const clean = String(subject || "").trim();
      if (!clean) return "";
      const normalized = clean.replace(/^(?:(?:re|fwd?|aw|sv)\\s*:\\s*)+/i, "").trim();
      return normalized || clean;
    }

    function threadGroupKey(message) {
      const explicitKey = String(message.thread_key || "").trim();
      if (explicitKey) return explicitKey;
      const subject = normalizedSubject(message.subject || "");
      if (subject) return `subject:${subject.toLowerCase()}`;
      const messageId = String(message.message_id || "").trim();
      if (messageId) return `message:${messageId}`;
      return `message:${message.id}`;
    }

    function groupContacts(messages) {
      const grouped = new Map();
      const orderedKeys = [];
      for (const message of messages) {
        const contactKey = String(message.contact_key || `contact:${message.id}`);
        if (!grouped.has(contactKey)) {
          grouped.set(contactKey, {
            contact_key: contactKey,
            contact_label: String(message.contact_label || ""),
            contact_name: String(message.contact_name || ""),
            contact_email: String(message.contact_email || ""),
            latest_message_id: message.id,
            subject: message.subject,
            happened_at: message.happened_at,
            count: 0,
            message_ids: [],
            threads: [],
          });
          orderedKeys.push(contactKey);
        }
        const contact = grouped.get(contactKey);
        contact.count += 1;
        contact.message_ids.push(message.id);

        const nextThreadKey = threadGroupKey(message);
        let thread = contact.threads.find((entry) => String(entry.thread_key || "") === nextThreadKey);
        if (!thread) {
          thread = {
            thread_key: nextThreadKey,
            title: normalizedSubject(String(message.subject || "")) || String(message.subject || "(no subject)"),
            latest_message_id: message.id,
            latest_happened_at: String(message.happened_at || ""),
            count: 0,
            message_ids: [],
          };
          contact.threads.push(thread);
        }
        thread.count += 1;
        thread.message_ids.push(message.id);
      }
      return orderedKeys.map((key) => grouped.get(key));
    }

    function normalizeOverviewPayload(payload, options = {}) {
      if (!payload) return payload;
      const allowPendingResolution = options.allowPendingResolution !== false;
      const pendingContactKeys = state.pendingDeletedContactKeys;
      const pendingMessageIds = state.pendingDeletedMessageIds;
      const serverContactKeys = new Set(
        (payload.contacts || []).map((contact) => String(contact.contact_key || "")).filter(Boolean),
      );
      const serverMessageIds = new Set(
        (payload.messages || []).map((message) => String(message.id || "")).filter(Boolean),
      );
      let pendingChanged = false;
      if (allowPendingResolution) {
        for (const contactKey of Array.from(pendingContactKeys)) {
          if (!serverContactKeys.has(contactKey)) {
            pendingContactKeys.delete(contactKey);
            pendingChanged = true;
          }
        }
        for (const messageId of Array.from(pendingMessageIds)) {
          if (!serverMessageIds.has(messageId)) {
            pendingMessageIds.delete(messageId);
            pendingChanged = true;
          }
        }
      }
      if (pendingChanged) {
        persistPendingDeleteState();
      }
      const messages = (payload.messages || []).filter((message) => {
        const messageId = String(message.id || "");
        const contactKey = String(message.contact_key || "");
        return !pendingMessageIds.has(messageId) && !pendingContactKeys.has(contactKey);
      });
      const details = {};
      for (const [messageId, detail] of Object.entries(payload.details || {})) {
        const detailContactKey = String(detail?.contact_key || "");
        if (pendingMessageIds.has(String(messageId)) || pendingContactKeys.has(detailContactKey)) {
          continue;
        }
        details[messageId] = detail;
      }
      const contacts = groupContacts(messages);
      return {
        ...payload,
        messages,
        details,
        contacts,
        message_count: messages.length,
        contact_count: contacts.length,
      };
    }

    async function prefetchVisibleDetails() {
      const messages = state.overview?.messages || [];
      for (const message of messages) {
        const key = String(message.id);
        if (state.detailCache.has(key) || state.pendingDetailLoads.has(key)) {
          continue;
        }
        try {
          await getDetail(message.id);
        } catch (_error) {
          // Ignore opportunistic prefetch failures. A direct click will retry.
        }
      }
    }

    function schedulePrefetch() {
      if (state.prefetchScheduled) return;
      state.prefetchScheduled = true;
      const runner = async () => {
        state.prefetchScheduled = false;
        await prefetchVisibleDetails();
      };
      if ("requestIdleCallback" in window) {
        window.requestIdleCallback(() => {
          runner().catch(() => {});
        }, { timeout: 500 });
      } else {
        window.setTimeout(() => {
          runner().catch(() => {});
        }, 0);
      }
    }

    function relativeSyncLabel(value) {
      if (!value) return "no successful sync yet";
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return value;
      const seconds = Math.max(0, Math.round((Date.now() - date.getTime()) / 1000));
      if (seconds < 60) return `${seconds}s ago`;
      const minutes = Math.round(seconds / 60);
      if (minutes < 60) return `${minutes}m ago`;
      const hours = Math.round(minutes / 60);
      if (hours < 24) return `${hours}h ago`;
      const days = Math.round(hours / 24);
      return `${days}d ago`;
    }

    function syncFreshnessSeconds(value) {
      if (!value) return null;
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return null;
      return Math.max(0, Math.round((Date.now() - date.getTime()) / 1000));
    }

    function timestampValue(value) {
      const date = new Date(value || "");
      return Number.isNaN(date.getTime()) ? 0 : date.getTime();
    }

    function humanTimestamp(value) {
      if (!value) return "unknown time";
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return String(value);
      return new Intl.DateTimeFormat(undefined, {
        month: "short",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
      }).format(date);
    }

    function exactSyncTimestamp(value) {
      if (!value) return null;
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return String(value);
      return new Intl.DateTimeFormat(undefined, {
        month: "short",
        day: "numeric",
        hour: "numeric",
        minute: "2-digit",
        second: "2-digit",
      }).format(date);
    }

    function renderSyncBadge(payload) {
      const sync = payload.cloudflare_sync || {};
      const cloudflare = payload.cloudflare_queue || {};
      const badge = $("syncBadge");
      const exactLastSync = exactSyncTimestamp(sync.last_sync_at || sync.last_success_at);
      const exactLastSuccess = exactSyncTimestamp(sync.last_success_at);
      const lastSyncLabel = exactLastSync || "waiting for first sync";
      const lastSuccessLabel = exactLastSuccess || lastSyncLabel;
      if (cloudflare.pending_count) {
        badge.textContent = `syncing… last ok ${lastSuccessLabel}`;
        return;
      }
      if (sync.status === "degraded") {
        badge.textContent = `delayed · last ok ${lastSuccessLabel}`;
        return;
      }
      badge.textContent = `last sync ${lastSyncLabel}`;
    }

    function renderChrome() {
      const inboxActive = state.activeView === "inbox";
      $("tabInbox").classList.toggle("active", inboxActive);
      $("tabInbox").setAttribute("aria-selected", inboxActive ? "true" : "false");
      $("tabDrafts").classList.toggle("active", !inboxActive);
      $("tabDrafts").setAttribute("aria-selected", inboxActive ? "false" : "true");
      $("panelTitle").textContent = inboxActive ? "correspondence" : "drafts";
      $("newDraftButton").classList.toggle("hidden", inboxActive);
      $("correspondenceSearchWrap").classList.toggle("hidden", !inboxActive);
      $("correspondenceSearch").value = state.correspondenceQuery || "";
    }

    function mailboxVersionSignature(payload) {
      const version = payload?.mailbox_version || {};
      return JSON.stringify([
        version.message_count || 0,
        version.contact_count || 0,
        version.latest_message_id || 0,
        version.latest_happened_at || "",
      ]);
    }

    function mergeSyncState(payload) {
      if (!state.overview) {
        state.overview = normalizeOverviewPayload(payload || {}, { allowPendingResolution: false });
      } else {
        state.overview = {
          ...state.overview,
          cloudflare_sync: payload?.cloudflare_sync || state.overview.cloudflare_sync || {},
          cloudflare_queue: payload?.cloudflare_queue || state.overview.cloudflare_queue || {},
          mailbox_version: payload?.mailbox_version || state.overview.mailbox_version || {},
        };
      }
      renderSyncBadge(state.overview);
    }

    async function loadSyncStatus() {
      const payload = await fetchJson(`/api/sync-status?source=${encodeURIComponent(CORRESPONDENCE_SOURCE)}&sync=1`);
      const nextSignature = mailboxVersionSignature(payload);
      const mailboxChanged = nextSignature !== state.mailboxVersionSignature;
      mergeSyncState(payload);
      state.mailboxVersionSignature = nextSignature;
      return { payload, mailboxChanged };
    }

    function closeConfirm() {
      state.pendingDelete = null;
      const layer = $("confirmLayer");
      layer.classList.add("hidden");
      layer.setAttribute("aria-hidden", "true");
      layer.innerHTML = "";
    }

    function renderConfirm() {
      const layer = $("confirmLayer");
      const pending = state.pendingDelete;
      if (!pending) {
        closeConfirm();
        return;
      }
      const objectLabel = pending.kind === "contact" ? "contact" : pending.kind === "message" ? "message" : "draft";
      const archiveCopy = pending.kind === "contact"
        ? "This contact and the visible correspondence with them will move to the 30-day archive before purge."
        : pending.kind === "message"
          ? "This message will move to the 30-day archive before purge."
          : "This draft will be sent through your configured outbound mail path and then leave the drafts list.";
      const actionLabel = pending.kind === "send-draft" ? "Send draft" : "Archive";
      const actionClass = pending.kind === "send-draft" ? "primary" : "danger";
      layer.classList.remove("hidden");
      layer.setAttribute("aria-hidden", "false");
      layer.innerHTML = `
        <div class="confirm-card" role="dialog" aria-modal="true" aria-labelledby="confirmTitle">
          <div class="confirm-title" id="confirmTitle">Ya sure?</div>
          <div class="confirm-copy">
            ${escapeHtml(actionLabel)} this <strong>${escapeHtml(objectLabel)}</strong> now?
            <br><br>
            <strong>${escapeHtml(pending.label || "(unknown)")}</strong>
            <br><br>
            ${escapeHtml(archiveCopy)}
          </div>
          <div class="confirm-actions">
            <button class="secondary" type="button" id="confirmCancel">Keep it</button>
            <button class="${actionClass}" type="button" id="confirmDelete">${escapeHtml(actionLabel)}</button>
          </div>
        </div>
      `;
      layer.addEventListener("click", (event) => {
        if (event.target === layer) closeConfirm();
      }, { once: true });
      const cancelButton = $("confirmCancel");
      const deleteButton = $("confirmDelete");
      if (cancelButton) {
        cancelButton.addEventListener("click", () => closeConfirm(), { once: true });
      }
      if (deleteButton) {
        deleteButton.addEventListener("click", async () => {
          const current = state.pendingDelete;
          if (!current) return;
          closeConfirm();
          try {
            if (current.kind === "contact") {
              await deleteContact(current.contactKey);
            } else if (current.kind === "message") {
              await deleteMessage(current.messageId);
            } else if (current.kind === "send-draft") {
              await sendDraft(current.draftId);
            }
          } catch (_error) {
            // The optimistic UI will restore from local state on failure.
          }
        }, { once: true });
      }
    }

    function renderContactList(payload) {
      const contacts = displayedContacts(payload.contacts || []);
      const signature = JSON.stringify([
        state.correspondenceQuery || "",
        contacts.map((contact) => [
          contact.contact_key,
          contact.latest_message_id,
          contact.count,
          contact.happened_at,
          contactHasUnread(contact.contact_key) ? 1 : 0,
          (contact.threads || []).map((thread) => [thread.thread_key, thread.latest_message_id, thread.count]),
        ]),
      ]);
      $("threadCount").textContent = `${contacts.length} contact${contacts.length === 1 ? "" : "s"}`;
      if (!contacts.length) {
        $("threadList").innerHTML = `<div class="empty">${state.correspondenceQuery ? "No correspondence matches that search." : "No correspondence yet."}</div>`;
        state.listSignature = signature;
        return;
      }
      if (signature === state.listSignature) {
        for (const node of document.querySelectorAll(".thread[data-contact-key]")) {
          node.classList.toggle("active", node.getAttribute("data-contact-key") === state.selectedContactKey);
        }
        return;
      }
      state.listSignature = signature;
      $("threadList").innerHTML = contacts.map((contact) => `
        <div class="thread ${contact.contact_key === state.selectedContactKey ? "active" : ""}" data-contact-key="${escapeHtml(contact.contact_key)}">
          <div class="thread-main">
            <span class="unread-orb ${contactHasUnread(contact.contact_key) ? "" : "hidden"}" aria-hidden="true"></span>
            <div class="subject">${escapeHtml(contact.contact_label || "(unknown contact)")}</div>
          </div>
          <button class="delete-button" type="button" data-delete-contact="${escapeHtml(contact.contact_key)}" title="Archive this correspondence">×</button>
        </div>
      `).join("");
      for (const node of document.querySelectorAll(".thread[data-contact-key]")) {
        node.addEventListener("click", () => {
          const contactKey = node.getAttribute("data-contact-key") || "";
          selectContact(contactKey);
        });
      }
      for (const node of document.querySelectorAll("[data-delete-contact]")) {
        node.addEventListener("click", async (event) => {
          event.stopPropagation();
          const contactKey = node.getAttribute("data-delete-contact") || "";
          if (!contactKey) return;
          requestDeleteContact(contactKey);
        });
      }
    }

    function sortedDrafts(drafts) {
      return (drafts || [])
        .slice()
        .sort((left, right) => timestampValue(right.updated_at) - timestampValue(left.updated_at));
    }

    function selectedDraft() {
      if (state.selectedDraftId == null && state.draftComposerSeed) {
        return seededDraft(state.draftComposerSeed);
      }
      return sortedDrafts(state.drafts).find((draft) => draft.id === state.selectedDraftId) || null;
    }

    function renderDraftList() {
      const drafts = sortedDrafts(state.drafts);
      $("threadCount").textContent = `${drafts.length} draft${drafts.length === 1 ? "" : "s"}`;
      if (!drafts.length) {
        $("threadList").innerHTML = `<div class="empty">No drafts yet.</div>`;
        state.listSignature = "drafts:empty";
        return;
      }
      const signature = JSON.stringify(drafts.map((draft) => [draft.id, draft.updated_at, draft.subject, draft.to]));
      if (signature === state.listSignature) {
        for (const node of document.querySelectorAll(".draft-item[data-draft-id]")) {
          node.classList.toggle("active", Number(node.getAttribute("data-draft-id")) === state.selectedDraftId);
        }
        return;
      }
      state.listSignature = signature;
      $("threadList").innerHTML = drafts.map((draft) => `
        <div class="draft-item ${draft.id === state.selectedDraftId ? "active" : ""}" data-draft-id="${draft.id}">
          <div class="draft-item-title">${escapeHtml(draft.label || "(untitled draft)")}</div>
          <div class="draft-item-meta">
            ${escapeHtml(draft.to || "no recipient yet")}
            <br>
            ${escapeHtml(draft.snippet || "empty draft")} · ${escapeHtml(humanTimestamp(draft.updated_at || ""))}
          </div>
        </div>
      `).join("");
      for (const node of document.querySelectorAll(".draft-item[data-draft-id]")) {
        node.addEventListener("click", () => {
          const draftId = Number(node.getAttribute("data-draft-id"));
          if (!draftId) return;
          state.draftComposerSeed = null;
          state.selectedDraftId = draftId;
          persistSelectionState();
          renderCurrentSelection();
        });
      }
    }

    function messagesForContact(contactKey) {
      return (state.overview?.messages || [])
        .filter((message) => message.contact_key === contactKey)
        .slice()
        .sort((left, right) => timestampValue(right.happened_at) - timestampValue(left.happened_at));
    }

    function messageDirectionLabel(message) {
      return String(message?.direction || "").toLowerCase() === "outbound" ? "out" : "in";
    }

    function messageCounterpartyValue(message) {
      if (String(message?.direction || "").toLowerCase() === "outbound") {
        return String(message?.external_to || "").trim() || String(message?.contact_label || "").trim();
      }
      return String(message?.external_from || "").trim() || String(message?.contact_label || "").trim();
    }

    function messageReadKeys(message) {
      if (!message) return [];
      const keys = [];
      const pushKey = (prefix, value) => {
        const clean = String(value || "").trim();
        if (clean) keys.push(`${prefix}:${clean}`);
      };
      pushKey("read", message.read_key);
      pushKey("message-id", message.message_id);
      pushKey("external-id", message.external_id);
      const fallbackParts = [
        String(message.source || "").trim(),
        String(message.direction || "").trim(),
        String(message.contact_key || "").trim(),
        String(message.thread_key || "").trim(),
        String(message.happened_at || "").trim(),
        String(message.subject || "").trim(),
      ].filter(Boolean);
      if (fallbackParts.length >= 4) {
        pushKey("fallback", fallbackParts.join("|"));
      }
      pushKey("local-id", message.id);
      const legacyId = String(message.id || "").trim();
      if (legacyId) keys.push(legacyId);
      return Array.from(new Set(keys));
    }

    function contactRecord(contactKey) {
      return (state.overview?.contacts || []).find((contact) => contact.contact_key === contactKey) || null;
    }

    function threadsForContact(contactKey) {
      const contact = contactRecord(contactKey);
      return Array.isArray(contact?.threads) ? contact.threads : [];
    }

    function messagesForThread(contactKey, threadKey) {
      const contactMessages = messagesForContact(contactKey);
      if (!threadKey) return contactMessages;
      const activeThread = threadsForContact(contactKey).find((thread) => thread.thread_key === threadKey);
      if (!activeThread) return contactMessages;
      const messageIds = new Set(activeThread.message_ids || []);
      return contactMessages.filter((message) => messageIds.has(message.id));
    }

    function selectThread(threadKey) {
      state.selectedThreadKey = threadKey || null;
      const threadMessages = messagesForThread(state.selectedContactKey, state.selectedThreadKey);
      state.selectedId = threadMessages[0]?.id ?? null;
      if (state.selectedId) {
        loadDetail(state.selectedId);
      } else {
        $("detailPanel").innerHTML = `<div class="empty">No messages in this thread yet.</div>`;
      }
    }

    function renderCurrentSelection() {
      renderChrome();
      if (state.activeView === "drafts") {
        renderDraftSelection();
        return;
      }
      const contacts = displayedContacts(state.overview?.contacts || []);
      const selectedContactStillPresent = contacts.some((contact) => contact.contact_key === state.selectedContactKey);
      if (!selectedContactStillPresent) {
        state.selectedContactKey = contacts[0]?.contact_key ?? null;
      }
      state.selectedThreadKey = null;
      renderContactList(state.overview || {});
      const contactMessages = messagesForContact(state.selectedContactKey);
      const selectedMessageStillPresent = contactMessages.some((message) => message.id === state.selectedId);
      if (!selectedMessageStillPresent) {
        state.selectedId = contactMessages[0]?.id ?? null;
      }
      persistSelectionState();
      if (!state.selectedId) {
        $("detailPanel").classList.remove("draft-mode");
        $("detailPanel").innerHTML = `<div class="empty">${state.correspondenceQuery ? "No correspondence matches that search." : "No correspondence yet."}</div>`;
        return;
      }
      const cached = state.detailCache.get(String(state.selectedId));
      if (cached) {
        renderDetail(cached);
        renderContactList(state.overview || {});
        return;
      }
      loadDetail(state.selectedId).catch((error) => {
        $("detailPanel").innerHTML = `<div class="empty">${escapeHtml(error.message || String(error))}</div>`;
      });
    }

    function renderDraftSelection() {
      const drafts = sortedDrafts(state.drafts);
      const usingComposerSeed = state.selectedDraftId == null && Boolean(state.draftComposerSeed);
      const selectedStillPresent = drafts.some((draft) => draft.id === state.selectedDraftId);
      if (!usingComposerSeed && !selectedStillPresent) {
        state.selectedDraftId = drafts[0]?.id ?? null;
      }
      persistSelectionState();
      renderDraftList();
      renderDraftDetail(selectedDraft());
    }

    function applyOptimisticOverviewUpdate() {
      state.overview = normalizeOverviewPayload(state.overview, { allowPendingResolution: false });
      if (state.overview) {
        persistOverviewCache(state.overview);
      }
      pruneDetailCache();
      renderCurrentSelection();
    }

    function enqueueDelete(entry) {
      const id = String(entry?.id || "");
      if (!id) return;
      const filtered = state.deleteQueue.filter((queued) => queued.id !== id);
      state.deleteQueue = [
        ...filtered,
        {
          id,
          kind: String(entry.kind || ""),
          contactKey: String(entry.contactKey || ""),
          messageId: Number(entry.messageId || 0) || null,
          queuedAt: entry.queuedAt || new Date().toISOString(),
        },
      ];
      persistDeleteQueue();
    }

    function dequeueDelete(id) {
      const nextQueue = state.deleteQueue.filter((entry) => entry.id !== id);
      if (nextQueue.length === state.deleteQueue.length) return;
      state.deleteQueue = nextQueue;
      persistDeleteQueue();
    }

    function resolvePendingDelete(next) {
      if (!next) return;
      if (next.kind === "contact" && next.contactKey) {
        state.pendingDeletedContactKeys.delete(String(next.contactKey));
      }
      if (next.kind === "message" && next.messageId) {
        state.pendingDeletedMessageIds.delete(String(next.messageId));
      }
      persistPendingDeleteState();
    }

    async function flushDeleteQueue() {
      if (state.deleteFlushInFlight || !state.deleteQueue.length) return;
      state.deleteFlushInFlight = true;
      try {
        while (state.deleteQueue.length) {
          const next = state.deleteQueue[0];
          try {
            if (next.kind === "contact" && next.contactKey) {
              await postJson("/api/contacts/delete", { contact_key: next.contactKey }, { keepalive: true });
            } else if (next.kind === "message" && next.messageId) {
              await postJson(`/api/communications/${next.messageId}/delete`, {}, { keepalive: true });
            } else {
              dequeueDelete(next.id);
              continue;
            }
            resolvePendingDelete(next);
            dequeueDelete(next.id);
            await loadOverview({ sync: false });
          } catch (_error) {
            break;
          }
        }
      } finally {
        state.deleteFlushInFlight = false;
      }
    }

    function blankDraft() {
      return {
        id: null,
        subject: "",
        label: "(untitled draft)",
        to: "",
        cc: "",
        bcc: "",
        body_text: "",
        snippet: "",
        updated_at: "",
        in_reply_to: "",
        references: [],
        thread_key: "",
        attachments: [],
      };
    }

    function seededDraft(seed) {
      const next = {
        ...blankDraft(),
        ...(seed || {}),
      };
      next.subject = String(next.subject || "");
      next.to = String(next.to || "");
      next.cc = String(next.cc || "");
      next.bcc = String(next.bcc || "");
      next.body_text = String(next.body_text || "");
      next.in_reply_to = String(next.in_reply_to || "");
      next.references = Array.isArray(next.references)
        ? next.references.map((value) => String(value || "").trim()).filter(Boolean)
        : [];
      next.thread_key = String(next.thread_key || "");
      next.attachments = Array.isArray(next.attachments) ? next.attachments : [];
      next.label = localDraftLabel(next.subject || next.label || "");
      next.snippet = localDraftSnippet(next.body_text);
      return next;
    }

    function draftRecipientSummary(draft) {
      const values = [];
      const toValue = String(draft?.to || "").trim();
      const ccValue = String(draft?.cc || "").trim();
      const bccValue = String(draft?.bcc || "").trim();
      if (toValue) values.push(`To ${toValue}`);
      if (ccValue) values.push(`Cc ${ccValue}`);
      if (bccValue) values.push(`Bcc ${bccValue}`);
      return values.length ? values.join(" · ") : "Expand to add recipients";
    }

    function startNewDraft() {
      state.draftComposerSeed = seededDraft({});
      state.selectedDraftId = null;
      state.draftStatus = "";
      persistSelectionState();
      renderCurrentSelection();
    }

    function replyRecipientValue(message) {
      const direction = String(message?.direction || "").toLowerCase();
      if (direction === "outbound") {
        return String(message?.external_to || "").trim();
      }
      const scaffoldTo = String(message?.drafts?.reply?.to || "").trim();
      if (scaffoldTo) return scaffoldTo;
      return String(message?.external_reply_to || message?.external_from || "").trim();
    }

    function openReplyDraft(message) {
      if (!message) return;
      const replySeed = message?.drafts?.reply || {};
      state.draftComposerSeed = seededDraft({
        to: replyRecipientValue(message),
        cc: String(replySeed.cc || ""),
        subject: String(replySeed.subject || ""),
        in_reply_to: String(replySeed.in_reply_to || ""),
        references: Array.isArray(replySeed.references) ? replySeed.references : [],
        thread_key: String(replySeed.thread_key || ""),
      });
      state.selectedDraftId = null;
      state.draftStatus = "";
      persistSelectionState();
      setActiveView("drafts");
    }

    function requestSendDraft(draft) {
      if (!draft?.id) return;
      state.pendingDelete = {
        kind: "send-draft",
        draftId: draft.id,
        label: draft.subject || draft.label || "(untitled draft)",
      };
      renderConfirm();
    }

    function upsertDraft(draft) {
      if (!draft || !draft.id) return;
      const nextId = Number(draft.id);
      state.drafts = sortedDrafts([
        ...(state.drafts || []).filter((entry) => Number(entry?.id || 0) !== nextId),
        draft,
      ]);
      persistDraftsCache(state.drafts);
    }

    function removeDraftById(draftId) {
      const nextId = Number(draftId || 0);
      state.drafts = sortedDrafts((state.drafts || []).filter((entry) => Number(entry?.id || 0) !== nextId));
      persistDraftsCache(state.drafts);
    }

    function mergeDraftsWithPending(serverDrafts) {
      const merged = [...(serverDrafts || [])];
      const existingIds = new Set(merged.map((entry) => Number(entry?.id || 0)));
      for (const pending of state.draftSaveQueue) {
        const localId = Number(pending.localId || 0);
        if (!localId || existingIds.has(localId)) continue;
        const localDraft = (state.drafts || []).find((entry) => Number(entry?.id || 0) === localId);
        if (localDraft) {
          merged.push(localDraft);
          existingIds.add(localId);
        }
      }
      return sortedDrafts(merged);
    }

    function localDraftLabel(subject) {
      const clean = String(subject || "").trim();
      return clean || "(untitled draft)";
    }

    function localDraftSnippet(bodyText) {
      const lines = String(bodyText || "").split(/\\n+/).map((line) => line.trim()).filter(Boolean);
      if (!lines.length) return "";
      return lines[0].slice(0, 180);
    }

    function nextTemporaryDraftId() {
      return -Date.now() - Math.floor(Math.random() * 1000);
    }

    function queueDraftSave(entry) {
      const localId = Number(entry?.localId || 0);
      if (!localId) return;
      const remoteId = Number(entry?.remoteId || 0) || 0;
      const nextEntry = {
        localId,
        remoteId,
        payload: entry.payload || {},
        queuedAt: entry.queuedAt || new Date().toISOString(),
      };
      state.draftSaveQueue = [
        ...state.draftSaveQueue.filter((item) => Number(item.localId || 0) !== localId),
        nextEntry,
      ];
      persistDraftSaveQueue();
    }

    function dequeueDraftSave(localId) {
      const nextLocalId = Number(localId || 0);
      if (!nextLocalId) return;
      state.draftSaveQueue = state.draftSaveQueue.filter((entry) => Number(entry.localId || 0) !== nextLocalId);
      persistDraftSaveQueue();
    }

    function replaceDraftId(oldId, nextDraft) {
      const oldNumericId = Number(oldId || 0);
      const nextNumericId = Number(nextDraft?.id || 0);
      if (!nextNumericId) return;
      state.drafts = sortedDrafts(
        (state.drafts || []).map((entry) => {
          if (Number(entry?.id || 0) !== oldNumericId) return entry;
          return { ...entry, ...nextDraft };
        }).filter((entry) => Number(entry?.id || 0) !== oldNumericId || nextNumericId === oldNumericId)
      );
      state.drafts = sortedDrafts([
        ...(state.drafts || []).filter((entry) => Number(entry?.id || 0) !== oldNumericId),
        nextDraft,
      ]);
      if (state.selectedDraftId === oldNumericId) {
        state.selectedDraftId = nextNumericId;
      }
      state.draftSaveQueue = state.draftSaveQueue.map((entry) => (
        Number(entry.localId || 0) === oldNumericId
          ? { ...entry, localId: nextNumericId, remoteId: nextNumericId }
          : entry
      ));
      persistDraftsCache(state.drafts);
      persistDraftSaveQueue();
      persistSelectionState();
    }

    async function flushDraftSaveQueue() {
      if (state.draftSaveFlushInFlight || !state.draftSaveQueue.length) return;
      state.draftSaveFlushInFlight = true;
      try {
        while (state.draftSaveQueue.length) {
          const next = state.draftSaveQueue[0];
          try {
            const response = await postJson("/api/drafts", {
              ...(next.payload || {}),
              id: Number(next.remoteId || 0) > 0 ? Number(next.remoteId) : undefined,
            });
            const savedDraft = response?.draft || null;
            if (savedDraft) {
              replaceDraftId(next.localId, savedDraft);
              upsertDraft(savedDraft);
            }
            dequeueDraftSave(next.localId);
            state.draftStatus = "saved";
            renderDraftStatus();
            if (state.activeView === "drafts") {
              renderCurrentSelection();
            }
          } catch (_error) {
            state.draftStatus = "saved locally";
            renderDraftStatus();
            break;
          }
        }
      } finally {
        state.draftSaveFlushInFlight = false;
      }
    }

    function humanAttachmentSize(bytes) {
      const value = Number(bytes || 0);
      if (!Number.isFinite(value) || value <= 0) return "0 bytes";
      if (value < 1024) return `${value} bytes`;
      if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
      return `${(value / (1024 * 1024)).toFixed(1)} MB`;
    }

    async function saveDraftFromForm(options = {}) {
      const forceRemote = Boolean(options?.forceRemote);
      if (state.draftSaveInFlight) return null;
      const subjectInput = $("draftSubject");
      const toInput = $("draftTo");
      const ccInput = $("draftCc");
      const bccInput = $("draftBcc");
      const bodyInput = $("draftBody");
      if (!subjectInput || !toInput || !ccInput || !bccInput || !bodyInput) return null;
      state.draftSaveInFlight = true;
      state.draftStatus = "saving...";
      renderDraftStatus();
      try {
        const activeDraft = selectedDraft() || blankDraft();
        const localId = Number(state.selectedDraftId || 0) || nextTemporaryDraftId();
        const payload = {
          subject: subjectInput.value,
          to: toInput.value,
          cc: ccInput.value,
          bcc: bccInput.value,
          body_text: bodyInput.value,
          in_reply_to: String(activeDraft.in_reply_to || ""),
          references: Array.isArray(activeDraft.references) ? activeDraft.references : [],
          thread_key: String(activeDraft.thread_key || ""),
        };
        const localDraft = {
          id: localId,
          subject: payload.subject,
          label: localDraftLabel(payload.subject),
          to: payload.to,
          cc: payload.cc,
          bcc: payload.bcc,
          body_text: payload.body_text,
          snippet: localDraftSnippet(payload.body_text),
          updated_at: new Date().toISOString(),
          in_reply_to: payload.in_reply_to,
          references: payload.references,
          thread_key: payload.thread_key,
          attachments: Array.isArray(activeDraft.attachments) ? activeDraft.attachments : [],
        };
        if (forceRemote) {
          state.draftComposerSeed = null;
          state.selectedDraftId = localId;
          upsertDraft(localDraft);
          const response = await postJson("/api/drafts", {
            ...payload,
            id: Number(activeDraft.id || 0) > 0 ? Number(activeDraft.id) : undefined,
          });
          const savedDraft = response?.draft || null;
          if (!savedDraft) {
            throw new Error("save failed");
          }
          replaceDraftId(localId, savedDraft);
          upsertDraft(savedDraft);
          state.draftStatus = "saved";
          persistSelectionState();
          renderCurrentSelection();
          return savedDraft;
        }
        state.draftComposerSeed = null;
        state.selectedDraftId = localId;
        upsertDraft(localDraft);
        state.draftStatus = "saved locally";
        persistSelectionState();
        renderCurrentSelection();
        queueDraftSave({
          localId,
          remoteId: localId > 0 ? localId : 0,
          payload,
        });
        flushDraftSaveQueue().catch(() => {});
        return localDraft;
      } catch (error) {
        state.draftStatus = `save failed`;
        return null;
      } finally {
        state.draftSaveInFlight = false;
        renderDraftStatus();
      }
    }

    function readFileAsBase64(file) {
      return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onerror = () => reject(new Error("failed to read attachment"));
        reader.onload = () => {
          const result = String(reader.result || "");
          const marker = "base64,";
          const index = result.indexOf(marker);
          resolve(index === -1 ? result : result.slice(index + marker.length));
        };
        reader.readAsDataURL(file);
      });
    }

    async function ensureSavedDraftForAttachments() {
      const activeDraft = selectedDraft();
      if (Number(activeDraft?.id || 0) > 0) {
        return activeDraft;
      }
      const savedDraft = await saveDraftFromForm({ forceRemote: true });
      if (!savedDraft || Number(savedDraft.id || 0) <= 0) {
        throw new Error("save the draft before attaching files");
      }
      return savedDraft;
    }

    async function uploadDraftAttachments(fileList) {
      const files = Array.from(fileList || []);
      if (!files.length) return;
      state.draftStatus = "attaching...";
      renderDraftStatus();
      try {
        const savedDraft = await ensureSavedDraftForAttachments();
        const uploads = await Promise.all(files.map(async (file) => ({
          filename: String(file.name || "attachment.bin"),
          mime_type: String(file.type || "application/octet-stream"),
          content_base64: await readFileAsBase64(file),
        })));
        const response = await postJson(`/api/drafts/${savedDraft.id}/attachments`, { attachments: uploads });
        const updatedDraft = response?.draft || null;
        if (updatedDraft) {
          upsertDraft(updatedDraft);
          state.selectedDraftId = Number(updatedDraft.id || savedDraft.id);
          state.draftComposerSeed = null;
        }
        state.draftStatus = "saved";
        persistSelectionState();
        renderCurrentSelection();
      } catch (error) {
        state.draftStatus = String(error?.message || "attachment failed");
        renderDraftStatus();
      }
    }

    async function removeDraftAttachment(draftId, attachmentId) {
      if (!draftId || !attachmentId) return;
      state.draftStatus = "updating attachments...";
      renderDraftStatus();
      try {
        const response = await postJson(`/api/drafts/${draftId}/attachments/delete`, {
          attachment_id: attachmentId,
        });
        const updatedDraft = response?.draft || null;
        if (updatedDraft) {
          upsertDraft(updatedDraft);
          state.selectedDraftId = Number(updatedDraft.id || draftId);
          state.draftComposerSeed = null;
        }
        state.draftStatus = "saved";
        persistSelectionState();
        renderCurrentSelection();
      } catch (error) {
        state.draftStatus = String(error?.message || "attachment delete failed");
        renderDraftStatus();
      }
    }

    async function sendDraft(draftId) {
      if (!draftId) return;
      const activeDraft = selectedDraft();
      if (!activeDraft || Number(activeDraft.id || 0) !== Number(draftId)) {
        state.draftStatus = "draft not found";
        renderDraftStatus();
        return;
      }
      if (!String(activeDraft.to || "").trim()) {
        state.draftStatus = "add a recipient first";
        renderDraftStatus();
        return;
      }
      state.draftStatus = "sending...";
      renderDraftStatus();
      try {
        const response = await postJson(`/api/drafts/${draftId}/send`, {});
        removeDraftById(draftId);
        if (state.selectedDraftId === draftId) {
          state.selectedDraftId = null;
        }
        state.draftStatus = response?.draft_status === "queued" ? "queued to send" : "sent";
        renderCurrentSelection();
      } catch (error) {
        state.draftStatus = String(error?.message || "send failed");
        renderDraftStatus();
      }
    }

    function renderDraftStatus() {
      const statusNode = $("draftStatus");
      if (!statusNode) return;
      statusNode.textContent = state.draftStatus || "";
    }

    function renderDraftPreview() {
      const previewNode = $("draftPreview");
      const bodyInput = $("draftBody");
      if (!previewNode || !bodyInput) return;
      previewNode.innerHTML = composeDraftPreviewHtml(bodyInput.value);
    }

    function renderDraftDetail(draft) {
      const entry = draft || blankDraft();
      const attachments = Array.isArray(entry.attachments) ? entry.attachments : [];
      const detail = $("detailPanel");
      detail.classList.add("draft-mode");
      detail.innerHTML = `
        <div class="detail-head">
          <div class="message-header">
            <div class="message-title">${escapeHtml(entry.label || "(untitled draft)")}</div>
            <div class="message-subtitle">${escapeHtml(entry.updated_at ? `Last saved ${humanTimestamp(entry.updated_at)}` : "Unsaved draft")}</div>
          </div>
        </div>
        <div class="detail-body draft-detail-body">
          <form class="draft-form" id="draftForm">
            <div class="draft-field">
              <label for="draftSubject">Subject</label>
              <input id="draftSubject" type="text" value="${escapeHtml(entry.subject || "")}" placeholder="Draft subject">
            </div>
            <details class="draft-recipient-details">
              <summary class="draft-recipient-summary">
                <div class="draft-recipient-copy">
                  <span class="draft-recipient-kicker">Recipients</span>
                  <span class="draft-recipient-preview">${escapeHtml(draftRecipientSummary(entry))}</span>
                </div>
              </summary>
              <div class="draft-recipient-fields">
                <div class="draft-field">
                  <label for="draftTo">To</label>
                  <input id="draftTo" type="text" list="draftContactSuggestions" value="${escapeHtml(entry.to || "")}" placeholder="recipient@example.com">
                </div>
                <div class="draft-field">
                  <label for="draftCc">Cc</label>
                  <input id="draftCc" type="text" list="draftContactSuggestions" value="${escapeHtml(entry.cc || "")}" placeholder="optional">
                </div>
                <div class="draft-field">
                  <label for="draftBcc">Bcc</label>
                  <input id="draftBcc" type="text" list="draftContactSuggestions" value="${escapeHtml(entry.bcc || "")}" placeholder="optional">
                </div>
                <datalist id="draftContactSuggestions">${contactOptionMarkup(state.contacts)}</datalist>
              </div>
            </details>
            <div class="draft-field draft-attachments-shell">
              <div class="draft-attachments-head">
                <label>Attachments</label>
                <button class="secondary" type="button" id="draftAttachButton">attach files</button>
              </div>
              <input id="draftAttachInput" class="hidden" type="file" multiple>
              <div class="draft-attachment-list">
                ${
                  attachments.length
                    ? attachments.map((attachment) => `
                      <div class="draft-attachment-item">
                        <div class="draft-attachment-copy">
                          <a class="draft-attachment-link" href="${escapeHtml(attachment.download_url || "")}" target="_blank" rel="noreferrer">
                            ${escapeHtml(attachment.filename || "(unnamed attachment)")}
                          </a>
                          <div class="draft-attachment-meta">${escapeHtml(attachment.kind_label || "file")} · ${escapeHtml(humanAttachmentSize(attachment.size || 0))}</div>
                        </div>
                        <button class="delete-button" type="button" data-remove-draft-attachment="${escapeHtml(String(attachment.id || 0))}" title="Remove attachment">×</button>
                      </div>
                    `).join("")
                    : '<div class="draft-attachment-empty">No attachments yet.</div>'
                }
              </div>
            </div>
            <div class="draft-field draft-body-field">
              <label for="draftBody">Body</label>
              <textarea id="draftBody" placeholder="Write your email here...">${escapeHtml(entry.body_text || "")}</textarea>
            </div>
            <div class="draft-preview-shell">
              <div class="label">Send preview</div>
              <div class="draft-preview" id="draftPreview"></div>
            </div>
            <div class="draft-actions">
              <button class="primary" type="submit" id="saveDraftButton">${entry.id ? "save draft" : "create draft"}</button>
              ${Number(entry.id || 0) > 0 ? '<button class="secondary" type="button" id="sendDraftButton">send</button>' : ""}
              <span class="draft-status" id="draftStatus">${escapeHtml(state.draftStatus || "")}</span>
            </div>
          </form>
        </div>
      `;
      const form = $("draftForm");
      if (form) {
        form.addEventListener("submit", async (event) => {
          event.preventDefault();
          await saveDraftFromForm();
        });
      }
      const bodyInput = $("draftBody");
      if (bodyInput) {
        bodyInput.addEventListener("input", () => renderDraftPreview());
      }
      for (const fieldId of ["draftTo", "draftCc", "draftBcc"]) {
        const input = $(fieldId);
        if (!input) continue;
        input.addEventListener("focus", () => {
          if (!state.contacts.length && !state.contactLookupInFlight) {
            state.contactLookupInFlight = loadContacts()
              .catch(() => {})
              .finally(() => {
                state.contactLookupInFlight = null;
              });
          } else {
            updateDraftContactSuggestions(state.contacts);
          }
        });
        input.addEventListener("input", () => {
          const query = String(input.value || "").trim();
          if (!query) {
            updateDraftContactSuggestions(state.contacts);
            return;
          }
          loadContacts({ query, limit: 20 }).catch(() => {
            updateDraftContactSuggestions(state.contacts);
          });
        });
      }
      const attachButton = $("draftAttachButton");
      const attachInput = $("draftAttachInput");
      if (attachButton && attachInput) {
        attachButton.addEventListener("click", () => attachInput.click());
        attachInput.addEventListener("change", async () => {
          const files = Array.from(attachInput.files || []);
          attachInput.value = "";
          if (!files.length) return;
          await uploadDraftAttachments(files);
        });
      }
      for (const removeButton of detail.querySelectorAll("[data-remove-draft-attachment]")) {
        removeButton.addEventListener("click", async () => {
          const attachmentId = Number(removeButton.getAttribute("data-remove-draft-attachment") || 0);
          if (!attachmentId || !entry.id) return;
          await removeDraftAttachment(Number(entry.id), attachmentId);
        });
      }
      const sendButton = $("sendDraftButton");
      if (sendButton && entry.id) {
        sendButton.addEventListener("click", () => requestSendDraft(entry));
      }
      renderDraftPreview();
      renderDraftStatus();
    }

    function selectContact(contactKey) {
      state.selectedContactKey = contactKey || null;
      state.selectedThreadKey = null;
      const messages = messagesForContact(state.selectedContactKey);
      state.selectedId = messages[0]?.id ?? null;
      persistSelectionState();
      renderContactList(state.overview || {});
      if (state.selectedId) {
        loadDetail(state.selectedId);
      } else {
        $("detailPanel").classList.remove("draft-mode");
        $("detailPanel").innerHTML = `<div class="empty">No correspondence with this contact yet.</div>`;
      }
    }

    function updateCorrespondenceSearch(value) {
      state.correspondenceQuery = String(value || "").slice(0, 200);
      const contacts = displayedContacts(state.overview?.contacts || []);
      if (!contacts.length) {
        state.selectedContactKey = null;
        state.selectedId = null;
      } else if (!contacts.some((contact) => contact.contact_key === state.selectedContactKey)) {
        state.selectedContactKey = contacts[0].contact_key;
        state.selectedId = null;
      }
      persistSelectionState();
      renderCurrentSelection();
    }

    function gridItem(label, value) {
      return `
        <div class="item">
          <div class="label">${escapeHtml(label)}</div>
          <div class="value">${escapeHtml(value || "—")}</div>
        </div>
      `;
    }

    function requestDeleteContact(contactKey) {
      const contact = (state.overview?.contacts || []).find((entry) => entry.contact_key === contactKey);
      state.pendingDelete = {
        kind: "contact",
        contactKey,
        label: contact?.contact_label || contactKey,
      };
      renderConfirm();
    }

    async function deleteContact(contactKey) {
      const existingMessages = messagesForContact(contactKey);
      const queueId = `contact:${contactKey}`;
      state.pendingDeletedContactKeys.add(contactKey);
      persistPendingDeleteState();
      for (const message of existingMessages) {
        state.detailCache.delete(String(message.id));
        state.pendingDetailLoads.delete(String(message.id));
      }
      if (state.selectedContactKey === contactKey) {
        state.selectedContactKey = null;
        state.selectedThreadKey = null;
        state.selectedId = null;
      }
      applyOptimisticOverviewUpdate();
      try {
        enqueueDelete({ id: queueId, kind: "contact", contactKey });
        await flushDeleteQueue();
      } catch (error) {
        state.pendingDeletedContactKeys.delete(contactKey);
        dequeueDelete(`contact:${contactKey}`);
        persistPendingDeleteState();
        await loadOverview({ sync: false });
        throw error;
      }
    }

    function requestDeleteMessage(messageId) {
      const message = (state.overview?.messages || []).find((entry) => entry.id === messageId) || state.detail;
      state.pendingDelete = {
        kind: "message",
        messageId,
        label: message?.subject || "(no subject)",
      };
      renderConfirm();
    }

    async function deleteMessage(messageId) {
      const queueId = `message:${messageId}`;
      state.pendingDeletedMessageIds.add(String(messageId));
      persistPendingDeleteState();
      state.detailCache.delete(String(messageId));
      state.pendingDetailLoads.delete(String(messageId));
      if (state.selectedId === messageId) {
        state.selectedId = null;
      }
      applyOptimisticOverviewUpdate();
      try {
        enqueueDelete({ id: queueId, kind: "message", messageId });
        await flushDeleteQueue();
      } catch (error) {
        state.pendingDeletedMessageIds.delete(String(messageId));
        dequeueDelete(`message:${messageId}`);
        persistPendingDeleteState();
        await loadOverview({ sync: false });
        throw error;
      }
    }

    function renderDetail(message) {
      state.detail = message;
      markMessageViewed(message);
      const detail = $("detailPanel");
      detail.classList.remove("draft-mode");
      const contact = contactRecord(state.selectedContactKey);
      const messages = messagesForContact(state.selectedContactKey);
      const outbound = String(message.direction || "").toLowerCase() === "outbound";
      const attachments = message.attachments || [];
      const bodyDisplay = message.body_display || {};
      const attachmentsMarkup = attachments.length ? `
          <div class="block">
            <div class="label">attachments</div>
            <div class="attachments">
              <div class="attachment-grid">
                ${attachments.map((attachment) => `
                  <div class="attachment-item">
                    <div class="attachment-preview">
                      ${attachment.preview_url ? `<img src="${escapeHtml(attachment.preview_url)}" alt="${escapeHtml(attachment.filename || "attachment")}">` : `<div class="attachment-fallback">${escapeHtml(attachment.kind_label || "file")}</div>`}
                    </div>
                    <div><strong>${escapeHtml(attachment.filename || "(unnamed)")}</strong></div>
                    <div>${escapeHtml(attachment.mime_type || "")} · ${escapeHtml(String(attachment.size || 0))} bytes</div>
                    <div><a href="${escapeHtml(attachment.download_url || "")}" target="_blank" rel="noreferrer">open attachment</a></div>
                    ${attachment.text_preview ? `<div class="attachment-text">${escapeHtml(attachment.text_preview)}</div>` : ""}
                  </div>
                `).join("")}
              </div>
            </div>
          </div>
      ` : "";
      const quoteMarkup = bodyDisplay.has_quote ? `
          <details class="quote-details" data-quoted-message-id="${message.id}" ${state.expandedQuotedMessageIds.has(String(message.id)) ? "open" : ""}>
            <summary class="quote-summary">Quoted previous email</summary>
            ${bodyDisplay.quoted_header ? `<div class="quote-header">${escapeHtml(bodyDisplay.quoted_header)}</div>` : ""}
            ${
              bodyDisplay.quoted_html
                ? `<div class="quote-rich">${bodyDisplay.quoted_html}</div>`
                : `<div class="quote-text">${escapeHtml(bodyDisplay.quoted_text || "")}</div>`
            }
          </details>
      ` : "";
      detail.innerHTML = `
        <div class="detail-head">
          <div class="message-header">
            <div class="message-title">${escapeHtml(message.contact_label || message.person || message.external_from || "(unknown contact)")}</div>
            <div class="message-subtitle">${escapeHtml(`${contact?.count || messages.length} message${(contact?.count || messages.length) === 1 ? "" : "s"} in this correspondence`)}</div>
          </div>
        </div>
        <div class="detail-body">
          <div class="block">
            <div class="label">messages</div>
            <select id="messageSelect" aria-label="Select a message from this contact">
              ${messages.map((entry) => `
                <option value="${entry.id}" ${entry.id === state.selectedId ? "selected" : ""}>
                  ${escapeHtml(`${messageIsUnread(entry) ? "● " : ""}${messageDirectionLabel(entry)} · ${entry.subject || "(no subject)"} — ${humanTimestamp(entry.happened_at || "")}`)}
                </option>
              `).join("")}
            </select>
          </div>
          <div class="detail-grid">
            ${gridItem(outbound ? "to" : "from", messageCounterpartyValue(message))}
            ${gridItem(outbound ? "sent" : "received", humanTimestamp(message.happened_at || ""))}
            <div class="item item-action">
              <div class="detail-action-buttons">
                <button class="secondary inline-reply" type="button" data-reply-message="${message.id}">reply</button>
                <button class="delete-button inline-delete" type="button" data-delete-message="${message.id}" title="Archive this message from correspondence">×</button>
              </div>
            </div>
          </div>
          <div class="block">
            <div class="label">body</div>
            ${
              bodyDisplay.primary_html
                ? `<div class="body-rich">${bodyDisplay.primary_html}</div>`
                : `<div class="body-text">${escapeHtml(bodyDisplay.primary_text || message.body_text || message.snippet || "")}</div>`
            }
          </div>
          ${quoteMarkup}
          ${attachmentsMarkup}
        </div>
      `;
      const messageSelect = detail.querySelector("#messageSelect");
      if (messageSelect) {
        messageSelect.addEventListener("change", () => {
          const nextId = Number(messageSelect.value);
          if (!nextId || nextId === state.selectedId) return;
          state.selectedId = nextId;
          persistSelectionState();
          loadDetail(nextId);
        });
      }
      const quoteDetails = detail.querySelector("[data-quoted-message-id]");
      if (quoteDetails) {
        quoteDetails.addEventListener("toggle", () => {
          const quotedMessageId = String(quoteDetails.getAttribute("data-quoted-message-id") || "");
          if (!quotedMessageId) return;
          if (quoteDetails.open) {
            state.expandedQuotedMessageIds.add(quotedMessageId);
          } else {
            state.expandedQuotedMessageIds.delete(quotedMessageId);
          }
          persistExpandedQuotedMessages();
        });
      }
      const deleteButton = detail.querySelector("[data-delete-message]");
      const replyButton = detail.querySelector("[data-reply-message]");
      if (replyButton) {
        replyButton.addEventListener("click", () => {
          openReplyDraft(message);
        });
      }
      if (deleteButton) {
        deleteButton.addEventListener("click", async () => {
          requestDeleteMessage(message.id);
        });
      }
    }

    async function loadOverview({ sync = false, render = true } = {}) {
      state.lastRefreshStartedAt = Date.now();
      const params = new URLSearchParams({
        limit: "80",
        source: CORRESPONDENCE_SOURCE,
      });
      if (sync) params.set("sync", "1");
      state.overview = normalizeOverviewPayload(await fetchJson(`/api/overview?${params.toString()}`), { allowPendingResolution: true });
      primeViewedMessagesFromOverview(state.overview);
      persistOverviewCache(state.overview);
      state.mailboxVersionSignature = mailboxVersionSignature(state.overview);
      pruneDetailCache();
      renderSyncBadge(state.overview);
      if (render) {
        renderCurrentSelection();
        schedulePrefetch();
      }
    }

    async function loadDrafts() {
      const payload = await fetchJson("/api/drafts");
      state.drafts = mergeDraftsWithPending(payload?.drafts || []);
      persistDraftsCache(state.drafts);
      if (!state.contacts.length) {
        loadContacts().catch(() => {});
      }
      if (state.activeView === "drafts") {
        renderCurrentSelection();
      }
    }

    function setActiveView(view) {
      const nextView = view === "drafts" ? "drafts" : "inbox";
      if (state.activeView === nextView) {
        renderCurrentSelection();
        return;
      }
      state.activeView = nextView;
      state.listSignature = "";
      state.draftStatus = "";
      persistSelectionState();
      renderCurrentSelection();
      if (nextView === "drafts") {
        if (!state.contacts.length) {
          loadContacts().catch(() => {});
        } else {
          updateDraftContactSuggestions(state.contacts);
        }
        loadDrafts().catch((error) => {
          $("detailPanel").innerHTML = `<div class="empty">${escapeHtml(error.message || String(error))}</div>`;
        });
      }
    }

    async function refreshMailboxIfNeeded() {
      try {
        const { mailboxChanged } = await loadSyncStatus();
        if (mailboxChanged || !state.overview) {
          await loadOverview({ sync: false, render: state.activeView === "inbox" });
        }
      } catch (_error) {
        // Keep the last rendered inbox visible if the sync step hiccups.
      }
    }

    function wakeRefresh() {
      const now = Date.now();
      if (now - state.lastRefreshStartedAt < 750) return;
      refreshMailboxIfNeeded().catch(() => {});
    }

    async function loadDetail(id) {
      if (!id) return;
      const payload = await getDetail(id);
      if (!payload) return;
      renderDetail(payload);
      renderContactList(state.overview || {});
    }

    function overviewCandidateRank(payload) {
      const version = payload?.mailbox_version || {};
      return [
        Number(version.latest_message_id || 0),
        Number(payload?.message_count || 0),
        Number(payload?.contact_count || 0),
        String(version.latest_happened_at || ""),
      ];
    }

    function isBetterOverviewCandidate(nextPayload, currentPayload) {
      const nextRank = overviewCandidateRank(nextPayload);
      const currentRank = overviewCandidateRank(currentPayload);
      for (let index = 0; index < nextRank.length; index += 1) {
        if (nextRank[index] > currentRank[index]) return true;
        if (nextRank[index] < currentRank[index]) return false;
      }
      return false;
    }

    function renderInitialOverview() {
      const candidates = [
        SERVER_BOOTSTRAP_OVERVIEW,
        loadStoredJson(OVERVIEW_CACHE_STORAGE_KEY),
      ].filter((payload) => payload && typeof payload === "object");
      if (!candidates.length) return;
      let nextOverview = candidates[0];
      for (const candidate of candidates.slice(1)) {
        if (isBetterOverviewCandidate(candidate, nextOverview)) {
          nextOverview = candidate;
        }
      }
      state.overview = normalizeOverviewPayload(nextOverview, { allowPendingResolution: false });
      primeViewedMessagesFromOverview(state.overview);
      persistOverviewCache(state.overview);
      state.mailboxVersionSignature = mailboxVersionSignature(state.overview);
      renderSyncBadge(state.overview);
      renderCurrentSelection();
      schedulePrefetch();
    }

    renderInitialOverview();
    renderChrome();
    $("tabInbox").addEventListener("click", () => setActiveView("inbox"));
    $("tabDrafts").addEventListener("click", () => setActiveView("drafts"));
    $("correspondenceSearch").addEventListener("input", (event) => updateCorrespondenceSearch(event.target.value));
    $("newDraftButton").addEventListener("click", () => startNewDraft());
    if (state.activeView === "drafts") {
      if (!state.contacts.length) {
        loadContacts().catch(() => {});
      }
      loadDrafts().catch((error) => {
        $("detailPanel").innerHTML = `<div class="empty">${escapeHtml(error.message || String(error))}</div>`;
      });
    }
    flushDeleteQueue().catch(() => {});
    flushDraftSaveQueue().catch(() => {});
    refreshMailboxIfNeeded().catch((error) => {
      $("detailPanel").innerHTML = `<div class="empty">${escapeHtml(error.message || String(error))}</div>`;
    });
    setInterval(() => {
      flushDeleteQueue().catch(() => {});
      flushDraftSaveQueue().catch(() => {});
      refreshMailboxIfNeeded().catch(() => {});
    }, __MAIL_UI_CLIENT_REFRESH_INTERVAL_MS__);
    window.addEventListener("focus", wakeRefresh);
    document.addEventListener("visibilitychange", () => {
      if (document.visibilityState === "visible") {
        wakeRefresh();
      }
    });
  </script>
</body>
</html>
"""


def _json_value(raw: str, default: Any) -> Any:
    try:
        return json.loads(raw)
    except Exception:
        return default


def _attachment_kind_label(mime_type: str) -> str:
    lower = str(mime_type or "").lower()
    if lower.startswith("image/"):
        return "image"
    if lower == "application/pdf":
        return "pdf"
    if lower.startswith("text/"):
        return "text"
    if lower.startswith("audio/"):
        return "audio"
    if lower.startswith("video/"):
        return "video"
    if "zip" in lower or "archive" in lower:
        return "archive"
    return "file"


def _safe_attachment_filename(filename: str, *, fallback: str = "attachment.bin") -> str:
    clean = Path(str(filename or "").strip()).name.strip()
    clean = clean.replace("\x00", "")
    return clean or fallback


def _attachment_download_disposition(filename: str, mime_type: str) -> str:
    safe_filename = _safe_attachment_filename(filename)
    lower = str(mime_type or "").lower()
    inline = (
        lower.startswith("image/")
        or lower.startswith("text/")
        or lower == "application/pdf"
    )
    token = "inline" if inline else "attachment"
    escaped = safe_filename.replace("\\", "_").replace('"', "'")
    return f'{token}; filename="{escaped}"'


def _attachment_summary_from_row(row: sqlite3.Row) -> dict[str, Any]:
    attachment_id = int(row["id"])
    mime_type = str(row["mime_type"] or "")
    preview_url = f"/api/attachments/{attachment_id}/content" if mime_type.lower().startswith("image/") else ""
    download_url = f"/api/attachments/{attachment_id}/content"
    return {
        "id": attachment_id,
        "filename": _safe_attachment_filename(str(row["filename"] or "")),
        "mime_type": mime_type,
        "size": int(row["size"] or 0),
        "relative_path": str(row["relative_path"] or ""),
        "ingest_status": str(row["ingest_status"] or ""),
        "extraction_method": str(row["extraction_method"] or ""),
        "text_preview": str(row["extracted_text"] or "")[:320],
        "kind_label": _attachment_kind_label(mime_type),
        "preview_url": preview_url,
        "download_url": download_url,
    }


def _attachment_rows_for_communication(connection: sqlite3.Connection, *, communication_id: int) -> list[sqlite3.Row]:
    return store.list_communication_attachments(connection, communication_id=communication_id, limit=MAX_DRAFT_ATTACHMENT_COUNT * 4)


def _sync_communication_attachments_json(connection: sqlite3.Connection, *, communication_id: int) -> list[dict[str, Any]]:
    rows = _attachment_rows_for_communication(connection, communication_id=communication_id)
    attachments_json = [
        {
            "id": int(row["id"]),
            "filename": _safe_attachment_filename(str(row["filename"] or "")),
            "mime_type": str(row["mime_type"] or ""),
            "size": int(row["size"] or 0),
            "relative_path": str(row["relative_path"] or ""),
            "kind_label": _attachment_kind_label(str(row["mime_type"] or "")),
        }
        for row in rows
    ]
    connection.execute(
        "UPDATE communications SET attachments_json = ? WHERE id = ?",
        (json.dumps(attachments_json), int(communication_id)),
    )
    return attachments_json


def _extract_attachment_text_from_bytes(*, raw_bytes: bytes, filename: str, mime_type: str) -> tuple[str, str]:
    clean_filename = _safe_attachment_filename(filename)
    suffix = Path(clean_filename).suffix or (mimetypes.guess_extension(str(mime_type or "")) or "")
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(prefix="cmail-attachment-", suffix=suffix, delete=False) as handle:
            handle.write(raw_bytes)
            temp_path = Path(handle.name)
        return extract_text_from_saved_attachment(temp_path, mime_type=str(mime_type or "application/octet-stream"))
    except Exception:
        return "", ""
    finally:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)


def _assert_draft_attachment_capacity(connection: sqlite3.Connection, *, draft_id: int, incoming_count: int) -> None:
    existing_count = len(_attachment_rows_for_communication(connection, communication_id=int(draft_id)))
    if existing_count + max(0, int(incoming_count)) > MAX_DRAFT_ATTACHMENT_COUNT:
        raise ValueError(f"draft attachments are limited to {MAX_DRAFT_ATTACHMENT_COUNT} files")


def _store_draft_attachment_bytes(
    connection: sqlite3.Connection,
    *,
    draft_id: int,
    filename: str,
    mime_type: str,
    raw_bytes: bytes,
) -> dict[str, Any]:
    clean_filename = _safe_attachment_filename(filename)
    if not raw_bytes:
        raise ValueError(f"{clean_filename} is empty")
    if len(raw_bytes) > MAX_DRAFT_ATTACHMENT_BYTES:
        raise ValueError(f"{clean_filename} exceeds the {MAX_DRAFT_ATTACHMENT_BYTES // (1024 * 1024)} MB draft attachment limit")
    effective_mime_type = str(mime_type or mimetypes.guess_type(clean_filename)[0] or "application/octet-stream").strip().lower()
    sha256 = hashlib.sha256(raw_bytes).hexdigest()
    existing = connection.execute(
        """
        SELECT *
        FROM communication_attachments
        WHERE communication_id = ? AND source = 'cmail_draft' AND sha256 = ? AND filename = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (int(draft_id), sha256, clean_filename),
    ).fetchone()
    if existing is not None:
        _sync_communication_attachments_json(connection, communication_id=int(draft_id))
        connection.commit()
        return _attachment_summary_from_row(existing)

    token = sha256[:12]
    relative_path, _ = mail_vault.write_encrypted_vault_file(
        vault_root=store.attachment_vault_root(),
        relative_dir=Path("cmail_draft") / f"communication-{int(draft_id)}" / token,
        logical_filename=clean_filename,
        raw_bytes=raw_bytes,
        metadata={"source": "cmail_draft", "communication_id": int(draft_id), "filename": clean_filename},
    )
    extracted_text, extraction_method = _extract_attachment_text_from_bytes(
        raw_bytes=raw_bytes,
        filename=clean_filename,
        mime_type=effective_mime_type,
    )
    attachment_id = store.upsert_communication_attachment(
        connection,
        external_key=f"cmail_draft:{int(draft_id)}:{sha256}:{clean_filename}",
        communication_id=int(draft_id),
        source="cmail_draft",
        external_message_id=f"draft:{int(draft_id)}",
        external_attachment_id=sha256,
        part_id=token,
        filename=clean_filename,
        mime_type=effective_mime_type,
        size=len(raw_bytes),
        relative_path=str(relative_path),
        extracted_text=extracted_text,
        extracted_text_path="",
        extraction_method=extraction_method,
        ingest_status="stored",
        error_text="",
        sha256=sha256,
    )
    _sync_communication_attachments_json(connection, communication_id=int(draft_id))
    connection.commit()
    row = connection.execute(
        "SELECT * FROM communication_attachments WHERE id = ?",
        (int(attachment_id),),
    ).fetchone()
    if row is None:
        raise KeyError(f"draft attachment {attachment_id} not found after save")
    return _attachment_summary_from_row(row)


def _store_draft_attachment_path(
    connection: sqlite3.Connection,
    *,
    draft_id: int,
    path_value: str | Path,
) -> dict[str, Any]:
    attachment_path = Path(str(path_value)).expanduser()
    if not attachment_path.exists() or not attachment_path.is_file():
        raise FileNotFoundError(f"attachment not found: {attachment_path}")
    return _store_draft_attachment_bytes(
        connection,
        draft_id=int(draft_id),
        filename=attachment_path.name,
        mime_type=mimetypes.guess_type(str(attachment_path))[0] or "application/octet-stream",
        raw_bytes=attachment_path.read_bytes(),
    )


def _draft_attachment_rows(connection: sqlite3.Connection, *, draft_id: int) -> list[sqlite3.Row]:
    return _attachment_rows_for_communication(connection, communication_id=int(draft_id))


def _draft_attachment_summaries(connection: sqlite3.Connection, *, draft_id: int) -> list[dict[str, Any]]:
    return [_attachment_summary_from_row(row) for row in _draft_attachment_rows(connection, draft_id=int(draft_id))]


def _add_draft_attachments(
    connection: sqlite3.Connection,
    *,
    draft_id: int,
    uploads: list[dict[str, Any]] | None = None,
    attachment_paths: list[str | Path] | None = None,
) -> list[dict[str, Any]]:
    uploads = [item for item in (uploads or []) if isinstance(item, dict)]
    attachment_paths = [item for item in (attachment_paths or []) if str(item or "").strip()]
    if not uploads and not attachment_paths:
        return _draft_attachment_summaries(connection, draft_id=int(draft_id))
    _assert_draft_attachment_capacity(
        connection,
        draft_id=int(draft_id),
        incoming_count=len(uploads) + len(attachment_paths),
    )
    for upload in uploads:
        encoded = str(upload.get("content_base64") or "").strip()
        if not encoded:
            raise ValueError("draft attachment payload is missing content")
        try:
            raw_bytes = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("draft attachment payload is not valid base64") from exc
        _store_draft_attachment_bytes(
            connection,
            draft_id=int(draft_id),
            filename=str(upload.get("filename") or ""),
            mime_type=str(upload.get("mime_type") or ""),
            raw_bytes=raw_bytes,
        )
    for attachment_path in attachment_paths:
        _store_draft_attachment_path(connection, draft_id=int(draft_id), path_value=attachment_path)
    return _draft_attachment_summaries(connection, draft_id=int(draft_id))


def _delete_draft_attachment(connection: sqlite3.Connection, *, draft_id: int, attachment_id: int) -> list[dict[str, Any]]:
    row = connection.execute(
        """
        SELECT id, relative_path
        FROM communication_attachments
        WHERE id = ? AND communication_id = ? AND source = 'cmail_draft'
        """,
        (int(attachment_id), int(draft_id)),
    ).fetchone()
    if row is None:
        raise KeyError(f"draft attachment {attachment_id} not found")
    relative_path = str(row["relative_path"] or "")
    connection.execute(
        "DELETE FROM communication_attachments WHERE id = ?",
        (int(attachment_id),),
    )
    _sync_communication_attachments_json(connection, communication_id=int(draft_id))
    connection.commit()
    if relative_path:
        mail_vault.delete_encrypted_vault_file(
            vault_root=store.attachment_vault_root(),
            relative_path=relative_path,
        )
    return _draft_attachment_summaries(connection, draft_id=int(draft_id))


def _reply_subject(subject: str) -> str:
    clean = str(subject or "").strip()
    if clean.lower().startswith("re:"):
        return clean
    return f"Re: {clean}" if clean else "Re:"


def _first_address(value: str) -> tuple[str, str]:
    addresses = getaddresses([str(value or "")])
    for name, email in addresses:
        clean_email = str(email or "").strip()
        clean_name = str(name or "").strip()
        if clean_email:
            return clean_name, clean_email
    return parseaddr(str(value or ""))


def _contact_identity(
    *,
    direction: str = "",
    person: str = "",
    external_from: str = "",
    external_to: str = "",
    source: str = "",
) -> dict[str, str]:
    clean_direction = str(direction or "").strip().lower()
    if clean_direction == "outbound":
        contact_name, contact_email = _first_address(str(external_to or ""))
        contact_name = contact_name.strip() or str(person or "").strip()
        contact_email = contact_email.strip()
        contact_label = contact_name or contact_email or str(source or "unknown recipient")
    else:
        contact_name, contact_email = parseaddr(str(external_from or ""))
        contact_name = contact_name.strip() or str(person or "").strip()
        contact_email = contact_email.strip()
        contact_label = contact_name or contact_email or str(source or "unknown sender")
    contact_key = contact_email.lower() if contact_email else f"person:{contact_label.lower()}"
    return {
        "contact_key": contact_key,
        "contact_name": contact_name,
        "contact_email": contact_email,
        "contact_label": contact_label,
    }


def _normalized_subject(subject: str) -> str:
    text = str(subject or "").strip()
    if not text:
        return ""
    normalized = _SUBJECT_PREFIX_RE.sub("", text).strip()
    return normalized or text


def _thread_group_key(message: dict[str, Any]) -> str:
    explicit_key = str(message.get("thread_key") or "").strip()
    if explicit_key:
        return explicit_key
    normalized_subject = _normalized_subject(str(message.get("subject") or ""))
    if normalized_subject:
        return f"subject:{normalized_subject.lower()}"
    message_id = str(message.get("message_id") or "").strip()
    if message_id:
        return f"message:{message_id}"
    return f"message:{message.get('id')}"


def _communication_read_key(row: Any) -> str:
    message_id = str(row["message_id"] or "").strip()
    if message_id:
        return f"message-id:{message_id}"
    external_id = str(row["external_id"] or "").strip()
    if external_id:
        return f"external-id:{external_id}"
    fallback_parts = [
        str(row["source"] or "").strip(),
        str(row["direction"] or "").strip(),
        str(row["external_from"] or "").strip(),
        str(row["external_to"] or "").strip(),
        str(row["thread_key"] or row["external_thread_id"] or "").strip(),
        str(row["happened_at"] or "").strip(),
        str(row["subject"] or "").strip(),
    ]
    fallback = "|".join(part for part in fallback_parts if part)
    if fallback:
        return f"fallback:{fallback}"
    return f"local-id:{int(row['id'])}"


def _coalesce_display_paragraphs(text: str) -> str:
    paragraphs: list[str] = []
    current_parts: list[str] = []
    for raw_line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        cleaned = " ".join(raw_line.split())
        if not cleaned:
            if current_parts:
                paragraphs.append(" ".join(current_parts))
                current_parts = []
            continue
        current_parts.append(cleaned)
    if current_parts:
        paragraphs.append(" ".join(current_parts))
    return _SIGNATURE_TAIL_RE.sub("", "\n\n".join(paragraphs)).strip()


def _coalesce_quoted_paragraphs(text: str) -> str:
    raw = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not raw:
        return ""
    if "\n" not in raw and " >" in raw:
        raw = raw.replace(" > >", "\n\n> ").replace(" > ", "\n> ")
    paragraphs: list[str] = []
    current_parts: list[str] = []
    for raw_line in raw.split("\n"):
        cleaned = re.sub(r"^>+\s*", "", raw_line.strip())
        cleaned = " ".join(cleaned.split())
        if not cleaned:
            if current_parts:
                paragraphs.append(" ".join(current_parts))
                current_parts = []
            continue
        current_parts.append(cleaned)
    if current_parts:
        paragraphs.append(" ".join(current_parts))
    return _SIGNATURE_TAIL_RE.sub("", "\n\n".join(paragraphs)).strip()


def _extract_balanced_tag_fragment(html_text: str, *, start_index: int, tag_name: str) -> str:
    lower = html_text.lower()
    open_marker = f"<{tag_name.lower()}"
    close_marker = f"</{tag_name.lower()}"
    index = start_index
    depth = 0
    length = len(html_text)
    while index < length:
        next_open = lower.find(open_marker, index)
        next_close = lower.find(close_marker, index)
        if next_open == -1 and next_close == -1:
            break
        if next_open != -1 and (next_close == -1 or next_open < next_close):
            close_bracket = html_text.find(">", next_open)
            if close_bracket == -1:
                break
            depth += 1
            index = close_bracket + 1
            continue
        close_bracket = html_text.find(">", next_close)
        if close_bracket == -1:
            break
        depth -= 1
        index = close_bracket + 1
        if depth <= 0:
            return html_text[start_index:index]
    return ""


def _extract_quoted_html_fragment(html_body: str) -> str:
    raw = str(html_body or "").strip()
    if not raw:
        return ""
    gmail_match = _GMAIL_QUOTE_RE.search(raw)
    if gmail_match:
        fragment = _extract_balanced_tag_fragment(raw, start_index=gmail_match.start(), tag_name="div")
        if fragment:
            return fragment
    lower = raw.lower()
    blockquote_index = lower.find("<blockquote")
    if blockquote_index != -1:
        fragment = _extract_balanced_tag_fragment(raw, start_index=blockquote_index, tag_name="blockquote")
        if fragment:
            return fragment
    return ""


def _extract_body_html_fragment(html_body: str) -> str:
    raw = str(html_body or "").strip()
    if not raw:
        return ""
    match = re.search(r"<body\b[^>]*>(?P<body>.*)</body\s*>", raw, re.IGNORECASE | re.DOTALL)
    if match:
        return str(match.group("body") or "").strip()
    return raw


def _extract_primary_html_fragment(html_body: str) -> str:
    body_fragment = _extract_body_html_fragment(html_body)
    if not body_fragment:
        return ""
    gmail_match = _GMAIL_QUOTE_RE.search(body_fragment)
    if gmail_match:
        return body_fragment[: gmail_match.start()].strip()
    lower = body_fragment.lower()
    blockquote_index = lower.find("<blockquote")
    if blockquote_index != -1:
        return body_fragment[:blockquote_index].strip()
    return body_fragment


def _sanitize_rich_html_fragment(html_fragment: str) -> str:
    fragment = str(html_fragment or "").strip()
    if not fragment:
        return ""
    sanitizer = _SafeHtmlFragmentSanitizer()
    sanitizer.feed(fragment)
    sanitizer.close()
    return sanitizer.get_html()


def _linkify_plain_text_line(text: str) -> str:
    raw = str(text or "")
    parts: list[str] = []
    last_index = 0
    linked = False
    for match in _PLAIN_TEXT_URL_RE.finditer(raw):
        url = str(match.group("url") or "")
        if not url:
            continue
        parts.append(html_escape(raw[last_index : match.start()]))
        trailing = ""
        while url and url[-1] in _TRAILING_URL_PUNCTUATION:
            trailing = url[-1] + trailing
            url = url[:-1]
        href = url if url.lower().startswith(("http://", "https://", "mailto:")) else f"https://{url}"
        safe_href = _SafeHtmlFragmentSanitizer._safe_href(href)
        if safe_href:
            parts.append(
                f'<a href="{html_escape(safe_href, quote=True)}" target="_blank" rel="noreferrer noopener">'
                f"{html_escape(url)}</a>"
            )
            linked = True
        else:
            parts.append(html_escape(str(match.group("url") or "")))
            trailing = ""
        parts.append(html_escape(trailing))
        last_index = match.end()
    if not linked:
        return ""
    parts.append(html_escape(raw[last_index:]))
    return "".join(parts)


def _linkify_plain_text_html(text: str) -> str:
    clean = str(text or "").strip()
    if not clean or not _PLAIN_TEXT_URL_RE.search(clean):
        return ""
    paragraphs: list[str] = []
    for paragraph in re.split(r"\n{2,}", clean):
        lines = [line for line in paragraph.split("\n")]
        rendered_lines: list[str] = []
        for line in lines:
            linked_line = _linkify_plain_text_line(line)
            rendered_lines.append(linked_line if linked_line else html_escape(line))
        rendered = "<br>".join(rendered_lines).strip()
        if rendered:
            paragraphs.append(f"<p>{rendered}</p>")
    return "".join(paragraphs)


def _looks_like_html_fragment(text: str) -> bool:
    clean = str(text or "").strip()
    if not clean:
        return False
    return bool(
        re.search(
            r"<(?:!doctype|html|body|head|meta|title|table|tr|td|th|tbody|thead|div|p|span|a|img|br|blockquote)\b|</[a-z][^>]*>",
            clean,
            re.IGNORECASE,
        )
    )


def _strip_duplicate_quoted_header_html(quoted_html: str, quoted_header: str) -> str:
    clean_html = str(quoted_html or "").strip()
    clean_header = str(quoted_header or "").strip()
    if not clean_html or not clean_header:
        return clean_html
    escaped_header = re.escape(html_escape(clean_header))
    pattern = re.compile(rf"^\s*<(div|p)>\s*{escaped_header}\s*</\1>\s*", re.IGNORECASE)
    return pattern.sub("", clean_html, count=1).strip()


def _body_display(body_text: str, *, html_body: str = "", snippet: str = "") -> dict[str, str | bool]:
    raw = str(body_text or "").strip() or str(snippet or "").strip()
    rich_source = str(html_body or "").strip()
    if not rich_source and _looks_like_html_fragment(body_text):
        rich_source = str(body_text or "").strip()
    if not raw:
        return {
            "primary_text": "",
            "primary_html": "",
            "quoted_header": "",
            "quoted_text": "",
            "quoted_html": "",
            "has_quote": False,
        }
    match = _QUOTED_REPLY_HEADER_RE.search(raw)
    if not match:
        primary_text = _coalesce_display_paragraphs(raw)
        primary_html = _sanitize_rich_html_fragment(_extract_primary_html_fragment(rich_source))
        return {
            "primary_text": primary_text,
            "primary_html": primary_html or _linkify_plain_text_html(primary_text),
            "quoted_header": "",
            "quoted_text": "",
            "quoted_html": "",
            "has_quote": False,
        }
    primary_text = _coalesce_display_paragraphs(raw[: match.start()].strip())
    primary_html = _sanitize_rich_html_fragment(_extract_primary_html_fragment(rich_source))
    quoted_header = " ".join(match.group("header").split())
    quoted_text = _coalesce_quoted_paragraphs(raw[match.end() :].strip())
    quoted_html = _strip_duplicate_quoted_header_html(
        _sanitize_rich_html_fragment(_extract_quoted_html_fragment(rich_source)),
        quoted_header,
    )
    if not primary_html:
        primary_html = _linkify_plain_text_html(primary_text)
    if not quoted_html:
        quoted_html = _linkify_plain_text_html(quoted_text)
    return {
        "primary_text": primary_text,
        "primary_html": primary_html,
        "quoted_header": quoted_header if quoted_text else "",
        "quoted_text": quoted_text,
        "quoted_html": quoted_html,
        "has_quote": bool(quoted_text or quoted_html),
    }


def _communication_summary(row: Any) -> dict[str, Any]:
    contact = _contact_identity(
        direction=str(row["direction"] or ""),
        person=str(row["person"] or ""),
        external_from=str(row["external_from"] or ""),
        external_to=str(row["external_to"] or ""),
        source=str(row["source"] or ""),
    )
    return {
        "id": int(row["id"]),
        "external_id": str(row["external_id"] or ""),
        "read_key": _communication_read_key(row),
        "subject": str(row["subject"] or ""),
        "direction": str(row["direction"] or "inbound"),
        "status": str(row["status"] or ""),
        "source": str(row["source"] or ""),
        "person": str(row["person"] or ""),
        "happened_at": str(row["happened_at"] or ""),
        "external_from": str(row["external_from"] or ""),
        "external_to": str(row["external_to"] or ""),
        "message_id": str(row["message_id"] or ""),
        "thread_key": str(row["thread_key"] or ""),
        "snippet": str(row["snippet"] or ""),
        "attachment_count": len(_json_value(str(row["attachments_json"] or "[]"), [])),
        **contact,
    }


def _group_contacts(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    ordered_keys: list[str] = []
    for message in messages:
        contact_key = str(message.get("contact_key") or f"contact:{message['id']}")
        if contact_key not in grouped:
            grouped[contact_key] = {
                "contact_key": contact_key,
                "contact_label": str(message.get("contact_label") or ""),
                "contact_name": str(message.get("contact_name") or ""),
                "contact_email": str(message.get("contact_email") or ""),
                "latest_message_id": message["id"],
                "subject": message["subject"],
                "happened_at": message["happened_at"],
                "count": 0,
                "message_ids": [],
                "threads": [],
            }
            ordered_keys.append(contact_key)
        contact_group = grouped[contact_key]
        contact_group["count"] += 1
        contact_group["message_ids"].append(message["id"])

        thread_group_key = _thread_group_key(message)
        threads = contact_group["threads"]
        existing_thread = next(
            (entry for entry in threads if str(entry.get("thread_key") or "") == thread_group_key),
            None,
        )
        if existing_thread is None:
            existing_thread = {
                "thread_key": thread_group_key,
                "title": _normalized_subject(str(message.get("subject") or "")) or str(message.get("subject") or "(no subject)"),
                "latest_message_id": int(message["id"]),
                "latest_happened_at": str(message.get("happened_at") or ""),
                "count": 0,
                "message_ids": [],
            }
            threads.append(existing_thread)
        existing_thread["count"] += 1
        existing_thread["message_ids"].append(int(message["id"]))
    contacts = []
    for key in ordered_keys:
        contacts.append(grouped[key])
    return contacts


def _communication_ids_for_contact(
    connection: sqlite3.Connection,
    *,
    contact_key: str,
    source: str | None = DEFAULT_MAIL_UI_CORRESPONDENCE_SOURCE,
    channel: str = "email",
    direction: str | None = None,
) -> list[int]:
    if not contact_key:
        return []
    matches: list[int] = []
    if (source or "").strip().lower() == DEFAULT_MAIL_UI_CORRESPONDENCE_SOURCE:
        rows = _list_correspondence_rows(connection, limit=None)
    else:
        rows = store.list_communications(
            connection,
            source=source or None,
            channel=channel or None,
            direction=direction or None,
            status="all",
            limit=None,
        )
    for row in rows:
        if str(row["status"] or "") == "deleted":
            continue
        contact = _contact_identity(
            direction=str(row["direction"] or ""),
            person=str(row["person"] or ""),
            external_from=str(row["external_from"] or ""),
            external_to=str(row["external_to"] or ""),
            source=str(row["source"] or ""),
        )
        if contact["contact_key"] == contact_key:
            matches.append(int(row["id"]))
    return matches


def _draft_label(subject: str) -> str:
    clean = str(subject or "").strip()
    return clean or "(untitled draft)"


def _draft_snippet(body_text: str) -> str:
    lines = [line.strip() for line in str(body_text or "").splitlines() if line.strip()]
    if not lines:
        return ""
    snippet = lines[0]
    return snippet[:180]


def _address_field_values(value: str) -> list[str]:
    formatted: list[str] = []
    for name, email in getaddresses([str(value or "")]):
        clean_email = str(email or "").strip()
        clean_name = str(name or "").strip()
        if not clean_email:
            continue
        formatted.append(formataddr((clean_name, clean_email)) if clean_name else clean_email)
    return formatted


def _draft_summary(connection: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
    draft_id = int(row["id"])
    return {
        "id": draft_id,
        "subject": str(row["subject"] or ""),
        "label": _draft_label(str(row["subject"] or "")),
        "to": str(row["external_to"] or ""),
        "cc": str(row["external_cc"] or ""),
        "bcc": str(row["external_bcc"] or ""),
        "body_text": str(row["body_text"] or ""),
        "snippet": _draft_snippet(str(row["body_text"] or "")),
        "updated_at": str(row["happened_at"] or ""),
        "in_reply_to": str(row["in_reply_to"] or ""),
        "references": _json_value(str(row["references_json"] or "[]"), []),
        "thread_key": str(row["thread_key"] or row["external_thread_id"] or ""),
        "attachments": _draft_attachment_summaries(connection, draft_id=draft_id),
    }


def _mail_contact_label(display_name: str, email: str) -> str:
    clean_name = str(display_name or "").strip()
    clean_email = str(email or "").strip()
    return clean_name or clean_email or "(unknown contact)"


def _mail_contact_address(display_name: str, email: str) -> str:
    clean_name = str(display_name or "").strip()
    clean_email = str(email or "").strip()
    if clean_email:
        return formataddr((clean_name, clean_email)) if clean_name else clean_email
    return clean_name


def _mail_contact_summary(row: sqlite3.Row) -> dict[str, Any]:
    display_name = str(row["display_name"] or "")
    email = str(row["email"] or "")
    return {
        "id": int(row["id"]),
        "contact_key": str(row["contact_key"] or ""),
        "display_name": display_name,
        "email": email,
        "label": _mail_contact_label(display_name, email),
        "address": _mail_contact_address(display_name, email),
        "interaction_count": int(row["interaction_count"] or 0),
        "last_seen_at": str(row["last_seen_at"] or ""),
        "last_direction": str(row["last_direction"] or ""),
        "last_source": str(row["last_source"] or ""),
    }


def _list_mail_contacts(connection: sqlite3.Connection, *, query: str = "", limit: int = DEFAULT_MAIL_UI_CONTACT_LIMIT) -> list[dict[str, Any]]:
    rows = store.list_mail_contacts(connection, query=query, limit=max(1, int(limit)))
    return [_mail_contact_summary(row) for row in rows]


def _draft_sort_key(draft: dict[str, Any]) -> tuple[int, int]:
    happened_at = _parse_mail_ui_timestamp(str(draft.get("updated_at") or ""))
    happened_sort = int(happened_at.timestamp()) if happened_at is not None else 0
    try:
        draft_id = int(draft.get("id") or 0)
    except Exception:
        draft_id = 0
    return (happened_sort, draft_id)


def _sorted_drafts(drafts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [_clone_json_payload(draft) for draft in drafts if isinstance(draft, dict)],
        key=_draft_sort_key,
        reverse=True,
    )


def _strip_trailing_cmail_signature(body_text: str) -> str:
    clean = str(body_text or "").rstrip()
    changed = True
    while changed:
        changed = False
        for signature in _CMAIL_KNOWN_SIGNATURE_TEXTS:
            marker = signature.strip()
            if clean.endswith(marker):
                clean = clean[: -len(marker)].rstrip()
                changed = True
                break
    return clean


def _compose_cmail_body_text(body_text: str) -> str:
    unsigned = _strip_trailing_cmail_signature(body_text)
    if not unsigned:
        return _CMAIL_SIGNATURE_TEXT
    return f"{unsigned}\n\n{_CMAIL_SIGNATURE_TEXT}"


def _paragraph_html(text: str) -> str:
    clean = str(text or "").strip()
    if not clean:
        return ""
    paragraphs = [part.strip() for part in re.split(r"\n{2,}", clean) if part.strip()]
    return "".join(
        f'<p style="margin:0 0 16px 0; color:#111111; font:400 16px/1.7 -apple-system,BlinkMacSystemFont,\'Segoe UI\',Arial,sans-serif;">'
        f"{html_escape(paragraph).replace(chr(10), '<br>')}"
        "</p>"
        for paragraph in paragraphs
    )


def _compose_cmail_html_body(body_text: str) -> str:
    unsigned = _strip_trailing_cmail_signature(body_text)
    body_markup = _paragraph_html(unsigned)
    if body_markup:
        return f"{body_markup}\n{_CMAIL_SIGNATURE_EMAIL_HTML}"
    return _CMAIL_SIGNATURE_EMAIL_HTML


def _list_cmail_drafts(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = store.list_communications(
        connection,
        source="cmail_draft",
        channel="email",
        direction="outbound",
        status="draft",
        limit=None,
    )
    return [_draft_summary(connection, row) for row in rows if str(row["status"] or "") != "deleted"]


def _save_cmail_draft(connection: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    draft_id = int(payload.get("id") or 0)
    subject = str(payload.get("subject") or "").strip()
    external_to = str(payload.get("to") or "").strip()
    external_cc = str(payload.get("cc") or "").strip()
    external_bcc = str(payload.get("bcc") or "").strip()
    in_reply_to = str(payload.get("in_reply_to") or "").strip()
    references = mail_metadata.message_id_tokens(payload.get("references") or [])
    thread_key = str(payload.get("thread_key") or "").strip()
    raw_body_text = str(payload.get("body_text") or "")
    attachment_paths = [
        str(item).strip()
        for item in (payload.get("attachment_paths") or [])
        if str(item).strip()
    ]
    body_text = _compose_cmail_body_text(raw_body_text)
    html_body = _compose_cmail_html_body(raw_body_text)
    happened_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    snippet = _draft_snippet(body_text)

    if draft_id > 0:
        cursor = connection.execute(
            """
            UPDATE communications
            SET subject = ?, happened_at = ?, status = 'draft', source = 'cmail_draft',
                external_to = ?, external_cc = ?, external_bcc = ?, body_text = ?, html_body = ?,
                snippet = ?, direction = 'outbound', channel = 'email',
                in_reply_to = ?, references_json = ?, thread_key = ?, deleted_at = NULL
            WHERE id = ? AND source = 'cmail_draft'
            """,
            (
                subject,
                happened_at,
                external_to,
                external_cc,
                external_bcc,
                body_text,
                html_body,
                snippet,
                in_reply_to,
                json.dumps(references),
                thread_key,
                draft_id,
            ),
        )
        if int(cursor.rowcount or 0) == 0:
            raise KeyError(f"draft {draft_id} not found")
        store.touch_mail_contacts_for_communication(
            connection,
            communication_id=draft_id,
            happened_at=happened_at,
            direction="outbound",
            source="cmail_draft",
            external_to=external_to,
            external_cc=external_cc,
            external_bcc=external_bcc,
        )
        connection.commit()
        row = store.get_communication_by_id(connection, draft_id)
        if row is None:
            raise KeyError(f"draft {draft_id} not found")
        if attachment_paths:
            _add_draft_attachments(connection, draft_id=draft_id, attachment_paths=attachment_paths)
            row = store.get_communication_by_id(connection, draft_id)
            if row is None:
                raise KeyError(f"draft {draft_id} not found")
        return _draft_summary(connection, row)

    cursor = connection.execute(
        """
        INSERT INTO communications (
            subject, channel, direction, person, happened_at, status, notes, source,
            external_from, external_to, external_cc, external_bcc, body_text, html_body, snippet,
            in_reply_to, references_json, thread_key
        ) VALUES (?, 'email', 'outbound', '', ?, 'draft', '', 'cmail_draft', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            subject,
            happened_at,
            "Cody <cody@frg.earth>",
            external_to,
            external_cc,
            external_bcc,
            body_text,
            html_body,
            snippet,
            in_reply_to,
            json.dumps(references),
            thread_key,
        ),
    )
    draft_communication_id = int(cursor.lastrowid)
    store.touch_mail_contacts_for_communication(
        connection,
        communication_id=draft_communication_id,
        happened_at=happened_at,
        direction="outbound",
        source="cmail_draft",
        external_to=external_to,
        external_cc=external_cc,
        external_bcc=external_bcc,
    )
    connection.commit()
    row = store.get_communication_by_id(connection, draft_communication_id)
    if row is None:
        raise KeyError("draft create failed")
    if attachment_paths:
        _add_draft_attachments(connection, draft_id=draft_communication_id, attachment_paths=attachment_paths)
        row = store.get_communication_by_id(connection, draft_communication_id)
        if row is None:
            raise KeyError("draft create failed")
    return _draft_summary(connection, row)


def list_cmail_drafts(*, db_path: Path) -> list[dict[str, Any]]:
    with store.open_db(db_path) as connection:
        return _list_cmail_drafts(connection)


def get_cmail_draft_scaffold(
    *,
    db_path: Path,
    communication_id: int,
    mode: str = "reply",
) -> dict[str, Any]:
    with store.open_db(db_path) as connection:
        detail = _communication_detail(connection, communication_id)
    drafts = detail.get("drafts") or {}
    scaffold = drafts.get(mode)
    if not isinstance(scaffold, dict):
        raise KeyError(f"draft scaffold {mode!r} not found for communication {communication_id}")
    return {
        "subject": str(scaffold.get("subject") or ""),
        "to": str(scaffold.get("to") or ""),
        "cc": str(scaffold.get("cc") or ""),
        "in_reply_to": str(scaffold.get("in_reply_to") or ""),
        "references": list(scaffold.get("references") or []),
        "thread_key": str(scaffold.get("thread_key") or ""),
    }


def save_cmail_draft(*, db_path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    with store.open_db(db_path) as connection:
        return _save_cmail_draft(connection, payload)


def send_cmail_draft(
    *,
    db_path: Path,
    draft_id: int,
    config_path: Path | None = None,
) -> dict[str, Any]:
    temp_attachment_dir: Path | None = None
    temp_attachment_paths: list[Path] = []
    with store.open_db(db_path) as connection:
        row = store.get_communication_by_id(connection, int(draft_id))
        if row is None or str(row["source"] or "") != "cmail_draft":
            raise KeyError(f"draft {draft_id} not found")
        if str(row["status"] or "") != "draft":
            raise ValueError(f"draft {draft_id} is not sendable")
        subject = str(row["subject"] or "")
        body_text = str(row["body_text"] or "")
        html_body = _compose_cmail_html_body(str(row["body_text"] or ""))
        to_values = _address_field_values(str(row["external_to"] or ""))
        cc_values = _address_field_values(str(row["external_cc"] or ""))
        bcc_values = _address_field_values(str(row["external_bcc"] or ""))
        if not to_values:
            raise ValueError("add a recipient first")
        attachment_rows = _draft_attachment_rows(connection, draft_id=int(draft_id))
        if attachment_rows:
            temp_attachment_dir = Path(tempfile.mkdtemp(prefix="cmail-send-"))
        for attachment in attachment_rows:
            raw_bytes = mail_vault.read_encrypted_vault_file(
                vault_root=store.attachment_vault_root(),
                relative_path=str(attachment["relative_path"] or ""),
            )
            clean_filename = _safe_attachment_filename(str(attachment["filename"] or ""), fallback=f"attachment-{int(attachment['id'])}.bin")
            if temp_attachment_dir is None:
                temp_attachment_dir = Path(tempfile.mkdtemp(prefix="cmail-send-"))
            temp_path = temp_attachment_dir / clean_filename
            counter = 1
            while temp_path.exists():
                stem = Path(clean_filename).stem or f"attachment-{int(attachment['id'])}"
                suffix = Path(clean_filename).suffix
                temp_path = temp_attachment_dir / f"{stem}-{counter}{suffix}"
                counter += 1
            temp_path.write_bytes(raw_bytes)
            temp_attachment_paths.append(temp_path)

    try:
        delivery = resend_send_email(
            to=to_values,
            cc=cc_values,
            bcc=bcc_values,
            subject=subject,
            text=body_text,
            html=html_body,
            in_reply_to=str(row["in_reply_to"] or ""),
            references=_json_value(str(row["references_json"] or "[]"), []),
            thread_key=str(row["thread_key"] or row["external_thread_id"] or ""),
            attachment_paths=[str(path) for path in temp_attachment_paths],
            journal_db_path=db_path,
            config_path=config_path,
            attempt_immediately=False,
            apply_signature=False,
        )
    finally:
        if temp_attachment_dir is not None:
            shutil.rmtree(temp_attachment_dir, ignore_errors=True)

    with store.open_db(db_path) as connection:
        store.set_communication_status(
            connection,
            communication_id=int(draft_id),
            status="deleted",
        )
    return {
        "draft_id": int(draft_id),
        "draft_status": str(delivery.get("status") or "queued"),
        "delivery": delivery,
    }


def _draft_scaffolds(row: Any) -> dict[str, dict[str, Any]]:
    direction = str(row["direction"] or "").lower()
    external_from = str(row["external_from"] or "")
    external_to = str(row["external_to"] or "")
    external_reply_to = str(row["external_reply_to"] or "")
    external_cc = str(row["external_cc"] or "")
    subject = str(row["subject"] or "")
    message_id = str(row["message_id"] or "")
    thread_key = str(row["thread_key"] or row["external_thread_id"] or row["message_id"] or "")
    references = _json_value(str(row["references_json"] or "[]"), [])
    if message_id and message_id not in references:
        references = [*references, message_id]
    reply_target = (
        external_to
        if direction == "outbound"
        else (external_reply_to or external_from)
    )
    common = {
        "subject": _reply_subject(subject),
        "in_reply_to": message_id,
        "references": references,
        "thread_key": thread_key,
        "body": "",
    }
    return {
        "reply": {
            **common,
            "label": "reply to sender only",
            "meta": "Reply keeps thread metadata and addresses only the sender/reply-to target.",
            "to": reply_target,
            "cc": "",
        },
        "reply_all": {
            **common,
            "label": "reply-all scaffold",
            "meta": "Reply-all keeps thread metadata and preserves visible CC recipients.",
            "to": reply_target,
            "cc": external_cc,
        },
        "new_draft": {
            "label": "fresh draft scaffold",
            "meta": "Fresh draft keeps the recipient context but drops thread metadata.",
            "to": reply_target,
            "cc": "",
            "subject": subject,
            "in_reply_to": "",
            "references": [],
            "thread_key": "",
            "body": "",
        },
    }


def _communication_detail(connection: Any, communication_id: int) -> dict[str, Any]:
    row = store.get_communication_by_id(connection, communication_id)
    if row is None:
        raise KeyError(f"communication {communication_id} not found")
    contact = _contact_identity(
        direction=str(row["direction"] or ""),
        person=str(row["person"] or ""),
        external_from=str(row["external_from"] or ""),
        external_to=str(row["external_to"] or ""),
        source=str(row["source"] or ""),
    )

    attachments = [
        _attachment_summary_from_row(attachment)
        for attachment in store.list_communication_attachments(connection, communication_id=communication_id, limit=200)
    ]

    thread_key = str(row["thread_key"] or row["external_thread_id"] or row["message_id"] or "")
    thread_rows: list[dict[str, Any]] = []
    if thread_key:
        for thread_row in connection.execute(
            """
            SELECT id, subject, happened_at, direction, source
            FROM communications
            WHERE thread_key = ?
            ORDER BY happened_at ASC, id ASC
            """,
            (thread_key,),
        ).fetchall():
            thread_rows.append(
                {
                    "id": int(thread_row["id"]),
                    "subject": str(thread_row["subject"] or ""),
                    "happened_at": str(thread_row["happened_at"] or ""),
                    "direction": str(thread_row["direction"] or ""),
                    "source": str(thread_row["source"] or ""),
                }
            )

    return {
        "id": int(row["id"]),
        "external_id": str(row["external_id"] or ""),
        "read_key": _communication_read_key(row),
        "subject": str(row["subject"] or ""),
        "channel": str(row["channel"] or ""),
        "direction": str(row["direction"] or ""),
        "status": str(row["status"] or ""),
        "source": str(row["source"] or ""),
        "person": str(row["person"] or ""),
        **contact,
        "organization_name": str(row["organization_name"] or ""),
        "happened_at": str(row["happened_at"] or ""),
        "follow_up_at": str(row["follow_up_at"] or ""),
        "external_from": str(row["external_from"] or ""),
        "external_to": str(row["external_to"] or ""),
        "external_cc": str(row["external_cc"] or ""),
        "external_bcc": str(row["external_bcc"] or ""),
        "external_reply_to": str(row["external_reply_to"] or ""),
        "message_id": str(row["message_id"] or ""),
        "in_reply_to": str(row["in_reply_to"] or ""),
        "thread_key": thread_key,
        "snippet": str(row["snippet"] or ""),
        "body_text": str(row["body_text"] or ""),
        "html_body": str(row["html_body"] or ""),
        "raw_relative_path": str(row["raw_relative_path"] or ""),
        "headers": _json_value(str(row["headers_json"] or "{}"), {}),
        "to": _json_value(str(row["to_json"] or "[]"), []),
        "cc": _json_value(str(row["cc_json"] or "[]"), []),
        "bcc": _json_value(str(row["bcc_json"] or "[]"), []),
        "reply_to": _json_value(str(row["reply_to_json"] or "[]"), []),
        "references": _json_value(str(row["references_json"] or "[]"), []),
        "attachments": attachments,
        "thread_messages": thread_rows,
        "body_display": _body_display(
            str(row["body_text"] or ""),
            html_body=str(row["html_body"] or ""),
            snippet=str(row["snippet"] or ""),
        ),
        "drafts": _draft_scaffolds(row),
    }


def _read_attachment_content(connection: Any, attachment_id: int) -> tuple[bytes, str]:
    row = connection.execute(
        "SELECT relative_path, mime_type FROM communication_attachments WHERE id = ?",
        (attachment_id,),
    ).fetchone()
    if row is None:
        raise KeyError(f"attachment {attachment_id} not found")
    relative_path = str(row["relative_path"] or "")
    mime_type = str(row["mime_type"] or "application/octet-stream")
    raw_bytes = mail_vault.read_encrypted_vault_file(
        vault_root=store.attachment_vault_root(),
        relative_path=relative_path,
    )
    return raw_bytes, mime_type


def _active_alerts_from_connection(connection: sqlite3.Connection, *, limit: int = 25) -> list[dict[str, Any]]:
    latest_cloudflare_success_at = store.get_sync_state(connection, "cloudflare_mail:last_success_at")
    return [
        {
            "alert_key": str(row["alert_key"] or ""),
            "source": str(row["source"] or ""),
            "severity": str(row["severity"] or ""),
            "status": str(row["status"] or ""),
            "title": str(row["title"] or ""),
            "message": str(row["message"] or ""),
        }
        for row in store.list_system_alerts(connection, status="active", limit=limit)
        if not _is_stale_cloudflare_sync_alert(
            row=row,
            latest_success_at=latest_cloudflare_success_at,
        )
    ]


def _resend_queue_status_from_connection(connection: sqlite3.Connection, *, limit: int = 25) -> dict[str, Any]:
    rows = store.list_mail_delivery_queue(
        connection,
        provider="resend",
        status="all",
        limit=max(1, int(limit)),
    )
    alerts = store.list_system_alerts(
        connection,
        source="resend_delivery",
        status="active",
        limit=max(1, int(limit)),
    )

    counts: dict[str, int] = {}
    for row in rows:
        status_key = str(row["status"] or "unknown")
        counts[status_key] = counts.get(status_key, 0) + 1
    active_queue_count = sum(
        1
        for row in rows
        if str(row["status"] or "") in {"queued", "retrying"}
    )
    return {
        "queue_count": active_queue_count,
        "retained_count": len(rows),
        "due_count": sum(
            1
            for row in rows
            if str(row["status"] or "") in {"queued", "retrying"}
            and str(row["next_attempt_at"] or "") <= store._utc_now_string()
        ),
        "counts": counts,
        "items": [
            {
                "queue_id": int(row["id"]),
                "queue_key": str(row["queue_key"] or ""),
                "communication_id": int(row["communication_id"]),
                "status": str(row["status"] or ""),
                "attempt_count": int(row["attempt_count"]),
                "max_attempts": int(row["max_attempts"]),
                "next_attempt_at": str(row["next_attempt_at"] or ""),
                "last_error": str(row["last_error"] or ""),
                "subject": str(row["subject"] or ""),
                "external_to": str(row["external_to"] or ""),
            }
            for row in rows
        ],
        "active_alert_count": len(alerts),
        "active_alerts": [
            {
                "alert_key": str(row["alert_key"] or ""),
                "severity": str(row["severity"] or ""),
                "title": str(row["title"] or ""),
                "message": str(row["message"] or ""),
            }
            for row in alerts
        ],
    }


def _parse_mail_ui_timestamp(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return store.parse_datetime(text)
    except Exception:
        return None


def _is_stale_cloudflare_sync_alert(*, row: Any, latest_success_at: str | None) -> bool:
    if str(row["alert_key"] or "") != "cloudflare_mail_sync":
        return False
    latest_success = _parse_mail_ui_timestamp(latest_success_at)
    alert_seen = _parse_mail_ui_timestamp(str(row["last_seen_at"] or row["updated_at"] or ""))
    if latest_success is None or alert_seen is None:
        return False
    return latest_success >= alert_seen


def _cloudflare_sync_status_from_connection(connection: sqlite3.Connection) -> dict[str, Any]:
    last_sync_at = store.get_sync_state(connection, "cloudflare_mail:last_sync_at")
    last_success_at = store.get_sync_state(connection, "cloudflare_mail:last_success_at")
    last_failure_at = store.get_sync_state(connection, "cloudflare_mail:last_failure_at")
    sync_dt = _parse_mail_ui_timestamp(last_sync_at)
    success_dt = _parse_mail_ui_timestamp(last_success_at)
    failure_dt = _parse_mail_ui_timestamp(last_failure_at)
    if sync_dt and (failure_dt is None or sync_dt >= failure_dt):
        status = "healthy"
    elif success_dt and (failure_dt is None or success_dt >= failure_dt):
        status = "healthy"
    elif failure_dt:
        status = "degraded"
    else:
        status = "unknown"
    return {
        "status": status,
        "last_sync_at": str(last_sync_at or ""),
        "last_success_at": str(last_success_at or ""),
        "last_failure_at": str(last_failure_at or ""),
    }


def _cloudflare_queue_status_from_connection(connection: sqlite3.Connection) -> dict[str, Any]:
    def _int_state(key: str) -> int | None:
        raw = store.get_sync_state(connection, key)
        if raw in (None, ""):
            return None
        try:
            return int(str(raw))
        except (TypeError, ValueError):
            return None

    def _bool_state(key: str) -> bool | None:
        raw = store.get_sync_state(connection, key)
        if raw in (None, ""):
            return None
        return str(raw).strip().lower() in {"1", "true", "yes"}

    return {
        "pending_count": _int_state("cloudflare_mail:pending_count"),
        "total_stored": _int_state("cloudflare_mail:total_stored"),
        "total_acknowledged": _int_state("cloudflare_mail:total_acknowledged"),
        "forwarding_enabled": _bool_state("cloudflare_mail:forwarding_enabled"),
        "archive_encryption_enabled": _bool_state("cloudflare_mail:archive_encryption_enabled"),
        "source": "local_sync_state",
    }


def _list_correspondence_rows(connection: sqlite3.Connection, *, limit: int | None) -> list[sqlite3.Row]:
    suppressed_ids = set(_orphaned_resend_correspondence_ids(connection))
    query = """
        SELECT communications.*, organizations.name AS organization_name
        FROM communications
        LEFT JOIN organizations ON organizations.id = communications.organization_id
        WHERE communications.channel = 'email'
          AND communications.status != 'deleted'
          AND (
            (communications.source = ? AND communications.direction = 'inbound')
            OR
            (communications.source = ? AND communications.direction = 'outbound')
          )
        ORDER BY communications.happened_at DESC, communications.id DESC
    """
    params: list[Any] = [DEFAULT_MAIL_UI_SOURCE, DEFAULT_MAIL_UI_OUTBOUND_SOURCE]
    if limit is not None:
        query += " LIMIT ?"
        params.append(max(1, int(limit)))
    rows = connection.execute(query, params).fetchall()
    if not suppressed_ids:
        return rows
    return [row for row in rows if int(row["id"]) not in suppressed_ids]


def _orphaned_resend_correspondence_ids(connection: sqlite3.Connection) -> list[int]:
    orphaned_rows = connection.execute(
        """
        SELECT communications.id
        FROM communications
        WHERE communications.channel = 'email'
          AND communications.source = ?
          AND communications.direction = 'outbound'
          AND communications.status IN ('queued', 'retrying', 'sending')
          AND NOT EXISTS (
            SELECT 1
            FROM mail_delivery_queue
            WHERE mail_delivery_queue.provider = 'resend'
              AND mail_delivery_queue.communication_id = communications.id
          )
        ORDER BY communications.id
        """,
        (DEFAULT_MAIL_UI_OUTBOUND_SOURCE,),
    ).fetchall()
    return [int(row["id"]) for row in orphaned_rows]


def _cleanup_orphaned_resend_correspondence(db_path: Path) -> list[int]:
    with store.open_db(db_path) as connection:
        orphaned_ids = _orphaned_resend_correspondence_ids(connection)
        if not orphaned_ids:
            return []

        deleted_at = store._utc_now_string()
        note = "Suppressed orphaned outbound Resend artifact with no live queue entry."
        placeholders = ", ".join("?" for _ in orphaned_ids)
        connection.execute(
            f"""
            UPDATE communications
            SET status = 'deleted',
                deleted_at = COALESCE(deleted_at, ?),
                notes = CASE
                    WHEN TRIM(COALESCE(notes, '')) = '' THEN ?
                    WHEN instr(notes, ?) > 0 THEN notes
                    ELSE notes || char(10) || ?
                END
            WHERE id IN ({placeholders})
            """,
            (deleted_at, note, note, note, *orphaned_ids),
        )
        connection.commit()
        return orphaned_ids


def _recipient_match_key(value: str) -> tuple[str, ...]:
    addresses: list[str] = []
    for _name, email in getaddresses([str(value or "").replace(";", ",")]):
        clean_email = str(email or "").strip().lower()
        if clean_email:
            addresses.append(clean_email)
    if not addresses:
        addresses = [
            part.strip().lower()
            for part in re.split(r"[,;]+", str(value or ""))
            if part.strip()
        ]
    return tuple(sorted(dict.fromkeys(addresses)))


def _superseded_cmail_draft_ids(connection: sqlite3.Connection) -> list[int]:
    draft_rows = connection.execute(
        """
        SELECT id, external_to, subject, body_text, in_reply_to, thread_key
        FROM communications
        WHERE channel = 'email'
          AND source = 'cmail_draft'
          AND direction = 'outbound'
          AND status IN ('queued', 'retrying', 'sending')
        ORDER BY id
        """,
    ).fetchall()
    if not draft_rows:
        return []

    outbound_rows = connection.execute(
        """
        SELECT id, external_to, subject, body_text, in_reply_to, thread_key
        FROM communications
        WHERE channel = 'email'
          AND source = ?
          AND direction = 'outbound'
          AND status IN ('queued', 'retrying', 'sent')
        ORDER BY id
        """,
        (DEFAULT_MAIL_UI_OUTBOUND_SOURCE,),
    ).fetchall()

    superseded_ids: list[int] = []
    for draft in draft_rows:
        draft_id = int(draft["id"])
        draft_recipients = _recipient_match_key(str(draft["external_to"] or ""))
        if not draft_recipients:
            continue
        draft_in_reply_to = str(draft["in_reply_to"] or "")
        draft_thread_key = str(draft["thread_key"] or "")
        for outbound in outbound_rows:
            if int(outbound["id"]) <= draft_id:
                continue
            if _recipient_match_key(str(outbound["external_to"] or "")) != draft_recipients:
                continue
            if str(outbound["subject"] or "") != str(draft["subject"] or ""):
                continue
            if str(outbound["body_text"] or "") != str(draft["body_text"] or ""):
                continue
            if draft_in_reply_to and str(outbound["in_reply_to"] or "") != draft_in_reply_to:
                continue
            if draft_thread_key and str(outbound["thread_key"] or "") != draft_thread_key:
                continue
            superseded_ids.append(draft_id)
            break
    return superseded_ids


def _cleanup_superseded_cmail_drafts(db_path: Path) -> list[int]:
    with store.open_db(db_path) as connection:
        superseded_ids = _superseded_cmail_draft_ids(connection)
        if not superseded_ids:
            return []

        deleted_at = store._utc_now_string()
        note = "Retired CMAIL draft after outbound handoff created the real resend_email correspondence."
        placeholders = ", ".join("?" for _ in superseded_ids)
        connection.execute(
            f"""
            UPDATE communications
            SET status = 'deleted',
                deleted_at = COALESCE(deleted_at, ?),
                notes = CASE
                    WHEN TRIM(COALESCE(notes, '')) = '' THEN ?
                    WHEN instr(notes, ?) > 0 THEN notes
                    ELSE notes || char(10) || ?
                END
            WHERE id IN ({placeholders})
            """,
            (deleted_at, note, note, note, *superseded_ids),
        )
        connection.commit()
        return superseded_ids


def _cleanup_active_cmail_draft_deleted_markers(db_path: Path) -> list[int]:
    with store.open_db(db_path) as connection:
        rows = connection.execute(
            """
            SELECT id
            FROM communications
            WHERE channel = 'email'
              AND source = 'cmail_draft'
              AND direction = 'outbound'
              AND status = 'draft'
              AND deleted_at IS NOT NULL
            ORDER BY id
            """
        ).fetchall()
        draft_ids = [int(row["id"]) for row in rows]
        if not draft_ids:
            return []
        placeholders = ", ".join("?" for _ in draft_ids)
        connection.execute(
            f"UPDATE communications SET deleted_at = NULL WHERE id IN ({placeholders})",
            draft_ids,
        )
        connection.commit()
        return draft_ids


def cleanup_cmail_correspondence_artifacts(*, db_path: Path) -> dict[str, list[int]]:
    orphaned_resend_ids = _cleanup_orphaned_resend_correspondence(db_path)
    superseded_draft_ids = _cleanup_superseded_cmail_drafts(db_path)
    restored_draft_ids = _cleanup_active_cmail_draft_deleted_markers(db_path)
    return {
        "orphaned_resend_ids": orphaned_resend_ids,
        "superseded_draft_ids": superseded_draft_ids,
        "restored_draft_ids": restored_draft_ids,
    }


def _correspondence_mailbox_version_from_connection(connection: sqlite3.Connection) -> dict[str, Any]:
    params: list[Any] = [DEFAULT_MAIL_UI_SOURCE, DEFAULT_MAIL_UI_OUTBOUND_SOURCE]
    suppressed_ids = _orphaned_resend_correspondence_ids(connection)
    suppressed_clause = ""
    if suppressed_ids:
        placeholders = ", ".join("?" for _ in suppressed_ids)
        suppressed_clause = f" AND communications.id NOT IN ({placeholders})"
        params.extend(int(value) for value in suppressed_ids)
    where_clause = f"""
        communications.channel = 'email'
        AND communications.status != 'deleted'
        AND (
            (communications.source = ? AND communications.direction = 'inbound')
            OR
            (communications.source = ? AND communications.direction = 'outbound')
        )
        {suppressed_clause}
    """
    count_row = connection.execute(
        f"""
        SELECT
            COUNT(*) AS message_count,
            MAX(id) AS latest_message_id,
            MAX(happened_at) AS latest_happened_at
        FROM communications
        WHERE {where_clause}
        """,
        params,
    ).fetchone()
    contact_row = connection.execute(
        f"""
        SELECT COUNT(*) AS contact_count
        FROM (
            SELECT
                LOWER(TRIM(
                    CASE
                        WHEN direction = 'outbound' THEN COALESCE(NULLIF(external_to, ''), NULLIF(person, ''), NULLIF(source, ''), 'unknown recipient')
                        ELSE COALESCE(NULLIF(external_from, ''), NULLIF(person, ''), NULLIF(source, ''), 'unknown sender')
                    END
                )) AS contact_key
            FROM communications
            WHERE {where_clause}
            GROUP BY contact_key
        ) grouped_contacts
        """,
        params,
    ).fetchone()
    return {
        "message_count": int((count_row["message_count"] if count_row is not None else 0) or 0),
        "contact_count": int((contact_row["contact_count"] if contact_row is not None else 0) or 0),
        "latest_message_id": int((count_row["latest_message_id"] if count_row is not None else 0) or 0),
        "latest_happened_at": str((count_row["latest_happened_at"] if count_row is not None else "") or ""),
    }


def _build_correspondence_overview_from_connection(
    connection: sqlite3.Connection,
    *,
    limit: int = DEFAULT_MAIL_UI_LIMIT,
    include_details: bool = False,
) -> dict[str, Any]:
    rows = _list_correspondence_rows(connection, limit=limit)
    messages = [_communication_summary(row) for row in rows]
    contacts = _group_contacts(messages)
    payload = {
        "message_count": len(messages),
        "contact_count": len(contacts),
        "messages": messages,
        "contacts": contacts,
        "cloudflare_queue": _cloudflare_queue_status_from_connection(connection),
        "cloudflare_sync": _cloudflare_sync_status_from_connection(connection),
        "mailbox_version": _correspondence_mailbox_version_from_connection(connection),
    }
    if include_details:
        payload["details"] = {
            str(message["id"]): _communication_detail(connection, int(message["id"]))
            for message in messages
        }
    return payload


def _record_cloudflare_queue_heartbeat(*, db_path: Path) -> dict[str, Any] | None:
    try:
        result = cloudflare_mail_queue_status(timeout_seconds=MAIL_UI_HEARTBEAT_REQUEST_TIMEOUT_SECONDS)
    except Exception:
        return None
    heartbeat_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    return {
        **result,
        "heartbeat_at": heartbeat_at,
    }


def _hidden_contacts_from_connection(connection: sqlite3.Connection) -> dict[str, str]:
    return store.get_hidden_mail_contacts(connection)


def _set_hidden_contacts(
    connection: sqlite3.Connection,
    hidden_contacts: dict[str, str],
) -> None:
    store.set_hidden_mail_contacts(connection, hidden_contacts)


def _mark_contact_hidden(
    connection: sqlite3.Connection,
    *,
    contact_key: str,
    hidden_at: str | None = None,
) -> None:
    store.mark_hidden_mail_contact(connection, contact_key=contact_key, hidden_at=hidden_at)


def _build_mail_ui_overview_from_connection(
    connection: sqlite3.Connection,
    *,
    source: str | None = DEFAULT_MAIL_UI_SOURCE,
    channel: str | None = "email",
    direction: str | None = None,
    status: str | None = "all",
    limit: int = DEFAULT_MAIL_UI_LIMIT,
    include_details: bool = False,
) -> dict[str, Any]:
    raw_messages = [
        _communication_summary(row)
        for row in store.list_communications(
            connection,
            source=source or None,
            channel=channel or None,
            direction=direction or None,
            status=status or None,
            limit=max(1, int(limit)),
        )
        if str(row["status"] or "") != "deleted"
    ]
    messages = raw_messages
    sync = _cloudflare_sync_status_from_connection(connection)

    cloudflare = _cloudflare_queue_status_from_connection(connection)

    contacts = _group_contacts(messages)
    payload = {
        "message_count": len(messages),
        "contact_count": len(contacts),
        "messages": messages,
        "contacts": contacts,
        "cloudflare_queue": cloudflare,
        "cloudflare_sync": sync,
        "mailbox_version": _mailbox_version_from_connection(
            connection,
            source=source,
            channel=channel,
            direction=direction,
            status=status,
        ),
    }
    if include_details:
        payload["details"] = {
            str(message["id"]): _communication_detail(connection, int(message["id"]))
            for message in messages
        }
    return payload


def _mailbox_version_from_connection(
    connection: sqlite3.Connection,
    *,
    source: str | None = DEFAULT_MAIL_UI_SOURCE,
    channel: str | None = "email",
    direction: str | None = None,
    status: str | None = "all",
) -> dict[str, Any]:
    clauses = ["status != ?"]
    params: list[Any] = ["deleted"]
    if source:
        clauses.append("source = ?")
        params.append(source)
    if channel:
        clauses.append("channel = ?")
        params.append(channel)
    if direction:
        clauses.append("direction = ?")
        params.append(direction)
    if status and status != "all":
        clauses.append("status = ?")
        params.append(status)
    where_clause = " AND ".join(clauses)
    count_row = connection.execute(
        f"SELECT COUNT(*) AS message_count, MAX(id) AS latest_message_id, MAX(happened_at) AS latest_happened_at FROM communications WHERE {where_clause}",
        params,
    ).fetchone()
    contact_count = 0
    contact_query = f"""
        SELECT COUNT(*) AS contact_count
        FROM (
            SELECT LOWER(TRIM(COALESCE(external_from, person, ''))) AS contact_key
            FROM communications
            WHERE {where_clause}
            GROUP BY LOWER(TRIM(COALESCE(external_from, person, '')))
        ) grouped_contacts
    """
    contact_row = connection.execute(contact_query, params).fetchone()
    if contact_row is not None:
        try:
            contact_count = int(contact_row["contact_count"] or 0)
        except Exception:
            contact_count = 0
    return {
        "message_count": int((count_row["message_count"] if count_row is not None else 0) or 0),
        "contact_count": contact_count,
        "latest_message_id": int((count_row["latest_message_id"] if count_row is not None else 0) or 0),
        "latest_happened_at": str((count_row["latest_happened_at"] if count_row is not None else "") or ""),
    }


def build_mail_ui_overview(
    *,
    db_path: Path,
    source: str | None = DEFAULT_MAIL_UI_SOURCE,
    channel: str | None = "email",
    direction: str | None = None,
    status: str | None = "all",
    limit: int = DEFAULT_MAIL_UI_LIMIT,
    include_details: bool = False,
) -> dict[str, Any]:
    with store.open_db(db_path) as connection:
        payload = (
            _build_correspondence_overview_from_connection(
                connection,
                limit=limit,
                include_details=include_details,
            )
            if (source or "").strip().lower() == DEFAULT_MAIL_UI_CORRESPONDENCE_SOURCE
            else _build_mail_ui_overview_from_connection(
                connection,
                source=source,
                channel=channel,
                direction=direction,
                status=status,
                limit=limit,
                include_details=include_details,
            )
        )
    return {
        "db_path": str(db_path),
        **payload,
    }


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, indent=2).encode("utf-8")


class _MailUiSnapshotCache:
    def __init__(self, *, db_path: Path) -> None:
        self.db_path = db_path
        self._lock = threading.Lock()
        self._stamp: tuple[str, int, int] | None = None
        self._snapshot_path: Path | None = None

    def _current_stamp(self) -> tuple[str, int, int] | None:
        manifest_path = store.encrypted_db_manifest_path(self.db_path)
        target = manifest_path if manifest_path.exists() else self.db_path
        if not target.exists():
            return None
        stat = target.stat()
        return (str(target), stat.st_mtime_ns, stat.st_size)

    def _load_snapshot_bytes(self) -> bytes:
        # The encrypted DB manifest is replaced atomically on seal, so the UI
        # can safely read the latest sealed snapshot without taking the
        # exclusive writer lock.
        if store.db_storage_exists(self.db_path):
            return store.read_db_bytes(self.db_path)
        return b""

    def _build_snapshot_file(self, payload_bytes: bytes) -> Path:
        snapshot_path = store._secure_temp_db_path(prefix="life-ops-mail-ui-")
        if payload_bytes:
            snapshot_path.write_bytes(payload_bytes)
            return snapshot_path
        connection = sqlite3.connect(str(snapshot_path))
        try:
            store._configure_connection(connection, encrypted_storage=False)
            connection.executescript(store.SCHEMA)
            store._apply_migrations(connection)
            connection.commit()
        finally:
            connection.close()
        return snapshot_path

    def _connection_from_snapshot(self, snapshot_path: Path) -> sqlite3.Connection:
        connection = sqlite3.connect(f"file:{snapshot_path}?mode=ro", uri=True, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA query_only = ON")
        return connection

    def get_connection(self) -> sqlite3.Connection:
        if not store.encrypted_db_enabled(self.db_path):
            connection = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True, timeout=30.0)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA busy_timeout = 30000")
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA query_only = ON")
            return connection
        with self._lock:
            stamp = self._current_stamp()
            snapshot_path = self._snapshot_path
            if snapshot_path is not None and snapshot_path.exists() and self._stamp == stamp:
                return self._connection_from_snapshot(snapshot_path)
            try:
                payload_bytes = self._load_snapshot_bytes()
            except TimeoutError:
                if snapshot_path is not None and snapshot_path.exists():
                    return self._connection_from_snapshot(snapshot_path)
                raise
            next_snapshot_path = self._build_snapshot_file(payload_bytes)
            previous_snapshot_path = self._snapshot_path
            self._snapshot_path = next_snapshot_path
            self._stamp = self._current_stamp()
            if previous_snapshot_path is not None and previous_snapshot_path != next_snapshot_path:
                previous_snapshot_path.unlink(missing_ok=True)
            return self._connection_from_snapshot(next_snapshot_path)

    def invalidate(self) -> None:
        with self._lock:
            self._stamp = None
            if self._snapshot_path is not None:
                self._snapshot_path.unlink(missing_ok=True)
            self._snapshot_path = None


def _clone_json_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not payload:
        return {}
    return json.loads(json.dumps(payload))


def _mailbox_version_from_messages(messages: list[dict[str, Any]]) -> dict[str, Any]:
    latest_message_id = 0
    latest_happened_at = ""
    for message in messages:
        try:
            latest_message_id = max(latest_message_id, int(message.get("id") or 0))
        except Exception:
            pass
        happened_at = str(message.get("happened_at") or "")
        if happened_at and happened_at > latest_happened_at:
            latest_happened_at = happened_at
    contacts = _group_contacts(messages)
    return {
        "message_count": len(messages),
        "contact_count": len(contacts),
        "latest_message_id": latest_message_id,
        "latest_happened_at": latest_happened_at,
    }


def _remove_message_from_overview_payload(payload: dict[str, Any] | None, communication_id: int) -> dict[str, Any]:
    if not payload:
        return {}
    messages = [
        message
        for message in list(payload.get("messages") or [])
        if int(message.get("id") or 0) != int(communication_id)
    ]
    contacts = _group_contacts(messages)
    return {
        **payload,
        "messages": messages,
        "contacts": contacts,
        "message_count": len(messages),
        "contact_count": len(contacts),
        "mailbox_version": _mailbox_version_from_messages(messages),
    }


def _remove_contact_from_overview_payload(payload: dict[str, Any] | None, contact_key: str) -> dict[str, Any]:
    if not payload:
        return {}
    clean_contact_key = str(contact_key or "").strip()
    if not clean_contact_key:
        return _clone_json_payload(payload)
    messages = [
        message
        for message in list(payload.get("messages") or [])
        if str(message.get("contact_key") or "") != clean_contact_key
    ]
    contacts = _group_contacts(messages)
    return {
        **payload,
        "messages": messages,
        "contacts": contacts,
        "message_count": len(messages),
        "contact_count": len(contacts),
        "mailbox_version": _mailbox_version_from_messages(messages),
    }


_CALENDAR_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Life Ops Calendar</title>
  <link rel="icon" href="/static/favicon.svg" type="image/svg+xml">
  <style>
    :root {
      color-scheme: dark;
      --bg: #090d0a;
      --panel: #111712;
      --panel-2: #172019;
      --line: #29352b;
      --text: #f1f5ed;
      --muted: #96a394;
      --accent: #b8ff4d;
      --warning: #ffd166;
      --danger: #ff8b78;
      --sans: "SF Pro Display", "Inter", system-ui, sans-serif;
      --mono: "SFMono-Regular", "JetBrains Mono", "Menlo", monospace;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      color: var(--text);
      font-family: var(--sans);
      background:
        radial-gradient(circle at 18% 0%, rgba(184,255,77,0.16) 0, transparent 34%),
        radial-gradient(circle at 86% 16%, rgba(255,209,102,0.12) 0, transparent 28%),
        linear-gradient(180deg, #0c120e 0%, var(--bg) 58%);
    }
    .shell {
      width: min(1320px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 22px 0 28px;
      display: grid;
      gap: 16px;
    }
    .topbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      flex-wrap: wrap;
    }
    h1 {
      margin: 0;
      font-size: clamp(40px, 7vw, 82px);
      line-height: 0.92;
      letter-spacing: -0.07em;
    }
    .tabbar {
      display: inline-flex;
      gap: 8px;
      padding: 4px;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: rgba(255,255,255,0.035);
    }
    .tabbar a {
      color: var(--muted);
      text-decoration: none;
      padding: 8px 14px;
      border-radius: 999px;
      font: 700 13px/1 var(--mono);
      letter-spacing: 0.08em;
      text-transform: uppercase;
    }
    .tabbar a.active {
      color: #0a1009;
      background: var(--accent);
    }
    .toolbar, .form-grid {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
    }
    input, select, textarea, button {
      border: 1px solid var(--line);
      border-radius: 13px;
      background: #0d130f;
      color: var(--text);
      padding: 10px 12px;
      font: 600 14px/1.3 var(--sans);
    }
    textarea {
      width: 100%;
      min-height: 82px;
      resize: vertical;
      line-height: 1.55;
    }
    button {
      cursor: pointer;
    }
    button.primary {
      border: none;
      color: #091008;
      background: linear-gradient(180deg, #cfff77 0%, #8ecc2d 100%);
      font-weight: 800;
    }
    button.secondary {
      background: var(--panel-2);
    }
    .grid {
      display: grid;
      grid-template-columns: minmax(0, 1.1fr) minmax(360px, 0.9fr);
      gap: 14px;
    }
    .panel {
      border: 1px solid var(--line);
      border-radius: 22px;
      background:
        linear-gradient(180deg, rgba(255,255,255,0.035), rgba(255,255,255,0.012)),
        var(--panel);
      overflow: hidden;
    }
    .panel-head {
      padding: 16px 18px;
      border-bottom: 1px solid var(--line);
      display: flex;
      justify-content: space-between;
      gap: 14px;
      align-items: center;
    }
    .panel-title {
      font-size: 18px;
      font-weight: 800;
      letter-spacing: -0.02em;
    }
    .panel-body {
      padding: 18px;
      display: grid;
      gap: 16px;
    }
    .stats {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
    }
    .stat {
      border: 1px solid var(--line);
      border-radius: 16px;
      background: rgba(255,255,255,0.025);
      padding: 12px;
    }
    .stat-label {
      color: var(--muted);
      font: 700 11px/1 var(--mono);
      letter-spacing: 0.1em;
      text-transform: uppercase;
    }
    .stat-value {
      margin-top: 8px;
      font-size: 30px;
      font-weight: 800;
    }
    .section {
      display: grid;
      gap: 10px;
    }
    .section h2 {
      margin: 0;
      color: var(--accent);
      font: 800 12px/1 var(--mono);
      letter-spacing: 0.14em;
      text-transform: uppercase;
    }
    .entry {
      border: 1px solid var(--line);
      border-radius: 16px;
      background: rgba(255,255,255,0.02);
      padding: 12px 13px;
      display: grid;
      gap: 8px;
    }
    .entry.done {
      opacity: 0.72;
    }
    .entry-title {
      font-size: 17px;
      font-weight: 800;
      line-height: 1.25;
    }
    .entry-meta, .muted {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
    }
    .entry-actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .entry-actions button {
      padding: 7px 10px;
      border-radius: 999px;
      font-size: 12px;
    }
    .note-card {
      border: 1px solid rgba(184,255,77,0.18);
      border-radius: 18px;
      background: rgba(184,255,77,0.07);
      padding: 14px;
      display: grid;
      gap: 8px;
    }
    .empty {
      color: var(--muted);
      padding: 14px;
      border: 1px dashed var(--line);
      border-radius: 16px;
    }
    .status {
      color: var(--muted);
      font-size: 13px;
      min-height: 18px;
    }
    @media (max-width: 980px) {
      .grid { grid-template-columns: 1fr; }
      .stats { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
  </style>
</head>
<body>
  <main class="shell">
    <div class="topbar">
      <h1>Calendar</h1>
      <div class="tabbar">
        <a href="/">Correspondence</a>
        <a href="/#drafts">Drafts</a>
        <a href="/calendar" class="active">Calendar</a>
      </div>
    </div>
    <div class="toolbar">
      <input id="calendarDate" type="date">
      <button class="secondary" type="button" id="loadDayButton">load day</button>
      <button class="primary" type="button" id="saveDayButton">save historic day</button>
      <button class="secondary" type="button" id="rolloverButton">roll unfinished to tomorrow</button>
      <span class="status" id="statusText"></span>
    </div>
    <section class="grid">
      <section class="panel">
        <div class="panel-head">
          <div>
            <div class="panel-title" id="dayTitle">Loading calendar…</div>
            <div class="muted" id="daySubtitle">Local-first Life Ops day view</div>
          </div>
        </div>
        <div class="panel-body" id="dayBody"></div>
      </section>
      <aside class="panel">
        <div class="panel-head">
          <div class="panel-title">Capture</div>
        </div>
        <div class="panel-body">
          <form class="section" id="entryForm">
            <h2>New Entry</h2>
            <input id="entryTitle" type="text" placeholder="What needs to be tracked?">
            <div class="form-grid">
              <select id="entryType">
                <option value="task">task</option>
                <option value="event">event</option>
                <option value="note">note</option>
                <option value="memory">memory</option>
                <option value="habit">habit</option>
                <option value="milestone">milestone</option>
              </select>
              <select id="entryPriority">
                <option value="normal">normal</option>
                <option value="urgent">urgent</option>
                <option value="high">high</option>
                <option value="low">low</option>
              </select>
              <input id="entryStartTime" type="time" aria-label="start time">
            </div>
            <textarea id="entryNotes" placeholder="Notes, evidence, context, or why this matters"></textarea>
            <button class="primary" type="submit">add to day</button>
          </form>
          <form class="section" id="noteForm">
            <h2>Day Notes</h2>
            <input id="dayIntention" type="text" placeholder="Intention">
            <input id="dayMood" type="text" placeholder="Mood">
            <input id="dayEnergy" type="text" placeholder="Energy">
            <textarea id="dayReflection" placeholder="Reflection"></textarea>
            <textarea id="dayLooseNotes" placeholder="Loose notes"></textarea>
            <button class="secondary" type="submit">save notes</button>
          </form>
        </div>
      </aside>
    </section>
  </main>
  <script>
    const INITIAL_DAY = __INITIAL_CALENDAR_JSON__;
    const state = { day: INITIAL_DAY || null };
    const $ = (id) => document.getElementById(id);

    function escapeHtml(value) {
      return String(value || "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;");
    }

    function tomorrowDate(value) {
      const date = new Date(`${value}T00:00:00`);
      date.setDate(date.getDate() + 1);
      return date.toISOString().slice(0, 10);
    }

    async function fetchJson(path) {
      const response = await fetch(path);
      if (!response.ok) throw new Error(await response.text());
      return await response.json();
    }

    async function postJson(path, payload) {
      const response = await fetch(path, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload || {}),
      });
      if (!response.ok) throw new Error(await response.text());
      return await response.json();
    }

    function setStatus(text) {
      $("statusText").textContent = text || "";
    }

    function entryMarkup(entry) {
      const time = entry.start_time ? `${entry.start_time}${entry.end_time ? `-${entry.end_time}` : ""}` : "anytime";
      const done = entry.status === "done";
      return `
        <article class="entry ${done ? "done" : ""}">
          <div class="entry-title">${escapeHtml(entry.title)}</div>
          <div class="entry-meta">#${entry.id} · ${escapeHtml(time)} · ${escapeHtml(entry.status)} · ${escapeHtml(entry.type)} · ${escapeHtml(entry.priority)}</div>
          ${entry.notes ? `<div class="muted">${escapeHtml(entry.notes)}</div>` : ""}
          <div class="entry-actions">
            <button type="button" data-entry-status="${entry.id}:done" class="secondary">done</button>
            <button type="button" data-entry-status="${entry.id}:missed" class="secondary">missed</button>
            <button type="button" data-entry-status="${entry.id}:deferred" class="secondary">defer</button>
          </div>
        </article>
      `;
    }

    function agendaMarkup(item) {
      return `
        <article class="entry">
          <div class="entry-title">${escapeHtml(item.title || "(untitled)")}</div>
          <div class="entry-meta">${escapeHtml(item.time || item.sort_time || "anytime")} · ${escapeHtml(item.type || "agenda")}</div>
        </article>
      `;
    }

    function renderDay(payload) {
      state.day = payload || {};
      $("calendarDate").value = state.day.date || new Date().toISOString().slice(0, 10);
      $("dayTitle").textContent = state.day.label || state.day.date || "Calendar";
      const stats = state.day.stats || {};
      const note = state.day.day_note || {};
      $("daySubtitle").textContent = `${stats.done_entries || 0} done · ${stats.open_entries || 0} not done · ${stats.agenda_items || 0} agenda`;
      $("dayIntention").value = note.intention || "";
      $("dayMood").value = note.mood || "";
      $("dayEnergy").value = note.energy || "";
      $("dayReflection").value = note.reflection || "";
      $("dayLooseNotes").value = note.notes || "";
      const entries = state.day.entries || [];
      const agenda = state.day.agenda?.items || [];
      const needs = state.day.need_to_get_to || [];
      const latestSave = (state.day.snapshots || [])[0];
      $("dayBody").innerHTML = `
        <section class="stats">
          <div class="stat"><div class="stat-label">tracked</div><div class="stat-value">${stats.tracked_entries || 0}</div></div>
          <div class="stat"><div class="stat-label">done</div><div class="stat-value">${stats.done_entries || 0}</div></div>
          <div class="stat"><div class="stat-label">not done</div><div class="stat-value">${stats.open_entries || 0}</div></div>
          <div class="stat"><div class="stat-label">saves</div><div class="stat-value">${(state.day.snapshots || []).length}</div></div>
        </section>
        ${(note.intention || note.reflection || note.notes || note.mood || note.energy) ? `
          <section class="note-card">
            ${note.intention ? `<div><strong>Intention:</strong> ${escapeHtml(note.intention)}</div>` : ""}
            ${note.reflection ? `<div><strong>Reflection:</strong> ${escapeHtml(note.reflection)}</div>` : ""}
            ${note.notes ? `<div><strong>Notes:</strong> ${escapeHtml(note.notes)}</div>` : ""}
            ${(note.mood || note.energy) ? `<div class="muted">${escapeHtml([note.mood, note.energy].filter(Boolean).join(" · "))}</div>` : ""}
          </section>
        ` : ""}
        <section class="section">
          <h2>Tracked</h2>
          ${entries.length ? entries.map(entryMarkup).join("") : `<div class="empty">No tracked entries for this day yet.</div>`}
        </section>
        <section class="section">
          <h2>Agenda</h2>
          ${agenda.length ? agenda.map(agendaMarkup).join("") : `<div class="empty">Open space.</div>`}
        </section>
        <section class="section">
          <h2>Need To Get To</h2>
          ${needs.length ? needs.map(entryMarkup).join("") : `<div class="empty">Nothing currently outstanding for this day.</div>`}
        </section>
        ${latestSave ? `
          <section class="section">
            <h2>Latest Historic Save</h2>
            <div class="entry">
              <div class="entry-title">${escapeHtml(latestSave.summary || "Saved day")}</div>
              <div class="entry-meta">#${latestSave.id} · ${escapeHtml(latestSave.snapshot_at || "")}</div>
            </div>
          </section>
        ` : ""}
      `;
      for (const button of document.querySelectorAll("[data-entry-status]")) {
        button.addEventListener("click", async () => {
          const [id, status] = String(button.getAttribute("data-entry-status") || "").split(":");
          if (!id || !status) return;
          setStatus("updating...");
          const next = await postJson(`/api/calendar/entries/${id}/status`, { status, date: state.day.date });
          renderDay(next.day);
          setStatus("updated");
        });
      }
    }

    async function loadDay(day) {
      setStatus("loading...");
      const payload = await fetchJson(`/api/calendar/day?date=${encodeURIComponent(day)}`);
      renderDay(payload.day);
      setStatus("loaded");
    }

    $("loadDayButton").addEventListener("click", () => loadDay($("calendarDate").value).catch((error) => setStatus(error.message)));
    $("saveDayButton").addEventListener("click", async () => {
      try {
        setStatus("saving historic day...");
        const payload = await postJson("/api/calendar/day-save", { date: $("calendarDate").value });
        renderDay(payload.day);
        setStatus(`saved snapshot #${payload.snapshot_id}`);
      } catch (error) {
        setStatus(error.message);
      }
    });
    $("rolloverButton").addEventListener("click", async () => {
      try {
        const sourceDate = $("calendarDate").value;
        setStatus("rolling unfinished work...");
        await postJson("/api/calendar/rollover", { source_date: sourceDate, target_date: tomorrowDate(sourceDate) });
        await loadDay(sourceDate);
        setStatus("rolled unfinished work to tomorrow");
      } catch (error) {
        setStatus(error.message);
      }
    });
    $("entryForm").addEventListener("submit", async (event) => {
      event.preventDefault();
      try {
        const title = $("entryTitle").value.trim();
        if (!title) {
          setStatus("add a title first");
          return;
        }
        setStatus("adding...");
        const payload = await postJson("/api/calendar/entries", {
          date: $("calendarDate").value,
          title,
          type: $("entryType").value,
          priority: $("entryPriority").value,
          start_time: $("entryStartTime").value,
          notes: $("entryNotes").value,
        });
        $("entryTitle").value = "";
        $("entryNotes").value = "";
        renderDay(payload.day);
        setStatus("added");
      } catch (error) {
        setStatus(error.message);
      }
    });
    $("noteForm").addEventListener("submit", async (event) => {
      event.preventDefault();
      try {
        setStatus("saving notes...");
        const payload = await postJson("/api/calendar/day-note", {
          date: $("calendarDate").value,
          intention: $("dayIntention").value,
          mood: $("dayMood").value,
          energy: $("dayEnergy").value,
          reflection: $("dayReflection").value,
          notes: $("dayLooseNotes").value,
        });
        renderDay(payload.day);
        setStatus("notes saved");
      } catch (error) {
        setStatus(error.message);
      }
    });
    renderDay(INITIAL_DAY);
  </script>
</body>
</html>
"""


def _render_mail_ui_html(initial_overview: dict[str, Any] | None = None) -> str:
    return (
        _HTML.replace(
            "__MAIL_UI_CLIENT_REFRESH_INTERVAL_MS__",
            str(MAIL_UI_CLIENT_REFRESH_INTERVAL_MS),
        )
        .replace("__INITIAL_OVERVIEW_JSON__", json.dumps(initial_overview or {}))
        .replace("__CMAIL_SIGNATURE_TEXT_JSON__", json.dumps(_CMAIL_SIGNATURE_TEXT))
        .replace("__CMAIL_KNOWN_SIGNATURE_TEXTS_JSON__", json.dumps(list(_CMAIL_KNOWN_SIGNATURE_TEXTS)))
        .replace("__CMAIL_SIGNATURE_PREVIEW_HTML_JSON__", json.dumps(_CMAIL_SIGNATURE_PREVIEW_HTML))
    )


def _render_calendar_ui_html(initial_day: dict[str, Any] | None = None) -> str:
    return _CALENDAR_HTML.replace("__INITIAL_CALENDAR_JSON__", json.dumps(initial_day or {}))


def _background_mail_ui_sync_loop(
    *,
    db_path: Path,
    stop_event: threading.Event,
    interval_seconds: float = MAIL_UI_BACKGROUND_SYNC_INTERVAL_SECONDS,
) -> None:
    while not stop_event.is_set():
        if stop_event.wait(interval_seconds):
            break
        try:
            sync_cloudflare_mail_queue(
                db_path=db_path,
                request_timeout_seconds=MAIL_UI_SYNC_REQUEST_TIMEOUT_SECONDS,
            )
        except Exception:
            # Keep the local inbox responsive even if a background sync hiccups.
            continue


def _make_handler(
    *,
    db_path: Path,
    limit: int,
    enable_background_remote_sync: bool = True,
) -> type[BaseHTTPRequestHandler]:
    snapshot_cache = _MailUiSnapshotCache(db_path=db_path)
    use_live_runtime_reads = not store.encrypted_db_enabled(db_path)
    sync_request_state: dict[str, Any] = {"thread": None}
    sync_request_lock = threading.Lock()
    heartbeat_request_state: dict[str, Any] = {"thread": None}
    heartbeat_request_lock = threading.Lock()
    runtime_sync_state: dict[str, Any] = {}
    runtime_sync_lock = threading.Lock()
    overview_cache_state: dict[str, Any] = {"default_payload": None}
    overview_cache_lock = threading.Lock()
    drafts_cache_state: dict[str, Any] = {"drafts": None}
    drafts_cache_lock = threading.Lock()

    def _run_startup_mailbox_maintenance() -> None:
        try:
            cleanup_result = cleanup_cmail_correspondence_artifacts(db_path=db_path)
        except Exception:
            cleanup_result = {"orphaned_resend_ids": [], "superseded_draft_ids": [], "restored_draft_ids": []}
        cleaned_ids = cleanup_result.get("orphaned_resend_ids") or []
        superseded_draft_ids = cleanup_result.get("superseded_draft_ids") or []
        restored_draft_ids = cleanup_result.get("restored_draft_ids") or []
        if cleaned_ids or superseded_draft_ids or restored_draft_ids:
            snapshot_cache.invalidate()
            with overview_cache_lock:
                overview_cache_state["default_payload"] = None

    def _is_default_overview_request(
        *,
        source: str | None,
        channel: str | None,
        direction: str | None,
        status: str | None,
        page_limit: int,
        include_details: bool,
    ) -> bool:
        return (
            (source or DEFAULT_MAIL_UI_CORRESPONDENCE_SOURCE) == DEFAULT_MAIL_UI_CORRESPONDENCE_SOURCE
            and (channel or "email") == "email"
            and not (direction or "").strip()
            and (status or "all") == "all"
            and int(page_limit) == int(limit)
            and not include_details
        )

    def _build_default_overview_payload(connection: sqlite3.Connection) -> dict[str, Any]:
        return {
            "db_path": str(db_path),
            **_build_correspondence_overview_from_connection(
                connection,
                limit=limit,
                include_details=False,
            ),
        }

    def _set_default_overview_cache(payload: dict[str, Any] | None) -> dict[str, Any]:
        cloned = _clone_json_payload(payload)
        with overview_cache_lock:
            overview_cache_state["default_payload"] = cloned
        return _clone_json_payload(cloned)

    def _get_default_overview_cache() -> dict[str, Any] | None:
        if use_live_runtime_reads:
            return None
        with overview_cache_lock:
            payload = overview_cache_state.get("default_payload")
        if not isinstance(payload, dict) or not payload:
            return None
        return _clone_json_payload(payload)

    def _refresh_default_overview_cache() -> dict[str, Any]:
        connection = snapshot_cache.get_connection()
        try:
            payload = _build_default_overview_payload(connection)
        finally:
            connection.close()
        return _set_default_overview_cache(payload)

    def _refresh_default_overview_cache_async() -> bool:
        def _run_refresh() -> None:
            try:
                _refresh_default_overview_cache()
            except Exception:
                return

        next_thread = threading.Thread(
            target=_run_refresh,
            daemon=True,
            name="life-ops-mail-ui-overview-cache-refresh",
        )
        next_thread.start()
        return True

    def _set_drafts_cache(drafts: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
        normalized = _sorted_drafts(drafts or [])
        with drafts_cache_lock:
            drafts_cache_state["drafts"] = normalized
        return _sorted_drafts(normalized)

    def _get_drafts_cache() -> list[dict[str, Any]] | None:
        with drafts_cache_lock:
            drafts = drafts_cache_state.get("drafts")
        if not isinstance(drafts, list):
            return None
        return _sorted_drafts(drafts)

    def _refresh_drafts_cache() -> list[dict[str, Any]]:
        with store.open_db(db_path) as connection:
            drafts = _list_cmail_drafts(connection)
        return _set_drafts_cache(drafts)

    def _kick_mail_sync() -> bool:
        if not enable_background_remote_sync:
            return False
        with sync_request_lock:
            active_thread = sync_request_state.get("thread")
            if isinstance(active_thread, threading.Thread) and active_thread.is_alive():
                return False

            def _run_sync() -> None:
                try:
                    sync_cloudflare_mail_queue(
                        db_path=db_path,
                        request_timeout_seconds=MAIL_UI_SYNC_REQUEST_TIMEOUT_SECONDS,
                    )
                    completed_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
                    with runtime_sync_lock:
                        runtime_sync_state["cloudflare_sync"] = {
                            **(runtime_sync_state.get("cloudflare_sync") or {}),
                            "status": "healthy",
                            "last_sync_at": completed_at,
                            "last_success_at": completed_at,
                        }
                except Exception:
                    failed_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
                    with runtime_sync_lock:
                        runtime_sync_state["cloudflare_sync"] = {
                            **(runtime_sync_state.get("cloudflare_sync") or {}),
                            "status": "degraded",
                            "last_failure_at": failed_at,
                        }
                    return
                finally:
                    snapshot_cache.invalidate()
                    _refresh_default_overview_cache_async()

            next_thread = threading.Thread(target=_run_sync, daemon=True, name="life-ops-mail-ui-sync")
            sync_request_state["thread"] = next_thread
            next_thread.start()
            return True

    def _kick_mail_heartbeat() -> bool:
        if not enable_background_remote_sync:
            return False
        with heartbeat_request_lock:
            active_thread = heartbeat_request_state.get("thread")
            if isinstance(active_thread, threading.Thread) and active_thread.is_alive():
                return False

            def _run_heartbeat() -> None:
                result = _record_cloudflare_queue_heartbeat(db_path=db_path)
                heartbeat_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
                with runtime_sync_lock:
                    if result is None:
                        runtime_sync_state["cloudflare_sync"] = {
                            **(runtime_sync_state.get("cloudflare_sync") or {}),
                            "status": "degraded",
                            "last_failure_at": heartbeat_at,
                        }
                        return
                    runtime_sync_state["cloudflare_queue"] = {
                        "pending_count": int(result.get("pending_count") or 0),
                        "total_stored": int(result.get("total_stored") or 0),
                        "total_acknowledged": int(result.get("total_acknowledged") or 0),
                        "forwarding_enabled": bool(result.get("forwarding_enabled")),
                        "archive_encryption_enabled": bool(result.get("archive_encryption_enabled")),
                        "source": "runtime_heartbeat",
                    }
                    runtime_sync_state["cloudflare_sync"] = {
                        **(runtime_sync_state.get("cloudflare_sync") or {}),
                        "status": "healthy",
                        "last_sync_at": str(result.get("heartbeat_at") or heartbeat_at),
                    }
                if int(result.get("pending_count") or 0) > 0:
                    _kick_mail_sync()

            next_thread = threading.Thread(target=_run_heartbeat, daemon=True, name="life-ops-mail-ui-heartbeat")
            heartbeat_request_state["thread"] = next_thread
            next_thread.start()
        return True

    threading.Thread(
        target=_run_startup_mailbox_maintenance,
        daemon=True,
        name="life-ops-mail-ui-startup-maintenance",
    ).start()

    class MailUIHandler(BaseHTTPRequestHandler):
        def _send_json(self, payload: dict[str, Any], status_code: int = 200) -> None:
            body = _json_bytes(payload)
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _send_html(self, body_text: str) -> None:
            body = body_text.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _send_bytes(self, body: bytes, *, content_type: str, content_disposition: str | None = None) -> None:
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            if content_disposition:
                self.send_header("Content-Disposition", content_disposition)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _read_json_body(self) -> dict[str, Any]:
            raw_length = self.headers.get("Content-Length", "0")
            try:
                length = int(raw_length)
            except ValueError:
                length = 0
            body = self.rfile.read(max(0, length)) if length else b""
            if not body:
                return {}
            payload = json.loads(body.decode("utf-8"))
            return payload if isinstance(payload, dict) else {}

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = parse_qs(parsed.query)

            if path.startswith("/static/"):
                try:
                    asset_path = _resolve_mail_ui_static_asset(path.removeprefix("/static/"))
                except ValueError:
                    self.send_error(400, "Invalid static asset path.")
                    return
                if not asset_path.exists() or not asset_path.is_file():
                    self.send_error(404, "Static asset not found.")
                    return
                content_type, _ = mimetypes.guess_type(str(asset_path))
                self._send_bytes(
                    asset_path.read_bytes(),
                    content_type=content_type or "application/octet-stream",
                )
                return

            if path == "/":
                initial_overview = _get_default_overview_cache()
                if initial_overview is None:
                    try:
                        if use_live_runtime_reads:
                            connection = snapshot_cache.get_connection()
                            try:
                                initial_overview = _build_default_overview_payload(connection)
                            finally:
                                connection.close()
                        else:
                            initial_overview = _refresh_default_overview_cache()
                    except TimeoutError:
                        initial_overview = {}
                self._send_html(_render_mail_ui_html(initial_overview=initial_overview))
                return

            if path == "/calendar":
                raw_day = str((query.get("date") or [""])[0] or "").strip()
                try:
                    target_day = date.fromisoformat(raw_day) if raw_day else date.today()
                    connection = snapshot_cache.get_connection()
                    try:
                        initial_day = build_calendar_day(connection, target_day=target_day)
                    finally:
                        connection.close()
                except (TimeoutError, ValueError):
                    initial_day = {}
                self._send_html(_render_calendar_ui_html(initial_day=initial_day))
                return

            if path == "/api/health":
                self._send_json({"ok": True, "db_path": str(db_path)})
                return

            if path == "/api/calendar/day":
                raw_day = str((query.get("date") or [""])[0] or "").strip()
                try:
                    target_day = date.fromisoformat(raw_day) if raw_day else date.today()
                    connection = snapshot_cache.get_connection()
                    try:
                        day_payload = build_calendar_day(connection, target_day=target_day)
                    finally:
                        connection.close()
                except ValueError:
                    self._send_json({"error": "invalid_calendar_date"}, status_code=400)
                    return
                except TimeoutError as exc:
                    self._send_json({"error": str(exc)}, status_code=503)
                    return
                self._send_json({"day": day_payload})
                return

            if path == "/api/contacts":
                query_text = str((query.get("query") or query.get("q") or [""])[0] or "").strip()
                try:
                    contact_limit = int((query.get("limit") or [str(DEFAULT_MAIL_UI_CONTACT_LIMIT)])[0] or DEFAULT_MAIL_UI_CONTACT_LIMIT)
                except ValueError:
                    contact_limit = DEFAULT_MAIL_UI_CONTACT_LIMIT
                try:
                    with store.open_db(db_path) as connection:
                        contacts = _list_mail_contacts(
                            connection,
                            query=query_text,
                            limit=max(1, contact_limit),
                        )
                except TimeoutError as exc:
                    self._send_json({"error": str(exc)}, status_code=503)
                    return
                self._send_json({"contacts": contacts, "query": query_text})
                return

            if path == "/api/drafts":
                try:
                    payload = {"drafts": _refresh_drafts_cache()}
                except TimeoutError as exc:
                    self._send_json({"error": str(exc)}, status_code=503)
                    return
                self._send_json(payload)
                return

            if path.startswith("/api/drafts/") and path.endswith("/send"):
                self._send_json({"error": "method_not_allowed"}, status_code=405)
                return

            if path.startswith("/api/drafts/") and (
                path.endswith("/attachments") or path.endswith("/attachments/delete")
            ):
                self._send_json({"error": "method_not_allowed"}, status_code=405)
                return

            if path == "/api/sync-status":
                direction = (query.get("direction") or [""])[0] or None
                source = (
                    (query.get("source") or [""])[0]
                    or (DEFAULT_MAIL_UI_SOURCE if direction else DEFAULT_MAIL_UI_CORRESPONDENCE_SOURCE)
                )
                channel = (query.get("channel") or ["email"])[0] or "email"
                status = (query.get("status") or ["all"])[0] or "all"
                should_sync = ((query.get("sync") or ["0"])[0] or "0").strip().lower() in {"1", "true", "yes"}
                is_default_request = _is_default_overview_request(
                    source=source,
                    channel=channel,
                    direction=direction,
                    status=status,
                    page_limit=limit,
                    include_details=False,
                )
                try:
                    triggered = False
                    if should_sync:
                        triggered = _kick_mail_heartbeat()
                    cached_overview = None if use_live_runtime_reads else (_get_default_overview_cache() if is_default_request else None)
                    if cached_overview is not None:
                        stored_queue = dict(cached_overview.get("cloudflare_queue") or {})
                        stored_sync = dict(cached_overview.get("cloudflare_sync") or {})
                        mailbox_version = dict(cached_overview.get("mailbox_version") or {})
                    else:
                        connection = snapshot_cache.get_connection()
                        try:
                            stored_queue = _cloudflare_queue_status_from_connection(connection)
                            stored_sync = _cloudflare_sync_status_from_connection(connection)
                            mailbox_version = (
                                _correspondence_mailbox_version_from_connection(connection)
                                if (source or "").strip().lower() == DEFAULT_MAIL_UI_CORRESPONDENCE_SOURCE
                                else _mailbox_version_from_connection(
                                    connection,
                                    source=source,
                                    channel=channel,
                                    direction=direction,
                                    status=status,
                                )
                            )
                        finally:
                            connection.close()
                    with runtime_sync_lock:
                        runtime_queue = dict(runtime_sync_state.get("cloudflare_queue") or {})
                        runtime_sync = dict(runtime_sync_state.get("cloudflare_sync") or {})
                    payload = {
                        "cloudflare_queue": {
                            **stored_queue,
                            **runtime_queue,
                        },
                        "cloudflare_sync": {
                            **stored_sync,
                            **runtime_sync,
                        },
                        "mailbox_version": mailbox_version,
                        "sync_requested": should_sync,
                        "sync_triggered": triggered,
                    }
                except TimeoutError as exc:
                    self._send_json({"error": str(exc)}, status_code=503)
                    return
                self._send_json(payload)
                return

            if path == "/api/overview" or path == "/api/communications":
                direction = (query.get("direction") or [""])[0] or None
                source = (
                    (query.get("source") or [""])[0]
                    or (DEFAULT_MAIL_UI_SOURCE if direction else DEFAULT_MAIL_UI_CORRESPONDENCE_SOURCE)
                )
                channel = (query.get("channel") or ["email"])[0] or "email"
                status = (query.get("status") or ["all"])[0] or "all"
                should_sync = ((query.get("sync") or ["0"])[0] or "0").strip().lower() in {"1", "true", "yes"}
                include_details = ((query.get("include_details") or ["0"])[0] or "0").strip().lower() in {"1", "true", "yes"}
                page_limit = int((query.get("limit") or [str(limit)])[0] or limit)
                is_default_request = _is_default_overview_request(
                    source=source,
                    channel=channel,
                    direction=direction,
                    status=status,
                    page_limit=page_limit,
                    include_details=include_details,
                )
                try:
                    if should_sync:
                        sync_cloudflare_mail_queue(db_path=db_path)
                        snapshot_cache.invalidate()
                        if is_default_request:
                            payload = _refresh_default_overview_cache()
                        else:
                            connection = snapshot_cache.get_connection()
                            try:
                                payload = (
                                    {
                                        "db_path": str(db_path),
                                        **_build_correspondence_overview_from_connection(
                                            connection,
                                            limit=page_limit,
                                            include_details=include_details,
                                        ),
                                    }
                                    if (source or "").strip().lower() == DEFAULT_MAIL_UI_CORRESPONDENCE_SOURCE
                                    else {
                                        "db_path": str(db_path),
                                        **_build_mail_ui_overview_from_connection(
                                            connection,
                                            source=source,
                                            channel=channel,
                                            direction=direction,
                                            status=status,
                                            limit=page_limit,
                                            include_details=include_details,
                                        ),
                                    }
                                )
                            finally:
                                connection.close()
                    elif is_default_request:
                        if use_live_runtime_reads:
                            connection = snapshot_cache.get_connection()
                            try:
                                payload = _build_default_overview_payload(connection)
                            finally:
                                connection.close()
                        else:
                            payload = _get_default_overview_cache()
                            if payload is None:
                                payload = _refresh_default_overview_cache()
                    else:
                        connection = snapshot_cache.get_connection()
                        try:
                            payload = (
                                {
                                    "db_path": str(db_path),
                                    **_build_correspondence_overview_from_connection(
                                        connection,
                                        limit=page_limit,
                                        include_details=include_details,
                                    ),
                                }
                                if (source or "").strip().lower() == DEFAULT_MAIL_UI_CORRESPONDENCE_SOURCE
                                else {
                                    "db_path": str(db_path),
                                    **_build_mail_ui_overview_from_connection(
                                        connection,
                                        source=source,
                                        channel=channel,
                                        direction=direction,
                                        status=status,
                                        limit=page_limit,
                                        include_details=include_details,
                                    ),
                                }
                            )
                        finally:
                            connection.close()
                except TimeoutError as exc:
                    self._send_json({"error": str(exc)}, status_code=503)
                    return
                self._send_json(payload)
                return

            if path.startswith("/api/communications/"):
                raw_id = path.split("/")[-1]
                try:
                    communication_id = int(raw_id)
                except ValueError:
                    self._send_json({"error": "invalid_communication_id"}, status_code=400)
                    return
                try:
                    connection = snapshot_cache.get_connection()
                    try:
                        payload = _communication_detail(connection, communication_id)
                    finally:
                        connection.close()
                except KeyError as exc:
                    self._send_json({"error": str(exc)}, status_code=404)
                    return
                except TimeoutError as exc:
                    self._send_json({"error": str(exc)}, status_code=503)
                    return
                self._send_json(payload)
                return

            if path.startswith("/api/attachments/") and path.endswith("/content"):
                parts = [part for part in path.split("/") if part]
                if len(parts) != 4:
                    self._send_json({"error": "invalid_attachment_path"}, status_code=400)
                    return
                try:
                    attachment_id = int(parts[2])
                except ValueError:
                    self._send_json({"error": "invalid_attachment_id"}, status_code=400)
                    return
                try:
                    connection = snapshot_cache.get_connection()
                    try:
                        raw_bytes, mime_type = _read_attachment_content(connection, attachment_id)
                        row = connection.execute(
                            "SELECT filename FROM communication_attachments WHERE id = ?",
                            (attachment_id,),
                        ).fetchone()
                    finally:
                        connection.close()
                except KeyError as exc:
                    self._send_json({"error": str(exc)}, status_code=404)
                    return
                except TimeoutError as exc:
                    self._send_json({"error": str(exc)}, status_code=503)
                    return
                self._send_bytes(
                    raw_bytes,
                    content_type=mime_type,
                    content_disposition=_attachment_download_disposition(
                        str(row["filename"] or "") if row is not None else "attachment.bin",
                        mime_type,
                    ),
                )
                return

            self._send_json({"error": "not_found"}, status_code=404)

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"

            try:
                payload = self._read_json_body()
            except json.JSONDecodeError:
                self._send_json({"error": "invalid_json"}, status_code=400)
                return

            if path == "/api/calendar/entries":
                raw_day = str(payload.get("date") or "").strip()
                title = str(payload.get("title") or "").strip()
                if not title:
                    self._send_json({"error": "missing_title"}, status_code=400)
                    return
                try:
                    target_day = date.fromisoformat(raw_day) if raw_day else date.today()
                    raw_tags = payload.get("tags") or []
                    if isinstance(raw_tags, str):
                        tags = [raw_tags]
                    elif isinstance(raw_tags, list):
                        tags = raw_tags
                    else:
                        tags = []
                    with store.open_db(db_path) as connection:
                        store.add_calendar_entry(
                            connection,
                            entry_date=target_day,
                            title=title,
                            entry_type=str(payload.get("type") or payload.get("entry_type") or "task"),
                            status=str(payload.get("status") or "planned"),
                            priority=str(payload.get("priority") or "normal"),
                            list_name=str(payload.get("list_name") or "personal"),
                            start_time=str(payload.get("start_time") or ""),
                            end_time=str(payload.get("end_time") or ""),
                            notes=str(payload.get("notes") or ""),
                            tags=tags,
                        )
                        day_payload = build_calendar_day(connection, target_day=target_day)
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, status_code=400)
                    return
                snapshot_cache.invalidate()
                self._send_json({"ok": True, "day": day_payload})
                return

            if path.startswith("/api/calendar/entries/") and path.endswith("/status"):
                parts = [part for part in path.split("/") if part]
                if len(parts) != 5:
                    self._send_json({"error": "invalid_calendar_status_path"}, status_code=400)
                    return
                try:
                    entry_id = int(parts[3])
                    raw_day = str(payload.get("date") or "").strip()
                    status = str(payload.get("status") or "").strip()
                    target_day = date.fromisoformat(raw_day) if raw_day else date.today()
                    with store.open_db(db_path) as connection:
                        store.set_calendar_entry_status(connection, entry_id=entry_id, status=status)
                        row = store.get_calendar_entry(connection, entry_id)
                        if row is not None:
                            target_day = date.fromisoformat(str(row["entry_date"]))
                        day_payload = build_calendar_day(connection, target_day=target_day)
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, status_code=400)
                    return
                snapshot_cache.invalidate()
                self._send_json({"ok": True, "day": day_payload})
                return

            if path == "/api/calendar/day-note":
                raw_day = str(payload.get("date") or "").strip()
                try:
                    target_day = date.fromisoformat(raw_day) if raw_day else date.today()
                    with store.open_db(db_path) as connection:
                        store.update_calendar_day_note(
                            connection,
                            day=target_day,
                            intention=str(payload.get("intention") or ""),
                            reflection=str(payload.get("reflection") or ""),
                            notes=str(payload.get("notes") or ""),
                            mood=str(payload.get("mood") or ""),
                            energy=str(payload.get("energy") or ""),
                        )
                        day_payload = build_calendar_day(connection, target_day=target_day)
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, status_code=400)
                    return
                snapshot_cache.invalidate()
                self._send_json({"ok": True, "day": day_payload})
                return

            if path == "/api/calendar/day-save":
                raw_day = str(payload.get("date") or "").strip()
                try:
                    target_day = date.fromisoformat(raw_day) if raw_day else date.today()
                    with store.open_db(db_path) as connection:
                        result = save_calendar_day(
                            connection,
                            target_day=target_day,
                            title=str(payload.get("title") or ""),
                            summary=str(payload.get("summary") or ""),
                        )
                        day_payload = build_calendar_day(connection, target_day=target_day)
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, status_code=400)
                    return
                snapshot_cache.invalidate()
                self._send_json(
                    {
                        "ok": True,
                        "snapshot_id": result["snapshot_id"],
                        "summary": result["summary"],
                        "day": day_payload,
                    }
                )
                return

            if path == "/api/calendar/rollover":
                raw_source_day = str(payload.get("source_date") or payload.get("from") or "").strip()
                raw_target_day = str(payload.get("target_date") or payload.get("to") or "").strip()
                try:
                    source_day = date.fromisoformat(raw_source_day) if raw_source_day else date.today()
                    target_day = date.fromisoformat(raw_target_day) if raw_target_day else source_day + timedelta(days=1)
                    with store.open_db(db_path) as connection:
                        result = rollover_calendar_day(
                            connection,
                            source_day=source_day,
                            target_day=target_day,
                        )
                        day_payload = build_calendar_day(connection, target_day=source_day)
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, status_code=400)
                    return
                snapshot_cache.invalidate()
                self._send_json({"ok": True, **result, "day": day_payload})
                return

            if path.startswith("/api/communications/") and path.endswith("/delete"):
                parts = [part for part in path.split("/") if part]
                if len(parts) != 4:
                    self._send_json({"error": "invalid_delete_path"}, status_code=400)
                    return
                try:
                    communication_id = int(parts[2])
                except ValueError:
                    self._send_json({"error": "invalid_communication_id"}, status_code=400)
                    return
                with store.open_db(db_path) as connection:
                    deleted = store.set_communication_status(
                        connection,
                        communication_id=communication_id,
                        status="deleted",
                    )
                snapshot_cache.invalidate()
                _set_default_overview_cache(
                    _remove_message_from_overview_payload(
                        _get_default_overview_cache(),
                        communication_id,
                    )
                )
                self._send_json(
                    {
                        "ok": True,
                        "deleted": bool(deleted),
                        "communication_id": communication_id,
                        "archived_for_days": store.DELETED_COMMUNICATION_RETENTION_DAYS,
                        "purge_scheduled": True,
                        "purged_deleted_count": 0,
                    }
                )
                return

            if path == "/api/contacts/delete":
                contact_key = str(payload.get("contact_key") or "").strip()
                if not contact_key:
                    self._send_json({"error": "missing_contact_key"}, status_code=400)
                    return
                with store.open_db(db_path) as connection:
                    communication_ids = _communication_ids_for_contact(connection, contact_key=contact_key)
                    deleted_count = store.set_communications_status(
                        connection,
                        communication_ids=communication_ids,
                        status="deleted",
                    )
                snapshot_cache.invalidate()
                _set_default_overview_cache(
                    _remove_contact_from_overview_payload(
                        _get_default_overview_cache(),
                        contact_key,
                    )
                )
                self._send_json(
                    {
                        "ok": True,
                        "contact_key": contact_key,
                        "deleted_count": deleted_count,
                        "archived_for_days": store.DELETED_COMMUNICATION_RETENTION_DAYS,
                        "purge_scheduled": True,
                        "purged_deleted_count": 0,
                    }
                )
                return

            if path == "/api/drafts":
                try:
                    with store.open_db(db_path) as connection:
                        draft = _save_cmail_draft(connection, payload)
                    cached_drafts = _get_drafts_cache() or []
                    next_drafts = [entry for entry in cached_drafts if int(entry.get("id") or 0) != int(draft.get("id") or 0)]
                    next_drafts.append(draft)
                    _set_drafts_cache(next_drafts)
                except KeyError as exc:
                    self._send_json({"error": str(exc)}, status_code=404)
                    return
                self._send_json({"draft": draft})
                return

            if path.startswith("/api/drafts/") and path.endswith("/send"):
                parts = [part for part in path.split("/") if part]
                if len(parts) != 4:
                    self._send_json({"error": "invalid_draft_send_path"}, status_code=400)
                    return
                try:
                    draft_id = int(parts[2])
                except ValueError:
                    self._send_json({"error": "invalid_draft_id"}, status_code=400)
                    return
                try:
                    result = send_cmail_draft(
                        db_path=db_path,
                        draft_id=draft_id,
                    )
                    cached_drafts = _get_drafts_cache() or []
                    _set_drafts_cache(
                        [entry for entry in cached_drafts if int(entry.get("id") or 0) != int(draft_id)]
                    )
                except KeyError as exc:
                    self._send_json({"error": str(exc)}, status_code=404)
                    return
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, status_code=409)
                    return
                except Exception as exc:
                    self._send_json({"error": str(exc)}, status_code=500)
                    return
                self._send_json(result)
                return

            if path.startswith("/api/drafts/") and path.endswith("/attachments"):
                parts = [part for part in path.split("/") if part]
                if len(parts) != 4:
                    self._send_json({"error": "invalid_draft_attachment_path"}, status_code=400)
                    return
                try:
                    draft_id = int(parts[2])
                except ValueError:
                    self._send_json({"error": "invalid_draft_id"}, status_code=400)
                    return
                uploads = payload.get("attachments") or []
                if not isinstance(uploads, list):
                    self._send_json({"error": "attachments_must_be_a_list"}, status_code=400)
                    return
                try:
                    with store.open_db(db_path) as connection:
                        row = store.get_communication_by_id(connection, draft_id)
                        if row is None or str(row["source"] or "") != "cmail_draft":
                            raise KeyError(f"draft {draft_id} not found")
                        _add_draft_attachments(connection, draft_id=draft_id, uploads=uploads)
                        refreshed = store.get_communication_by_id(connection, draft_id)
                        if refreshed is None:
                            raise KeyError(f"draft {draft_id} not found")
                        draft = _draft_summary(connection, refreshed)
                    cached_drafts = _get_drafts_cache() or []
                    next_drafts = [entry for entry in cached_drafts if int(entry.get("id") or 0) != int(draft.get("id") or 0)]
                    next_drafts.append(draft)
                    _set_drafts_cache(next_drafts)
                except KeyError as exc:
                    self._send_json({"error": str(exc)}, status_code=404)
                    return
                except ValueError as exc:
                    self._send_json({"error": str(exc)}, status_code=400)
                    return
                self._send_json({"draft": draft})
                return

            if path.startswith("/api/drafts/") and path.endswith("/attachments/delete"):
                parts = [part for part in path.split("/") if part]
                if len(parts) != 5:
                    self._send_json({"error": "invalid_draft_attachment_delete_path"}, status_code=400)
                    return
                try:
                    draft_id = int(parts[2])
                    attachment_id = int(payload.get("attachment_id") or 0)
                except ValueError:
                    self._send_json({"error": "invalid_attachment_id"}, status_code=400)
                    return
                if not attachment_id:
                    self._send_json({"error": "missing_attachment_id"}, status_code=400)
                    return
                try:
                    with store.open_db(db_path) as connection:
                        _delete_draft_attachment(connection, draft_id=draft_id, attachment_id=attachment_id)
                        refreshed = store.get_communication_by_id(connection, draft_id)
                        if refreshed is None:
                            raise KeyError(f"draft {draft_id} not found")
                        draft = _draft_summary(connection, refreshed)
                    cached_drafts = _get_drafts_cache() or []
                    next_drafts = [entry for entry in cached_drafts if int(entry.get("id") or 0) != int(draft.get("id") or 0)]
                    next_drafts.append(draft)
                    _set_drafts_cache(next_drafts)
                except KeyError as exc:
                    self._send_json({"error": str(exc)}, status_code=404)
                    return
                self._send_json({"draft": draft})
                return

            self._send_json({"error": "not_found"}, status_code=404)

        def log_message(self, format: str, *args: Any) -> None:  # pragma: no cover
            return

    MailUIHandler._snapshot_cache = snapshot_cache  # type: ignore[attr-defined]
    MailUIHandler._prime_overview_cache = _refresh_default_overview_cache  # type: ignore[attr-defined]
    MailUIHandler._prime_drafts_cache = _refresh_drafts_cache  # type: ignore[attr-defined]
    return MailUIHandler


def serve_mail_ui(
    *,
    db_path: Path,
    host: str = DEFAULT_MAIL_UI_HOST,
    port: int = DEFAULT_MAIL_UI_PORT,
    limit: int = DEFAULT_MAIL_UI_LIMIT,
    enable_background_remote_sync: bool = True,
) -> None:
    print(f"[mail_ui] preparing handler for http://{host}:{port}", flush=True)
    handler = _make_handler(
        db_path=db_path,
        limit=limit,
        enable_background_remote_sync=enable_background_remote_sync,
    )
    snapshot_cache = getattr(handler, "_snapshot_cache", None)
    prime_overview_cache = getattr(handler, "_prime_overview_cache", None)
    prime_drafts_cache = getattr(handler, "_prime_drafts_cache", None)
    print("[mail_ui] binding HTTP server", flush=True)
    server = ThreadingHTTPServer((host, port), handler)
    print("[mail_ui] HTTP server bound", flush=True)
    def _warm_caches_after_bind() -> None:
        if snapshot_cache is not None:
            try:
                warm_connection = snapshot_cache.get_connection()
            finally:
                if "warm_connection" in locals():
                    warm_connection.close()
        if callable(prime_overview_cache):
            try:
                prime_overview_cache()
            except Exception:
                pass
        if callable(prime_drafts_cache):
            try:
                prime_drafts_cache()
            except Exception:
                pass

    threading.Thread(
        target=_warm_caches_after_bind,
        name="life-ops-mail-ui-warm",
        daemon=True,
    ).start()
    print("[mail_ui] cache warm thread started", flush=True)
    try:
        print("[mail_ui] serve_forever entering", flush=True)
        server.serve_forever()
    finally:  # pragma: no cover
        server.server_close()
