from __future__ import annotations

import base64
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
import urllib.request
import urllib.error
from datetime import datetime
from http.server import HTTPServer
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops import mail_ui
from life_ops import mail_vault
from life_ops import store
from life_ops import vault_crypto

PNG_BYTES = bytes.fromhex(
    "89504e470d0a1a0a"
    "0000000d4948445200000001000000010804000000b51c0c02"
    "0000000b4944415478da63fcff1f0003030200ef97d9ae"
    "0000000049454e44ae426082"
)


class MailUiTests(unittest.TestCase):
    def test_strip_trailing_cmail_signature_handles_legacy_blank_line_variant(self) -> None:
        legacy_signature = (
            "Hello there.\n\n"
            "Best,\n\n"
            "Cody Mitchell\n"
            "Fractal Research Group\n"
            "https://frg.earth\n"
            "cody@frg.earth"
        )
        self.assertEqual("Hello there.", mail_ui._strip_trailing_cmail_signature(legacy_signature))
        duplicated = f"{legacy_signature}\n\n{mail_ui._CMAIL_SIGNATURE_TEXT}"
        self.assertEqual("Hello there.", mail_ui._strip_trailing_cmail_signature(duplicated))

    def test_strip_trailing_cmail_signature_handles_previous_full_signature(self) -> None:
        old_full_signature = (
            "Hello there.\n\n"
            "Best,\n\n"
            "Cody Mitchell\n"
            "Fractal Research Group LLC\n"
            "https://frg.earth\n"
            "https://www.npmjs.com/~sproutseeds\n"
            "https://github.com/SproutSeeds\n"
            "cody@frg.earth"
        )

        self.assertEqual("Hello there.", mail_ui._strip_trailing_cmail_signature(old_full_signature))
        self.assertEqual(
            "Hello there.\n\n" + mail_ui._CMAIL_SIGNATURE_TEXT,
            mail_ui._compose_cmail_body_text(old_full_signature),
        )

    def test_strip_trailing_cmail_signature_handles_previous_contact_details_signature(self) -> None:
        previous_contact_details_signature = (
            "Hello there.\n\n"
            "Best,\n\n"
            "Cody Mitchell\n"
            "Fractal Research Group LLC\n"
            "https://frg.earth\n"
            "https://panhandlefoodtrucks.com\n"
            "cody@frg.earth\n"
            "Call: (448) 432-0112\n"
            "Text: (850) 529-8452"
        )

        self.assertEqual(
            "Hello there.",
            mail_ui._strip_trailing_cmail_signature(previous_contact_details_signature),
        )
        self.assertEqual(
            "Hello there.\n\n" + mail_ui._CMAIL_SIGNATURE_TEXT,
            mail_ui._compose_cmail_body_text(previous_contact_details_signature),
        )

    def test_strip_trailing_cmail_signature_handles_previous_website_only_signature(self) -> None:
        previous_website_only_signature = (
            "Hello there.\n\n"
            "Best,\n\n"
            "Cody Mitchell\n"
            "Fractal Research Group LLC\n"
            "https://frg.earth"
        )

        self.assertEqual(
            "Hello there.",
            mail_ui._strip_trailing_cmail_signature(previous_website_only_signature),
        )
        self.assertEqual(
            "Hello there.\n\n" + mail_ui._CMAIL_SIGNATURE_TEXT,
            mail_ui._compose_cmail_body_text(previous_website_only_signature),
        )

    def test_strip_trailing_cmail_signature_handles_previous_business_phone_signature(self) -> None:
        old_business_phone_signature = (
            "Hello there.\n\n"
            "Best,\n\n"
            "Cody Mitchell\n"
            "Fractal Research Group LLC\n"
            "https://frg.earth\n"
            "https://www.npmjs.com/~sproutseeds\n"
            "https://github.com/SproutSeeds\n"
            "cody@frg.earth\n"
            "(448) 432-0112"
        )

        self.assertEqual("Hello there.", mail_ui._strip_trailing_cmail_signature(old_business_phone_signature))
        self.assertEqual(
            "Hello there.\n\n" + mail_ui._CMAIL_SIGNATURE_TEXT,
            mail_ui._compose_cmail_body_text(old_business_phone_signature),
        )

    def test_compose_cmail_html_body_uses_email_safe_colors(self) -> None:
        html = mail_ui._compose_cmail_html_body("Hello there.")
        preview = mail_ui._CMAIL_SIGNATURE_PREVIEW_HTML

        self.assertIn("color:#111111", html)
        self.assertIn('href="https://frg.earth"', html)
        self.assertIn('href="https://frg.earth"', preview)
        self.assertIn('href="tel:+18505298452"', html)
        self.assertIn('href="tel:+18505298452"', preview)
        self.assertIn("(850) 529-8452", html)
        self.assertIn("(850) 529-8452", preview)
        self.assertNotIn("https://panhandlefoodtrucks.com", html)
        self.assertNotIn("https://panhandlefoodtrucks.com", preview)
        self.assertNotIn("mailto:cody@frg.earth", html)
        self.assertNotIn("mailto:cody@frg.earth", preview)
        self.assertNotIn("https://www.npmjs.com/~sproutseeds", html)
        self.assertNotIn("https://github.com/SproutSeeds", html)
        self.assertNotIn("https://www.npmjs.com/~sproutseeds", preview)
        self.assertNotIn("https://github.com/SproutSeeds", preview)
        self.assertNotIn("Call:", html)
        self.assertNotIn("Call:", preview)
        self.assertNotIn("Text:", html)
        self.assertNotIn("Text:", preview)
        self.assertNotIn("sms:", html)
        self.assertNotIn("color:#edf2eb", html)

    def test_bind_my_soul_signature_contains_only_public_brand_details(self) -> None:
        text = mail_ui._compose_cmail_body_text("Thanks for reaching out.", "bind_my_soul")
        html = mail_ui._compose_cmail_html_body("Thanks for reaching out.", "bind_my_soul")

        self.assertIn("Bind My Soul Support", text)
        self.assertIn("https://bindmysoul.com", text)
        self.assertIn("Bind My Soul Support", html)
        self.assertNotIn("Cody", text + html)
        self.assertNotIn("Fractal Research", text + html)
        self.assertNotIn("cody@frg.earth", text + html)
        self.assertNotIn("850", text + html)

    def test_codacli_signature_uses_codacli_identity(self) -> None:
        text = mail_ui._compose_cmail_body_text("Thanks for the note.", "codacli")
        html = mail_ui._compose_cmail_html_body("Thanks for the note.", "codacli")

        self.assertIn("Cody Mitchell", text)
        self.assertIn("CodaCLI", text)
        self.assertIn("https://codacli.com", text)
        self.assertIn('href="https://codacli.com"', html)
        self.assertNotIn("Fractal Research", text + html)
        self.assertNotIn("bindmysoul.com", text + html)

    def test_deckfound_signature_uses_support_identity(self) -> None:
        text = mail_ui._compose_cmail_body_text("Thanks for reaching out.", "deckfound")
        html = mail_ui._compose_cmail_html_body("Thanks for reaching out.", "deckfound")

        self.assertIn("DeckFound Support", text)
        self.assertIn("https://deckfound.com", text)
        self.assertIn("support@deckfound.com", text)
        self.assertIn("deckfound.com/assets/favicon-192.png", html)
        self.assertIn("mailto:support@deckfound.com", html)
        self.assertNotIn("Cody", text + html)
        self.assertNotIn("Fractal Research", text + html)
        self.assertNotIn("bindmysoul.com", text + html)

    def test_frg_info_signature_uses_frg_business_identity(self) -> None:
        text = mail_ui._compose_cmail_body_text("Thanks for reaching out.", "frg_info")
        html = mail_ui._compose_cmail_html_body("Thanks for reaching out.", "frg_info")

        self.assertIn("Cody Mitchell", text)
        self.assertIn("Fractal Research Group LLC", text)
        self.assertIn("https://frg.earth", text)
        self.assertIn("(850) 529-8452", text)
        self.assertIn('href="https://frg.earth"', html)
        self.assertNotIn("DeckFound", text + html)
        self.assertNotIn("Bind My Soul", text + html)

    def test_mail_ui_includes_codacli_identity_picker(self) -> None:
        html = mail_ui._render_mail_ui_html()

        self.assertIn('<option value="codacli">Cody / CodaCLI</option>', html)
        self.assertIn('"address": "cody@codacli.com"', html)

    def test_mail_ui_includes_deckfound_identity_picker(self) -> None:
        html = mail_ui._render_mail_ui_html()

        self.assertIn('<option value="deckfound">DeckFound Support</option>', html)
        self.assertIn('"address": "support@deckfound.com"', html)

    def test_mail_ui_includes_frg_info_identity_picker(self) -> None:
        html = mail_ui._render_mail_ui_html()

        self.assertIn('<option value="frg_info">FRG Info</option>', html)
        self.assertIn('"address": "info@frg.earth"', html)
        self.assertIn('"from_email": "Fractal Research Group <info@frg.earth>"', html)

    def test_mail_ui_includes_mobile_first_responsive_styles(self) -> None:
        self.assertIn('@media (max-width: 720px)', mail_ui._HTML)
        self.assertIn('env(safe-area-inset-top, 0px)', mail_ui._HTML)
        self.assertIn('-webkit-overflow-scrolling: touch', mail_ui._HTML)
        self.assertIn('min-height: calc(100dvh - 250px)', mail_ui._HTML)
        self.assertIn('overflow-x: hidden', mail_ui._HTML)
        self.assertIn('grid-template-columns: minmax(0, 380px) minmax(0, 1fr)', mail_ui._HTML)
        self.assertIn('table-layout: auto', mail_ui._HTML)
        self.assertNotIn('table-layout: fixed', mail_ui._HTML)
        self.assertNotIn('.body-rich td:first-child:not(:only-child)', mail_ui._HTML)
        self.assertIn('scrollbar-width: none', mail_ui._HTML)
        self.assertIn('mobile-web-app-capable', mail_ui._HTML)
        self.assertIn('apple-mobile-web-app-capable', mail_ui._HTML)
        self.assertIn('rel="manifest" href="/manifest.webmanifest"', mail_ui._HTML)
        self.assertIn('rel="apple-touch-icon" sizes="180x180" href="/static/apple-touch-icon.png"', mail_ui._HTML)
        self.assertIn('property="og:image" content="__CMAIL_PUBLIC_URL__/static/og-image.png"', mail_ui._HTML)
        self.assertIn('id="deliveryAlerts"', mail_ui._HTML)
        self.assertIn("function renderDeliveryAlerts(payload)", mail_ui._HTML)
        self.assertIn("delivery issue", mail_ui._HTML)
        self.assertIn("initialUrlParams", mail_ui._HTML)
        self.assertIn('initialUrlParams.get("draft")', mail_ui._HTML)
        self.assertIn("function initialHasDraftComposerSeedParams()", mail_ui._HTML)
        self.assertIn("if (initialHasDraftComposerSeedParams()) return null;", mail_ui._HTML)
        self.assertIn("function initialDraftComposerSeed()", mail_ui._HTML)
        self.assertIn('initialUrlParams.get("to")', mail_ui._HTML)
        self.assertIn('initialUrlParams.get("subject")', mail_ui._HTML)
        self.assertIn('initialUrlParams.get("body_text")', mail_ui._HTML)
        self.assertIn("draftComposerSeed: initialDraftComposerSeed()", mail_ui._HTML)
        self.assertIn("function initialActiveView()", mail_ui._HTML)
        self.assertIn("function prepareExternalEmailLinks(root)", mail_ui._HTML)
        self.assertIn('link.target = "_blank"', mail_ui._HTML)
        self.assertNotIn("handleExternalEmailLinkClick", mail_ui._HTML)
        self.assertNotIn('window.open(href, "_blank"', mail_ui._HTML)
        self.assertIn("function renderMailboxRefreshPreservingDetail()", mail_ui._HTML)
        self.assertIn("preserveActiveDetail: true", mail_ui._HTML)
        self.assertGreater(
            mail_ui.MAIL_UI_CLIENT_REFRESH_INTERVAL_MS,
            mail_ui.MAIL_UI_CLIENT_QUEUE_FLUSH_INTERVAL_MS,
        )
        self.assertIn("Cmail is private. Open Tailscale", mail_ui._render_mail_ui_html())
        self.assertEqual("standalone", mail_ui._render_mail_ui_manifest()["display"])

    def test_mail_ui_rich_email_tables_preserve_readable_content_width(self) -> None:
        body_display = mail_ui._body_display(
            "Verify your contact email. Dear Publisher, please confirm this address.",
            html_body="""
                <table><tr>
                  <td></td>
                  <td><table><tr>
                    <td></td>
                    <td><table><tr><td>Verify your contact email</td></tr></table></td>
                    <td></td>
                  </tr></table></td>
                  <td></td>
                </tr></table>
            """,
        )

        self.assertIn("Verify your contact email", str(body_display["primary_html"]))
        self.assertGreaterEqual(str(body_display["primary_html"]).count("<table>"), 3)
        self.assertIn("table-layout: auto", mail_ui._HTML)
        self.assertIn("word-break: normal", mail_ui._HTML)
        self.assertIn("overflow-wrap: anywhere", mail_ui._HTML)
        self.assertNotIn("table-layout: fixed", mail_ui._HTML)
        self.assertNotIn(".body-rich tbody,", mail_ui._HTML)

    def test_address_field_values_handles_unquoted_comma_display_name(self) -> None:
        self.assertEqual(
            ['"Chen, Rui" <ruic20@hs.uci.edu>'],
            mail_ui._address_field_values("Chen, Rui <ruic20@hs.uci.edu>"),
        )

    def test_address_field_values_handles_mixed_recipients_with_comma_display_name(self) -> None:
        self.assertEqual(
            [
                "Alice Example <alice@example.com>",
                '"Chen, Rui" <ruic20@hs.uci.edu>',
            ],
            mail_ui._address_field_values(
                "Alice Example <alice@example.com>, Chen, Rui <ruic20@hs.uci.edu>"
            ),
        )

    def test_address_field_values_drops_invalid_split_recipient_fragments(self) -> None:
        self.assertNotIn(
            "Chen",
            mail_ui._address_field_values("Chen, Rui <ruic20@hs.uci.edu>"),
        )

    def test_mail_ui_selecting_new_correspondence_moves_to_opened_tab(self) -> None:
        self.assertIn("function moveNewSelectionToOpenedMode()", mail_ui._HTML)
        self.assertIn('if (state.contactSortMode !== "new") return false;', mail_ui._HTML)
        self.assertIn('state.contactSortMode = "opened";', mail_ui._HTML)
        select_contact_start = mail_ui._HTML.index("function selectContact(contactKey)")
        select_contact_end = mail_ui._HTML.index("function setContactSortMode", select_contact_start)
        select_contact_js = mail_ui._HTML[select_contact_start:select_contact_end]
        self.assertLess(
            select_contact_js.index("moveNewSelectionToOpenedMode();"),
            select_contact_js.index("markContactOpened(state.selectedContactKey);"),
        )
        self.assertIn("renderChrome();", select_contact_js)

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "life_ops.db"
        self.vault_root = Path(self.temp_dir.name) / "attachments"
        self.original_master_key = os.environ.get(vault_crypto.MASTER_KEY_NAME)
        os.environ[vault_crypto.MASTER_KEY_NAME] = vault_crypto._b64url_encode(b"b" * 32)

        preview_relative_path, preview_sha256 = mail_vault.write_encrypted_vault_file(
            vault_root=self.vault_root,
            relative_dir=Path("cloudflare_email") / "communication-2" / "preview",
            logical_filename="thread-preview.png",
            raw_bytes=PNG_BYTES,
            metadata={"kind": "test-preview"},
        )

        with store.open_db(self.db_path) as connection:
            self.root_communication_id = store.upsert_communication_from_sync(
                connection,
                source="cloudflare_email",
                external_id="msg-001",
                subject="Structured mail test",
                channel="email",
                happened_at=datetime(2026, 3, 30, 9, 0, 0),
                follow_up_at=None,
                direction="inbound",
                person="Alice Example",
                organization_name="FRG",
                status="reference",
                external_thread_id="<thread@example.com>",
                external_from="Alice Example <alice@example.com>",
                external_to="cody@frg.earth",
                message_id="<msg-001@example.com>",
                thread_key="<thread@example.com>",
                snippet="Initial thread context",
                body_text="This is the opening message in the thread.",
                category="research",
                priority_level="high",
                priority_score=87,
            )
            self.latest_communication_id = store.upsert_communication_from_sync(
                connection,
                source="cloudflare_email",
                external_id="msg-002",
                subject="Re: Structured mail test",
                channel="email",
                happened_at=datetime(2026, 3, 30, 9, 15, 0),
                follow_up_at=None,
                direction="inbound",
                person="Alice Example",
                organization_name="FRG",
                status="reference",
                external_thread_id="<thread@example.com>",
                external_from="Alice Example <alice@example.com>",
                external_to="cody@frg.earth",
                external_cc="Bob Example <bob@example.com>",
                external_reply_to="Replies Desk <reply@example.com>",
                message_id="<msg-002@example.com>",
                in_reply_to="<msg-001@example.com>",
                references=["<thread@example.com>", "<msg-001@example.com>"],
                thread_key="<thread@example.com>",
                snippet="Follow-up with attachment preview",
                body_text="This follow-up carries the preview image attachment.",
                html_body="<p>This follow-up carries the preview image attachment.</p>",
                attachments=[{"filename": "thread-preview.png", "mime_type": "image/png", "size": len(PNG_BYTES)}],
                category="research",
                priority_level="high",
                priority_score=92,
            )
            self.other_thread_communication_id = store.upsert_communication_from_sync(
                connection,
                source="cloudflare_email",
                external_id="msg-005",
                subject="Problem 857 follow-up",
                channel="email",
                happened_at=datetime(2026, 3, 29, 12, 0, 0),
                follow_up_at=None,
                direction="inbound",
                person="Alice Example",
                organization_name="FRG",
                status="reference",
                external_thread_id="<thread-2@example.com>",
                external_from="Alice Example <alice@example.com>",
                external_to="cody@frg.earth",
                message_id="<msg-005@example.com>",
                thread_key="<thread-2@example.com>",
                snippet="New conversation branch",
                body_text="Fresh thread body for problem 857.",
                category="research",
                priority_level="normal",
                priority_score=76,
            )
            self.outbound_communication_id = store.upsert_communication_from_sync(
                connection,
                source="resend_email",
                external_id="msg-003",
                subject="Outbound note",
                channel="email",
                happened_at=datetime(2026, 3, 30, 9, 20, 0),
                follow_up_at=None,
                direction="outbound",
                person="Bob Example",
                organization_name="FRG",
                status="sent",
                external_from="Cody <cody@frg.earth>",
                external_to="bob@example.com",
                message_id="<msg-003@example.com>",
                thread_key="<outbound@example.com>",
                snippet="Outbound note",
                body_text="This outbound note should stay out of the inbox view.",
                category="research",
                priority_level="normal",
                priority_score=50,
            )
            self.manual_communication_id = store.upsert_communication_from_sync(
                connection,
                source="manual",
                external_id="msg-004",
                subject="Manual reminder",
                channel="manual",
                happened_at=datetime(2026, 3, 30, 9, 25, 0),
                follow_up_at=None,
                direction="inbound",
                person="Cody Mitchell",
                organization_name="FRG",
                status="reference",
                snippet="This manual item should stay out of the inbox view.",
                body_text="Manual reminder",
                category="general",
                priority_level="normal",
                priority_score=25,
            )
            self.draft_communication_id = store.upsert_communication_from_sync(
                connection,
                source="cmail_draft",
                external_id="draft-001",
                subject="Draft to Terence Tao",
                channel="email",
                happened_at=datetime(2026, 3, 30, 10, 0, 0),
                follow_up_at=None,
                direction="outbound",
                person="",
                organization_name="FRG",
                status="draft",
                external_from="Cody <cody@frg.earth>",
                external_to="terry@example.com",
                message_id="",
                thread_key="",
                snippet="Draft intro note",
                body_text="Professor Tao,\n\nI wanted to share a note.",
                category="research",
                priority_level="normal",
                priority_score=40,
            )
            self.attachment_id = store.upsert_communication_attachment(
                connection,
                external_key="att-002",
                communication_id=self.latest_communication_id,
                source="cloudflare_email",
                external_message_id="msg-002",
                external_attachment_id="attachment-2",
                part_id="2",
                filename="thread-preview.png",
                mime_type="image/png",
                size=len(PNG_BYTES),
                relative_path=preview_relative_path,
                extracted_text="Preview image for the thread.",
                extraction_method="inline_image",
                ingest_status="saved",
                sha256=preview_sha256,
            )
            store.upsert_system_alert(
                connection,
                alert_key="resend_delivery_queue",
                source="resend_delivery",
                severity="warning",
                title="Outbound queue retrying",
                message="One resend delivery is retrying.",
                details={"queue": 1},
            )
            store.upsert_system_alert(
                connection,
                alert_key="cloudflare_mail_sync",
                source="cloudflare_email",
                severity="error",
                title="Cloudflare mail sync failed",
                message="timed out waiting for DB lock",
                details={"lock_path": str(self.db_path)},
            )
            store.set_sync_state(connection, "cloudflare_mail:last_success_at", "2099-01-01T00:00:00Z")
            store.set_sync_state(connection, "cloudflare_mail:last_failure_at", "2026-03-30T09:20:00Z")
            store.set_sync_state(connection, "cloudflare_mail:pending_count", "2")
            store.set_sync_state(connection, "cloudflare_mail:total_stored", "4")
            store.set_sync_state(connection, "cloudflare_mail:total_acknowledged", "2")
            store.set_sync_state(connection, "cloudflare_mail:forwarding_enabled", "0")
            store.set_sync_state(connection, "cloudflare_mail:archive_encryption_enabled", "1")

    def test_cmail_agent_button_is_keyboard_accessible_and_posts_selection(self) -> None:
        self.assertIn('id="agentTerminalButton"', mail_ui._HTML)
        self.assertIn('type="button"', mail_ui._HTML)
        self.assertIn("cursor: pointer;", mail_ui._HTML)
        self.assertIn("function openCmailAgentTerminal()", mail_ui._HTML)
        self.assertIn('postJson("/api/agent-terminal", cmailAgentSelectionPayload())', mail_ui._HTML)
        self.assertIn('label.textContent = "Opening…";', mail_ui._HTML)
        self.assertIn('label.textContent = "Opened";', mail_ui._HTML)
        launcher_start = mail_ui._HTML.index("async function openCmailAgentTerminal()")
        launcher_end = mail_ui._HTML.index("function normalizedContacts", launcher_start)
        self.assertNotIn("/v1/dispatch", mail_ui._HTML[launcher_start:launcher_end])

    def test_cmail_agent_selection_uses_server_side_pointer_fields_only(self) -> None:
        with store.open_db(self.db_path) as connection:
            selection = mail_ui._cmail_agent_selection_from_payload(
                connection,
                {
                    "view": "inbox",
                    "identity": "all",
                    "communication_id": self.latest_communication_id,
                    "body": "untrusted browser content",
                    "thread_key": "untrusted-thread",
                },
            )

        self.assertEqual(self.latest_communication_id, selection["communicationId"])
        self.assertEqual("<thread@example.com>", selection["threadKey"])
        self.assertEqual("<msg-002@example.com>", selection["messageId"])
        self.assertEqual("alice@example.com", selection["contactKey"])
        self.assertNotIn("body", selection)

    def test_cmail_agent_launch_authorization_accepts_local_and_verified_tailnet(self) -> None:
        local_ok, local_mode = mail_ui._cmail_agent_request_authorized(
            {"Host": "127.0.0.1:4311", "Origin": "http://127.0.0.1:4311"},
            client_host="127.0.0.1",
        )
        wrong_origin_ok, wrong_origin_reason = mail_ui._cmail_agent_request_authorized(
            {"Host": "127.0.0.1:4311", "Origin": "https://example.com"},
            client_host="127.0.0.1",
        )
        with mock.patch(
            "life_ops.mail_ui._cmail_agent_allowed_users",
            return_value={"cody@example.com"},
        ):
            tailnet_ok, tailnet_mode = mail_ui._cmail_agent_request_authorized(
                {
                    "Host": "cmail.tail649edd.ts.net",
                    "Origin": "https://cmail.tail649edd.ts.net",
                    "Tailscale-User-Login": "cody@example.com",
                },
                client_host="127.0.0.1",
            )
            missing_identity_ok, missing_identity_reason = mail_ui._cmail_agent_request_authorized(
                {
                    "Host": "cmail.tail649edd.ts.net",
                    "Origin": "https://cmail.tail649edd.ts.net",
                },
                client_host="127.0.0.1",
            )

        self.assertTrue(local_ok)
        self.assertEqual("local", local_mode)
        self.assertFalse(wrong_origin_ok)
        self.assertEqual("agent_terminal_requires_same_origin", wrong_origin_reason)
        self.assertTrue(tailnet_ok)
        self.assertEqual("tailnet", tailnet_mode)
        self.assertFalse(missing_identity_ok)
        self.assertEqual("agent_terminal_requires_tailnet_identity", missing_identity_reason)

    def test_cmail_agent_endpoint_launches_with_validated_message_pointer(self) -> None:
        handler = mail_ui._make_handler(
            db_path=self.db_path,
            limit=20,
            enable_background_remote_sync=False,
        )
        server = HTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base_url = f"http://127.0.0.1:{server.server_port}"
        try:
            with mock.patch(
                "life_ops.mail_ui.cmail_agent.open_terminal",
                return_value={
                    "ok": True,
                    "sessionId": "cmail-session",
                    "launchMode": "resume",
                },
            ) as open_terminal:
                request = urllib.request.Request(
                    f"{base_url}/api/agent-terminal",
                    data=json.dumps(
                        {
                            "view": "inbox",
                            "identity": "all",
                            "communication_id": self.latest_communication_id,
                            "body": "must not pass through",
                        }
                    ).encode("utf-8"),
                    method="POST",
                    headers={
                        "Content-Type": "application/json",
                        "Origin": base_url,
                    },
                )
                with urllib.request.urlopen(request) as response:
                    payload = json.loads(response.read().decode("utf-8"))

            self.assertTrue(payload["ok"])
            selection = open_terminal.call_args.args[0]
            self.assertEqual(self.latest_communication_id, selection["communicationId"])
            self.assertEqual("<thread@example.com>", selection["threadKey"])
            self.assertNotIn("body", selection)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def tearDown(self) -> None:
        if self.original_master_key is None:
            os.environ.pop(vault_crypto.MASTER_KEY_NAME, None)
        else:
            os.environ[vault_crypto.MASTER_KEY_NAME] = self.original_master_key
        self.temp_dir.cleanup()

    def test_build_mail_ui_overview_groups_threads_and_surfaces_queue_state(self) -> None:
        payload = mail_ui.build_mail_ui_overview(
            db_path=self.db_path,
            direction="inbound",
            limit=10,
        )

        self.assertEqual(3, payload["message_count"])
        self.assertEqual(1, payload["contact_count"])
        self.assertEqual(1, len(payload["contacts"]))
        self.assertEqual("Re: Structured mail test", payload["messages"][0]["subject"])
        self.assertEqual(self.latest_communication_id, payload["contacts"][0]["latest_message_id"])
        self.assertEqual(3, payload["contacts"][0]["count"])
        self.assertEqual("Alice Example", payload["contacts"][0]["contact_label"])
        self.assertEqual(2, len(payload["contacts"][0]["threads"]))
        self.assertEqual("<thread@example.com>", payload["contacts"][0]["threads"][0]["thread_key"])
        self.assertEqual(2, payload["contacts"][0]["threads"][0]["count"])
        self.assertEqual("<thread-2@example.com>", payload["contacts"][0]["threads"][1]["thread_key"])
        self.assertEqual(1, payload["contacts"][0]["threads"][1]["count"])
        self.assertEqual("healthy", payload["cloudflare_sync"]["status"])
        self.assertEqual("2099-01-01T00:00:00Z", payload["cloudflare_sync"]["last_success_at"])
        self.assertEqual(2, payload["cloudflare_queue"]["pending_count"])
        self.assertEqual(1, payload["resend_queue"]["active_alert_count"])
        self.assertEqual("Outbound queue retrying", payload["resend_queue"]["active_alerts"][0]["title"])
        self.assertIn("message-id:<msg-002@example.com>", payload["viewed_message_keys"])
        with store.open_db(self.db_path) as connection:
            self.assertTrue(store.get_sync_state(connection, store.MAIL_UI_VIEWED_MESSAGES_SEEDED_SYNC_KEY))
            self.assertTrue(store.get_sync_state(connection, store.MAIL_UI_VIEWED_MESSAGES_BASELINE_SYNC_KEY))
            viewed_keys = store.get_viewed_mail_message_keys(connection)
        self.assertIn("message-id:<msg-002@example.com>", viewed_keys)

        with store.open_db(self.db_path) as connection:
            store.upsert_communication_from_sync(
                connection,
                source="cloudflare_email",
                external_id="msg-999",
                subject="Later inbound",
                channel="email",
                happened_at=datetime(2026, 3, 30, 11, 0, 0),
                follow_up_at=None,
                direction="inbound",
                person="Alice Example",
                organization_name="FRG",
                status="reference",
                external_from="Alice Example <alice@example.com>",
                external_to="cody@frg.earth",
                message_id="<msg-999@example.com>",
                thread_key="<thread@example.com>",
                snippet="Arrived after the baseline seed",
                body_text="This should still be new.",
                category="research",
                priority_level="high",
                priority_score=95,
            )
        next_payload = mail_ui.build_mail_ui_overview(
            db_path=self.db_path,
            direction="inbound",
            limit=10,
        )
        self.assertNotIn("message-id:<msg-999@example.com>", next_payload["viewed_message_keys"])
        self.assertNotIn(self.manual_communication_id, [message["id"] for message in payload["messages"]])

    def test_sync_status_surfaces_resend_delivery_alerts(self) -> None:
        handler = mail_ui._make_handler(db_path=self.db_path, limit=20)
        server = HTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{server.server_port}/api/sync-status") as response:
                payload = json.loads(response.read().decode("utf-8"))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        self.assertIn("resend_queue", payload)
        self.assertEqual(1, payload["resend_queue"]["active_alert_count"])
        self.assertEqual("Outbound queue retrying", payload["resend_queue"]["active_alerts"][0]["title"])

    def test_build_correspondence_overview_combines_inbound_and_outbound_by_counterparty(self) -> None:
        payload = mail_ui.build_mail_ui_overview(
            db_path=self.db_path,
            source=mail_ui.DEFAULT_MAIL_UI_CORRESPONDENCE_SOURCE,
            limit=10,
        )

        self.assertEqual(4, payload["message_count"])
        self.assertEqual(2, payload["contact_count"])
        self.assertEqual(
            [
                self.outbound_communication_id,
                self.latest_communication_id,
                self.root_communication_id,
                self.other_thread_communication_id,
            ],
            [message["id"] for message in payload["messages"]],
        )
        self.assertEqual("bob@example.com", payload["contacts"][0]["contact_key"])
        self.assertEqual(1, payload["contacts"][0]["count"])
        self.assertEqual([self.outbound_communication_id], payload["contacts"][0]["message_ids"])
        self.assertEqual("alice@example.com", payload["contacts"][1]["contact_key"])
        self.assertEqual(3, payload["contacts"][1]["count"])
        self.assertEqual(
            [self.latest_communication_id, self.root_communication_id, self.other_thread_communication_id],
            payload["contacts"][1]["message_ids"],
        )
        self.assertNotIn(self.draft_communication_id, [message["id"] for message in payload["messages"]])
        self.assertNotIn(self.manual_communication_id, [message["id"] for message in payload["messages"]])

    def test_correspondence_query_uses_dedicated_index(self) -> None:
        with store.open_db(self.db_path) as connection:
            indexes = {
                str(row["name"])
                for row in connection.execute("PRAGMA index_list(communications)").fetchall()
            }
            plan = connection.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT communications.id
                FROM communications
                WHERE communications.channel = 'email'
                  AND communications.status != 'deleted'
                  AND communications.source IN (?, ?, ?, ?)
                  AND communications.direction IN ('inbound', 'outbound')
                ORDER BY communications.happened_at DESC, communications.id DESC
                """,
                mail_ui.DEFAULT_MAIL_UI_CORRESPONDENCE_SOURCES,
            ).fetchall()
            orphan_plan = connection.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT communications.id
                FROM communications
                WHERE communications.channel = 'email'
                  AND communications.source = ?
                  AND communications.direction = 'outbound'
                  AND communications.status IN ('queued', 'retrying', 'sending')
                ORDER BY communications.id
                """,
                (mail_ui.DEFAULT_MAIL_UI_OUTBOUND_SOURCE,),
            ).fetchall()

        self.assertIn("idx_communications_mail_ui_correspondence", indexes)
        self.assertIn("idx_communications_mail_ui_source_state", indexes)
        self.assertTrue(
            any("idx_communications_mail_ui_" in str(row["detail"]) for row in plan),
            plan,
        )
        self.assertTrue(
            any("idx_communications_mail_ui_source_state" in str(row["detail"]) for row in orphan_plan),
            orphan_plan,
        )

    def test_build_correspondence_overview_includes_resend_received_support_mail(self) -> None:
        with store.open_db(self.db_path) as connection:
            support_message_id = store.upsert_communication_from_sync(
                connection,
                source="resend_inbound",
                external_id="resend-receiving:support-1",
                subject="Character binding issue",
                channel="email",
                happened_at=datetime(2026, 8, 18, 8, 30),
                follow_up_at=None,
                direction="inbound",
                mail_identity="bind_my_soul",
                external_from="Player One <player@example.com>",
                external_to="support@bindmysoul.com",
                message_id="<support-1@example.com>",
                thread_key="<support-1@example.com>",
                body_text="My characters are not binding.",
            )

        payload = mail_ui.build_mail_ui_overview(
            db_path=self.db_path,
            source=mail_ui.DEFAULT_MAIL_UI_CORRESPONDENCE_SOURCE,
            identity="bind_my_soul",
            limit=10,
        )

        self.assertEqual(1, payload["message_count"])
        self.assertEqual([support_message_id], [message["id"] for message in payload["messages"]])
        self.assertEqual("resend_inbound", payload["messages"][0]["source"])
        self.assertEqual("bind_my_soul", payload["messages"][0]["mail_identity"])

    def test_build_correspondence_overview_filters_deckfound_identity(self) -> None:
        with store.open_db(self.db_path) as connection:
            deckfound_message_id = store.upsert_communication_from_sync(
                connection,
                source="cloudflare_email",
                external_id="deckfound-support-1",
                subject="DeckFound install question",
                channel="email",
                happened_at=datetime(2026, 8, 23, 16, 53),
                follow_up_at=None,
                direction="inbound",
                mail_identity="deckfound",
                external_from="Customer <customer@example.com>",
                external_to="support@deckfound.com",
                message_id="<deckfound-support-1@example.com>",
                thread_key="<deckfound-support-1@example.com>",
                body_text="Can you help with installation?",
            )

        payload = mail_ui.build_mail_ui_overview(
            db_path=self.db_path,
            source=mail_ui.DEFAULT_MAIL_UI_CORRESPONDENCE_SOURCE,
            identity="deckfound",
            limit=10,
        )

        self.assertEqual([deckfound_message_id], [message["id"] for message in payload["messages"]])
        self.assertEqual("deckfound", payload["identity"])
        self.assertEqual("DeckFound Support", payload["messages"][0]["mail_identity_label"])

    def test_build_correspondence_overview_includes_codacli_gmail_archive(self) -> None:
        with store.open_db(self.db_path) as connection:
            inbound_id = store.upsert_communication_from_sync(
                connection,
                source="gmail_archive",
                external_id="gmail:cody@codacli.com:archive-in-1",
                subject="Historical CodaCLI message",
                channel="email",
                happened_at=datetime(2026, 1, 5, 8, 30),
                follow_up_at=None,
                direction="inbound",
                mail_identity="codacli",
                external_from="Customer <customer@example.com>",
                external_to="cody@codacli.com",
                message_id="<archive-in-1@example.com>",
                thread_key="<archive-in-1@example.com>",
                body_text="Archived before the Workspace cancellation.",
            )

        payload = mail_ui.build_mail_ui_overview(
            db_path=self.db_path,
            source=mail_ui.DEFAULT_MAIL_UI_CORRESPONDENCE_SOURCE,
            identity="codacli",
            limit=10,
        )

        self.assertEqual([inbound_id], [message["id"] for message in payload["messages"]])
        self.assertEqual("gmail_archive", payload["messages"][0]["source"])
        self.assertEqual("codacli", payload["messages"][0]["mail_identity"])

    def test_correspondence_mailbox_version_counts_beyond_visible_page(self) -> None:
        payload = mail_ui.build_mail_ui_overview(
            db_path=self.db_path,
            source=mail_ui.DEFAULT_MAIL_UI_CORRESPONDENCE_SOURCE,
            limit=1,
        )

        self.assertEqual(1, payload["message_count"])
        self.assertEqual(4, payload["mailbox_version"]["message_count"])
        self.assertEqual(2, payload["mailbox_version"]["contact_count"])
        self.assertEqual(self.outbound_communication_id, payload["mailbox_version"]["latest_message_id"])
        self.assertIn("ui_state_digest", payload["mailbox_version"])

    def test_opened_correspondence_stays_visible_beyond_visible_page(self) -> None:
        with store.open_db(self.db_path) as connection:
            store.mark_touched_mail_contact(
                connection,
                contact_key="alice@example.com",
                touched_at="2026-04-22T13:00:00Z",
            )

        payload = mail_ui.build_mail_ui_overview(
            db_path=self.db_path,
            source=mail_ui.DEFAULT_MAIL_UI_CORRESPONDENCE_SOURCE,
            limit=1,
        )
        alice_contact = next(
            contact
            for contact in payload["contacts"]
            if contact["contact_key"] == "alice@example.com"
        )

        self.assertEqual(2, payload["message_count"])
        self.assertEqual(
            1,
            len(
                [
                    message
                    for message in payload["messages"]
                    if message["contact_key"] == "alice@example.com"
                ]
            ),
        )
        self.assertEqual("2026-04-22T13:00:00Z", alice_contact["opened_at"])
        self.assertEqual("2026-04-22T13:00:00Z", alice_contact["touched_at"])
        self.assertEqual(1, payload["mailbox_version"]["opened_contact_count"])

    def test_hidden_correspondence_stays_out_of_main_view_until_restored(self) -> None:
        handler = mail_ui._make_handler(db_path=self.db_path, limit=20)
        server = HTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base_url = f"http://127.0.0.1:{server.server_port}"
        try:
            request = urllib.request.Request(
                f"{base_url}/api/contacts/hide",
                data=json.dumps({"contact_key": "alice@example.com"}).encode("utf-8"),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(request) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertTrue(payload["hidden"])

            with urllib.request.urlopen(f"{base_url}/api/overview?source=correspondence") as response:
                visible = json.loads(response.read().decode("utf-8"))
            self.assertEqual(1, visible["message_count"])
            self.assertEqual(["bob@example.com"], [contact["contact_key"] for contact in visible["contacts"]])

            with urllib.request.urlopen(f"{base_url}/api/overview?source=correspondence&mailbox=hidden") as response:
                hidden = json.loads(response.read().decode("utf-8"))
            self.assertEqual(3, hidden["message_count"])
            self.assertEqual(["alice@example.com"], [contact["contact_key"] for contact in hidden["contacts"]])

            with store.open_db(self.db_path) as connection:
                root_row = store.get_communication_by_id(connection, self.root_communication_id)
                store.upsert_communication_from_sync(
                    connection,
                    source="cloudflare_email",
                    external_id="msg-006",
                    subject="Fresh hidden note",
                    channel="email",
                    happened_at=datetime(2026, 3, 30, 11, 45, 0),
                    follow_up_at=None,
                    direction="inbound",
                    person="Alice Example",
                    organization_name="FRG",
                    status="reference",
                    external_thread_id="<thread-3@example.com>",
                    external_from="Alice Example <alice@example.com>",
                    external_to="cody@frg.earth",
                    message_id="<msg-006@example.com>",
                    thread_key="<thread-3@example.com>",
                    snippet="Fresh hidden note",
                    body_text="New mail from a hidden sender should stay hidden.",
                    category="research",
                    priority_level="normal",
                    priority_score=60,
                )
            self.assertEqual("reference", str(root_row["status"]))

            with urllib.request.urlopen(f"{base_url}/api/overview?source=correspondence") as response:
                visible_after_new_mail = json.loads(response.read().decode("utf-8"))
            self.assertEqual(["bob@example.com"], [contact["contact_key"] for contact in visible_after_new_mail["contacts"]])

            with urllib.request.urlopen(f"{base_url}/api/overview?source=correspondence&mailbox=hidden") as response:
                hidden_after_new_mail = json.loads(response.read().decode("utf-8"))
            self.assertEqual(4, hidden_after_new_mail["message_count"])
            self.assertEqual("Fresh hidden note", hidden_after_new_mail["messages"][0]["subject"])

            request = urllib.request.Request(
                f"{base_url}/api/contacts/unhide",
                data=json.dumps({"contact_key": "alice@example.com"}).encode("utf-8"),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(request) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertFalse(payload["hidden"])
            self.assertTrue(payload["cleared"])

            with urllib.request.urlopen(f"{base_url}/api/overview?source=correspondence") as response:
                restored = json.loads(response.read().decode("utf-8"))
            self.assertEqual(5, restored["message_count"])
            self.assertEqual({"alice@example.com", "bob@example.com"}, {contact["contact_key"] for contact in restored["contacts"]})
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_http_app_auth_locks_ui_and_apis_until_unlocked(self) -> None:
        with mock.patch.dict(os.environ, {mail_ui.CMAIL_APP_SECRET_NAME: "test-unlock-code"}, clear=False):
            handler = mail_ui._make_handler(db_path=self.db_path, limit=20, require_app_auth=True)
            server = HTTPServer(("127.0.0.1", 0), handler)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            base_url = f"http://127.0.0.1:{server.server_port}"
            try:
                with urllib.request.urlopen(f"{base_url}/healthz") as response:
                    healthz = json.loads(response.read().decode("utf-8"))
                self.assertTrue(healthz["ok"])
                self.assertTrue(healthz["auth_required"])
                self.assertTrue(healthz["auth_configured"])

                with self.assertRaises(urllib.error.HTTPError) as locked_root:
                    urllib.request.urlopen(f"{base_url}/")
                self.assertEqual(401, locked_root.exception.code)
                self.assertIn("Unlock Cmail", locked_root.exception.read().decode("utf-8"))

                with self.assertRaises(urllib.error.HTTPError) as locked_api:
                    urllib.request.urlopen(f"{base_url}/api/overview?direction=inbound")
                self.assertEqual(401, locked_api.exception.code)
                self.assertEqual("cmail_locked", json.loads(locked_api.exception.read().decode("utf-8"))["error"])

                bad_unlock = urllib.request.Request(
                    f"{base_url}/api/auth/unlock",
                    data=json.dumps({"unlock_code": "wrong"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with self.assertRaises(urllib.error.HTTPError) as bad_response:
                    urllib.request.urlopen(bad_unlock)
                self.assertEqual(401, bad_response.exception.code)

                good_unlock = urllib.request.Request(
                    f"{base_url}/api/auth/unlock",
                    data=json.dumps({"unlock_code": "test-unlock-code"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urllib.request.urlopen(good_unlock) as response:
                    unlock_payload = json.loads(response.read().decode("utf-8"))
                    set_cookie = response.headers.get("Set-Cookie") or ""
                self.assertTrue(unlock_payload["ok"])
                self.assertIn(mail_ui.CMAIL_SESSION_COOKIE_NAME, set_cookie)

                cookie_header = set_cookie.split(";", 1)[0]
                authed_request = urllib.request.Request(
                    f"{base_url}/api/overview?direction=inbound",
                    headers={"Cookie": cookie_header},
                )
                with urllib.request.urlopen(authed_request) as response:
                    overview = json.loads(response.read().decode("utf-8"))
                self.assertEqual(1, overview["contact_count"])

                status_request = urllib.request.Request(
                    f"{base_url}/api/auth/status",
                    headers={"Cookie": cookie_header},
                )
                with urllib.request.urlopen(status_request) as response:
                    status_payload = json.loads(response.read().decode("utf-8"))
                self.assertTrue(status_payload["authenticated"])
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

    def test_http_root_does_not_wait_for_mailbox_overview(self) -> None:
        handler = mail_ui._make_handler(
            db_path=self.db_path,
            limit=20,
            enable_background_remote_sync=False,
        )
        server = HTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with mock.patch(
                "life_ops.mail_ui._build_correspondence_overview_from_connection",
                side_effect=AssertionError("root must not build mailbox overview"),
            ):
                with urllib.request.urlopen(f"http://127.0.0.1:{server.server_port}/", timeout=2) as response:
                    body = response.read().decode("utf-8")
            self.assertEqual(200, response.status)
            self.assertIn("CMAIL", body)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_build_correspondence_overview_suppresses_orphaned_resend_queue_artifacts(self) -> None:
        with store.open_db(self.db_path) as connection:
            orphaned_id = store.upsert_communication_from_sync(
                connection,
                source="resend_email",
                external_id="queued:<orphaned@example.com>",
                subject="Orphaned resend artifact",
                channel="email",
                happened_at=datetime(2026, 3, 30, 11, 0, 0),
                follow_up_at=None,
                direction="outbound",
                person="Ghost Queue",
                organization_name="FRG",
                status="queued",
                external_from="Cody <cody@frg.earth>",
                external_to="ghost@example.com",
                message_id="<orphaned@example.com>",
                thread_key="<orphaned@example.com>",
                snippet="This should not show up as live correspondence.",
                body_text="This queued resend row never reached the real delivery queue.",
                category="research",
                priority_level="normal",
                priority_score=10,
            )

        cleaned_ids = mail_ui._cleanup_orphaned_resend_correspondence(self.db_path)
        payload = mail_ui.build_mail_ui_overview(
            db_path=self.db_path,
            source=mail_ui.DEFAULT_MAIL_UI_CORRESPONDENCE_SOURCE,
            limit=20,
        )

        self.assertEqual([orphaned_id], cleaned_ids)
        self.assertNotIn(orphaned_id, [message["id"] for message in payload["messages"]])
        with store.open_db(self.db_path) as connection:
            row = store.get_communication_by_id(connection, orphaned_id)
        self.assertIsNotNone(row)
        self.assertEqual("deleted", str(row["status"] or ""))
        self.assertIn("orphaned outbound Resend artifact", str(row["notes"] or ""))

    def test_cleanup_generated_frg_confirmation_drafts_keeps_manual_drafts(self) -> None:
        with store.open_db(self.db_path) as connection:
            generated = mail_ui._save_cmail_draft(
                connection,
                {
                    "subject": "FRG booking confirmed · Apr 18, 2026, 1:00 PM",
                    "to": "Ada Lovelace <ada@example.com>",
                    "body_text": (
                        "Hi Ada,\n\n"
                        "Your Fractal Research Group booking is confirmed.\n\n"
                        "When: Apr 18, 2026, 1:00 PM"
                    ),
                },
            )
            manual = mail_ui._save_cmail_draft(
                connection,
                {
                    "subject": "FRG booking confirmed · manual follow-up",
                    "to": "Ada Lovelace <ada@example.com>",
                    "body_text": "Manual note I am still editing.",
                },
            )

        with mock.patch.dict(
            os.environ,
            {
                "FRG_BOOKING_CONFIRMATION_MODE": "",
                "FRG_FORGE_CONFIRMATION_MODE": "",
            },
        ):
            cleaned_ids = mail_ui._cleanup_generated_frg_confirmation_drafts(self.db_path)

        self.assertEqual([int(generated["id"])], cleaned_ids)
        drafts = mail_ui.list_cmail_drafts(db_path=self.db_path)
        draft_ids = [int(draft["id"]) for draft in drafts]
        self.assertIn(int(manual["id"]), draft_ids)
        self.assertNotIn(int(generated["id"]), draft_ids)

    def test_mail_contacts_are_persisted_and_searchable(self) -> None:
        with store.open_db(self.db_path) as connection:
            contacts = store.list_mail_contacts(connection, limit=20)
            alice_results = store.list_mail_contacts(connection, query="alice@example.com", limit=20)
            bob_results = store.list_mail_contacts(connection, query="bob@example.com", limit=20)
            terry_results = store.list_mail_contacts(connection, query="terry@example.com", limit=20)

        contact_emails = {str(row["email"] or "") for row in contacts}
        self.assertIn("alice@example.com", contact_emails)
        self.assertIn("bob@example.com", contact_emails)
        self.assertIn("terry@example.com", contact_emails)
        self.assertEqual("alice@example.com", str(alice_results[0]["email"]))
        self.assertEqual("bob@example.com", str(bob_results[0]["email"]))
        self.assertEqual("terry@example.com", str(terry_results[0]["email"]))

    def test_body_display_uses_sanitized_rich_html_for_quoted_email(self) -> None:
        body_display = mail_ui._body_display(
            "Thanks for the note.\n\nOn Wed, Apr 1, 2026 at 12:45 PM Cody <cody@frg.earth> wrote:\n> Professor Tao,\n> https://frg.earth",
            html_body="""
                <div>Thanks for the note.</div>
                <div class="gmail_quote">
                  <div>Professor Tao,</div>
                  <p><a href="https://frg.earth">frg.earth</a></p>
                  <img src="https://frg.earth/branding/frg-bimi-iris-floating.png" alt="FRG iris">
                  <script>alert('xss')</script>
                </div>
            """,
            snippet="Thanks for the note.",
        )

        self.assertTrue(body_display["has_quote"])
        self.assertEqual("Thanks for the note.", body_display["primary_text"])
        self.assertIn('href="https://frg.earth"', str(body_display["quoted_html"]))
        self.assertIn('target="_blank" rel="noreferrer noopener"', str(body_display["quoted_html"]))
        self.assertIn("frg-bimi-iris-floating.png", str(body_display["quoted_html"]))
        self.assertNotIn("<script", str(body_display["quoted_html"]))
        self.assertNotIn("On Wed, Apr 1, 2026 at 12:45 PM Cody &lt;cody@frg.earth&gt; wrote:", str(body_display["quoted_html"]))

    def test_body_display_uses_sanitized_rich_html_for_primary_body(self) -> None:
        body_display = mail_ui._body_display(
            """<!DOCTYPE html><html lang="en"><head><title>Receipt</title><style>.x{color:red}</style></head>
            <body>
              <table><tr><td><div>Payment Receipt Confirmation</div></td></tr></table>
              <p><a href="https://example.com/receipt">View receipt</a></p>
              <img src="https://securecheckout-fl.cdc.nicusa.com/logo.png" alt="remote tracking image">
              <script>alert('xss')</script>
            </body></html>""",
            html_body="",
            snippet="Payment Receipt Confirmation",
        )

        self.assertFalse(body_display["has_quote"])
        self.assertIn("Payment Receipt Confirmation", str(body_display["primary_html"]))
        self.assertIn('href="https://example.com/receipt"', str(body_display["primary_html"]))
        self.assertIn('target="_blank" rel="noreferrer noopener"', str(body_display["primary_html"]))
        self.assertNotIn("securecheckout-fl.cdc.nicusa.com", str(body_display["primary_html"]))
        self.assertNotIn("<script", str(body_display["primary_html"]))
        self.assertNotIn("<style", str(body_display["primary_html"]))
        self.assertNotIn("<!DOCTYPE", str(body_display["primary_html"]))

    def test_body_display_linkifies_plain_text_urls_safely(self) -> None:
        body_display = mail_ui._body_display(
            "Activate here: https://urs.earthdata.nasa.gov/activate/abc_123. Then visit www.frg.earth/docs or mailto:cody@frg.earth",
        )

        self.assertFalse(body_display["has_quote"])
        self.assertIn(
            'href="https://urs.earthdata.nasa.gov/activate/abc_123"',
            str(body_display["primary_html"]),
        )
        self.assertIn('href="https://www.frg.earth/docs"', str(body_display["primary_html"]))
        self.assertIn('href="mailto:cody@frg.earth"', str(body_display["primary_html"]))
        self.assertIn("</a>.", str(body_display["primary_html"]))
        self.assertNotIn("javascript:", str(body_display["primary_html"]))

    def test_body_display_linkifies_plain_text_quoted_urls_safely(self) -> None:
        body_display = mail_ui._body_display(
            "Looks good.\n\nOn Wed, Apr 1, 2026 at 12:45 PM Cody <cody@frg.earth> wrote:\n> See https://github.com/SproutSeeds/erdos-problems.",
        )

        self.assertTrue(body_display["has_quote"])
        self.assertIn(
            'href="https://github.com/SproutSeeds/erdos-problems"',
            str(body_display["quoted_html"]),
        )
        self.assertIn("</a>.", str(body_display["quoted_html"]))

    def test_http_endpoints_expose_threads_drafts_and_image_previews(self) -> None:
        handler = mail_ui._make_handler(db_path=self.db_path, limit=20)
        handler._prime_overview_cache()  # type: ignore[attr-defined]
        server = HTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base_url = f"http://127.0.0.1:{server.server_port}"
        try:
            with mock.patch(
                "life_ops.mail_ui.store.attachment_vault_root",
                return_value=self.vault_root,
            ):
                with urllib.request.urlopen(f"{base_url}/") as response:
                    html = response.read().decode("utf-8")
                self.assertIn("CMAIL", html)
                self.assertNotIn('const SERVER_BOOTSTRAP_OVERVIEW = {};', html)
                self.assertIn("Structured mail test", html)
                self.assertLess(len(html.encode("utf-8")), 1_500_000)
                node_binary = shutil.which("node")
                if node_binary:
                    inline_scripts = re.findall(r"<script>(.*?)</script>", html, flags=re.DOTALL)
                    syntax_check = subprocess.run(
                        [node_binary, "--check", "-"],
                        input="\n".join(inline_scripts),
                        text=True,
                        capture_output=True,
                        check=False,
                    )
                    self.assertEqual(0, syntax_check.returncode, syntax_check.stderr)
                self.assertIn('rel="icon" href="/static/favicon.svg"', html)
                self.assertIn('rel="manifest" href="/manifest.webmanifest"', html)
                self.assertIn('rel="apple-touch-icon" sizes="180x180" href="/static/apple-touch-icon.png"', html)
                self.assertIn('rel="canonical" href="https://cmail.tail649edd.ts.net/"', html)
                self.assertIn('property="og:url" content="https://cmail.tail649edd.ts.net/"', html)
                self.assertIn(
                    'property="og:image" content="https://cmail.tail649edd.ts.net/static/og-image.png"',
                    html,
                )
                self.assertIn('property="og:image:type" content="image/png"', html)
                self.assertIn("Cmail is private. Open Tailscale", html)
                self.assertIn('id="correspondenceSearch"', html)
                self.assertIn('placeholder="search name, email, or body"', html)
                self.assertNotIn('placeholder="search name or email"', html)
                self.assertIn("disabled aria-disabled='true'", html)
                self.assertIn('aria-label="Move sender to Spam"', html)
                self.assertIn('class="hide-icon"', html)
                self.assertIn('class="trash-icon"', html)
                self.assertIn('data-clear-selection', html)
                self.assertIn("function clearCorrespondenceSelection", html)
                self.assertIn('event.key !== "Escape"', html)
                self.assertIn('aria-label="Archive correspondence"', html)
                self.assertIn('aria-label="Archive message"', html)
                self.assertIn("state.selectedContactKey = null;", html)
                self.assertIn('data-reply-message="${message.id}"', html)
                self.assertIn("lifeops.mail.viewedMessageIds", html)
                self.assertIn(".unread-orb", html)
                self.assertIn("function displayedContacts(contacts)", html)
                self.assertIn("function contactMatchesSortMode(contact)", html)
                self.assertIn("sortAllButton", html)
                self.assertIn('state.contactSortMode === "new"', html)
                self.assertIn("return contactHasNewMail(contact);", html)
                self.assertIn("function messageHasViewedKey(message)", html)
                self.assertIn("version.ui_state_digest", html)
                self.assertIn("function messageIsNew(message)", html)
                self.assertIn("function contactTimestampLabel(contact)", html)
                self.assertIn('class="thread-timestamp"', html)
                self.assertIn('No new correspondence.', html)
                self.assertIn('No opened correspondence yet.', html)
                self.assertIn("message?.search_text", html)
                self.assertIn(".slice(0, 8)", html)
                self.assertNotIn("__MAIL_UI_CLIENT_DETAIL_PREFETCH_LIMIT__", html)
                self.assertIn("/api/messages/read", html)
                self.assertIn("/api/contacts/open", html)
                self.assertIn("viewed_message_keys", html)
                self.assertIn("sortOpenedButton", html)
                self.assertIn('id="sortOpenedButton" role="tab" aria-selected="false">opened</button>', html)
                self.assertIn('setContactSortMode("opened")', html)
                self.assertIn('if (clean === "open" || clean === "opened" || clean === "touched") return "opened";', html)
                self.assertIn("function mergeContactUiMetadata", html)
                self.assertNotIn('state.contactSortMode = "all";', html)
                self.assertIn("lifeops.mail.contactSortMode", html)
                self.assertIn("lifeops.mail.contactSortModeVersion", html)
                self.assertIn('state.contactSortMode || "all"', html)
                self.assertIn('messageIsNew(entry) ? "● " : ""', html)
                self.assertIn("function messageReadKeys(message)", html)
                self.assertNotIn("all sources", html)
                self.assertNotIn("quick drafts", html)
                self.assertNotIn("__MAIL_UI_CLIENT_REFRESH_INTERVAL_MS__", html)
                self.assertNotIn("__MAIL_UI_CLIENT_QUEUE_FLUSH_INTERVAL_MS__", html)

                with urllib.request.urlopen(f"{base_url}/static/favicon.svg") as response:
                    favicon_bytes = response.read()
                    favicon_type = response.headers.get_content_type()
                self.assertEqual("image/svg+xml", favicon_type)
                self.assertIn(b"<svg", favicon_bytes)

                with urllib.request.urlopen(f"{base_url}/static/apple-touch-icon.png") as response:
                    touch_icon_bytes = response.read()
                    touch_icon_type = response.headers.get_content_type()
                self.assertEqual("image/png", touch_icon_type)
                self.assertTrue(touch_icon_bytes.startswith(b"\x89PNG"))

                with urllib.request.urlopen(f"{base_url}/static/og-image.png") as response:
                    og_image_bytes = response.read()
                    og_image_type = response.headers.get_content_type()
                self.assertEqual("image/png", og_image_type)
                self.assertTrue(og_image_bytes.startswith(b"\x89PNG"))

                with urllib.request.urlopen(f"{base_url}/manifest.webmanifest") as response:
                    manifest = json.loads(response.read().decode("utf-8"))
                    manifest_type = response.headers.get_content_type()
                self.assertEqual("application/manifest+json", manifest_type)
                self.assertEqual("Cmail", manifest["name"])
                self.assertEqual("Cmail", manifest["short_name"])
                self.assertEqual("https://cmail.tail649edd.ts.net/", manifest["id"])
                self.assertEqual("https://cmail.tail649edd.ts.net/", manifest["start_url"])
                self.assertEqual("https://cmail.tail649edd.ts.net/", manifest["scope"])
                self.assertEqual("standalone", manifest["display"])
                self.assertIn("/static/icon-192.png", [icon["src"] for icon in manifest["icons"]])
                self.assertIn("/static/icon-512.png", [icon["src"] for icon in manifest["icons"]])

                with urllib.request.urlopen(f"{base_url}/healthz") as response:
                    healthz = json.loads(response.read().decode("utf-8"))
                    healthz_type = response.headers.get_content_type()
                self.assertEqual("application/json", healthz_type)
                self.assertTrue(healthz["ok"])
                self.assertEqual("cmail", healthz["app"])
                self.assertEqual("https://cmail.tail649edd.ts.net", healthz["public_url"])
                self.assertRegex(healthz["version"], r"^\d+\.\d+\.\d+|unknown$")

                with urllib.request.urlopen(f"{base_url}/api/health") as response:
                    api_health = json.loads(response.read().decode("utf-8"))
                self.assertTrue(api_health["ok"])
                self.assertEqual("cmail", api_health["app"])
                self.assertIn("version", api_health)
                self.assertEqual("https://cmail.tail649edd.ts.net", api_health["public_url"])
                self.assertEqual(str(self.db_path), api_health["db_path"])

                with urllib.request.urlopen(f"{base_url}/api/overview?direction=inbound&include_details=1") as response:
                    overview = json.loads(response.read().decode("utf-8"))
                self.assertEqual(1, overview["contact_count"])
                self.assertEqual(3, overview["message_count"])
                self.assertEqual("healthy", overview["cloudflare_sync"]["status"])
                self.assertEqual(self.latest_communication_id, overview["contacts"][0]["latest_message_id"])
                self.assertEqual("msg-002", overview["messages"][0]["external_id"])
                self.assertEqual("message-id:<msg-002@example.com>", overview["messages"][0]["read_key"])
                self.assertIn("preview image attachment", overview["messages"][0]["search_text"])
                self.assertIn("message-id:<msg-002@example.com>", overview["viewed_message_keys"])
                self.assertEqual(
                    [self.latest_communication_id, self.root_communication_id, self.other_thread_communication_id],
                    overview["contacts"][0]["message_ids"],
                )
                self.assertEqual(2, len(overview["contacts"][0]["threads"]))
                self.assertNotIn(self.outbound_communication_id, [message["id"] for message in overview["messages"]])
                self.assertNotIn(self.manual_communication_id, [message["id"] for message in overview["messages"]])
                self.assertIn(str(self.latest_communication_id), overview["details"])
                self.assertEqual(
                    "This follow-up carries the preview image attachment.",
                    overview["details"][str(self.latest_communication_id)]["body_display"]["primary_text"],
                )

                with urllib.request.urlopen(f"{base_url}/api/communications/{self.latest_communication_id}") as response:
                    detail = json.loads(response.read().decode("utf-8"))
                self.assertEqual("Re: Structured mail test", detail["subject"])
                self.assertEqual("message-id:<msg-002@example.com>", detail["read_key"])
                self.assertEqual("Alice Example <alice@example.com>", detail["external_from"])
                self.assertEqual("Replies Desk <reply@example.com>", detail["drafts"]["reply"]["to"])
                self.assertEqual("Bob Example <bob@example.com>", detail["drafts"]["reply_all"]["cc"])
                self.assertEqual(
                    ["<thread@example.com>", "<msg-001@example.com>", "<msg-002@example.com>"],
                    detail["drafts"]["reply"]["references"],
                )
                self.assertEqual("This follow-up carries the preview image attachment.", detail["body_display"]["primary_text"])
                self.assertFalse(detail["body_display"]["has_quote"])
                self.assertEqual(1, len(detail["attachments"]))
                self.assertEqual(
                    f"/api/attachments/{self.attachment_id}/content",
                    detail["attachments"][0]["preview_url"],
                )
                self.assertEqual("Preview image for the thread.", detail["attachments"][0]["text_preview"])
                self.assertEqual(2, len(detail["thread_messages"]))

                mark_read_request = urllib.request.Request(
                    f"{base_url}/api/messages/read",
                    data=json.dumps({"read_keys": ["read:unit-test-key", "local-id:unit-test"]}).encode("utf-8"),
                    method="POST",
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(mark_read_request) as response:
                    mark_read_payload = json.loads(response.read().decode("utf-8"))
                self.assertTrue(mark_read_payload["ok"])
                self.assertIn("read:unit-test-key", mark_read_payload["viewed_message_keys"])
                with store.open_db(self.db_path) as connection:
                    viewed_keys = store.get_viewed_mail_message_keys(connection)
                self.assertIn("read:unit-test-key", viewed_keys)

                open_request = urllib.request.Request(
                    f"{base_url}/api/contacts/open",
                    data=json.dumps(
                        {
                            "contact_key": "alice@example.com",
                            "opened_at": "2026-04-22T13:00:00Z",
                        }
                    ).encode("utf-8"),
                    method="POST",
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(open_request) as response:
                    open_payload = json.loads(response.read().decode("utf-8"))
                self.assertTrue(open_payload["ok"])
                self.assertEqual("alice@example.com", open_payload["contact_key"])
                self.assertEqual("2026-04-22T13:00:00Z", open_payload["opened_at"])
                self.assertEqual("2026-04-22T13:00:00Z", open_payload["touched_at"])

                with urllib.request.urlopen(f"{base_url}/api/overview?source=correspondence") as response:
                    touched_overview = json.loads(response.read().decode("utf-8"))
                alice_contact = next(
                    contact
                    for contact in touched_overview["contacts"]
                    if contact["contact_key"] == "alice@example.com"
                )
                self.assertEqual("2026-04-22T13:00:00Z", alice_contact["opened_at"])
                self.assertEqual("2026-04-22T13:00:00Z", alice_contact["touched_at"])
                self.assertGreaterEqual(touched_overview["mailbox_version"]["viewed_message_count"], 2)
                self.assertEqual(1, touched_overview["mailbox_version"]["opened_contact_count"])
                self.assertEqual(1, touched_overview["mailbox_version"]["touched_contact_count"])

                with urllib.request.urlopen(f"{base_url}/api/communications/{self.outbound_communication_id}") as response:
                    outbound_detail = json.loads(response.read().decode("utf-8"))
                self.assertEqual("bob@example.com", outbound_detail["drafts"]["reply"]["to"])
                self.assertEqual("bob@example.com", outbound_detail["drafts"]["new_draft"]["to"])

                with urllib.request.urlopen(f"{base_url}/api/attachments/{self.attachment_id}/content") as response:
                    preview_bytes = response.read()
                    content_type = response.headers.get_content_type()
                self.assertEqual("image/png", content_type)
                self.assertEqual(PNG_BYTES, preview_bytes)

                with urllib.request.urlopen(f"{base_url}/api/drafts") as response:
                    drafts_payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(1, len(drafts_payload["drafts"]))
                self.assertEqual(self.draft_communication_id, drafts_payload["drafts"][0]["id"])

                saved_out_of_band = mail_ui.save_cmail_draft(
                    db_path=self.db_path,
                    payload={
                        "subject": "Out-of-band draft update",
                        "to": "alex@example.com",
                        "cc": "",
                        "bcc": "",
                        "body_text": "Saved outside the HTTP handler.",
                    },
                )
                with urllib.request.urlopen(f"{base_url}/api/drafts") as response:
                    refreshed_drafts_payload = json.loads(response.read().decode("utf-8"))
                self.assertTrue(
                    any(int(entry["id"]) == int(saved_out_of_band["id"]) for entry in refreshed_drafts_payload["drafts"])
                )

                with urllib.request.urlopen(f"{base_url}/api/contacts?query=alice@example.com") as response:
                    contacts_payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual("alice@example.com", contacts_payload["contacts"][0]["email"])

                with urllib.request.urlopen(f"{base_url}/api/contacts?query=bob@example.com") as response:
                    contacts_payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual("Bob Example <bob@example.com>", contacts_payload["contacts"][0]["address"])

                request = urllib.request.Request(
                    f"{base_url}/api/drafts",
                    data=json.dumps(
                        {
                            "id": self.draft_communication_id,
                            "subject": "Updated draft to Terence Tao",
                            "to": "tao@example.com",
                            "cc": "",
                            "bcc": "",
                            "body_text": "Professor Tao,\n\nUpdated body.",
                        }
                    ).encode("utf-8"),
                    method="POST",
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(request) as response:
                    saved_payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual("Updated draft to Terence Tao", saved_payload["draft"]["subject"])
                self.assertEqual("tao@example.com", saved_payload["draft"]["to"])
                self.assertIn("https://frg.earth", saved_payload["draft"]["body_text"])
                self.assertIn("(850) 529-8452", saved_payload["draft"]["body_text"])
                self.assertNotIn("https://panhandlefoodtrucks.com", saved_payload["draft"]["body_text"])
                self.assertNotIn("cody@frg.earth", saved_payload["draft"]["body_text"])
                self.assertNotIn("Call:", saved_payload["draft"]["body_text"])
                self.assertNotIn("Text:", saved_payload["draft"]["body_text"])
                self.assertNotIn("https://www.npmjs.com/~sproutseeds", saved_payload["draft"]["body_text"])
                self.assertNotIn("https://github.com/SproutSeeds", saved_payload["draft"]["body_text"])
                with store.open_db(self.db_path) as connection:
                    saved_row = store.get_communication_by_id(connection, self.draft_communication_id)
                    tao_contacts = store.list_mail_contacts(connection, query="tao@example.com", limit=20)
                self.assertIsNotNone(saved_row)
                self.assertIn("frg-bimi-iris-floating.png", str(saved_row["html_body"]))
                self.assertIn("https://frg.earth", str(saved_row["html_body"]))
                self.assertIn("tel:+18505298452", str(saved_row["html_body"]))
                self.assertIn("(850) 529-8452", str(saved_row["html_body"]))
                self.assertNotIn("https://panhandlefoodtrucks.com", str(saved_row["html_body"]))
                self.assertNotIn("mailto:cody@frg.earth", str(saved_row["html_body"]))
                self.assertNotIn("Call:", str(saved_row["html_body"]))
                self.assertNotIn("Text:", str(saved_row["html_body"]))
                self.assertNotIn("https://www.npmjs.com/~sproutseeds", str(saved_row["html_body"]))
                self.assertNotIn("https://github.com/SproutSeeds", str(saved_row["html_body"]))
                self.assertEqual("tao@example.com", str(tao_contacts[0]["email"]))

                with mock.patch(
                    "life_ops.mail_ui.resend_send_email",
                    return_value={
                        "queued": True,
                        "sent": False,
                        "status": "queued",
                        "communication_id": 999,
                        "queue_id": 77,
                        "message_id": "<sent@example.com>",
                    },
                ) as resend_send_mock:
                    request = urllib.request.Request(
                        f"{base_url}/api/drafts/{self.draft_communication_id}/send",
                        data=b"{}",
                        method="POST",
                        headers={"Content-Type": "application/json"},
                    )
                    with urllib.request.urlopen(request) as response:
                        send_payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(self.draft_communication_id, send_payload["draft_id"])
                self.assertEqual("queued", send_payload["draft_status"])
                resend_kwargs = resend_send_mock.call_args.kwargs
                self.assertIn("https://frg.earth", resend_kwargs["text"])
                self.assertIn("(850) 529-8452", resend_kwargs["text"])
                self.assertNotIn("https://panhandlefoodtrucks.com", resend_kwargs["text"])
                self.assertNotIn("cody@frg.earth", resend_kwargs["text"])
                self.assertNotIn("Call:", resend_kwargs["text"])
                self.assertNotIn("Text:", resend_kwargs["text"])
                self.assertNotIn("https://www.npmjs.com/~sproutseeds", resend_kwargs["text"])
                self.assertNotIn("https://github.com/SproutSeeds", resend_kwargs["text"])
                self.assertIn("frg-bimi-iris-floating.png", resend_kwargs["html"])
                self.assertIn("tel:+18505298452", resend_kwargs["html"])
                self.assertIn("(850) 529-8452", resend_kwargs["html"])
                self.assertNotIn("https://panhandlefoodtrucks.com", resend_kwargs["html"])
                self.assertNotIn("mailto:cody@frg.earth", resend_kwargs["html"])
                self.assertNotIn("Call:", resend_kwargs["html"])
                self.assertNotIn("Text:", resend_kwargs["html"])
                self.assertNotIn("https://www.npmjs.com/~sproutseeds", resend_kwargs["html"])
                self.assertNotIn("https://github.com/SproutSeeds", resend_kwargs["html"])
                self.assertIn("color:#111111", resend_kwargs["html"])
                self.assertNotIn("color:#edf2eb", resend_kwargs["html"])
                self.assertFalse(resend_kwargs["apply_signature"])
                self.assertFalse(resend_kwargs["attempt_immediately"])
                with urllib.request.urlopen(f"{base_url}/api/drafts") as response:
                    drafts_payload = json.loads(response.read().decode("utf-8"))
                self.assertFalse(
                    any(int(entry["id"]) == int(self.draft_communication_id) for entry in drafts_payload["drafts"])
                )
                with store.open_db(self.db_path) as connection:
                    sent_draft_row = store.get_communication_by_id(connection, self.draft_communication_id)
                self.assertEqual("deleted", str(sent_draft_row["status"]))
                self.assertTrue(str(sent_draft_row["deleted_at"] or ""))

                request = urllib.request.Request(
                    f"{base_url}/api/drafts",
                    data=json.dumps(
                        {
                            "subject": "Recipient-less draft",
                            "to": "",
                            "cc": "",
                            "bcc": "",
                            "body_text": "Body without recipient",
                        }
                    ).encode("utf-8"),
                    method="POST",
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(request) as response:
                    recipientless_payload = json.loads(response.read().decode("utf-8"))
                recipientless_id = int(recipientless_payload["draft"]["id"])
                request = urllib.request.Request(
                    f"{base_url}/api/drafts/{recipientless_id}/send",
                    data=b"{}",
                    method="POST",
                    headers={"Content-Type": "application/json"},
                )
                with self.assertRaises(urllib.error.HTTPError) as error_context:
                    urllib.request.urlopen(request)
                self.assertEqual(409, error_context.exception.code)
                error_body = error_context.exception.read().decode("utf-8")
                self.assertIn("add a recipient first", error_body)

                request = urllib.request.Request(
                    f"{base_url}/api/drafts/{recipientless_id}/delete",
                    data=b"{}",
                    method="POST",
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(request) as response:
                    deleted_payload = json.loads(response.read().decode("utf-8"))
                self.assertTrue(deleted_payload["deleted"])
                self.assertEqual(recipientless_id, deleted_payload["draft_id"])
                with urllib.request.urlopen(f"{base_url}/api/drafts") as response:
                    drafts_payload = json.loads(response.read().decode("utf-8"))
                self.assertFalse(
                    any(int(entry["id"]) == recipientless_id for entry in drafts_payload["drafts"])
                )
                with store.open_db(self.db_path) as connection:
                    deleted_draft_row = store.get_communication_by_id(connection, recipientless_id)
                self.assertEqual("deleted", str(deleted_draft_row["status"]))
                self.assertTrue(str(deleted_draft_row["deleted_at"] or ""))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_reply_draft_metadata_is_preserved_through_send(self) -> None:
        saved = mail_ui.save_cmail_draft(
            db_path=self.db_path,
            payload={
                "subject": "Re: Structured mail test",
                "to": "Replies Desk <reply@example.com>",
                "cc": "",
                "bcc": "",
                "body_text": "Thanks for the note.",
                "in_reply_to": "<msg-002@example.com>",
                "references": ["<thread@example.com>", "<msg-001@example.com>", "<msg-002@example.com>"],
                "thread_key": "<thread@example.com>",
            },
        )

        self.assertEqual("<msg-002@example.com>", saved["in_reply_to"])
        self.assertEqual(
            ["<thread@example.com>", "<msg-001@example.com>", "<msg-002@example.com>"],
            saved["references"],
        )
        self.assertEqual("<thread@example.com>", saved["thread_key"])
        self.assertEqual("Re: Structured mail test", saved["reply_context"]["subject"])
        self.assertEqual("Alice Example <alice@example.com>", saved["reply_context"]["from"])
        self.assertEqual("<msg-002@example.com>", saved["reply_context"]["message_id"])
        self.assertEqual(3, saved["reply_context"]["reference_count"])
        self.assertIn("preview image attachment", saved["reply_context"]["preview"])

        with mock.patch(
            "life_ops.mail_ui.resend_send_email",
            return_value={"status": "queued", "communication_id": 999},
        ) as resend_send:
            result = mail_ui.send_cmail_draft(
                db_path=self.db_path,
                draft_id=int(saved["id"]),
            )

        self.assertEqual("queued", result["draft_status"])
        resend_send.assert_called_once()
        self.assertEqual("<msg-002@example.com>", resend_send.call_args.kwargs["in_reply_to"])
        self.assertEqual(
            ["<thread@example.com>", "<msg-001@example.com>", "<msg-002@example.com>"],
            resend_send.call_args.kwargs["references"],
        )
        self.assertEqual("<thread@example.com>", resend_send.call_args.kwargs["thread_key"])
        self.assertIn("color:#111111", resend_send.call_args.kwargs["html"])
        self.assertNotIn("color:#edf2eb", resend_send.call_args.kwargs["html"])
        with store.open_db(self.db_path) as connection:
            sent_draft_row = store.get_communication_by_id(connection, int(saved["id"]))
        self.assertEqual("deleted", str(sent_draft_row["status"]))
        self.assertTrue(str(sent_draft_row["deleted_at"] or ""))

    def test_bind_my_soul_reply_identity_is_locked_through_send(self) -> None:
        with store.open_db(self.db_path) as connection:
            source_id = store.upsert_communication_from_sync(
                connection,
                source="cloudflare_email",
                external_id="support-inbound-1",
                subject="Character archive question",
                channel="email",
                happened_at=datetime(2026, 8, 12, 5, 0),
                follow_up_at=None,
                direction="inbound",
                mail_identity="bind_my_soul",
                external_from="Player One <player@example.com>",
                external_to="support@bindmysoul.com",
                message_id="<support-inbound-1@example.com>",
                thread_key="<support-inbound-1@example.com>",
                body_text="Can you help?",
            )

        scaffold = mail_ui.get_cmail_draft_scaffold(
            db_path=self.db_path,
            communication_id=source_id,
            mode="reply",
        )
        self.assertEqual("bind_my_soul", scaffold["mail_identity"])
        self.assertTrue(scaffold["identity_locked"])

        saved = mail_ui.save_cmail_draft(
            db_path=self.db_path,
            payload={
                **scaffold,
                "mail_identity": "frg",
                "body_text": "Absolutely.",
            },
        )
        self.assertEqual("bind_my_soul", saved["mail_identity"])
        self.assertTrue(saved["identity_locked"])
        self.assertIn("Bind My Soul Support", saved["body_text"])
        self.assertNotIn("Cody", saved["body_text"])

        with mock.patch(
            "life_ops.mail_ui.resend_send_email",
            return_value={"status": "queued", "communication_id": 3001},
        ) as resend_send:
            mail_ui.send_cmail_draft(db_path=self.db_path, draft_id=int(saved["id"]))

        kwargs = resend_send.call_args.kwargs
        self.assertEqual("Bind My Soul Support <support@bindmysoul.com>", kwargs["from_email"])
        self.assertEqual("support@bindmysoul.com", kwargs["reply_to"])
        self.assertEqual("bind_my_soul", kwargs["mail_identity"])
        self.assertIsNone(kwargs["config_path"])
        self.assertNotIn("Cody", kwargs["text"] + kwargs["html"])

    def test_deckfound_reply_identity_is_locked_through_send(self) -> None:
        with store.open_db(self.db_path) as connection:
            source_id = store.upsert_communication_from_sync(
                connection,
                source="cloudflare_email",
                external_id="deckfound-inbound-1",
                subject="DeckFound install question",
                channel="email",
                happened_at=datetime(2026, 8, 23, 16, 53),
                follow_up_at=None,
                direction="inbound",
                mail_identity="deckfound",
                external_from="Customer <customer@example.com>",
                external_to="support@deckfound.com",
                message_id="<deckfound-inbound-1@example.com>",
                thread_key="<deckfound-inbound-1@example.com>",
                body_text="Can you help?",
            )

        scaffold = mail_ui.get_cmail_draft_scaffold(
            db_path=self.db_path,
            communication_id=source_id,
            mode="reply",
        )
        self.assertEqual("deckfound", scaffold["mail_identity"])
        self.assertTrue(scaffold["identity_locked"])

        saved = mail_ui.save_cmail_draft(
            db_path=self.db_path,
            payload={
                **scaffold,
                "mail_identity": "frg",
                "body_text": "Absolutely.",
            },
        )
        self.assertEqual("deckfound", saved["mail_identity"])
        self.assertTrue(saved["identity_locked"])
        self.assertIn("DeckFound Support", saved["body_text"])
        self.assertNotIn("Cody", saved["body_text"])

        with mock.patch(
            "life_ops.mail_ui.resend_send_email",
            return_value={"status": "queued", "communication_id": 3002},
        ) as resend_send:
            mail_ui.send_cmail_draft(db_path=self.db_path, draft_id=int(saved["id"]))

        kwargs = resend_send.call_args.kwargs
        self.assertEqual("DeckFound Support <support@deckfound.com>", kwargs["from_email"])
        self.assertEqual("support@deckfound.com", kwargs["reply_to"])
        self.assertEqual("deckfound", kwargs["mail_identity"])
        self.assertIsNone(kwargs["config_path"])
        self.assertNotIn("Cody", kwargs["text"] + kwargs["html"])

    def test_frg_info_reply_identity_is_locked_through_send(self) -> None:
        with store.open_db(self.db_path) as connection:
            source_id = store.upsert_communication_from_sync(
                connection,
                source="cloudflare_email",
                external_id="frg-info-inbound-1",
                subject="General FRG inquiry",
                channel="email",
                happened_at=datetime(2026, 8, 24, 0, 15),
                follow_up_at=None,
                direction="inbound",
                mail_identity="frg_info",
                external_from="Prospective Client <client@example.com>",
                external_to="info@frg.earth",
                message_id="<frg-info-inbound-1@example.com>",
                thread_key="<frg-info-inbound-1@example.com>",
                body_text="Can you tell me more about FRG?",
            )

        scaffold = mail_ui.get_cmail_draft_scaffold(
            db_path=self.db_path,
            communication_id=source_id,
            mode="reply",
        )
        self.assertEqual("frg_info", scaffold["mail_identity"])
        self.assertTrue(scaffold["identity_locked"])

        saved = mail_ui.save_cmail_draft(
            db_path=self.db_path,
            payload={
                **scaffold,
                "mail_identity": "frg",
                "body_text": "Absolutely.",
            },
        )
        self.assertEqual("frg_info", saved["mail_identity"])
        self.assertTrue(saved["identity_locked"])
        self.assertIn("Fractal Research Group LLC", saved["body_text"])

        with mock.patch(
            "life_ops.mail_ui.resend_send_email",
            return_value={"status": "queued", "communication_id": 3003},
        ) as resend_send:
            mail_ui.send_cmail_draft(db_path=self.db_path, draft_id=int(saved["id"]))

        kwargs = resend_send.call_args.kwargs
        self.assertEqual("Fractal Research Group <info@frg.earth>", kwargs["from_email"])
        self.assertEqual("info@frg.earth", kwargs["reply_to"])
        self.assertEqual("frg_info", kwargs["mail_identity"])
        self.assertIsNone(kwargs["config_path"])
        self.assertIn("Cody Mitchell", kwargs["text"] + kwargs["html"])

    def test_send_cmail_draft_forwards_scheduled_at_to_delivery_queue(self) -> None:
        saved = mail_ui.save_cmail_draft(
            db_path=self.db_path,
            payload={
                "subject": "Scheduled draft",
                "to": "friend@example.com",
                "body_text": "Hello later.",
            },
        )

        with mock.patch(
            "life_ops.mail_ui.resend_send_email",
            return_value={"status": "queued", "communication_id": 1002, "next_attempt_at": "2099-01-01T12:00:00Z"},
        ) as resend_send:
            result = mail_ui.send_cmail_draft(
                db_path=self.db_path,
                draft_id=int(saved["id"]),
                scheduled_at="2099-01-01T12:00:00Z",
            )

        self.assertEqual("queued", result["draft_status"])
        self.assertEqual("2099-01-01T12:00:00Z", resend_send.call_args.kwargs["scheduled_at"])

    def test_send_cmail_draft_batch_schedules_drafts_with_default_gap(self) -> None:
        first = mail_ui.save_cmail_draft(
            db_path=self.db_path,
            payload={"subject": "First", "to": "first@example.com", "body_text": "First body."},
        )
        second = mail_ui.save_cmail_draft(
            db_path=self.db_path,
            payload={"subject": "Second", "to": "second@example.com", "body_text": "Second body."},
        )

        with mock.patch(
            "life_ops.mail_ui.resend_send_email",
            side_effect=[
                {"status": "queued", "communication_id": 2001, "next_attempt_at": "2099-01-01T12:00:00Z"},
                {"status": "queued", "communication_id": 2002, "next_attempt_at": "2099-01-01T12:12:00Z"},
            ],
        ) as resend_send:
            result = mail_ui.send_cmail_draft_batch(
                db_path=self.db_path,
                draft_ids=[int(first["id"]), int(second["id"])],
                start_at="2099-01-01T12:00:00Z",
            )

        self.assertEqual(2, result["queued_count"])
        self.assertEqual(12.0, result["effective_gap_minutes"])
        self.assertEqual("2099-01-01T12:00:00Z", resend_send.call_args_list[0].kwargs["scheduled_at"])
        self.assertEqual("2099-01-01T12:12:00Z", resend_send.call_args_list[1].kwargs["scheduled_at"])
        with store.open_db(self.db_path) as connection:
            first_row = store.get_communication_by_id(connection, int(first["id"]))
            second_row = store.get_communication_by_id(connection, int(second["id"]))
        self.assertEqual("deleted", str(first_row["status"]))
        self.assertEqual("deleted", str(second_row["status"]))

    def test_send_cmail_draft_batch_dry_run_keeps_drafts_unsent(self) -> None:
        first = mail_ui.save_cmail_draft(
            db_path=self.db_path,
            payload={"subject": "First", "to": "first@example.com", "body_text": "First body."},
        )
        second = mail_ui.save_cmail_draft(
            db_path=self.db_path,
            payload={"subject": "Second", "to": "second@example.com", "body_text": "Second body."},
        )

        with mock.patch("life_ops.mail_ui.resend_send_email") as resend_send:
            result = mail_ui.send_cmail_draft_batch(
                db_path=self.db_path,
                draft_ids=[int(first["id"]), int(second["id"])],
                start_at="2099-01-01T12:00:00Z",
                dry_run=True,
            )

        resend_send.assert_not_called()
        self.assertTrue(result["dry_run"])
        self.assertEqual(0, result["queued_count"])
        self.assertEqual(2, result["draft_count"])
        self.assertEqual(["2099-01-01T12:00:00Z", "2099-01-01T12:12:00Z"], [item["scheduled_at"] for item in result["scheduled"]])
        with store.open_db(self.db_path) as connection:
            first_row = store.get_communication_by_id(connection, int(first["id"]))
            second_row = store.get_communication_by_id(connection, int(second["id"]))
        self.assertEqual("draft", str(first_row["status"]))
        self.assertEqual("draft", str(second_row["status"]))

    def test_send_cmail_draft_rebuilds_email_safe_html_body_from_body_text(self) -> None:
        saved = mail_ui.save_cmail_draft(
            db_path=self.db_path,
            payload={
                "subject": "Legacy styled draft",
                "to": "friend@example.com",
                "body_text": "Hello there.",
            },
        )
        with store.open_db(self.db_path) as connection:
            connection.execute(
                "UPDATE communications SET html_body = ? WHERE id = ?",
                (
                    '<p style="color:#edf2eb">Hello there.</p><div style="color:#edf2eb">Old preview signature</div>',
                    int(saved["id"]),
                ),
            )

        with mock.patch(
            "life_ops.mail_ui.resend_send_email",
            return_value={"status": "queued", "communication_id": 1001},
        ) as resend_send:
            result = mail_ui.send_cmail_draft(
                db_path=self.db_path,
                draft_id=int(saved["id"]),
            )

        self.assertEqual("queued", result["draft_status"])
        self.assertIn("color:#111111", resend_send.call_args.kwargs["html"])
        self.assertNotIn("color:#edf2eb", resend_send.call_args.kwargs["html"])

    def test_draft_attachments_can_be_uploaded_downloaded_and_sent(self) -> None:
        handler = mail_ui._make_handler(db_path=self.db_path, limit=20)
        server = HTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base_url = f"http://127.0.0.1:{server.server_port}"
        attachment_bytes = b"npm i -g erdos-problems\n"
        try:
            with mock.patch(
                "life_ops.mail_ui.store.attachment_vault_root",
                return_value=self.vault_root,
            ):
                upload_request = urllib.request.Request(
                    f"{base_url}/api/drafts/{self.draft_communication_id}/attachments",
                    data=json.dumps(
                        {
                            "attachments": [
                                {
                                    "filename": "release-note.txt",
                                    "mime_type": "text/plain",
                                    "content_base64": base64.b64encode(attachment_bytes).decode("ascii"),
                                }
                            ]
                        }
                    ).encode("utf-8"),
                    method="POST",
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(upload_request) as response:
                    upload_payload = json.loads(response.read().decode("utf-8"))
                self.assertEqual(1, len(upload_payload["draft"]["attachments"]))
                attachment = upload_payload["draft"]["attachments"][0]
                self.assertEqual("release-note.txt", attachment["filename"])
                self.assertEqual("text/plain", attachment["mime_type"])
                self.assertTrue(attachment["download_url"])

                with urllib.request.urlopen(f"{base_url}{attachment['download_url']}") as response:
                    downloaded_bytes = response.read()
                    content_type = response.headers.get_content_type()
                    content_disposition = response.headers.get("Content-Disposition") or ""
                self.assertEqual("text/plain", content_type)
                self.assertEqual(attachment_bytes, downloaded_bytes)
                self.assertIn('filename="release-note.txt"', content_disposition)

                with mock.patch(
                    "life_ops.mail_ui.resend_send_email",
                    return_value={"status": "queued", "communication_id": 999},
                ) as resend_send:
                    result = mail_ui.send_cmail_draft(
                        db_path=self.db_path,
                        draft_id=self.draft_communication_id,
                    )
                self.assertEqual("queued", result["draft_status"])
                attachment_paths = resend_send.call_args.kwargs["attachment_paths"]
                self.assertEqual(1, len(attachment_paths))
                self.assertEqual("release-note.txt", Path(attachment_paths[0]).name)
                with store.open_db(self.db_path) as connection:
                    sent_draft_row = store.get_communication_by_id(connection, self.draft_communication_id)
                self.assertEqual("deleted", str(sent_draft_row["status"]))
                self.assertTrue(str(sent_draft_row["deleted_at"] or ""))
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_cleanup_superseded_cmail_drafts_hides_ghost_queued_rows(self) -> None:
        with store.open_db(self.db_path) as connection:
            draft_row = store.get_communication_by_id(connection, self.draft_communication_id)
            self.assertIsNotNone(draft_row)
            connection.execute(
                """
                UPDATE communications
                SET status = 'queued',
                    external_to = 'hmm10@pitt.edu,ninamorse@pitt.edu'
                WHERE id = ?
                """,
                (self.draft_communication_id,),
            )
            connection.execute(
                """
                INSERT INTO communications (
                    subject, channel, direction, person, happened_at, status, notes, source,
                    external_from, external_to, external_cc, external_bcc, body_text, html_body, snippet,
                    message_id, in_reply_to, references_json, thread_key
                ) VALUES (?, 'email', 'outbound', '', ?, 'sent', '', ?, ?, ?, '', '', ?, ?, ?, ?, ?, '[]', ?)
                """,
                (
                    str(draft_row["subject"] or ""),
                    "2026-04-09T15:46:19Z",
                    mail_ui.DEFAULT_MAIL_UI_OUTBOUND_SOURCE,
                    "Cody <cody@frg.earth>",
                    "hmm10@pitt.edu, ninamorse@pitt.edu",
                    str(draft_row["body_text"] or ""),
                    str(draft_row["html_body"] or ""),
                    str(draft_row["snippet"] or ""),
                    "<lifeops-superseded@example.com>",
                    str(draft_row["in_reply_to"] or ""),
                    str(draft_row["thread_key"] or ""),
                ),
            )
            connection.commit()

        cleaned_ids = mail_ui._cleanup_superseded_cmail_drafts(self.db_path)
        self.assertEqual([self.draft_communication_id], cleaned_ids)

        with store.open_db(self.db_path) as connection:
            cleaned_row = store.get_communication_by_id(connection, self.draft_communication_id)
        self.assertEqual("deleted", str(cleaned_row["status"]))
        self.assertTrue(str(cleaned_row["deleted_at"] or ""))

    def test_active_cmail_drafts_do_not_keep_stale_deleted_marker(self) -> None:
        with store.open_db(self.db_path) as connection:
            connection.execute(
                "UPDATE communications SET deleted_at = ? WHERE id = ?",
                ("2026-04-10T05:41:39Z", self.draft_communication_id),
            )
            connection.commit()

        restored_ids = mail_ui._cleanup_active_cmail_draft_deleted_markers(self.db_path)
        self.assertEqual([self.draft_communication_id], restored_ids)
        with store.open_db(self.db_path) as connection:
            restored_row = store.get_communication_by_id(connection, self.draft_communication_id)
        self.assertEqual("", str(restored_row["deleted_at"] or ""))

        with store.open_db(self.db_path) as connection:
            connection.execute(
                "UPDATE communications SET deleted_at = ? WHERE id = ?",
                ("2026-04-10T05:41:39Z", self.draft_communication_id),
            )
            connection.commit()
        mail_ui.save_cmail_draft(
            db_path=self.db_path,
            payload={
                "id": self.draft_communication_id,
                "subject": "Still active",
                "to": "friend@example.com",
                "body_text": "Keep this draft.",
            },
        )
        with store.open_db(self.db_path) as connection:
            saved_row = store.get_communication_by_id(connection, self.draft_communication_id)
        self.assertEqual("draft", str(saved_row["status"] or ""))
        self.assertEqual("", str(saved_row["deleted_at"] or ""))

    def test_delete_actions_hide_message_and_contact_from_inbox(self) -> None:
        handler = mail_ui._make_handler(db_path=self.db_path, limit=20)
        server = HTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base_url = f"http://127.0.0.1:{server.server_port}"
        try:
            request = urllib.request.Request(
                f"{base_url}/api/communications/{self.latest_communication_id}/delete",
                data=b"{}",
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(request) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertTrue(payload["deleted"])
            self.assertEqual(30, payload["archived_for_days"])

            with urllib.request.urlopen(f"{base_url}/api/overview?direction=inbound") as response:
                overview = json.loads(response.read().decode("utf-8"))
            self.assertEqual(2, overview["message_count"])
            self.assertEqual(1, overview["contact_count"])
            self.assertEqual(self.root_communication_id, overview["messages"][0]["id"])
            with store.open_db(self.db_path) as connection:
                row = store.get_communication_by_id(connection, self.latest_communication_id)
            self.assertEqual("deleted", row["status"])
            self.assertTrue(row["deleted_at"])

            request = urllib.request.Request(
                f"{base_url}/api/contacts/delete",
                data=json.dumps({"contact_key": "alice@example.com"}).encode("utf-8"),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(request) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual(2, payload["deleted_count"])
            self.assertEqual(30, payload["archived_for_days"])

            with urllib.request.urlopen(f"{base_url}/api/overview?direction=inbound") as response:
                overview = json.loads(response.read().decode("utf-8"))
            self.assertEqual(0, overview["message_count"])
            self.assertEqual(0, overview["contact_count"])
            with store.open_db(self.db_path) as connection:
                row = store.get_communication_by_id(connection, self.root_communication_id)
            self.assertEqual("deleted", row["status"])
            self.assertTrue(row["deleted_at"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    def test_contact_delete_hides_current_messages_but_later_mail_can_reappear(self) -> None:
        with store.open_db(self.db_path) as connection:
            deleted_count = store.set_communications_status(
                connection,
                communication_ids=[
                    self.root_communication_id,
                    self.latest_communication_id,
                    self.other_thread_communication_id,
                ],
                status="deleted",
            )
        self.assertEqual(3, deleted_count)

        hidden_payload = mail_ui.build_mail_ui_overview(
            db_path=self.db_path,
            direction="inbound",
            limit=10,
        )
        self.assertEqual(0, hidden_payload["contact_count"])
        self.assertEqual(0, hidden_payload["message_count"])

        with store.open_db(self.db_path) as connection:
            store.upsert_communication_from_sync(
                connection,
                source="cloudflare_email",
                external_id="msg-006",
                subject="Fresh note after archive",
                channel="email",
                happened_at=datetime(2026, 3, 30, 9, 45, 0),
                follow_up_at=None,
                direction="inbound",
                person="Alice Example",
                organization_name="FRG",
                status="reference",
                external_thread_id="<thread-3@example.com>",
                external_from="Alice Example <alice@example.com>",
                external_to="cody@frg.earth",
                message_id="<msg-006@example.com>",
                thread_key="<thread-3@example.com>",
                snippet="Fresh note after archive",
                body_text="New message should restore the contact to the inbox.",
                category="research",
                priority_level="normal",
                priority_score=60,
            )

        restored_payload = mail_ui.build_mail_ui_overview(
            db_path=self.db_path,
            direction="inbound",
            limit=10,
        )
        self.assertEqual(1, restored_payload["contact_count"])
        self.assertEqual(1, restored_payload["message_count"])
        self.assertEqual("alice@example.com", restored_payload["contacts"][0]["contact_key"])
        self.assertEqual("Fresh note after archive", restored_payload["messages"][0]["subject"])

    def test_snapshot_cache_returns_last_good_connection_when_reload_hits_lock_timeout(self) -> None:
        cache = mail_ui._MailUiSnapshotCache(db_path=self.db_path)
        first_connection = cache.get_connection()
        first_row = first_connection.execute("SELECT COUNT(*) AS c FROM communications").fetchone()
        first_connection.close()

        with mock.patch.object(
            cache,
            "_current_stamp",
            side_effect=[("manifest", 1, 1), ("manifest", 2, 1)],
        ), mock.patch.object(
            cache,
            "_load_snapshot_bytes",
            side_effect=TimeoutError("timed out waiting for DB lock"),
        ):
            second_connection = cache.get_connection()
            second_row = second_connection.execute("SELECT COUNT(*) AS c FROM communications").fetchone()
            second_connection.close()

        self.assertEqual(6, int(first_row["c"]))
        self.assertEqual(6, int(second_row["c"]))

    def test_body_display_splits_latest_note_from_quoted_history(self) -> None:
        payload = mail_ui._body_display(
            "dude that is a nice email On Sat, Mar 28, 2026 at 8:49 AM Cody <cody@frg.earth> wrote: > Professor Tao, > > I found my way into this area through Erdős problem 857 on > erdosproblems.com, and I wanted to say that I’m genuinely grateful",
        )

        self.assertEqual("dude that is a nice email", payload["primary_text"])
        self.assertTrue(payload["has_quote"])
        self.assertIn("On Sat, Mar 28, 2026 at 8:49 AM Cody <cody@frg.earth> wrote:", payload["quoted_header"])
        self.assertIn("Professor Tao,", payload["quoted_text"])
        self.assertIn("erdosproblems.com", payload["quoted_text"])


if __name__ == "__main__":
    unittest.main()
