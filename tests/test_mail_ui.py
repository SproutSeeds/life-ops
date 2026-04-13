from __future__ import annotations

import base64
import json
import os
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

    def test_compose_cmail_html_body_uses_email_safe_colors(self) -> None:
        html = mail_ui._compose_cmail_html_body("Hello there.")

        self.assertIn("color:#111111", html)
        self.assertIn("https://github.com/SproutSeeds", html)
        self.assertNotIn("color:#edf2eb", html)

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
        self.assertNotIn(self.manual_communication_id, [message["id"] for message in payload["messages"]])

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
                self.assertIn('rel="icon" href="/static/favicon.svg"', html)
                self.assertIn('property="og:image" content="/static/og-image.svg"', html)
                self.assertIn('id="correspondenceSearch"', html)
                self.assertIn('placeholder="search name or email"', html)
                self.assertIn('data-reply-message="${message.id}"', html)
                self.assertIn("lifeops.mail.viewedMessageIds", html)
                self.assertIn(".unread-orb", html)
                self.assertIn("function displayedContacts(contacts)", html)
                self.assertIn('messageIsUnread(entry) ? "● " : ""', html)
                self.assertIn("function messageReadKeys(message)", html)
                self.assertNotIn("all sources", html)
                self.assertNotIn("quick drafts", html)
                self.assertNotIn("__MAIL_UI_CLIENT_REFRESH_INTERVAL_MS__", html)

                with urllib.request.urlopen(f"{base_url}/static/favicon.svg") as response:
                    favicon_bytes = response.read()
                    favicon_type = response.headers.get_content_type()
                self.assertEqual("image/svg+xml", favicon_type)
                self.assertIn(b"<svg", favicon_bytes)

                with urllib.request.urlopen(f"{base_url}/api/overview?direction=inbound&include_details=1") as response:
                    overview = json.loads(response.read().decode("utf-8"))
                self.assertEqual(1, overview["contact_count"])
                self.assertEqual(3, overview["message_count"])
                self.assertEqual("healthy", overview["cloudflare_sync"]["status"])
                self.assertEqual(self.latest_communication_id, overview["contacts"][0]["latest_message_id"])
                self.assertEqual("msg-002", overview["messages"][0]["external_id"])
                self.assertEqual("message-id:<msg-002@example.com>", overview["messages"][0]["read_key"])
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
                self.assertIn("https://www.npmjs.com/~sproutseeds", saved_payload["draft"]["body_text"])
                self.assertIn("https://github.com/SproutSeeds", saved_payload["draft"]["body_text"])
                with store.open_db(self.db_path) as connection:
                    saved_row = store.get_communication_by_id(connection, self.draft_communication_id)
                    tao_contacts = store.list_mail_contacts(connection, query="tao@example.com", limit=20)
                self.assertIsNotNone(saved_row)
                self.assertIn("frg-bimi-iris-floating.png", str(saved_row["html_body"]))
                self.assertIn("https://frg.earth", str(saved_row["html_body"]))
                self.assertIn("https://www.npmjs.com/~sproutseeds", str(saved_row["html_body"]))
                self.assertIn("https://github.com/SproutSeeds", str(saved_row["html_body"]))
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
                self.assertIn("https://www.npmjs.com/~sproutseeds", resend_kwargs["text"])
                self.assertIn("https://github.com/SproutSeeds", resend_kwargs["text"])
                self.assertIn("frg-bimi-iris-floating.png", resend_kwargs["html"])
                self.assertIn("https://www.npmjs.com/~sproutseeds", resend_kwargs["html"])
                self.assertIn("https://github.com/SproutSeeds", resend_kwargs["html"])
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
