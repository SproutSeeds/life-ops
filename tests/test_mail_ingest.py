from __future__ import annotations

import base64
import json
import sys
import tempfile
import unittest
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from unittest import mock
import os

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops import mail_ingest
from life_ops import mail_vault
from life_ops import store
from life_ops import vault_crypto


class MailIngestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "life_ops.db"
        self.original_master_key = os.environ.get(vault_crypto.MASTER_KEY_NAME)
        os.environ[vault_crypto.MASTER_KEY_NAME] = vault_crypto._b64url_encode(b"a" * 32)

    def tearDown(self) -> None:
        if self.original_master_key is None:
            os.environ.pop(vault_crypto.MASTER_KEY_NAME, None)
        else:
            os.environ[vault_crypto.MASTER_KEY_NAME] = self.original_master_key
        self.temp_dir.cleanup()

    def _payload(self) -> dict:
        message = EmailMessage()
        message["From"] = "Alice Example <alice@example.com>"
        message["To"] = "cody@frg.earth"
        message["Subject"] = "Action required: review this draft"
        message["Message-ID"] = "<msg-123@example.com>"
        message.set_content("Please review this draft and reply today.")
        message.add_attachment(
            b"hello world",
            maintype="application",
            subtype="pdf",
            filename="draft.pdf",
        )
        raw_bytes = message.as_bytes()
        return {
            "provider": "cloudflare-email-routing",
            "envelope_from": "alice@example.com",
            "envelope_to": "cody@frg.earth",
            "raw_base64": base64.b64encode(raw_bytes).decode("ascii"),
            "raw_size": len(raw_bytes),
        }

    def test_mail_ingest_status_reflects_secret_presence(self) -> None:
        with mock.patch("life_ops.mail_ingest.credentials.resolve_secret", return_value="secret"):
            status = mail_ingest.mail_ingest_status(db_path=self.db_path)

        self.assertTrue(status["secret_present"])
        self.assertTrue(status["ready"])

    def test_generate_mail_ingest_secret_stores_named_secret(self) -> None:
        with mock.patch("life_ops.mail_ingest.credentials.set_secret", return_value={"backend": "file"}) as set_mock:
            result = mail_ingest.generate_mail_ingest_secret()

        self.assertEqual(mail_ingest.MAIL_INGEST_SECRET_NAME, result["secret_name"])
        self.assertTrue(result["generated"])
        self.assertEqual(mail_ingest.MAIL_INGEST_SECRET_NAME, set_mock.call_args.kwargs["name"])
        self.assertTrue(set_mock.call_args.kwargs["value"])

    def test_ingest_cloudflare_email_payload_stores_communication(self) -> None:
        with mock.patch("life_ops.mail_ingest.extract_text_from_saved_attachment", return_value=("", "")):
            result = mail_ingest.ingest_cloudflare_email_payload(self._payload(), db_path=self.db_path)

        self.assertEqual("cloudflare_email", result["source"])
        self.assertEqual("reference", result["status"])
        self.assertEqual(1, result["attachments_count"])

        with store.open_db(self.db_path) as connection:
            row = store.get_communication_by_id(connection, int(result["communication_id"]))
            attachment_rows = store.list_communication_attachments(connection, communication_id=int(result["communication_id"]))

        self.assertIsNotNone(row)
        assert row is not None
        self.assertEqual("cloudflare_email", row["source"])
        self.assertEqual("Action required: review this draft", row["subject"])
        self.assertEqual("reference", row["status"])
        self.assertEqual("Alice Example", row["person"])
        self.assertIn("reply today", row["body_text"])
        self.assertTrue(row["raw_relative_path"])
        self.assertTrue(row["raw_sha256"])
        self.assertEqual("inbound", row["direction"])
        self.assertEqual("Alice Example <alice@example.com>", row["external_from"])
        self.assertEqual("cody@frg.earth", row["external_to"])
        self.assertEqual("<msg-123@example.com>", row["message_id"])
        self.assertEqual("<msg-123@example.com>", row["thread_key"])
        self.assertEqual([], json.loads(row["references_json"]))
        self.assertEqual([{"name": "", "email": "cody@frg.earth"}], json.loads(row["to_json"]))
        attachments = json.loads(row["attachments_json"])
        self.assertEqual("draft.pdf", attachments[0]["filename"])

        self.assertEqual(1, len(attachment_rows))
        self.assertEqual("draft.pdf", attachment_rows[0]["filename"])
        self.assertEqual("application/pdf", attachment_rows[0]["mime_type"])
        self.assertTrue((store.attachment_vault_root() / row["raw_relative_path"]).exists())
        self.assertTrue((store.attachment_vault_root() / attachment_rows[0]["relative_path"]).exists())
        self.assertTrue(str(row["raw_relative_path"]).endswith(".enc.json"))
        self.assertTrue(str(attachment_rows[0]["relative_path"]).endswith(".enc.json"))
        raw_bytes = mail_vault.read_encrypted_vault_file(
            vault_root=store.attachment_vault_root(),
            relative_path=str(row["raw_relative_path"]),
        )
        self.assertIn(b"Action required: review this draft", raw_bytes)

    def test_ingest_cloudflare_email_payload_preserves_inline_image_attachments(self) -> None:
        message = EmailMessage()
        message["From"] = "Alice Example <alice@example.com>"
        message["To"] = "cody@frg.earth"
        message["Subject"] = "Inline image test"
        message["Message-ID"] = "<msg-inline@example.com>"
        message.set_content("HTML version below.")
        message.add_alternative('<p>Hello there <img src="cid:chart-image"></p>', subtype="html")
        html_part = message.get_payload()[-1]
        html_part.add_related(
            b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff!\xf9\x04\x01\x00\x00\x00\x00,\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02D\x01\x00;",
            maintype="image",
            subtype="gif",
            cid="<chart-image>",
        )
        payload = {
            "provider": "cloudflare-email-routing",
            "envelope_from": "alice@example.com",
            "envelope_to": "cody@frg.earth",
            "raw_base64": base64.b64encode(message.as_bytes()).decode("ascii"),
            "raw_size": len(message.as_bytes()),
        }

        with mock.patch("life_ops.mail_ingest.extract_text_from_saved_attachment", return_value=("", "")):
            result = mail_ingest.ingest_cloudflare_email_payload(payload, db_path=self.db_path)

        with store.open_db(self.db_path) as connection:
            row = store.get_communication_by_id(connection, int(result["communication_id"]))
            attachment_rows = store.list_communication_attachments(connection, communication_id=int(result["communication_id"]))

        assert row is not None
        attachments = json.loads(row["attachments_json"])
        self.assertEqual(1, len(attachments))
        self.assertTrue(attachments[0]["inline"])
        self.assertEqual("chart-image", attachments[0]["content_id"])
        self.assertEqual("image/gif", attachments[0]["mime_type"])
        self.assertEqual("<msg-inline@example.com>", row["message_id"])
        self.assertEqual("<msg-inline@example.com>", row["thread_key"])
        self.assertEqual(1, len(attachment_rows))
        self.assertTrue(attachment_rows[0]["filename"].endswith(".gif"))
        self.assertTrue((store.attachment_vault_root() / attachment_rows[0]["relative_path"]).exists())
        self.assertTrue(str(attachment_rows[0]["relative_path"]).endswith(".enc.json"))

    def test_ingest_cloudflare_email_payload_tracks_reply_metadata(self) -> None:
        message = EmailMessage()
        message["From"] = "Alice Example <alice@example.com>"
        message["To"] = "cody@frg.earth"
        message["Cc"] = "Bob Example <bob@example.com>"
        message["Reply-To"] = "Replies <reply@example.com>"
        message["Subject"] = "Re: Threaded note"
        message["Message-ID"] = "<msg-reply@example.com>"
        message["In-Reply-To"] = "<root@example.com>"
        message["References"] = "<root@example.com> <mid@example.com>"
        message.set_content("Thread reply body.")
        payload = {
            "provider": "cloudflare-email-routing",
            "envelope_from": "alice@example.com",
            "envelope_to": "cody@frg.earth",
            "raw_base64": base64.b64encode(message.as_bytes()).decode("ascii"),
            "raw_size": len(message.as_bytes()),
        }

        with mock.patch("life_ops.mail_ingest.extract_text_from_saved_attachment", return_value=("", "")):
            result = mail_ingest.ingest_cloudflare_email_payload(payload, db_path=self.db_path)

        with store.open_db(self.db_path) as connection:
            row = store.get_communication_by_id(connection, int(result["communication_id"]))

        assert row is not None
        self.assertEqual("<msg-reply@example.com>", row["message_id"])
        self.assertEqual("<root@example.com>", row["in_reply_to"])
        self.assertEqual("<root@example.com>", row["thread_key"])
        self.assertEqual("<root@example.com>", row["external_thread_id"])
        self.assertEqual("Bob Example <bob@example.com>", row["external_cc"])
        self.assertEqual("Replies <reply@example.com>", row["external_reply_to"])
        self.assertEqual(
            [{"name": "Bob Example", "email": "bob@example.com"}],
            json.loads(row["cc_json"]),
        )
        self.assertEqual(
            [{"name": "Replies", "email": "reply@example.com"}],
            json.loads(row["reply_to_json"]),
        )
        self.assertEqual(
            ["<root@example.com>", "<mid@example.com>"],
            json.loads(row["references_json"]),
        )

    def test_ingest_cloudflare_email_payload_uses_operational_header_allowlist_by_default(self) -> None:
        message = EmailMessage()
        message["From"] = "Alice Example <alice@example.com>"
        message["To"] = "cody@frg.earth"
        message["Subject"] = "Header minimization"
        message["Message-ID"] = "<msg-header@example.com>"
        message["X-Custom-Secret"] = "should-not-persist"
        message["Received"] = "from mx.example.net by frg.earth"
        message.set_content("hello")
        payload = {
            "provider": "cloudflare-email-routing",
            "envelope_from": "alice@example.com",
            "envelope_to": "cody@frg.earth",
            "raw_base64": base64.b64encode(message.as_bytes()).decode("ascii"),
            "raw_size": len(message.as_bytes()),
        }

        with mock.patch("life_ops.mail_ingest.extract_text_from_saved_attachment", return_value=("", "")):
            result = mail_ingest.ingest_cloudflare_email_payload(payload, db_path=self.db_path)

        with store.open_db(self.db_path) as connection:
            row = store.get_communication_by_id(connection, int(result["communication_id"]))

        assert row is not None
        headers = json.loads(row["headers_json"])
        self.assertIn("Message-ID", headers)
        self.assertNotIn("X-Custom-Secret", headers)
        self.assertNotIn("Received", headers)

    def test_sign_and_verify_mail_ingest_payload(self) -> None:
        body = b'{"hello":"world"}'
        timestamp = "2026-03-27T09:30:00Z"
        signature = mail_ingest.sign_mail_ingest_payload(
            body_bytes=body,
            secret="secret-value",
            timestamp=timestamp,
        )

        verified, reason = mail_ingest.verify_mail_ingest_signature(
            body_bytes=body,
            secret="secret-value",
            timestamp=timestamp,
            signature=signature,
            now=datetime(2026, 3, 27, 9, 30, 30),
        )

        self.assertTrue(verified)
        self.assertEqual("ok", reason)

    def test_verify_mail_ingest_signature_rejects_invalid_signature(self) -> None:
        verified, reason = mail_ingest.verify_mail_ingest_signature(
            body_bytes=b'{"hello":"world"}',
            secret="secret-value",
            timestamp="2026-03-27T09:30:00Z",
            signature="sha256=deadbeef",
            now=datetime(2026, 3, 27, 9, 30, 30),
        )

        self.assertFalse(verified)
        self.assertEqual("invalid_signature", reason)

    def test_verify_mail_ingest_signature_rejects_stale_timestamp(self) -> None:
        body = b'{"hello":"world"}'
        timestamp = "2026-03-27T09:30:00Z"
        signature = mail_ingest.sign_mail_ingest_payload(
            body_bytes=body,
            secret="secret-value",
            timestamp=timestamp,
        )

        verified, reason = mail_ingest.verify_mail_ingest_signature(
            body_bytes=body,
            secret="secret-value",
            timestamp=timestamp,
            signature=signature,
            now=datetime(2026, 3, 27, 9, 40, 1),
            max_skew_seconds=300,
        )

        self.assertFalse(verified)
        self.assertEqual("stale_timestamp", reason)


if __name__ == "__main__":
    unittest.main()
