from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops import resend_integration
from life_ops import store


class ResendIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = Path(self.temp_dir.name) / "resend.json"
        self.db_path = Path(self.temp_dir.name) / "life_ops.db"
        self.sample_attachment_path = Path(self.temp_dir.name) / "sample.pdf"
        self.sample_attachment_path.write_bytes(b"%PDF-1.4 sample")
        self.inline_image_path = Path(self.temp_dir.name) / "logo.png"
        self.inline_image_path.write_bytes(b"\x89PNG\r\n\x1a\nsample")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write_config(
        self,
        *,
        token: str = "",
        token_env: str = "RESEND_API_KEY",
        signature_text: str = "",
        signature_html: str = "",
    ) -> None:
        self.config_path.write_text(
            json.dumps(
                {
                    "api_base_url": resend_integration.RESEND_API_BASE_URL,
                    "api_key": token,
                    "api_key_env": token_env,
                    "default_from": "Cody <cody@frg.earth>",
                    "default_reply_to": "cody@frg.earth",
                    "default_signature_text": signature_text,
                    "default_signature_html": signature_html,
                }
            )
        )

    def test_write_resend_config_template(self) -> None:
        result = resend_integration.write_resend_config_template(self.config_path)

        self.assertTrue(result["created"])
        payload = json.loads(self.config_path.read_text())
        self.assertEqual("RESEND_API_KEY", payload["api_key_env"])

    def test_resend_status_reports_missing_setup(self) -> None:
        status = resend_integration.resend_status(config_path=self.config_path)

        self.assertFalse(status["config_present"])
        self.assertFalse(status["ready"])
        self.assertTrue(status["next_steps"])

    def test_resend_list_domains_uses_bearer_auth(self) -> None:
        self._write_config(token="resend-token")

        class _FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return json.dumps({"data": [{"name": "frg.earth"}]}).encode("utf-8")

        def _fake_urlopen(req, timeout=0):
            self.assertEqual("Bearer resend-token", req.headers["Authorization"])
            user_agent = req.headers.get("User-Agent") or req.headers.get("User-agent")
            self.assertEqual(resend_integration.RESEND_USER_AGENT, user_agent)
            self.assertTrue(req.full_url.endswith("/domains"))
            return _FakeResponse()

        with mock.patch("life_ops.resend_integration.request.urlopen", side_effect=_fake_urlopen):
            payload = resend_integration.resend_list_domains(config_path=self.config_path)

        self.assertEqual("frg.earth", payload["data"][0]["name"])

    def test_resend_status_includes_domain_names(self) -> None:
        self._write_config()
        with mock.patch("life_ops.resend_integration.credentials.resolve_secret", return_value="resend-token"):
            with mock.patch(
                "life_ops.resend_integration.resend_list_domains",
                return_value={
                    "data": [
                        {
                            "name": "frg.earth",
                            "status": "verified",
                            "capabilities": {"sending": "enabled"},
                        }
                    ]
                },
            ):
                status = resend_integration.resend_status(config_path=self.config_path)

        self.assertTrue(status["api_key_present"])
        self.assertTrue(status["ready"])
        self.assertTrue(status["sender_domain_ready"])
        self.assertEqual(["frg.earth"], status["domains"]["names"])
        self.assertFalse(status["default_signature_text_present"])
        self.assertFalse(status["default_signature_html_present"])

    def test_resend_send_email_posts_expected_payload(self) -> None:
        self._write_config(token="resend-token")
        with mock.patch(
            "life_ops.resend_integration._resend_request_json",
            return_value={"id": "email-123"},
        ) as request_mock, mock.patch(
            "life_ops.resend_integration._queue_outbound_mail_artifacts",
            return_value={"raw_relative_path": "", "raw_sha256": "", "saved_attachment_ids": [], "stored_attachments": []},
        ), mock.patch(
            "life_ops.resend_integration._save_delivery_receipt_manifest",
            return_value={"raw_relative_path": "", "raw_sha256": ""},
        ):
            payload = resend_integration.resend_send_email(
                to=["friend@example.com"],
                subject="Hello",
                text="Test email",
                journal_db_path=self.db_path,
                config_path=self.config_path,
            )

        self.assertEqual("email-123", payload["id"])
        sent = request_mock.call_args.kwargs["payload"]
        self.assertEqual("Cody <cody@frg.earth>", sent["from"])
        self.assertEqual(["friend@example.com"], sent["to"])
        self.assertEqual(["cody@frg.earth"], sent["reply_to"])
        self.assertTrue(sent["headers"]["Message-ID"].startswith("<lifeops-"))

    def test_resend_send_email_appends_saved_signature(self) -> None:
        self._write_config(
            token="resend-token",
            signature_text="Cody Mitchell\nFractal Research Group\ncody@frg.earth",
        )
        with mock.patch(
            "life_ops.resend_integration._resend_request_json",
            return_value={"id": "email-456"},
        ) as request_mock, mock.patch(
            "life_ops.resend_integration._queue_outbound_mail_artifacts",
            return_value={"raw_relative_path": "", "raw_sha256": "", "saved_attachment_ids": [], "stored_attachments": []},
        ), mock.patch(
            "life_ops.resend_integration._save_delivery_receipt_manifest",
            return_value={"raw_relative_path": "", "raw_sha256": ""},
        ):
            resend_integration.resend_send_email(
                to=["friend@example.com"],
                subject="Hello",
                text="Test email",
                journal_db_path=self.db_path,
                config_path=self.config_path,
            )

        sent = request_mock.call_args.kwargs["payload"]
        self.assertEqual(
            "Test email\n\nCody Mitchell\nFractal Research Group\ncody@frg.earth",
            sent["text"],
        )

    def test_resend_send_email_can_skip_saved_signature(self) -> None:
        self._write_config(
            token="resend-token",
            signature_text="Cody Mitchell\nFractal Research Group\ncody@frg.earth",
        )
        with mock.patch(
            "life_ops.resend_integration._resend_request_json",
            return_value={"id": "email-789"},
        ) as request_mock, mock.patch(
            "life_ops.resend_integration._queue_outbound_mail_artifacts",
            return_value={"raw_relative_path": "", "raw_sha256": "", "saved_attachment_ids": [], "stored_attachments": []},
        ), mock.patch(
            "life_ops.resend_integration._save_delivery_receipt_manifest",
            return_value={"raw_relative_path": "", "raw_sha256": ""},
        ):
            resend_integration.resend_send_email(
                to=["friend@example.com"],
                subject="Hello",
                text="Test email",
                apply_signature=False,
                journal_db_path=self.db_path,
                config_path=self.config_path,
            )

        sent = request_mock.call_args.kwargs["payload"]
        self.assertEqual("Test email", sent["text"])

    def test_resend_signature_set_and_show_round_trip(self) -> None:
        self._write_config(token="resend-token")

        update = resend_integration.resend_set_default_signature(
            signature_text="Cody Mitchell\nFractal Research Group\ncody@frg.earth",
            signature_html="<p>Cody Mitchell<br>Fractal Research Group<br>cody@frg.earth</p>",
            config_path=self.config_path,
        )
        shown = resend_integration.resend_get_default_signature(config_path=self.config_path)

        self.assertTrue(update["default_signature_text_present"])
        self.assertTrue(update["default_signature_html_present"])
        self.assertEqual(
            "Cody Mitchell\nFractal Research Group\ncody@frg.earth",
            shown["default_signature_text"],
        )
        self.assertEqual(
            "<p>Cody Mitchell<br>Fractal Research Group<br>cody@frg.earth</p>",
            shown["default_signature_html"],
        )

    def test_resend_send_email_supports_attachments_and_inline_images(self) -> None:
        self._write_config(token="resend-token")
        with mock.patch(
            "life_ops.resend_integration._resend_request_json",
            return_value={"id": "email-attachments"},
        ) as request_mock, mock.patch(
            "life_ops.resend_integration._queue_outbound_mail_artifacts",
            return_value={"raw_relative_path": "", "raw_sha256": "", "saved_attachment_ids": [], "stored_attachments": []},
        ), mock.patch(
            "life_ops.resend_integration._payload_with_stored_attachments",
            return_value={
                "from": "Cody <cody@frg.earth>",
                "to": ["friend@example.com"],
                "subject": "Hello",
                "html": '<p>See <img src="cid:logo-image"></p>',
                "reply_to": ["cody@frg.earth"],
                "attachments": [
                    {"filename": "sample.pdf", "content": "pdf-bytes"},
                    {"filename": "logo.png", "content": "png-bytes", "contentId": "logo-image"},
                ],
            },
        ), mock.patch(
            "life_ops.resend_integration._save_delivery_receipt_manifest",
            return_value={"raw_relative_path": "", "raw_sha256": ""},
        ):
            resend_integration.resend_send_email(
                to=["friend@example.com"],
                subject="Hello",
                html='<p>See <img src="cid:logo-image"></p>',
                attachment_paths=[str(self.sample_attachment_path)],
                inline_attachment_specs=[f"{self.inline_image_path}::logo-image"],
                journal_db_path=self.db_path,
                config_path=self.config_path,
            )

        sent = request_mock.call_args.kwargs["payload"]
        self.assertEqual(2, len(sent["attachments"]))
        self.assertEqual("sample.pdf", sent["attachments"][0]["filename"])
        self.assertTrue(sent["attachments"][0]["content"])
        self.assertEqual("logo.png", sent["attachments"][1]["filename"])
        self.assertEqual("logo-image", sent["attachments"][1]["contentId"])

    def test_resend_send_email_supports_cc_bcc_and_thread_headers(self) -> None:
        self._write_config(token="resend-token")
        with mock.patch(
            "life_ops.resend_integration._resend_request_json",
            return_value={"id": "email-threaded"},
        ) as request_mock, mock.patch(
            "life_ops.resend_integration._queue_outbound_mail_artifacts",
            return_value={"raw_relative_path": "", "raw_sha256": "", "saved_attachment_ids": [], "stored_attachments": []},
        ), mock.patch(
            "life_ops.resend_integration._save_delivery_receipt_manifest",
            return_value={"raw_relative_path": "", "raw_sha256": ""},
        ):
            resend_integration.resend_send_email(
                to=["friend@example.com"],
                cc=["cc@example.com"],
                bcc=["bcc@example.com"],
                subject="Threaded",
                text="Hi",
                in_reply_to="<root@example.com>",
                references=["<root@example.com>", "<mid@example.com>"],
                journal_db_path=self.db_path,
                config_path=self.config_path,
            )

        sent = request_mock.call_args.kwargs["payload"]
        self.assertEqual(["cc@example.com"], sent["cc"])
        self.assertEqual(["bcc@example.com"], sent["bcc"])
        self.assertTrue(sent["headers"]["Message-ID"].startswith("<lifeops-"))
        self.assertEqual("<root@example.com>", sent["headers"]["In-Reply-To"])
        self.assertEqual("<root@example.com> <mid@example.com>", sent["headers"]["References"])

    def test_resend_send_email_journals_outbound_metadata_locally(self) -> None:
        self._write_config(token="resend-token")

        def _fake_save_artifacts(**kwargs):
            return {"raw_relative_path": "resend_email/outbound.json", "raw_sha256": "abc123", "saved_attachment_ids": []}

        with mock.patch(
            "life_ops.resend_integration._resend_request_json",
            return_value={"id": "email-journaled"},
        ), mock.patch(
            "life_ops.resend_integration._queue_outbound_mail_artifacts",
            side_effect=lambda **kwargs: {**_fake_save_artifacts(**kwargs), "stored_attachments": []},
        ), mock.patch(
            "life_ops.resend_integration._save_delivery_receipt_manifest",
            return_value={"raw_relative_path": "resend_email/delivery.json", "raw_sha256": "def456"},
        ):
            resend_integration.resend_send_email(
                to=["Friend Example <friend@example.com>"],
                cc=["Cc Example <cc@example.com>"],
                bcc=["bcc@example.com"],
                subject="Journal me",
                text="Hello world",
                reply_to="reply@example.com",
                in_reply_to="<root@example.com>",
                references=["<root@example.com>", "<mid@example.com>"],
                thread_key="thread:custom",
                journal_db_path=self.db_path,
                config_path=self.config_path,
            )

        with store.open_db(self.db_path) as connection:
            row = store.get_communication_by_external_id(connection, source="resend_email", external_id="email-journaled")

        assert row is not None
        self.assertEqual("outbound", row["direction"])
        self.assertEqual("sent", row["status"])
        self.assertEqual("Cody <cody@frg.earth>", row["external_from"])
        self.assertEqual("Friend Example <friend@example.com>", row["external_to"])
        self.assertEqual("Cc Example <cc@example.com>", row["external_cc"])
        self.assertEqual("bcc@example.com", row["external_bcc"])
        self.assertEqual("reply@example.com", row["external_reply_to"])
        self.assertEqual("thread:custom", row["thread_key"])
        self.assertTrue(str(row["message_id"]).startswith("<lifeops-"))
        self.assertEqual("<root@example.com>", row["in_reply_to"])
        self.assertEqual(
            ["<root@example.com>", "<mid@example.com>"],
            json.loads(row["references_json"]),
        )
        self.assertEqual(
            [{"name": "Friend Example", "email": "friend@example.com"}],
            json.loads(row["to_json"]),
        )
        self.assertEqual(
            [{"name": "Cc Example", "email": "cc@example.com"}],
            json.loads(row["cc_json"]),
        )

    def test_resend_send_email_suppresses_orphaned_row_when_queue_setup_fails(self) -> None:
        self._write_config(token="resend-token")

        with mock.patch(
            "life_ops.resend_integration._queue_outbound_mail_artifacts",
            side_effect=RuntimeError("artifact staging failed"),
        ):
            with self.assertRaisesRegex(RuntimeError, "artifact staging failed"):
                resend_integration.resend_send_email(
                    to=["friend@example.com"],
                    subject="Hello",
                    text="Test email",
                    journal_db_path=self.db_path,
                    config_path=self.config_path,
                    attempt_immediately=False,
                )

        with store.open_db(self.db_path) as connection:
            rows = connection.execute(
                """
                SELECT id, status, deleted_at, notes
                FROM communications
                WHERE source = 'resend_email'
                ORDER BY id DESC
                """
            ).fetchall()
            queue_rows = store.list_mail_delivery_queue(connection, provider="resend", status="all", limit=20)

        self.assertEqual(1, len(rows))
        self.assertEqual("deleted", str(rows[0]["status"] or ""))
        self.assertTrue(str(rows[0]["deleted_at"] or ""))
        self.assertIn("Suppressed orphaned outbound Resend artifact before queue creation", str(rows[0]["notes"] or ""))
        self.assertEqual([], queue_rows)

    def test_resend_send_email_keeps_failed_delivery_queued_for_retry(self) -> None:
        self._write_config(token="resend-token")

        with mock.patch(
            "life_ops.resend_integration._resend_request_json",
            side_effect=RuntimeError("temporary provider outage"),
        ), mock.patch(
            "life_ops.resend_integration._queue_outbound_mail_artifacts",
            return_value={"raw_relative_path": "", "raw_sha256": "", "saved_attachment_ids": [], "stored_attachments": []},
        ):
            result = resend_integration.resend_send_email(
                to=["friend@example.com"],
                subject="Retry me",
                text="Hello world",
                journal_db_path=self.db_path,
                config_path=self.config_path,
            )

        self.assertFalse(result["sent"])
        self.assertEqual("retrying", result["status"])
        with store.open_db(self.db_path) as connection:
            queue = store.list_mail_delivery_queue(connection, provider="resend", status="all", limit=5)
            alerts = store.list_system_alerts(connection, source="resend_delivery", status="active", limit=5)
        self.assertEqual(1, len(queue))
        self.assertEqual("retrying", queue[0]["status"])
        self.assertEqual(1, int(queue[0]["attempt_count"]))
        self.assertTrue(alerts)

    def test_process_resend_delivery_queue_sends_existing_due_item(self) -> None:
        self._write_config(token="resend-token")
        queued_artifacts = {
            "raw_relative_path": "resend_email/outbound.json",
            "raw_sha256": "abc123",
            "saved_attachment_ids": [],
            "stored_attachments": [],
        }
        with mock.patch(
            "life_ops.resend_integration._queue_outbound_mail_artifacts",
            return_value=queued_artifacts,
        ):
            created = resend_integration.resend_send_email(
                to=["friend@example.com"],
                subject="Queue only",
                text="Hello queue",
                journal_db_path=self.db_path,
                config_path=self.config_path,
                attempt_immediately=False,
            )

        with mock.patch(
            "life_ops.resend_integration._resend_request_json",
            return_value={"id": "email-processed"},
        ), mock.patch(
            "life_ops.resend_integration._save_delivery_receipt_manifest",
            return_value={"raw_relative_path": "resend_email/delivery.json", "raw_sha256": "def456"},
        ):
            processed = resend_integration.process_resend_delivery_queue(
                db_path=self.db_path,
                config_path=self.config_path,
                queue_ids=[int(created["queue_id"])],
            )

        self.assertEqual(1, processed["processed_count"])
        with store.open_db(self.db_path) as connection:
            queue_row = store.get_mail_delivery_queue_item(connection, queue_id=int(created["queue_id"]))
            communication = store.get_communication_by_id(connection, int(created["communication_id"]))
        assert queue_row is not None
        assert communication is not None
        self.assertEqual("sent", queue_row["status"])
        self.assertEqual("email-processed", queue_row["provider_message_id"])
        self.assertEqual("sent", communication["status"])

    def test_process_resend_delivery_queue_ignores_retained_sent_rows_when_loading_due_work(self) -> None:
        self._write_config(token="resend-token")
        queued_artifacts = {
            "raw_relative_path": "resend_email/outbound.json",
            "raw_sha256": "abc123",
            "saved_attachment_ids": [],
            "stored_attachments": [],
        }
        with mock.patch(
            "life_ops.resend_integration._queue_outbound_mail_artifacts",
            return_value=queued_artifacts,
        ):
            created = resend_integration.resend_send_email(
                to=["friend@example.com"],
                subject="Queue only",
                text="Hello queue",
                journal_db_path=self.db_path,
                config_path=self.config_path,
                attempt_immediately=False,
            )

        with store.open_db(self.db_path) as connection:
            for index in range(30):
                store.enqueue_mail_delivery(
                    connection,
                    queue_key=f"sent-filler-{index}",
                    provider="resend",
                    communication_id=int(created["communication_id"]),
                    payload={},
                    metadata={},
                    status="sent",
                    attempt_count=1,
                    max_attempts=8,
                    next_attempt_at=f"2026-01-01T00:00:{index:02d}Z",
                    provider_message_id=f"sent-{index}",
                )

        with mock.patch(
            "life_ops.resend_integration._resend_request_json",
            return_value={"id": "email-processed"},
        ), mock.patch(
            "life_ops.resend_integration._save_delivery_receipt_manifest",
            return_value={"raw_relative_path": "resend_email/delivery.json", "raw_sha256": "def456"},
        ):
            processed = resend_integration.process_resend_delivery_queue(
                db_path=self.db_path,
                config_path=self.config_path,
                limit=25,
            )

        self.assertEqual(1, processed["processed_count"])
        self.assertEqual(int(created["queue_id"]), int(processed["processed"][0]["queue_id"]))
        with store.open_db(self.db_path) as connection:
            queue_row = store.get_mail_delivery_queue_item(connection, queue_id=int(created["queue_id"]))
        assert queue_row is not None
        self.assertEqual("sent", queue_row["status"])

    def test_resend_queue_status_distinguishes_active_from_retained_rows(self) -> None:
        self._write_config(token="resend-token")
        with mock.patch(
            "life_ops.resend_integration._queue_outbound_mail_artifacts",
            return_value={
                "raw_relative_path": "resend_email/outbound.json",
                "raw_sha256": "abc123",
                "saved_attachment_ids": [],
                "stored_attachments": [],
            },
        ):
            created = resend_integration.resend_send_email(
                to=["friend@example.com"],
                subject="Queue only",
                text="Hello queue",
                journal_db_path=self.db_path,
                config_path=self.config_path,
                attempt_immediately=False,
            )

        with store.open_db(self.db_path) as connection:
            store.update_mail_delivery_queue_item(
                connection,
                queue_id=int(created["queue_id"]),
                status="sent",
                attempt_count=1,
            )

        status = resend_integration.resend_queue_status(db_path=self.db_path)

        self.assertEqual(0, status["queue_count"])
        self.assertEqual(1, status["retained_count"])
        self.assertEqual({"sent": 1}, status["counts"])


if __name__ == "__main__":
    unittest.main()
