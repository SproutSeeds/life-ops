from __future__ import annotations

import base64
import json
import sys
import tempfile
import unittest
from email.message import EmailMessage
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops import cloudflare_email
from life_ops import store


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class CloudflareEmailTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = Path(self.temp_dir.name) / "cloudflare_mail.json"
        self.output_dir = Path(self.temp_dir.name) / "worker"
        self.db_path = Path(self.temp_dir.name) / "life_ops.db"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write_config(self, *, forward_to: str = "", worker_public_url: str = "https://worker.example.workers.dev") -> None:
        self.config_path.write_text(
            json.dumps(
                {
                    "zone_name": "frg.earth",
                    "route_address": "cody",
                    "forward_to": forward_to,
                    "worker_name": "life-ops-email-ingest",
                    "worker_public_url": worker_public_url,
                    "ingest_secret_env": "LIFE_OPS_MAIL_INGEST_SECRET",
                }
            )
        )

    def _payload(self) -> dict:
        message = EmailMessage()
        message["From"] = "Alice Example <alice@example.com>"
        message["To"] = "cody@frg.earth"
        message["Subject"] = "Action required: review this draft"
        message["Message-ID"] = "<msg-123@example.com>"
        message.set_content("Please review this draft and reply today.")
        raw_bytes = message.as_bytes()
        return {
            "provider": "cloudflare-email-routing",
            "worker": "life-ops-email-ingest",
            "received_at": "2026-03-27T12:00:00Z",
            "envelope_from": "alice@example.com",
            "envelope_to": "cody@frg.earth",
            "headers": {"Subject": "Action required: review this draft"},
            "raw_base64": base64.b64encode(raw_bytes).decode("ascii"),
            "raw_size": len(raw_bytes),
            "payload_hash": "deadbeef",
        }

    def test_write_cloudflare_mail_config_template(self) -> None:
        result = cloudflare_email.write_cloudflare_mail_config_template(self.config_path)

        self.assertTrue(result["created"])
        payload = json.loads(self.config_path.read_text())
        self.assertEqual("frg.earth", payload["zone_name"])
        self.assertIn("worker_public_url", payload)

    def test_cloudflare_mail_status_reports_missing_setup(self) -> None:
        status = cloudflare_email.cloudflare_mail_status(config_path=self.config_path)

        self.assertFalse(status["config_present"])
        self.assertFalse(status["ready_for_worker"])
        self.assertFalse(status["ready_for_local_sync"])
        self.assertTrue(status["next_steps"])

    def test_cloudflare_mail_status_builds_route_without_forwarding(self) -> None:
        self._write_config(forward_to="")
        with mock.patch("life_ops.cloudflare_email.credentials.resolve_secret", return_value="secret"):
            status = cloudflare_email.cloudflare_mail_status(config_path=self.config_path)

        self.assertEqual("cody@frg.earth", status["route_full_address"])
        self.assertTrue(status["ingest_secret_present"])
        self.assertTrue(status["ready_for_worker"])
        self.assertTrue(status["ready_for_local_sync"])
        self.assertFalse(status["forwarding_enabled"])

    def test_write_cloudflare_worker_template(self) -> None:
        self._write_config()

        result = cloudflare_email.write_cloudflare_worker_template(
            self.output_dir,
            config_path=self.config_path,
        )

        self.assertTrue(result["created"])
        self.assertTrue((self.output_dir / "src" / "index.mjs").exists())
        self.assertTrue((self.output_dir / "wrangler.toml").exists())
        self.assertTrue((self.output_dir / "README.md").exists())
        worker_source = (self.output_dir / "src" / "index.mjs").read_text()
        wrangler_toml = (self.output_dir / "wrangler.toml").read_text()
        self.assertIn("MailQueue", worker_source)
        self.assertIn("/api/mail/queue/inject", worker_source)
        self.assertIn("/api/mail/queue/pull", worker_source)
        self.assertIn("X-Life-Ops-Signature", worker_source)
        self.assertIn("[[durable_objects.bindings]]", wrangler_toml)
        self.assertIn('new_sqlite_classes = ["MailQueue"]', wrangler_toml)

    def test_enqueue_cloudflare_mail_payload_calls_worker(self) -> None:
        self._write_config()
        with mock.patch("life_ops.cloudflare_email.credentials.resolve_secret", return_value="secret"), mock.patch(
            "life_ops.cloudflare_email.request.urlopen",
            return_value=_FakeResponse({"stored": True, "id": "mail_0000000000000001"}),
        ):
            result = cloudflare_email.enqueue_cloudflare_mail_payload(
                payload=self._payload(),
                config_path=self.config_path,
            )

        self.assertTrue(result["stored"])
        self.assertEqual("mail_0000000000000001", result["id"])

    def test_cloudflare_mail_queue_status_calls_worker(self) -> None:
        self._write_config()
        with mock.patch("life_ops.cloudflare_email.credentials.resolve_secret", return_value="secret"), mock.patch(
            "life_ops.cloudflare_email.request.urlopen",
            return_value=_FakeResponse(
                {
                    "pending_count": 2,
                    "total_stored": 10,
                    "total_acknowledged": 8,
                    "cloud_backup_mode": "durable_queue",
                }
            ),
        ):
            status = cloudflare_email.cloudflare_mail_queue_status(config_path=self.config_path)

        self.assertEqual(2, status["pending_count"])
        self.assertEqual("durable_queue", status["cloud_backup_mode"])

    def test_sync_cloudflare_mail_queue_ingests_and_acks(self) -> None:
        self._write_config()
        queue_payload = self._payload()
        responses = iter(
            [
                _FakeResponse(
                    {
                        "items": [
                            {
                                "id": "mail_0000000000000001",
                                "seq": 1,
                                "payload": queue_payload,
                            }
                        ],
                        "pending_count": 1,
                        "total_stored": 1,
                        "total_acknowledged": 0,
                    }
                ),
                _FakeResponse(
                    {
                        "acknowledged_count": 1,
                        "pending_count": 0,
                        "total_stored": 1,
                        "total_acknowledged": 1,
                    }
                ),
            ]
        )

        def _fake_urlopen(req, timeout=60):
            return next(responses)

        with mock.patch("life_ops.cloudflare_email.credentials.resolve_secret", return_value="secret"), mock.patch(
            "life_ops.cloudflare_email.request.urlopen",
            side_effect=_fake_urlopen,
        ):
            result = cloudflare_email.sync_cloudflare_mail_queue(
                db_path=self.db_path,
                config_path=self.config_path,
                limit=10,
            )

        self.assertEqual(1, result["pulled_count"])
        self.assertEqual(1, result["ingested_count"])
        self.assertEqual(1, result["acked_count"])
        self.assertEqual(0, result["pending_count"])
        with store.open_db(self.db_path) as connection:
            rows = store.list_communications(connection, source="cloudflare_email", limit=5)
        self.assertEqual(1, len(rows))
        self.assertEqual("Action required: review this draft", rows[0]["subject"])

    def test_sync_cloudflare_mail_queue_records_alert_on_ingest_failure(self) -> None:
        self._write_config()
        queue_payload = self._payload()

        with mock.patch("life_ops.cloudflare_email.credentials.resolve_secret", return_value="secret"), mock.patch(
            "life_ops.cloudflare_email._cloudflare_worker_request_json",
            return_value={
                "items": [{"id": "mail_0000000000000001", "seq": 1, "payload": queue_payload}],
                "pending_count": 1,
                "total_stored": 1,
                "total_acknowledged": 0,
            },
        ), mock.patch(
            "life_ops.cloudflare_email.ingest_cloudflare_email_payload",
            side_effect=RuntimeError("boom"),
        ):
            result = cloudflare_email.sync_cloudflare_mail_queue(
                db_path=self.db_path,
                config_path=self.config_path,
                limit=10,
            )

        self.assertEqual(1, result["failed_count"])
        with store.open_db(self.db_path) as connection:
            alerts = store.list_system_alerts(connection, source="cloudflare_email", status="active", limit=5)
        self.assertTrue(alerts)
        self.assertEqual("Cloudflare mail sync ingested with errors", alerts[0]["title"])


if __name__ == "__main__":
    unittest.main()
