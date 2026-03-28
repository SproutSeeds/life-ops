from __future__ import annotations

import sys
import unittest
from email.message import EmailMessage
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops import mail_metadata


class MailMetadataTests(unittest.TestCase):
    def test_headers_snapshot_defaults_to_operational_allowlist(self) -> None:
        message = EmailMessage()
        message["From"] = "Alice <alice@example.com>"
        message["To"] = "cody@frg.earth"
        message["Subject"] = "Hello"
        message["Message-ID"] = "<msg@example.com>"
        message["Received"] = "from mx.example.net"
        message["X-Custom-Secret"] = "redact-me"

        headers = mail_metadata.headers_snapshot(message)

        self.assertIn("From", headers)
        self.assertIn("Message-ID", headers)
        self.assertNotIn("Received", headers)
        self.assertNotIn("X-Custom-Secret", headers)

    def test_headers_snapshot_forensic_mode_keeps_all_headers(self) -> None:
        message = EmailMessage()
        message["From"] = "Alice <alice@example.com>"
        message["X-Custom-Secret"] = "keep-me"

        headers = mail_metadata.headers_snapshot(message, forensic=True)

        self.assertEqual(["keep-me"], headers["X-Custom-Secret"])


if __name__ == "__main__":
    unittest.main()
