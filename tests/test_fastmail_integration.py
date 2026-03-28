from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops import fastmail_integration


class FastmailIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = Path(self.temp_dir.name) / "fastmail.json"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write_config(self, *, token: str = "", token_env: str = "FASTMAIL_API_TOKEN") -> None:
        self.config_path.write_text(
            json.dumps(
                {
                    "account_email": "cody@frg.earth",
                    "session_url": fastmail_integration.FASTMAIL_JMAP_SESSION_URL,
                    "api_token": token,
                    "api_token_env": token_env,
                }
            )
        )

    def test_write_fastmail_config_template(self) -> None:
        result = fastmail_integration.write_fastmail_config_template(self.config_path)

        self.assertTrue(result["created"])
        payload = json.loads(self.config_path.read_text())
        self.assertEqual("FASTMAIL_API_TOKEN", payload["api_token_env"])
        self.assertEqual(fastmail_integration.FASTMAIL_JMAP_SESSION_URL, payload["session_url"])

    def test_fastmail_status_reports_missing_setup(self) -> None:
        status = fastmail_integration.fastmail_status(config_path=self.config_path)

        self.assertFalse(status["config_present"])
        self.assertFalse(status["ready"])
        self.assertTrue(status["next_steps"])

    def test_fastmail_session_uses_bearer_auth(self) -> None:
        self._write_config(token="fastmail-test-token")

        class _FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return json.dumps(
                    {
                        "username": "cody@frg.earth",
                        "apiUrl": "https://api.fastmail.com/jmap/api/",
                        "primaryAccounts": {
                            fastmail_integration.FASTMAIL_JMAP_MAIL: "u1",
                        },
                        "capabilities": {
                            fastmail_integration.FASTMAIL_JMAP_CORE: {},
                            fastmail_integration.FASTMAIL_JMAP_MAIL: {},
                        },
                    }
                ).encode("utf-8")

        def _fake_urlopen(req, timeout=0):
            self.assertEqual("Bearer fastmail-test-token", req.headers["Authorization"])
            self.assertEqual(fastmail_integration.FASTMAIL_JMAP_SESSION_URL, req.full_url)
            return _FakeResponse()

        with mock.patch("life_ops.fastmail_integration.request.urlopen", side_effect=_fake_urlopen):
            payload = fastmail_integration.fastmail_session(config_path=self.config_path)

        self.assertEqual("cody@frg.earth", payload["username"])

    def test_fastmail_status_includes_session_metadata(self) -> None:
        self._write_config()
        with mock.patch("life_ops.fastmail_integration.credentials.resolve_secret", return_value="fastmail-test-token"):
            with mock.patch(
                "life_ops.fastmail_integration.fastmail_session",
                return_value={
                    "username": "cody@frg.earth",
                    "apiUrl": "https://api.fastmail.com/jmap/api/",
                    "downloadUrl": "https://api.fastmail.com/download/{accountId}/{blobId}/",
                    "uploadUrl": "https://api.fastmail.com/upload/{accountId}/",
                    "primaryAccounts": {fastmail_integration.FASTMAIL_JMAP_MAIL: "u1"},
                    "capabilities": {
                        fastmail_integration.FASTMAIL_JMAP_CORE: {},
                        fastmail_integration.FASTMAIL_JMAP_MAIL: {},
                    },
                },
            ):
                status = fastmail_integration.fastmail_status(config_path=self.config_path)

        self.assertTrue(status["api_token_present"])
        self.assertTrue(status["ready"])
        self.assertEqual("cody@frg.earth", status["session"]["username"])
        self.assertEqual("u1", status["session"]["mail_account_id"])

    def test_fastmail_mailboxes_uses_jmap_mailbox_get(self) -> None:
        self._write_config()
        with mock.patch("life_ops.fastmail_integration.credentials.resolve_secret", return_value="fastmail-test-token"):
            with mock.patch(
                "life_ops.fastmail_integration.fastmail_session",
                return_value={
                    "apiUrl": "https://api.fastmail.com/jmap/api/",
                    "primaryAccounts": {fastmail_integration.FASTMAIL_JMAP_MAIL: "u1"},
                },
            ):
                with mock.patch(
                    "life_ops.fastmail_integration._fastmail_request_json",
                    return_value={
                        "methodResponses": [
                            [
                                "Mailbox/get",
                                {
                                    "state": "state-1",
                                    "list": [
                                        {
                                            "id": "m1",
                                            "name": "Inbox",
                                            "role": "inbox",
                                            "totalEmails": 42,
                                            "unreadEmails": 5,
                                        }
                                    ],
                                    "notFound": [],
                                },
                                "m1",
                            ]
                        ]
                    },
                ) as request_mock:
                    payload = fastmail_integration.fastmail_mailboxes(config_path=self.config_path)

        self.assertEqual("u1", payload["account_id"])
        self.assertEqual("Inbox", payload["list"][0]["name"])
        sent_payload = request_mock.call_args.kwargs["payload"]
        self.assertEqual("Mailbox/get", sent_payload["methodCalls"][0][0])


if __name__ == "__main__":
    unittest.main()
