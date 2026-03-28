from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops import emma_integration


class EmmaIntegrationTests(unittest.TestCase):
    def test_emma_status_reflects_key_presence(self) -> None:
        with mock.patch("life_ops.emma_integration.credentials.resolve_secret", return_value="emma-test-key"):
            status = emma_integration.emma_status()

        self.assertTrue(status["api_key_present"])
        self.assertTrue(status["ready"])
        self.assertEqual("soulbind", status["default_agent"])

    def test_emma_me_uses_bearer_auth(self) -> None:
        fake_payload = {"user": {"username": "cody"}}

        class _FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return json.dumps(fake_payload).encode("utf-8")

        def _fake_urlopen(req, timeout=0):
            self.assertEqual("Bearer emma-test-key", req.headers["Authorization"])
            self.assertTrue(req.full_url.endswith("/api/v1/me"))
            return _FakeResponse()

        with mock.patch("life_ops.emma_integration.credentials.resolve_secret", return_value="emma-test-key"):
            with mock.patch("life_ops.emma_integration.request.urlopen", side_effect=_fake_urlopen):
                payload = emma_integration.emma_me()

        self.assertEqual("cody", payload["user"]["username"])

    def test_emma_chat_sends_expected_payload(self) -> None:
        fake_payload = {
            "agent": "soulbind",
            "persona": "mom",
            "message": "I am here.",
            "source": "fallback",
        }

        class _FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return json.dumps(fake_payload).encode("utf-8")

        def _fake_urlopen(req, timeout=0):
            body = json.loads(req.data.decode("utf-8"))
            self.assertEqual("soulbind", body["agent"])
            self.assertEqual("listen", body["mode"])
            self.assertEqual("user", body["messages"][0]["role"])
            self.assertEqual("Check in with me.", body["messages"][0]["content"])
            self.assertTrue(body["messages"][0]["id"])
            self.assertTrue(body["messages"][0]["createdAt"])
            return _FakeResponse()

        with mock.patch("life_ops.emma_integration.credentials.resolve_secret", return_value="emma-test-key"):
            with mock.patch("life_ops.emma_integration.request.urlopen", side_effect=_fake_urlopen):
                payload = emma_integration.emma_chat(message="Check in with me.")

        self.assertEqual("soulbind", payload["agent"])
        self.assertEqual("I am here.", payload["message"])

    def test_emma_chat_rejects_invalid_agent(self) -> None:
        with self.assertRaises(ValueError):
            emma_integration.emma_chat(message="hi", agent="not-real")


if __name__ == "__main__":
    unittest.main()
