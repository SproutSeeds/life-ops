from __future__ import annotations

import io
import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops.x_integration import (
    X_AUTHORIZE_URL,
    build_x_authorize_url,
    write_x_client_template,
    x_auth,
    x_status,
)


class XIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.client_path = Path(self.temp_dir.name) / "x_client.json"
        self.token_path = Path(self.temp_dir.name) / "x_token.json"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write_client_config(self, *, client_secret: str = "") -> None:
        self.client_path.write_text(
            json.dumps(
                {
                    "client_id": "abc123",
                    "client_secret": client_secret,
                    "redirect_uri": "http://127.0.0.1:8787/x/callback",
                    "scopes": ["tweet.read", "users.read", "tweet.write", "offline.access"],
                    "bearer_token": "",
                }
            )
        )

    def test_write_x_client_template_creates_file(self) -> None:
        result = write_x_client_template(self.client_path)

        self.assertTrue(result["created"])
        self.assertTrue(self.client_path.exists())
        payload = json.loads(self.client_path.read_text())
        self.assertEqual("replace-me", payload["client_id"])
        self.assertIn("tweet.write", payload["scopes"])

    def test_build_x_authorize_url_contains_pkce_parameters(self) -> None:
        url = build_x_authorize_url(
            client_id="abc123",
            redirect_uri="http://127.0.0.1:8787/x/callback",
            scopes=["tweet.read", "users.read"],
            state="state123",
            code_challenge="challenge456",
        )

        self.assertTrue(url.startswith(X_AUTHORIZE_URL))
        self.assertIn("client_id=abc123", url)
        self.assertIn("redirect_uri=http%3A%2F%2F127.0.0.1%3A8787%2Fx%2Fcallback", url)
        self.assertIn("scope=tweet.read+users.read", url)
        self.assertIn("state=state123", url)
        self.assertIn("code_challenge=challenge456", url)
        self.assertIn("code_challenge_method=S256", url)

    def test_x_status_reports_missing_setup(self) -> None:
        status = x_status(client_path=self.client_path, token_path=self.token_path)

        self.assertFalse(status["client_config_present"])
        self.assertFalse(status["token_present"])
        self.assertFalse(status["ready_for_user_actions"])
        self.assertTrue(status["next_steps"])

    def test_x_status_reports_ready_user_actions_when_token_and_scopes_exist(self) -> None:
        self._write_client_config()
        self.token_path.write_text(
            json.dumps(
                {
                    "access_token": "token",
                    "refresh_token": "refresh",
                }
            )
        )

        status = x_status(client_path=self.client_path, token_path=self.token_path)

        self.assertTrue(status["ready_for_user_actions"])
        self.assertTrue(status["ready_for_post_write"])
        self.assertEqual([], status["missing_required_scopes"])
        self.assertFalse(status["token_expired"])

    def test_x_status_marks_expired_tokens(self) -> None:
        self._write_client_config()
        obtained_at = (datetime.now(timezone.utc) - timedelta(seconds=7200)).isoformat()
        self.token_path.write_text(
            json.dumps(
                {
                    "access_token": "token",
                    "refresh_token": "refresh",
                    "expires_in": 3600,
                    "obtained_at": obtained_at,
                }
            )
        )

        status = x_status(client_path=self.client_path, token_path=self.token_path)

        self.assertTrue(status["token_expired"])

    @mock.patch("life_ops.x_integration.x_get_authenticated_user")
    @mock.patch("life_ops.x_integration._token_response")
    @mock.patch("life_ops.x_integration._wait_for_callback")
    @mock.patch("life_ops.x_integration._pkce_verifier")
    def test_x_auth_stores_token_and_me_payload(
        self,
        verifier_mock: mock.Mock,
        wait_mock: mock.Mock,
        token_response_mock: mock.Mock,
        me_mock: mock.Mock,
    ) -> None:
        self._write_client_config(client_secret="shhh")
        verifier_mock.return_value = "verifier-123"
        wait_mock.return_value = {
            "state": ["state-123"],
            "code": ["auth-code-123"],
        }
        token_response_mock.return_value = {
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "expires_in": 7200,
            "scope": "tweet.read users.read tweet.write offline.access",
        }
        me_mock.return_value = {
            "data": {
                "id": "42",
                "username": "BackToTheFort",
                "name": "Cody Mitchell",
            }
        }

        with mock.patch("life_ops.x_integration.secrets.token_urlsafe", side_effect=["state-123"]):
            with mock.patch("sys.stdout", new=io.StringIO()):
                result = x_auth(
                    client_path=self.client_path,
                    token_path=self.token_path,
                    timeout_seconds=10,
                    open_browser=False,
                )

        saved = json.loads(self.token_path.read_text())
        self.assertEqual("access-token", saved["access_token"])
        self.assertEqual("refresh-token", saved["refresh_token"])
        self.assertEqual("BackToTheFort", saved["me"]["username"])
        self.assertEqual("BackToTheFort", result["me"]["username"])
        self.assertFalse(result["browser_opened"])
        token_response_mock.assert_called_once()
        me_mock.assert_called_once_with(client_path=self.client_path, token_path=self.token_path)


if __name__ == "__main__":
    unittest.main()
