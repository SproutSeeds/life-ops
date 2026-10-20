from __future__ import annotations

import base64
import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, Mock

from life_ops import cmail_cloud as cloud, mail_ui, resend_integration, store


class CloudMailTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config = Path(self.temp.name) / "cloud.json"
        self.config.write_text(json.JSONEncoder().encode({
            "system": "cmail-cloud", "active": True, "projection_start_cursor": "mail_0000000000000001",
            "legacy_drafts": {"12": "draft_10000000-0000-4000-8000-000000000012"},
        }))
        self.patch = patch.object(cloud, "CONFIG", self.config)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def test_legacy_writes_and_direct_provider_fail_before_database_or_network(self):
        with patch.object(store, "open_db", side_effect=AssertionError("must not open legacy DB")):
            for function, args in [
                (mail_ui._save_cmail_draft, (None, {})),
            ]:
                with self.assertRaisesRegex(RuntimeError, "Local CMAIL is archived"):
                    function(*args)
            with self.assertRaises(RuntimeError):
                mail_ui.send_cmail_draft(db_path=Path("/missing"), draft_id=12)
            with self.assertRaises(RuntimeError):
                mail_ui.delete_cmail_draft(db_path=Path("/missing"), draft_id=12)
            with self.assertRaises(RuntimeError):
                resend_integration.resend_send_email(to=["person@example.test"], subject="Never send")
            self.assertTrue(resend_integration.process_resend_delivery_queue()["retired"])

    def test_cli_read_uses_cloud_and_failure_never_falls_back(self):
        with patch.object(cloud, "request", return_value={"items": []}) as api, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cloud.main(["drafts", "--identity", "frg", "--format", "json"]), 0)
            api.assert_called_once_with("drafts", {"mailbox": "cody@frg.earth", "query": "", "limit": 50})
        with patch.object(cloud, "request", side_effect=RuntimeError("cloud unavailable")), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(cloud.main(["drafts"]), 1)

    def test_save_requires_stable_id_and_send_requires_confirmation_and_revision(self):
        with patch.object(cloud, "request") as api, contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(cloud.main(["draft-save", "--to", "person@example.test"]), 1)
            api.assert_not_called()
            for args in [["draft-send", "12"], ["draft-send", "12", "--revision", "1"]]:
                with self.assertRaises(SystemExit):
                    cloud.main(args)
            api.assert_not_called()
        body = Path(self.temp.name) / "body.txt"
        body.write_text("Exact original wording.\n\nCody")
        calls = []
        def fake(action, parameters=None):
            calls.append((action, parameters))
            if action == "identities":
                return {"identities": [{"address": "cody@frg.earth", "signature": "Cody"}]}
            return {"id": parameters["id"], "revision": 1, "state": "editing"}
        args = ["draft-save", "--save-id", "10000000-0000-4000-8000-000000000014",
                "--body-file", str(body), "--to", "person@example.test", "--cc", "copy@example.test"]
        with patch.object(cloud, "request", side_effect=fake), contextlib.redirect_stdout(io.StringIO()):
            cloud.main(args); cloud.main(args)
        self.assertEqual(calls[1], calls[3])
        self.assertEqual(calls[1][1]["draft"]["text"], body.read_text())
        self.assertEqual(calls[1][1]["draft"]["cc"], ["copy@example.test"])
        self.assertEqual([c[0] for c in calls], ["identities", "draft-save", "identities", "draft-save"])

    def test_old_http_redirects_or_returns_gone_and_numeric_draft_links_resolve(self):
        handler = Mock(command="GET", path="/?draft=12")
        self.assertTrue(cloud.retired_http(handler))
        handler.send_response.assert_called_once_with(308)
        self.assertIn("draft_10000000", str(handler.send_header.call_args_list))
        handler = Mock(command="POST", path="/api/drafts/12/send")
        cloud.retired_http(handler)
        self.assertEqual(handler._send_json.call_args.kwargs["status_code"], 410)
        self.config.write_text("broken")
        with self.assertRaises(ValueError):
            cloud.active()

    def test_retired_ui_uses_redirect_without_opening_mailbox_or_warming_caches(self):
        with patch.object(mail_ui, "_make_handler", side_effect=AssertionError("legacy mailbox must stay closed")), patch.object(cloud, "serve_redirect") as redirect:
            mail_ui.serve_mail_ui(db_path=Path("/must-not-open"), host="127.0.0.1", port=4311)
            redirect.assert_called_once_with(host="127.0.0.1", port=4311)

    def test_runtime_sync_uses_cloud_projection_instead_of_legacy_provider_polling(self):
        from life_ops.cmail_runtime import _run_periodic_loop
        stopped = Mock()
        stopped.is_set.return_value = False
        stopped.wait.return_value = True
        legacy = Mock(side_effect=AssertionError("legacy provider sync must stay disabled"))
        with patch.object(cloud, "sync_projection") as projection:
            _run_periodic_loop(stop_event=stopped, interval_seconds=60,
                               runtime_db_path=Path("/projection"), worker_name="sync", target=legacy)
            projection.assert_called_once_with(runtime_db_path=Path("/projection"))
            legacy.assert_not_called()
    def test_flag_edits_keep_existing_sender_recipients_and_reply_headers(self):
        original = {"kind": "compose", "sender": "support@bindmysoul.com",
                    "to": ["person@example.test"], "cc": ["copy@example.test"], "subject": "Original",
                    "text": "Exact text and signature.", "signature": "", "pending": "",
                    "in_reply_to": "<parent@example.test>", "references": ["<parent@example.test>"]}
        calls = []
        def fake(action, parameters=None):
            calls.append((action, parameters))
            if action == "draft":
                return {"draft": original, "revision": 4, "state": "editing"}
            if action == "identities":
                return {"identities": [{"address": original["sender"], "signature": "Default signature"}]}
            return {"state": "editing"}
        with patch.object(cloud, "request", side_effect=fake), contextlib.redirect_stdout(io.StringIO()):
            code = cloud.main(["draft-save", "--id", "draft_test", "--revision", "4",
                "--save-id", "10000000-0000-4000-8000-000000000017", "--subject", "Revised"])
        self.assertEqual(code, 0)
        self.assertEqual(calls[-1][1]["draft"], {**original, "subject": "Revised", "bcc": []})

    def test_projection_deduplicates_existing_mail_and_resumes_after_ingest_failure(self):
        db = Path(self.temp.name) / "projection.db"
        with store.open_db(db) as connection:
            connection.execute("INSERT INTO communications (subject, channel, happened_at, direction, status, message_id) VALUES ('Keep', 'email', '2026-10-20', 'inbound', 'done', '<known@example.test>')")
            connection.commit()
        def item(n, message):
            raw = f"From: person@example.test\r\nTo: cody@frg.earth\r\nMessage-ID: <{message}@example.test>\r\n\r\nBody"
            return {"id": f"mail_{n:016}", "payload": {"raw_base64": base64.b64encode(raw.encode()).decode()}}
        data = {"items": [item(2, "known"), item(3, "new")], "cursor": "mail_0000000000000003", "has_more": False}
        with patch.object(cloud, "request", return_value=data), patch("life_ops.mail_ingest.ingest_cloudflare_email_payload", side_effect=RuntimeError("disk unavailable")):
            with self.assertRaises(RuntimeError):
                cloud.sync_projection(runtime_db_path=db)
        with store.open_db(db) as connection:
            self.assertEqual(connection.execute("SELECT value FROM cmail_cloud_projection").fetchone()[0], "mail_0000000000000002")
            self.assertEqual(connection.execute("SELECT status FROM communications").fetchone()[0], "done")
        data["items"] = [item(3, "new")]
        with patch.object(cloud, "request", return_value=data) as api, patch("life_ops.mail_ingest.ingest_cloudflare_email_payload") as ingest:
            self.assertEqual(cloud.sync_projection(runtime_db_path=db)["imported"], 1)
            self.assertEqual(api.call_args.args[1]["cursor"], "mail_0000000000000002")
            self.assertEqual(ingest.call_args.args[0]["provider"], "cmail-cloud")

    def test_cloud_outbound_projection_is_sent_and_retains_thread_headers(self):
        from life_ops.mail_ingest import ingest_cloudflare_email_payload
        from life_ops import vault_crypto
        db = Path(self.temp.name) / "sent-projection.db"
        raw = ("From: Cody <cody@frg.earth>\r\nTo: person@example.test\r\n"
               "Cc: copy@example.test\r\nSubject: Re: Fixture\r\n"
               "Message-ID: <outbound@example.test>\r\nIn-Reply-To: <parent@example.test>\r\n"
               "References: <parent@example.test>\r\n\r\nSynthetic cloud response.")
        payload = {"provider": "cmail-cloud", "direction": "outbound",
                   "envelope_to": "cody@frg.earth", "external_id": "cmail-cloud:mail_0000000000000099",
                   "raw_base64": base64.b64encode(raw.encode()).decode()}
        with patch.dict(os.environ, {vault_crypto.MASTER_KEY_NAME: vault_crypto._b64url_encode(b"a" * 32),
                                     "LIFE_OPS_HOME": self.temp.name}):
            ingest_cloudflare_email_payload(payload, db_path=db)
        with store.open_db(db) as connection:
            row = connection.execute("SELECT direction,status,in_reply_to,external_cc,mail_identity FROM communications").fetchone()
            self.assertEqual(tuple(row), ("outbound", "sent", "<parent@example.test>", "copy@example.test", "frg"))


if __name__ == "__main__":
    unittest.main()
