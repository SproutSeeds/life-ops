from __future__ import annotations

import base64
import hashlib
import contextlib
import io
import json
import os
import tempfile
import unittest
import uuid
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

    def test_spam_commands_use_the_same_cloud_actions_and_search_folder(self):
        with patch.object(cloud, "request", return_value={"items": []}) as api, contextlib.redirect_stdout(io.StringIO()):
            message = "mail_0000000000000001"
            for action in ["spam", "not-spam"]:
                self.assertEqual(cloud.main([action, message]), 0)
                api.assert_called_with("message-action", {"ids": [message], "action": action})
            self.assertEqual(cloud.main(["search", "--view", "spam", "--query", "sender"]), 0)
            self.assertEqual(api.call_args.args[1]["view"], "spam")
            self.assertEqual(cloud.main(["spam-senders"]), 0)
            self.assertEqual(api.call_args.args[0], "spam-senders")

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

    def test_cli_attachment_upload_retry_download_remove_and_copy_preserve_drafts(self):
        path = Path(self.temp.name) / "Résumé ☕.png"
        content = bytes(range(256)); path.write_bytes(content)
        original = {"kind": "compose", "sender": "cody@frg.earth", "to": ["person@example.test"],
                    "text": "Keep my body", "subject": "Keep my subject", "signature": "", "pending": "",
                    "attachments": [{"id": str(uuid.uuid4()), "size": 4, "filename": "website.txt"}]}
        saved = {"draft": original, "state": "editing", "revision": 4}
        calls = []
        def fake(action, p=None):
            calls.append((action, p))
            if action == "status":
                return {"attachments": {"max_bytes": 20 * 1024 * 1024, "max_count": 50, "blocked_extensions": ["exe"]}}
            if action == "identities":
                return {"identities": [{"address": "cody@frg.earth", "signature": ""}]}
            if action == "draft":
                return saved
            if action == "attachment-upload":
                data = base64.b64decode(p["content_base64"], validate=True)
                return {"attachment": {"id": p["id"], "filename": p["filename"], "content_type": p["content_type"],
                        "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}}
            if action == "draft-attachment":
                return {**saved["draft"]["attachments"][-1], "content_base64": base64.b64encode(content).decode()}
            if action == "draft-save":
                saved.update(draft=p["draft"], revision=5)
                return saved
            raise AssertionError(action)
        args = ["draft-save", "--id", "draft_test", "--revision", "4", "--save-id", str(uuid.uuid4()), "--attach", str(path)]
        with patch.object(cloud, "request", side_effect=fake), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cloud.main(args), 0)
            first = calls[-1][1]
            self.assertEqual(cloud.main(args), 0)
            self.assertEqual(calls[-1][1], first)
            self.assertEqual(len(saved["draft"]["attachments"]), 2)
            self.assertEqual(saved["draft"]["text"], original["text"])
            uploaded = saved["draft"]["attachments"][-1]
            output = Path(self.temp.name) / "download.png"
            self.assertEqual(cloud.main(["draft", "draft_test", "--attachment", uploaded["id"], "--output", str(output)]), 0)
            self.assertEqual(output.read_bytes(), content)
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(cloud.main(["draft", "draft_test", "--attachment", "1", "--output", str(output)]), 1)
            self.assertEqual(cloud.main(["draft-copy", "draft_test", "--revision", "5", "--save-id", str(uuid.uuid4())]), 0)
            self.assertEqual(calls[-1][1]["draft"]["attachments"], first["draft"]["attachments"])
            self.assertEqual(cloud.main(["draft-save", "--id", "draft_test", "--revision", "5", "--save-id", str(uuid.uuid4()), "--remove-attachment", uploaded["id"]]), 0)
            self.assertEqual(saved["draft"]["attachments"], original["attachments"])
            self.assertEqual(cloud.main(["draft-save", "--id", "draft_test", "--revision", "5", "--save-id", str(uuid.uuid4()), "--clear-attachments"]), 0)
            self.assertEqual(saved["draft"]["attachments"], [])
        self.assertNotIn("draft-send", [action for action, _ in calls])

    def test_attachment_selection_rejects_before_upload_and_download_verifies_hash(self):
        limit = 20 * 1024 * 1024
        policy = {"attachments": {"max_bytes": limit, "max_count": 50, "blocked_extensions": ["exe"]}}
        envelope = {"save_id": str(uuid.uuid4()), "draft": {}}
        path = Path(self.temp.name) / "file.txt"
        for name, size in [("empty.txt", 0), ("blocked.exe", 1), ("large.zip", limit + 1)]:
            path = Path(self.temp.name) / name
            with path.open("wb") as stream:
                stream.truncate(size)
            with patch.object(cloud, "request", return_value=policy) as api:
                with self.assertRaises(ValueError):
                    cloud.attach_files(envelope, [path], [], False)
                self.assertEqual(api.call_count, 1)
        path.write_bytes(b"x")
        with patch.object(cloud, "request", return_value=policy):
            with self.assertRaisesRegex(ValueError, "at most 50"):
                cloud.attach_files(envelope, [path] * 51, [], False)
        output = Path(self.temp.name) / "corrupt.txt"
        with patch.object(cloud, "request", return_value={"content_base64": "eA==", "size": 1, "sha256": "wrong"}), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(cloud.main(["draft", "draft_test", "--attachment", "0", "--output", str(output)]), 1)
        self.assertFalse(output.exists())

    def test_date_filters_and_contact_revision_match_browser_parameters(self):
        with patch.object(cloud, "request", return_value={}) as api, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cloud.main(["search", "--after", "2026-10-01", "--before", "2026-10-29"]), 0)
            self.assertEqual(api.call_args.args[1]["after"], "2026-10-01")
            self.assertEqual(api.call_args.args[1]["before"], "2026-10-29")
            self.assertEqual(cloud.main(["contacts", "--revision", "42", "--cursor", "next"]), 0)
            self.assertEqual(api.call_args.args[1]["revision"], 42)

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
