from __future__ import annotations

import base64
import json
import os
import sqlite3
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timedelta
from email.message import EmailMessage
from http.server import HTTPServer
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from life_ops import mail_ingest, mail_labels, mail_metadata, mail_spam, mail_ui, store, vault_crypto

ALIAS = "clawdad@frg.earth"
WELCOME = "<welcome-1@resend.example>"


class MailLabelTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.env = mock.patch.dict(
            os.environ,
            {
                "LIFE_OPS_HOME": str(self.root),
                "LIFE_OPS_FORCE_ENCRYPTED_DB": "0",
                vault_crypto.MASTER_KEY_NAME: vault_crypto._b64url_encode(b"l" * 32),
            },
        )
        self.env.start()
        self.db = self.root / "mail.db"
        self.connection = store.open_db(self.db)

    def tearDown(self):
        self.connection.close()
        self.env.stop()
        self.temp.cleanup()

    def message(self, external_id, *, sender="Tester <tester@example.com>", to="cody@frg.earth", subject="Hello",
                body="Synthetic body", minutes=0, message_id=None, references=(), **overrides):
        message_id = message_id or f"<{external_id}@example.com>"
        references = list(references)
        in_reply_to = references[-1] if references else ""
        args = dict(
            source="cloudflare_email", external_id=external_id, subject=subject, channel="email",
            happened_at=datetime(2026, 9, 20, 9) + timedelta(minutes=minutes), follow_up_at=None,
            direction="inbound", status="open", external_from=sender, external_to=to, body_text=body,
            message_id=message_id, in_reply_to=in_reply_to, references=references,
            thread_key=mail_metadata.derive_thread_key(message_id=message_id, in_reply_to=in_reply_to, references=references),
        )
        args.update(overrides)
        return store.upsert_communication_from_sync(self.connection, **args)

    def cody_reply(self, external_id, *, to, references, **overrides):
        return self.message(
            external_id, source="resend_email", direction="outbound", status="sent",
            sender="Cody <cody@frg.earth>", to=to, message_id=f"<lifeops-{external_id}@frg.earth>",
            references=references, **overrides,
        )

    def overview(self, label=None, *, identity="all", spam=False, limit=80):
        return mail_ui._build_correspondence_overview_from_connection(
            self.connection, label=label, identity=identity, hidden=spam, limit=limit,
        )

    def labeled(self, label="clawdad", **kwargs):
        return {message["id"] for message in self.overview(label, **kwargs)["messages"]}

    def reasons(self, label="clawdad"):
        payload = self.overview(label)
        key = str(payload["label"]["id"])
        return {message["id"]: message["label_reasons"][key] for message in payload["messages"]}

    def test_rules_parse_keywords_addresses_and_domains(self):
        self.assertEqual(("address", ALIAS), mail_labels.parse_rule("ClawDad <CLAWDAD@frg.earth>"))
        self.assertEqual(("domain", "linkedin.com"), mail_labels.parse_rule("@LinkedIn.com"))
        self.assertEqual(("keyword", "grant*"), mail_labels.parse_rule("Grant*"))
        self.assertEqual(("keyword", "machine learning"), mail_labels.parse_rule('"Machine   Learning"'))
        for value in ["", "a", "@", "@localhost", "*", "gr*ant", "a@example.com, b@example.com",
                      "a@example.com\nBcc: b@example.com", "k" * 121]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                mail_labels.parse_rule(value)

    def test_keywords_match_whole_words_prefixes_and_phrases(self):
        self.message("digest", subject="Weekly email digest", body="Nothing relevant")
        ai = self.message("ai", subject="AI safety grant", body="A new AI program")
        grants = self.message("grants", sender="Fund <fund@example.org>", subject="Grants open", body="Apply for grants")
        phrase = self.message("phrase", sender="Lab <lab@example.net>", body="We study machine\n  learning here")
        mail_labels.create_label(self.connection, "AI", ["ai"])
        mail_labels.create_label(self.connection, "Grants", ["grant*"])
        mail_labels.create_label(self.connection, "ML", ["machine learning"])
        self.assertEqual({ai}, self.labeled("ai"))
        self.assertEqual({ai, grants}, self.labeled("grants"))
        self.assertEqual({phrase}, self.labeled("ml"))
        self.assertEqual("Mentions “grant*”", self.reasons("grants")[grants])

    def test_addresses_match_every_address_field_and_domains_skip_cmail_addresses(self):
        sender = self.message("from", sender=f"ClawDad <{ALIAS}>")
        recipient = self.message("to", to=ALIAS)
        copied = self.message("cc", external_cc=ALIAS)
        reply_to = self.message("reply-to", external_reply_to=ALIAS)
        envelope = self.message("envelope", envelope_to=ALIAS)
        self.message("other")
        mail_labels.create_label(self.connection, "ClawDad", [ALIAS])
        self.assertEqual(
            {
                sender: f"From {ALIAS}",
                recipient: f"Sent to {ALIAS}",
                copied: f"Cc {ALIAS}",
                reply_to: f"Reply-To {ALIAS}",
                envelope: f"Delivered to {ALIAS}",
            },
            self.reasons(),
        )
        linkedin = self.message("jobs", sender="Jobs <jobs@em.linkedin.com>")
        mail_labels.create_label(self.connection, "LinkedIn", ["@linkedin.com"])
        mail_labels.create_label(self.connection, "FRG domain", ["@frg.earth"])
        self.assertEqual({linkedin}, self.labeled("linkedin"))
        # Every message is addressed to cody@frg.earth; only correspondents at the domain count.
        self.assertEqual({sender, recipient, copied, reply_to, envelope}, self.labeled("frg-domain"))

    def test_clawdad_handoff_conversations_follow_verified_reply_headers(self):
        mail_labels.create_label(self.connection, "ClawDad", [ALIAS])
        notification = self.message("notify", sender=f"ClawDad <{ALIAS}>", subject="New ClawDad beta request")
        # A tester answers a site welcome that CMAIL never saw, at the alias.
        first = self.message("t1", sender="Dana Tester <dana@example.com>", to=ALIAS,
                             subject="Re: ClawDad beta: you're in", references=[WELCOME], minutes=1)
        cody = self.cody_reply("c1", to="dana@example.com", subject="Re: ClawDad beta: you're in",
                               references=[WELCOME, "<t1@example.com>"], minutes=2)
        second = self.message("t2", sender="Dana Tester <dana@example.com>", to="cody@frg.earth",
                              subject="Re: ClawDad beta: you're in",
                              references=[WELCOME, "<t1@example.com>", "<lifeops-c1@frg.earth>"], minutes=3)
        copied = self.message("cc1", sender="Eli <eli@example.com>", external_cc=ALIAS, subject="Question", minutes=4)
        # Same person and subject with no header link: a suggestion, never an automatic label.
        unrelated = self.message("u1", sender="Dana Tester <dana@example.com>",
                                 subject="Re: ClawDad beta: you're in", minutes=5)

        reasons = self.reasons()
        self.assertEqual({notification, first, cody, second, copied}, set(reasons))
        self.assertEqual(f"Sent to {ALIAS}", reasons[first])
        self.assertEqual(mail_labels.REASON_CONVERSATION, reasons[cody])
        self.assertEqual(mail_labels.REASON_CONVERSATION, reasons[second])
        self.assertEqual(f"Cc {ALIAS}", reasons[copied])
        self.assertEqual(set(reasons), self.labeled(identity="frg"))
        self.assertEqual({"frg"}, {message["mail_identity"] for message in self.overview("clawdad")["messages"]})
        everything = {message["id"] for message in self.overview()["messages"]}
        self.assertTrue(set(reasons) | {unrelated} <= everything)
        suggestions = mail_ui._label_suggestions_from_connection(self.connection, label="clawdad")["suggestions"]
        self.assertEqual([unrelated], [suggestion["communication_id"] for suggestion in suggestions])

    def test_envelope_recipient_is_kept_from_ingest_through_replay(self):
        email = EmailMessage()
        email["From"] = "Fran Tester <fran@example.com>"
        email["To"] = "team@example.com"
        email["Subject"] = "Blind copy for the beta"
        email["Message-ID"] = "<bcc-1@example.com>"
        email.set_content("The alias only appears in the envelope.")
        raw = email.as_bytes()
        payload = {
            "provider": "cloudflare-email-routing",
            "envelope_from": "fran@example.com",
            "envelope_to": ALIAS,
            "raw_base64": base64.b64encode(raw).decode("ascii"),
            "raw_size": len(raw),
        }
        communication_id = mail_ingest.ingest_cloudflare_email_payload(payload, connection=self.connection)["communication_id"]
        replay = {key: value for key, value in payload.items() if key != "envelope_to"}
        mail_ingest.ingest_cloudflare_email_payload(replay, connection=self.connection)
        row = store.get_communication_by_id(self.connection, communication_id)
        self.assertEqual(ALIAS, row["envelope_to"])
        self.assertEqual("frg", row["mail_identity"])
        mail_labels.create_label(self.connection, "ClawDad", [ALIAS])
        self.assertEqual({communication_id: f"Delivered to {ALIAS}"}, self.reasons())

    def test_manual_choices_override_rules_and_carry_to_later_replies(self):
        mail_labels.create_label(self.connection, "ClawDad", [ALIAS])
        matched = self.message("m1", to=ALIAS, subject="Beta question")
        loose = self.message("l1", sender="Gus <gus@example.com>", subject="Old thread about the beta")
        mail_labels.set_conversation(self.connection, "clawdad", communication_id=matched, action="remove")
        mail_labels.set_conversation(self.connection, "clawdad", communication_id=loose, action="add")
        self.assertEqual({loose}, self.labeled())
        loose_reply = self.message("l2", sender="Gus <gus@example.com>", references=["<l1@example.com>"], minutes=1)
        matched_reply = self.message("m2", to=ALIAS, references=["<m1@example.com>"], minutes=1)
        self.assertEqual({loose: mail_labels.REASON_ADDED, loose_reply: mail_labels.REASON_ADDED}, self.reasons())
        # The most recent choice in a conversation wins, whichever message it was made from.
        mail_labels.set_conversation(self.connection, "clawdad", communication_id=matched_reply, action="add")
        self.assertEqual({loose, loose_reply, matched, matched_reply}, self.labeled())
        mail_labels.set_conversation(self.connection, "clawdad", communication_id=matched, action="remove")
        self.assertEqual({loose, loose_reply}, self.labeled())
        for action in ["", "delete"]:
            with self.subTest(action=action), self.assertRaises(ValueError):
                mail_labels.set_conversation(self.connection, "clawdad", communication_id=matched, action=action)
        with self.assertRaises(LookupError):
            mail_labels.set_conversation(self.connection, "clawdad", communication_id=999999, action="add")

    def test_suggestions_name_the_label_and_dismissal_is_remembered(self):
        mail_labels.create_label(self.connection, "ClawDad", [ALIAS])
        listing = self.message("v1", sender="Viv <viv@example.dev>", subject="Listing ClawDad in our directory")
        answer = self.cody_reply("v2", to="viv@example.dev", subject="Re: Listing ClawDad in our directory",
                                 references=["<v1@example.com>"], minutes=1)
        self.message("clawdadish", sender="Other <other@example.com>", subject="clawdadish pun")
        suggestions = mail_ui._label_suggestions_from_connection(self.connection, label="clawdad")["suggestions"]
        self.assertEqual([(answer, 2)], [(item["communication_id"], item["message_count"]) for item in suggestions])
        self.assertEqual(1, self.overview()["labels"][0]["suggestion_count"])
        mail_labels.set_conversation(self.connection, "clawdad", communication_id=listing, action="remove")
        self.assertEqual([], mail_ui._label_suggestions_from_connection(self.connection, label="clawdad")["suggestions"])
        self.assertEqual(set(), self.labeled())

    def test_label_view_lists_labeled_mail_beyond_the_default_message_cap(self):
        mail_labels.create_label(self.connection, "ClawDad", [ALIAS])
        old = self.message("old", to=ALIAS, minutes=-1000)
        for index in range(90):
            self.message(f"new-{index}", sender=f"Sender {index} <s{index}@example.com>", minutes=index)
        default = self.overview(limit=80)
        self.assertNotIn(old, {message["id"] for message in default["messages"]})
        self.assertEqual({old}, self.labeled(limit=80))
        summary = default["labels"][0]
        self.assertEqual((1, 1, 1), (summary["contact_count"], summary["conversation_count"], summary["message_count"]))

    def test_spam_still_wins_and_labels_leave_mail_records_unchanged(self):
        mail_labels.create_label(self.connection, "ClawDad", [ALIAS])
        spam = self.message("spam", sender="Spammer <spam@example.com>", to=ALIAS)
        kept = self.message("kept", to=ALIAS)
        before = dict(store.get_communication_by_id(self.connection, kept))
        mail_spam.add_sender(self.connection, "spam@example.com")
        self.assertEqual({kept}, self.labeled())
        self.assertEqual({spam}, self.labeled(spam=True))
        mail_labels.set_conversation(self.connection, "clawdad", communication_id=kept, action="remove")
        mail_labels.set_conversation(self.connection, "clawdad", communication_id=kept, action="add")
        self.assertEqual(before, dict(store.get_communication_by_id(self.connection, kept)))

    def test_every_label_edit_changes_the_mailbox_refresh_digest(self):
        communication_id = self.message("d1")

        def digest():
            return mail_ui._mail_ui_state_version_from_connection(self.connection)["ui_state_digest"]

        digests = [digest()]
        mail_labels.create_label(self.connection, "ClawDad")
        digests.append(digest())
        rule = mail_labels.add_rule(self.connection, "clawdad", ALIAS)
        digests.append(digest())
        mail_labels.set_conversation(self.connection, "clawdad", communication_id=communication_id, action="add")
        digests.append(digest())
        mail_labels.remove_rule(self.connection, "clawdad", rule["id"])
        digests.append(digest())
        mail_labels.rename_label(self.connection, "clawdad", "ClawDad Beta")
        digests.append(digest())
        mail_labels.delete_label(self.connection, "clawdad-beta")
        digests.append(digest())
        for before, after in zip(digests, digests[1:]):
            self.assertNotEqual(before, after)

    def test_labels_persist_stay_unique_and_delete_cleanly(self):
        label = mail_labels.create_label(self.connection, "  ClawDad ", [ALIAS, ALIAS.upper(), "@frg.earth"])
        self.assertEqual(("ClawDad", "clawdad"), (label["name"], label["slug"]))
        self.assertEqual([ALIAS, "@frg.earth"], [rule["text"] for rule in label["rules"]])
        with self.assertRaises(ValueError):
            mail_labels.create_label(self.connection, "clawdad")
        other = mail_labels.create_label(self.connection, "Other")
        with self.assertRaises(ValueError):
            mail_labels.rename_label(self.connection, other["id"], "CLAWDAD")
        communication_id = self.message("p1")
        mail_labels.set_conversation(self.connection, "clawdad", communication_id=communication_id, action="add")
        self.connection.close()
        self.connection = store.open_db(self.db)
        self.assertEqual([ALIAS, "@frg.earth"], [rule["text"] for rule in mail_labels.get_label(self.connection, "clawdad")["rules"]])
        self.assertEqual({communication_id}, self.labeled())
        self.assertTrue(mail_labels.delete_label(self.connection, "clawdad"))
        self.assertFalse(mail_labels.delete_label(self.connection, "clawdad"))
        for table in ("mail_label_rules", "mail_label_conversations"):
            count = self.connection.execute(f"SELECT COUNT(*) FROM {table} WHERE label_id = ?", (label["id"],)).fetchone()[0]
            self.assertEqual(0, count)
        missing = self.overview("clawdad")
        self.assertEqual(("not_found", []), (missing["label_error"], missing["messages"]))

    def test_read_only_snapshots_from_before_the_migration_still_render(self):
        self.message("legacy")
        for table in ("mail_label_conversations", "mail_label_rules", "mail_labels"):
            self.connection.execute(f"DROP TABLE {table}")
        self.connection.execute("ALTER TABLE communications DROP COLUMN envelope_to")
        self.connection.commit()
        snapshot = sqlite3.connect(f"file:{self.db}?mode=ro", uri=True)
        snapshot.row_factory = sqlite3.Row
        snapshot.execute("PRAGMA query_only = ON")
        try:
            payload = mail_ui._build_correspondence_overview_from_connection(snapshot)
            self.assertEqual((1, []), (payload["message_count"], payload["labels"]))
            self.assertEqual(1, mail_ui._correspondence_mailbox_version_from_connection(snapshot)["message_count"])
        finally:
            snapshot.close()

    def test_page_includes_label_controls(self):
        html = mail_ui._render_mail_ui_html(None)
        for marker in ('id="labelSelect"', 'id="labelDialog"', 'id="conversationLabelDialog"',
                       'id="saveSearchLabelButton"', "data-label-conversation"):
            self.assertIn(marker, html)

    def test_http_label_lifecycle_filters_overview_and_guards_origin(self):
        notification = self.message("h1", sender=f"ClawDad <{ALIAS}>", subject="ClawDad signup")
        suggested = self.message("h2", sender="Viv <viv@example.dev>", subject="ClawDad listing", minutes=1)
        server = HTTPServer(("127.0.0.1", 0), mail_ui._make_handler(db_path=self.db, limit=20, enable_background_remote_sync=False))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        url = f"http://127.0.0.1:{server.server_port}"

        def request(path, payload=None, origin=None):
            headers = {"Content-Type": "application/json"}
            if origin:
                headers["Origin"] = origin
            data = json.dumps(payload).encode() if payload is not None else None
            with urllib.request.urlopen(urllib.request.Request(url + path, data=data, headers=headers)) as response:
                return json.load(response)

        def failure(path, payload=None, origin=None):
            with self.assertRaises(urllib.error.HTTPError) as caught:
                request(path, payload, origin)
            return caught.exception.code, json.loads(caught.exception.read())

        try:
            self.assertEqual(403, failure("/api/labels", {"name": "ClawDad"}, "https://evil.example")[0])
            self.assertEqual([], request("/api/labels")["labels"])
            code, body = failure("/api/labels", {"name": "ClawDad", "rules": ["x"]})
            self.assertEqual((400, "Keywords need at least two letters or numbers."), (code, body["error"]))
            created = request("/api/labels", {"name": "ClawDad", "rules": [ALIAS]})
            label_id = created["label"]["id"]
            self.assertEqual((1, 1), (created["labels"][0]["message_count"], created["labels"][0]["suggestion_count"]))
            view = request("/api/overview?source=correspondence&label=clawdad")
            self.assertEqual(([notification], "ClawDad"), ([message["id"] for message in view["messages"]], view["label"]["name"]))
            self.assertEqual(2, request("/api/overview?source=correspondence")["message_count"])
            suggestions = request(f"/api/labels/suggestions?label={label_id}")["suggestions"]
            self.assertEqual([suggested], [item["communication_id"] for item in suggestions])
            added = request("/api/labels/conversation", {"label": label_id, "communication_id": suggested, "action": "add"})
            self.assertEqual(2, added["labels"][0]["message_count"])
            rule = request("/api/labels/rules", {"label": label_id, "rule": "@example.dev"})["rule"]
            self.assertEqual(("domain", "@example.dev"), (rule["kind"], rule["text"]))
            self.assertTrue(request("/api/labels/rules/remove", {"label": label_id, "rule_id": rule["id"]})["changed"])
            renamed = request("/api/labels/update", {"label": label_id, "name": "ClawDad Beta"})
            self.assertEqual("clawdad-beta", renamed["label"]["slug"])
            self.assertEqual(404, failure("/api/labels/suggestions?label=missing")[0])
            self.assertEqual(404, failure("/api/labels/conversation", {"label": 999, "communication_id": notification, "action": "add"})[0])
            deleted = request("/api/labels/delete", {"label": label_id})
            self.assertEqual((True, []), (deleted["changed"], deleted["labels"]))
            self.assertEqual("not_found", request("/api/overview?source=correspondence&label=clawdad-beta")["label_error"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
