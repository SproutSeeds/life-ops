from __future__ import annotations

import base64
import sys
import tempfile
import unittest
import zipfile
from datetime import datetime
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops import store
from life_ops.document_ingest import (
    backfill_profile_attachments,
    backfill_profile_attachments_until_exhausted,
    extract_text_from_saved_attachment,
    ingest_profile_attachments,
)
from life_ops.profile_context import extract_profile_context_items


def _gmail_data(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii")


class _StaticRequest:
    def __init__(self, payload):
        self._payload = payload

    def execute(self):
        return self._payload


class _FakeGmailAttachmentsResource:
    def __init__(self, attachment_payloads):
        self._attachment_payloads = attachment_payloads

    def get(self, *, userId: str, messageId: str, id: str):
        return _StaticRequest({"data": self._attachment_payloads[(messageId, id)]})


class _FakeGmailMessagesResource:
    def __init__(self, attachment_payloads):
        self._attachment_payloads = attachment_payloads

    def attachments(self):
        return _FakeGmailAttachmentsResource(self._attachment_payloads)


class _FakeGmailThreadsResource:
    def __init__(self, threads):
        self._threads = threads

    def get(self, *, userId: str, id: str, format: str):
        return _StaticRequest(self._threads[id])


class _FakeGmailUsersResource:
    def __init__(self, threads, attachment_payloads):
        self._threads = threads
        self._attachment_payloads = attachment_payloads

    def threads(self):
        return _FakeGmailThreadsResource(self._threads)

    def messages(self):
        return _FakeGmailMessagesResource(self._attachment_payloads)


class _FakeGmailService:
    def __init__(self, threads, attachment_payloads):
        self._threads = threads
        self._attachment_payloads = attachment_payloads

    def users(self):
        return _FakeGmailUsersResource(self._threads, self._attachment_payloads)


class DocumentIngestTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "document-ingest-test.db"
        self.vault_root = Path(self.temp_dir.name) / "attachment-vault"
        self.connection = store.open_db(self.db_path)

    def tearDown(self) -> None:
        self.connection.close()
        self.temp_dir.cleanup()

    def _seed_attachment_thread(
        self,
        *,
        attachment_specs: list[dict[str, str | int]],
        category: str = "record_keeping",
        categories: list[str] | None = None,
        create_profile_candidate: bool = True,
    ) -> tuple[int, _FakeGmailService]:
        attachments = [
            {
                "filename": str(spec["filename"]),
                "mime_type": str(spec["mime_type"]),
                "attachment_id": str(spec["attachment_id"]),
                "part_id": str(spec["part_id"]),
            }
            for spec in attachment_specs
        ]
        communication_id = store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="thread:thread-1",
            external_thread_id="thread-1",
            subject="Record attached",
            channel="email",
            happened_at=datetime(2026, 3, 25, 10, 0),
            follow_up_at=None,
            status="reference",
            external_from="codyshanemitchell@gmail.com",
            snippet="Please keep this for reference.",
            attachments=attachments,
            category=category,
            categories=categories or [category],
        )
        if create_profile_candidate:
            store.upsert_profile_context_item(
                self.connection,
                external_key="profile:test:seed",
                subject_key="self",
                item_type="tax_record",
                title="Seed candidate",
                source="gmail",
                communication_id=communication_id,
                happened_at=datetime(2026, 3, 25, 10, 0),
                confidence=20,
                status="candidate",
                details={"seed": True},
                evidence=[],
            )

        service = _FakeGmailService(
            threads={
                "thread-1": {
                    "messages": [
                        {
                            "id": "msg-1",
                            "threadId": "thread-1",
                            "snippet": "Please keep this for reference.",
                            "internalDate": str(int(datetime(2026, 3, 25, 10, 0).timestamp() * 1000)),
                            "payload": {
                                "headers": [{"name": "Subject", "value": "Record attached"}],
                                "parts": [
                                    {
                                        "filename": str(spec["filename"]),
                                        "mimeType": str(spec["mime_type"]),
                                        "partId": str(spec["part_id"]),
                                        "body": {
                                            "attachmentId": str(spec["attachment_id"]),
                                            "size": int(spec["size"]),
                                        },
                                    }
                                    for spec in attachment_specs
                                ],
                            },
                        }
                    ]
                }
            },
            attachment_payloads={
                ("msg-1", str(spec["attachment_id"])): _gmail_data(str(spec["text"]))
                for spec in attachment_specs
            },
        )
        return communication_id, service

    def test_ingest_profile_attachments_downloads_and_extracts_text(self) -> None:
        communication_id, service = self._seed_attachment_thread(
            attachment_specs=[
                {
                    "filename": "passport.txt",
                    "mime_type": "text/plain",
                    "attachment_id": "att-1",
                    "part_id": "2",
                    "size": 56,
                    "text": "Passport number and driver license copy for record keeping.",
                }
            ]
        )

        summary = ingest_profile_attachments(
            self.connection,
            service=service,
            vault_root=self.vault_root,
            limit=10,
        )

        rows = store.list_communication_attachments(
            self.connection,
            communication_id=communication_id,
            limit=10,
        )

        self.assertEqual(1, summary["communications_scanned"])
        self.assertEqual(1, summary["attachments_saved"])
        self.assertEqual(1, summary["attachments_extracted"])
        self.assertEqual(1, len(rows))
        self.assertEqual("extracted", rows[0]["ingest_status"])
        self.assertIn("passport", str(rows[0]["extracted_text"]).lower())
        self.assertTrue((self.vault_root / str(rows[0]["relative_path"])).exists())

    def test_profile_context_can_use_ingested_attachment_text(self) -> None:
        _, service = self._seed_attachment_thread(
            attachment_specs=[
                {
                    "filename": "passport.txt",
                    "mime_type": "text/plain",
                    "attachment_id": "att-1",
                    "part_id": "2",
                    "size": 62,
                    "text": "Driver license and birth certificate enclosed for your records.",
                }
            ]
        )

        ingest_profile_attachments(
            self.connection,
            service=service,
            vault_root=self.vault_root,
            limit=10,
        )
        summary = extract_profile_context_items(self.connection)
        rows = store.list_profile_context_items(
            self.connection,
            item_type="identity_document",
            limit=10,
        )

        self.assertEqual(1, summary["by_item_type"]["identity_document"])
        self.assertEqual(1, len(rows))
        self.assertIn("identity_document", str(rows[0]["item_type"]))

    def test_profile_context_status_can_be_reviewed(self) -> None:
        item_id = store.upsert_profile_context_item(
            self.connection,
            external_key="profile:test:review",
            subject_key="self",
            item_type="identity_document",
            title="Driver license",
            source="gmail",
            communication_id=None,
            happened_at=datetime(2026, 3, 25, 12, 0),
            confidence=90,
            status="candidate",
            details={},
            evidence=[],
        )

        store.update_profile_context_item_status(
            self.connection,
            item_id=item_id,
            status="approved",
            review_notes="Confirmed from attachment review.",
        )
        row = store.get_profile_context_item(self.connection, item_id)

        self.assertEqual("approved", row["status"])
        self.assertEqual("Confirmed from attachment review.", row["review_notes"])
        self.assertIsNotNone(row["reviewed_at"])

    def test_ingest_profile_attachments_skips_decorative_template_assets(self) -> None:
        communication_id, service = self._seed_attachment_thread(
            attachment_specs=[
                {
                    "filename": "passport.txt",
                    "mime_type": "text/plain",
                    "attachment_id": "att-1",
                    "part_id": "2",
                    "size": 44,
                    "text": "Passport copy for identity verification.",
                },
                {
                    "filename": "top-bar-v2.png",
                    "mime_type": "image/png",
                    "attachment_id": "att-2",
                    "part_id": "3",
                    "size": 1024,
                    "text": "not a real document",
                },
            ]
        )

        summary = ingest_profile_attachments(
            self.connection,
            service=service,
            vault_root=self.vault_root,
            limit=10,
        )
        rows = store.list_communication_attachments(
            self.connection,
            communication_id=communication_id,
            limit=10,
        )

        self.assertEqual(2, summary["attachments_seen"])
        self.assertEqual(1, summary["attachments_saved"])
        self.assertEqual(1, summary["attachments_extracted"])
        self.assertEqual(1, summary["attachments_skipped_filtered"])
        self.assertEqual(["passport.txt"], [str(row["filename"]) for row in rows])

    def test_ingest_profile_attachments_sensitive_scope_reaches_beyond_profile_candidates(self) -> None:
        communication_id, service = self._seed_attachment_thread(
            attachment_specs=[
                {
                    "filename": "insurance-card.txt",
                    "mime_type": "text/plain",
                    "attachment_id": "att-1",
                    "part_id": "2",
                    "size": 54,
                    "text": "Insurance card with member number and coverage details.",
                }
            ],
            category="insurance",
            categories=["insurance", "record_keeping"],
            create_profile_candidate=False,
        )

        summary = ingest_profile_attachments(
            self.connection,
            service=service,
            vault_root=self.vault_root,
            scope="sensitive",
            limit=10,
        )
        rows = store.list_communication_attachments(
            self.connection,
            communication_id=communication_id,
            limit=10,
        )

        self.assertEqual(1, summary["communications_scanned"])
        self.assertEqual(1, summary["attachments_saved"])
        self.assertEqual(1, len(rows))
        self.assertEqual("insurance-card.txt", str(rows[0]["filename"]))

    def test_extract_text_from_saved_attachment_summarizes_zip_archive(self) -> None:
        archive_path = Path(self.temp_dir.name) / "sample.zip"
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr("notes.txt", "hello world")
            archive.writestr("src/example.py", "print('hi')\n")

        extracted_text, method = extract_text_from_saved_attachment(
            archive_path,
            mime_type="application/zip",
        )

        self.assertEqual("archive_summary", method)
        self.assertIn("ZIP archive", extracted_text)
        self.assertIn("notes.txt", extracted_text)
        self.assertIn("src/example.py", extracted_text)

    def test_extract_text_from_saved_attachment_summarizes_code_file(self) -> None:
        code_path = Path(self.temp_dir.name) / "hello.py"
        code_path.write_text("#!/usr/bin/env python3\nprint('hello')\n", encoding="utf-8")

        extracted_text, method = extract_text_from_saved_attachment(
            code_path,
            mime_type="text/x-python",
        )

        self.assertEqual("code_summary", method)
        self.assertIn("Code file (py)", extracted_text)
        self.assertIn("shebang:", extracted_text)
        self.assertIn("print('hello')", extracted_text)

    def test_extract_text_from_saved_attachment_summarizes_binary_and_media_files(self) -> None:
        exe_path = Path(self.temp_dir.name) / "tool.exe"
        exe_path.write_bytes(b"MZ" + bytes(range(32)))
        audio_path = Path(self.temp_dir.name) / "clip.mp3"
        audio_path.write_bytes(b"ID3" + bytes(range(32)))

        exe_text, exe_method = extract_text_from_saved_attachment(
            exe_path,
            mime_type="application/x-msdownload",
        )
        audio_text, audio_method = extract_text_from_saved_attachment(
            audio_path,
            mime_type="audio/mpeg",
        )

        self.assertEqual("binary_summary", exe_method)
        self.assertIn("PE/Windows executable", exe_text)
        self.assertIn("sha256:", exe_text)
        self.assertEqual("media_summary", audio_method)
        self.assertIn("Media file", audio_text)
        self.assertIn("audio/mpeg", audio_text)

    def test_extract_text_from_saved_attachment_falls_back_when_pdftotext_missing(self) -> None:
        pdf_path = Path(self.temp_dir.name) / "sample.pdf"
        pdf_path.write_bytes(b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\n")

        with mock.patch("life_ops.document_ingest.subprocess.run", side_effect=FileNotFoundError("pdftotext")):
            extracted_text, method = extract_text_from_saved_attachment(
                pdf_path,
                mime_type="application/pdf",
            )

        self.assertEqual("pdf_summary", method)
        self.assertIn("PDF attachment (text extraction unavailable)", extracted_text)
        self.assertIn("sha256:", extracted_text)

    def test_backfill_profile_attachments_resumes_with_local_cursor(self) -> None:
        newer_id = store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="thread:thread-1",
            external_thread_id="thread-1",
            subject="Newer record",
            channel="email",
            happened_at=datetime(2026, 3, 25, 11, 0),
            follow_up_at=None,
            status="reference",
            external_from="codyshanemitchell@gmail.com",
            snippet="Newer insurance record.",
            attachments=[{"filename": "newer-card.txt", "mime_type": "text/plain", "attachment_id": "att-1", "part_id": "2"}],
            category="insurance",
            categories=["insurance", "record_keeping"],
        )
        older_id = store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="thread:thread-2",
            external_thread_id="thread-2",
            subject="Older record",
            channel="email",
            happened_at=datetime(2026, 3, 24, 11, 0),
            follow_up_at=None,
            status="reference",
            external_from="codyshanemitchell@gmail.com",
            snippet="Older insurance record.",
            attachments=[{"filename": "older-card.txt", "mime_type": "text/plain", "attachment_id": "att-2", "part_id": "2"}],
            category="insurance",
            categories=["insurance", "record_keeping"],
        )
        service = _FakeGmailService(
            threads={
                "thread-1": {
                    "messages": [
                        {
                            "id": "msg-1",
                            "threadId": "thread-1",
                            "snippet": "Newer insurance record.",
                            "internalDate": str(int(datetime(2026, 3, 25, 11, 0).timestamp() * 1000)),
                            "payload": {
                                "headers": [{"name": "Subject", "value": "Newer record"}],
                                "parts": [
                                    {
                                        "filename": "newer-card.txt",
                                        "mimeType": "text/plain",
                                        "partId": "2",
                                        "body": {"attachmentId": "att-1", "size": 30},
                                    }
                                ],
                            },
                        }
                    ]
                },
                "thread-2": {
                    "messages": [
                        {
                            "id": "msg-2",
                            "threadId": "thread-2",
                            "snippet": "Older insurance record.",
                            "internalDate": str(int(datetime(2026, 3, 24, 11, 0).timestamp() * 1000)),
                            "payload": {
                                "headers": [{"name": "Subject", "value": "Older record"}],
                                "parts": [
                                    {
                                        "filename": "older-card.txt",
                                        "mimeType": "text/plain",
                                        "partId": "2",
                                        "body": {"attachmentId": "att-2", "size": 30},
                                    }
                                ],
                            },
                        }
                    ]
                },
            },
            attachment_payloads={
                ("msg-1", "att-1"): _gmail_data("newer attachment"),
                ("msg-2", "att-2"): _gmail_data("older attachment"),
            },
        )

        first = backfill_profile_attachments(
            self.connection,
            service=service,
            vault_root=self.vault_root,
            max_results=1,
        )
        second = backfill_profile_attachments(
            self.connection,
            service=service,
            vault_root=self.vault_root,
            max_results=1,
        )

        self.assertEqual(1, first["communications_scanned"])
        self.assertEqual(1, first["attachments_saved"])
        self.assertFalse(first["backfill_exhausted"])
        self.assertEqual(newer_id, first["next_communication_id"])
        self.assertEqual(1, second["communications_scanned"])
        self.assertEqual(1, second["attachments_saved"])
        self.assertTrue(second["backfill_exhausted"])
        self.assertEqual("", store.get_sync_state(self.connection, "profile_attachment_backfill:next_communication_id"))
        rows = store.list_communication_attachments(self.connection, limit=10)
        self.assertEqual({newer_id, older_id}, {int(row["communication_id"]) for row in rows})

    def test_backfill_profile_attachments_until_exhausted_aggregates_runs(self) -> None:
        _, service = self._seed_attachment_thread(
            attachment_specs=[
                {
                    "filename": "insurance-card.txt",
                    "mime_type": "text/plain",
                    "attachment_id": "att-1",
                    "part_id": "2",
                    "size": 54,
                    "text": "Insurance card with member number and coverage details.",
                }
            ],
            category="insurance",
            categories=["insurance", "record_keeping"],
            create_profile_candidate=False,
        )

        summary = backfill_profile_attachments_until_exhausted(
            self.connection,
            service=service,
            vault_root=self.vault_root,
            max_results=10,
            max_runs=None,
        )

        self.assertEqual(1, summary["runs_completed"])
        self.assertEqual(1, summary["communications_scanned"])
        self.assertEqual(1, summary["attachments_saved"])
        self.assertTrue(summary["backfill_exhausted"])
        self.assertEqual("backfill_exhausted", summary["stop_reason"])


if __name__ == "__main__":
    unittest.main()
