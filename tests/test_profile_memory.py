from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops import store
from life_ops.profile_context import extract_profile_context_items
from life_ops.profile_memory import (
    approve_profile_context_item,
    get_profile_record_payload,
    list_profile_alerts,
    merge_profile_context_item,
)


class ProfileMemoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "profile-memory-test.db"
        self.connection = store.open_db(self.db_path)

    def tearDown(self) -> None:
        self.connection.close()
        self.temp_dir.cleanup()

    def _seed_candidate(
        self,
        *,
        external_id: str,
        item_type: str,
        title: str,
        category: str,
        categories: list[str],
        attachment_filename: str = "",
        subject_key: str = "self",
        details: dict | None = None,
        evidence: list[dict] | None = None,
        confidence: int = 88,
        happened_at: datetime | None = None,
    ) -> tuple[int, int]:
        timestamp = happened_at or datetime(2026, 3, 26, 9, 0)
        attachments = []
        if attachment_filename:
            attachments.append({"filename": attachment_filename, "mime_type": "application/pdf"})

        communication_id = store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id=external_id,
            external_thread_id=external_id,
            subject=title,
            channel="email",
            happened_at=timestamp,
            follow_up_at=None,
            status="reference",
            external_from="records@example.com",
            snippet=f"Reference for {title}",
            attachments=attachments,
            category=category,
            categories=categories,
        )
        if attachment_filename:
            store.upsert_communication_attachment(
                self.connection,
                external_key=f"attachment:{external_id}",
                communication_id=communication_id,
                source="gmail",
                external_message_id=f"msg:{external_id}",
                external_attachment_id=f"att:{external_id}",
                part_id="2",
                filename=attachment_filename,
                mime_type="application/pdf",
                size=120,
                relative_path=f"gmail/test/{attachment_filename}",
                extracted_text=f"Extracted text for {title}",
                extracted_text_path=f"gmail/test/{attachment_filename}.txt",
                extraction_method="pdf_text",
                ingest_status="extracted",
                error_text="",
                sha256=f"sha:{external_id}",
            )

        item_id = store.upsert_profile_context_item(
            self.connection,
            external_key=f"profile:{external_id}",
            subject_key=subject_key,
            item_type=item_type,
            title=title,
            source="gmail",
            communication_id=communication_id,
            happened_at=timestamp,
            confidence=confidence,
            status="candidate",
            details=details or {},
            evidence=evidence or [{"kind": "subject", "text": title}],
        )
        return item_id, communication_id

    def test_approve_profile_item_creates_canonical_record_and_links_attachment(self) -> None:
        item_id, _ = self._seed_candidate(
            external_id="identity-1",
            item_type="identity_document",
            title="Driver license",
            category="identity",
            categories=["identity", "record_keeping"],
            attachment_filename="driver-license.pdf",
            details={"matched_strong_terms": ["driver license"]},
        )

        result = approve_profile_context_item(
            self.connection,
            item_id=item_id,
            notes="Verified from saved attachment.",
        )

        row = store.get_profile_context_item(self.connection, item_id)
        self.assertEqual("approved", str(row["status"]))
        self.assertEqual("self", result["subject_key"])
        self.assertEqual("document", result["record_kind"])
        self.assertEqual(1, len(result["linked_profile_items"]))
        self.assertEqual(1, len(result["linked_attachments"]))
        self.assertEqual("driver-license.pdf", result["linked_attachments"][0]["filename"])

    def test_merge_profile_item_into_existing_record_accumulates_links(self) -> None:
        first_item_id, _ = self._seed_candidate(
            external_id="ins-1",
            item_type="insurance_record",
            title="Insurance card",
            category="insurance",
            categories=["insurance", "record_keeping"],
            attachment_filename="insurance-card.pdf",
            details={"matched_strong_terms": ["insurance card"]},
            confidence=75,
        )
        second_item_id, _ = self._seed_candidate(
            external_id="ins-2",
            item_type="insurance_record",
            title="Insurance card replacement",
            category="insurance",
            categories=["insurance", "record_keeping"],
            attachment_filename="insurance-card-2026.pdf",
            details={"matched_strong_terms": ["member id", "policy number"]},
            confidence=92,
            happened_at=datetime(2026, 3, 27, 9, 0),
        )

        first_record = approve_profile_context_item(self.connection, item_id=first_item_id)
        merged = merge_profile_context_item(
            self.connection,
            item_id=second_item_id,
            record_id=int(first_record["id"]),
            notes="Merged the newer replacement card.",
        )

        second_item = store.get_profile_context_item(self.connection, second_item_id)
        self.assertEqual("approved", str(second_item["status"]))
        self.assertEqual(2, len(merged["linked_profile_items"]))
        self.assertEqual(2, len(merged["linked_attachments"]))
        self.assertEqual(92, merged["confidence"])
        self.assertIn("Merged the newer replacement card.", merged["notes"])

    def test_profile_alerts_flag_recent_immigration_records(self) -> None:
        item_id, _ = self._seed_candidate(
            external_id="imm-1",
            item_type="immigration_record",
            title="USCIS interview notice",
            category="immigration",
            categories=["immigration"],
            attachment_filename="uscis-interview-notice.pdf",
            details={"matched_strong_terms": ["interview notice", "uscis"]},
            confidence=95,
        )

        approved = approve_profile_context_item(self.connection, item_id=item_id)
        alerts = list_profile_alerts(self.connection)

        matching = [alert for alert in alerts if int(alert["record_id"]) == int(approved["id"])]
        self.assertTrue(matching)
        self.assertEqual("high", matching[0]["level"])
        self.assertIn("Matched terms", matching[0]["reason"])

    def test_extract_profile_context_preserves_reviewed_status_on_rerun(self) -> None:
        store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="thread:stable-insurance",
            external_thread_id="thread:stable-insurance",
            subject="Member card ready",
            channel="email",
            happened_at=datetime(2026, 3, 25, 10, 0),
            follow_up_at=None,
            status="reference",
            external_from="support@mymemberinfo.com",
            snippet="Insurance card attached.",
            attachments=[{"filename": "member-card.pdf", "mime_type": "application/pdf"}],
            category="insurance",
            categories=["insurance", "record_keeping"],
        )

        extract_profile_context_items(self.connection)
        first_row = store.next_profile_context_review_item(self.connection)
        self.assertIsNotNone(first_row)
        store.update_profile_context_item_status(
            self.connection,
            item_id=int(first_row["id"]),
            status="approved",
            review_notes="Already verified.",
        )

        extract_profile_context_items(self.connection)
        updated_row = store.get_profile_context_item(self.connection, int(first_row["id"]))

        self.assertIsNotNone(updated_row)
        self.assertEqual("approved", str(updated_row["status"]))
        self.assertEqual("Already verified.", str(updated_row["review_notes"]))

    def test_get_profile_record_payload_returns_linked_items_and_attachments(self) -> None:
        item_id, _ = self._seed_candidate(
            external_id="tax-1",
            item_type="tax_record",
            title="IRS 1099 package",
            category="tax",
            categories=["tax", "record_keeping"],
            attachment_filename="1099-package.pdf",
            details={"matched_strong_terms": ["1099", "irs"]},
        )
        approved = approve_profile_context_item(self.connection, item_id=item_id)

        payload = get_profile_record_payload(self.connection, int(approved["id"]))

        self.assertEqual("tax_record", payload["item_type"])
        self.assertEqual(1, len(payload["linked_profile_items"]))
        self.assertEqual(1, len(payload["linked_attachments"]))


if __name__ == "__main__":
    unittest.main()
