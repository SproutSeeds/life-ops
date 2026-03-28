from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops import store
from life_ops.profile_context import extract_profile_context_items


class ProfileContextTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "profile-context-test.db"
        self.connection = store.open_db(self.db_path)

    def tearDown(self) -> None:
        self.connection.close()
        self.temp_dir.cleanup()

    def test_extract_profile_context_items_creates_identity_insurance_and_immigration_candidates(self) -> None:
        store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="thread:id-1",
            subject="Driver license photo for record keeping",
            channel="email",
            happened_at=datetime(2026, 3, 25, 8, 0),
            follow_up_at=None,
            status="reference",
            external_from="codyshanemitchell@gmail.com",
            body_text="Keeping a copy of my ID.",
            attachments=[{"filename": "drivers_license_front.jpg", "mime_type": "image/jpeg"}],
            category="identity",
            categories=["identity", "record_keeping"],
        )
        store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="thread:ins-1",
            subject="Ambetter member ID card",
            channel="email",
            happened_at=datetime(2026, 3, 25, 9, 0),
            follow_up_at=None,
            status="reference",
            external_from="member@ambetterhealth.softheon.com",
            body_text="Your insurance card and member ID are attached.",
            attachments=[{"filename": "insurance-card.pdf", "mime_type": "application/pdf"}],
            category="insurance",
            categories=["insurance"],
        )
        store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="thread:imm-1",
            subject="USCIS interview notice for Sisy",
            channel="email",
            happened_at=datetime(2026, 3, 25, 10, 0),
            follow_up_at=None,
            status="reference",
            external_from="updates@uscis.dhs.gov",
            body_text="Your wife Sisy has an immigration interview notice for Form I-485.",
            attachments=[{"filename": "i485-interview-notice.pdf", "mime_type": "application/pdf"}],
            category="immigration",
            categories=["immigration"],
        )
        store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="thread:noise-1",
            subject="My wife is pregnant and just accidentally ate papaya. Is there any way to save the baby?",
            channel="email",
            happened_at=datetime(2026, 3, 25, 11, 0),
            follow_up_at=None,
            status="reference",
            external_from="english-personalized-digest@quora.com",
            body_text="A generic digest that mentions a wife and a doctor but is not a personal record.",
            category="medical",
            categories=["medical"],
        )

        summary = extract_profile_context_items(self.connection)
        rows = store.list_profile_context_items(self.connection, limit=20)

        self.assertEqual(4, summary["communications_scanned"])
        self.assertEqual(3, summary["items_extracted"])
        self.assertEqual(3, len(rows))

        item_types = {str(row["item_type"]) for row in rows}
        subject_keys = {str(row["subject_key"]) for row in rows}

        self.assertIn("identity_document", item_types)
        self.assertIn("insurance_record", item_types)
        self.assertIn("immigration_record", item_types)
        self.assertIn("wife_sisy", subject_keys)

    def test_profile_context_filters_credit_card_visa_and_job_insurance_noise(self) -> None:
        store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="thread:visa-noise",
            subject="Upcoming Minimum Payment due alert",
            channel="email",
            happened_at=datetime(2026, 3, 25, 12, 0),
            follow_up_at=None,
            status="reference",
            external_from="CardServices@info6.citi.com",
            body_text="Your My Best Buy Visa Platinum minimum payment is due.",
            category="immigration",
            categories=["billing", "immigration"],
        )
        store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="thread:job-noise",
            subject="Bartender at Highgate and 10 more jobs in Pensacola, FL for you. Apply Now.",
            channel="email",
            happened_at=datetime(2026, 3, 25, 13, 0),
            follow_up_at=None,
            status="reference",
            external_from="noreply@glassdoor.com",
            body_text="Southeastern Insurance Group is hiring.",
            category="insurance",
            categories=["career", "insurance"],
        )

        summary = extract_profile_context_items(self.connection)

        self.assertEqual(2, summary["communications_scanned"])
        self.assertEqual(0, summary["items_extracted"])
        self.assertEqual([], store.list_profile_context_items(self.connection, limit=20))

    def test_attachment_beneficiary_text_does_not_auto_assign_wife_sisy(self) -> None:
        communication_id = store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="thread:ins-attachment-1",
            subject="Coverage documents are ready",
            channel="email",
            happened_at=datetime(2026, 3, 25, 14, 0),
            follow_up_at=None,
            status="reference",
            external_from="support@mymemberinfo.com",
            snippet="Member ID and policy information are attached.",
            attachments=[{"filename": "coverage.pdf", "mime_type": "application/pdf"}],
            category="insurance",
            categories=["insurance", "record_keeping"],
        )
        store.upsert_communication_attachment(
            self.connection,
            external_key="attachment:test:beneficiary",
            communication_id=communication_id,
            source="gmail",
            external_message_id="msg-1",
            external_attachment_id="att-1",
            part_id="2",
            filename="coverage.pdf",
            mime_type="application/pdf",
            size=128,
            relative_path="gmail/test/coverage.pdf",
            extracted_text="Member ID: X12345. Policy details enclosed. Beneficiary information on file.",
            extracted_text_path="gmail/test/coverage.pdf.txt",
            extraction_method="pdf_text",
            ingest_status="extracted",
            error_text="",
            sha256="abc",
        )

        extract_profile_context_items(self.connection)
        rows = store.list_profile_context_items(self.connection, item_type="insurance_record", limit=10)

        self.assertEqual(1, len(rows))
        self.assertEqual("self", str(rows[0]["subject_key"]))

    def test_tax_attachment_social_security_numbers_do_not_create_benefits_record(self) -> None:
        communication_id = store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="thread:tax-attachment-1",
            subject="Important Document - 1099B",
            channel="email",
            happened_at=datetime(2026, 3, 25, 15, 0),
            follow_up_at=None,
            status="reference",
            external_from="AMP@ampstatements.com",
            snippet="Please review your tax report for correct tax IDs and social security numbers.",
            attachments=[{"filename": "1099B.pdf", "mime_type": "application/pdf"}],
            category="tax",
            categories=["tax", "record_keeping"],
        )
        store.upsert_communication_attachment(
            self.connection,
            external_key="attachment:test:tax",
            communication_id=communication_id,
            source="gmail",
            external_message_id="msg-2",
            external_attachment_id="att-2",
            part_id="3",
            filename="1099B.pdf",
            mime_type="application/pdf",
            size=256,
            relative_path="gmail/test/1099B.pdf",
            extracted_text="The IRS may fine firms for incorrect tax IDs, social security numbers, and addresses.",
            extracted_text_path="gmail/test/1099B.pdf.txt",
            extraction_method="pdf_text",
            ingest_status="extracted",
            error_text="",
            sha256="def",
        )

        extract_profile_context_items(self.connection)
        rows = store.list_profile_context_items(self.connection, item_type="benefits_record", limit=10)

        self.assertEqual([], rows)

    def test_profile_context_ignores_low_signal_attachment_names_in_titles_and_evidence(self) -> None:
        communication_id = store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="thread:insurance-card-1",
            subject="Primary Card you Requested.",
            channel="email",
            happened_at=datetime(2026, 3, 25, 16, 0),
            follow_up_at=None,
            status="reference",
            external_from="support@mymemberinfo.com",
            snippet="Member ID and benefits information are attached.",
            attachments=[
                {"filename": "Logo", "mime_type": "text/html"},
                {"filename": "Member_AdminNotification.pdf", "mime_type": "application/pdf"},
            ],
            category="insurance",
            categories=["insurance", "record_keeping"],
        )
        store.upsert_communication_attachment(
            self.connection,
            external_key="attachment:test:logo",
            communication_id=communication_id,
            source="gmail",
            external_message_id="msg-3",
            external_attachment_id="att-3",
            part_id="3",
            filename="Logo",
            mime_type="text/html",
            size=40000,
            relative_path="gmail/test/logo",
            extracted_text="body { margin: 0; padding: 0; } table, tr, td { vertical-align: top; }",
            extracted_text_path="gmail/test/logo.txt",
            extraction_method="html_text",
            ingest_status="extracted",
            error_text="",
            sha256="ghi",
        )
        store.upsert_communication_attachment(
            self.connection,
            external_key="attachment:test:member-pdf",
            communication_id=communication_id,
            source="gmail",
            external_message_id="msg-3",
            external_attachment_id="att-4",
            part_id="4",
            filename="Member_AdminNotification.pdf",
            mime_type="application/pdf",
            size=2048,
            relative_path="gmail/test/member.pdf",
            extracted_text="Member ID: CRC2039998. Insurance card and coverage details enclosed.",
            extracted_text_path="gmail/test/member.pdf.txt",
            extraction_method="pdf_text",
            ingest_status="extracted",
            error_text="",
            sha256="jkl",
        )

        extract_profile_context_items(self.connection)
        rows = store.list_profile_context_items(self.connection, item_type="insurance_record", limit=10)

        self.assertEqual(1, len(rows))
        self.assertIn("Member_AdminNotification.pdf", str(rows[0]["title"]))
        self.assertNotIn("Logo", str(rows[0]["title"]))
        evidence = str(rows[0]["evidence_json"])
        self.assertIn("Member_AdminNotification.pdf", evidence)
        self.assertNotIn("\"Logo\"", evidence)


if __name__ == "__main__":
    unittest.main()
