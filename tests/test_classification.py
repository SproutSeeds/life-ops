from __future__ import annotations

import base64
import sys
import tempfile
import unittest
from datetime import date, datetime, time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops.agenda import build_agenda, render_agenda_text
from life_ops.classification import classify_message
from life_ops.google_sync import _gmail_attachment_metadata, _gmail_body_text
from life_ops import store


def _gmail_data(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii")


class ClassificationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "classification-test.db"
        self.connection = store.open_db(self.db_path)

    def tearDown(self) -> None:
        self.connection.close()
        self.temp_dir.cleanup()

    def test_self_sent_identity_attachment_becomes_reference_record(self) -> None:
        result = classify_message(
            subject="Driver license photo for record keeping",
            sender="Cody <codyshanemitchell@gmail.com>",
            to="codyshanemitchell@gmail.com",
            snippet="Keeping a copy of my ID photo attached.",
            attachments=[{"filename": "id_front.jpg", "mime_type": "image/jpeg"}],
            triage={"actionable": False, "score": 0},
            user_email="codyshanemitchell@gmail.com",
        )

        self.assertEqual("reference", result["status"])
        self.assertEqual("identity", result["primary_category"])
        self.assertIn("record_keeping", result["categories"])
        self.assertEqual("records", result["retention_bucket"])

    def test_tax_notice_with_action_language_becomes_open(self) -> None:
        result = classify_message(
            subject="IRS action required for your 1099 upload",
            sender="IRS <noreply@irs.gov>",
            to="codyshanemitchell@gmail.com",
            body_text="Please verify your tax return details before the deadline.",
            triage={"actionable": False, "score": 0},
            user_email="codyshanemitchell@gmail.com",
        )

        self.assertEqual("open", result["status"])
        self.assertEqual("tax", result["primary_category"])
        self.assertIn("tax", result["categories"])
        self.assertIn(result["priority_level"], {"high", "urgent"})

    def test_benefits_notice_is_categorized_for_records(self) -> None:
        result = classify_message(
            subject="SNAP benefits approval letter",
            sender="Benefits Office <caseworker@example.gov>",
            to="codyshanemitchell@gmail.com",
            body_text="Food stamps benefits letter attached for your records.",
            attachments=[{"filename": "snap-benefits-letter.pdf", "mime_type": "application/pdf"}],
            triage={"actionable": False, "score": 0},
            user_email="codyshanemitchell@gmail.com",
        )

        self.assertIn("benefits", result["categories"])
        self.assertEqual("records", result["retention_bucket"])
        self.assertEqual("reference", result["status"])

    def test_job_alert_is_career_reference(self) -> None:
        result = classify_message(
            subject="Job alert: Remote researcher roles in Portland, OR",
            sender="Indeed <donotreply@jobalert.indeed.com>",
            snippet="12 more jobs in Portland for your saved search.",
            triage={"actionable": False, "score": 0},
        )

        self.assertEqual("career", result["primary_category"])
        self.assertEqual("reference", result["status"])

    def test_meetup_confirmation_is_scheduling_not_insurance(self) -> None:
        result = classify_message(
            subject="Just scheduled: LA-AI Mobile Meetup",
            sender="Meetup <info@email.meetup.com>",
            snippet="Your meetup is confirmed for next Tuesday.",
            triage={"actionable": False, "score": 0},
        )

        self.assertEqual("scheduling", result["primary_category"])
        self.assertNotIn("insurance", result["categories"])

    def test_account_verification_stays_security(self) -> None:
        result = classify_message(
            subject="[ORCID] Verify your email address",
            sender="ORCID - Do not reply <donotreply@verify.orcid.org>",
            snippet="Please verify your email address to finish setting up your account.",
            triage={"actionable": True, "score": 2},
        )

        self.assertEqual("security", result["primary_category"])
        self.assertNotIn("insurance", result["categories"])

    def test_credit_score_notice_is_finance_reference(self) -> None:
        result = classify_message(
            subject="Action recommended: Cody's credit score has been impacted.",
            sender="Credit Karma <no-reply@creditkarma.com>",
            snippet="Review the change in your score.",
            triage={"actionable": False, "score": 0},
        )

        self.assertEqual("finance", result["primary_category"])
        self.assertEqual("financial_records", result["retention_bucket"])

    def test_order_summary_is_shopping_reference(self) -> None:
        result = classify_message(
            subject="Namecheap Order Summary (Order# 197755861)",
            sender="Namecheap Support <support@namecheap.com>",
            snippet="Your order summary is ready.",
            triage={"actionable": False, "score": 0},
        )

        self.assertEqual("shopping", result["primary_category"])
        self.assertEqual("reference", result["status"])

    def test_fedex_delivery_update_is_logistics_reference(self) -> None:
        result = classify_message(
            subject="FedEx Delivery Manager Profile Updated",
            sender="FedEx Delivery Manager <Notifications@fedex.com>",
            snippet="Your Delivery Manager profile has been updated.",
            triage={"actionable": False, "score": 0},
        )

        self.assertEqual("logistics", result["primary_category"])

    def test_turo_spring_break_email_is_travel_reference(self) -> None:
        result = classify_message(
            subject="Ride into spring break fun",
            sender="Turo <noreply@hello.turo.com>",
            snippet="Travel plans and rides for spring break.",
            triage={"actionable": False, "score": 0},
        )

        self.assertEqual("travel", result["primary_category"])

    def test_nvidia_program_email_is_developer_reference(self) -> None:
        result = classify_message(
            subject="Welcome to the NVIDIA Developer Program",
            sender="NVIDIA Developer Relations <noreply@nvidia.com>",
            snippet="Get started with the developer program and SDK updates.",
            triage={"actionable": False, "score": 0},
        )

        self.assertEqual("developer", result["primary_category"])

    def test_uscis_notice_is_immigration_reference(self) -> None:
        result = classify_message(
            subject="USCIS interview notice for Form I-485",
            sender="USCIS <updates@uscis.dhs.gov>",
            snippet="Your adjustment of status interview notice is available.",
            triage={"actionable": False, "score": 0},
        )

        self.assertEqual("immigration", result["primary_category"])

    def test_credit_card_visa_email_is_not_immigration(self) -> None:
        result = classify_message(
            subject="Upcoming Minimum Payment due alert",
            sender="Card Services <CardServices@info6.citi.com>",
            snippet="Your My Best Buy Visa Platinum minimum payment is due soon.",
            triage={"actionable": False, "score": 0},
        )

        self.assertNotIn("immigration", result["categories"])

    def test_lenovo_sale_email_is_shopping_reference(self) -> None:
        result = classify_message(
            subject="Big Savings. Priced to Move.",
            sender="Lenovo Overstock Sale <lenovo@ecomm.lenovo.com>",
            snippet="Deals end soon, shop now.",
            triage={"actionable": False, "score": 0},
        )

        self.assertEqual("shopping", result["primary_category"])

    def test_vet_promo_email_is_pets_reference(self) -> None:
        result = classify_message(
            subject="Shamrockin' Savings for Habanero!",
            sender="Cordova Animal Medical Center <cordovaanimalmedicalcenter@pets.vetcove.com>",
            snippet="Pot of Gold Finds for your pet.",
            triage={"actionable": False, "score": 0},
        )

        self.assertEqual("pets", result["primary_category"])

    def test_gmail_body_and_attachment_helpers_extract_expected_content(self) -> None:
        message = {
            "payload": {
                "mimeType": "multipart/mixed",
                "parts": [
                    {
                        "mimeType": "multipart/alternative",
                        "parts": [
                            {
                                "mimeType": "text/plain",
                                "filename": "",
                                "body": {"data": _gmail_data("Visit summary attached\nBring insurance card")},
                            },
                            {
                                "mimeType": "text/html",
                                "filename": "",
                                "body": {"data": _gmail_data("<p>Visit summary attached</p><p>Bring insurance card</p>")},
                            },
                        ],
                    },
                    {
                        "mimeType": "application/pdf",
                        "filename": "visit-summary.pdf",
                        "partId": "2",
                        "headers": [{"name": "Content-Disposition", "value": "attachment; filename=visit-summary.pdf"}],
                        "body": {"attachmentId": "att-1", "size": 2048},
                    },
                ],
            }
        }

        body_text = _gmail_body_text(message)
        attachments = _gmail_attachment_metadata(message)

        self.assertIn("Visit summary attached", body_text)
        self.assertIn("Bring insurance card", body_text)
        self.assertEqual("visit-summary.pdf", attachments[0]["filename"])
        self.assertEqual("application/pdf", attachments[0]["mime_type"])

    def test_agenda_render_shows_category_and_priority_for_followup(self) -> None:
        store.upsert_communication_from_sync(
            self.connection,
            source="gmail",
            external_id="thread:tax-1",
            subject="IRS action required for your 1099 upload",
            channel="email",
            happened_at=datetime.combine(date(2026, 3, 25), time(8, 0)),
            follow_up_at=datetime.combine(date(2026, 3, 25), time(10, 0)),
            status="open",
            category="tax",
            categories=["tax", "record_keeping"],
            priority_level="high",
            priority_score=70,
            retention_bucket="action_queue",
        )

        agenda = build_agenda(self.connection, start_day=date(2026, 3, 25), days=1)
        output = render_agenda_text(agenda)

        self.assertIn("Follow up: IRS action required for your 1099 upload [email, tax, high]", output)


if __name__ == "__main__":
    unittest.main()
