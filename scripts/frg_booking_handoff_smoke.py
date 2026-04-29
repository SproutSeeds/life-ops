#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


def _next_weekday(start: date) -> date:
    current = start
    while current.weekday() >= 5:
        current += timedelta(days=1)
    return current


def _source_id(booking_id: str) -> int:
    digest = hashlib.sha256(booking_id.encode("utf-8")).hexdigest()
    return int(digest[:15], 16)


def _build_payload(*, booking_id: str, booking_day: date) -> dict:
    return {
        "event": "booking.paid",
        "booking": {
            "id": booking_id,
            "name": "LifeOps Smoke Test",
            "email": "smoke+lifeops@frg.earth",
            "focus": "LifeOps booking handoff smoke",
            "durationMinutes": 30,
            "selectedDate": booking_day.isoformat(),
            "selectedTime": "1:00 PM",
            "selectedSlotLabel": f"{booking_day.isoformat()} 1:00 PM",
            "timezone": "America/Chicago",
            "notes": "Smoke test for signed FRG booking handoff into LifeOps.",
        },
        "payment": {
            "amountTotalCents": 100,
            "currency": "usd",
            "paymentStatus": "paid",
            "stripeCheckoutSessionId": f"cs_smoke_{booking_id.replace('-', '_')}",
        },
        "zoomUrl": "https://zoom.example/lifeops-smoke",
    }


def _signed_headers(raw_body: bytes, *, secret: str) -> dict[str, str]:
    timestamp = str(int(time.time()))
    signature = hmac.new(
        secret.encode("utf-8"),
        f"{timestamp}.".encode("utf-8") + raw_body,
        hashlib.sha256,
    ).hexdigest()
    return {
        "Content-Type": "application/json",
        "X-FRG-Booking-Timestamp": timestamp,
        "X-FRG-Booking-Signature": f"v1={signature}",
    }


def _post_payload(url: str, raw_body: bytes, headers: dict[str, str]) -> tuple[int, dict]:
    request = urllib.request.Request(url, data=raw_body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            body = response.read().decode("utf-8")
            return int(response.status), json.loads(body or "{}")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        try:
            payload = json.loads(body or "{}")
        except json.JSONDecodeError:
            payload = {"error": body}
        return int(exc.code), payload


def _verify_db(db_path: Path, *, booking_id: str) -> dict:
    source_id = _source_id(booking_id)
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            """
            SELECT id, entry_date, start_time, end_time, status, title, source, source_table, notes
            FROM calendar_entries
            WHERE source = 'frg_site_booking'
              AND source_table = 'stripe_checkout_sessions'
              AND source_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (source_id,),
        ).fetchone()
        if row is None:
            raise RuntimeError("LifeOps DB does not contain the smoke booking row")
        return dict(row)


def _cleanup_db(db_path: Path, *, entry_id: int) -> None:
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            UPDATE calendar_entries
            SET status = 'canceled', updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
              AND source = 'frg_site_booking'
              AND title LIKE 'FRG booking: LifeOps Smoke Test%'
            """,
            (entry_id,),
        )
        connection.commit()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Smoke-test the signed FRG booking handoff into LifeOps.")
    parser.add_argument("--url", default=os.environ.get("FRG_BOOKING_WEBHOOK_URL", "http://127.0.0.1:4311/api/frg/bookings"))
    parser.add_argument("--secret", default=os.environ.get("FRG_BOOKING_WEBHOOK_SECRET", ""))
    parser.add_argument("--db", type=Path, default=Path.home() / ".lifeops" / "data" / "cmail_runtime.db")
    parser.add_argument("--booking-id", default="")
    parser.add_argument("--date", dest="booking_date", default="")
    parser.add_argument("--dry-run", action="store_true", help="Build and sign the payload but do not POST it.")
    parser.add_argument("--cleanup", action="store_true", help="Mark the created smoke row canceled after verification.")
    args = parser.parse_args(argv)

    secret = str(args.secret or "").strip()
    if not secret:
        print("error: FRG_BOOKING_WEBHOOK_SECRET or --secret is required", file=sys.stderr)
        return 2

    booking_id = str(args.booking_id or "").strip()
    if not booking_id:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        booking_id = f"frg-booking-smoke-{stamp}"

    booking_day = date.fromisoformat(args.booking_date) if args.booking_date else _next_weekday(date.today() + timedelta(days=14))
    payload = _build_payload(booking_id=booking_id, booking_day=booking_day)
    raw_body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    headers = _signed_headers(raw_body, secret=secret)

    if args.dry_run:
        print(json.dumps({"ok": True, "dry_run": True, "booking_id": booking_id, "url": args.url}, indent=2))
        return 0

    status_code, response_payload = _post_payload(str(args.url), raw_body, headers)
    if status_code >= 400 or not response_payload.get("ok"):
        print(json.dumps({"ok": False, "status_code": status_code, "response": response_payload}, indent=2), file=sys.stderr)
        return 1

    row = _verify_db(args.db.expanduser(), booking_id=booking_id)
    if args.cleanup:
        _cleanup_db(args.db.expanduser(), entry_id=int(row["id"]))
        row["status"] = "canceled"

    print(
        json.dumps(
            {
                "ok": True,
                "booking_id": booking_id,
                "status_code": status_code,
                "calendar_entry": {
                    "id": row["id"],
                    "date": row["entry_date"],
                    "start_time": row["start_time"],
                    "end_time": row["end_time"],
                    "status": row["status"],
                    "title": row["title"],
                },
                "response": response_payload,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
