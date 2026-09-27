"""Local synthetic mailbox for browser QA. No provider sync or outbound sends."""
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))
from life_ops import mail_ui, store

with tempfile.TemporaryDirectory(prefix="cmail-spam-qa-") as fixture:
    os.environ["LIFE_OPS_HOME"] = fixture
    db = Path(fixture) / "fixture.db"
    with store.open_db(db) as connection:
        for index, sender in enumerate(["QA Offers <offers@example.invalid>", "QA Friend <friend@example.invalid>"]):
            store.upsert_communication_from_sync(connection, source="cloudflare_email", external_id=f"qa-{index}",
                subject="Synthetic Spam folder test" if index == 0 else "Synthetic friendly message",
                channel="email", happened_at=datetime(2026,9,23,18), follow_up_at=None,
                external_from=sender, external_to="cody@frg.earth", direction="inbound", status="open",
                body_text="Synthetic fixture only. No real correspondence or outbound delivery.",
                message_id=f"<qa-{index}@example.invalid>", thread_key=f"qa-{index}")
    print(f"fixture_db={db}", flush=True)
    mail_ui.serve_mail_ui(db_path=db, port=4318, enable_background_remote_sync=False, require_app_auth=False)
