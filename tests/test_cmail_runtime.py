from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops import cmail_runtime
from life_ops import store
from life_ops import vault_crypto


class CmailRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.canonical_db_path = Path(self.temp_dir.name) / "life_ops.db"
        self.runtime_db_path = Path(self.temp_dir.name) / "cmail_runtime.db"
        self.original_master_key = os.environ.get(vault_crypto.MASTER_KEY_NAME)
        os.environ[vault_crypto.MASTER_KEY_NAME] = vault_crypto._b64url_encode(b"c" * 32)

    def tearDown(self) -> None:
        if self.original_master_key is None:
            os.environ.pop(vault_crypto.MASTER_KEY_NAME, None)
        else:
            os.environ[vault_crypto.MASTER_KEY_NAME] = self.original_master_key
        self.temp_dir.cleanup()

    def test_ensure_runtime_db_hydrates_from_canonical_store(self) -> None:
        with mock.patch("life_ops.store.default_db_path", return_value=self.canonical_db_path):
            with store.open_db(self.canonical_db_path) as connection:
                store.upsert_communication_from_sync(
                    connection,
                    source="cloudflare_email",
                    external_id="msg-100",
                    subject="Hydrated inbox item",
                    channel="email",
                    happened_at=datetime(2026, 3, 31, 8, 0, 0),
                    follow_up_at=None,
                    direction="inbound",
                    person="Alice Example",
                    status="reference",
                    external_from="Alice Example <alice@example.com>",
                    external_to="cody@frg.earth",
                    message_id="<msg-100@example.com>",
                    thread_key="<thread@example.com>",
                    body_text="Hydrated body",
                )

            result = cmail_runtime.ensure_cmail_runtime_db(
                runtime_db_path=self.runtime_db_path,
                canonical_db_path=self.canonical_db_path,
            )

            self.assertTrue(result["hydrated_from_canonical"])
            self.assertTrue(self.runtime_db_path.exists())
            with store.open_db(self.runtime_db_path) as connection:
                row = connection.execute(
                    "SELECT subject FROM communications WHERE external_id = ?",
                    ("msg-100",),
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual("Hydrated inbox item", str(row["subject"]))

    def test_seal_runtime_db_persists_runtime_changes_back_to_canonical_store(self) -> None:
        with mock.patch("life_ops.store.default_db_path", return_value=self.canonical_db_path):
            with store.open_db(self.canonical_db_path) as connection:
                connection.commit()

            cmail_runtime.ensure_cmail_runtime_db(
                runtime_db_path=self.runtime_db_path,
                canonical_db_path=self.canonical_db_path,
            )
            with store.open_db(self.runtime_db_path) as connection:
                store.upsert_communication_from_sync(
                    connection,
                    source="cmail_draft",
                    external_id="draft-200",
                    subject="Runtime draft",
                    channel="email",
                    happened_at=datetime(2026, 3, 31, 9, 0, 0),
                    follow_up_at=None,
                    direction="outbound",
                    person="",
                    status="draft",
                    body_text="Draft body",
                )

            result = cmail_runtime.seal_cmail_runtime_db(
                runtime_db_path=self.runtime_db_path,
                canonical_db_path=self.canonical_db_path,
            )

            self.assertTrue(result["sealed"])
            with store.open_db(self.canonical_db_path) as connection:
                row = connection.execute(
                    "SELECT subject FROM communications WHERE external_id = ?",
                    ("draft-200",),
                ).fetchone()
                self.assertIsNotNone(row)
                self.assertEqual("Runtime draft", str(row["subject"]))

    def test_default_cmail_db_resolution_uses_runtime_path_for_default_store(self) -> None:
        with mock.patch("life_ops.store.default_db_path", return_value=self.canonical_db_path):
            resolved = cmail_runtime.resolve_cmail_db_path(self.canonical_db_path)
            self.assertEqual(
                cmail_runtime.default_cmail_runtime_db_path().resolve(strict=False),
                resolved.resolve(strict=False),
            )

    def test_runtime_worker_failure_records_alert_and_recovery_clears_it(self) -> None:
        with mock.patch("life_ops.store.default_db_path", return_value=self.canonical_db_path):
            cmail_runtime.ensure_cmail_runtime_db(
                runtime_db_path=self.runtime_db_path,
                canonical_db_path=self.canonical_db_path,
            )

            for count in range(1, cmail_runtime.DEFAULT_CMAIL_RUNTIME_ALERT_THRESHOLD + 1):
                cmail_runtime._record_runtime_worker_failure(
                    runtime_db_path=self.runtime_db_path,
                    worker_name="sync",
                    exc=RuntimeError("boom"),
                    consecutive_failures=count,
                )

            with store.open_db(self.runtime_db_path) as connection:
                self.assertEqual(
                    str(cmail_runtime.DEFAULT_CMAIL_RUNTIME_ALERT_THRESHOLD),
                    store.get_sync_state(connection, "cmail_runtime:sync:consecutive_failures"),
                )
                alerts = store.list_system_alerts(connection, source="cmail_runtime", status="active", limit=10)
            self.assertEqual(1, len(alerts))
            self.assertEqual("cmail_runtime:sync", str(alerts[0]["alert_key"]))

            cmail_runtime._record_runtime_worker_recovery(
                runtime_db_path=self.runtime_db_path,
                worker_name="sync",
            )

            with store.open_db(self.runtime_db_path) as connection:
                self.assertEqual("0", store.get_sync_state(connection, "cmail_runtime:sync:consecutive_failures"))
                self.assertIsNotNone(store.get_sync_state(connection, "cmail_runtime:sync:last_success_at"))
                cleared_alerts = store.list_system_alerts(connection, source="cmail_runtime", status="active", limit=10)
            self.assertEqual([], cleared_alerts)

    def test_runtime_list_items_can_be_backfilled_from_existing_canonical_store(self) -> None:
        with mock.patch("life_ops.store.default_db_path", return_value=self.canonical_db_path):
            with store.open_db(self.canonical_db_path) as connection:
                connection.commit()

            cmail_runtime.ensure_cmail_runtime_db(
                runtime_db_path=self.runtime_db_path,
                canonical_db_path=self.canonical_db_path,
            )

            with store.open_db(self.canonical_db_path) as connection:
                canonical_item_id = store.add_list_item(
                    connection,
                    list_name="personal",
                    title="Buy new dish sponge",
                )

            result = cmail_runtime.ensure_cmail_runtime_list_items(
                runtime_db_path=self.runtime_db_path,
                canonical_db_path=self.canonical_db_path,
            )

            self.assertTrue(result["merged"])
            self.assertEqual(1, result["merged_count"])
            with store.open_db(self.runtime_db_path) as connection:
                row = store.get_list_item(connection, canonical_item_id)
                hydrated_at = store.get_sync_state(
                    connection,
                    cmail_runtime.DEFAULT_CMAIL_RUNTIME_LIST_ITEMS_HYDRATED_SYNC_KEY,
                )
            self.assertIsNotNone(row)
            self.assertEqual("Buy new dish sponge", str(row["title"]))
            self.assertTrue(hydrated_at)

    def test_seal_runtime_db_merges_canonical_list_items_before_persisting(self) -> None:
        with mock.patch("life_ops.store.default_db_path", return_value=self.canonical_db_path):
            with store.open_db(self.canonical_db_path) as connection:
                canonical_item_id = store.add_list_item(
                    connection,
                    list_name="personal",
                    title="Buy new dish sponge",
                )

            cmail_runtime.ensure_cmail_runtime_db(
                runtime_db_path=self.runtime_db_path,
                canonical_db_path=self.canonical_db_path,
            )

            with store.open_db(self.runtime_db_path) as connection:
                connection.execute("DELETE FROM list_items")
                connection.commit()

            result = cmail_runtime.seal_cmail_runtime_db(
                runtime_db_path=self.runtime_db_path,
                canonical_db_path=self.canonical_db_path,
            )

            self.assertTrue(result["sealed"])
            with store.open_db(self.runtime_db_path) as runtime_connection:
                runtime_row = store.get_list_item(runtime_connection, canonical_item_id)
            with store.open_db(self.canonical_db_path) as canonical_connection:
                canonical_row = store.get_list_item(canonical_connection, canonical_item_id)
            self.assertIsNotNone(runtime_row)
            self.assertEqual("Buy new dish sponge", str(runtime_row["title"]))
            self.assertIsNotNone(canonical_row)
            self.assertEqual("Buy new dish sponge", str(canonical_row["title"]))
