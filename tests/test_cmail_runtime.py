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
        self.original_life_ops_home = os.environ.get(store.LIFE_OPS_HOME_ENV)
        self.original_legacy_home = os.environ.get(cmail_runtime.DEFAULT_CMAIL_LEGACY_HOME_ENV)
        self.original_backup_root = os.environ.get(cmail_runtime.DEFAULT_CMAIL_EXTERNAL_BACKUP_ROOT_ENV)
        self.original_runtime_db = os.environ.get(cmail_runtime.DEFAULT_CMAIL_RUNTIME_DB_ENV)
        self.original_allow_db_mismatch = os.environ.get(cmail_runtime.DEFAULT_CMAIL_ALLOW_DB_MISMATCH_ENV)
        os.environ[vault_crypto.MASTER_KEY_NAME] = vault_crypto._b64url_encode(b"c" * 32)

    def tearDown(self) -> None:
        if self.original_master_key is None:
            os.environ.pop(vault_crypto.MASTER_KEY_NAME, None)
        else:
            os.environ[vault_crypto.MASTER_KEY_NAME] = self.original_master_key
        if self.original_life_ops_home is None:
            os.environ.pop(store.LIFE_OPS_HOME_ENV, None)
        else:
            os.environ[store.LIFE_OPS_HOME_ENV] = self.original_life_ops_home
        if self.original_legacy_home is None:
            os.environ.pop(cmail_runtime.DEFAULT_CMAIL_LEGACY_HOME_ENV, None)
        else:
            os.environ[cmail_runtime.DEFAULT_CMAIL_LEGACY_HOME_ENV] = self.original_legacy_home
        if self.original_backup_root is None:
            os.environ.pop(cmail_runtime.DEFAULT_CMAIL_EXTERNAL_BACKUP_ROOT_ENV, None)
        else:
            os.environ[cmail_runtime.DEFAULT_CMAIL_EXTERNAL_BACKUP_ROOT_ENV] = self.original_backup_root
        if self.original_runtime_db is None:
            os.environ.pop(cmail_runtime.DEFAULT_CMAIL_RUNTIME_DB_ENV, None)
        else:
            os.environ[cmail_runtime.DEFAULT_CMAIL_RUNTIME_DB_ENV] = self.original_runtime_db
        if self.original_allow_db_mismatch is None:
            os.environ.pop(cmail_runtime.DEFAULT_CMAIL_ALLOW_DB_MISMATCH_ENV, None)
        else:
            os.environ[cmail_runtime.DEFAULT_CMAIL_ALLOW_DB_MISMATCH_ENV] = self.original_allow_db_mismatch
        self.temp_dir.cleanup()

    def test_ensure_cmail_app_secret_creates_reuses_and_rotates_unlock_code(self) -> None:
        stored: dict[str, str] = {}

        def fake_resolve_secret(*, name: str, path=None) -> str | None:
            return stored.get(name)

        def fake_set_secret(
            *,
            name: str,
            value: str,
            backend: str = "auto",
            path=None,
            allow_insecure_file_backend: bool = False,
        ) -> dict[str, str]:
            stored[name] = value
            return {"name": name, "backend": backend, "registry_path": "/tmp/keys.json"}

        with mock.patch("life_ops.cmail_runtime.credentials.resolve_secret", side_effect=fake_resolve_secret):
            with mock.patch("life_ops.cmail_runtime.credentials.set_secret", side_effect=fake_set_secret):
                created = cmail_runtime.ensure_cmail_app_secret()
                self.assertTrue(created["created"])
                self.assertEqual(cmail_runtime.CMAIL_APP_SECRET_NAME, created["name"])
                self.assertEqual(created["secret"], stored[cmail_runtime.CMAIL_APP_SECRET_NAME])

                reused = cmail_runtime.ensure_cmail_app_secret()
                self.assertFalse(reused["created"])
                self.assertFalse(reused["rotated"])
                self.assertEqual(created["secret"], reused["secret"])

                rotated = cmail_runtime.ensure_cmail_app_secret(rotate=True, value="new-code")
                self.assertTrue(rotated["rotated"])
                self.assertEqual("new-code", rotated["secret"])
                self.assertEqual("new-code", stored[cmail_runtime.CMAIL_APP_SECRET_NAME])

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

    def test_default_cmail_runtime_path_uses_home_state_root_without_life_ops_home(self) -> None:
        os.environ.pop(store.LIFE_OPS_HOME_ENV, None)
        os.environ.pop(cmail_runtime.DEFAULT_CMAIL_RUNTIME_DB_ENV, None)
        fake_home = Path(self.temp_dir.name) / "home"

        with mock.patch.object(cmail_runtime.Path, "home", return_value=fake_home):
            self.assertEqual(
                fake_home / ".lifeops" / "data" / "cmail_runtime.db",
                cmail_runtime.default_cmail_runtime_db_path(),
            )

    def test_resolve_cmail_db_path_maps_legacy_development_runtime_to_home_default(self) -> None:
        os.environ.pop(store.LIFE_OPS_HOME_ENV, None)
        os.environ.pop(cmail_runtime.DEFAULT_CMAIL_RUNTIME_DB_ENV, None)
        fake_home = Path(self.temp_dir.name) / "home"
        fake_package_root = Path(self.temp_dir.name) / "life-ops"

        with (
            mock.patch.object(cmail_runtime.Path, "home", return_value=fake_home),
            mock.patch("life_ops.store.package_root", return_value=fake_package_root),
        ):
            resolved = cmail_runtime.resolve_cmail_db_path(
                fake_package_root / "data" / "cmail_runtime.db"
            )

        self.assertEqual(fake_home / ".lifeops" / "data" / "cmail_runtime.db", resolved)

    def test_ensure_cmail_db_matches_live_service_rejects_mismatch(self) -> None:
        live_runtime_path = Path(self.temp_dir.name) / "live" / "cmail_runtime.db"
        wrong_runtime_path = Path(self.temp_dir.name) / "wrong" / "cmail_runtime.db"
        service_health = {
            "ok": True,
            "url": "http://127.0.0.1:4311/api/health",
            "payload": {"ok": True, "db_path": str(live_runtime_path)},
            "error": "",
        }

        with mock.patch("life_ops.cmail_runtime._check_cmail_service_http_health", return_value=service_health):
            with self.assertRaisesRegex(RuntimeError, "CMAIL database mismatch"):
                cmail_runtime.ensure_cmail_db_matches_live_service(
                    runtime_db_path=wrong_runtime_path,
                )

            self.assertEqual(
                service_health,
                cmail_runtime.ensure_cmail_db_matches_live_service(
                    runtime_db_path=live_runtime_path,
                ),
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

            self.assertGreaterEqual(int(result.get("list_item_count") or 0), 1)
            self.assertIn(int(result.get("merged_count") or 0), {0, 1})
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

    def test_ensure_runtime_db_rebinds_canonical_sync_state_for_existing_runtime_db(self) -> None:
        with store.open_db(self.runtime_db_path) as connection:
            store.set_sync_state(connection, "cmail_runtime:canonical_db_path", "/tmp/old/life_ops.db")

        cmail_runtime.ensure_cmail_runtime_db(
            runtime_db_path=self.runtime_db_path,
            canonical_db_path=self.canonical_db_path,
        )

        with store.open_db(self.runtime_db_path) as connection:
            sync_value = store.get_sync_state(connection, "cmail_runtime:canonical_db_path")
        self.assertEqual(str(self.canonical_db_path), sync_value)

    def test_migrate_legacy_cmail_state_imports_runtime_mailbox_configs_and_attachments(self) -> None:
        current_home = Path(self.temp_dir.name) / "current-home"
        legacy_home = Path(self.temp_dir.name) / "legacy-home"
        os.environ[store.LIFE_OPS_HOME_ENV] = str(current_home)
        os.environ[cmail_runtime.DEFAULT_CMAIL_LEGACY_HOME_ENV] = str(legacy_home)

        legacy_runtime_path = legacy_home / "data" / "cmail_runtime.db"
        with store.open_db(legacy_runtime_path) as connection:
            store.upsert_communication_from_sync(
                connection,
                source="cloudflare_email",
                external_id="legacy-msg-1",
                subject="Legacy mailbox item",
                channel="email",
                happened_at=datetime(2026, 4, 5, 10, 0, 0),
                follow_up_at=None,
                direction="inbound",
                person="Legacy Person",
                status="reference",
                external_from="Legacy Person <legacy@example.com>",
                external_to="cody@frg.earth",
                message_id="<legacy-msg-1@example.com>",
                thread_key="<legacy-thread@example.com>",
                body_text="Legacy body",
            )
        legacy_attachment = legacy_home / "data" / "attachments" / "cloudflare_email" / "communication-1" / "attachments" / "receipt.txt"
        legacy_attachment.parent.mkdir(parents=True, exist_ok=True)
        legacy_attachment.write_text("receipt", encoding="utf-8")
        legacy_resend = legacy_home / "config" / "resend.json"
        legacy_resend.parent.mkdir(parents=True, exist_ok=True)
        legacy_resend.write_text('{"api_key":"abc"}', encoding="utf-8")
        legacy_cloudflare = legacy_home / "config" / "cloudflare_mail.json"
        legacy_cloudflare.write_text('{"account_id":"acct"}', encoding="utf-8")

        current_runtime_path = current_home / "data" / "cmail_runtime.db"
        current_canonical_path = current_home / "data" / "life_ops.db"
        result = cmail_runtime.migrate_legacy_cmail_state_if_needed(
            runtime_db_path=current_runtime_path,
            canonical_db_path=current_canonical_path,
        )

        self.assertTrue(result["migrated"])
        self.assertEqual(str(legacy_home.resolve(strict=False)), result["migrated_from"])
        self.assertIn("resend.json", result["copied_configs"])
        self.assertIn("cloudflare_mail.json", result["copied_configs"])
        with store.open_db(current_runtime_path) as connection:
            row = connection.execute(
                "SELECT subject FROM communications WHERE external_id = ?",
                ("legacy-msg-1",),
            ).fetchone()
            migrated_from = store.get_sync_state(connection, cmail_runtime.DEFAULT_CMAIL_MIGRATED_FROM_SYNC_KEY)
        self.assertIsNotNone(row)
        self.assertEqual("Legacy mailbox item", str(row["subject"]))
        self.assertEqual(str(legacy_home.resolve(strict=False)), migrated_from)
        self.assertTrue((current_home / "config" / "resend.json").exists())
        self.assertTrue((current_home / "config" / "cloudflare_mail.json").exists())
        self.assertTrue((current_home / "data" / "attachments" / "cloudflare_email" / "communication-1" / "attachments" / "receipt.txt").exists())

    def test_external_backup_copies_runtime_snapshot_and_attachments(self) -> None:
        current_home = Path(self.temp_dir.name) / "current-home"
        backup_root = Path(self.temp_dir.name) / "external-backup"
        os.environ[store.LIFE_OPS_HOME_ENV] = str(current_home)
        os.environ[cmail_runtime.DEFAULT_CMAIL_EXTERNAL_BACKUP_ROOT_ENV] = str(backup_root)

        runtime_path = current_home / "data" / "cmail_runtime.db"
        canonical_path = current_home / "data" / "life_ops.db"
        with store.open_db(runtime_path) as connection:
            store.upsert_communication_from_sync(
                connection,
                source="cloudflare_email",
                external_id="msg-backup-1",
                subject="Backup me",
                channel="email",
                happened_at=datetime(2026, 4, 5, 11, 0, 0),
                follow_up_at=None,
                direction="inbound",
                person="Backup Person",
                status="reference",
                external_from="Backup Person <backup@example.com>",
                external_to="cody@frg.earth",
                message_id="<backup-msg@example.com>",
                thread_key="<backup-thread@example.com>",
                body_text="Backup body",
            )
        current_attachment = current_home / "data" / "attachments" / "cloudflare_email" / "communication-1" / "attachments" / "proof.txt"
        current_attachment.parent.mkdir(parents=True, exist_ok=True)
        current_attachment.write_text("proof", encoding="utf-8")

        result = cmail_runtime.backup_cmail_runtime_to_external(
            runtime_db_path=runtime_path,
            canonical_db_path=canonical_path,
        )

        self.assertTrue(result["enabled"])
        self.assertEqual(str(backup_root.resolve(strict=False)), result["backup_root"])
        self.assertTrue((backup_root / "data" / "cmail_runtime.db").exists())
        self.assertTrue((backup_root / "data" / "attachments" / "cloudflare_email" / "communication-1" / "attachments" / "proof.txt").exists())
        self.assertTrue((backup_root / "data" / "cmail-backup-status.json").exists())

    def test_run_cmail_health_check_repairs_due_queue_and_reports_actions(self) -> None:
        with mock.patch("life_ops.store.default_db_path", return_value=self.canonical_db_path):
            cmail_runtime.ensure_cmail_runtime_db(
                runtime_db_path=self.runtime_db_path,
                canonical_db_path=self.canonical_db_path,
            )
            with store.open_db(self.runtime_db_path) as connection:
                store.set_sync_state(connection, "cmail_runtime:last_external_backup_at", "2026-04-09T00:00:00Z")

            resend_before = {
                "queue_count": 2,
                "due_count": 2,
                "active_alert_count": 0,
                "items": [],
            }
            resend_after = {
                "queue_count": 0,
                "due_count": 0,
                "active_alert_count": 0,
                "items": [],
            }
            with (
                mock.patch(
                    "life_ops.cmail_runtime.cleanup_cmail_correspondence_artifacts",
                    return_value={"orphaned_resend_ids": [10], "superseded_draft_ids": [20]},
                ),
                mock.patch(
                    "life_ops.cmail_runtime.resend_queue_status",
                    side_effect=[resend_before, resend_after],
                ),
                mock.patch(
                    "life_ops.cmail_runtime.process_resend_delivery_queue",
                    return_value={"processed_count": 2, "failed_count": 0, "processed": [], "failures": []},
                ) as resend_process_mock,
                mock.patch(
                    "life_ops.cmail_runtime.cloudflare_mail_queue_status",
                    return_value={"pending_count": 0},
                ),
                mock.patch(
                    "life_ops.cmail_runtime._check_cmail_service_http_health",
                    return_value={"ok": True, "url": "http://127.0.0.1:4311/api/health", "payload": {"ok": True}, "error": ""},
                ),
            ):
                result = cmail_runtime.run_cmail_health_check(
                    runtime_db_path=self.runtime_db_path,
                    canonical_db_path=self.canonical_db_path,
                    repair=True,
                )

            self.assertTrue(result["ok"])
            self.assertEqual([10], result["cleanup"]["orphaned_resend_ids"])
            self.assertEqual([20], result["cleanup"]["superseded_draft_ids"])
            self.assertEqual(2, int(result["resend_processed"]["processed_count"]))
            self.assertIn("processed resend queue items: 2", result["actions"])
            resend_process_mock.assert_called_once()

    def test_run_cmail_health_check_reports_unhealthy_when_service_is_down(self) -> None:
        with mock.patch("life_ops.store.default_db_path", return_value=self.canonical_db_path):
            cmail_runtime.ensure_cmail_runtime_db(
                runtime_db_path=self.runtime_db_path,
                canonical_db_path=self.canonical_db_path,
            )
            with (
                mock.patch(
                    "life_ops.cmail_runtime.resend_queue_status",
                    return_value={"queue_count": 0, "due_count": 0, "active_alert_count": 0, "items": []},
                ),
                mock.patch(
                    "life_ops.cmail_runtime.cloudflare_mail_queue_status",
                    return_value={"pending_count": 0},
                ),
                mock.patch(
                    "life_ops.cmail_runtime.backup_cmail_runtime_to_external",
                    return_value={"enabled": True, "completed_at": "2026-04-09T15:00:00Z"},
                ),
                mock.patch(
                    "life_ops.cmail_runtime._check_cmail_service_http_health",
                    return_value={"ok": False, "url": "http://127.0.0.1:4311/api/health", "payload": {}, "error": "connection refused"},
                ),
            ):
                result = cmail_runtime.run_cmail_health_check(
                    runtime_db_path=self.runtime_db_path,
                    canonical_db_path=self.canonical_db_path,
                    repair=True,
                )

            self.assertFalse(result["ok"])
            self.assertEqual("connection refused", result["service_health"]["error"])
