from __future__ import annotations

import base64
import os
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops import backups
from life_ops import store
from life_ops import vault_crypto


def _master_key() -> str:
    return base64.urlsafe_b64encode(b"a" * 32).decode("ascii").rstrip("=")


class EncryptedStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "life_ops.db"
        self.backup_dir = Path(self.temp_dir.name) / "backups"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_forced_encrypted_db_round_trips_without_plaintext_sqlite_file(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                vault_crypto.MASTER_KEY_NAME: _master_key(),
                store.FORCE_ENCRYPTED_DB_ENV: "1",
            },
            clear=False,
        ):
            with store.open_db(self.db_path) as connection:
                connection.execute(
                    "INSERT INTO sync_state(key, value) VALUES (?, ?)",
                    ("alpha", "beta"),
                )

            self.assertFalse(self.db_path.exists())
            self.assertFalse((self.db_path.parent / f"{self.db_path.name}-wal").exists())
            self.assertFalse((self.db_path.parent / f"{self.db_path.name}-shm").exists())
            manifest_path = store.encrypted_db_manifest_path(self.db_path)
            self.assertTrue(manifest_path.exists())

            plaintext = store.read_db_bytes(self.db_path)
            self.assertTrue(plaintext.startswith(b"SQLite format 3"))

            with store.open_db(self.db_path) as connection:
                row = connection.execute(
                    "SELECT value FROM sync_state WHERE key = ?",
                    ("alpha",),
                ).fetchone()

            self.assertEqual("beta", str(row["value"]))

    def test_encrypted_db_requires_master_key(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                store.FORCE_ENCRYPTED_DB_ENV: "1",
            },
            clear=False,
        ), mock.patch("life_ops.credentials.resolve_secret", return_value=None):
            os.environ.pop(vault_crypto.MASTER_KEY_NAME, None)
            with self.assertRaises(ValueError) as excinfo:
                store.open_db(self.db_path)

        self.assertIn("LIFE_OPS_MASTER_KEY", str(excinfo.exception))

    def test_create_and_restore_backup_with_encrypted_db_storage(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                vault_crypto.MASTER_KEY_NAME: _master_key(),
                store.FORCE_ENCRYPTED_DB_ENV: "1",
            },
            clear=False,
        ):
            with store.open_db(self.db_path) as connection:
                connection.execute(
                    "INSERT INTO sync_state(key, value) VALUES (?, ?)",
                    ("gamma", "delta"),
                )

            backup = backups.create_encrypted_db_backup(
                db_path=self.db_path,
                output_dir=self.backup_dir,
            )
            manifest_path = Path(str(backup["manifest_path"]))
            self.assertTrue(manifest_path.exists())

            restored_path = Path(self.temp_dir.name) / "restored" / "life_ops.db"
            restored = backups.restore_encrypted_db_backup(
                manifest_path=manifest_path,
                output_path=restored_path,
            )

            self.assertTrue(restored["encrypted_storage"])
            self.assertTrue(store.encrypted_db_manifest_path(restored_path).exists())

            with store.open_db(restored_path) as connection:
                row = connection.execute(
                    "SELECT value FROM sync_state WHERE key = ?",
                    ("gamma",),
                ).fetchone()

            self.assertEqual("delta", str(row["value"]))
            self.assertFalse(restored_path.exists())
            restored_plaintext = store.read_db_bytes(restored_path)
            self.assertTrue(restored_plaintext.startswith(b"SQLite format 3"))

    def test_read_only_open_does_not_reseal_manifest(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                vault_crypto.MASTER_KEY_NAME: _master_key(),
                store.FORCE_ENCRYPTED_DB_ENV: "1",
            },
            clear=False,
        ):
            with store.open_db(self.db_path) as connection:
                connection.execute(
                    "INSERT INTO sync_state(key, value) VALUES (?, ?)",
                    ("epsilon", "zeta"),
                )

            manifest_path = store.encrypted_db_manifest_path(self.db_path)
            before = manifest_path.read_text()

            with store.open_db(self.db_path) as connection:
                row = connection.execute(
                    "SELECT value FROM sync_state WHERE key = ?",
                    ("epsilon",),
                ).fetchone()

            self.assertEqual("zeta", str(row["value"]))
            after = manifest_path.read_text()
            self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
