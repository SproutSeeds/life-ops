from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from life_ops import mail_ui, store


class CmailReadConnectionTests(unittest.TestCase):
    """UI reads of the live runtime DB (see docs/qa/cmail-labels-2026-09-27/README.md).

    Scheduled jobs use a SQLite build that deletes the WAL side files when their last
    connection closes. macOS SQLite cannot recreate them from a file-level read-only
    connection, so UI reads open read-write and rely on query_only for protection.
    """

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.env = mock.patch.dict(os.environ, {"LIFE_OPS_HOME": str(self.root), "LIFE_OPS_FORCE_ENCRYPTED_DB": "0"})
        self.env.start()
        self.db = self.root / "cmail_runtime.db"
        with store.open_db(self.db) as connection:
            store.upsert_communication_from_sync(
                connection, source="cloudflare_email", external_id="read-1", subject="Hello", channel="email",
                happened_at=datetime(2026, 9, 27, 9), follow_up_at=None, external_from="friend@example.com",
                external_to="cody@frg.earth", message_id="<read-1@example.com>",
            )
        self.cache = mail_ui._MailUiSnapshotCache(db_path=self.db)

    def tearDown(self):
        self.env.stop()
        self.temp.cleanup()

    def test_reads_work_after_the_wal_files_are_removed_and_sql_writes_stay_blocked(self):
        self.assertFalse(Path(f"{self.db}-wal").exists() or Path(f"{self.db}-shm").exists())
        connection = self.cache.get_connection()
        try:
            self.assertEqual(1, connection.execute("SELECT COUNT(*) FROM communications").fetchone()[0])
            self.assertEqual(1, connection.execute("PRAGMA query_only").fetchone()[0])
            with self.assertRaises(sqlite3.OperationalError):
                connection.execute("INSERT INTO sync_state (key, value) VALUES ('probe', 'x')")
        finally:
            connection.close()

    def test_read_connection_is_not_a_file_level_read_only_open(self):
        connection = self.cache.get_connection()
        try:
            # With query_only lifted the file itself must be writable, which lets SQLite
            # rebuild the WAL side files. Roll back so nothing is stored.
            connection.execute("PRAGMA query_only = OFF")
            connection.execute("BEGIN")
            connection.execute("INSERT INTO sync_state (key, value) VALUES ('probe', 'x')")
            connection.execute("ROLLBACK")
        finally:
            connection.close()
        with store.open_db(self.db) as connection:
            self.assertIsNone(store.get_sync_state(connection, "probe"))

    def test_missing_database_is_never_created(self):
        missing = self.root / "missing.db"
        with self.assertRaises(sqlite3.OperationalError):
            mail_ui._MailUiSnapshotCache(db_path=missing).get_connection().execute("SELECT 1").fetchone()
        self.assertFalse(missing.exists())


if __name__ == "__main__":
    unittest.main()
