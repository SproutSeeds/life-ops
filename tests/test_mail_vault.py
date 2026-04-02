from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops import mail_vault
from life_ops import vault_crypto


class MailVaultTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.vault_root = Path(self.temp_dir.name) / "vault"
        self.original_master_key = os.environ.get(vault_crypto.MASTER_KEY_NAME)
        os.environ[vault_crypto.MASTER_KEY_NAME] = vault_crypto._b64url_encode(b"b" * 32)

    def tearDown(self) -> None:
        if self.original_master_key is None:
            os.environ.pop(vault_crypto.MASTER_KEY_NAME, None)
        else:
            os.environ[vault_crypto.MASTER_KEY_NAME] = self.original_master_key
        self.temp_dir.cleanup()

    def test_write_and_read_encrypted_vault_file_round_trip(self) -> None:
        relative_path, sha256 = mail_vault.write_encrypted_vault_file(
            vault_root=self.vault_root,
            relative_dir=Path("mail") / "case-1",
            logical_filename="message.eml",
            raw_bytes=b"Subject: hello\r\n\r\nbody",
            metadata={"kind": "test"},
        )

        self.assertTrue(relative_path.endswith(".enc.json"))
        plaintext = mail_vault.read_encrypted_vault_file(
            vault_root=self.vault_root,
            relative_path=relative_path,
        )

        self.assertEqual(b"Subject: hello\r\n\r\nbody", plaintext)
        self.assertEqual(64, len(sha256))

    def test_read_and_delete_reject_path_traversal(self) -> None:
        outside_path = Path(self.temp_dir.name) / "outside.enc.json"
        outside_path.write_text("{}", encoding="utf-8")

        with self.assertRaises(ValueError):
            mail_vault.read_encrypted_vault_file(
                vault_root=self.vault_root,
                relative_path="../../outside.enc.json",
            )

        with self.assertRaises(ValueError):
            mail_vault.delete_encrypted_vault_file(
                vault_root=self.vault_root,
                relative_path="../../outside.enc.json",
            )

        self.assertTrue(outside_path.exists())


if __name__ == "__main__":
    unittest.main()
