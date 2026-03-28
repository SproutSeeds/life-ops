from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops import vault_crypto


class VaultCryptoTests(unittest.TestCase):
    def test_encrypt_and_decrypt_round_trip(self) -> None:
        master_key = vault_crypto._b64url_encode(b"a" * 32)
        envelope = vault_crypto.encrypt_bytes(
            b"hello world",
            purpose=vault_crypto.LOCAL_DB_BACKUP_PURPOSE,
            metadata={"kind": "test"},
            master_key=master_key,
        )

        plaintext = vault_crypto.decrypt_bytes(
            envelope,
            purpose=vault_crypto.LOCAL_DB_BACKUP_PURPOSE,
            master_key=master_key,
        )

        self.assertEqual(b"hello world", plaintext)
        self.assertEqual("AES-256-GCM", envelope["alg"])

    def test_generate_master_key_uses_credentials_backend(self) -> None:
        with mock.patch("life_ops.vault_crypto.credentials.set_secret", return_value={"name": vault_crypto.MASTER_KEY_NAME}):
            result = vault_crypto.generate_master_key(backend="file")

        self.assertTrue(result["generated"])
        self.assertEqual(vault_crypto.MASTER_KEY_NAME, result["secret_name"])


if __name__ == "__main__":
    unittest.main()
