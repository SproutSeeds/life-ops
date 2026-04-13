from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops import credentials


class CredentialsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.registry_path = Path(self.temp_dir.name) / "keys.json"
        self.service_secrets_path = Path(self.temp_dir.name) / "service-secrets.json"
        self.original_env = dict(os.environ)

    def tearDown(self) -> None:
        os.environ.clear()
        os.environ.update(self.original_env)
        self.temp_dir.cleanup()

    def test_file_backend_store_list_export_and_delete(self) -> None:
        with self.assertRaises(RuntimeError):
            credentials.set_secret(
                name="OPENAI_API_KEY",
                value="sk-test",
                backend="file",
                path=self.registry_path,
            )

        credentials.set_secret(
            name="OPENAI_API_KEY",
            value="sk-test",
            backend="file",
            path=self.registry_path,
            allow_insecure_file_backend=True,
        )

        listed = credentials.list_secrets(path=self.registry_path)
        exported = credentials.export_secrets(path=self.registry_path)

        self.assertEqual(1, len(listed))
        self.assertEqual("OPENAI_API_KEY", listed[0]["name"])
        self.assertTrue(listed[0]["available"])
        self.assertIn("export OPENAI_API_KEY=", exported["export_text"])

        deleted = credentials.delete_secret(name="OPENAI_API_KEY", path=self.registry_path)
        self.assertTrue(deleted["deleted"])
        self.assertEqual([], credentials.list_secrets(path=self.registry_path))

    def test_load_registered_secrets_populates_process_env(self) -> None:
        credentials.set_secret(
            name="OPENAI_API_KEY",
            value="sk-test-load",
            backend="file",
            path=self.registry_path,
            allow_insecure_file_backend=True,
        )
        os.environ.pop("OPENAI_API_KEY", None)

        loaded = credentials.load_registered_secrets(path=self.registry_path)

        self.assertEqual("sk-test-load", loaded["OPENAI_API_KEY"])
        self.assertEqual("sk-test-load", os.environ["OPENAI_API_KEY"])

    def test_auto_backend_requires_secure_store_when_keychain_missing(self) -> None:
        with mock.patch("life_ops.credentials._has_macos_keychain", return_value=False):
            with self.assertRaises(RuntimeError):
                credentials.set_secret(
                    name="OPENAI_API_KEY",
                    value="sk-test",
                    backend="auto",
                    path=self.registry_path,
                )

    def test_write_service_secret_snapshot_persists_resolved_values(self) -> None:
        credentials.set_secret(
            name="OPENAI_API_KEY",
            value="sk-test-service",
            backend="file",
            path=self.registry_path,
            allow_insecure_file_backend=True,
        )

        snapshot = credentials.write_service_secret_snapshot(
            names=["OPENAI_API_KEY"],
            path=self.registry_path,
            target=self.service_secrets_path,
        )

        self.assertEqual(["OPENAI_API_KEY"], snapshot["names"])
        self.assertTrue(self.service_secrets_path.exists())
        self.assertEqual("sk-test-service", credentials.resolve_secret(name="OPENAI_API_KEY", path=self.registry_path))

    def test_resolve_secret_uses_service_snapshot_before_registry(self) -> None:
        self.service_secrets_path.write_text('{"OPENAI_API_KEY":"sk-from-service"}\n')
        os.environ[credentials.SERVICE_SECRETS_PATH_ENV] = str(self.service_secrets_path)

        resolved = credentials.resolve_secret(name="OPENAI_API_KEY", path=self.registry_path)

        self.assertEqual("sk-from-service", resolved)


if __name__ == "__main__":
    unittest.main()
