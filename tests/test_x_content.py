from __future__ import annotations

import base64
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops import store
from life_ops.x_content import create_x_article_package, generate_x_media_asset


class XContentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repo_root = Path(self.temp_dir.name)
        self.db_path = self.repo_root / "data" / "life_ops.db"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_create_x_article_package_stores_article_posts_and_assets(self) -> None:
        with store.open_db(self.db_path) as connection:
            result = create_x_article_package(
                connection,
                title="Canonical dossier",
                angle="turn life-admin chaos into a usable system",
                audience="builders who live in their inbox",
                thesis="A clean operating record compounds faster than memory",
                key_points=["Capture the truth layer", "Surface the active queue", "Keep the archive searchable"],
                cta="Follow for more life-ops systems.",
                tags=["systems", "x"],
            )

            article = store.get_x_content_item(connection, result["article_id"])
            children = store.list_x_content_children(connection, result["article_id"])
            assets = store.list_x_media_assets(connection, content_item_id=result["article_id"], limit=None)

        self.assertIsNotNone(article)
        self.assertEqual("article", str(article["kind"]))
        self.assertEqual(len(result["posts"]), len(children))
        self.assertEqual(3, len(assets))
        self.assertTrue(str(article["body_text"]))

    def test_generate_x_media_asset_saves_file_and_updates_record(self) -> None:
        fake_png = base64.b64encode(b"fake-image-bytes").decode("ascii")

        with mock.patch("life_ops.store.life_ops_home", return_value=self.repo_root):
            with store.open_db(self.db_path) as connection:
                package = create_x_article_package(
                    connection,
                    title="Canonical dossier",
                    thesis="Make your records searchable and useful",
                    key_points=["Capture", "Classify", "Act"],
                )
                asset_id = package["asset_ids"][0]

                with mock.patch(
                    "life_ops.x_content._openai_post_json",
                    return_value={"data": [{"b64_json": fake_png, "revised_prompt": "revised prompt"}]},
                ):
                    result = generate_x_media_asset(
                        connection,
                        asset_id=asset_id,
                        provider="openai",
                        model="gpt-image-1.5",
                        size="1536x1024",
                        quality="high",
                        output_format="png",
                        background="auto",
                        moderation="auto",
                    )

                asset = store.get_x_media_asset(connection, asset_id)

        self.assertEqual(asset_id, result["asset_id"])
        self.assertEqual("generated", str(asset["status"]))
        self.assertEqual("gpt-image-1.5", str(asset["model_name"]))
        self.assertTrue(str(asset["relative_path"]).endswith(".png"))
        generated_path = self.repo_root / str(asset["relative_path"])
        self.assertTrue(generated_path.exists())
        self.assertEqual(b"fake-image-bytes", generated_path.read_bytes())

    def test_generate_x_media_asset_supports_xai_provider(self) -> None:
        fake_jpg = base64.b64encode(b"fake-jpg-bytes").decode("ascii")

        with mock.patch("life_ops.store.life_ops_home", return_value=self.repo_root):
            with store.open_db(self.db_path) as connection:
                package = create_x_article_package(
                    connection,
                    title="Grok image brief",
                    thesis="Let xAI render the imagery layer",
                    key_points=["Prompt", "Render", "Post"],
                )
                asset_id = package["asset_ids"][0]

                with mock.patch(
                    "life_ops.x_content._xai_post_json",
                    return_value={"data": [{"b64_json": fake_jpg}]},
                ):
                    result = generate_x_media_asset(
                        connection,
                        asset_id=asset_id,
                        provider="xai",
                        model="grok-imagine-image",
                        aspect_ratio="16:9",
                        resolution="2k",
                    )

                asset = store.get_x_media_asset(connection, asset_id)

        self.assertEqual("xai", result["provider"])
        self.assertEqual("grok-imagine-image", result["model"])
        self.assertEqual("generated", str(asset["status"]))
        self.assertEqual("grok-imagine-image", str(asset["model_name"]))
        self.assertTrue(str(asset["relative_path"]).endswith(".jpg"))
        generated_path = self.repo_root / str(asset["relative_path"])
        self.assertTrue(generated_path.exists())
        self.assertEqual(b"fake-jpg-bytes", generated_path.read_bytes())

    def test_generate_x_media_asset_falls_back_to_openai_when_xai_fails(self) -> None:
        fake_png = base64.b64encode(b"openai-fallback-bytes").decode("ascii")

        with mock.patch("life_ops.store.life_ops_home", return_value=self.repo_root):
            with store.open_db(self.db_path) as connection:
                package = create_x_article_package(
                    connection,
                    title="Fallback image brief",
                    thesis="Use OpenAI when xAI is blocked",
                    key_points=["Try xAI", "Fallback to OpenAI", "Keep moving"],
                )
                asset_id = package["asset_ids"][0]

                with mock.patch("life_ops.x_content._has_openai_key", return_value=True):
                    with mock.patch(
                        "life_ops.x_content._xai_post_json",
                        side_effect=RuntimeError("xAI image generation failed (403): error code: 1010"),
                    ):
                        with mock.patch(
                            "life_ops.x_content._openai_post_json",
                            return_value={"data": [{"b64_json": fake_png}]},
                        ):
                            result = generate_x_media_asset(
                                connection,
                                asset_id=asset_id,
                                provider="xai",
                                model="",
                                size="1536x1024",
                                quality="high",
                                output_format="png",
                                background="auto",
                                moderation="auto",
                            )

                asset = store.get_x_media_asset(connection, asset_id)

        self.assertEqual("openai", result["provider"])
        self.assertEqual("xai", result["requested_provider"])
        self.assertTrue(result["fallback_used"])
        self.assertEqual(["xai", "openai"], result["attempted_providers"])
        self.assertEqual("generated", str(asset["status"]))
        self.assertEqual("gpt-image-1.5", str(asset["model_name"]))
        self.assertTrue(str(asset["relative_path"]).endswith(".png"))
        self.assertIn("xAI image generation failed", str(asset["metadata_json"]))
        generated_path = self.repo_root / str(asset["relative_path"])
        self.assertTrue(generated_path.exists())
        self.assertEqual(b"openai-fallback-bytes", generated_path.read_bytes())

    def test_generate_x_media_asset_explains_missing_openai_fallback(self) -> None:
        with mock.patch("life_ops.store.life_ops_home", return_value=self.repo_root):
            with store.open_db(self.db_path) as connection:
                package = create_x_article_package(
                    connection,
                    title="Fallback unavailable brief",
                    thesis="Explain why the fallback could not run",
                    key_points=["Try xAI", "Need OpenAI key"],
                )
                asset_id = package["asset_ids"][0]

                with mock.patch("life_ops.x_content._has_openai_key", return_value=False):
                    with mock.patch(
                        "life_ops.x_content._xai_post_json",
                        side_effect=RuntimeError("xAI image generation failed (403): error code: 1010"),
                    ):
                        with self.assertRaises(RuntimeError) as excinfo:
                            generate_x_media_asset(
                                connection,
                                asset_id=asset_id,
                                provider="auto",
                            )

        self.assertIn("xAI image generation failed (403): error code: 1010", str(excinfo.exception))
        self.assertIn("OpenAI fallback unavailable because OPENAI_API_KEY is not configured", str(excinfo.exception))


if __name__ == "__main__":
    unittest.main()
