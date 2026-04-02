from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from life_ops.outreach_validation import validate_outreach_manifest


class OutreachValidationTests(unittest.TestCase):
    def test_validate_manifest_accepts_distinct_recipient_drafts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            tao_path = base / "tao.txt"
            bloom_path = base / "bloom.txt"
            tao_path.write_text(
                "\n".join(
                    [
                        "Subject: Tao note",
                        "",
                        "Professor Tao,",
                        "",
                        "https://www.npmjs.com/package/erdos-problems",
                        "https://github.com/SproutSeeds/erdos-problems",
                        "npm install -g erdos-problems",
                        "",
                        "I built this for both humans and agents to use.",
                        "usable AI tools for research",
                    ]
                ),
                encoding="utf-8",
            )
            bloom_path.write_text(
                "\n".join(
                    [
                        "Subject: Bloom note",
                        "",
                        "Dr. Bloom,",
                        "",
                        "https://www.npmjs.com/package/erdos-problems",
                        "https://github.com/SproutSeeds/erdos-problems",
                        "npm install -g erdos-problems",
                        "",
                        "I built this for both humans and agents to use.",
                        "ecosystem around the site",
                    ]
                ),
                encoding="utf-8",
            )
            manifest = {
                "project": {
                    "name": "erdos-problems",
                    "links": [
                        "https://www.npmjs.com/package/erdos-problems",
                        "https://github.com/SproutSeeds/erdos-problems",
                    ],
                    "install_command": "npm install -g erdos-problems",
                },
                "rules": {
                    "require_project_links": True,
                    "require_install_command": True,
                    "require_human_agent_language": True,
                    "forbidden_phrases": ["no pressure"],
                    "min_distinctives": 2,
                    "max_similarity": 0.95,
                },
                "recipients": [
                    {
                        "id": "tao",
                        "salutation": "Professor Tao,",
                        "title_source": "https://math.math.ucla.edu/people/ladder/tao",
                        "distinctives": ["ai", "radar"],
                        "must_include": ["usable AI tools for research"],
                        "draft_path": "tao.txt",
                    },
                    {
                        "id": "bloom",
                        "salutation": "Dr. Bloom,",
                        "title_source": "https://research.manchester.ac.uk/en/persons/thomas-bloom/",
                        "distinctives": ["site", "ecosystem"],
                        "must_include": ["ecosystem around the site"],
                        "draft_path": "bloom.txt",
                    },
                ],
            }
            manifest_path = base / "manifest.json"
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

            result = validate_outreach_manifest(manifest_path)
            self.assertTrue(result["ok"])
            self.assertEqual(result["recipient_count"], 2)

    def test_validate_manifest_rejects_forbidden_phrase_and_duplicate_drafts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            draft_text = "\n".join(
                [
                    "Subject: Duplicate note",
                    "",
                    "Professor Tao,",
                    "",
                    "https://www.npmjs.com/package/erdos-problems",
                    "https://github.com/SproutSeeds/erdos-problems",
                    "npm install -g erdos-problems",
                    "",
                    "I built this for both humans and agents to use.",
                    "No pressure at all if not.",
                ]
            )
            (base / "one.txt").write_text(draft_text, encoding="utf-8")
            (base / "two.txt").write_text(draft_text.replace("Professor Tao,", "Dr. Bloom,"), encoding="utf-8")
            manifest = {
                "project": {
                    "name": "erdos-problems",
                    "links": [
                        "https://www.npmjs.com/package/erdos-problems",
                        "https://github.com/SproutSeeds/erdos-problems",
                    ],
                    "install_command": "npm install -g erdos-problems",
                },
                "rules": {
                    "require_project_links": True,
                    "require_install_command": True,
                    "require_human_agent_language": True,
                    "forbidden_phrases": ["no pressure"],
                    "min_distinctives": 2,
                    "max_similarity": 0.7,
                },
                "recipients": [
                    {
                        "id": "one",
                        "salutation": "Professor Tao,",
                        "title_source": "https://math.math.ucla.edu/people/ladder/tao",
                        "distinctives": ["one", "two"],
                        "draft_path": "one.txt",
                    },
                    {
                        "id": "two",
                        "salutation": "Dr. Bloom,",
                        "title_source": "https://research.manchester.ac.uk/en/persons/thomas-bloom/",
                        "distinctives": ["three", "four"],
                        "draft_path": "two.txt",
                    },
                ],
            }
            manifest_path = base / "manifest.json"
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

            result = validate_outreach_manifest(manifest_path)
            self.assertFalse(result["ok"])
            messages = [issue["message"] for issue in result["issues"]]
            self.assertTrue(any("forbidden phrase" in message for message in messages))
            self.assertTrue(any("too similar" in message for message in messages))


if __name__ == "__main__":
    unittest.main()
