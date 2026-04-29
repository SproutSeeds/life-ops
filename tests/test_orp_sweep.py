from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops import store
from life_ops.calendar import build_day_sheet
from life_ops.orp_sweep import build_orp_project_sweep, sync_orp_sweep_calendar


COLLAB_ID = "55801968-1500-41a4-92e0-800f32818022"
LONGEVITY_ID = "87518203-9633-4253-b97b-d990f9468f5c"


def _completed(cmd: list[str], payload: dict) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(payload), stderr="")


class OrpSweepTests(unittest.TestCase):
    def test_project_sweep_includes_pinned_maps_and_redacts_sensitive_notes(self) -> None:
        token = "eyJaaaaaaaaaaaaaaaa.abcdefghijklmnop.abcdefghijklmnop"

        def runner(cmd: list[str]) -> subprocess.CompletedProcess[str]:
            if cmd[:3] == ["orp", "ideas", "list"]:
                return _completed(
                    cmd,
                    {
                        "ideas": [
                            {
                                "id": "recent-starred-side-quest",
                                "title": "Recent starred side quest",
                                "starred": True,
                                "superStarred": False,
                                "updatedAt": "2026-04-20T12:00:00Z",
                            },
                            {
                                "id": "noise",
                                "title": "Untracked noise",
                                "starred": False,
                                "superStarred": False,
                                "updatedAt": "2026-04-20T10:00:00Z",
                            },
                            {
                                "id": COLLAB_ID,
                                "title": "Open-source collaboration operating map",
                                "starred": False,
                                "superStarred": False,
                                "updatedAt": "2026-04-19T10:00:00Z",
                            },
                            {
                                "id": LONGEVITY_ID,
                                "title": "Longevity / biotech project execution map",
                                "starred": True,
                                "superStarred": True,
                                "updatedAt": "2026-04-18T10:00:00Z",
                            },
                        ]
                    },
                )
            if cmd[:3] == ["orp", "feature", "list"] and cmd[3] == COLLAB_ID:
                return _completed(
                    cmd,
                    {
                        "features": [
                            {
                                "id": "collab-feature",
                                "title": "Queue and inbox operating model",
                                "starred": True,
                                "superStarred": False,
                                "notes": f"Current priority: Finish the daily sweep without leaking {token}.",
                                "updatedAt": "2026-04-19T11:00:00Z",
                            }
                        ]
                    },
                )
            if cmd[:3] == ["orp", "feature", "list"] and cmd[3] == LONGEVITY_ID:
                return _completed(
                    cmd,
                    {
                        "features": [
                            {
                                "id": "longevity-feature",
                                "title": "Regulatory planning path",
                                "starred": False,
                                "superStarred": False,
                                "notes": "Next action: choose the next regulatory planning block.",
                                "updatedAt": "2026-04-18T11:00:00Z",
                            }
                        ]
                    },
                )
            return _completed(cmd, {"features": []})

        sweep = build_orp_project_sweep(runner=runner, max_projects=2)
        project_titles = [project["idea_title"] for project in sweep["projects"]]
        collab = next(project for project in sweep["projects"] if project["idea_id"] == COLLAB_ID)

        self.assertIn("Open-source collaboration operating map", project_titles)
        self.assertIn("Longevity / biotech project execution map", project_titles)
        self.assertNotIn("Recent starred side quest", project_titles)
        self.assertNotIn("Untracked noise", project_titles)
        self.assertEqual("Queue and inbox operating model", collab["feature_title"])
        self.assertIn("[redacted-token]", collab["action_summary"])
        self.assertNotIn(token, json.dumps(sweep))

    def test_calendar_sync_places_orp_items_on_day_sheet(self) -> None:
        target_day = date(2026, 4, 20)
        sweep = {
            "generated_at": "2026-04-20T08:20:00-05:00",
            "tracked_project_count": 1,
            "projects": [
                {
                    "idea_id": COLLAB_ID,
                    "idea_title": "Open-source collaboration operating map",
                    "idea_super_starred": False,
                    "feature_id": "collab-feature",
                    "feature_title": "Queue and inbox operating model",
                    "priority": "high",
                    "action_summary": "Turn the sweep into a daily project priority list.",
                }
            ],
            "failures": [],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            connection = store.open_db(Path(temp_dir) / "lifeops.db")
            try:
                results = sync_orp_sweep_calendar(
                    connection,
                    sweep=sweep,
                    target_day=target_day,
                    report_path=Path(temp_dir) / "latest.md",
                )
                second_results = sync_orp_sweep_calendar(
                    connection,
                    sweep=sweep,
                    target_day=target_day,
                    report_path=Path(temp_dir) / "latest.md",
                )
                sheet = build_day_sheet(connection, target_day=target_day)
            finally:
                connection.close()

        section_names = [section["name"] for section in sheet["sections"]]
        focus_titles = [item["title"] for item in sheet["focus_priorities"]["items"]]

        self.assertEqual(["created", "created"], [result["action"] for result in results])
        self.assertEqual(["skipped-existing", "skipped-existing"], [result["action"] for result in second_results])
        self.assertNotIn("ORP / Project Priorities", section_names)
        self.assertIn("Open-source collaboration operating map", focus_titles)
        self.assertEqual(1, sheet["focus_priorities"]["count"])
        self.assertTrue(sheet["focus_priorities"]["items"][0]["question"].endswith("?"))


if __name__ == "__main__":
    unittest.main()
