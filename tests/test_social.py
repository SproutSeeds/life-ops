from __future__ import annotations

import contextlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from life_ops.social.browser import (
    browser_root,
    clear_session,
    list_sessions,
    platform_profile_dir,
)
from life_ops.social.platforms import facebook, linkedin
from life_ops.social.post import available_platforms, post_multi


class _FakePage:
    def __init__(self, outcomes: list[object]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[str, str | None]] = []

    def wait_for_selector(self, selector: str, *, state: str | None = None, timeout: int | None = None):
        self.calls.append((selector, state))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class _LoginCheckFailsPage:
    def goto(self, *_args, **_kwargs) -> None:
        return None

    def wait_for_selector(self, *_args, **_kwargs):
        raise RuntimeError("not logged in")


class BrowserSessionManagementTests(unittest.TestCase):
    def test_browser_root_is_under_home(self) -> None:
        root = browser_root()
        self.assertIn(".life-ops", str(root))
        self.assertEqual("browser", root.name)

    def test_platform_profile_dir_uses_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            result = platform_profile_dir("linkedin", root=root)
            self.assertEqual(root / "linkedin", result)

    def test_list_sessions_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            self.assertEqual({}, list_sessions(root=Path(temp_dir)))

    def test_list_sessions_with_profiles(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "linkedin").mkdir()
            (root / "facebook").mkdir()
            (root / "linkedin" / "cookies.json").write_text(
                json.dumps([{"name": "li_at", "value": "xxx"}]),
                encoding="utf-8",
            )
            sessions = list_sessions(root=root)
            self.assertIn("linkedin", sessions)
            self.assertIn("facebook", sessions)
            self.assertEqual(1, sessions["linkedin"]["cookies"])
            self.assertEqual(0, sessions["facebook"]["cookies"])

    def test_clear_session_removes_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            profile = root / "linkedin"
            profile.mkdir()
            (profile / "cookies.json").write_text("[]", encoding="utf-8")
            self.assertTrue(clear_session("linkedin", root=root))
            self.assertFalse(profile.exists())

    def test_clear_session_missing_is_false(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            self.assertFalse(clear_session("linkedin", root=Path(temp_dir)))


class PostModuleTests(unittest.TestCase):
    def test_available_platforms(self) -> None:
        platforms = available_platforms()
        self.assertIn("linkedin", platforms)
        self.assertIn("facebook", platforms)

    def test_post_multi_dispatches_to_adapters(self) -> None:
        mock_result = {"ok": True, "message": "Posted."}
        with mock.patch(
            "life_ops.social.post.PLATFORMS",
            {
                "linkedin": mock.MagicMock(post=mock.MagicMock(return_value=mock_result)),
                "facebook": mock.MagicMock(post=mock.MagicMock(return_value=mock_result)),
            },
        ):
            results = post_multi(["linkedin", "facebook"], "Hello world", headless=True)
        self.assertTrue(results["linkedin"]["ok"])
        self.assertTrue(results["facebook"]["ok"])

    def test_post_multi_uses_overrides(self) -> None:
        linkedin_mock = mock.MagicMock(post=mock.MagicMock(return_value={"ok": True, "message": "ok"}))
        facebook_mock = mock.MagicMock(post=mock.MagicMock(return_value={"ok": True, "message": "ok"}))
        with mock.patch(
            "life_ops.social.post.PLATFORMS",
            {"linkedin": linkedin_mock, "facebook": facebook_mock},
        ):
            post_multi(
                ["linkedin", "facebook"],
                "default text",
                platform_text={"linkedin": "professional text"},
                headless=True,
            )
        linkedin_mock.post.assert_called_once_with(
            "professional text",
            image=None,
            headless=True,
            root=None,
        )
        facebook_mock.post.assert_called_once_with(
            "default text",
            image=None,
            headless=True,
            root=None,
        )

    def test_post_multi_catches_exceptions(self) -> None:
        with mock.patch(
            "life_ops.social.post.PLATFORMS",
            {
                "linkedin": mock.MagicMock(
                    post=mock.MagicMock(side_effect=RuntimeError("network down")),
                ),
            },
        ):
            results = post_multi(["linkedin"], "Hello", headless=True)
        self.assertFalse(results["linkedin"]["ok"])
        self.assertIn("network down", results["linkedin"]["message"])


class PlatformAdapterTests(unittest.TestCase):
    def test_linkedin_confirmation_accepts_toast(self) -> None:
        page = _FakePage([object()])
        self.assertTrue(linkedin._wait_for_post_confirmation(page))
        self.assertEqual([(linkedin.SEL_POST_SUCCESS_TOAST, None)], page.calls)

    def test_linkedin_confirmation_accepts_modal_close_after_missing_toast(self) -> None:
        page = _FakePage([RuntimeError("no toast"), object()])
        self.assertTrue(linkedin._wait_for_post_confirmation(page))
        self.assertEqual(
            [
                (linkedin.SEL_POST_SUCCESS_TOAST, None),
                (linkedin.SEL_POST_MODAL, "hidden"),
            ],
            page.calls,
        )

    def test_linkedin_confirmation_failure_returns_false(self) -> None:
        page = _FakePage([RuntimeError("no toast"), RuntimeError("still open")])
        self.assertFalse(linkedin._wait_for_post_confirmation(page))

    def test_facebook_confirmation_requires_dialog_close(self) -> None:
        page = _FakePage([object()])
        self.assertTrue(facebook._wait_for_post_confirmation(page))
        self.assertEqual([(facebook.SEL_POST_DIALOG, "hidden")], page.calls)

    def test_facebook_confirmation_failure_returns_false(self) -> None:
        page = _FakePage([RuntimeError("still open")])
        self.assertFalse(facebook._wait_for_post_confirmation(page))

    def test_linkedin_not_logged_in_message_uses_real_command_name(self) -> None:
        with mock.patch(
            "life_ops.social.platforms.linkedin.browser_context",
            return_value=contextlib.nullcontext(object()),
        ), mock.patch(
            "life_ops.social.platforms.linkedin.get_page",
            return_value=_LoginCheckFailsPage(),
        ):
            result = linkedin.post("hello")
        self.assertEqual(
            "Not logged in. Run: life-ops social-auth linkedin",
            result["message"],
        )

    def test_facebook_not_logged_in_message_uses_real_command_name(self) -> None:
        with mock.patch(
            "life_ops.social.platforms.facebook.browser_context",
            return_value=contextlib.nullcontext(object()),
        ), mock.patch(
            "life_ops.social.platforms.facebook.get_page",
            return_value=_LoginCheckFailsPage(),
        ):
            result = facebook.post("hello")
        self.assertEqual(
            "Not logged in. Run: life-ops social-auth facebook",
            result["message"],
        )


class CLITests(unittest.TestCase):
    def test_social_status_parses(self) -> None:
        from life_ops.cli import build_parser

        parser = build_parser()
        args = parser.parse_args(["social-status"])
        self.assertEqual("social-status", args.command)
        self.assertIsNone(args.platform)

    def test_social_auth_parses(self) -> None:
        from life_ops.cli import build_parser

        parser = build_parser()
        args = parser.parse_args(["social-auth", "linkedin"])
        self.assertEqual("social-auth", args.command)
        self.assertEqual("linkedin", args.platform)

    def test_social_post_parses(self) -> None:
        from life_ops.cli import build_parser

        parser = build_parser()
        args = parser.parse_args([
            "social-post",
            "--platforms",
            "linkedin,facebook",
            "--text",
            "Hello",
            "--linkedin-text",
            "Professional hello",
            "--image",
            "/tmp/pic.png",
            "--visible",
        ])
        self.assertEqual("social-post", args.command)
        self.assertEqual("linkedin,facebook", args.platforms)
        self.assertEqual("Hello", args.text)
        self.assertEqual("Professional hello", args.linkedin_text)
        self.assertIsNone(args.facebook_text)
        self.assertEqual("/tmp/pic.png", args.image)
        self.assertTrue(args.visible)

    def test_social_logout_parses(self) -> None:
        from life_ops.cli import build_parser

        parser = build_parser()
        args = parser.parse_args(["social-logout", "facebook"])
        self.assertEqual("social-logout", args.command)
        self.assertEqual("facebook", args.platform)

    def test_social_status_checks_saved_profiles_instead_of_cookie_counts(self) -> None:
        from life_ops.cli import build_parser, run

        parser = build_parser()
        args = parser.parse_args(["social-status"])
        with mock.patch("life_ops.cli.social_available_platforms", return_value=["facebook", "linkedin"]), mock.patch(
            "life_ops.cli.social_list_sessions",
            return_value={"linkedin": {"cookies": 0}},
        ), mock.patch(
            "life_ops.cli.social_check_status",
            side_effect=lambda platform: platform == "linkedin",
        ) as check_mock:
            output = run(args)
        self.assertIn("Social platform sessions:", output)
        self.assertIn("linkedin: ok", output)
        self.assertIn("facebook: no session", output)
        check_mock.assert_called_once_with("linkedin")

    def test_social_post_requires_text_for_each_selected_platform(self) -> None:
        from life_ops.cli import build_parser, run

        parser = build_parser()
        args = parser.parse_args([
            "social-post",
            "--platforms",
            "linkedin,facebook",
            "--linkedin-text",
            "Professional hello",
        ])
        with self.assertRaisesRegex(ValueError, "Provide post text for: facebook"):
            run(args)

    def test_social_post_rejects_unknown_platforms(self) -> None:
        from life_ops.cli import build_parser, run

        parser = build_parser()
        args = parser.parse_args([
            "social-post",
            "--platforms",
            "linkedin,mastodon",
            "--text",
            "Hello",
        ])
        with self.assertRaisesRegex(ValueError, "Unknown platform\\(s\\): mastodon"):
            run(args)


if __name__ == "__main__":
    unittest.main()
