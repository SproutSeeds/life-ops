"""Persistent Playwright browser context manager for social platform automation.

Provides a reusable browser session that stores cookies and local storage
per platform in ``~/.life-ops/browser/<platform>/``.  Sessions survive
between CLI invocations so the user only logs in once per platform.
"""

from __future__ import annotations

import json
import logging
import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator, Optional

logger = logging.getLogger(__name__)

DEFAULT_BROWSER_ROOT = Path.home() / ".life-ops" / "browser"
DEFAULT_VIEWPORT = {"width": 1280, "height": 900}
DEFAULT_NAVIGATION_TIMEOUT_MS = 30_000
DEFAULT_ACTION_TIMEOUT_MS = 15_000


def browser_root() -> Path:
    return DEFAULT_BROWSER_ROOT


def platform_profile_dir(platform: str, root: Optional[Path] = None) -> Path:
    base = root or browser_root()
    return base / platform


def list_sessions(root: Optional[Path] = None) -> dict[str, dict[str, Any]]:
    """Return a summary of stored browser sessions keyed by platform name."""
    base = root or browser_root()
    sessions: dict[str, dict[str, Any]] = {}
    if not base.is_dir():
        return sessions
    for child in sorted(base.iterdir()):
        if child.is_dir() and not child.name.startswith("."):
            cookie_count = 0
            cookie_file = child / "cookies.json"
            if cookie_file.exists():
                try:
                    cookie_count = len(json.loads(cookie_file.read_text()))
                except (json.JSONDecodeError, OSError):
                    pass
            sessions[child.name] = {
                "profile_dir": str(child),
                "cookies": cookie_count,
            }
    return sessions


def clear_session(platform: str, root: Optional[Path] = None) -> bool:
    """Delete the stored browser session for *platform*.  Returns True if removed."""
    profile = platform_profile_dir(platform, root)
    if profile.is_dir():
        shutil.rmtree(profile)
        logger.info("cleared browser session for %s", platform)
        return True
    return False


def _ensure_playwright() -> Any:
    """Import and return the ``playwright.sync_api`` module, raising a
    friendly error if Playwright is not installed."""
    try:
        from playwright.sync_api import sync_playwright  # type: ignore[import-untyped]
        return sync_playwright
    except ImportError:
        raise SystemExit(
            "Playwright is required for social posting.  Install it with:\n"
            "  pip install playwright && python -m playwright install chromium"
        )


@contextmanager
def browser_context(
    platform: str,
    *,
    headless: bool = True,
    root: Optional[Path] = None,
    viewport: Optional[dict[str, int]] = None,
    navigation_timeout_ms: int = DEFAULT_NAVIGATION_TIMEOUT_MS,
    action_timeout_ms: int = DEFAULT_ACTION_TIMEOUT_MS,
) -> Generator[Any, None, None]:
    """Context manager that yields a Playwright ``BrowserContext`` with
    persistent storage for *platform*.

    Usage::

        with browser_context("linkedin") as ctx:
            page = ctx.pages[0] if ctx.pages else ctx.new_page()
            page.goto("https://www.linkedin.com/feed/")
    """
    sync_playwright = _ensure_playwright()
    profile = platform_profile_dir(platform, root)
    profile.mkdir(parents=True, exist_ok=True)

    vp = viewport or DEFAULT_VIEWPORT

    with sync_playwright() as pw:
        ctx = pw.chromium.launch_persistent_context(
            user_data_dir=str(profile),
            headless=headless,
            viewport=vp,
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="America/Los_Angeles",
            ignore_https_errors=False,
            args=[
                "--disable-blink-features=AutomationControlled",
            ],
        )
        ctx.set_default_navigation_timeout(navigation_timeout_ms)
        ctx.set_default_timeout(action_timeout_ms)
        try:
            yield ctx
        finally:
            try:
                ctx.close()
            except Exception:
                pass


def get_page(ctx: Any) -> Any:
    """Return the first open page in the context, or create one."""
    if ctx.pages:
        return ctx.pages[0]
    return ctx.new_page()


def wait_for_login(
    page: Any,
    *,
    check_selector: str,
    timeout_ms: int = 120_000,
    poll_interval_ms: int = 2_000,
) -> bool:
    """Wait for the user to complete a manual login.

    Polls for *check_selector* to appear on the page, indicating a
    successful login.  Returns ``True`` if the element appeared before
    *timeout_ms*, ``False`` otherwise.
    """
    try:
        page.wait_for_selector(check_selector, timeout=timeout_ms)
        return True
    except Exception:
        return False


def upload_file(page: Any, trigger_selector: str, file_path: str) -> None:
    """Click *trigger_selector* to open a file chooser and upload *file_path*.

    Uses Playwright's file chooser interception so no native OS dialog appears.
    """
    with page.expect_file_chooser() as fc_info:
        page.click(trigger_selector)
    file_chooser = fc_info.value
    file_chooser.set_files(file_path)
