"""Facebook platform adapter — Playwright-based posting and auth."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Optional

from life_ops.social.browser import browser_context, get_page, upload_file, wait_for_login

logger = logging.getLogger(__name__)

PLATFORM = "facebook"

FACEBOOK_HOME = "https://www.facebook.com/"
FACEBOOK_LOGIN = "https://www.facebook.com/login"

# Selectors — these target the current Facebook UI as of early 2026.
SEL_LOGGED_IN = '[aria-label="Facebook"], [data-pagelet="LeftRail"], [role="banner"] a[aria-label="Profile"]'
SEL_WHATS_ON_YOUR_MIND = '[aria-label*="What\'s on your mind"], [data-pagelet="FeedComposer"] [role="button"], span:has-text("What\'s on your mind")'
SEL_POST_DIALOG = '[role="dialog"][aria-label*="post"], [role="dialog"][aria-label*="Create"]'
SEL_TEXT_EDITOR = '[role="dialog"] [contenteditable="true"][role="textbox"]'
SEL_ADD_PHOTO_BUTTON = '[role="dialog"] [aria-label*="Photo"], [role="dialog"] [aria-label*="photo/video"]'
SEL_FILE_INPUT = '[role="dialog"] input[type="file"][accept*="image"], [role="dialog"] input[type="file"][accept*="video"]'
SEL_POST_BUTTON = '[role="dialog"] [aria-label="Post"][role="button"], [role="dialog"] div[aria-label="Post"]'

AUTH_CHECK_SELECTOR = SEL_LOGGED_IN
AUTH_LOGIN_TIMEOUT_MS = 120_000
POST_CONFIRMATION_TIMEOUT_MS = 12_000


def _wait_for_post_confirmation(page: Any) -> bool:
    """Return True only once Facebook gives us a real completion signal."""
    try:
        page.wait_for_selector(
            SEL_POST_DIALOG,
            state="hidden",
            timeout=POST_CONFIRMATION_TIMEOUT_MS,
        )
        return True
    except Exception:
        return False


def authenticate(*, headless: bool = False, root: Optional[Path] = None) -> bool:
    """Open Facebook in a visible browser for the user to log in manually.

    Returns ``True`` if the login was detected, ``False`` on timeout.
    """
    with browser_context(PLATFORM, headless=False, root=root) as ctx:
        page = get_page(ctx)
        page.goto(FACEBOOK_LOGIN, wait_until="domcontentloaded")

        # Already logged in?
        try:
            page.wait_for_selector(AUTH_CHECK_SELECTOR, timeout=5_000)
            logger.info("facebook: already logged in")
            return True
        except Exception:
            pass

        logger.info("facebook: waiting for manual login (up to 2 minutes) …")
        return wait_for_login(
            page,
            check_selector=AUTH_CHECK_SELECTOR,
            timeout_ms=AUTH_LOGIN_TIMEOUT_MS,
        )


def is_logged_in(*, headless: bool = True, root: Optional[Path] = None) -> bool:
    """Quick check whether the stored session is still valid."""
    try:
        with browser_context(PLATFORM, headless=headless, root=root) as ctx:
            page = get_page(ctx)
            page.goto(FACEBOOK_HOME, wait_until="domcontentloaded")
            try:
                page.wait_for_selector(AUTH_CHECK_SELECTOR, timeout=8_000)
                return True
            except Exception:
                return False
    except Exception:
        return False


def post(
    text: str,
    *,
    image: Optional[str] = None,
    headless: bool = True,
    root: Optional[Path] = None,
    visible_timeout_ms: int = 10_000,
) -> dict[str, Any]:
    """Create a Facebook post on the personal timeline with optional image.

    Returns a dict with ``ok`` and a ``message``.
    """
    with browser_context(PLATFORM, headless=headless, root=root) as ctx:
        page = get_page(ctx)
        page.goto(FACEBOOK_HOME, wait_until="domcontentloaded")

        # Verify logged in.
        try:
            page.wait_for_selector(AUTH_CHECK_SELECTOR, timeout=8_000)
        except Exception:
            return {"ok": False, "message": "Not logged in. Run: life-ops social-auth facebook"}

        # Click "What's on your mind?" to open composer.
        page.wait_for_selector(SEL_WHATS_ON_YOUR_MIND, timeout=visible_timeout_ms)
        page.click(SEL_WHATS_ON_YOUR_MIND)

        # Wait for the post dialog.
        page.wait_for_selector(SEL_TEXT_EDITOR, timeout=visible_timeout_ms)
        time.sleep(0.8)

        # Type the post text.
        editor = page.query_selector(SEL_TEXT_EDITOR)
        if not editor:
            return {"ok": False, "message": "Could not find the text editor."}
        editor.click()
        editor.fill(text)

        # Attach image if provided.
        if image:
            image_path = Path(image).expanduser().resolve()
            if not image_path.is_file():
                return {"ok": False, "message": f"Image not found: {image_path}"}
            try:
                # Facebook exposes an <input type="file"> that we can set directly,
                # falling back to the photo/video button + file chooser interception.
                file_input = page.query_selector(SEL_FILE_INPUT)
                if file_input:
                    file_input.set_input_files(str(image_path))
                else:
                    page.wait_for_selector(SEL_ADD_PHOTO_BUTTON, timeout=5_000)
                    upload_file(page, SEL_ADD_PHOTO_BUTTON, str(image_path))
                time.sleep(3)  # wait for upload thumbnail
            except Exception as exc:
                logger.warning("facebook: image upload may have failed: %s", exc)

        # Click Post.
        page.wait_for_selector(SEL_POST_BUTTON, timeout=visible_timeout_ms)
        page.click(SEL_POST_BUTTON)

        if not _wait_for_post_confirmation(page):
            return {
                "ok": False,
                "message": "Facebook post submission could not be confirmed.",
            }
        return {"ok": True, "message": "Facebook post published."}
