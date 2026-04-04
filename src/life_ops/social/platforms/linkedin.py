"""LinkedIn platform adapter — Playwright-based posting and auth."""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Optional

from life_ops.social.browser import browser_context, get_page, upload_file, wait_for_login

logger = logging.getLogger(__name__)

PLATFORM = "linkedin"

LINKEDIN_HOME = "https://www.linkedin.com/feed/"
LINKEDIN_LOGIN = "https://www.linkedin.com/login"

# Selectors — these may need updating when LinkedIn changes their UI.
SEL_FEED_IDENTITY = 'div.feed-identity-module, [data-test-id="feed-identity-module"]'
SEL_START_POST_BUTTON = 'button.artdeco-button--muted[class*="share-box"], button[aria-label*="Start a post"], div.share-box-feed-entry__trigger'
SEL_POST_MODAL = 'div.share-creation-state__text-editor, div[role="dialog"][aria-label*="Create a post"], div[data-test-modal-id="share-modal"]'
SEL_TEXT_EDITOR = 'div.ql-editor[data-placeholder], div[role="textbox"][contenteditable="true"], div.share-creation-state__text-editor div[contenteditable="true"]'
SEL_ADD_MEDIA_BUTTON = 'button[aria-label*="Add media"], button[aria-label*="Add a photo"], button[aria-label*="photo"]'
SEL_POST_BUTTON = 'button.share-actions__primary-action, button[class*="share-actions__primary-action"], button:has-text("Post")'
SEL_POST_SUCCESS_TOAST = 'div.artdeco-toast-item, [data-test-artdeco-toast]'

AUTH_CHECK_SELECTOR = SEL_FEED_IDENTITY
AUTH_LOGIN_TIMEOUT_MS = 120_000
POST_CONFIRMATION_TIMEOUT_MS = 12_000


def _wait_for_post_confirmation(page: Any) -> bool:
    """Return True only once LinkedIn gives us a real completion signal."""
    try:
        page.wait_for_selector(SEL_POST_SUCCESS_TOAST, timeout=8_000)
        return True
    except Exception:
        pass
    try:
        page.wait_for_selector(
            SEL_POST_MODAL,
            state="hidden",
            timeout=POST_CONFIRMATION_TIMEOUT_MS,
        )
        return True
    except Exception:
        return False


def authenticate(*, headless: bool = False, root: Optional[Path] = None) -> bool:
    """Open LinkedIn in a visible browser for the user to log in manually.

    Returns ``True`` if the login was detected, ``False`` on timeout.
    """
    with browser_context(PLATFORM, headless=False, root=root) as ctx:
        page = get_page(ctx)
        page.goto(LINKEDIN_LOGIN, wait_until="domcontentloaded")

        # Check if already logged in (cookies from a previous session).
        try:
            page.wait_for_selector(AUTH_CHECK_SELECTOR, timeout=5_000)
            logger.info("linkedin: already logged in")
            return True
        except Exception:
            pass

        logger.info("linkedin: waiting for manual login (up to 2 minutes) …")
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
            page.goto(LINKEDIN_HOME, wait_until="domcontentloaded")
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
    """Create a LinkedIn post with optional image attachment.

    Returns a dict with ``ok`` and a ``message`` describing what happened.
    """
    with browser_context(PLATFORM, headless=headless, root=root) as ctx:
        page = get_page(ctx)
        page.goto(LINKEDIN_HOME, wait_until="domcontentloaded")

        # Verify logged in.
        try:
            page.wait_for_selector(AUTH_CHECK_SELECTOR, timeout=8_000)
        except Exception:
            return {"ok": False, "message": "Not logged in. Run: life-ops social-auth linkedin"}

        # Open the post composer.
        page.wait_for_selector(SEL_START_POST_BUTTON, timeout=visible_timeout_ms)
        page.click(SEL_START_POST_BUTTON)

        # Wait for the modal / text editor.
        page.wait_for_selector(SEL_TEXT_EDITOR, timeout=visible_timeout_ms)
        time.sleep(0.5)  # let the animation settle

        # Type the post text.
        editor = page.query_selector(SEL_TEXT_EDITOR)
        if not editor:
            return {"ok": False, "message": "Could not find the text editor element."}
        editor.click()
        editor.fill(text)

        # Attach image if provided.
        if image:
            image_path = Path(image).expanduser().resolve()
            if not image_path.is_file():
                return {"ok": False, "message": f"Image not found: {image_path}"}
            try:
                page.wait_for_selector(SEL_ADD_MEDIA_BUTTON, timeout=5_000)
                upload_file(page, SEL_ADD_MEDIA_BUTTON, str(image_path))
                # Wait for the image to finish uploading (thumbnail appears).
                time.sleep(3)
            except Exception as exc:
                logger.warning("linkedin: image upload may have failed: %s", exc)

        # Click Post.
        page.wait_for_selector(SEL_POST_BUTTON, timeout=visible_timeout_ms)
        page.click(SEL_POST_BUTTON)

        if not _wait_for_post_confirmation(page):
            return {
                "ok": False,
                "message": "LinkedIn post submission could not be confirmed.",
            }
        return {"ok": True, "message": "LinkedIn post published."}
