"""Unified social posting interface.

Dispatches to platform-specific adapters and provides helpers for
multi-platform posting from a single call.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

from life_ops.social.platforms import facebook, linkedin

logger = logging.getLogger(__name__)

PLATFORMS: dict[str, Any] = {
    "linkedin": linkedin,
    "facebook": facebook,
}


def available_platforms() -> list[str]:
    return sorted(PLATFORMS.keys())


def authenticate(
    platform: str,
    *,
    root: Optional[Path] = None,
) -> bool:
    """Run the interactive auth flow for *platform*."""
    adapter = PLATFORMS.get(platform)
    if adapter is None:
        raise ValueError(f"Unknown platform: {platform!r}. Available: {available_platforms()}")
    return adapter.authenticate(headless=False, root=root)


def check_status(
    platform: str,
    *,
    root: Optional[Path] = None,
) -> bool:
    """Return whether *platform* has a valid stored session."""
    adapter = PLATFORMS.get(platform)
    if adapter is None:
        raise ValueError(f"Unknown platform: {platform!r}. Available: {available_platforms()}")
    return adapter.is_logged_in(headless=True, root=root)


def post_to_platform(
    platform: str,
    text: str,
    *,
    image: Optional[str] = None,
    headless: bool = True,
    root: Optional[Path] = None,
) -> dict[str, Any]:
    """Post to a single *platform*."""
    adapter = PLATFORMS.get(platform)
    if adapter is None:
        raise ValueError(f"Unknown platform: {platform!r}. Available: {available_platforms()}")
    return adapter.post(text, image=image, headless=headless, root=root)


def post_multi(
    platforms: list[str],
    text: str,
    *,
    platform_text: Optional[dict[str, str]] = None,
    image: Optional[str] = None,
    headless: bool = True,
    root: Optional[Path] = None,
) -> dict[str, dict[str, Any]]:
    """Post to multiple platforms, optionally with per-platform text overrides.

    *platform_text* maps platform names to override text.  If a platform
    is not in the map, *text* is used as the fallback.

    Returns a dict keyed by platform name with each adapter's result.
    """
    overrides = platform_text or {}
    results: dict[str, dict[str, Any]] = {}
    for plat in platforms:
        plat_text = overrides.get(plat, text)
        try:
            results[plat] = post_to_platform(
                plat, plat_text, image=image, headless=headless, root=root,
            )
        except Exception as exc:
            logger.error("social post to %s failed: %s", plat, exc)
            results[plat] = {"ok": False, "message": str(exc)}
    return results
