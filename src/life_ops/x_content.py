from __future__ import annotations

import base64
import json
import os
import re
from pathlib import Path
from typing import Any, Optional
from urllib import error, request

from life_ops import credentials
from life_ops import store

OPENAI_IMAGE_GENERATE_URL = "https://api.openai.com/v1/images/generations"
XAI_IMAGE_GENERATE_URL = "https://api.x.ai/v1/images/generations"
DEFAULT_OPENAI_IMAGE_MODEL = "gpt-image-1.5"
DEFAULT_XAI_IMAGE_MODEL = "grok-imagine-image"
DEFAULT_OPENAI_IMAGE_SIZE = "1536x1024"
DEFAULT_OPENAI_IMAGE_QUALITY = "high"
DEFAULT_OPENAI_IMAGE_OUTPUT_FORMAT = "png"
DEFAULT_OPENAI_IMAGE_BACKGROUND = "auto"
DEFAULT_OPENAI_IMAGE_MODERATION = "auto"
PACKAGE_GENERATOR_VERSION = "x_package_v1"


def _compact_text(value: str) -> str:
    return " ".join(str(value or "").split())


def _slugify(value: str) -> str:
    collapsed = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower())
    return collapsed.strip("-") or "asset"


def _trim_post(text: str, *, limit: int = 260) -> str:
    collapsed = str(text or "").strip()
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: limit - 3].rstrip() + "..."


def _normalize_points(
    *,
    points: list[str],
    title: str,
    angle: str,
    thesis: str,
) -> list[str]:
    normalized = [_compact_text(point) for point in points if _compact_text(point)]
    if normalized:
        return normalized

    fallback = [_compact_text(thesis), _compact_text(angle), _compact_text(title)]
    return [value for value in fallback if value][:3] or ["Why this matters now", "What people miss", "What to do next"]


def build_x_article_package(
    *,
    title: str,
    angle: str = "",
    audience: str = "",
    thesis: str = "",
    key_points: Optional[list[str]] = None,
    cta: str = "",
    voice: str = "bold, clear, slightly playful",
    visual_style: str = "editorial, cinematic, tactile, high-contrast",
    tags: Optional[list[str]] = None,
) -> dict[str, Any]:
    clean_title = _compact_text(title)
    clean_angle = _compact_text(angle)
    clean_audience = _compact_text(audience)
    clean_thesis = _compact_text(thesis) or clean_angle or clean_title
    clean_cta = _compact_text(cta) or "Follow along if you want sharper systems, cleaner execution, and less drift."
    normalized_tags = [_compact_text(tag) for tag in (tags or []) if _compact_text(tag)]
    points = _normalize_points(points=key_points or [], title=clean_title, angle=clean_angle, thesis=clean_thesis)

    audience_line = (
        f"This is for {clean_audience}: people who need a clearer way to think, decide, and move."
        if clean_audience
        else "This is for people who want a clearer way to think, decide, and move."
    )
    summary = f"{clean_title}. {clean_thesis}. {audience_line}"

    article_sections = [
        f"{clean_title}\n\n{clean_thesis}. {audience_line}",
        "What this piece is really about\n\n"
        + (
            f"The angle here is {clean_angle}. "
            if clean_angle
            else ""
        )
        + "The goal is to make the idea usable, not just interesting.",
    ]
    for index, point in enumerate(points, start=1):
        article_sections.append(
            f"{index}. {point}\n\n"
            f"This is where the piece gets practical. {point} should not stay abstract; it should change how you operate."
        )
    article_sections.append(f"Close\n\n{clean_cta}")
    article_body = "\n\n".join(article_sections).strip()

    posts = [
        _trim_post(
            f"{clean_title}\n\n{clean_thesis}\n\nMost people stay vague here. I'm trying to make it usable."
        )
    ]
    for index, point in enumerate(points, start=1):
        posts.append(
            _trim_post(
                f"{index}/{len(points)} {point}\n\n"
                f"This is one lever that changes the whole system when you actually act on it."
            )
        )
    posts.append(_trim_post(clean_cta))

    image_briefs = [
        {
            "kind": "hero_image",
            "title": f"{clean_title} hero",
            "target_slot": "lead_post",
            "alt_text": f"Hero image for {clean_title}",
            "prompt": (
                f"Create a striking X post illustration for '{clean_title}'. "
                f"Theme: {clean_thesis}. "
                f"Visual style: {visual_style}. "
                "Composition: one strong focal subject, cinematic lighting, strong negative space, designed for a social post cover. "
                "No watermark, no UI chrome, no collage clutter."
            ),
        },
        {
            "kind": "quote_card",
            "title": f"{clean_title} quote card",
            "target_slot": "reply_card",
            "alt_text": f"Quote card visualizing the thesis of {clean_title}",
            "prompt": (
                f"Design a clean visual quote-card style image inspired by '{clean_title}'. "
                f"Core message: {clean_thesis}. "
                f"Style: {visual_style}. "
                "Minimal, bold, elegant, with strong geometry and texture. Avoid dense text; if any lettering appears, keep it minimal and legible."
            ),
        },
        {
            "kind": "process_diagram",
            "title": f"{clean_title} process visual",
            "target_slot": "thread_support",
            "alt_text": f"Process-style image showing the key ideas in {clean_title}",
            "prompt": (
                f"Create an editorial process-style visual for '{clean_title}' using these ideas: {', '.join(points[:3])}. "
                f"Style: {visual_style}. "
                "Make it feel premium, strategic, and information-dense without looking corporate. Designed for X."
            ),
        },
    ]

    return {
        "title": clean_title,
        "summary": summary,
        "article_body": article_body,
        "posts": posts,
        "image_briefs": image_briefs,
        "metadata": {
            "generator_version": PACKAGE_GENERATOR_VERSION,
            "angle": clean_angle,
            "audience": clean_audience,
            "thesis": clean_thesis,
            "voice": voice,
            "visual_style": visual_style,
            "key_points": points,
            "cta": clean_cta,
            "tags": normalized_tags,
        },
    }


def create_x_article_package(
    connection,
    *,
    title: str,
    angle: str = "",
    audience: str = "",
    thesis: str = "",
    key_points: Optional[list[str]] = None,
    cta: str = "",
    voice: str = "bold, clear, slightly playful",
    visual_style: str = "editorial, cinematic, tactile, high-contrast",
    tags: Optional[list[str]] = None,
) -> dict[str, Any]:
    package = build_x_article_package(
        title=title,
        angle=angle,
        audience=audience,
        thesis=thesis,
        key_points=key_points,
        cta=cta,
        voice=voice,
        visual_style=visual_style,
        tags=tags,
    )

    article_id = store.add_x_content_item(
        connection,
        platform="x",
        kind="article",
        title=package["title"],
        summary=package["summary"],
        body_text=package["article_body"],
        status="draft",
        tags=package["metadata"]["tags"],
        metadata=package["metadata"],
    )

    post_ids: list[int] = []
    for index, post_text in enumerate(package["posts"], start=1):
        post_ids.append(
            store.add_x_content_item(
                connection,
                platform="x",
                kind="post",
                title=f"{package['title']} post {index}",
                summary=post_text,
                body_text=post_text,
                status="draft",
                parent_id=article_id,
                sequence_index=index,
                tags=package["metadata"]["tags"],
                metadata={
                    "generator_version": PACKAGE_GENERATOR_VERSION,
                    "target_platform": "x",
                    "post_index": index,
                    "post_count": len(package["posts"]),
                },
            )
        )

    asset_ids: list[int] = []
    for brief in package["image_briefs"]:
        asset_ids.append(
            store.add_x_media_asset(
                connection,
                content_item_id=article_id,
                asset_kind=brief["kind"],
                title=brief["title"],
                prompt_text=brief["prompt"],
                alt_text=brief["alt_text"],
                status="planned",
                metadata={
                    "generator_version": PACKAGE_GENERATOR_VERSION,
                    "target_slot": brief["target_slot"],
                    "visual_style": package["metadata"]["visual_style"],
                },
            )
        )

    return {
        "article_id": article_id,
        "post_ids": post_ids,
        "asset_ids": asset_ids,
        "title": package["title"],
        "summary": package["summary"],
        "posts": package["posts"],
        "image_briefs": package["image_briefs"],
    }


def _openai_api_key() -> str:
    api_key = str(os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        api_key = str(credentials.resolve_secret(name="OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("Set OPENAI_API_KEY before running x-generate-image.")
    return api_key


def _xai_api_key() -> str:
    api_key = str(os.getenv("XAI_API_KEY") or "").strip()
    if not api_key:
        api_key = str(credentials.resolve_secret(name="XAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("Set XAI_API_KEY before running x-generate-image with --provider xai.")
    return api_key


def _post_json_with_bearer(*, url: str, api_key: str, provider_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    try:
        with request.urlopen(req, timeout=120) as response:
            raw = response.read().decode("utf-8")
    except error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="ignore")
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"raw": raw}
        message = payload.get("error", {}).get("message") or payload.get("message") or raw or str(exc)
        raise RuntimeError(f"{provider_name} image generation failed ({exc.code}): {message}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"{provider_name} image generation failed: {exc.reason}") from exc

    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{provider_name} image generation returned non-JSON output.") from exc


def _openai_post_json(*, url: str, payload: dict[str, Any]) -> dict[str, Any]:
    return _post_json_with_bearer(
        url=url,
        api_key=_openai_api_key(),
        provider_name="OpenAI",
        payload=payload,
    )


def _xai_post_json(*, url: str, payload: dict[str, Any]) -> dict[str, Any]:
    return _post_json_with_bearer(
        url=url,
        api_key=_xai_api_key(),
        provider_name="xAI",
        payload=payload,
    )


def _has_openai_key() -> bool:
    return bool(str(os.getenv("OPENAI_API_KEY") or "").strip() or credentials.resolve_secret(name="OPENAI_API_KEY"))


def _has_xai_key() -> bool:
    return bool(str(os.getenv("XAI_API_KEY") or "").strip() or credentials.resolve_secret(name="XAI_API_KEY"))


def _preferred_image_provider() -> str:
    if _has_xai_key():
        return "xai"
    if _has_openai_key():
        return "openai"
    raise RuntimeError("No image provider key is available. Register XAI_API_KEY or OPENAI_API_KEY first.")


def _generate_image_payload(
    *,
    provider: str,
    prompt_text: str,
    model: str,
    size: str,
    quality: str,
    output_format: str,
    background: str,
    moderation: str,
    aspect_ratio: str,
    resolution: str,
) -> tuple[dict[str, Any], str, str]:
    if provider == "xai":
        selected_model = model or DEFAULT_XAI_IMAGE_MODEL
        response_payload = _xai_post_json(
            url=XAI_IMAGE_GENERATE_URL,
            payload={
                "model": selected_model,
                "prompt": prompt_text,
                "response_format": "b64_json",
                "aspect_ratio": aspect_ratio,
                "resolution": resolution,
                "n": 1,
            },
        )
        return response_payload, selected_model, "jpeg"

    if provider == "openai":
        selected_model = model or DEFAULT_OPENAI_IMAGE_MODEL
        response_payload = _openai_post_json(
            url=OPENAI_IMAGE_GENERATE_URL,
            payload={
                "model": selected_model,
                "prompt": prompt_text,
                "size": size,
                "quality": quality,
                "output_format": output_format,
                "background": background,
                "moderation": moderation,
                "n": 1,
            },
        )
        return response_payload, selected_model, output_format

    raise ValueError("provider must be one of: openai, xai")


def _asset_output_paths(
    *,
    asset_id: int,
    asset_title: str,
    content_item_id: Optional[int],
    output_format: str,
) -> tuple[Path, Path]:
    extension = "jpg" if output_format == "jpeg" else output_format
    base = store.life_ops_home()
    directory = store.x_media_root()
    if content_item_id is not None:
        directory = directory / f"content-{content_item_id}"
    filename = f"{asset_id}-{_slugify(asset_title)[:48]}.{extension}"
    absolute = directory / filename
    return absolute, absolute.relative_to(base)


def generate_x_media_asset(
    connection,
    *,
    asset_id: int,
    provider: str = "auto",
    model: str = "",
    size: str = DEFAULT_OPENAI_IMAGE_SIZE,
    quality: str = DEFAULT_OPENAI_IMAGE_QUALITY,
    output_format: str = DEFAULT_OPENAI_IMAGE_OUTPUT_FORMAT,
    background: str = DEFAULT_OPENAI_IMAGE_BACKGROUND,
    moderation: str = DEFAULT_OPENAI_IMAGE_MODERATION,
    aspect_ratio: str = "auto",
    resolution: str = "1k",
) -> dict[str, Any]:
    asset = store.get_x_media_asset(connection, asset_id)
    if asset is None:
        raise ValueError(f"x media asset #{asset_id} was not found")

    prompt_text = _compact_text(str(asset["prompt_text"] or ""))
    if not prompt_text:
        raise ValueError(f"x media asset #{asset_id} does not have a prompt")

    selected_provider = provider
    if selected_provider == "auto":
        selected_provider = _preferred_image_provider()
    if selected_provider not in {"openai", "xai"}:
        raise ValueError("provider must be one of: auto, openai, xai")

    openai_fallback_available = selected_provider == "xai" and _has_openai_key()
    attempted_providers = [selected_provider]
    if openai_fallback_available:
        attempted_providers.append("openai")

    errors: list[str] = []
    response_payload: dict[str, Any] | None = None
    selected_model = ""
    resolved_output_format = output_format
    active_provider = selected_provider

    for candidate_provider in attempted_providers:
        try:
            response_payload, selected_model, resolved_output_format = _generate_image_payload(
                provider=candidate_provider,
                prompt_text=prompt_text,
                model=model,
                size=size,
                quality=quality,
                output_format=output_format,
                background=background,
                moderation=moderation,
                aspect_ratio=aspect_ratio,
                resolution=resolution,
            )
            active_provider = candidate_provider
            break
        except RuntimeError as exc:
            errors.append(f"{candidate_provider}: {exc}")
    else:
        if selected_provider == "xai" and not openai_fallback_available:
            errors.append("openai: OpenAI fallback unavailable because OPENAI_API_KEY is not configured")
        raise RuntimeError(" | ".join(errors))

    assert response_payload is not None

    response_items = response_payload.get("data") or []
    if not response_items:
        raise RuntimeError(f"{active_provider} image generation returned no image data.")

    first_item = response_items[0]
    encoded_image = str(first_item.get("b64_json") or "").strip()
    if not encoded_image:
        raise RuntimeError(f"{active_provider} image generation did not return a base64 image payload.")

    image_bytes = base64.b64decode(encoded_image)
    absolute_path, relative_path = _asset_output_paths(
        asset_id=asset_id,
        asset_title=str(asset["title"] or ""),
        content_item_id=int(asset["content_item_id"]) if asset["content_item_id"] is not None else None,
        output_format=resolved_output_format,
    )
    absolute_path.parent.mkdir(parents=True, exist_ok=True)
    absolute_path.write_bytes(image_bytes)

    metadata = {
        "provider": active_provider,
        "provider_requested": provider,
        "attempted_providers": attempted_providers,
        "fallback_used": active_provider != selected_provider,
        "generated_with": "openai_images_api",
        "output_format": resolved_output_format,
    }
    if errors:
        metadata["fallback_errors"] = errors
    if active_provider == "xai":
        metadata["generated_with"] = "xai_images_api"
        metadata["aspect_ratio"] = aspect_ratio
        metadata["resolution"] = resolution
    else:
        metadata["size"] = size
        metadata["quality"] = quality
        metadata["background"] = background
        metadata["moderation"] = moderation
    revised_prompt = _compact_text(str(first_item.get("revised_prompt") or ""))
    if revised_prompt:
        metadata["revised_prompt"] = revised_prompt

    store.update_x_media_asset(
        connection,
        asset_id=asset_id,
        status="generated",
        model_name=selected_model,
        relative_path=str(relative_path),
        metadata=metadata,
        error_text="",
    )

    return {
        "asset_id": asset_id,
        "title": str(asset["title"] or ""),
        "provider": active_provider,
        "requested_provider": provider,
        "attempted_providers": attempted_providers,
        "fallback_used": active_provider != selected_provider,
        "model": selected_model,
        "relative_path": str(relative_path),
        "size": size if active_provider == "openai" else None,
        "quality": quality if active_provider == "openai" else None,
        "output_format": resolved_output_format,
        "aspect_ratio": aspect_ratio if active_provider == "xai" else None,
        "resolution": resolution if active_provider == "xai" else None,
        "bytes_written": len(image_bytes),
    }
