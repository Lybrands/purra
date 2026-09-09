"""Provider-neutral, inline static image input. No resource fetching or decoding."""

from __future__ import annotations

import base64
from collections.abc import Mapping, Sequence
from typing import Any

STATIC_IMAGE_PROFILE = "purra.static-images/v1"


def static_image_content(text: str, images: list[dict[str, Any]]) -> dict[str, Any]:
    """Build validated JSON content; inputTokens is the host's image token allowance."""
    return parse_static_image_content({"type": STATIC_IMAGE_PROFILE, "text": text, "images": images})


def parse_static_image_content(value: Any) -> dict[str, Any] | None:
    """Return a fresh validated envelope, or None for unrelated JSON content."""
    if not isinstance(value, Mapping) or value.get("type") != STATIC_IMAGE_PROFILE:
        return None
    if set(value) != {"type", "text", "images"} or not isinstance(value["text"], str):
        raise ValueError("Invalid static image content")
    if not isinstance(value["images"], Sequence) or isinstance(value["images"], (str, bytes)) or not value["images"]:
        raise ValueError("Static image content requires images")
    images = []
    total = 0
    for item in value["images"]:
        if not isinstance(item, Mapping) or set(item) != {"mediaType", "dataBase64", "inputTokens"}:
            raise ValueError("Invalid static image fields")
        if item["mediaType"] not in ("image/png", "image/jpeg", "image/webp"):
            raise ValueError("Unsupported static image media type")
        data, tokens = item["dataBase64"], item["inputTokens"]
        if not isinstance(data, str) or not data:
            raise ValueError("Static image data must be canonical base64")
        try:
            decoded = base64.b64decode(data, validate=True)
            if base64.b64encode(decoded).decode("ascii") != data:
                raise ValueError()
        except (ValueError, UnicodeError):
            raise ValueError("Static image data must be canonical base64") from None
        if type(tokens) is not int or tokens <= 0 or tokens > 2**53 - 1:
            raise ValueError("Static image inputTokens must be a positive safe integer")
        total += tokens
        if total > 2**53 - 1:
            raise ValueError("Static image token allowance exceeds safe integer range")
        images.append(dict(item))
    return {"type": STATIC_IMAGE_PROFILE, "text": value["text"], "images": images}


def image_input_tokens(content: Any) -> int:
    value = parse_static_image_content(content)
    return 0 if value is None else sum(image["inputTokens"] for image in value["images"])
