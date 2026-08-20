"""Defensive parsing helpers for model-generated JSON objects."""

from __future__ import annotations

import json
import re
from typing import Any, Mapping


class StructuredOutputParseError(ValueError):
    """A model response did not contain one complete JSON object."""

    def __init__(
        self,
        message: str,
        *,
        reason_code: str,
        output_character_count: int,
    ) -> None:
        super().__init__(message)
        self.reason_code = str(reason_code)
        self.output_character_count = max(0, int(output_character_count))


def parse_json_object(content: Any) -> Mapping[str, Any]:
    """Parse a JSON object, tolerating fences or short surrounding prose."""

    if isinstance(content, Mapping):
        return content
    text = str(content or "").strip()
    if not text:
        raise StructuredOutputParseError(
            "model output is empty",
            reason_code="empty_output",
            output_character_count=0,
        )
    fenced = re.fullmatch(r"```(?:json)?\s*(\{.*\})\s*```", text, re.S | re.I)
    candidate = fenced.group(1) if fenced else text
    try:
        value = json.loads(candidate)
    except (TypeError, json.JSONDecodeError):
        value = _first_embedded_object(text)
        if value is None:
            raise StructuredOutputParseError(
                "model output does not contain a complete JSON object",
                reason_code="invalid_json",
                output_character_count=len(text),
            ) from None
    if not isinstance(value, Mapping):
        raise StructuredOutputParseError(
            "model output must be a JSON object",
            reason_code="non_object_json",
            output_character_count=len(text),
        )
    return value


def _first_embedded_object(text: str) -> Mapping[str, Any] | None:
    start = text.find("{")
    if start < 0:
        return None
    decoder = json.JSONDecoder()
    try:
        value, _end = decoder.raw_decode(text[start:])
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, Mapping) else None


__all__ = [
    "StructuredOutputParseError",
    "parse_json_object",
]
