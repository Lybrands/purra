"""Validation helpers for provider-neutral tool payload paths."""

from __future__ import annotations

from typing import Any


def tool_data_path(value: Any) -> str:
    path = str(value or "").strip()
    if not path:
        raise ValueError("tool data path must not be empty")
    segments = path.split(".")
    if any(
        not segment
        or segment == "[]"
        or "[" in segment.removesuffix("[]")
        or "]" in segment.removesuffix("[]")
        for segment in segments
    ):
        raise ValueError(f"invalid tool data path: {path!r}")
    return path


__all__ = ["tool_data_path"]
