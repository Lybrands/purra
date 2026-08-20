"""Tolerant value readers shared by content-free evaluation reports."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def as_sequence(value: Any) -> tuple[Any, ...]:
    return tuple(value) if isinstance(value, (list, tuple)) else ()


def non_negative_integer(value: Any) -> int:
    if value is None or isinstance(value, bool):
        return 0
    return permissive_non_negative_integer(value)


def permissive_non_negative_integer(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def optional_non_negative_integer(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


__all__ = [
    "as_mapping",
    "as_sequence",
    "non_negative_integer",
    "optional_non_negative_integer",
    "permissive_non_negative_integer",
]
