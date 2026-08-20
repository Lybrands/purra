"""Small, explicit normalizers shared by PurrA value contracts."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any


def required_text(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field} is required")
    return text


def optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def positive_int(value: Any, field: str) -> int:
    normalized = int(value)
    if normalized <= 0:
        raise ValueError(f"{field} must be positive")
    return normalized


def non_negative_int(value: Any, field: str) -> int:
    normalized = int(value)
    if normalized < 0:
        raise ValueError(f"{field} must be non-negative")
    return normalized


def optional_non_negative_int(value: Any, field: str) -> int | None:
    return None if value is None else non_negative_int(value, field)


def optional_positive_int(value: Any, field: str) -> int | None:
    return None if value is None else positive_int(value, field)


def text_tuple(values: Iterable[Any]) -> tuple[str, ...]:
    return tuple(
        text
        for value in values
        if (text := str(value).strip())
    )


def unique_text_tuple(values: Iterable[Any]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(text_tuple(values)))


def text_frozenset(values: Iterable[Any]) -> frozenset[str]:
    return frozenset(
        text
        for value in values
        if (text := str(value).strip())
    )


__all__ = [
    "non_negative_int",
    "optional_non_negative_int",
    "optional_positive_int",
    "optional_text",
    "positive_int",
    "required_text",
    "text_frozenset",
    "text_tuple",
    "unique_text_tuple",
]
