"""Context assembly styles selected at Agent composition time."""

from __future__ import annotations

from enum import Enum


class ContextStrategy(str, Enum):
    """Choose one-pass context or plan-aware staged retrieval."""

    SINGLE_PASS = "single_pass"
    STAGED = "staged"

    @property
    def uses_staged_context(self) -> bool:
        return self is ContextStrategy.STAGED


__all__ = ["ContextStrategy"]
