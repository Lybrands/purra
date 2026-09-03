"""Stable retrieval failures safe for host-side policy decisions."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from purra.errors import CodedAgentCoreError


_ERROR_CODES = frozenset({
    "retrieval_scope_unavailable",
    "retrieval_access_denied",
    "retrieval_source_unavailable",
    "retrieval_timeout",
    "retrieval_index_not_ready",
    "invalid_retrieval_result",
    "retrieval_result_too_large",
})


class RetrievalError(CodedAgentCoreError):
    def __init__(
        self,
        message: str,
        *,
        code: str,
        details: Mapping[str, Any] | None = None,
        retryable: bool = False,
    ) -> None:
        normalized = str(code or "").strip()
        if normalized not in _ERROR_CODES:
            raise ValueError(f"unsupported retrieval error code: {normalized}")
        super().__init__(message, code=normalized, details=details)
        self.retryable = bool(retryable)


__all__ = ["RetrievalError"]
