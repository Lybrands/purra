"""Public contracts for host-provided retrieval capabilities."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from purra.json_values import freeze_json_mapping
from purra.ports import CancellationSignal


@dataclass(frozen=True, slots=True)
class RetrievalRequest:
    query: str
    limit: int
    run_id: str | None = None
    scope: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "query", _required_text(
            self.query, "retrieval query"
        ))
        object.__setattr__(self, "limit", _positive_int(
            self.limit, "retrieval limit"
        ))
        if self.run_id is not None:
            object.__setattr__(self, "run_id", _required_text(
                self.run_id, "retrieval run_id"
            ))
        if not isinstance(self.scope, Mapping):
            raise TypeError("retrieval scope must be a mapping")
        object.__setattr__(self, "scope", freeze_json_mapping(self.scope))


@dataclass(frozen=True, slots=True)
class RetrievalHit:
    id: str
    content: str
    source: str
    version: int | None = None
    score: float | None = None
    untrusted: bool = True
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", _required_text(
            self.id, "retrieval hit id"
        ))
        object.__setattr__(self, "content", _required_content(self.content))
        object.__setattr__(self, "source", _required_text(
            self.source, "retrieval hit source"
        ))
        if self.version is not None:
            if isinstance(self.version, bool) or not isinstance(self.version, int):
                raise TypeError("retrieval hit version must be an integer")
            if self.version < 0:
                raise ValueError("retrieval hit version must be non-negative")
        if self.score is not None:
            if isinstance(self.score, bool) or not isinstance(
                self.score, (int, float)
            ):
                raise TypeError("retrieval hit score must be a number")
            score = float(self.score)
            if not math.isfinite(score):
                raise ValueError("retrieval hit score must be finite")
            object.__setattr__(self, "score", score)
        if not isinstance(self.untrusted, bool):
            raise TypeError("retrieval hit untrusted must be a boolean")
        if not isinstance(self.metadata, Mapping):
            raise TypeError("retrieval hit metadata must be a mapping")
        object.__setattr__(
            self, "metadata", freeze_json_mapping(self.metadata)
        )


@runtime_checkable
class Retriever(Protocol):
    async def retrieve(
        self,
        request: RetrievalRequest,
        signal: CancellationSignal | None = None,
    ) -> Sequence[RetrievalHit]: ...


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{field} must be text")
    text = value.strip()
    if not text:
        raise ValueError(f"{field} is required")
    return text


def _required_content(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("retrieval hit content must be text")
    if not value.strip():
        raise ValueError("retrieval hit content is required")
    return value


def _positive_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{field} must be an integer")
    if value <= 0:
        raise ValueError(f"{field} must be positive")
    return value


__all__ = ["RetrievalHit", "RetrievalRequest", "Retriever"]
