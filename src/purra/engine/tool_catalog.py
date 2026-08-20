"""Tool-catalog composition for Core-owned capabilities."""

from __future__ import annotations

from typing import Sequence

from purra.contracts import AgentRunRequest
from purra.ports import ToolCatalog, ToolRegistration


class AugmentedToolCatalog:
    """Add Core-owned capability tools without bypassing product enablement."""

    def __init__(
        self,
        base: ToolCatalog,
        extras: Sequence[ToolRegistration],
    ) -> None:
        self._base = base
        self._extras = tuple(extras)
        self._extra_names = frozenset(
            registration.schema.name for registration in self._extras
        )

    def registrations(self) -> tuple[ToolRegistration, ...]:
        return (*tuple(self._base.registrations()), *self._extras)

    def enabled_names(self, request: AgentRunRequest) -> frozenset[str]:
        return frozenset(self._base.enabled_names(request)) | self._extra_names


__all__ = ["AugmentedToolCatalog"]
