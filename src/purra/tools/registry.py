"""Instance-scoped immutable catalog for explicitly injected Core tools."""

from __future__ import annotations

from collections.abc import Callable, Collection, Iterable
from types import MappingProxyType

from purra.contracts import AgentRunRequest, ToolSchema
from purra.errors import ContractViolationError
from purra.ports import ToolRegistration
from purra.tools.contract import validate_tool_contract


ToolEnablement = Callable[[AgentRunRequest], Iterable[str]]


class InMemoryToolCatalog:
    """A closed registry; domain adapters inject registrations and enablement."""

    def __init__(
        self,
        registrations: Iterable[ToolRegistration],
        enablement: ToolEnablement | None = None,
    ) -> None:
        snapshot = validate_tool_contract(registrations)
        self._registrations = snapshot
        self._by_name = MappingProxyType(
            {registration.schema.name: registration for registration in snapshot}
        )
        self._names = frozenset(self._by_name)
        self._enablement = enablement

    @property
    def names(self) -> frozenset[str]:
        return self._names

    def registrations(self) -> tuple[ToolRegistration, ...]:
        return self._registrations

    def enabled_names(self, request: AgentRunRequest) -> frozenset[str]:
        if self._enablement is None:
            return self.names
        enabled = frozenset(
            str(name).strip()
            for name in self._enablement(request)
            if str(name).strip()
        )
        unknown = enabled - self.names
        if unknown:
            raise ContractViolationError(
                "tool enablement returned unregistered names: "
                + ", ".join(sorted(unknown))
            )
        return enabled

    def get(self, name: str) -> ToolRegistration | None:
        return self._by_name.get(str(name or "").strip())

    def schemas(self, names: Collection[str] | None = None) -> tuple[ToolSchema, ...]:
        selected = self.names if names is None else frozenset(str(name) for name in names)
        unknown = selected - self.names
        if unknown:
            raise ContractViolationError(
                "requested schemas for unregistered tools: "
                + ", ".join(sorted(unknown))
            )
        return tuple(
            registration.schema
            for registration in self._registrations
            if registration.schema.name in selected
        )
