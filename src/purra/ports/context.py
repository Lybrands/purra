"""Context retrieval and compaction ports."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Protocol, runtime_checkable

from purra.context_orchestration.contracts import (
    ContextCompressionRequest,
    ConversationCompactionResult,
)
from purra.context_orchestration.ledger import ContextCompactionBudget
from purra.operations.contracts import OperationScope
from purra.contracts import (
    AgentRunRequest,
    ContextBudget,
    ContextBudgetClaim,
    ContextBundle,
    TaskContextRequest,
)
from purra.ports.model import CancellationSignal


@runtime_checkable
class ContextProvider(Protocol):
    async def build_context(
        self,
        request: AgentRunRequest,
        budget: ContextBudget,
        signal: CancellationSignal | None = None,
    ) -> ContextBundle: ...


@runtime_checkable
class ContextDemandProvider(Protocol):
    """Optionally describe request-specific demand before Core allocates it."""

    async def describe_context_demands(
        self,
        request: AgentRunRequest,
        signal: CancellationSignal | None = None,
    ) -> tuple[ContextBudgetClaim, ...]: ...


@runtime_checkable
class StagedContextProvider(Protocol):
    """Optional provider separating lightweight planning from formal recall."""

    async def build_planning_context(
        self,
        request: AgentRunRequest,
        budget: ContextBudget,
        signal: CancellationSignal | None = None,
    ) -> ContextBundle: ...

    async def build_task_context(
        self,
        request: AgentRunRequest,
        budget: ContextBudget,
        task: TaskContextRequest,
        signal: CancellationSignal | None = None,
    ) -> ContextBundle: ...


@runtime_checkable
class TaskContextDemandProvider(Protocol):
    """Optionally declare post-planning demand from a compiled TaskSpec."""

    async def describe_task_context_demands(
        self,
        request: AgentRunRequest,
        task: TaskContextRequest,
        signal: CancellationSignal | None = None,
    ) -> tuple[ContextBudgetClaim, ...]: ...


@runtime_checkable
class ContextCompressionHook(Protocol):
    """Application-owned implementation of semantic context reduction."""

    async def compress(
        self,
        compression: ContextCompressionRequest,
        signal: CancellationSignal | None = None,
    ) -> ConversationCompactionResult: ...


@runtime_checkable
class ConversationCompactor(Protocol):
    """Core coordinator used by the run lifecycle."""

    async def prepare(
        self,
        request: AgentRunRequest,
        signal: CancellationSignal | None = None,
        *,
        on_compaction_started: (
            Callable[[Mapping[str, Any]], Awaitable[None]] | None
        ) = None,
        budget: ContextCompactionBudget | None = None,
        operation_scope: OperationScope | None = None,
    ) -> ConversationCompactionResult: ...


__all__ = [name for name in globals() if not name.startswith("_")]
