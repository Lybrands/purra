"""Context-provider dispatch for the selected context strategy."""

from __future__ import annotations

from purra.context_strategies import ContextStrategy
from purra.contracts import (
    AgentRunRequest,
    ContextBudget,
    ContextBundle,
    TaskContextRequest,
)
from purra.errors import ContractViolationError
from purra.ports import (
    CancellationSignal,
    ContextProvider,
    StagedContextProvider,
)


class ContextCapability:
    """Build one-pass or plan-aware context without domain knowledge."""

    def __init__(
        self,
        strategy: ContextStrategy,
        provider: ContextProvider,
    ) -> None:
        self.strategy = strategy
        self.provider = provider
        self.staged_provider = (
            provider if isinstance(provider, StagedContextProvider) else None
        )
        if self.uses_staged_context and self.staged_provider is None:
            raise TypeError(
                "staged context strategy requires a StagedContextProvider"
            )

    @property
    def uses_staged_context(self) -> bool:
        return self.strategy.uses_staged_context

    async def build_initial(
        self,
        request: AgentRunRequest,
        budget: ContextBudget,
        signal: CancellationSignal | None,
    ) -> ContextBundle:
        if self.uses_staged_context:
            assert self.staged_provider is not None
            bundle = await self.staged_provider.build_planning_context(
                request,
                budget,
                signal,
            )
        else:
            bundle = await self.provider.build_context(request, budget, signal)
        return _require_context_bundle(bundle)

    async def build_execution(
        self,
        request: AgentRunRequest,
        budget: ContextBudget,
        planning_bundle: ContextBundle,
        task_context: TaskContextRequest | None,
        signal: CancellationSignal | None,
    ) -> tuple[ContextBundle, str]:
        if not self.uses_staged_context:
            return planning_bundle, "reactive"
        assert self.staged_provider is not None
        if task_context is not None:
            bundle = await self.staged_provider.build_task_context(
                request,
                budget,
                task_context,
                signal,
            )
            return _require_context_bundle(bundle), "task_spec"
        bundle = await self.staged_provider.build_context(request, budget, signal)
        return _require_context_bundle(bundle), "staged_general"


def _require_context_bundle(value: object) -> ContextBundle:
    if not isinstance(value, ContextBundle):
        raise ContractViolationError(
            "context provider must return ContextBundle"
        )
    return value


__all__ = ["ContextCapability"]
