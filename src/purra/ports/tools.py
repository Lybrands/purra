"""Tool execution, observation, approval, and idempotency ports."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    ApprovalDecision,
    ApprovalRequest,
    ApprovalResult,
    ApprovalStatus,
    ExecutionState,
    ExecutionTransition,
    RunId,
    ToolBatchOutcome,
    ToolBatchRequest,
    ToolBatchResult,
    ToolCall,
    ToolContextContract,
    ToolDataContract,
    ToolHandlerResult,
    ToolPolicy,
    ToolPlanningRequirement,
    ToolSchema,
    TraceRecord,
)
from purra.structured import StructuredOutputContract
from purra.normalization import optional_positive_int
from purra.events import AgentEvent
from purra.json_values import freeze_json_mapping
from purra.ports.model import CancellationSignal


@runtime_checkable
class ToolHandler(Protocol):
    async def __call__(
        self,
        state: ExecutionState,
        arguments: Mapping[str, Any],
        signal: CancellationSignal | None = None,
    ) -> ToolHandlerResult: ...


@runtime_checkable
class ToolCallHandler(Protocol):
    """Core system handler that also receives the immutable ToolCall identity."""

    async def __call__(
        self,
        state: ExecutionState,
        arguments: Mapping[str, Any],
        tool_call: ToolCall,
        signal: CancellationSignal | None = None,
    ) -> ToolHandlerResult: ...


@runtime_checkable
class ScopeValidator(Protocol):
    async def __call__(
        self,
        state: ExecutionState,
        arguments: Mapping[str, Any],
        signal: CancellationSignal | None = None,
    ) -> str | None: ...


@runtime_checkable
class CacheProbe(Protocol):
    def will_hit(
        self,
        state: ExecutionState,
        arguments: Mapping[str, Any],
    ) -> bool: ...


@runtime_checkable
class OperationDisplayParamsResolver(Protocol):
    """Return a small public display projection for one validated tool call."""

    def __call__(
        self,
        state: ExecutionState,
        arguments: Mapping[str, Any],
        tool_call: ToolCall,
    ) -> Mapping[str, Any]: ...


@dataclass(frozen=True, slots=True)
class ToolRegistration:
    schema: ToolSchema
    handler: ToolHandler
    policy: ToolPolicy
    scope_validator: ScopeValidator | None = None
    cache_probe: CacheProbe | None = None
    cancellation_linearizable: bool = False
    # Long-running host workflows may own durable state transitions outside
    # Core's single tool-receipt transaction.
    host_managed_durability: bool = False
    context_contract: ToolContextContract = ToolContextContract()
    data_contract: ToolDataContract = ToolDataContract()
    max_argument_chars: int | None = None
    planning_capability: ToolSchema | None = None
    planning_requirement: ToolPlanningRequirement = (
        ToolPlanningRequirement.OPTIONAL
    )
    host_planned_arguments: Mapping[str, Any] | None = None
    call_handler: ToolCallHandler | None = None
    operation_display_params: OperationDisplayParamsResolver | None = None
    argument_contract: StructuredOutputContract | None = None
    concurrency_safe: bool = False

    def __post_init__(self) -> None:
        if type(self.concurrency_safe) is not bool or (self.concurrency_safe and (self.policy.mode != "read" or self.policy.risk_level != "read")):
            raise ValueError("concurrency_safe requires an explicitly read-only tool")
        if self.argument_contract is not None:
            if not isinstance(self.argument_contract, StructuredOutputContract) or self.argument_contract.schema != self.schema.parameters or self.argument_contract.mode != "local":
                raise ValueError("argument contract must match the tool schema and use local validation")
        if self.call_handler is not None and not isinstance(
            self.call_handler,
            ToolCallHandler,
        ):
            raise TypeError("tool call handler must implement ToolCallHandler")
        if self.operation_display_params is not None and not isinstance(
            self.operation_display_params,
            OperationDisplayParamsResolver,
        ):
            raise TypeError(
                "tool operation display params must implement "
                "OperationDisplayParamsResolver"
            )
        object.__setattr__(
            self,
            "host_managed_durability",
            bool(self.host_managed_durability),
        )
        if not isinstance(self.context_contract, ToolContextContract):
            raise TypeError("tool context_contract must be ToolContextContract")
        if not isinstance(self.data_contract, ToolDataContract):
            raise TypeError("tool data_contract must be ToolDataContract")
        object.__setattr__(self, "max_argument_chars", optional_positive_int(
            self.max_argument_chars, "tool max_argument_chars"
        ))
        if (
            self.planning_capability is not None
            and not isinstance(self.planning_capability, ToolSchema)
        ):
            raise TypeError("tool planning_capability must be a ToolSchema")
        object.__setattr__(
            self,
            "planning_requirement",
            ToolPlanningRequirement(self.planning_requirement),
        )
        if self.host_planned_arguments is not None:
            if not isinstance(self.host_planned_arguments, Mapping):
                raise TypeError("tool host_planned_arguments must be a mapping")
            if self.data_contract.model_owned_paths:
                raise ValueError(
                    "host-planned tools cannot require model-owned arguments"
                )
            object.__setattr__(
                self,
                "host_planned_arguments",
                freeze_json_mapping(self.host_planned_arguments),
            )

    @property
    def prerequisite_tools(self) -> tuple[str, ...]:
        return self.context_contract.prerequisite_tools


@runtime_checkable
class ToolCatalog(Protocol):
    """Expose the full registry for startup validation and a request scope."""

    def registrations(self) -> Sequence[ToolRegistration]: ...

    def enabled_names(self, request: AgentRunRequest) -> frozenset[str]: ...


@runtime_checkable
class EventSink(Protocol):
    async def emit(self, event: AgentEvent) -> None: ...


@runtime_checkable
class ToolExecutionGateway(Protocol):
    """Boundary over a complete, policy-enforcing executor."""

    async def execute_batch(
        self,
        request: ToolBatchRequest,
        event_sink: EventSink,
        signal: CancellationSignal | None = None,
    ) -> ToolBatchResult: ...


@runtime_checkable
class RuntimeObserver(Protocol):
    """Observe runtime state and expose the compiled active transition."""

    def current_execution_transition(self) -> ExecutionTransition | None: ...

    async def record_trace(self, trace: TraceRecord) -> None: ...

    async def on_model_delta(self) -> None: ...

    async def on_tool_calls_started(self, tool_names: tuple[str, ...]) -> None: ...

    async def on_tool_round_completed(
        self,
        outcome: ToolBatchOutcome = ToolBatchOutcome.COMPLETED,
    ) -> None: ...


@runtime_checkable
class RuntimePlanningHook(Protocol):
    """Revise runtime authority after an exceptional planning event."""

    async def replan_after_tool(
        self,
        messages: Sequence[AgentMessage],
        *,
        round_number: int,
        remaining_model_rounds: int,
        outcome: ToolBatchOutcome,
        signal: CancellationSignal | None = None,
    ) -> AgentMessage | None: ...


@runtime_checkable
class ApprovalGateway(Protocol):
    """Own approval events and consume each run-bound decision once."""

    async def request(
        self,
        run_id: RunId,
        approval: ApprovalRequest,
        event_sink: EventSink,
        signal: CancellationSignal | None = None,
    ) -> ApprovalResult: ...

    async def resolve(
        self,
        run_id: RunId,
        approval_id: str,
        decision: ApprovalDecision,
    ) -> ApprovalStatus | None: ...

    async def cancel_pending(self, run_id: RunId) -> int: ...


@runtime_checkable
class ToolIdempotencyGateway(Protocol):
    """Execute one side-effecting tool call at most once for a Run."""

    async def execute_once(
        self,
        run_id: RunId,
        tool_call: ToolCall,
        operation: Callable[[], Awaitable[ToolHandlerResult]],
    ) -> ToolHandlerResult: ...


__all__ = [name for name in globals() if not name.startswith("_")]
