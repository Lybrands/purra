"""Model-defined delegated Agent execution inside one active Root Run."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass, replace

from purra.context_budget import (
    allocate_context_budget,
    resolve_context_budget_claims,
)
from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    ContextBudget,
    ContextBundle,
    ExecutionState,
    MessageOrigin,
    MessageRole,
    RuntimeLimits,
    RuntimeOutcome,
    ToolCall,
    ToolExecutionLimits,
)
from purra.delegation.coordinator import (
    DelegatedAgentRequest,
    DelegatedAgentResult,
)
from purra.delegation.policy import DelegationPolicy
from purra.engine.canonical_sink import runtime_output_event
from purra.engine.context_phase import (
    assemble_messages,
    validate_context_allocations,
)
from purra.engine.planning_validation import effective_registrations
from purra.errors import ContractViolationError
from purra.events import AgentEvent
from purra.json_values import thaw_json_mapping
from purra.model_execution import AgentModelTaskRunner
from purra.model_invocation import AgentModelInvocationManager, ModelInvocationContext
from purra.model_protocol import resolve_invocation_output_limit
from purra.operations import AgentOperationController
from purra.output import ResponseTransactionMode
from purra.output.processor import AgentOutputProcessor
from purra.ports import (
    ApprovalGateway,
    CancellationSignal,
    ContextProvider,
    ConversationCompactor,
    ExecutionStateFactory,
    ModelGateway,
    ToolCatalog,
    ToolIdempotencyGateway,
)
from purra.recovery import RecoveryPolicy
from purra.runtime import AgentRuntime
from purra.tools import CoreToolExecutor, InMemoryToolCatalog


@dataclass(frozen=True, slots=True)
class _BoundRun:
    request: AgentRunRequest


class DynamicDelegatedAgentExecutor:
    """Create and execute model-defined, authority-bounded Agents."""

    def __init__(
        self,
        *,
        tool_catalog: ToolCatalog,
        model_gateway: ModelGateway,
        model_manager: AgentModelInvocationManager,
        output_processor: AgentOutputProcessor,
        operation_controller: AgentOperationController,
        approval_gateway: ApprovalGateway,
        tool_idempotency_gateway: ToolIdempotencyGateway,
        policy: DelegationPolicy = DelegationPolicy(),
        context_provider: ContextProvider | None = None,
        context_provider_factory=None,
        conversation_compactor: ConversationCompactor | None = None,
        conversation_compactor_factory=None,
        execution_state_factory: ExecutionStateFactory | None = None,
        runtime_limits: RuntimeLimits = RuntimeLimits(),
        recovery_policy: RecoveryPolicy = RecoveryPolicy(),
        tool_execution_limits: ToolExecutionLimits = ToolExecutionLimits(),
    ) -> None:
        if not isinstance(tool_catalog, ToolCatalog):
            raise TypeError("delegated Agent executor requires a tool catalog")
        if not isinstance(policy, DelegationPolicy):
            raise TypeError("delegated Agent executor requires a DelegationPolicy")
        if context_provider is not None and context_provider_factory is not None:
            raise ValueError(
                "delegated Agent context provider and factory are mutually exclusive"
            )
        if (
            conversation_compactor is not None
            and conversation_compactor_factory is not None
        ):
            raise ValueError(
                "delegated Agent compactor and factory are mutually exclusive"
            )
        if not isinstance(tool_idempotency_gateway, ToolIdempotencyGateway):
            raise TypeError(
                "delegated Agent executor requires a tool idempotency gateway"
            )
        self._tool_catalog = tool_catalog
        self._model_gateway = model_gateway
        self._model_manager = model_manager
        self._output = output_processor
        self._operations = operation_controller
        self._approval = approval_gateway
        self._idempotency = tool_idempotency_gateway
        self._policy = policy
        self._context_provider = context_provider
        self._context_provider_factory = context_provider_factory
        self._conversation_compactor = conversation_compactor
        self._conversation_compactor_factory = conversation_compactor_factory
        self._execution_state_factory = execution_state_factory
        self._runtime_limits = runtime_limits
        self._recovery_policy = recovery_policy
        self._tool_limits = tool_execution_limits
        self._runs: dict[str, _BoundRun] = {}

    def bind_run(
        self,
        run_id: str,
        request: AgentRunRequest,
        prepared_messages: Sequence[AgentMessage],
    ) -> None:
        del prepared_messages
        if run_id in self._runs:
            raise ContractViolationError("delegation Run is already bound")
        self._runs[run_id] = _BoundRun(request=request)

    def release_run(self, run_id: str) -> None:
        self._runs.pop(run_id, None)

    async def execute(
        self,
        request: DelegatedAgentRequest,
        signal: CancellationSignal | None = None,
    ) -> DelegatedAgentResult:
        bound = self._runs.get(request.run_id)
        if bound is None:
            return DelegatedAgentResult(
                outcome=RuntimeOutcome.FAILED,
                error_code="delegation_root_run_not_active",
            )
        if request.context_mode is not self._policy.context_mode:
            return DelegatedAgentResult(
                outcome=RuntimeOutcome.FAILED,
                error_code="delegation_context_mode_not_allowed",
            )

        delegated_request = _delegated_request(request, bound)
        invocation_context = ModelInvocationContext(
            run_id=request.run_id,
            turn_id=request.delegation_id,
        )
        model_tasks = AgentModelTaskRunner(
            self._model_manager,
            invocation_context,
        )
        provider = self._resolve_context_provider(model_tasks)
        registrations, enabled_names = self._read_capabilities(bound.request)
        schemas = tuple(
            registration.schema
            for registration in registrations
            if registration.schema.name in enabled_names
        )
        catalog = InMemoryToolCatalog(
            registrations,
            enablement=lambda _request: enabled_names,
        )
        output_limit = resolve_invocation_output_limit(
            delegated_request.model.capability_snapshot,
            delegated_request.model.options.get("max_tokens"),
        )
        budget = allocate_context_budget(
            window_tokens=delegated_request.context_window or 128_000,
            output_reserve_tokens=output_limit.max_tokens,
            tools=schemas,
            claims=await resolve_context_budget_claims(
                provider,
                delegated_request,
                signal=signal,
            ),
        )
        bundle = await provider.build_context(delegated_request, budget, signal)
        if not isinstance(bundle, ContextBundle):
            raise ContractViolationError(
                "delegated Agent context provider must return ContextBundle"
            )
        validate_context_allocations(bundle, budget)
        prepared = replace(
            delegated_request,
            messages=assemble_messages(
                delegated_request.messages,
                bundle.blocks,
                None,
            ),
            context_window=budget.window_tokens,
        )
        state = (
            self._execution_state_factory.create(prepared)
            if self._execution_state_factory is not None
            else ExecutionState()
        )
        if not isinstance(state, ExecutionState):
            raise ContractViolationError(
                "delegated execution state factory must return ExecutionState"
            )
        state.run_id = request.run_id
        compactor = (
            self._conversation_compactor_factory(model_tasks)
            if self._conversation_compactor_factory is not None
            else self._conversation_compactor
        )
        runtime = AgentRuntime(
            model_gateway=self._model_gateway,
            tool_execution_gateway=CoreToolExecutor(
                catalog,
                self._approval,
                self._tool_limits,
                _ScopedToolIdempotencyGateway(
                    self._idempotency,
                    request.delegation_id,
                ),
                self._operations,
            ),
            context_compressor=compactor,
            limits=self._runtime_limits,
            recovery_policy=self._recovery_policy,
            operation_controller=self._operations,
            output_observer=self._output,
            model_manager=self._model_manager,
        )
        result = None
        async for update in runtime.run(
            prepared,
            tools=schemas,
            execution_state=state,
            run_id=request.run_id,
            turn_id=request.delegation_id,
            context_budget=budget,
            output_limit=output_limit,
            scope_tools_to_observer=False,
            response_transaction_mode=ResponseTransactionMode.VALIDATED_RESULT,
            signal=signal,
        ):
            if isinstance(update, AgentEvent):
                event = replace(update, payload={
                    **thaw_json_mapping(update.payload),
                    "delegationId": request.delegation_id,
                    "delegationBatchId": request.batch_id,
                    "delegatedAgentName": request.agent_name,
                    "delegatedAgentTitle": request.agent_title,
                })
                await self._output.accept_runtime_event(
                    runtime_output_event(event, request.run_id)
                )
            else:
                result = update
        if result is None:
            return DelegatedAgentResult(
                outcome=RuntimeOutcome.FAILED,
                error_code="delegated_agent_returned_no_result",
            )
        return DelegatedAgentResult(
            outcome=result.outcome,
            content=result.final_response,
            error_code=result.error_code,
        )

    def _resolve_context_provider(
        self,
        model_tasks: AgentModelTaskRunner,
    ) -> ContextProvider:
        provider = (
            self._context_provider_factory(model_tasks)
            if self._context_provider_factory is not None
            else self._context_provider or _EmptyContextProvider()
        )
        if not isinstance(provider, ContextProvider):
            raise ContractViolationError(
                "delegated Agent context factory must return ContextProvider"
            )
        return provider

    def _read_capabilities(
        self,
        root_request: AgentRunRequest,
    ) -> tuple[tuple, frozenset[str]]:
        registrations, enabled = effective_registrations(
            self._tool_catalog,
            self._tool_catalog.registrations(),
            root_request,
            model_supports_tools=True,
        )
        readable = tuple(
            registration
            for registration in registrations
            if registration.policy.mode is self._policy.tool_mode
            and (
                self._policy.allows_recursive_delegation
                or registration.schema.name != "delegateToAgents"
            )
        )
        readable_names = frozenset(
            registration.schema.name for registration in readable
        )
        return readable, frozenset(enabled) & readable_names


def _delegated_request(
    request: DelegatedAgentRequest,
    bound: _BoundRun,
) -> AgentRunRequest:
    input_payload = thaw_json_mapping(request.input_payload)
    objective = request.objective
    if input_payload:
        objective += "\n\nInput:\n" + json.dumps(
            input_payload,
            ensure_ascii=False,
            separators=(",", ":"),
        )
    return replace(
        bound.request,
        messages=(
            AgentMessage(
                role=MessageRole.SYSTEM,
                content=request.agent_instruction,
                origin=MessageOrigin.MODEL,
                attributes={
                    "delegated_agent_definition": "model",
                    "delegated_agent_name": request.agent_name,
                },
            ),
            AgentMessage(role=MessageRole.USER, content=objective),
        ),
        domain_context=bound.request.domain_context,
        metadata={
            "delegationId": request.delegation_id,
            "delegationBatchId": request.batch_id,
            "delegatedAgentName": request.agent_name,
            "delegatedAgentTitle": request.agent_title,
        },
    )


class _EmptyContextProvider:
    async def build_context(
        self,
        request: AgentRunRequest,
        budget: ContextBudget,
        signal: CancellationSignal | None = None,
    ) -> ContextBundle:
        del request, budget, signal
        return ContextBundle()


class _ScopedToolIdempotencyGateway:
    def __init__(
        self,
        gateway: ToolIdempotencyGateway,
        delegation_id: str,
    ) -> None:
        self._gateway = gateway
        self._prefix = f"delegation:{delegation_id}:"

    async def execute_once(self, run_id, tool_call, operation):
        scoped_call = replace(tool_call, id=self._prefix + tool_call.id)
        if not isinstance(scoped_call, ToolCall):
            raise ContractViolationError("invalid scoped delegated tool call")
        return await self._gateway.execute_once(run_id, scoped_call, operation)


__all__ = ["DynamicDelegatedAgentExecutor"]
