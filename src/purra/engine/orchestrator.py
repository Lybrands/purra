"""The single high-level orchestration entry for a complete PurrA run."""

from __future__ import annotations

import asyncio
from contextlib import aclosing, suppress
from dataclasses import dataclass, replace
from time import perf_counter
from typing import Any, AsyncIterator, Callable, Iterable, Mapping, Sequence

from purra.agent_presets import (
    AgentPreset,
    AgentPresetSnapshot,
)
from purra.cancellation import (
    OperationCanceled,
    await_with_cancellation,
    is_canceled as _is_canceled,
)
from purra.context_budget import (
    allocate_context_budget,
    estimate_agent_messages_tokens,
    estimate_json_tokens,
    resolve_context_budget_claims,
    resolve_task_context_budget_claims,
)
from purra.context_strategies import ContextStrategy
from purra.context_orchestration.contracts import (
    ConversationCompactionResult,
)
from purra.context_orchestration.compaction import (
    ContextCompressionCoordinator,
)
from purra.context_orchestration.ledger import (
    ContextCompactionBudget,
    ContextCompactionPhase,
)
from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    AgentRunResult,
    AgentRuntimeResult,
    ApprovalDecision,
    ApprovalStatus,
    ContextBlock,
    ContextBudget,
    ContextBudgetClaim,
    ContextBundle,
    ExecutionState,
    MessageOrigin,
    MessageRole,
    ResponseConstraints,
    RunCreateParams,
    RunId,
    RunProvenance,
    RunStatus,
    RuntimeLimits,
    RuntimeOutcome,
    StepExecutor,
    StepStatus,
    TaskContextRequest,
    ExecutionPlan,
    TaskStep,
    ToolExecutionLimits,
    TraceRecord,
)
from purra.normalization import optional_text as _optional_text
from purra.errors import (
    ContextOverflowError,
    ContractViolationError,
    UnsupportedModelFeatureError,
)
from purra.events import AgentEvent, CoreEventType
from purra.engine.context_phase import (
    assemble_messages as _assemble_messages,
    compile_task_context_request as _compile_task_context_request,
    context_demand_diagnostics as _context_demand_diagnostics,
    merge_context_claims as _merge_context_claims,
    planned_tool_names as _planned_tool_names,
    validate_context_allocations as _validate_context_allocations,
)
from purra.engine.context_capability import ContextCapability
from purra.engine.canonical_sink import (
    BufferedEventSink as _BufferedEventSink,
    runtime_output_event as _runtime_output_event,
)
from purra.engine.durable_execution import (
    bind_event_to_run as _bind_event_to_run,
)
from purra.engine.options import AgentCoreRunOptions
from purra.engine.planning_phase import PlanningCapability, PlanningPhaseResult
from purra.engine.task_orchestration import TaskOrchestrationCapability
from purra.delegation import (
    DelegatedAgentExecutor,
    DelegationCoordinator,
    DelegationPolicy,
    build_delegation_tool_registration,
)
from purra.delegation.dynamic_executor import DynamicDelegatedAgentExecutor
from purra.execution import AgentRunHandle, AgentRunSupervisor
from purra.engine.planning_validation import (
    effective_registrations as _effective_registrations,
)
from purra.engine.tool_catalog import AugmentedToolCatalog as _AugmentedToolCatalog
from purra.execution_profiles import ExecutionProfile
from purra.host_planned_tool_gateway import HostPlannedToolGateway
from purra.json_values import thaw_json_mapping
from purra.model_protocol import resolve_invocation_output_limit
from purra.model_invocation import (
    AgentModelInvocationManager,
    ModelInvocationContext,
)
from purra.model_execution import AgentModelResponseJudge, AgentModelTaskRunner
from purra.planner import effective_planning_tool_names
from purra.planning_policies import ReactivePlanningPolicy
from purra.plan_compiler import (
    projected_planning_tool_schemas,
    runtime_tool_names_for_planning_names,
)
from purra.ports import (
    ApprovalGateway,
    CancellationSignal,
    CONTROLLER_OWNED_RUN_EVENT_TYPES,
    ContextProvider,
    ConversationCompactor,
    DelegationRepository,
    ExecutionLeaseStore,
    ExecutionStateFactory,
    PlanningPolicy,
    ResponseJudge,
    ResponseValidator,
    RunRepository,
    TaskContextDemandProvider,
    WorkPlanner,
    ToolCatalog,
    ToolIdempotencyGateway,
    ToolRegistration,
    ModelGateway,
)
from purra.output import AgentResponseTransaction
from purra.output.contracts import PublicPresentationMode, ResponseTransactionMode
from purra.output.processor import AgentOutputProcessor
from purra.output.run_repository import CanonicalRunRepository
from purra.output.ports import AgentOutputPublisher, AgentOutputRepository
from purra.run_controller import AgentRunController
from purra.runtime import AgentRuntime
from purra.recovery import RecoveryPolicy
from purra.tools import (
    CoreToolExecutor,
    InMemoryApprovalGateway,
    InMemoryToolCatalog,
    model_visible_tool_schema,
    resolve_tool_display_name,
)
from purra.task_admission import (
    ExecutionMode,
    LongTaskDispatcher,
    LongTaskExecutionStatus,
    LongTaskExecutionUpdate,
    TaskAdmissionEvaluator,
)
from purra.timing import duration_ms as _duration_ms
from purra.operations import AgentOperationController, OperationScope


class AgentCore:
    """Compose context, model/tool runtime and optional execution capabilities.

    ``submit`` is the only complete-run entry and returns a stable,
    server-owned execution handle.
    Concrete domain tools enter only as registrations in ``tool_catalog``.
    A bare Core is reactive. Planning requires both an explicit planner and an
    explicit non-reactive policy.
    """

    def __init__(
        self,
        *,
        model_gateway: ModelGateway,
        run_repository: RunRepository,
        preset: AgentPreset | None = None,
        execution_profile: ExecutionProfile | None = None,
        planner: WorkPlanner | None = None,
        planning_policy: PlanningPolicy | None = None,
        context_strategy: ContextStrategy | str | None = None,
        context_provider: ContextProvider | None = None,
        context_provider_factory: (
            Callable[[AgentModelTaskRunner], ContextProvider] | None
        ) = None,
        conversation_compactor: ConversationCompactor | None = None,
        conversation_compactor_factory: (
            Callable[[AgentModelTaskRunner], ConversationCompactor] | None
        ) = None,
        execution_state_factory: ExecutionStateFactory | None = None,
        tool_catalog: ToolCatalog | None = None,
        delegation_policy: DelegationPolicy = DelegationPolicy(),
        task_admission_evaluator: TaskAdmissionEvaluator | None = None,
        long_task_dispatcher: LongTaskDispatcher | None = None,
        approval_gateway: ApprovalGateway | None = None,
        tool_idempotency_gateway: ToolIdempotencyGateway | None = None,
        runtime_limits: RuntimeLimits = RuntimeLimits(),
        recovery_policy: RecoveryPolicy = RecoveryPolicy(),
        tool_execution_limits: ToolExecutionLimits = ToolExecutionLimits(),
        operation_controller: AgentOperationController | None = None,
        output_processor: AgentOutputProcessor | None = None,
        output_repository: AgentOutputRepository | None = None,
        output_publisher: AgentOutputPublisher | None = None,
        execution_lease_store: ExecutionLeaseStore | None = None,
        execution_owner_id: str | None = None,
        execution_lease_duration_ms: int | None = None,
        delegation_repository: DelegationRepository | None = None,
        delegated_agent_executor: DelegatedAgentExecutor | None = None,
    ) -> None:
        if preset is not None:
            if not isinstance(preset, AgentPreset):
                raise TypeError("preset must be an AgentPreset")
            if any((
                execution_profile is not None,
                planner is not None,
                planning_policy is not None,
                context_strategy is not None,
                context_provider is not None,
                context_provider_factory is not None,
                conversation_compactor is not None,
                conversation_compactor_factory is not None,
                execution_state_factory is not None,
                tool_catalog is not None,
                task_admission_evaluator is not None,
                long_task_dispatcher is not None,
                runtime_limits != RuntimeLimits(),
                recovery_policy != RecoveryPolicy(),
            )):
                raise ValueError(
                    "AgentPreset cannot be mixed with Agent composition arguments"
                )
            execution_profile = preset.execution_profile
            context_provider = preset.context_provider
            context_provider_factory = preset.context_provider_factory
            conversation_compactor = preset.conversation_compactor
            conversation_compactor_factory = preset.conversation_compactor_factory
            execution_state_factory = preset.execution_state_factory
            tool_catalog = preset.tool_catalog
            runtime_limits = preset.runtime_limits
            recovery_policy = preset.recovery_policy
        if not isinstance(delegation_policy, DelegationPolicy):
            raise TypeError("delegation_policy must be a DelegationPolicy")
        self._preset = preset
        self._model_gateway = model_gateway
        self._output_repository = output_repository
        self._runtime_limits = runtime_limits
        self._recovery_policy = recovery_policy
        if (output_repository is None) != (output_publisher is None):
            raise ValueError(
                "canonical output repository and publisher must be configured together"
            )
        self._output_processor = output_processor or (
            AgentOutputProcessor(output_repository, output_publisher)
            if output_repository is not None and output_publisher is not None
            else None
        )
        self._repository = (
            CanonicalRunRepository(run_repository, self._output_processor)
            if self._output_processor is not None
            else run_repository
        )
        self._operations = operation_controller or (
            AgentOperationController(self._output_processor)
            if self._output_processor is not None
            else None
        )
        self._model_invocations = AgentModelInvocationManager(
            model_gateway,
            output_observer=self._output_processor,
            operation_controller=self._operations,
        )
        if context_provider is not None and context_provider_factory is not None:
            raise ValueError(
                "context provider and context provider factory are mutually exclusive"
            )
        if (
            conversation_compactor is not None
            and conversation_compactor_factory is not None
        ):
            raise ValueError(
                "conversation compactor and compactor factory are mutually exclusive"
            )
        self._context_provider_factory = context_provider_factory
        self._conversation_compactor_factory = conversation_compactor_factory
        if isinstance(conversation_compactor, ContextCompressionCoordinator):
            self._conversation_compactor = ContextCompressionCoordinator(
                conversation_compactor.hook,
                conversation_compactor.settings,
                operation_controller=self._operations,
            )
        else:
            self._conversation_compactor = conversation_compactor or (
                ContextCompressionCoordinator(
                    operation_controller=self._operations,
                )
            )
        if execution_profile is not None and any((
            planner is not None,
            planning_policy is not None,
            context_strategy is not None,
            task_admission_evaluator is not None,
            long_task_dispatcher is not None,
        )):
            raise ValueError(
                "execution profile cannot be mixed with orchestration arguments"
            )
        if execution_profile is None:
            if planner is not None and planning_policy is None:
                raise ValueError("planner requires an explicit planning policy")
            resolved_policy = planning_policy or ReactivePlanningPolicy()
            if context_strategy is None:
                context_strategy = ContextStrategy.SINGLE_PASS
            execution_profile = ExecutionProfile(
                planner=planner,
                planning_policy=resolved_policy,
                context_strategy=ContextStrategy(context_strategy),
                task_admission_evaluator=task_admission_evaluator,
                long_task_dispatcher=long_task_dispatcher,
            )
        self._execution_profile = execution_profile
        self._task_admission_evaluator = (
            execution_profile.task_admission_evaluator
        )
        self._long_task_dispatcher = execution_profile.long_task_dispatcher
        self._task_orchestration = (
            TaskOrchestrationCapability(
                self._task_admission_evaluator,
                self._long_task_dispatcher,
            )
            if self._task_admission_evaluator is not None
            or self._long_task_dispatcher is not None
            else None
        )
        self._planning_policy = execution_profile.planning_policy
        self._planning_enabled = execution_profile.planning_enabled
        self._context_strategy = execution_profile.context_strategy
        self._planner = execution_profile.planner
        if self._planning_enabled and self._planner is None:
            raise ValueError(
                "planned execution profile requires an explicit planner"
            )
        if not self._planning_enabled and self._planner is not None:
            raise ValueError(
                "reactive execution profile cannot configure an unused planner"
            )
        self._context_provider = context_provider or _EmptyContextProvider()
        self._execution_state_factory = (
            execution_state_factory or _DefaultExecutionStateFactory()
        )
        self._approval_gateway = approval_gateway or InMemoryApprovalGateway()
        self._delegation_coordinator: DelegationCoordinator | None = None
        self._dynamic_delegated_executor: DynamicDelegatedAgentExecutor | None = None
        base_tool_catalog = tool_catalog or InMemoryToolCatalog(())
        if delegation_repository is not None:
            if tool_idempotency_gateway is None:
                raise ValueError(
                    "delegated Agents require a tool idempotency gateway"
                )
            if self._output_processor is None or self._operations is None:
                raise ValueError(
                    "delegation requires canonical output infrastructure"
                )
            if delegated_agent_executor is None:
                self._dynamic_delegated_executor = DynamicDelegatedAgentExecutor(
                    tool_catalog=base_tool_catalog,
                    model_gateway=self._model_gateway,
                    model_manager=self._model_invocations,
                    output_processor=self._output_processor,
                    operation_controller=self._operations,
                    approval_gateway=self._approval_gateway,
                    tool_idempotency_gateway=tool_idempotency_gateway,
                    policy=delegation_policy,
                    context_provider=context_provider,
                    context_provider_factory=context_provider_factory,
                    conversation_compactor=conversation_compactor,
                    conversation_compactor_factory=(
                        conversation_compactor_factory
                    ),
                    execution_state_factory=execution_state_factory,
                    runtime_limits=runtime_limits,
                    recovery_policy=recovery_policy,
                    tool_execution_limits=tool_execution_limits,
                )
                delegated_agent_executor = self._dynamic_delegated_executor
            self._delegation_coordinator = DelegationCoordinator(
                repository=delegation_repository,
                executor=delegated_agent_executor,
                output_processor=self._output_processor,
                operation_controller=self._operations,
                max_parallel=delegation_policy.max_parallel,
            )
        elif delegated_agent_executor is not None:
            raise ValueError(
                "delegated Agent executor requires a delegation repository"
            )
        elif delegation_policy != DelegationPolicy():
            raise ValueError(
                "delegation policy requires a delegation repository"
            )
        if self._delegation_coordinator is not None:
            self._tool_catalog = _AugmentedToolCatalog(
                base_tool_catalog,
                (build_delegation_tool_registration(
                    self._delegation_coordinator,
                    delegation_policy,
                ),),
            )
        else:
            self._tool_catalog = base_tool_catalog
        self._registrations = tuple(self._tool_catalog.registrations())
        # This is deliberately not replaceable by a domain ``execute`` hook:
        # every registered handler crosses the same Core policy boundary.
        self._tool_executor = CoreToolExecutor(
            _CapturedToolCatalog(self._registrations),
            self._approval_gateway,
            tool_execution_limits,
            tool_idempotency_gateway,
            self._operations,
        )
        self._run_supervisor = None
        if output_repository is not None and output_publisher is not None:
            owner_id = (
                execution_owner_id
                or getattr(run_repository, "owner_id", None)
            )
            lease_duration_ms = (
                execution_lease_duration_ms
                or getattr(run_repository, "lease_duration_ms", None)
                or 30_000
            )
            self._run_supervisor = AgentRunSupervisor(
                output_repository=output_repository,
                output_publisher=output_publisher,
                execution_factory=self._supervised_execution,
                lease_store=execution_lease_store,
                owner_id=owner_id,
                lease_duration_ms=lease_duration_ms,
            )

    async def resolve_approval(
        self,
        run_id: RunId,
        approval_id: str,
        decision: ApprovalDecision,
    ) -> ApprovalStatus | None:
        return await self._approval_gateway.resolve(
            run_id,
            approval_id,
            ApprovalDecision(decision),
        )

    async def cancel_pending_approvals(self, run_id: RunId) -> int:
        return await self._approval_gateway.cancel_pending(run_id)

    async def close(self) -> None:
        if self._run_supervisor is not None:
            await self._run_supervisor.close()
        if self._delegation_coordinator is not None:
            await self._delegation_coordinator.close()

    async def submit(
        self,
        request: AgentRunRequest,
        *,
        options: AgentCoreRunOptions | None = None,
    ) -> AgentRunHandle:
        if not isinstance(request, AgentRunRequest):
            raise TypeError("AgentCore.submit requires AgentRunRequest")
        if self._run_supervisor is None:
            raise ContractViolationError(
                "AgentCore.submit requires canonical output infrastructure"
            )
        resolved_options = options or AgentCoreRunOptions()
        if self._preset is not None:
            resolved_options = await self._restore_continuation_preset(
                resolved_options
            )
            request = self._preset.apply(request)
            snapshot = self._preset.snapshot(request)
            persisted = resolved_options.agent_preset_snapshot
            if persisted is not None and persisted != snapshot:
                raise ContractViolationError(
                    "Run options carry a different AgentPreset snapshot"
                )
            resolved_options = replace(
                resolved_options,
                agent_preset_snapshot=snapshot,
            )
        elif resolved_options.agent_preset_snapshot is not None:
            raise ContractViolationError(
                "AgentPreset snapshot cannot be used without a configured Preset"
            )
        return await self._run_supervisor.submit(
            request,
            options=resolved_options,
        )

    async def _restore_continuation_preset(
        self,
        options: AgentCoreRunOptions,
    ) -> AgentCoreRunOptions:
        continuation = options.durable_continuation
        if continuation is None:
            return options
        source = continuation.source
        stored = source.agent_preset_snapshot
        if not stored:
            if self._output_repository is None:
                raise ContractViolationError(
                    "durable continuation cannot load its AgentPreset snapshot"
                )
            events = await self._output_repository.list_events(
                source.run_id,
                after_sequence=0,
                limit=1,
            )
            stored = (
                events[0].payload.get("agentPreset", {})
                if events
                else {}
            )
        if not stored:
            raise ContractViolationError(
                "durable continuation source has no AgentPreset snapshot"
            )
        snapshot = AgentPresetSnapshot.from_mapping(stored)
        if (
            options.agent_preset_snapshot is not None
            and options.agent_preset_snapshot != snapshot
        ):
            raise ContractViolationError(
                "durable continuation selected a different AgentPreset snapshot"
            )
        restored = replace(
            source,
            agent_preset_snapshot=snapshot.to_mapping(),
        )
        return replace(
            options,
            agent_preset_snapshot=snapshot,
            durable_continuation=replace(
                continuation,
                source=restored,
            ),
        )

    def _supervised_execution(
        self,
        request: AgentRunRequest,
        options: object | None,
        signal: asyncio.Event,
    ) -> AsyncIterator[AgentEvent | AgentRunResult]:
        if options is not None and not isinstance(options, AgentCoreRunOptions):
            raise TypeError("supervised execution requires AgentCoreRunOptions")
        return self._execute_run(
            request,
            options=options,
            signal=signal,
        )

    async def _execute_run(
        self,
        request: AgentRunRequest,
        *,
        options: AgentCoreRunOptions | None = None,
        signal: CancellationSignal | None = None,
    ) -> AsyncIterator[AgentEvent | AgentRunResult]:
        options = options or AgentCoreRunOptions()
        output_limit = options.output_limit or resolve_invocation_output_limit(
            request.model.capability_snapshot,
            request.model.options.get("max_tokens"),
        )
        selected_context_window = (
            request.context_window or options.default_context_window_tokens
        )
        if output_limit.max_tokens >= selected_context_window:
            raise UnsupportedModelFeatureError(
                "model output limit leaves no room for provider input",
                code="model_context_capacity_incompatible",
                retryable=False,
            )
        sink = _BufferedEventSink(self._output_processor)
        controller = AgentRunController(
            repository=self._repository,
            event_sink=sink,
        )
        runtime_stream = None
        delegation_bound = False
        compaction_source_request = request
        pre_planning_compaction: dict[str, Any] = {
            "outcome": "not_configured",
        }
        try:
            await controller.start(
                RunCreateParams(
                    session_id=request.session_id,
                    prompt=request.latest_user_text(),
                    mode=request.mode,
                    turn_id=options.turn_id,
                    provenance=options.provenance,
                    binding=options.binding,
                    agent_preset_snapshot=(
                        options.agent_preset_snapshot.to_mapping()
                        if options.agent_preset_snapshot is not None
                        else {}
                    ),
                )
            )
            model_tasks = AgentModelTaskRunner(
                self._model_invocations,
                ModelInvocationContext(
                    run_id=controller.run_id,
                    turn_id=options.turn_id,
                ),
            )
            context_provider = (
                self._context_provider_factory(model_tasks)
                if self._context_provider_factory is not None
                else self._context_provider
            )
            conversation_compactor = (
                self._conversation_compactor_factory(model_tasks)
                if self._conversation_compactor_factory is not None
                else self._conversation_compactor
            )
            if (
                self._conversation_compactor_factory is not None
                and isinstance(
                    conversation_compactor,
                    ContextCompressionCoordinator,
                )
            ):
                conversation_compactor = ContextCompressionCoordinator(
                    conversation_compactor.hook,
                    conversation_compactor.settings,
                    operation_controller=self._operations,
                )
            if not isinstance(context_provider, ContextProvider):
                raise TypeError("context provider factory returned an invalid port")
            if (
                conversation_compactor is not None
                and not isinstance(conversation_compactor, ConversationCompactor)
            ):
                raise TypeError(
                    "conversation compactor factory returned an invalid port"
                )
            context_capability = ContextCapability(
                self._context_strategy,
                context_provider,
            )
            for event in sink.drain():
                yield event

            compaction_trace = request.metadata.get("conversationCompaction")
            if isinstance(compaction_trace, Mapping):
                await controller.record_trace(TraceRecord(
                    stage="conversation_compaction",
                    outcome=str(
                        compaction_trace.get("outcome") or "unknown"
                    ),
                    details={
                        str(key): value
                        for key, value in compaction_trace.items()
                        if key != "outcome"
                    },
                ))

            if _is_canceled(signal):
                await controller.cancel("request_canceled")
                for event in sink.drain():
                    yield event
                yield _run_result(controller)
                return

            if (continuation := options.durable_continuation) is not None:
                task_orchestration = self._task_orchestration or (
                    TaskOrchestrationCapability(None, None)
                )
                async for event in task_orchestration.continue_durable(
                    controller,
                    request,
                    continuation,
                    sink,
                    signal,
                ):
                    yield event
                yield _run_result(controller)
                return

            # Reserve against every enabled runtime schema. Planned profiles
            # may narrow the visible set after compiling their ExecutionPlan.
            reservation_started = perf_counter()
            try:
                context_claims = await await_with_cancellation(
                    resolve_context_budget_claims(
                        context_provider,
                        request,
                        options.context_claims,
                        signal,
                    ),
                    signal,
                )
                registrations, enabled_names = _effective_registrations(
                    self._tool_catalog,
                    self._registrations,
                    request,
                    model_supports_tools=options.model_supports_tools,
                )
                display_locale = str(
                    request.metadata.get("locale") or "zh-CN"
                )
                reserved_schemas = tuple(
                    model_visible_tool_schema(
                        registration.schema,
                        display_locale,
                    )
                    for registration in registrations
                    if registration.schema.name in enabled_names
                )
                reserved_budget = allocate_context_budget(
                    window_tokens=(
                        request.context_window
                        or options.default_context_window_tokens
                    ),
                    output_reserve_tokens=output_limit.max_tokens,
                    tools=reserved_schemas,
                    claims=context_claims,
                    safety_reserve_tokens=options.safety_reserve_tokens,
                    runtime_reserve_tokens=options.runtime_reserve_tokens,
                    minimum_message_tokens=options.minimum_message_tokens,
                )
                compactor = conversation_compactor
                if compactor is not None and context_capability.uses_staged_context:
                    compaction_started = asyncio.Event()
                    compaction_started_payload: dict[str, Any] = {}

                    async def notify_compaction_started(
                        payload: Mapping[str, Any],
                    ) -> None:
                        compaction_started_payload.update(dict(payload))
                        compaction_started.set()

                    compaction_task = asyncio.create_task(
                        await_with_cancellation(
                            compactor.prepare(
                                compaction_source_request,
                                signal,
                                on_compaction_started=(
                                    notify_compaction_started
                                ),
                                budget=ContextCompactionBudget(
                                    phase=(
                                        ContextCompactionPhase.PRE_PLANNING
                                    ),
                                    provider_input_tokens=(
                                        reserved_budget.provider_input_tokens
                                    ),
                                    context_tokens=sum(
                                        reserved_budget.context_allocations.values()
                                    ),
                                    context_tokens_are_resolved=False,
                                    output_reserve_tokens=(
                                        reserved_budget.output_reserve_tokens
                                    ),
                                ),
                                operation_scope=OperationScope(
                                    run_id=controller.run_id,
                                ),
                            ),
                            signal,
                        )
                    )
                    compaction_started_wait = asyncio.create_task(
                        compaction_started.wait()
                    )
                    compaction_result: ConversationCompactionResult | None = None
                    try:
                        await asyncio.wait(
                            (compaction_task, compaction_started_wait),
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if compaction_started.is_set():
                            started_event = AgentEvent(
                                type="conversation.compaction.started",
                                run_id=controller.run_id,
                                payload={
                                    "status": "running",
                                    "phase": "pre_planning",
                                    "postPlanning": False,
                                    **compaction_started_payload,
                                },
                            )
                            await self._publish_runtime_event(
                                controller.run_id,
                                started_event,
                            )
                            yield started_event
                        candidate = await compaction_task
                        if not isinstance(
                            candidate,
                            ConversationCompactionResult,
                        ):
                            raise ContractViolationError(
                                "conversation compactor returned an invalid result"
                            )
                        compaction_result = candidate
                    except OperationCanceled:
                        raise
                    except Exception as error:
                        pre_planning_compaction = {
                            "outcome": "failed_open",
                            "phase": "pre_planning",
                        }
                        await _record_safe_exception(
                            controller,
                            stage="conversation_compaction",
                            outcome="failed_open",
                            error=error,
                            safe_details={"phase": "pre_planning"},
                        )
                    finally:
                        if not compaction_started_wait.done():
                            compaction_started_wait.cancel()
                        if not compaction_task.done():
                            compaction_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await compaction_started_wait
                        with suppress(asyncio.CancelledError, Exception):
                            await compaction_task

                    if compaction_result is not None:
                        request = compaction_result.request
                        pre_planning_compaction = {
                            **thaw_json_mapping(
                                compaction_result.diagnostics
                            ),
                            "outcome": compaction_result.outcome,
                            "phase": "pre_planning",
                            "compactedTurnCount": (
                                compaction_result.compacted_turn_count
                            ),
                            "retainedRawTurnCount": (
                                compaction_result.retained_raw_turn_count
                            ),
                            "summaryVersion": (
                                compaction_result.compression_state_version
                            ),
                        }
                        await controller.record_trace(TraceRecord(
                            stage="conversation_compaction",
                            outcome=compaction_result.outcome,
                            details={
                                key: value
                                for key, value in pre_planning_compaction.items()
                                if key != "outcome"
                            },
                        ))
                    if compaction_started.is_set():
                        completed_event = AgentEvent(
                            type="conversation.compaction.completed",
                            run_id=controller.run_id,
                            payload={
                                "status": (
                                    "completed"
                                    if compaction_result is not None
                                    and compaction_result.outcome.startswith(
                                        "compacted"
                                    )
                                    else "failed"
                                ),
                                "phase": "pre_planning",
                                "postPlanning": False,
                                "outcome": (
                                    compaction_result.outcome
                                    if compaction_result is not None
                                    else "failed_open"
                                ),
                                "compactedTurnCount": (
                                    compaction_result.compacted_turn_count
                                    if compaction_result is not None
                                    else 0
                                ),
                                "retainedRawTurnCount": (
                                    compaction_result.retained_raw_turn_count
                                    if compaction_result is not None
                                    else 0
                                ),
                                "summaryVersion": (
                                    compaction_result.compression_state_version
                                    if compaction_result is not None
                                    else None
                                ),
                            },
                        )
                        await self._publish_runtime_event(
                            controller.run_id,
                            completed_event,
                        )
                        yield completed_event
                planning_bundle = await await_with_cancellation(
                    context_capability.build_initial(
                        request,
                        reserved_budget,
                        signal,
                    ),
                    signal,
                )
                _validate_context_allocations(planning_bundle, reserved_budget)
                bundle = planning_bundle
            except OperationCanceled:
                await controller.cancel("request_canceled")
                for event in sink.drain():
                    yield event
                yield _run_result(controller)
                return
            except ContextOverflowError as error:
                await _record_safe_exception(
                    controller,
                    stage="context_reservation",
                    outcome="overflow",
                    error=error,
                    started=reservation_started,
                )
                await controller.fail("context_overflow_initial")
                for event in sink.drain():
                    yield event
                yield _run_result(controller)
                return
            except Exception as error:
                await _record_safe_exception(
                    controller,
                    stage="context_reservation",
                    outcome="failed",
                    error=error,
                    started=reservation_started,
                )
                await controller.fail("context_setup_failed")
                for event in sink.drain():
                    yield event
                yield _run_result(controller)
                return

            if self._planning_enabled:
                planning_result = await PlanningCapability(
                    planner=self._planner,
                    policy=self._planning_policy,
                    runtime_limits=self._runtime_limits,
                    task_orchestration=self._task_orchestration,
                ).execute(
                    request=request,
                    planning_bundle=planning_bundle,
                    registrations=registrations,
                    enabled_names=enabled_names,
                    display_locale=display_locale,
                    model_supports_tools=options.model_supports_tools,
                    controller=controller,
                    signal=signal,
                    turn_id=options.turn_id,
                )
            else:
                planning_result = PlanningPhaseResult.reactive()
                await controller.record_trace(TraceRecord(
                    stage="execution_profile",
                    outcome="reactive",
                    details={"toolCount": len(enabled_names)},
                ))

            if planning_result.terminal:
                for event in sink.drain():
                    yield event
                yield _run_result(controller)
                return

            plan = planning_result.execution_plan
            capabilities = planning_result.capabilities
            admission = planning_result.admission

            if (
                admission is not None
                and admission.mode is not ExecutionMode.INLINE
                and plan is not None
            ):
                if self._task_orchestration is None:
                    raise ContractViolationError(
                        "task admission requires task orchestration"
                    )
                async for admitted_event in (
                    self._task_orchestration.complete_admission(
                        controller=controller,
                        request=request,
                        plan=plan,
                        admission=admission,
                        sink=sink,
                        signal=signal,
                    )
                ):
                    yield admitted_event
                yield _run_result(controller)
                return

            for event in sink.drain():
                yield event

            planning_hook = planning_result.dynamic_planning
            selected_names = (
                runtime_tool_names_for_planning_names(
                    registrations,
                    enabled_names,
                    effective_planning_tool_names(capabilities),
                )
                if planning_hook is not None
                else _planned_tool_names(plan)
                if plan is not None
                else enabled_names
            )
            selected_registrations = tuple(
                registration
                for registration in registrations
                if registration.schema.name in selected_names
            )
            schemas = tuple(
                model_visible_tool_schema(
                    registration.schema,
                    display_locale,
                )
                for registration in selected_registrations
            )

            setup_started = perf_counter()
            try:
                task_context: TaskContextRequest | None = None
                task_context_claims: tuple[ContextBudgetClaim, ...] = ()
                if (
                    context_capability.staged_provider is not None
                    and plan is not None
                    and plan.task_spec is not None
                ):
                    task_context = _compile_task_context_request(
                        plan,
                        selected_registrations,
                        run_id=controller.run_id,
                    )
                    if isinstance(
                        context_capability.staged_provider,
                        TaskContextDemandProvider,
                    ):
                        task_context_claims = await await_with_cancellation(
                            resolve_task_context_budget_claims(
                                context_capability.staged_provider,
                                request,
                                task_context,
                                signal,
                            ),
                            signal,
                        )
                effective_context_claims = _merge_context_claims(
                    context_claims,
                    task_context_claims,
                )
                budget = allocate_context_budget(
                    window_tokens=(
                        request.context_window
                        or options.default_context_window_tokens
                    ),
                    output_reserve_tokens=output_limit.max_tokens,
                    tools=schemas,
                    claims=effective_context_claims,
                    safety_reserve_tokens=options.safety_reserve_tokens,
                    runtime_reserve_tokens=options.runtime_reserve_tokens,
                    minimum_message_tokens=options.minimum_message_tokens,
                )
                context_mode = "single_pass"
                if context_capability.staged_provider is not None:
                    retrieval_started = perf_counter()
                    bundle, context_mode = await await_with_cancellation(
                        context_capability.build_execution(
                            request,
                            budget,
                            planning_bundle,
                            task_context,
                            signal,
                        ),
                        signal,
                    )
                    await controller.record_trace(TraceRecord(
                        stage="context_retrieval",
                        outcome=context_mode,
                        details={
                            "postPlanning": context_mode == "task_spec",
                            "plannedToolCount": len(
                                _planned_tool_names(plan)
                                if plan is not None
                                else ()
                            ),
                            "requiredContextBlockCount": (
                                len(task_context.required_context_blocks)
                                if context_mode == "task_spec"
                                else 0
                            ),
                            "evidenceKindCount": (
                                len(task_context.evidence_kinds)
                                if context_mode == "task_spec"
                                else 0
                            ),
                        },
                        duration_ms=_duration_ms(retrieval_started),
                    ))
                _validate_context_allocations(bundle, budget)
                post_planning_diagnostics: dict[str, Any] = {
                    "outcome": "not_configured",
                    "plannedStepCount": len(plan.steps) if plan is not None else 0,
                    "plannedToolCount": (
                        sum(
                            step.executor is StepExecutor.TOOL
                            for step in plan.steps
                        )
                        if plan is not None
                        else 0
                    ),
                    "selectedToolCount": len(selected_names),
                }
                compactor = conversation_compactor
                if compactor is not None and context_capability.uses_staged_context:
                    resolved_context_tokens = estimate_agent_messages_tokens(
                        _assemble_messages((), bundle.blocks, plan)
                    )
                    optimization_started = asyncio.Event()
                    optimization_started_payload: dict[str, Any] = {}

                    async def notify_optimization_started(
                        payload: Mapping[str, Any],
                    ) -> None:
                        optimization_started_payload.update(dict(payload))
                        optimization_started.set()

                    async def run_context_optimization(
                    ) -> ConversationCompactionResult:
                        source_request = replace(
                            compaction_source_request,
                            metadata={
                                **compaction_source_request.metadata,
                                **request.metadata,
                            },
                        )
                        compacted = await await_with_cancellation(
                            compactor.prepare(
                                source_request,
                                signal,
                                budget=ContextCompactionBudget(
                                    phase=ContextCompactionPhase.POST_PLANNING,
                                    provider_input_tokens=(
                                        budget.provider_input_tokens
                                    ),
                                    context_tokens=resolved_context_tokens,
                                    context_tokens_are_resolved=True,
                                    output_reserve_tokens=(
                                        budget.output_reserve_tokens
                                    ),
                                    planned_step_count=(
                                        post_planning_diagnostics[
                                            "plannedStepCount"
                                        ]
                                    ),
                                    planned_tool_count=(
                                        post_planning_diagnostics[
                                            "plannedToolCount"
                                        ]
                                    ),
                                    selected_tool_count=len(selected_names),
                                ),
                                on_compaction_started=(
                                    notify_optimization_started
                                ),
                                operation_scope=OperationScope(
                                    run_id=controller.run_id,
                                ),
                            ),
                            signal,
                        )
                        if not isinstance(
                            compacted,
                            ConversationCompactionResult,
                        ):
                            raise ContractViolationError(
                                "conversation compactor returned an invalid "
                                "post-planning result"
                            )
                        return replace(
                            compacted,
                            request=replace(
                                compacted.request,
                                metadata={
                                    **request.metadata,
                                    **compacted.request.metadata,
                                },
                            ),
                        )

                    optimization_task = asyncio.create_task(
                        run_context_optimization()
                    )
                    optimization_started_wait = asyncio.create_task(
                        optimization_started.wait()
                    )
                    optimization_result: (
                        ConversationCompactionResult | None
                    ) = None
                    optimization_failed = False
                    try:
                        await asyncio.wait(
                            (optimization_task, optimization_started_wait),
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if optimization_started.is_set():
                            started_event = AgentEvent(
                                type="conversation.compaction.started",
                                run_id=controller.run_id,
                                payload={
                                    "status": "running",
                                    "phase": "post_planning",
                                    "postPlanning": True,
                                    **optimization_started_payload,
                                },
                            )
                            await self._publish_runtime_event(
                                controller.run_id,
                                started_event,
                            )
                            yield started_event
                        candidate = await optimization_task
                        if not isinstance(
                            candidate,
                            ConversationCompactionResult,
                        ):
                            raise ContractViolationError(
                                "conversation compactor returned an invalid "
                                "post-planning result"
                            )
                        optimization_result = candidate
                    except OperationCanceled:
                        raise
                    except Exception as error:
                        optimization_failed = True
                        await _record_safe_exception(
                            controller,
                            stage="post_planning_context_optimization",
                            outcome="failed_open",
                            error=error,
                        )
                    finally:
                        if not optimization_started_wait.done():
                            optimization_started_wait.cancel()
                        if not optimization_task.done():
                            optimization_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await optimization_started_wait
                        with suppress(asyncio.CancelledError, Exception):
                            await optimization_task

                    if optimization_result is not None:
                        request = optimization_result.request
                        post_planning_diagnostics = {
                            **post_planning_diagnostics,
                            **thaw_json_mapping(
                                optimization_result.diagnostics
                            ),
                            "outcome": optimization_result.outcome,
                            "postPlanning": True,
                            "resolvedContextTokens": resolved_context_tokens,
                            "compactedTurnCount": (
                                optimization_result.compacted_turn_count
                            ),
                            "retainedRawTurnCount": (
                                optimization_result.retained_raw_turn_count
                            ),
                            "summaryVersion": (
                                optimization_result.compression_state_version
                            ),
                        }
                        await controller.record_trace(TraceRecord(
                            stage="post_planning_context_optimization",
                            outcome=optimization_result.outcome,
                            details=post_planning_diagnostics,
                        ))
                    elif optimization_failed:
                        post_planning_diagnostics = {
                            **post_planning_diagnostics,
                            "outcome": "failed_open",
                            "postPlanning": True,
                            "resolvedContextTokens": resolved_context_tokens,
                        }

                    if optimization_started.is_set():
                        completed_event = AgentEvent(
                            type="conversation.compaction.completed",
                            run_id=controller.run_id,
                            payload={
                                "status": (
                                    "completed"
                                    if optimization_result is not None
                                    and optimization_result.outcome.startswith(
                                        "compacted"
                                    )
                                    else "failed"
                                ),
                                "phase": "post_planning",
                                "postPlanning": True,
                                "outcome": (
                                    optimization_result.outcome
                                    if optimization_result is not None
                                    else "failed_open"
                                ),
                                "compactedTurnCount": (
                                    optimization_result.compacted_turn_count
                                    if optimization_result is not None
                                    else 0
                                ),
                                "retainedRawTurnCount": (
                                    optimization_result.retained_raw_turn_count
                                    if optimization_result is not None
                                    else 0
                                ),
                                "summaryVersion": (
                                    optimization_result.compression_state_version
                                    if optimization_result is not None
                                    else None
                                ),
                            },
                        )
                        await self._publish_runtime_event(
                            controller.run_id,
                            completed_event,
                        )
                        yield completed_event
                prepared_request = replace(
                    request,
                    messages=_assemble_messages(request.messages, bundle.blocks, plan),
                    context_window=budget.window_tokens,
                )
                if self._dynamic_delegated_executor is not None:
                    self._dynamic_delegated_executor.bind_run(
                        controller.run_id,
                        request,
                        prepared_request.messages,
                    )
                    delegation_bound = True
                estimated_input_tokens = estimate_agent_messages_tokens(
                    prepared_request.messages
                )
                dropped_message_count = 0
                overflow_tokens = max(
                    0,
                    estimated_input_tokens - budget.provider_input_tokens,
                )
                if overflow_tokens:
                    raise ContextOverflowError(
                        "required messages exceed the initial provider input budget",
                        reason_code="required_messages_exceed_provider_budget",
                        details={
                            "providerInputTokens": budget.provider_input_tokens,
                            "estimatedInputTokens": estimated_input_tokens,
                            "overflowTokens": overflow_tokens,
                        },
                    )
                state = self._execution_state_factory.create(request)
                if not isinstance(state, ExecutionState):
                    raise ContractViolationError(
                        "execution state factory must return ExecutionState"
                    )
                state.run_id = controller.run_id
                await controller.record_trace(TraceRecord(
                    stage="context_budget",
                    outcome="within_budget",
                    details={
                        "windowTokens": budget.window_tokens,
                        "providerInputTokens": budget.provider_input_tokens,
                        "toolSchemaTokens": budget.tool_schema_tokens,
                        "reservedToolSchemaTokens": (
                            reserved_budget.tool_schema_tokens
                        ),
                        "contextMode": context_mode,
                        "contextDemands": _context_demand_diagnostics(
                            effective_context_claims,
                            budget,
                        ),
                        "droppedMessages": dropped_message_count,
                        "prePlanningCompaction": pre_planning_compaction,
                        "postPlanningOptimization": post_planning_diagnostics,
                    },
                    duration_ms=_duration_ms(setup_started),
                ))
                budget_event = AgentEvent(
                    type=CoreEventType.CONTEXT_BUDGETED,
                    run_id=controller.run_id,
                    payload={
                        "windowTokens": budget.window_tokens,
                        "providerInputTokens": budget.provider_input_tokens,
                        "estimatedInputTokens": estimated_input_tokens,
                        "outputReserveTokens": budget.output_reserve_tokens,
                        "outputLimit": output_limit.to_mapping(),
                        "runtimeReserveTokens": budget.runtime_reserve_tokens,
                        "safetyReserveTokens": budget.safety_reserve_tokens,
                        "toolSchemaTokens": budget.tool_schema_tokens,
                        "reservedToolSchemaTokens": (
                            reserved_budget.tool_schema_tokens
                        ),
                        "contextMode": context_mode,
                        "droppedMessages": dropped_message_count,
                        "projectedTotalTokens": (
                            estimated_input_tokens
                            + budget.tool_schema_tokens
                            + budget.output_reserve_tokens
                            + budget.runtime_reserve_tokens
                            + budget.safety_reserve_tokens
                        ),
                        "overflowTokens": 0,
                        "contextAllocations": thaw_json_mapping(
                            budget.context_allocations
                        ),
                        "diagnostics": {
                            **thaw_json_mapping(bundle.diagnostics),
                            "contextDemands": _context_demand_diagnostics(
                                effective_context_claims,
                                budget,
                            ),
                            "prePlanningCompaction": (
                                pre_planning_compaction
                            ),
                            "postPlanningOptimization": (
                                post_planning_diagnostics
                            ),
                        },
                    },
                )
                await self._publish_runtime_event(controller.run_id, budget_event)
                yield budget_event
            except OperationCanceled:
                await controller.cancel("request_canceled")
                for event in sink.drain():
                    yield event
                yield _run_result(controller)
                return
            except ContextOverflowError as error:
                await _record_safe_exception(
                    controller,
                    stage="context_budget",
                    outcome="overflow",
                    error=error,
                    started=setup_started,
                )
                await controller.fail("context_overflow_initial")
                for event in sink.drain():
                    yield event
                yield _run_result(controller)
                return
            except Exception as error:
                await _record_safe_exception(
                    controller,
                    stage="context_budget",
                    outcome="failed",
                    error=error,
                    started=setup_started,
                )
                await controller.fail("context_setup_failed")
                for event in sink.drain():
                    yield event
                yield _run_result(controller)
                return

            host_planned_arguments = {
                registration.schema.name: thaw_json_mapping(
                    registration.host_planned_arguments
                )
                for registration in registrations
                if registration.host_planned_arguments is not None
            }
            runtime_model_gateway: ModelGateway = self._model_gateway
            runtime_model_manager = self._model_invocations
            if host_planned_arguments:
                runtime_model_gateway = HostPlannedToolGateway(
                    self._model_gateway,
                    host_planned_arguments,
                )
                runtime_model_manager = AgentModelInvocationManager(
                    runtime_model_gateway,
                    output_observer=self._output_processor,
                    operation_controller=self._operations,
                )
            runtime = AgentRuntime(
                model_gateway=runtime_model_gateway,
                tool_execution_gateway=self._tool_executor,
                observer=controller,
                context_compressor=conversation_compactor,
                limits=self._runtime_limits,
                recovery_policy=self._recovery_policy,
                operation_controller=self._operations,
                output_observer=self._output_processor,
                model_manager=runtime_model_manager,
            )
            runtime_result: AgentRuntimeResult | None = None
            try:
                runtime_stream = runtime.run(
                    prepared_request,
                    tools=schemas,
                    response_constraints=options.response_constraints,
                    response_validators=options.response_validators,
                    response_judges=(
                        *options.response_judges,
                        *(
                            AgentModelResponseJudge(
                                model_tasks=model_tasks,
                                model_request=request.model,
                                policy=policy,
                            )
                            for policy in options.response_judge_policies
                        ),
                    ),
                    response_transaction_mode=(
                        options.resolved_response_transaction_policy.mode
                    ),
                    execution_state=state,
                    run_id=controller.run_id,
                    turn_id=options.turn_id,
                    context_budget=budget,
                    output_limit=output_limit,
                    scope_tools_to_observer=(plan is not None),
                    force_tool_choice=bool(
                        plan is not None
                        and selected_names
                        and options.force_planned_tool_choice
                    ),
                    reasoning_mode=options.reasoning_mode,
                    require_tool_call=(
                        options.require_tool_call
                        if options.require_tool_call is not None
                        else bool(plan is not None and selected_names)
                    ),
                    tools_executable=True,
                    planning_hook=planning_hook,
                    tool_context_contracts={
                        registration.schema.name: registration.context_contract
                        for registration in registrations
                    },
                    stage_context_projection_enabled=bool(
                        plan is not None and plan.task_spec is not None
                    ),
                    signal=signal,
                )
                async with aclosing(runtime_stream) as updates:
                    async for update in updates:
                        for event in sink.drain():
                            yield event
                        if isinstance(update, AgentEvent):
                            event = _bind_event_to_run(update, controller.run_id)
                            await self._publish_runtime_event(
                                controller.run_id,
                                event,
                            )
                            yield event
                        else:
                            runtime_result = update
                runtime_stream = None
            except OperationCanceled:
                runtime_result = AgentRuntimeResult(
                    run_id=controller.run_id,
                    outcome=RuntimeOutcome.CANCELED,
                    final_response="",
                    model=request.model.model,
                    round_count=0,
                    error_code="request_canceled",
                )
            except Exception as error:
                await _record_safe_exception(
                    controller,
                    stage="runtime",
                    outcome="exception",
                    error=error,
                )
                runtime_result = AgentRuntimeResult(
                    run_id=controller.run_id,
                    outcome=RuntimeOutcome.FAILED,
                    final_response="",
                    model=request.model.model,
                    round_count=0,
                    error_code="runtime_exception",
                )

            for event in sink.drain():
                yield event
            if runtime_result is None:
                await controller.fail("runtime_returned_no_result")
            elif runtime_result.outcome is RuntimeOutcome.COMPLETED:
                try:
                    final_response = runtime_result.final_response
                    validated_result: str | None = None
                    transaction_policy = (
                        options.resolved_response_transaction_policy
                    )
                    if (
                        transaction_policy.mode
                        is ResponseTransactionMode.VALIDATED_RESULT
                    ):
                        validated_result = final_response
                        final_response = ""
                        if (
                            transaction_policy.public_presentation
                            is PublicPresentationMode.MODEL_LIVE
                        ):
                            transaction = AgentResponseTransaction(
                                AgentModelInvocationManager(
                                    self._model_gateway,
                                    output_observer=self._output_processor,
                                    operation_controller=self._operations,
                                ),
                                policy=transaction_policy,
                                facts_provider=(
                                    options.committed_result_facts_provider
                                ),
                                operation_controller=self._operations,
                            )
                            final_response = await transaction.present(
                                AgentRunResult(
                                    run_id=controller.run_id,
                                    status=RunStatus.DONE,
                                    final_response=(
                                        runtime_result.final_response
                                    ),
                                    model=runtime_result.model,
                                ),
                                request=request.model,
                                context=ModelInvocationContext(
                                    run_id=controller.run_id,
                                    turn_id=options.turn_id,
                                ),
                                signal=signal,
                            )
                    if validated_result is None:
                        await controller.complete(final_response)
                    else:
                        await controller.complete_validated_result(
                            validated_result,
                            final_response=final_response,
                        )
                except Exception as error:
                    # A host projector participates in the terminal repository
                    # transaction. Rejection means the result was not durably
                    # committed, so the Run must fail instead of leaking a
                    # false completed state.
                    await _record_safe_exception(
                        controller,
                        stage="completion_projection",
                        outcome="failed",
                        error=error,
                    )
                    code = str(
                        getattr(error, "code", "")
                        or "completion_projection_failed"
                    ).strip()
                    await controller.fail(code)
            elif runtime_result.outcome is RuntimeOutcome.CANCELED:
                await controller.cancel(
                    runtime_result.error_code or "request_canceled"
                )
            else:
                await controller.fail(
                    runtime_result.error_code or "runtime_failed"
                )
            for event in sink.drain():
                yield event
            yield _run_result(
                controller,
                model=(runtime_result.model if runtime_result is not None else request.model.model),
            )
        finally:
            if runtime_stream is not None:
                with suppress(asyncio.CancelledError, Exception):
                    await runtime_stream.aclose()
            run_id = controller.run_id
            if run_id is not None:
                with suppress(Exception):
                    await self._approval_gateway.cancel_pending(run_id)
                if delegation_bound and self._dynamic_delegated_executor is not None:
                    self._dynamic_delegated_executor.release_run(run_id)
            snapshot = controller.snapshot
            if snapshot is not None and not snapshot.terminal:
                # This private execution iterator is owned by Supervisor. If
                # it is stopped before a terminal result, the execution owner
                # itself is shutting down; subscriber disposal never reaches
                # this path.
                await controller.cancel("execution_owner_stopped")

    async def _publish_runtime_event(
        self,
        run_id: RunId | None,
        event: AgentEvent,
    ) -> None:
        if run_id is None:
            raise ContractViolationError("runtime event requires a run id")
        if event.type in CONTROLLER_OWNED_RUN_EVENT_TYPES:
            raise ContractViolationError(
                "runtime, tool, approval and domain events cannot use "
                f"controller-owned event type {event.type!r}"
            )
        # Runtime events are structured lifecycle, context, tool, approval or
        # domain facts. Provider text is owned exclusively by OutputProcessor.
        await self._repository.append_event(run_id, event)
        if self._output_processor is not None:
            await self._output_processor.accept_runtime_event(
                _runtime_output_event(event, run_id)
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


class _DefaultExecutionStateFactory:
    def create(self, request: AgentRunRequest) -> ExecutionState:
        del request
        return ExecutionState()


class _CapturedToolCatalog:
    """Hold the exact registration snapshot shared by Engine and Executor."""

    def __init__(self, registrations: Sequence[ToolRegistration]) -> None:
        self._registrations = tuple(registrations)
        self._names = frozenset(
            registration.schema.name for registration in self._registrations
        )

    def registrations(self) -> tuple[ToolRegistration, ...]:
        return self._registrations

    def enabled_names(self, request: AgentRunRequest) -> frozenset[str]:
        del request
        return self._names


async def _record_safe_exception(
    controller: AgentRunController,
    *,
    stage: str,
    outcome: str,
    error: Exception,
    started: float | None = None,
    safe_details: Mapping[str, Any] | None = None,
) -> None:
    overflow_details = (
        {
            "reasonCode": error.reason_code,
            **error.details,
        }
        if isinstance(error, ContextOverflowError)
        else {}
    )
    await controller.record_trace(TraceRecord(
        stage=stage,
        outcome=outcome,
        details={
            "errorType": type(error).__name__,
            **overflow_details,
            **(safe_details or {}),
        },
        duration_ms=(_duration_ms(started) if started is not None else None),
    ))


def _run_result(
    controller: AgentRunController,
    *,
    model: str | None = None,
) -> AgentRunResult:
    snapshot = controller.snapshot
    if snapshot is None or not snapshot.terminal:
        raise RuntimeError("agent run has no terminal snapshot")
    return AgentRunResult(
        run_id=snapshot.run_id,
        status=snapshot.status,
        final_response=snapshot.final_response,
        error=snapshot.error,
        model=model,
    )
