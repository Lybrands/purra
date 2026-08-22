"""The single high-level orchestration entry for a complete PurrA run."""

from __future__ import annotations

import asyncio
from contextlib import aclosing, suppress
from dataclasses import dataclass, replace
from time import perf_counter
from typing import Any, AsyncIterator, Callable, Iterable, Mapping, Sequence

from purra.agent_presets import (
    AgentPreset,
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
from purra.context_orchestration.compaction import (
    ContextCompressionCoordinator,
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
from purra.engine.compaction_phase import (
    compact_after_planning,
    compact_before_planning,
)
from purra.engine.canonical_sink import (
    BufferedEventSink as _BufferedEventSink,
    runtime_output_event as _runtime_output_event,
)
from purra.engine.durable_execution import (
    bind_event_to_run as _bind_event_to_run,
)
from purra.engine.options import AgentCoreRunOptions, restore_continuation_preset
from purra.engine.planning_phase import PlanningCapability, PlanningPhaseResult
from purra.engine.task_orchestration import TaskOrchestrationCapability
from purra.delegation import DelegatedAgentExecutor, DelegationCoordinator, DelegationPolicy
from purra.engine.delegation_assembly import assemble_delegation
from purra.delegation.dynamic_executor import DynamicDelegatedAgentExecutor
from purra.execution import AgentRunHandle, AgentRunSupervisor
from purra.engine.planning_validation import (
    effective_registrations as _effective_registrations,
)
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
from purra.operations import AgentOperationController


@dataclass(frozen=True, slots=True)
class _PreparedRuntimePhase:
    request: AgentRunRequest
    prepared_request: AgentRunRequest
    budget: ContextBudget
    state: ExecutionState
    events: tuple[AgentEvent, ...]
    delegation_bound: bool


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
        context_provider_factory: Callable[[AgentModelTaskRunner], ContextProvider] | None = None,
        conversation_compactor: ConversationCompactor | None = None,
        conversation_compactor_factory: Callable[[AgentModelTaskRunner], ConversationCompactor] | None = None,
        execution_state_factory: ExecutionStateFactory | None = None,
        tool_catalog: ToolCatalog | None = None,
        delegation_policy: DelegationPolicy | None = None,
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
                delegation_policy is not None,
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
            delegation_policy = preset.delegation_policy
            runtime_limits = preset.runtime_limits
            recovery_policy = preset.recovery_policy
        if delegation_policy is not None and not isinstance(
            delegation_policy,
            DelegationPolicy,
        ):
            raise TypeError(
                "delegation_policy must be a DelegationPolicy or None"
            )
        self._preset = preset
        self._model_gateway = model_gateway
        self._output_repository = output_repository
        self._runtime_limits = runtime_limits
        self._recovery_policy = recovery_policy
        (
            self._output_processor,
            self._repository,
            self._operations,
            self._model_invocations,
        ) = _configure_output_runtime(
            model_gateway,
            run_repository,
            output_processor=output_processor,
            output_repository=output_repository,
            output_publisher=output_publisher,
            operation_controller=operation_controller,
        )
        self._context_provider_factory = context_provider_factory
        self._conversation_compactor_factory = conversation_compactor_factory
        self._conversation_compactor = _resolve_conversation_compactor(
            context_provider,
            context_provider_factory,
            conversation_compactor,
            conversation_compactor_factory,
            self._operations,
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
        base_tool_catalog = tool_catalog or InMemoryToolCatalog(())
        (
            self._delegation_coordinator,
            self._dynamic_delegated_executor,
            self._tool_catalog,
        ) = assemble_delegation(
            base_tool_catalog=base_tool_catalog,
            policy=delegation_policy,
            repository=delegation_repository,
            executor=delegated_agent_executor,
            idempotency=tool_idempotency_gateway,
            output=self._output_processor,
            operations=self._operations,
            model_gateway=self._model_gateway,
            model_manager=self._model_invocations,
            approval=self._approval_gateway,
            context_provider=context_provider,
            context_provider_factory=context_provider_factory,
            conversation_compactor=conversation_compactor,
            conversation_compactor_factory=conversation_compactor_factory,
            execution_state_factory=execution_state_factory,
            runtime_limits=runtime_limits,
            recovery_policy=recovery_policy,
            tool_execution_limits=tool_execution_limits,
        )
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
        self._run_supervisor = _build_run_supervisor(
            run_repository=run_repository,
            output_repository=output_repository,
            output_publisher=output_publisher,
            execution_factory=self._supervised_execution,
            lease_store=execution_lease_store,
            owner_id=execution_owner_id,
            lease_duration_ms=execution_lease_duration_ms,
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
            snapshot = self._preset.snapshot(
                request,
                tool_catalog=self._tool_catalog,
            )
            persisted = resolved_options.agent_preset_snapshot
            if persisted is not None and persisted != snapshot:
                raise ContractViolationError(
                    "Run options carry a different AgentPreset snapshot"
                )
            resolved_options = replace(
                resolved_options,
                agent_preset_snapshot=snapshot,
            )
        elif resolved_options.durable_continuation is not None:
            raise ContractViolationError(
                "durable continuation requires a configured AgentPreset",
                code="agent_preset_snapshot_unsupported",
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
        return await restore_continuation_preset(
            options,
            self._output_repository,
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
                if (
                    conversation_compactor is not None
                    and context_capability.uses_staged_context
                ):
                    (
                        request,
                        pre_planning_compaction,
                        compaction_events,
                    ) = await compact_before_planning(
                        compactor=conversation_compactor,
                        request=compaction_source_request,
                        budget=reserved_budget,
                        controller=controller,
                        publish=self._publish_runtime_event,
                        signal=signal,
                    )
                    for event in compaction_events:
                        yield event
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
                prepared = await self._prepare_runtime_phase(
                    request=request,
                    compaction_source_request=compaction_source_request,
                    options=options,
                    controller=controller,
                    context_capability=context_capability,
                    conversation_compactor=conversation_compactor,
                    planning_bundle=planning_bundle,
                    bundle=bundle,
                    plan=plan,
                    selected_registrations=selected_registrations,
                    schemas=schemas,
                    selected_names=selected_names,
                    context_claims=context_claims,
                    reserved_budget=reserved_budget,
                    pre_planning_compaction=pre_planning_compaction,
                    output_limit=output_limit,
                    signal=signal,
                )
                request = prepared.request
                prepared_request = prepared.prepared_request
                budget = prepared.budget
                state = prepared.state
                delegation_bound = prepared.delegation_bound
                for event in prepared.events:
                    yield event
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

            runtime_result, runtime_events = await self._drive_runtime(
                request=request,
                prepared_request=prepared_request,
                options=options,
                controller=controller,
                sink=sink,
                conversation_compactor=conversation_compactor,
                model_tasks=model_tasks,
                schemas=schemas,
                registrations=registrations,
                selected_names=selected_names,
                planning_hook=planning_hook,
                plan=plan,
                state=state,
                budget=budget,
                output_limit=output_limit,
                signal=signal,
            )
            for event in runtime_events:
                yield event
            await self._settle_runtime_result(
                request=request,
                options=options,
                controller=controller,
                result=runtime_result,
                signal=signal,
            )
            for event in sink.drain():
                yield event
            yield _run_result(
                controller,
                model=(runtime_result.model if runtime_result is not None else request.model.model),
            )
        finally:
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

    async def _prepare_runtime_phase(
        self,
        *,
        request: AgentRunRequest,
        compaction_source_request: AgentRunRequest,
        options: AgentCoreRunOptions,
        controller: AgentRunController,
        context_capability: ContextCapability,
        conversation_compactor: ConversationCompactor | None,
        planning_bundle: ContextBundle,
        bundle: ContextBundle,
        plan: ExecutionPlan | None,
        selected_registrations: Sequence[ToolRegistration],
        schemas,
        selected_names: frozenset[str],
        context_claims: Sequence[ContextBudgetClaim],
        reserved_budget: ContextBudget,
        pre_planning_compaction: Mapping[str, Any],
        output_limit,
        signal: CancellationSignal | None,
    ) -> _PreparedRuntimePhase:
        setup_started = perf_counter()
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
                request.context_window or options.default_context_window_tokens
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
                        _planned_tool_names(plan) if plan is not None else ()
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
        diagnostics: dict[str, Any] = {
            "outcome": "not_configured",
            "plannedStepCount": len(plan.steps) if plan is not None else 0,
            "plannedToolCount": (
                sum(step.executor is StepExecutor.TOOL for step in plan.steps)
                if plan is not None
                else 0
            ),
            "selectedToolCount": len(selected_names),
        }
        events: tuple[AgentEvent, ...] = ()
        if (
            conversation_compactor is not None
            and context_capability.uses_staged_context
        ):
            request, diagnostics, events = await compact_after_planning(
                compactor=conversation_compactor,
                source_request=compaction_source_request,
                request=request,
                budget=budget,
                context_blocks=bundle.blocks,
                plan=plan,
                selected_names=selected_names,
                controller=controller,
                publish=self._publish_runtime_event,
                signal=signal,
            )
        prepared_request = replace(
            request,
            messages=_assemble_messages(request.messages, bundle.blocks, plan),
            context_window=budget.window_tokens,
        )
        estimated_input_tokens = estimate_agent_messages_tokens(
            prepared_request.messages
        )
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
                "reservedToolSchemaTokens": reserved_budget.tool_schema_tokens,
                "contextMode": context_mode,
                "contextDemands": _context_demand_diagnostics(
                    effective_context_claims,
                    budget,
                ),
                "droppedMessages": 0,
                "prePlanningCompaction": pre_planning_compaction,
                "postPlanningOptimization": diagnostics,
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
                "reservedToolSchemaTokens": reserved_budget.tool_schema_tokens,
                "contextMode": context_mode,
                "droppedMessages": 0,
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
                    "prePlanningCompaction": pre_planning_compaction,
                    "postPlanningOptimization": diagnostics,
                },
            },
        )
        await self._publish_runtime_event(controller.run_id, budget_event)
        delegation_bound = self._dynamic_delegated_executor is not None
        if self._dynamic_delegated_executor is not None:
            self._dynamic_delegated_executor.bind_run(
                controller.run_id,
                request,
                prepared_request.messages,
            )
        return _PreparedRuntimePhase(
            request=request,
            prepared_request=prepared_request,
            budget=budget,
            state=state,
            events=(*events, budget_event),
            delegation_bound=delegation_bound,
        )

    async def _settle_runtime_result(
        self,
        *,
        request: AgentRunRequest,
        options: AgentCoreRunOptions,
        controller: AgentRunController,
        result: AgentRuntimeResult | None,
        signal: CancellationSignal | None,
    ) -> None:
        if result is None:
            await controller.fail("runtime_returned_no_result")
            return
        if result.outcome is RuntimeOutcome.CANCELED:
            await controller.cancel(result.error_code or "request_canceled")
            return
        if result.outcome is not RuntimeOutcome.COMPLETED:
            await controller.fail(result.error_code or "runtime_failed")
            return
        try:
            final_response = result.final_response
            validated_result: str | None = None
            policy = options.resolved_response_transaction_policy
            if policy.mode is ResponseTransactionMode.VALIDATED_RESULT:
                validated_result = final_response
                final_response = ""
                if policy.public_presentation is PublicPresentationMode.MODEL_LIVE:
                    transaction = AgentResponseTransaction(
                        AgentModelInvocationManager(
                            self._model_gateway,
                            output_observer=self._output_processor,
                            operation_controller=self._operations,
                        ),
                        policy=policy,
                        facts_provider=options.committed_result_facts_provider,
                        operation_controller=self._operations,
                    )
                    final_response = await transaction.present(
                        AgentRunResult(
                            run_id=controller.run_id,
                            status=RunStatus.DONE,
                            final_response=result.final_response,
                            model=result.model,
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
            await _record_safe_exception(
                controller,
                stage="completion_projection",
                outcome="failed",
                error=error,
            )
            code = str(
                getattr(error, "code", "") or "completion_projection_failed"
            ).strip()
            await controller.fail(code)

    async def _drive_runtime(
        self,
        *,
        request: AgentRunRequest,
        prepared_request: AgentRunRequest,
        options: AgentCoreRunOptions,
        controller: AgentRunController,
        sink: _BufferedEventSink,
        conversation_compactor: ConversationCompactor | None,
        model_tasks: AgentModelTaskRunner,
        schemas,
        registrations: Sequence[ToolRegistration],
        selected_names: frozenset[str],
        planning_hook,
        plan: ExecutionPlan | None,
        state: ExecutionState,
        budget: ContextBudget,
        output_limit,
        signal: CancellationSignal | None,
    ) -> tuple[AgentRuntimeResult | None, tuple[AgentEvent, ...]]:
        host_arguments = {
            item.schema.name: thaw_json_mapping(item.host_planned_arguments)
            for item in registrations
            if item.host_planned_arguments is not None
        }
        gateway: ModelGateway = self._model_gateway
        manager = self._model_invocations
        if host_arguments:
            gateway = HostPlannedToolGateway(self._model_gateway, host_arguments)
            manager = AgentModelInvocationManager(
                gateway,
                output_observer=self._output_processor,
                operation_controller=self._operations,
            )
        runtime = AgentRuntime(
            model_gateway=gateway,
            tool_execution_gateway=self._tool_executor,
            observer=controller,
            context_compressor=conversation_compactor,
            limits=self._runtime_limits,
            recovery_policy=self._recovery_policy,
            operation_controller=self._operations,
            output_observer=self._output_processor,
            model_manager=manager,
        )
        result: AgentRuntimeResult | None = None
        events: list[AgentEvent] = []
        try:
            stream = runtime.run(
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
                scope_tools_to_observer=plan is not None,
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
                    item.schema.name: item.context_contract
                    for item in registrations
                },
                stage_context_projection_enabled=bool(
                    plan is not None and plan.task_spec is not None
                ),
                signal=signal,
            )
            async with aclosing(stream) as updates:
                async for update in updates:
                    events.extend(sink.drain())
                    if isinstance(update, AgentEvent):
                        event = _bind_event_to_run(update, controller.run_id)
                        await self._publish_runtime_event(controller.run_id, event)
                        events.append(event)
                    else:
                        result = update
        except OperationCanceled:
            result = AgentRuntimeResult(
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
            result = AgentRuntimeResult(
                run_id=controller.run_id,
                outcome=RuntimeOutcome.FAILED,
                final_response="",
                model=request.model.model,
                round_count=0,
                error_code="runtime_exception",
            )
        events.extend(sink.drain())
        return result, tuple(events)

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


def _resolve_conversation_compactor(
    context_provider: ContextProvider | None,
    context_provider_factory,
    compactor: ConversationCompactor | None,
    compactor_factory,
    operations: AgentOperationController | None,
) -> ConversationCompactor:
    if context_provider is not None and context_provider_factory is not None:
        raise ValueError(
            "context provider and context provider factory are mutually exclusive"
        )
    if compactor is not None and compactor_factory is not None:
        raise ValueError(
            "conversation compactor and compactor factory are mutually exclusive"
        )
    if isinstance(compactor, ContextCompressionCoordinator):
        return ContextCompressionCoordinator(
            compactor.hook,
            compactor.settings,
            operation_controller=operations,
        )
    return compactor or ContextCompressionCoordinator(
        operation_controller=operations,
    )


def _configure_output_runtime(
    model_gateway: ModelGateway,
    run_repository: RunRepository,
    *,
    output_processor: AgentOutputProcessor | None,
    output_repository: AgentOutputRepository | None,
    output_publisher: AgentOutputPublisher | None,
    operation_controller: AgentOperationController | None,
):
    if (output_repository is None) != (output_publisher is None):
        raise ValueError(
            "canonical output repository and publisher must be configured together"
        )
    processor = output_processor or (
        AgentOutputProcessor(output_repository, output_publisher)
        if output_repository is not None and output_publisher is not None
        else None
    )
    repository = (
        CanonicalRunRepository(run_repository, processor)
        if processor is not None
        else run_repository
    )
    operations = operation_controller or (
        AgentOperationController(processor) if processor is not None else None
    )
    invocations = AgentModelInvocationManager(
        model_gateway,
        output_observer=processor,
        operation_controller=operations,
    )
    return processor, repository, operations, invocations


def _build_run_supervisor(
    *,
    run_repository: RunRepository,
    output_repository: AgentOutputRepository | None,
    output_publisher: AgentOutputPublisher | None,
    execution_factory,
    lease_store: ExecutionLeaseStore | None,
    owner_id: str | None,
    lease_duration_ms: int | None,
) -> AgentRunSupervisor | None:
    if output_repository is None or output_publisher is None:
        return None
    return AgentRunSupervisor(
        output_repository=output_repository,
        output_publisher=output_publisher,
        execution_factory=execution_factory,
        lease_store=lease_store,
        owner_id=owner_id or getattr(run_repository, "owner_id", None),
        lease_duration_ms=(
            lease_duration_ms
            or getattr(run_repository, "lease_duration_ms", None)
            or 30_000
        ),
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
