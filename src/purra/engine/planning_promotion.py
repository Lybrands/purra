"""Planning promotion and runtime-phase preparation for AgentCore."""

from __future__ import annotations

from purra.engine.orchestration_support import (
    record_stage_failure,
    _PreparedRuntimePhase,
    _collect_durable_updates,
    _record_safe_exception,
    _uses_validated_result,
)
from dataclasses import (
    replace,
)
from purra.agent_execution_checkpoint import (
    AgentExecutionCheckpoint,
)
from purra.cancellation import (
    OperationCanceled,
    await_with_cancellation,
)
from purra.context_budget import (
    estimate_agent_messages_tokens,
    estimate_tool_schema_tokens,
    resolve_task_context_budget_claims,
)
from purra.contracts import (
    AgentRunRequest,
    AgentRuntimeResult,
    ContextBudget,
    ContextBudgetClaim,
    ContextBundle,
    ExecutionPlan,
    ExecutionState,
    PlanningMode,
    StepExecutor,
    TaskContextRequest,
    ToolPlanningRequirement,
    TraceRecord,
)
from purra.engine.canonical_sink import BufferedEventSink as _BufferedEventSink
from purra.engine.compaction_phase import (
    compact_after_planning,
    compact_before_planning,
)
from purra.engine.context_capability import (
    ContextCapability,
)
from purra.engine.context_phase import assemble_messages as _assemble_messages
from purra.engine.context_phase import compile_task_context_request as _compile_task_context_request
from purra.engine.context_phase import context_demand_diagnostics as _context_demand_diagnostics
from purra.engine.context_phase import merge_context_claims as _merge_context_claims
from purra.engine.context_phase import planned_tool_names as _planned_tool_names
from purra.engine.context_phase import validate_context_allocations as _validate_context_allocations
from purra.engine.options import (
    AgentCoreRunOptions,
)
from purra.engine.planning_phase import (
    PlanningCapability,
)
from purra.engine.run_budget import (
    _execution_context_budget,
)
from purra.errors import (
    ContextOverflowError,
    ContractViolationError,
)
from purra.events import (
    AgentEvent,
    CoreEventType,
)
from purra.json_values import (
    thaw_json_mapping,
)
from purra.model_execution import (
    AgentModelTaskRunner,
)
from purra.plan_compiler import (
    runtime_tool_names_for_planning_names,
)
from purra.planner import (
    build_execution_message,
    effective_planning_tool_names,
)
from purra.planning_activation import (
    AutoPlanningRequest,
)
from purra.ports import (
    CancellationSignal,
    ConversationCompactor,
    TaskContextDemandProvider,
    ToolRegistration,
)
from purra.run_controller import (
    AgentRunController,
)
from purra.task_admission import (
    ExecutionMode,
)
from purra.timing import duration_ms as _duration_ms
from purra.tools import (
    model_visible_tool_schema,
)
from time import (
    perf_counter,
)
from typing import (
    Any,
    Mapping,
    Sequence,
)


async def _prepare_runtime_phase(
    core,
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
    output_budget,
    planned: bool,
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
    budget = _execution_context_budget(
        request, options, output_budget, schemas, effective_context_claims,
    )
    context_mode = "single_pass" if planned else "reactive"
    if context_capability.staged_provider is not None and planned:
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
        planned
        and conversation_compactor is not None
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
            publish=core._publish_runtime_event,
            signal=signal,
        )
    prepared_request = replace(
        request,
        messages=_assemble_messages(request.messages, bundle.blocks, plan),
        context_window=budget.window_tokens,
        tools_enabled=bool(schemas),
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
    state = core._execution_state_factory.create(request)
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
            "outputBudget": output_budget.to_mapping(),
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
    await core._publish_runtime_event(controller.run_id, budget_event)
    return _PreparedRuntimePhase(
        request=request,
        prepared_request=prepared_request,
        budget=budget,
        state=state,
        events=(*events, budget_event),
    )


async def _promote_auto_run(
    core,
    *,
    activation: AutoPlanningRequest,
    request: AgentRunRequest,
    options: AgentCoreRunOptions,
    controller: AgentRunController,
    sink: _BufferedEventSink,
    model_tasks: AgentModelTaskRunner,
    context_capability: ContextCapability,
    conversation_compactor: ConversationCompactor | None,
    registrations: Sequence[ToolRegistration],
    enabled_names: frozenset[str],
    display_locale: str,
    context_claims: Sequence[ContextBudgetClaim],
    reserved_budget: ContextBudget,
    output_budget,
    state: ExecutionState,
    budget: ContextBudget,
    signal: CancellationSignal | None,
) -> tuple[AgentRuntimeResult | None, tuple[AgentEvent, ...]]:
    events: list[AgentEvent] = []
    remaining_checkpoint = (
        activation.resume_checkpoint
        if activation.phase == "remaining"
        and isinstance(
            activation.resume_checkpoint,
            AgentExecutionCheckpoint,
        )
        else None
    )
    if activation.phase == "remaining" and remaining_checkpoint is None:
        await controller.fail("remaining_planning_checkpoint_unavailable")
        return None, ()
    if remaining_checkpoint is not None:
        request = replace(request, messages=remaining_checkpoint.messages)
    remaining_model_rounds = (
        core._runtime_limits.max_model_rounds - activation.round_count
    )
    if remaining_model_rounds < 1:
        await controller.record_trace(TraceRecord(
            stage="planning_activation",
            outcome="budget_exhausted",
            details={"roundsUsed": activation.round_count},
        ))
        await controller.fail("planning_activation_budget_exhausted")
        return None, ()
    promoted_limits = replace(
        core._runtime_limits,
        max_model_rounds=remaining_model_rounds,
    )
    pre_planning_compaction: dict[str, Any] = {
        "outcome": "not_configured"
    }
    try:
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
                request=request,
                budget=reserved_budget,
                controller=controller,
                publish=core._publish_runtime_event,
                signal=signal,
            )
            events.extend(compaction_events)
        planning_bundle = await await_with_cancellation(
            context_capability.build_initial(
                request,
                reserved_budget,
                signal,
            ),
            signal,
        )
        _validate_context_allocations(planning_bundle, reserved_budget)
    except OperationCanceled:
        await controller.cancel("request_canceled")
        return None, tuple(events)
    except ContextOverflowError as error:
        await record_stage_failure(
            controller,
            error=error,
            stage="context_reservation",
            outcome="overflow",
            code="context_overflow_initial",
        )
        return None, tuple(events)
    except Exception as error:
        await record_stage_failure(
            controller,
            error=error,
            stage="context_reservation",
            outcome="failed",
            code="context_setup_failed",
        )
        return None, tuple(events)

    planning_result = await PlanningCapability(
        operations=core._operations,
        model_manager=core._model_invocations,
        planner=core._planner,
        policy=core._planning_policy,
        runtime_limits=promoted_limits,
        task_orchestration=core._task_orchestration,
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
        reasoning_mode=options.reasoning_mode,
    )
    if planning_result.terminal:
        return None, tuple(events)

    plan = planning_result.execution_plan
    capabilities = planning_result.capabilities
    admission = planning_result.admission
    if (
        admission is not None
        and admission.mode is not ExecutionMode.INLINE
        and plan is not None
    ):
        if core._task_orchestration is None:
            await controller.fail("planning_admission_unavailable")
            return None, tuple(events)
        durable_result, durable_events = await _collect_durable_updates(
            core._task_orchestration.complete_admission(
                controller=controller,
                request=request,
                plan=plan,
                admission=admission,
                sink=sink,
                signal=signal, defer_successful_completion=_uses_validated_result(options),
            ),
            run_id=controller.run_id, model=request.model.model,
        )
        events.extend(durable_events)
        return durable_result, tuple(events)

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
        model_visible_tool_schema(registration.schema, display_locale)
        for registration in selected_registrations
    )
    if remaining_checkpoint is not None:
        resume_messages = remaining_checkpoint.messages
        if plan is not None:
            resume_messages = (
                *resume_messages,
                build_execution_message(plan),
            )
        resume_checkpoint = replace(
            remaining_checkpoint,
            messages=resume_messages,
            round_limit=max(
                remaining_checkpoint.next_round,
                remaining_model_rounds,
            ),
        )
        resumed_tool_schema_tokens = estimate_tool_schema_tokens(schemas)
        resumed_budget = replace(
            budget,
            tool_schema_tokens=resumed_tool_schema_tokens,
            provider_input_tokens=(
                budget.provider_input_tokens
                + budget.tool_schema_tokens
                - resumed_tool_schema_tokens
            ),
        )
        runtime_result, runtime_events = await core._drive_runtime(
            request=request,
            prepared_request=replace(
                request,
                messages=resume_messages,
                tools_enabled=bool(schemas),
            ),
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
            budget=resumed_budget,
            output_budget=output_budget,
            planning_mode=PlanningMode.PLANNED,
            planning_available=True,
            planning_required_tool_names=frozenset(),
            model_round_limit=resume_checkpoint.round_limit,
            signal=signal,
            resume_checkpoint=resume_checkpoint,
        )
        events.extend(runtime_events)
        if isinstance(runtime_result, AutoPlanningRequest):
            await controller.fail("invalid_planning_control_call")
            return None, tuple(events)
        return runtime_result, tuple(events)
    try:
        prepared = await core._prepare_runtime_phase(
            request=request,
            compaction_source_request=request,
            options=options,
            controller=controller,
            context_capability=context_capability,
            conversation_compactor=conversation_compactor,
            planning_bundle=planning_bundle,
            bundle=planning_bundle,
            plan=plan,
            selected_registrations=selected_registrations,
            schemas=schemas,
            selected_names=selected_names,
            context_claims=context_claims,
            reserved_budget=reserved_budget,
            pre_planning_compaction=pre_planning_compaction,
            output_budget=output_budget,
            planned=True,
            signal=signal,
        )
        events.extend(prepared.events)
    except OperationCanceled:
        await controller.cancel("request_canceled")
        return None, tuple(events)
    except ContextOverflowError as error:
        await record_stage_failure(
            controller,
            error=error,
            stage="context_budget",
            outcome="overflow",
            code="context_overflow_initial",
        )
        return None, tuple(events)
    except Exception as error:
        await record_stage_failure(
            controller,
            error=error,
            stage="context_budget",
            outcome="failed",
            code="context_setup_failed",
        )
        return None, tuple(events)

    runtime_result, runtime_events = await core._drive_runtime(
        request=prepared.request,
        prepared_request=prepared.prepared_request,
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
        state=prepared.state,
        budget=prepared.budget,
        output_budget=output_budget,
        planning_mode=PlanningMode.PLANNED,
        planning_available=True,
        planning_required_tool_names=frozenset(),
        model_round_limit=remaining_model_rounds,
        signal=signal,
    )
    events.extend(runtime_events)
    if isinstance(runtime_result, AutoPlanningRequest):
        await controller.fail("invalid_planning_control_call")
        return None, tuple(events)
    return runtime_result, tuple(events)


async def _execute_runtime_with_auto_promotion(
    core,
    *,
    request: AgentRunRequest,
    prepared_request: AgentRunRequest,
    compaction_source_request: AgentRunRequest,
    options: AgentCoreRunOptions,
    controller: AgentRunController,
    sink: _BufferedEventSink,
    context_capability: ContextCapability,
    conversation_compactor: ConversationCompactor | None,
    model_tasks: AgentModelTaskRunner,
    schemas,
    registrations: Sequence[ToolRegistration],
    enabled_names: frozenset[str],
    selected_names: frozenset[str],
    display_locale: str,
    context_claims: Sequence[ContextBudgetClaim],
    reserved_budget: ContextBudget,
    planning_hook,
    plan: ExecutionPlan | None,
    state: ExecutionState,
    budget: ContextBudget,
    output_budget,
    planning_available: bool,
    signal: CancellationSignal | None,
    resume_checkpoint: AgentExecutionCheckpoint | None = None,
) -> tuple[
    AgentRuntimeResult | None,
    tuple[AgentEvent, ...],
]:
    result, events = await core._drive_runtime(
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
        output_budget=output_budget,
        planning_mode=PlanningMode.PLANNED if plan is not None else request.planning_mode,
        planning_available=planning_available,
        planning_required_tool_names=frozenset(
            registration.schema.name
            for registration in registrations
            if registration.planning_requirement
            is ToolPlanningRequirement.REQUIRED
        ),
        signal=signal,
        resume_checkpoint=resume_checkpoint,
    )
    if not isinstance(result, AutoPlanningRequest):
        return result, events
    promoted, promoted_events = await core._promote_auto_run(
        activation=result,
        request=compaction_source_request,
        options=options,
        controller=controller,
        sink=sink,
        model_tasks=model_tasks,
        context_capability=context_capability,
        conversation_compactor=conversation_compactor,
        registrations=registrations,
        enabled_names=enabled_names,
        display_locale=display_locale,
        context_claims=context_claims,
        reserved_budget=reserved_budget,
        output_budget=output_budget,
        state=state,
        budget=budget,
        signal=signal,
    )
    return promoted, (*events, *promoted_events)
