"""The single high-level orchestration entry for a complete PurrA run."""

from __future__ import annotations


import asyncio
from contextlib import aclosing, suppress
from dataclasses import dataclass, replace
from hashlib import sha256
import json
from time import perf_counter, time
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Mapping,
    Sequence,
)
from uuid import uuid4

from purra.agent_tree import (
    AgentCapabilityGrant,
    AgentNode,
    AgentRunAggregation,
    AgentTreeRun,
    AgentTreeRunStatus,
    BeginRootAgentCommand,
    ContinueAgentCommand,
    ContinueAgentReceipt,
    ContextCheckpoint,
    RunTreeRepository,
    SpawnAgentsCommand,
    SpawnAgentsReceipt,
)
from purra.agent_execution_checkpoint import AgentExecutionCheckpoint, AgentToolExecutionCheckpoint
from purra.interaction import UserInputRequired
from purra.approvals import ApprovalRequired
from purra.evidence import RunEvidenceStore
from purra.agent_tree_execution import (
    AgentTreeExecutionResult,
    RunCommandService,
)
from purra.agent_tree.lease import bind_agent_run_lease
from purra.engine.root_output import RootOutputObserver
from purra.engine.agent_result_delivery import AgentResultDelivery
from purra.engine.agent_context import AgentConversationLoader, child_run_options, task_message
from purra.agent_tree_tool import AGENT_TREE_TOOL_NAMES, AgentToolContext, build_agent_tree_tools
from purra.agent_tree_policy import AgentTreePolicy
from purra.agent_presets import (
    AgentPreset,
    AgentPresetSnapshot,
)
from purra.cancellation import (
    ExecutionDeadlineExceeded,
    ExecutionStopSignal,
    OperationCanceled,
    await_with_cancellation,
    is_canceled as _is_canceled,
    stop_reason,
)
from purra.context_budget import (
    allocate_context_budget,
    estimate_agent_messages_tokens,
    estimate_tool_schema_tokens,
    max_generation_tokens_for_context,
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
    ContextBudget,
    ContextBudgetClaim,
    ContextBundle,
    ExecutionState,
    MessageOrigin,
    MessageRole,
    PlanningMode,
    PlanningCapabilities,
    RunCreateParams,
    RunId,
    RunStatus,
    RuntimeLimits,
    RuntimeOutcome,
    StepExecutor,
    TaskContextRequest,
    ExecutionPlan,
    ToolExecutionLimits,
    ToolPlanningRequirement,
    TraceRecord,
)
from purra.normalization import (
    required_text,
)
from purra.errors import (
    AgentCoreError,
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
from purra.engine.run_budget import (
    _context_result_reserve_tokens,
    _execution_context_budget,
    _resolve_run_output_budget,
    _selected_context_window_tokens,
)
import purra.engine.durable_resume as durable_resume
import purra.engine.planning_promotion as planning_promotion
from purra.engine.orchestration_support import (
    record_stage_failure,
    _run_result,
    _runtime_result_from_durable,
    _uses_validated_result,
    _collect_durable_updates,
    _record_safe_exception,
    _settle_execution_exception,
    _execution_error_code,
    _PreparedRuntimePhase,
)
import purra.engine.agent_tree_orchestrator as agent_tree_orchestration
from purra.engine.agent_tree_orchestrator import (
    _AgentCoreTreeRunExecutor,
    _AgentTreeRootBinding,
    _bind_agent_tree_lease,
    _bind_agent_tree_run,
    _configure_agent_tree_capability,
    _require_root_agent_tree_quiescent,
    _restrict_agent_capabilities,
    _settle_root_agent_tree_run,
)
from purra.engine.canonical_sink import (
    BufferedEventSink as _BufferedEventSink,
    runtime_output_event as _runtime_output_event,
)
from purra.engine.durable_execution import (
    DurableExecutionCompletion,
    bind_event_to_run as _bind_event_to_run,
)
from purra.engine.options import AgentCoreRunOptions, restore_continuation_preset
from purra.engine.planning_phase import PlanningCapability, PlanningPhaseResult
from purra.engine.dynamic_planning import DynamicPlanningOrchestrator
from purra.engine.task_orchestration import TaskOrchestrationCapability
from purra.engine.tool_catalog import AugmentedToolCatalog
from purra.execution import AgentRunHandle, AgentRunSupervisor
from purra.engine.planning_validation import (
    effective_registrations as _effective_registrations,
)
from purra.execution_profiles import ExecutionProfile
from purra.host_planned_tool_gateway import HostPlannedToolGateway
from purra.json_values import freeze_json_mapping, thaw_json_mapping
from purra.model_protocol import (
    ResultCapacitySource,
    constrain_output_budget_to_context,
    resolve_invocation_output_budget,
)
from purra.model_invocation import (
    AgentModelInvocationManager,
    create_model_invocation_manager,
    ModelInvocationContext,
)
from purra.model_execution import AgentModelResponseJudge, AgentModelTaskRunner
from purra.planner import build_execution_message, effective_planning_tool_names
from purra.planning_activation import (
    AUTO_PLANNING_TOOL_NAME,
    AUTO_PLANNING_TOOL_SCHEMAS,
    AutoPlanningRequest,
)
from purra.plan_compiler import (
    runtime_tool_names_for_planning_names,
)
from purra.ports import (
    ApprovalGateway,
    CancellationSignal,
    CONTROLLER_OWNED_RUN_EVENT_TYPES,
    ContextProvider,
    ConversationCompactor,
    ExecutionLeaseStore,
    ExecutionStateFactory,
    PlanningPolicy,
    RunCommit,
    RunRepository,
    TaskContextDemandProvider,
    WorkPlanner,
    ToolCatalog,
    ToolIdempotencyGateway,
    ToolRegistration,
    ModelGateway,
    ModelInputEvidenceValidator,
)
from purra.output import AgentResponseTransaction
from purra.output.contracts import (
    PublicPresentationMode,
    ResponseTransactionPolicy,
    ResponseTransactionMode,
)
from purra.output.processor import AgentOutputProcessor
from purra.output.run_repository import CanonicalRunRepository
from purra.output.ports import AgentOutputPublisher, AgentOutputRepository
from purra.run_controller import AgentRunController
from purra.run_state import RunStateMachine
from purra.runtime import AgentRuntime
from purra.recovery import RecoveryPolicy
from purra.tools import (
    CoreToolExecutor,
    InMemoryApprovalGateway,
    InMemoryToolCatalog,
    model_visible_tool_schema,
)
from purra.task_admission import (
    ExecutionMode,
    LongTaskDispatcher,
    LongTaskExecutionStatus,
    TaskAdmissionEvaluator,
)
from purra.timing import duration_ms as _duration_ms
from purra.operations import AgentOperationController


@dataclass(frozen=True, slots=True)
class _RuntimeDependencies:
    model_tasks: AgentModelTaskRunner
    context_provider: ContextProvider
    conversation_compactor: ConversationCompactor | None
    context_capability: ContextCapability


def _require_runtime_limits(value: RuntimeLimits | None) -> RuntimeLimits:
    if value is None:
        raise TypeError(
            "AgentCore requires RuntimeLimits with an explicit "
            "max_run_generation_tokens value"
        )
    return value


class AgentCore:
    """Compose context, model/tool runtime and optional execution capabilities.

    ``submit`` is the only complete-run entry and returns a stable,
    server-owned execution handle.
    Concrete domain tools enter only as registrations in ``tool_catalog``.
    Runs default to Auto. A configured planner enables governed activation;
    an optional planning policy constrains the activated plan. Without a planner,
    ordinary model/tool execution remains available.
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
        task_admission_evaluator: TaskAdmissionEvaluator | None = None,
        long_task_dispatcher: LongTaskDispatcher | None = None,
        approval_gateway: ApprovalGateway | None = None,
        tool_idempotency_gateway: ToolIdempotencyGateway | None = None,
        runtime_limits: RuntimeLimits | None = None,
        recovery_policy: RecoveryPolicy = RecoveryPolicy(),
        tool_execution_limits: ToolExecutionLimits = ToolExecutionLimits(),
        operation_controller: AgentOperationController | None = None,
        output_processor: AgentOutputProcessor | None = None,
        output_repository: AgentOutputRepository | None = None,
        output_publisher: AgentOutputPublisher | None = None,
        execution_lease_store: ExecutionLeaseStore | None = None,
        execution_owner_id: str | None = None,
        execution_lease_duration_ms: int | None = None,
        run_tree_repository: RunTreeRepository | None = None,
        root_agent_id: str | None = None,
        agent_capability_grant: AgentCapabilityGrant | None = None,
        evidence_validator: ModelInputEvidenceValidator | None = None,
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
                runtime_limits is not None,
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
            agent_tree_policy = preset.agent_tree_policy
            runtime_limits = preset.runtime_limits
            recovery_policy = preset.recovery_policy
        else:
            agent_tree_policy = None
        runtime_limits = _require_runtime_limits(runtime_limits)
        if agent_tree_policy is not None and not isinstance(
            agent_tree_policy,
            AgentTreePolicy,
        ):
            raise TypeError(
                "agent_tree_policy must be an AgentTreePolicy or None"
            )
        self._preset = preset
        self._agent_tree_policy = agent_tree_policy
        self._model_gateway, self._evidence_validator = model_gateway, evidence_validator
        self._output_repository = output_repository
        self._runtime_limits, self._tool_execution_limits = runtime_limits, tool_execution_limits
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
            runtime_limits=runtime_limits,
            max_tool_argument_chars=tool_execution_limits.max_argument_chars,
            evidence_validator=evidence_validator,
        )
        self._context_provider_factory, self._conversation_compactor_factory = context_provider_factory, conversation_compactor_factory
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
            if context_strategy is None:
                context_strategy = ContextStrategy.SINGLE_PASS
            execution_profile = ExecutionProfile(
                planner=planner,
                planning_policy=planning_policy,
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
        self._context_provider = context_provider or _EmptyContextProvider()
        self._execution_state_factory = (
            execution_state_factory or _DefaultExecutionStateFactory()
        )
        self._approval_gateway = approval_gateway or InMemoryApprovalGateway()
        self._run_supervisor = _build_run_supervisor(
            run_repository=run_repository,
            output_repository=output_repository,
            output_publisher=output_publisher,
            execution_factory=self._supervised_execution,
            lease_store=execution_lease_store,
            owner_id=execution_owner_id,
            lease_duration_ms=execution_lease_duration_ms,
        )
        if run_tree_repository is not None and self._run_supervisor is None:
            raise ValueError(
                "Agent tree execution requires output repository and publisher"
            )
        base_tool_catalog = tool_catalog or InMemoryToolCatalog(())
        self._configure_agent_tree_capability(
            base_tool_catalog=base_tool_catalog,
            policy=agent_tree_policy,
            run_tree_repository=run_tree_repository,
            root_agent_id=root_agent_id,
            agent_capability_grant=agent_capability_grant,
        )
        self._registrations = tuple(self._tool_catalog.registrations())
        if any(
            registration.schema.name == AUTO_PLANNING_TOOL_NAME
            for registration in self._registrations
        ):
            raise ValueError(
                f"{AUTO_PLANNING_TOOL_NAME} is reserved for Planner activation"
            )
        self._tool_idempotency_gateway = tool_idempotency_gateway
        # This is deliberately not replaceable by a domain ``execute`` hook:
        # every registered handler crosses the same Core policy boundary.
        self._tool_executor = CoreToolExecutor(
            _CapturedToolCatalog(self._registrations),
            self._approval_gateway,
            tool_execution_limits,
            tool_idempotency_gateway,
            self._operations,
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

    async def spawn_agents(
        self,
        command: SpawnAgentsCommand,
    ) -> SpawnAgentsReceipt:
        return await self._require_agent_tree_commands().spawn_agents(command)

    async def continue_agent(
        self,
        command: ContinueAgentCommand,
    ) -> ContinueAgentReceipt:
        return await self._require_agent_tree_commands().continue_agent(command)

    async def join_agent_runs(
        self,
        requester_run_id: str,
        run_ids: tuple[str, ...],
        signal: CancellationSignal | None = None,
        *,
        lease_owner_id: str | None = None,
        lease_epoch: int | None = None,
        delivery_signal: CancellationSignal | None = None,
    ) -> AgentRunAggregation:
        self._require_agent_tree_commands()
        requester = await self._run_tree_repository.get_run(requester_run_id)
        if requester.root_run_id not in self._agent_tree_roots:
            raise ContractViolationError(
                "Joining Children requires their active Root; recover the Root with resume()",
                code="agent_tree_root_not_bound",
            )
        if requester_run_id == requester.root_run_id:
            await self._result_delivery.completed(requester_run_id)
        return await self._require_agent_tree_commands().join_runs(
            requester_run_id,
            run_ids,
            signal,
            lease_owner_id=lease_owner_id,
            lease_epoch=lease_epoch,
            delivery_signal=delivery_signal,
        )

    async def cancel_agent_run(self, run_id: str) -> tuple[str, ...]:
        return await self._require_agent_tree_commands().cancel_run(run_id)

    async def close_agent(self, agent_id: str) -> AgentNode:
        return await self._require_agent_tree_commands().close_agent(agent_id)

    def _require_agent_tree_commands(self) -> RunCommandService:
        commands = self._run_commands
        if commands is None:
            raise ContractViolationError(
                "Agent tree execution is not configured",
                code="agent_tree_unavailable",
            )
        return commands

    def _root_agent_grant(
        self,
        request: AgentRunRequest,
    ) -> AgentCapabilityGrant:
        policy = self._agent_tree_policy
        if policy is None:
            raise ContractViolationError(
                "Agent tree execution has no policy"
            )
        return AgentCapabilityGrant(
            can_spawn_agents=True,
            max_depth=policy.max_depth,
            max_children_per_call=policy.max_children_per_call,
            max_agents_per_root=policy.max_agents_per_root,
            max_parallel_runs=policy.max_parallel_runs,
            allowed_tools=tuple(
                registration.schema.name
                for registration in self._registrations
                if registration.schema.name not in AGENT_TREE_TOOL_NAMES
            ),
            allowed_models=(request.model.model,),
        )

    def _configure_agent_tree_capability(self, *args, **kwargs):
        return agent_tree_orchestration._configure_agent_tree_capability(self, *args, **kwargs)

    async def _bind_agent_tree_run(self, *args, **kwargs):
        return await agent_tree_orchestration._bind_agent_tree_run(self, *args, **kwargs)

    async def _settle_root_agent_tree_run(self, *args, **kwargs):
        return await agent_tree_orchestration._settle_root_agent_tree_run(self, *args, **kwargs)

    def _restrict_agent_capabilities(self, *args, **kwargs):
        return agent_tree_orchestration._restrict_agent_capabilities(*args, **kwargs)

    def _bind_agent_tree_lease(self, *args, **kwargs):
        return agent_tree_orchestration._bind_agent_tree_lease(self, *args, **kwargs)

    async def _require_root_agent_tree_quiescent(self, *args, **kwargs):
        return await agent_tree_orchestration._require_root_agent_tree_quiescent(self, *args, **kwargs)


    async def _require_parent_delivery_settled(self, run_id):
        if run_id in self._agent_tree_roots:
            if self._run_commands is not None:
                await self._run_commands.results.wait(run_id)
            await self._result_delivery.require_settled(run_id)

    def _root_output_observer(self):
        if self._run_tree_repository is None:
            return self._output_processor
        return RootOutputObserver(self._output_processor,
            self._require_root_agent_tree_quiescent,
            self._require_parent_delivery_settled, self._result_delivery.public_locks)


    @property
    def active_agent_root_ids(self):
        return tuple(self._agent_tree_roots)

    async def report_agent_results(self, run_id, results, signal=None):
        await self._result_delivery.report(run_id, results, signal)

    async def close(self) -> None:
        if self._run_commands is not None:
            await self._run_commands.results.close()
        if self._run_supervisor is not None:
            await self._run_supervisor.close()

    async def resume(self, run_id, request, *, options=None):
        """Resume a canonical checkpoint using the host's execution lease store."""
        if self._run_tree_repository is not None and (await self._run_tree_repository.get_run(run_id)).root_run_id != run_id:
            raise ContractViolationError("Resume Child Runs through their Root scheduler", code="child_run_resume_requires_scheduler")
        if self._run_supervisor is None or not self._run_supervisor.supports_root_recovery:
            raise ContractViolationError(
                "Root recovery requires an execution lease store",
                code="run_lease_required",
            )
        snapshot = await self._repository.get(run_id)
        if snapshot.terminal:
            raise ContractViolationError("Run is terminal", code="run_terminal")
        if snapshot.execution_checkpoint is None:
            raise ContractViolationError(
                "Run has no resumable checkpoint", code="checkpoint_missing",
            )
        if self._run_tree_repository is not None:
            await self._result_delivery.completed(run_id)
        if isinstance(snapshot.execution_checkpoint, AgentToolExecutionCheckpoint):
            pending_names = {call.name for call in snapshot.execution_checkpoint.assistant.tool_calls}
            if (options is None or options.tool_checkpoint_handler is None
                    or options.tool_checkpoint_names is not None and not pending_names <= options.tool_checkpoint_names):
                raise ContractViolationError("Tool-ready recovery requires its host gate", code="approval_gate_unavailable")
        return await self.submit(request, options=replace(
            options or AgentCoreRunOptions(), agent_execution_checkpoint=snapshot.execution_checkpoint,
            deadline_at_ms=snapshot.deadline_at_ms,
        ))

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
                tool_execution_limits=self._tool_execution_limits,
            )
            tree_grant: AgentCapabilityGrant | None = None
            if self._run_tree_repository is not None:
                tree_grant = (
                    self._configured_agent_grant
                    or self._root_agent_grant(request)
                )
                tree_composition = thaw_json_mapping(snapshot.composition)
                tree_composition["agentTree"] = {
                    **tree_composition["agentTree"],
                    "capabilityGrant": tree_grant.to_mapping(),
                }
                snapshot = AgentPresetSnapshot(
                    id=snapshot.id,
                    revision=snapshot.revision,
                    fingerprint=_preset_fingerprint_for_composition(
                        snapshot.id,
                        snapshot.revision,
                        tree_composition,
                    ),
                    composition=tree_composition,
                )
            persisted = resolved_options.agent_preset_snapshot
            if persisted is not None and persisted != snapshot:
                raise ContractViolationError(
                    "Run options carry a different AgentPreset snapshot"
                )
            resolved_options = replace(
                resolved_options,
                agent_preset_snapshot=snapshot,
                agent_capability_grant=(
                    resolved_options.agent_capability_grant
                    if resolved_options.agent_tree_run_id is not None
                    else tree_grant
                    if self._run_tree_repository is not None
                    else resolved_options.agent_capability_grant
                ),
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


    def _run_create_params(
        self,
        request: AgentRunRequest,
        options: AgentCoreRunOptions,
    ) -> RunCreateParams:
        execution_intent = (
            options.provenance.execution_intent
            if options.provenance is not None
            else None
        )
        if execution_intent is not None and (
            execution_intent.requested_user_max_generation_tokens
            != request.model.max_generation_tokens
            or execution_intent.result_capacity_target_tokens
            != options.result_capacity_target_tokens
        ):
            raise ContractViolationError(
                "Run provenance generation intent differs from the request",
                code="run_identity_conflict",
            )
        return RunCreateParams(
            session_id=request.session_id,
            prompt=request.latest_user_text(),
            mode=request.mode,
            turn_id=options.turn_id,
            deadline_at_ms=options.deadline_at_ms,
            runtime_limits=self._runtime_limits,
            provenance=options.provenance,
            binding=options.binding,
            agent_preset_snapshot=(
                options.agent_preset_snapshot.to_mapping()
                if options.agent_preset_snapshot is not None
                else {}
            ),
            requested_user_max_generation_tokens=(
                request.model.max_generation_tokens
            ),
            result_capacity_target_tokens=(
                options.result_capacity_target_tokens
            ),
            selected_context_window_tokens=_selected_context_window_tokens(
                request,
                options,
            ),
            requested_run_id=options.agent_tree_run_id,
            root_run_id=options.agent_tree_root_run_id,
            agent_id=(
                options.agent_tree_agent_id
                or self._root_agent_id
                if self._run_tree_repository is not None
                else None
            ),
            parent_run_id=options.agent_tree_parent_run_id,
            lease_owner_id=options.agent_tree_lease_owner_id,
            lease_epoch=options.agent_tree_lease_epoch,
        )


    def _execution_registrations(
        self, request: AgentRunRequest, options: AgentCoreRunOptions,
    ) -> tuple[tuple[ToolRegistration, ...], frozenset[str]]:
        registrations, enabled_names = _effective_registrations(
            self._tool_catalog, self._registrations, request,
            model_supports_tools=options.model_supports_tools,
        )
        registrations, enabled_names = self._restrict_agent_capabilities(
            request, options.agent_capability_grant, registrations, enabled_names,
        )
        return self._bind_agent_tree_lease(registrations, options), enabled_names

    async def execute_operation(
        self, request: AgentRunRequest, *, run_id: str, operation_id: str,
        options: AgentCoreRunOptions = AgentCoreRunOptions(),
        signal: CancellationSignal | None = None,
    ) -> AgentRuntimeResult:
        """Execute a private reactive tool loop inside an existing Run.

        The caller owns admission and operation settlement. This method never
        creates an Agent, changes the Run plan, saves a Run checkpoint, or
        commits a Run result. Model usage and tool events belong to run_id.
        """
        operation_id = required_text(operation_id, "operation id")
        if self._run_tree_repository is not None:
            raise ContractViolationError("Operation Core must not expose Agent creation", code="operation_scope_invalid")
        if any(value is not None for value in (
            options.agent_tree_run_id,
            options.durable_continuation,
            options.agent_execution_checkpoint,
            options.checkpoint_handler,
            options.tool_checkpoint_handler,
        )):
            raise ContractViolationError(
                "Operation cannot replace a Run execution",
                code="operation_scope_invalid",
            )
        snapshot = await self._repository.get(run_id)
        if snapshot is None or snapshot.status is not RunStatus.RUNNING:
            raise ContractViolationError("Operation requires its running owner", code="operation_owner_not_running")
        deadlines = tuple(value for value in (
            snapshot.deadline_at_ms, options.deadline_at_ms,
        ) if value is not None)
        deadline = min(deadlines) if deadlines else None
        options = replace(options, deadline_at_ms=deadline,
            response_transaction_policy=ResponseTransactionPolicy(
                mode=ResponseTransactionMode.VALIDATED_RESULT,
                public_presentation=PublicPresentationMode.NONE,
            ))
        request = replace(request, planning_mode=PlanningMode.REACTIVE)
        stop = ExecutionStopSignal(signal, deadline_at_ms=deadline, deadline_code="run_deadline_exceeded")
        sink = _BufferedEventSink(self._output_processor)
        controller = AgentRunController(repository=self._repository, event_sink=sink)
        await controller.attach(snapshot)
        try:
            dependencies = self._runtime_dependencies(controller, options, request, stop)
            registrations, names = self._execution_registrations(request, options)
            registrations = tuple(item for item in registrations if item.schema.name in names)
            schemas = tuple(model_visible_tool_schema(item.schema, str(request.metadata.get("locale") or "zh-CN")) for item in registrations)
            output_budget = _resolve_run_output_budget(request, options)
            claims = options.context_claims
            budget = _execution_context_budget(request, options, output_budget, schemas, claims)
            bundle = await await_with_cancellation(dependencies.context_capability.build_reactive(request, budget, stop), stop)
            prepared = await self._prepare_runtime_phase(
                request=request, compaction_source_request=request, options=options,
                controller=controller, context_capability=dependencies.context_capability,
                conversation_compactor=None, planning_bundle=bundle, bundle=bundle,
                plan=None, selected_registrations=registrations, schemas=schemas,
                selected_names=names, context_claims=claims, reserved_budget=budget,
                pre_planning_compaction={"outcome": "not_configured"},
                output_budget=output_budget, planned=False, signal=stop,
            )
            result, _ = await self._drive_runtime(
                request=request, prepared_request=prepared.prepared_request, options=options,
                controller=controller, sink=sink, conversation_compactor=None,
                model_tasks=dependencies.model_tasks, schemas=schemas, registrations=registrations,
                selected_names=names, planning_hook=None, plan=None, state=prepared.state,
                budget=prepared.budget, output_budget=output_budget, signal=stop,
                operation_id=operation_id,
            )
            if not isinstance(result, AgentRuntimeResult):
                raise ContractViolationError("Operation did not return a runtime result", code="operation_result_missing")
            return result
        finally:
            stop.close()

    async def _execute_run(
        self,
        request: AgentRunRequest,
        *,
        options: AgentCoreRunOptions | None = None,
        signal: CancellationSignal | None = None,
    ) -> AsyncIterator[AgentEvent | AgentRunResult]:
        options = _resolve_run_deadline(
            options or AgentCoreRunOptions(),
            self._runtime_limits,
        )
        owned_stop = ExecutionStopSignal(
            signal,
            deadline_at_ms=options.deadline_at_ms,
            deadline_code="run_deadline_exceeded",
        )
        signal = owned_stop
        output_budget = _resolve_run_output_budget(request, options)
        sink = _BufferedEventSink(self._output_processor)
        controller = AgentRunController(
            repository=self._repository,
            event_sink=sink,
        )
        agent_tree_root_owner = False
        agent_tree_context_version: int | None = None
        compaction_source_request = request
        pre_planning_compaction: dict[str, Any] = {"outcome": "not_configured"}
        input_suspended = False
        try:
            await self._start_or_attach_run(request, options, controller)
            (
                agent_tree_root_owner,
                agent_tree_context_version,
            ) = await self._bind_agent_tree_run(request, options, controller)
            dependencies = self._runtime_dependencies(controller, options, request, signal)
            model_tasks = dependencies.model_tasks
            context_provider = dependencies.context_provider
            conversation_compactor = dependencies.conversation_compactor
            context_capability = dependencies.context_capability
            planning_required, auto_planning, auto_planning_available = (
                self._planning_mode_flags(request, options)
            )
            for event in sink.drain():
                yield event
            if await self._reject_unavailable_planning(request, controller):
                for event in sink.drain():
                    yield event
                yield _run_result(controller)
                return
            if options.agent_execution_checkpoint is not None:
                async for event in self._resume_checkpointed_run(
                    request,
                    options,
                    controller,
                    sink,
                    model_tasks,
                    output_budget,
                    signal,
                ):
                    yield event
                return

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
                reason = stop_reason(signal)
                if reason == "run_deadline_exceeded":
                    await controller.fail(reason)
                else:
                    await controller.cancel(reason)
                for event in sink.drain():
                    yield event
                yield _run_result(controller)
                return

            if (continuation := options.durable_continuation) is not None:
                async for update in self._continue_durable_run(
                    request=request,
                    options=options,
                    controller=controller,
                    sink=sink,
                    continuation=continuation,
                    output_budget=output_budget,
                    signal=signal,
                ):
                    yield update
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
                registrations, enabled_names = self._execution_registrations(request, options)
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
                ) + (
                    AUTO_PLANNING_TOOL_SCHEMAS
                    if auto_planning_available
                    else ()
                )
                reserved_budget = _execution_context_budget(
                    request, options, output_budget, reserved_schemas, context_claims,
                )
                if (
                    planning_required
                    and conversation_compactor is not None
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
                    (
                        context_capability.build_initial
                        if planning_required
                        else context_capability.build_reactive
                    )(
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
                await record_stage_failure(
                    controller,
                    error=error,
                    stage="context_reservation",
                    outcome="overflow",
                    code="context_overflow_initial",
                    started=reservation_started,
                )
                for event in sink.drain():
                    yield event
                yield _run_result(controller)
                return
            except Exception as error:
                await record_stage_failure(
                    controller,
                    error=error,
                    stage="context_reservation",
                    outcome="failed",
                    code="context_setup_failed",
                    started=reservation_started,
                )
                for event in sink.drain():
                    yield event
                yield _run_result(controller)
                return

            planning_result = await self._resolve_initial_planning(
                request=request,
                planning_bundle=planning_bundle,
                registrations=registrations,
                enabled_names=enabled_names,
                display_locale=display_locale,
                options=options,
                controller=controller,
                planning_required=planning_required,
                auto_planning=auto_planning,
                signal=signal,
            )

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
                async for update in self._settle_durable_updates(
                    self._task_orchestration.complete_admission(
                        controller=controller,
                        request=request,
                        plan=plan,
                        admission=admission,
                        sink=sink,
                        signal=signal,
                        defer_successful_completion=_uses_validated_result(options),
                    ),
                    request=request,
                    options=options,
                    controller=controller,
                    sink=sink,
                    signal=signal,
                    output_budget=output_budget,
                ):
                    yield update
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
            ) + (
                AUTO_PLANNING_TOOL_SCHEMAS
                if auto_planning_available
                else ()
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
                    output_budget=output_budget,
                    planned=planning_required,
                    signal=signal,
                )
                request = prepared.request
                prepared_request = prepared.prepared_request
                budget = prepared.budget
                state = prepared.state
                for event in prepared.events:
                    yield event
            except OperationCanceled:
                await controller.cancel("request_canceled")
                for event in sink.drain():
                    yield event
                yield _run_result(controller)
                return
            except ContextOverflowError as error:
                await record_stage_failure(
                    controller,
                    error=error,
                    stage="context_budget",
                    outcome="overflow",
                    code="context_overflow_initial",
                    started=setup_started,
                )
                for event in sink.drain():
                    yield event
                yield _run_result(controller)
                return
            except Exception as error:
                await record_stage_failure(
                    controller,
                    error=error,
                    stage="context_budget",
                    outcome="failed",
                    code="context_setup_failed",
                    started=setup_started,
                )
                for event in sink.drain():
                    yield event
                yield _run_result(controller)
                return

            runtime_result, runtime_events = await self._execute_runtime_with_auto_promotion(
                request=request,
                prepared_request=prepared_request,
                compaction_source_request=compaction_source_request,
                options=options,
                controller=controller,
                sink=sink,
                context_capability=context_capability,
                conversation_compactor=conversation_compactor,
                model_tasks=model_tasks,
                schemas=schemas,
                registrations=registrations,
                enabled_names=enabled_names,
                selected_names=selected_names,
                display_locale=display_locale,
                context_claims=context_claims,
                reserved_budget=reserved_budget,
                planning_hook=planning_hook,
                plan=plan,
                state=state,
                budget=budget,
                output_budget=output_budget,
                planning_available=auto_planning_available,
                signal=signal,
            )
            for event in runtime_events:
                yield event
            snapshot = controller.snapshot
            if snapshot is not None and snapshot.terminal:
                for event in sink.drain():
                    yield event
                yield _run_result(controller, model=request.model.model)
                return
            await self._settle_runtime_result(
                request=request,
                options=options,
                controller=controller,
                result=runtime_result,
                output_budget=output_budget,
                signal=signal,
            )
            for event in sink.drain():
                yield event
            yield _run_result(
                controller,
                model=(runtime_result.model if runtime_result is not None else request.model.model),
            )
        except (UserInputRequired, ApprovalRequired):
            input_suspended = True
            raise
        except Exception as error:
            updates = await _settle_execution_exception(controller, sink, error)
            if updates is None:
                raise
            for update in updates:
                yield update
        finally:
            if self._run_commands is not None and controller.run_id is not None:
                await self._run_commands.results.close(controller.run_id)
            owned_stop.close()
            run_id = controller.run_id
            if run_id is not None:
                with suppress(Exception):
                    await self._approval_gateway.cancel_pending(run_id)
            snapshot = controller.snapshot
            if snapshot is not None and not snapshot.terminal and not input_suspended:
                # Supervisor shutdown closes this iterator; subscriptions do not.
                await controller.cancel("execution_owner_stopped")
                snapshot = controller.snapshot
            if snapshot is not None and snapshot.terminal and self._operations is not None:
                await self._operations.release_terminal_run(snapshot.run_id)
            if agent_tree_root_owner and snapshot is not None and not input_suspended:
                await self._settle_root_agent_tree_run(
                    snapshot,
                    agent_tree_context_version,
                )
                self._result_delivery.release(snapshot.run_id)

    def _planning_mode_flags(
        self,
        request: AgentRunRequest,
        options: AgentCoreRunOptions,
    ) -> tuple[bool, bool, bool]:
        planned = request.planning_mode is PlanningMode.PLANNED
        auto = request.planning_mode is PlanningMode.AUTO
        return planned, auto, bool(
            auto and self._planning_enabled and options.model_supports_tools
        )

    async def _reject_unavailable_planning(
        self,
        request: AgentRunRequest,
        controller: AgentRunController,
    ) -> bool:
        if (
            request.planning_mode is not PlanningMode.PLANNED
            or self._planning_enabled
        ):
            return False
        await controller.record_trace(TraceRecord(
            stage="execution_profile",
            outcome="planning_unavailable",
            details={"planningMode": request.planning_mode.value},
        ))
        await controller.fail("planning_unavailable")
        return True

    async def _resolve_initial_planning(
        self,
        *,
        request: AgentRunRequest,
        planning_bundle: ContextBundle,
        registrations: Sequence[ToolRegistration],
        enabled_names: frozenset[str],
        display_locale: str,
        options: AgentCoreRunOptions,
        controller: AgentRunController,
        planning_required: bool,
        auto_planning: bool,
        signal: CancellationSignal | None,
    ) -> PlanningPhaseResult:
        if not planning_required:
            await controller.record_trace(TraceRecord(
                stage="execution_profile",
                outcome="auto" if auto_planning else "reactive",
                details={
                    "toolCount": len(enabled_names),
                    "planningAvailable": self._planning_enabled,
                },
            ))
            return PlanningPhaseResult.reactive()
        return await PlanningCapability(
            operations=self._operations,
            model_manager=self._model_invocations,
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
            reasoning_mode=options.reasoning_mode,
        )

    async def _resume_checkpointed_run(self, *args, **kwargs):
        async for _event in durable_resume._resume_checkpointed_run(self, *args, **kwargs):
            yield _event

    async def _resume_child_runs(self, *args, **kwargs):
        return await durable_resume._resume_child_runs(self, *args, **kwargs)

    async def _continue_durable_run(self, *args, **kwargs):
        async for _event in durable_resume._continue_durable_run(self, *args, **kwargs):
            yield _event

    async def _settle_durable_updates(self, *args, **kwargs):
        async for _event in durable_resume._settle_durable_updates(self, *args, **kwargs):
            yield _event

    async def _prepare_runtime_phase(self, *args, **kwargs):
        return await planning_promotion._prepare_runtime_phase(self, *args, **kwargs)

    async def _promote_auto_run(self, *args, **kwargs):
        return await planning_promotion._promote_auto_run(self, *args, **kwargs)

    async def _execute_runtime_with_auto_promotion(self, *args, **kwargs):
        return await planning_promotion._execute_runtime_with_auto_promotion(self, *args, **kwargs)


    async def _start_or_attach_run(
        self,
        request: AgentRunRequest,
        options: AgentCoreRunOptions,
        controller: AgentRunController,
    ) -> None:
        checkpoint = options.agent_execution_checkpoint
        if checkpoint is None:
            await controller.start(self._run_create_params(request, options))
            return
        persisted = await self._repository.get(checkpoint.run_id)
        if persisted.execution_checkpoint != checkpoint:
            raise ContractViolationError(
                "Selected checkpoint is not canonical",
                code="agent_execution_checkpoint_conflict",
            )
        expected_preset = (
            options.agent_preset_snapshot.to_mapping()
            if options.agent_preset_snapshot is not None
            else {}
        )
        if persisted.agent_preset_snapshot != freeze_json_mapping(
            expected_preset
        ):
            raise ContractViolationError(
                "Agent composition differs from the checkpointed Run",
                code="agent_preset_mismatch",
            )
        if (
            persisted.requested_user_max_generation_tokens
            != request.model.max_generation_tokens
            or persisted.result_capacity_target_tokens
            != options.result_capacity_target_tokens
            or persisted.selected_context_window_tokens
            != _selected_context_window_tokens(request, options)
        ):
            raise ContractViolationError(
                "Model generation intent differs from the checkpointed Run",
                code="run_identity_conflict",
            )
        await controller.attach(persisted)

    def _runtime_dependencies(
        self,
        controller: AgentRunController,
        options: AgentCoreRunOptions,
        request: AgentRunRequest,
        signal: CancellationSignal | None = None,
    ) -> _RuntimeDependencies:
        model_tasks = AgentModelTaskRunner(
            self._model_invocations,
            ModelInvocationContext(
                run_id=controller.run_id,
                turn_id=options.turn_id,
                requested_reasoning_mode=options.reasoning_mode,
                deadline_at_ms=options.deadline_at_ms,
                context_window_tokens=_selected_context_window_tokens(
                    request,
                    options,
                ),
                safety_reserve_tokens=options.safety_reserve_tokens,
                runtime_reserve_tokens=options.runtime_reserve_tokens,
            ),
            request.model,
            signal=signal,
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
        if conversation_compactor is not None and not isinstance(
            conversation_compactor,
            ConversationCompactor,
        ):
            raise TypeError(
                "conversation compactor factory returned an invalid port"
            )
        return _RuntimeDependencies(
            model_tasks=model_tasks,
            context_provider=context_provider,
            conversation_compactor=conversation_compactor,
            context_capability=ContextCapability(
                self._context_strategy,
                context_provider,
            ),
        )

    async def _resume_runtime(
        self,
        *,
        request: AgentRunRequest,
        options: AgentCoreRunOptions,
        controller: AgentRunController,
        sink: _BufferedEventSink,
        model_tasks: AgentModelTaskRunner,
        output_budget,
        signal: CancellationSignal | None,
    ) -> tuple[AgentRuntimeResult | None, tuple[AgentEvent, ...]]:
        checkpoint = options.agent_execution_checkpoint
        if checkpoint is None:  # pragma: no cover - caller invariant
            raise ContractViolationError("Run resume requires a checkpoint")
        registrations, enabled_names = self._execution_registrations(request, options)
        display_locale = str(request.metadata.get("locale") or "zh-CN")
        plan = controller.snapshot.execution_plan if checkpoint.execution_profile == "planned" else None
        if checkpoint.execution_profile == "planned" and plan is None:
            raise ContractViolationError("Planned checkpoint has no admitted plan")
        planning_hook = None
        selected_names = enabled_names
        if checkpoint.planning_state:
            if not callable(getattr(self._planner, "revise_plan", None)):
                raise ContractViolationError("Checkpoint requires its dynamic Planner")
            planning_hook = DynamicPlanningOrchestrator(
                planner=self._planner, request=replace(request, messages=checkpoint.messages),
                capabilities=PlanningCapabilities(), controller=controller,
                enabled_names=enabled_names, registrations=registrations,
                turn_id=options.turn_id, reasoning_mode=options.reasoning_mode,
                operations=self._operations, model_manager=self._model_invocations,
            )
            planning_hook.restore_checkpoint_state(checkpoint.planning_state)
            selected_names = runtime_tool_names_for_planning_names(
                registrations, enabled_names, planning_hook.planning_tool_names,
            )
        elif plan is not None:
            selected_names = _planned_tool_names(plan) & enabled_names
        planning_available = plan is None and request.planning_mode is PlanningMode.AUTO and self._planning_enabled and options.model_supports_tools
        schemas = tuple(
            model_visible_tool_schema(registration.schema, display_locale)
            for registration in registrations
            if registration.schema.name in selected_names
        ) + (AUTO_PLANNING_TOOL_SCHEMAS if planning_available else ())
        budget = _execution_context_budget(
            request, options, output_budget, schemas, (),
        )
        prepared_request = replace(
            request,
            messages=checkpoint.messages,
            context_window=budget.window_tokens,
        )
        state = ExecutionState(
            domain=thaw_json_mapping(checkpoint.execution_state_domain),
            run_id=controller.run_id,
        )
        dependencies = self._runtime_dependencies(controller, options, request, signal)
        return await self._execute_runtime_with_auto_promotion(
            request=request,
            prepared_request=prepared_request,
            compaction_source_request=replace(request, messages=checkpoint.messages),
            options=options,
            controller=controller,
            sink=sink,
            context_capability=dependencies.context_capability,
            conversation_compactor=dependencies.conversation_compactor,
            model_tasks=model_tasks,
            schemas=schemas,
            registrations=registrations,
            enabled_names=enabled_names,
            selected_names=selected_names,
            display_locale=display_locale,
            context_claims=(),
            reserved_budget=budget,
            planning_hook=planning_hook,
            plan=plan,
            state=state,
            budget=budget,
            output_budget=output_budget,
            planning_available=planning_available,
            signal=signal,
            resume_checkpoint=checkpoint,
        )


    async def _settle_runtime_result(
        self,
        *,
        request: AgentRunRequest,
        options: AgentCoreRunOptions,
        controller: AgentRunController,
        result: AgentRuntimeResult | None,
        output_budget,
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
            await self._require_root_agent_tree_quiescent(controller.run_id)
            await self._require_parent_delivery_settled(controller.run_id)
            final_response = result.final_response
            validated_result: str | None = None
            policy = options.resolved_response_transaction_policy
            if policy.mode is ResponseTransactionMode.VALIDATED_RESULT:
                validated_result = final_response
                final_response = ""
                if policy.public_presentation is PublicPresentationMode.MODEL_LIVE:
                    transaction = AgentResponseTransaction(
                        create_model_invocation_manager(
                            self._model_gateway,
                            output_observer=self._root_output_observer(),
                            operation_controller=self._operations,
                            runtime_limits=self._runtime_limits,
                            max_tool_argument_chars=(
                                self._tool_execution_limits.max_argument_chars
                            ),
                            budget_repository=self._repository,
                            evidence_validator=self._evidence_validator,
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
                            requested_reasoning_mode=options.reasoning_mode,
                            deadline_at_ms=options.deadline_at_ms,
                            context_window_tokens=(
                                _selected_context_window_tokens(request, options)
                            ),
                            safety_reserve_tokens=options.safety_reserve_tokens,
                            runtime_reserve_tokens=options.runtime_reserve_tokens,
                        ),
                        output_budget=output_budget,
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
        output_budget,
        planning_mode: PlanningMode = PlanningMode.REACTIVE,
        planning_available: bool = False,
        planning_required_tool_names: frozenset[str] = frozenset(),
        model_round_limit: int | None = None,
        signal: CancellationSignal | None,
        resume_checkpoint: AgentExecutionCheckpoint | None = None,
        operation_id: str | None = None,
    ) -> tuple[
        AgentRuntimeResult | AutoPlanningRequest | None,
        tuple[AgentEvent, ...],
    ]:
        host_arguments = {
            item.schema.name: thaw_json_mapping(item.host_planned_arguments)
            for item in registrations
            if item.host_planned_arguments is not None
        }
        gateway: ModelGateway = self._model_gateway
        manager = self._model_invocations
        if host_arguments:
            gateway = HostPlannedToolGateway(self._model_gateway, host_arguments)
        if host_arguments or self._run_tree_repository is not None:
            manager = create_model_invocation_manager(
                gateway,
                output_observer=self._root_output_observer(),
                operation_controller=self._operations,
                runtime_limits=self._runtime_limits,
                max_tool_argument_chars=(
                    self._tool_execution_limits.max_argument_chars
                ),
                budget_repository=self._repository,
                evidence_validator=self._evidence_validator,
            )
        runtime = AgentRuntime(
            model_gateway=gateway,
            tool_execution_gateway=(
                self._tool_executor
                if tuple(registrations) == self._registrations
                else CoreToolExecutor(
                    _CapturedToolCatalog(registrations),
                    self._approval_gateway,
                    self._tool_execution_limits,
                    self._tool_idempotency_gateway,
                    self._operations,
                )
            ),
            observer=None if operation_id is not None else controller,
            context_compressor=conversation_compactor,
            limits=self._runtime_limits,
            recovery_policy=self._recovery_policy,
            operation_controller=self._operations,
            output_observer=self._root_output_observer(),
            model_manager=manager,
        )
        result: AgentRuntimeResult | AutoPlanningRequest | None = None
        events: list[AgentEvent] = []

        def _enrich_checkpoint(checkpoint):
            return replace(checkpoint,
                execution_profile="planned" if plan is not None else planning_mode.value,
                planning_state=planning_hook.checkpoint_state() if planning_hook is not None else {},
            )

        async def save_checkpoint(checkpoint):
            saved = _enrich_checkpoint(checkpoint)
            await controller.save_execution_checkpoint(saved)
            return saved

        async def tool_checkpoint(checkpoint):
            await options.tool_checkpoint_handler(_enrich_checkpoint(checkpoint))

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
                response_transaction_mode=options.resolved_response_transaction_policy.mode,
                execution_state=state,
                run_id=controller.run_id,
                turn_id=options.turn_id,
                context_budget=budget,
                output_budget=output_budget,
                scope_tools_to_observer=plan is not None,
                publish_model_commentary=(
                    operation_id is None and options.agent_tree_parent_run_id is None
                ),
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
                planning_mode=planning_mode,
                planning_available=planning_available,
                planning_required_tool_names=planning_required_tool_names,
                model_round_limit=model_round_limit,
                tool_context_contracts={
                    item.schema.name: item.context_contract
                    for item in registrations
                },
                tool_argument_limits={
                    item.schema.name: item.max_argument_chars
                    for item in registrations
                    if item.max_argument_chars is not None
                },
                stage_context_projection_enabled=bool(
                    plan is not None and plan.task_spec is not None
                ),
                resume_checkpoint=resume_checkpoint,
                checkpoint_writer=(
                    save_checkpoint
                    if operation_id is None and (options.agent_tree_run_id is not None or options.checkpoint_handler is not None or options.tool_checkpoint_handler is not None)
                    else None
                ),
                checkpoint_handler=options.checkpoint_handler,
                tool_checkpoint_handler=tool_checkpoint if options.tool_checkpoint_handler is not None else None,
                tool_checkpoint_names=options.tool_checkpoint_names,
                signal=signal,
            )
            async with aclosing(stream) as updates:
                async for update in updates:
                    events.extend(sink.drain())
                    if isinstance(update, AgentEvent):
                        event = _bind_event_to_run(update, controller.run_id)
                        if operation_id is not None:
                            event = replace(event, payload={**event.payload, "executionScopeId": operation_id})
                        await self._publish_runtime_event(controller.run_id, event)
                        events.append(event)
                    elif isinstance(update, (AgentRuntimeResult, AutoPlanningRequest)):
                        result = update
                    else:  # pragma: no cover - closed RuntimeUpdate union
                        raise ContractViolationError(
                            "runtime returned an unknown update"
                        )
        except OperationCanceled:
            result = AgentRuntimeResult(
                run_id=controller.run_id,
                outcome=RuntimeOutcome.CANCELED,
                final_response="",
                model=request.model.model,
                round_count=0,
                error_code="request_canceled",
            )
        except ExecutionDeadlineExceeded as error:
            result = AgentRuntimeResult(
                run_id=controller.run_id,
                outcome=RuntimeOutcome.FAILED,
                final_response="",
                model=request.model.model,
                round_count=0,
                error_code=error.code,
            )
        except (UserInputRequired, ApprovalRequired):
            raise
        except ContextOverflowError as error:
            await _record_safe_exception(
                controller, stage="runtime", outcome="overflow", error=error,
            )
            result = AgentRuntimeResult(
                run_id=controller.run_id,
                outcome=RuntimeOutcome.FAILED,
                final_response="",
                model=request.model.model,
                round_count=0,
                error_code=error.reason_code,
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
    runtime_limits: RuntimeLimits,
    max_tool_argument_chars: int,
    evidence_validator: ModelInputEvidenceValidator | None,
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
    if operation_controller is not None and processor is not None:
        operations = operation_controller.with_output(processor)
    invocations = create_model_invocation_manager(
        model_gateway,
        output_observer=processor,
        operation_controller=operations,
        runtime_limits=runtime_limits,
        max_tool_argument_chars=max_tool_argument_chars,
        budget_repository=repository,
        evidence_validator=evidence_validator,
    )
    return processor, repository, operations, invocations


def _resolve_run_deadline(
    options: AgentCoreRunOptions,
    limits: RuntimeLimits,
) -> AgentCoreRunOptions:
    if (
        options.deadline_at_ms is not None
        or options.durable_continuation is not None
        or limits.root_run_timeout_ms is None
    ):
        return options
    return replace(
        options,
        deadline_at_ms=int(time() * 1000) + limits.root_run_timeout_ms,
    )


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


def _preset_fingerprint_for_composition(
    preset_id: str,
    revision: str,
    composition: Mapping[str, Any],
) -> str:
    encoded = json.dumps(
        {
            "id": preset_id,
            "revision": revision,
            "composition": composition,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()
