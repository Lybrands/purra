"""Agent-tree orchestration for AgentCore.

Extracted from the orchestrator: root binding, the tree run executor,
capability restriction, lease binding, and root quiescence checks.
"""

from __future__ import annotations

from dataclasses import dataclass

from dataclasses import (
    replace,
)
from hashlib import (
    sha256,
)
from purra.agent_execution_checkpoint import (
    AgentExecutionCheckpoint,
)
from purra.agent_tree import (
    AgentCapabilityGrant,
    AgentNode,
    AgentTreeRun,
    AgentTreeRunStatus,
    BeginRootAgentCommand,
    ContextCheckpoint,
    RunTreeRepository,
)
from purra.agent_tree.lease import (
    bind_agent_run_lease,
)
from purra.agent_tree_execution import (
    AgentTreeExecutionResult,
    RunCommandService,
)
from purra.agent_tree_policy import (
    AgentTreePolicy,
)
from purra.agent_tree_tool import (
    AGENT_TREE_TOOL_NAMES,
    AgentToolContext,
    build_agent_tree_tools,
)
from purra.cancellation import (
    OperationCanceled,
    await_with_cancellation,
)
from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    MessageOrigin,
    MessageRole,
    PlanningMode,
    RunStatus,
)
from purra.engine.agent_context import (
    AgentConversationLoader,
    child_run_options,
    task_message,
)
from purra.engine.agent_result_delivery import (
    AgentResultDelivery,
)
from purra.engine.options import (
    AgentCoreRunOptions,
)
from purra.engine.run_budget import (
    _selected_context_window_tokens,
)
from purra.engine.tool_catalog import (
    AugmentedToolCatalog,
)
from purra.errors import (
    ContractViolationError,
)
from purra.events import (
    AgentEvent,
    CoreEventType,
)
from purra.json_values import (
    thaw_json_mapping,
)
from purra.normalization import (
    required_text,
)
from purra.ports import (
    CancellationSignal,
    RunCommit,
    ToolCatalog,
    ToolRegistration,
)
from purra.run_controller import (
    AgentRunController,
)
from purra.run_state import (
    RunStateMachine,
)
from uuid import (
    uuid4,
)
import asyncio



@dataclass(frozen=True, slots=True)
class _AgentTreeRootBinding:
    request: AgentRunRequest
    options: AgentCoreRunOptions


class _AgentCoreTreeRunExecutor:
    """Route Child Runs back through this AgentCore's normal supervisor."""

    def __init__(self, core: "AgentCore") -> None:
        self._core = core

    async def execute(
        self,
        run: AgentTreeRun,
        agent: AgentNode,
        checkpoint: ContextCheckpoint | None,
        signal: CancellationSignal | None = None,
    ) -> AgentTreeExecutionResult:
        binding = self._core._agent_tree_roots.get(run.root_run_id)
        if binding is None:
            return AgentTreeExecutionResult(
                status=AgentTreeRunStatus.FAILED,
                error_code="agent_tree_root_not_bound",
            )
        with bind_agent_run_lease(
            run.run_id,
            required_text(run.lease_owner_id, "Agent Run lease owner"),
            run.lease_epoch,
        ):
            reconciled = await self._reconcile_canonical_run(run)
            if isinstance(reconciled, AgentTreeExecutionResult):
                return reconciled
            resume_checkpoint = reconciled

        history = await AgentConversationLoader(
            self._core._run_tree_repository, self._core._output_repository,
        ).load(run, agent)
        child_request = replace(
            binding.request,
            messages=(
                AgentMessage(
                    role=MessageRole.SYSTEM,
                    content=agent.instruction,
                    origin=MessageOrigin.MODEL,
                    attributes={
                        "agentId": agent.agent_id,
                        "parentAgentId": agent.parent_agent_id or "",
                    },
                ),
                *history,
                task_message(run),
            ),
            tools_enabled=bool(
                agent.capability_grant.allowed_tools
                or agent.capability_grant.can_spawn_agents
            ),
            # Child Runs execute the objective delegated by their parent. They
            # do not open a second planning protocol inside that bounded task.
            planning_mode=PlanningMode.REACTIVE,
            metadata={
                **thaw_json_mapping(binding.request.metadata),
                "agentId": agent.agent_id,
                "rootRunId": run.root_run_id,
                "parentRunId": run.parent_run_id or "",
                "previousRunId": run.previous_run_id or "",
                "contextVersion": agent.context_version,
                "contextCheckpointId": (
                    checkpoint.checkpoint_id if checkpoint is not None else ""
                ),
                "contextContentRef": (
                    checkpoint.content_ref if checkpoint is not None else ""
                ),
            },
        )
        child_options = child_run_options(binding.options, run, agent, resume_checkpoint)
        with bind_agent_run_lease(
            run.run_id,
            required_text(run.lease_owner_id, "Agent Run lease owner"),
            run.lease_epoch,
        ):
            handle = await self._core.submit(child_request, options=child_options)
            try:
                result = await await_with_cancellation(handle.wait(), signal)
            except OperationCanceled:
                await handle.cancel("ancestor_run_canceled")
                return AgentTreeExecutionResult(
                    status=AgentTreeRunStatus.CANCELED,
                    error_code="agent_run_canceled",
                )
            except asyncio.CancelledError:
                await handle.cancel("ancestor_run_canceled")
                raise
        if result.status is RunStatus.DONE:
            if self._core._output_repository is None:
                raise ContractViolationError(
                    "Child Run result requires canonical output repository"
                )
            content = await self._core._output_repository.load_validated_result(
                run.run_id
            )
            return AgentTreeExecutionResult(
                status=AgentTreeRunStatus.DONE,
                result={"content": content},
                content_ref=f"run://{run.run_id}/validated-result",
                fingerprint=sha256(content.encode("utf-8")).hexdigest(),
            )
        if result.status is RunStatus.CANCELED:
            return AgentTreeExecutionResult(
                status=AgentTreeRunStatus.CANCELED,
                error_code=result.error or "agent_run_canceled",
            )
        return AgentTreeExecutionResult(
            status=AgentTreeRunStatus.FAILED,
            error_code=result.error or "agent_run_failed",
        )

    async def _reconcile_canonical_run(
        self,
        run: AgentTreeRun,
    ) -> AgentTreeExecutionResult | AgentExecutionCheckpoint | None:
        """Resolve a Tree/Run crash seam without replaying Provider work."""

        try:
            snapshot = await self._core._repository.get(run.run_id)
        except ContractViolationError as error:
            if error.code == "run_not_found":
                return None
            raise
        if snapshot.status is RunStatus.DONE:
            if self._core._output_repository is None:
                raise ContractViolationError(
                    "Child Run result requires canonical output repository"
                )
            content = await self._core._output_repository.load_validated_result(
                run.run_id
            )
            return AgentTreeExecutionResult(
                status=AgentTreeRunStatus.DONE,
                result={"content": content},
                content_ref=f"run://{run.run_id}/validated-result",
                fingerprint=sha256(content.encode("utf-8")).hexdigest(),
            )
        if snapshot.status is RunStatus.CANCELED:
            return AgentTreeExecutionResult(
                status=AgentTreeRunStatus.CANCELED,
                error_code=snapshot.error or "agent_run_canceled",
            )
        if snapshot.status in {RunStatus.FAILED, RunStatus.BLOCKED}:
            return AgentTreeExecutionResult(
                status=AgentTreeRunStatus.FAILED,
                error_code=snapshot.error or "agent_run_failed",
            )
        if snapshot.execution_checkpoint is not None:
            return snapshot.execution_checkpoint

        # A canonical running row proves that execution began, but the current
        # snapshot has no model/tool cursor. Replay could duplicate effects, so
        # close both authorities with one stable fail-stop result.
        error_code = "agent_run_resume_checkpoint_missing"
        transition = RunStateMachine.fail(snapshot, error_code)
        await self._core._repository.commit(
            run.run_id,
            RunCommit(
                step_updates=transition.step_updates,
                terminal_status=RunStatus.FAILED,
                error=error_code,
                events=(AgentEvent(
                    type=CoreEventType.RUN_FAILED,
                    run_id=run.run_id,
                    payload={
                        "status": RunStatus.FAILED.value,
                        "error": error_code,
                    },
                ),),
            ),
        )
        return AgentTreeExecutionResult(
            status=AgentTreeRunStatus.FAILED,
            error_code=error_code,
        )


def _configure_agent_tree_capability(
    core,
    *,
    base_tool_catalog: ToolCatalog,
    policy: AgentTreePolicy | None,
    run_tree_repository: RunTreeRepository | None,
    root_agent_id: str | None,
    agent_capability_grant: AgentCapabilityGrant | None,
) -> None:
    if run_tree_repository is not None and not isinstance(
        run_tree_repository,
        RunTreeRepository,
    ):
        raise TypeError("run_tree_repository must implement RunTreeRepository")
    if agent_capability_grant is not None and not isinstance(
        agent_capability_grant,
        AgentCapabilityGrant,
    ):
        raise TypeError("agent_capability_grant is invalid")
    if run_tree_repository is not None and policy is None:
        raise ValueError(
            "Agent tree execution requires an AgentPreset with "
            "AgentTreePolicy"
        )
    if policy is not None and run_tree_repository is None:
        raise ValueError("AgentTreePolicy requires a RunTreeRepository")
    core._run_tree_repository = run_tree_repository
    core._root_agent_id = str(
        root_agent_id or f"root-agent-{uuid4().hex}"
    ).strip()
    if not core._root_agent_id:
        raise ValueError("root_agent_id must be non-empty")
    core._configured_agent_grant = agent_capability_grant
    core._agent_tree_child_allowed_tools: tuple[str, ...] = ()
    core._agent_tree_roots: dict[str, _AgentTreeRootBinding] = {}
    core._result_delivery = AgentResultDelivery(
        tree=run_tree_repository, output_repository=core._output_repository,
        output_processor=core._output_processor, model_invocations=core._model_invocations,
        bindings=core._agent_tree_roots, policy=policy, context_window=_selected_context_window_tokens,
    )
    core._run_commands: RunCommandService | None = None
    if run_tree_repository is not None:
        if core._output_processor is None or core._output_repository is None:
            raise ValueError(
                "Agent tree execution requires canonical output infrastructure"
            )
        assert core._run_supervisor is not None
        core._run_supervisor.configure_agent_tree(
            run_tree_repository,
            _AgentCoreTreeRunExecutor(core),
            deliver_results=(core.report_agent_results
                if policy is not None and policy.result_presentation_instruction is not None
                else None),
        )
        core._run_commands = RunCommandService(
            run_tree_repository,
            core._run_supervisor,
        )
        readable_tools = tuple(
            registration.schema.name
            for registration in base_tool_catalog.registrations()
            if registration.policy.mode.value == "read"
        )
        core._agent_tree_child_allowed_tools = readable_tools
        core._tool_catalog = AugmentedToolCatalog(
            base_tool_catalog,
            build_agent_tree_tools(AgentToolContext(core._run_commands, policy, readable_tools)),
        )
        return
    core._tool_catalog = base_tool_catalog


async def _bind_agent_tree_run(
    core,
    request: AgentRunRequest,
    options: AgentCoreRunOptions,
    controller: AgentRunController,
) -> tuple[bool, int | None]:
    repository = core._run_tree_repository
    if repository is None:
        return False, None
    if options.agent_tree_run_id is None:
        grant = core._configured_agent_grant or core._root_agent_grant(
            request
        )
        tree_run = await repository.get_run(controller.run_id) if options.agent_execution_checkpoint is not None else await repository.begin_root(BeginRootAgentCommand(
            run_id=controller.run_id or "",
            agent_id=core._root_agent_id,
            name="root",
            title="Root Agent",
            instruction="Own the root request.",
            objective=request.latest_user_text() or "Run the request.",
            capability_grant=grant,
            idempotency_key=f"begin:{controller.run_id}",
        ))
        core._agent_tree_roots[tree_run.root_run_id] = _AgentTreeRootBinding(
            request=request,
            options=options,
        )
        root_owner = True
    else:
        tree_run = await repository.get_run(options.agent_tree_run_id)
        if (
            tree_run.run_id != controller.run_id
            or tree_run.status is not AgentTreeRunStatus.RUNNING
        ):
            raise ContractViolationError(
                "Child Run identity or state does not match its tree claim",
                code="run_identity_conflict",
            )
        root_owner = False
    agent = await repository.get_agent(tree_run.agent_id)
    return root_owner, agent.context_version


async def _settle_root_agent_tree_run(
    core,
    snapshot,
    expected_context_version: int | None,
) -> None:
    repository = core._run_tree_repository
    if repository is None or expected_context_version is None:
        raise ContractViolationError("Root Agent tree settlement is unbound")
    try:
        if snapshot.status is RunStatus.DONE:
            await repository.complete_run(
                snapshot.run_id,
                expected_context_version=expected_context_version,
                result={"content": snapshot.final_response},
                content_ref=f"run://{snapshot.run_id}/final",
                fingerprint=sha256(
                    snapshot.final_response.encode("utf-8")
                ).hexdigest(),
            )
        elif snapshot.status is RunStatus.CANCELED:
            await repository.cancel_subtree(snapshot.run_id)
        else:
            await repository.fail_run(
                snapshot.run_id,
                snapshot.error or "root_run_failed",
            )
    finally:
        core._agent_tree_roots.pop(snapshot.run_id, None)


def _restrict_agent_capabilities(
    request: AgentRunRequest,
    grant: AgentCapabilityGrant | None,
    registrations: tuple[ToolRegistration, ...],
    enabled_names: frozenset[str],
) -> tuple[tuple[ToolRegistration, ...], frozenset[str]]:
    if grant is None:
        return registrations, enabled_names
    if request.model.model not in grant.allowed_models:
        raise ContractViolationError(
            "Agent Run model is outside its capability grant",
            code="agent_capability_escalation",
        )
    allowed_names = set(grant.allowed_tools)
    if grant.can_spawn_agents:
        allowed_names.update(AGENT_TREE_TOOL_NAMES)
    return (
        tuple(
            registration
            for registration in registrations
            if registration.schema.name in allowed_names
        ),
        frozenset(enabled_names) & allowed_names,
    )


def _bind_agent_tree_lease(
    core,
    registrations: tuple[ToolRegistration, ...],
    options: AgentCoreRunOptions,
) -> tuple[ToolRegistration, ...]:
    if (
        options.agent_tree_run_id is None
        or core._run_commands is None
        or core._agent_tree_policy is None
    ):
        return registrations
    tools = {item.schema.name: item for item in build_agent_tree_tools(AgentToolContext(
        core._run_commands, core._agent_tree_policy, core._agent_tree_child_allowed_tools,
        options.agent_tree_lease_owner_id, options.agent_tree_lease_epoch,
    ))}
    return tuple(tools.get(item.schema.name, item) for item in registrations)


async def _require_root_agent_tree_quiescent(
    core,
    run_id: str | None,
) -> None:
    repository = core._run_tree_repository
    if (
        repository is None
        or run_id is None
        or run_id not in core._agent_tree_roots
    ):
        return
    if core._run_commands is not None:
        core._run_commands.results.require_received(run_id)
    descendants = await repository.list_descendants(run_id)
    if any(
        run.status in {
            AgentTreeRunStatus.QUEUED,
            AgentTreeRunStatus.RUNNING,
            AgentTreeRunStatus.WAITING,
        }
        for run in descendants
    ):
        raise ContractViolationError(
            "Root Run has unfinished descendants",
            code="root_run_not_quiescent",
        )
