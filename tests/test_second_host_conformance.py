"""A non-writing host proving that identity, context, and tools stay separate."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace

import pytest

from purra.api import (
    AgentExecutionCheckpoint,
    AgentCapabilityGrant,
    AgentComponentBinding,
    AgentCore,
    AgentCoreRunOptions,
    AgentPreset,
    BeginRootAgentCommand,
    DelegationPolicy,
    ChildAgentSpec,
    ContinueAgentCommand,
    DurableTaskContinuation,
    InMemoryAgentAdapters,
    PromptSection,
    SpawnAgentsCommand,
)
from purra.artifacts import (
    ArtifactAccessController,
    ArtifactAccessMode,
    ArtifactAccessRequest,
    ArtifactAppendCommand,
    ArtifactCreateCommand,
    ArtifactFinalizeCommand,
    ArtifactLifecycle,
    ArtifactMutationLease,
    ArtifactOwnerRef,
    ArtifactResumeCandidate,
)
from purra.agent_tree_lease import bind_agent_run_lease
from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    ContextBlock,
    ContextBundle,
    DelegationStatus,
    DomainContext,
    ExecutionPlan,
    ExecutionRecipe,
    ExecutionRecipeStep,
    MessageOrigin,
    MessageRole,
    ModelCompletion,
    ModelFinishReason,
    ModelRequest,
    ModelStream,
    ModelStreamChunk,
    RunCreateParams,
    RunStatus,
    StepExecutor,
    StepType,
    TaskSpec,
    TaskStep,
    ToolCallDelta,
    ToolCall,
    ToolHandlerResult,
    ToolPolicy,
    ToolSchema,
)
from purra.model_protocol import generic_capability_snapshot
from purra.long_tasks import (
    LongTaskCoordinator,
    LongTaskCreateCommand,
    LongTaskStatus,
    LongTaskUnitResult,
    LongTaskUnitSpec,
)
from purra.run_recovery import RunRecoverySnapshot
from purra.task_admission import (
    ExecutionMode,
    LongTaskDispatchReceipt,
    TaskAdmissionDecision,
)
from purra.errors import ContractViolationError
from purra.events import AgentEvent
from purra.ports import RunCommit, ToolRegistration
from purra.tools import InMemoryToolCatalog


OPERATIONS_IDENTITY = """\
You are PurrA, a calm incident-triage agent.
Separate observed facts from inference. Use available tools before causal claims.
Answer with current status, evidence, and the smallest safe next action.
"""


def _context_binding():
    return {
        "contextProvider": AgentComponentBinding("operations.context", "1"),
    }


class _IncidentDurableRunner:
    def __init__(self, adapters: InMemoryAgentAdapters) -> None:
        self._artifacts = ArtifactLifecycle(adapters.artifacts)
        self._access = ArtifactAccessController(adapters.artifact_claims)

    async def run_unit(self, task, unit, signal=None):
        del signal
        if unit.id == "collect":
            return LongTaskUnitResult(output_ref="memory://incident/evidence")
        artifact = await self._artifacts.begin(ArtifactCreateCommand(
            namespace="operations.incident",
            kind="incident_report",
            owner_id=task.owner_id,
            owner_ref=ArtifactOwnerRef("durable_task", task.id),
            created_by_run_id=task.created_by_run_id,
            expected_item_count=1,
        ))
        grant = await self._access.authorize(
            ArtifactResumeCandidate(
                artifact_id=artifact.id,
                namespace=artifact.namespace,
                kind=artifact.kind,
                owner_id=artifact.owner_id,
                owner_ref=artifact.owner_ref,
                created_by_run_id=artifact.created_by_run_id,
                status=artifact.status,
                revision=artifact.revision,
            ),
            ArtifactAccessRequest(
                artifact_id=artifact.id,
                run_id=task.created_by_run_id,
                mode=ArtifactAccessMode.WRITE,
                expected_revision=artifact.revision,
            ),
            lease_duration_ms=30_000,
        )
        assert grant.write_claim is not None
        lease = ArtifactMutationLease(
            run_id=grant.write_claim.run_id,
            claim_token=grant.write_claim.claim_token,
        )
        receipt = await self._artifacts.append(ArtifactAppendCommand(
            artifact_id=artifact.id,
            expected_revision=artifact.revision,
            sequence=artifact.next_sequence,
            batch_id="report",
            idempotency_key="incident-report-v1",
            items=({"status": "degraded", "failedHealthChecks": 1},),
            coverage_keys=("incident-summary",),
            write_lease=lease,
        ))
        finalized = await self._artifacts.finalize(ArtifactFinalizeCommand(
            artifact_id=artifact.id,
            expected_revision=receipt.committed_revision,
            write_lease=lease,
            expected_item_count=1,
            expected_coverage_keys=("incident-summary",),
            resource_ref=f"memory://artifact/{artifact.id}",
        ))
        return LongTaskUnitResult(
            output_ref=finalized.resource_ref or "",
            artifact_digest=finalized.coverage_digest,
            validation_receipt={"accepted": True},
        )


def test_preset_cannot_mix_with_low_level_agent_composition_arguments():
    adapters = InMemoryAgentAdapters()
    preset = AgentPreset(
        id="operations",
        revision="1",
        tool_catalog=InMemoryToolCatalog(()),
        context_provider=_IncidentContext(),
        component_bindings=_context_binding(),
    )
    with pytest.raises(ValueError, match="cannot be mixed"):
        AgentCore(
            model_gateway=_OperationsGateway(),
            run_repository=adapters.runs,
            preset=preset,
            context_provider=_IncidentContext(),
        )


@pytest.mark.asyncio
async def test_non_writing_host_runs_durable_task_into_finalized_artifact():
    adapters = InMemoryAgentAdapters()
    task = await adapters.long_tasks.create(
        "incident-task-42",
        LongTaskCreateCommand(
            namespace="operations.incident",
            kind="incident_report",
            owner_id="incident-42",
            created_by_run_id="incident-run-1",
            units=(
                LongTaskUnitSpec(id="collect", position=0),
                LongTaskUnitSpec(
                    id="report",
                    position=1,
                    dependencies=("collect",),
                ),
            ),
        ),
    )

    completed = await LongTaskCoordinator(
        adapters.long_tasks,
        worker_id="operations-worker",
        idle_poll_ms=1,
    ).run(task.id, _IncidentDurableRunner(adapters))
    artifact = await adapters.artifacts.find_for_owner(
        namespace="operations.incident",
        kind="incident_report",
        owner_id="incident-42",
        owner_ref=ArtifactOwnerRef("durable_task", task.id),
    )

    assert completed.status is LongTaskStatus.COMPLETED
    assert artifact is not None
    assert artifact.status.value == "finalized"
    assert artifact.resource_ref == f"memory://artifact/{artifact.id}"


@pytest.mark.asyncio
async def test_recovery_rejects_a_different_preset_composition_before_run_start():
    adapters = InMemoryAgentAdapters()
    catalog = InMemoryToolCatalog(())
    original = AgentPreset(
        id="operations",
        revision="1",
        tool_catalog=catalog,
        context_provider=_IncidentContext(),
        component_bindings=_context_binding(),
        prompt_sections=(PromptSection(name="identity", text="Original."),),
    )
    changed = AgentPreset(
        id="operations",
        revision="1",
        tool_catalog=catalog,
        context_provider=_IncidentContext(),
        component_bindings=_context_binding(),
        prompt_sections=(PromptSection(name="identity", text="Changed."),),
    )
    core = AgentCore(
        model_gateway=_OperationsGateway(),
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
        preset=changed,
    )
    try:
        with pytest.raises(ContractViolationError, match="different"):
            await core.submit(
                _request(),
                options=AgentCoreRunOptions(
                    agent_preset_snapshot=original.snapshot(_request()),
                ),
            )
    finally:
        await core.close()


@pytest.mark.asyncio
async def test_legacy_snapshot_continuation_fails_before_provider_invocation():
    plan = ExecutionPlan(
        title="Resume",
        task_spec=TaskSpec(goal="Resume safely"),
        steps=(TaskStep(
            id="resume",
            title="Resume",
            type=StepType.WRITE,
            executor=StepExecutor.MODEL,
        ),),
    )
    decision = TaskAdmissionDecision(
        mode=ExecutionMode.DURABLE,
        reason_code="resume",
        covered_step_ids=("resume",),
        execution_recipe=ExecutionRecipe(
            kind="operations.resume",
            steps=(ExecutionRecipeStep(
                id="resume",
                kind="model",
                plan_step_id="resume",
            ),),
        ),
    )
    continuation = DurableTaskContinuation(
        source=RunRecoverySnapshot(
            run_id="legacy-run",
            status=RunStatus.DONE,
            execution_plan=plan,
            agent_preset_snapshot={
                "id": "operations",
                "revision": "1",
                "fingerprint": "legacy",
                "composition": {},
            },
        ),
        continuation_command="resume-legacy",
        receipt=LongTaskDispatchReceipt(
            task_id="task-legacy",
            message="Resume.",
            admission=decision,
        ),
    )
    gateway = _OperationsGateway()
    adapters = InMemoryAgentAdapters()
    core = AgentCore(
        model_gateway=gateway,
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
        preset=AgentPreset(
            id="operations",
            revision="2",
            tool_catalog=InMemoryToolCatalog(()),
        ),
    )
    try:
        with pytest.raises(ContractViolationError) as captured:
            await core.submit(
                _request(),
                options=AgentCoreRunOptions(
                    durable_continuation=continuation,
                ),
            )
    finally:
        await core.close()

    assert captured.value.code == "agent_preset_snapshot_unsupported"
    assert gateway.rounds == []

    valid_snapshot = AgentPreset(
        id="operations",
        revision="2",
        tool_catalog=InMemoryToolCatalog(()),
    ).snapshot(_request()).to_mapping()
    loose_continuation = replace(
        continuation,
        source=replace(
            continuation.source,
            agent_preset_snapshot=valid_snapshot,
        ),
    )
    loose_core = AgentCore(
        model_gateway=gateway,
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
    )
    try:
        with pytest.raises(ContractViolationError) as loose_error:
            await loose_core.submit(
                _request(),
                options=AgentCoreRunOptions(
                    durable_continuation=loose_continuation,
                ),
            )
    finally:
        await loose_core.close()

    assert loose_error.value.code == "agent_preset_snapshot_unsupported"
    assert gateway.rounds == []


@pytest.mark.asyncio
async def test_continuation_restores_preset_authority_from_source_run_journal():
    class AnswerGateway:
        async def stream(self, messages, invocation, signal=None):
            del messages, invocation, signal

            async def chunks():
                yield ModelStreamChunk(
                    content_delta="done",
                    finish_reason=ModelFinishReason.STOP,
                )

            return ModelStream(chunks=chunks(), model="operations-model")

        async def complete(self, messages, invocation, signal=None):
            del messages, invocation, signal
            return ModelCompletion(
                message=AgentMessage(role=MessageRole.ASSISTANT, content="done"),
                model="operations-model",
                finish_reason=ModelFinishReason.STOP,
            )

    adapters = InMemoryAgentAdapters()
    catalog = InMemoryToolCatalog(())
    original = AgentPreset(
        id="operations",
        revision="1",
        tool_catalog=catalog,
        prompt_sections=(PromptSection(name="identity", text="Original."),),
    )
    source_core = AgentCore(
        model_gateway=AnswerGateway(),
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
        preset=original,
    )
    try:
        source = await (await source_core.submit(_request())).wait()
    finally:
        await source_core.close()

    plan = ExecutionPlan(
        title="Resume",
        task_spec=TaskSpec(goal="Resume safely"),
        steps=(TaskStep(
            id="resume",
            title="Resume",
            type=StepType.WRITE,
            executor=StepExecutor.MODEL,
        ),),
    )
    decision = TaskAdmissionDecision(
        mode=ExecutionMode.DURABLE,
        reason_code="resume",
        covered_step_ids=("resume",),
        execution_recipe=ExecutionRecipe(
            kind="operations.resume",
            steps=(ExecutionRecipeStep(
                id="resume",
                kind="model",
                plan_step_id="resume",
            ),),
        ),
    )
    continuation = DurableTaskContinuation(
        source=RunRecoverySnapshot(
            run_id=source.run_id,
            status=RunStatus.DONE,
            execution_plan=plan,
        ),
        continuation_command="resume-1",
        receipt=LongTaskDispatchReceipt(
            task_id="task-1",
            message="Resume.",
            admission=decision,
        ),
    )
    changed_core = AgentCore(
        model_gateway=AnswerGateway(),
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
        preset=AgentPreset(
            id="operations",
            revision="1",
            tool_catalog=catalog,
            prompt_sections=(PromptSection(name="identity", text="Changed."),),
        ),
    )
    try:
        with pytest.raises(ContractViolationError, match="different AgentPreset"):
            await changed_core.submit(
                _request(),
                options=AgentCoreRunOptions(
                    durable_continuation=continuation,
                ),
            )
    finally:
        await changed_core.close()


class _IncidentContext:
    def __init__(self) -> None:
        self.requests = []

    async def build_context(self, request, budget, signal=None):
        del budget, signal
        self.requests.append(request)
        return ContextBundle(blocks=(ContextBlock(
            name="incident-runbook",
            content=(
                "A degraded service remains online. Escalate only after two "
                "consecutive failed health checks."
            ),
            token_count=20,
            untrusted=False,
        ),))


class _OperationsGateway:
    def __init__(self) -> None:
        self.rounds = []

    async def stream(self, messages, invocation, signal=None):
        del signal
        self.rounds.append((tuple(messages), invocation))

        async def chunks():
            if len(self.rounds) == 1:
                yield ModelStreamChunk(
                    tool_call_deltas=(ToolCallDelta(
                        index=0,
                        id="call-status",
                        type="function",
                        name="readServiceStatus",
                        arguments_fragment='{"serviceId":"payments"}',
                    ),),
                    finish_reason=ModelFinishReason.TOOL_CALLS,
                )
                return
            yield ModelStreamChunk(
                content_delta=(
                    "Status: degraded. Evidence: health check 1/2 failed; "
                    "service remains online. Next action: run one more check."
                ),
                finish_reason=ModelFinishReason.STOP,
            )

        return ModelStream(chunks=chunks(), model="operations-model")

    async def complete(self, messages, invocation, signal=None):
        del messages, invocation, signal
        return ModelCompletion(
            message=AgentMessage(role="assistant", content="unused"),
            model="operations-model",
            finish_reason=ModelFinishReason.STOP,
        )


class _DelegationGateway:
    def __init__(self) -> None:
        self.rounds = []

    async def stream(self, messages, invocation, signal=None):
        del signal
        messages = tuple(messages)
        self.rounds.append((messages, invocation))
        is_reviewer = any(
            message.content == "You are an independent evidence reviewer."
            for message in messages
        )

        async def chunks():
            if is_reviewer:
                yield ModelStreamChunk(
                    content_delta="Review: the service is degraded, not down.",
                    finish_reason=ModelFinishReason.STOP,
                )
                return
            tool_result = next(
                (
                    message
                    for message in messages
                    if message.role is MessageRole.TOOL
                ),
                None,
            )
            if tool_result is None:
                yield ModelStreamChunk(
                    tool_call_deltas=(ToolCallDelta(
                        index=0,
                        id="call-delegate",
                        type="function",
                        name="delegateToAgents",
                        arguments_fragment=json.dumps({
                            "delegations": [{
                                "agentName": "incident-reviewer",
                                "title": "Evidence reviewer",
                                "instruction": (
                                    "You are an independent evidence reviewer."
                                ),
                                "objective": "Review the incident evidence.",
                            }],
                        }, separators=(",", ":")),
                    ),),
                    finish_reason=ModelFinishReason.TOOL_CALLS,
                )
                return
            yield ModelStreamChunk(
                content_delta="Status: degraded; independent review agrees.",
                finish_reason=ModelFinishReason.STOP,
            )

        return ModelStream(chunks=chunks(), model="operations-model")

    async def complete(self, messages, invocation, signal=None):
        del messages, invocation, signal
        return ModelCompletion(
            message=AgentMessage(role="assistant", content="unused"),
            model="operations-model",
            finish_reason=ModelFinishReason.STOP,
        )


class _RecursiveDelegationGateway:
    async def stream(self, messages, invocation, signal=None):
        del invocation, signal
        messages = tuple(messages)
        level_one = any(message.content == "LEVEL_ONE" for message in messages)
        level_two = any(message.content == "LEVEL_TWO" for message in messages)
        has_tool_result = any(
            message.role is MessageRole.TOOL for message in messages
        )

        async def chunks():
            if level_two:
                yield ModelStreamChunk(
                    content_delta="level-two done",
                    finish_reason=ModelFinishReason.STOP,
                )
                return
            if not has_tool_result:
                child = (
                    {
                        "agentName": "level-two",
                        "title": "Level two",
                        "instruction": "LEVEL_TWO",
                        "objective": "Finish level two.",
                    }
                    if level_one
                    else {
                        "agentName": "level-one",
                        "title": "Level one",
                        "instruction": "LEVEL_ONE",
                        "objective": "Finish level one.",
                    }
                )
                yield ModelStreamChunk(
                    tool_call_deltas=(ToolCallDelta(
                        index=0,
                        id=("spawn-level-two" if level_one else "spawn-level-one"),
                        type="function",
                        name="delegateToAgents",
                        arguments_fragment=json.dumps(
                            {"delegations": [child]},
                            separators=(",", ":"),
                        ),
                    ),),
                    finish_reason=ModelFinishReason.TOOL_CALLS,
                )
                return
            yield ModelStreamChunk(
                content_delta=("level-one done" if level_one else "root done"),
                finish_reason=ModelFinishReason.STOP,
            )

        return ModelStream(chunks=chunks(), model="operations-model")

    async def complete(self, messages, invocation, signal=None):
        del messages, invocation, signal
        return ModelCompletion(
            message=AgentMessage(role="assistant", content="unused"),
            model="operations-model",
            finish_reason=ModelFinishReason.STOP,
        )


def _request() -> AgentRunRequest:
    return AgentRunRequest(
        messages=(
            AgentMessage(
                role=MessageRole.USER,
                content="Is payments down, and should we escalate?",
            ),
        ),
        model=ModelRequest(
            provider="operations",
            model="operations-model",
            capability_snapshot=replace(
                generic_capability_snapshot(),
                profile_id="operations:model",
                max_output_tokens=1_024,
            ),
            options={"max_tokens": 512},
        ),
        domain_context=DomainContext(
            namespace="operations.incident",
            payload={"incidentId": "inc-42"},
        ),
        context_window=65_536,
        tools_enabled=True,
    )


@pytest.mark.asyncio
async def test_non_writing_host_keeps_identity_context_and_tool_authority_separate():
    observed_arguments = []

    async def read_status(state, arguments, signal=None):
        del state, signal
        observed_arguments.append(dict(arguments))
        return ToolHandlerResult(json.dumps({
            "serviceId": "payments",
            "status": "degraded",
            "failedHealthChecks": 1,
            "online": True,
        }, separators=(",", ":")))

    catalog = InMemoryToolCatalog((ToolRegistration(
        schema=ToolSchema(
            name="readServiceStatus",
            description="Read the current status of one service.",
            parameters={
                "type": "object",
                "properties": {"serviceId": {"type": "string"}},
                "required": ["serviceId"],
                "additionalProperties": False,
            },
        ),
        handler=read_status,
        policy=ToolPolicy(mode="read", title="Read service status"),
    ),))
    gateway = _OperationsGateway()
    adapters = InMemoryAgentAdapters()
    core = AgentCore(
        model_gateway=gateway,
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
        preset=AgentPreset(
            id="operations",
            revision="1",
            tool_catalog=catalog,
            context_provider=_IncidentContext(),
            component_bindings=_context_binding(),
            prompt_sections=(PromptSection(
                name="identity",
                order=-100,
                text=OPERATIONS_IDENTITY,
            ),),
        ),
    )
    try:
        handle = await core.submit(_request())
        result = await handle.wait()
        events = await adapters.outputs.list_events(
            handle.run_id,
            after_sequence=0,
        )
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert result.final_response.startswith("Status: degraded.")
    assert observed_arguments == [{"serviceId": "payments"}]
    assert len(gateway.rounds) == 3
    snapshot = events[0].payload["agentPreset"]
    assert snapshot["id"] == "operations"
    assert snapshot["revision"] == "1"
    assert snapshot["composition"]["promptSections"][0]["name"] == "identity"
    assert snapshot["composition"]["tools"][0]["name"] == "readServiceStatus"

    first_messages, first_invocation = gateway.rounds[0]
    assert first_messages[0].content == OPERATIONS_IDENTITY.strip()
    assert first_messages[0].origin is MessageOrigin.HOST_CONTEXT
    assert first_messages[1].role is MessageRole.DEVELOPER
    assert first_messages[1].origin is MessageOrigin.HOST_CONTEXT
    assert "incident-runbook" in first_messages[1].attributes["context_name"]
    assert first_messages[-1].role is MessageRole.USER
    assert tuple(tool.name for tool in first_invocation.tools) == (
        "readServiceStatus",
    )

    second_messages, _ = gateway.rounds[1]
    assert second_messages[0] == first_messages[0]
    tool_result = next(
        message
        for message in second_messages
        if message.role is MessageRole.TOOL
    )
    assert tool_result.origin is MessageOrigin.HOST_TOOL_RESULT
    assert json.loads(tool_result.content)["online"] is True

    final_messages, final_invocation = gateway.rounds[2]
    assert final_messages[0] == first_messages[0]
    assert not final_invocation.tools
    assert final_messages[-1].role is MessageRole.DEVELOPER
    assert "final user-facing answer" in final_messages[-1].content


@pytest.mark.asyncio
async def test_native_delegation_uses_model_defined_isolated_agent():
    gateway = _DelegationGateway()
    adapters = InMemoryAgentAdapters()
    incident_context = _IncidentContext()
    core = AgentCore(
        model_gateway=gateway,
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
        delegation_repository=adapters.delegations,
        tool_idempotency_gateway=adapters.idempotency,
        preset=AgentPreset(
            id="operations",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
            context_provider=incident_context,
            component_bindings=_context_binding(),
            delegation_policy=DelegationPolicy(),
            prompt_sections=(PromptSection(
                name="identity",
                text=OPERATIONS_IDENTITY,
            ),),
        ),
    )
    try:
        handle = await core.submit(_request())
        result = await handle.wait()
        delegations = await adapters.delegations.list_for_run(handle.run_id)
        events = await adapters.outputs.list_events(
            handle.run_id,
            after_sequence=0,
        )
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert result.final_response.startswith("Status: degraded")
    assert len(delegations) == 1
    assert delegations[0].status is DelegationStatus.DONE
    assert delegations[0].run_id == handle.run_id
    assert delegations[0].agent_name == "incident-reviewer"
    assert delegations[0].agent_title == "Evidence reviewer"
    assert delegations[0].agent_instruction == (
        "You are an independent evidence reviewer."
    )
    assert not hasattr(delegations[0], "child_run_id")
    assert all(event.run_id == handle.run_id for event in events)
    started_snapshot = events[0].payload["agentPreset"]
    assert started_snapshot["snapshotVersion"] == 4
    assert started_snapshot["composition"]["delegation"]["enabled"] is True
    assert [
        tool["name"] for tool in started_snapshot["composition"]["tools"]
    ] == ["delegateToAgents"]

    reviewer_rounds = [
        messages
        for messages, _invocation in gateway.rounds
        if any(
            message.content == "You are an independent evidence reviewer."
            for message in messages
        )
    ]
    assert reviewer_rounds
    reviewer_messages = reviewer_rounds[0]
    assert reviewer_messages[0].content == (
        "You are an independent evidence reviewer."
    )
    assert any(
        "incident-runbook" in message.attributes.get("context_name", "")
        for message in reviewer_messages
    )
    parent_text = "\n".join(message.content for message in reviewer_messages)
    assert "Is payments down" not in parent_text
    assert OPERATIONS_IDENTITY.strip() not in parent_text
    delegated_request = next(
        request
        for request in incident_context.requests
        if request.metadata.get("delegatedAgentName") == "incident-reviewer"
    )
    assert delegated_request.domain_context.namespace == "operations.incident"
    assert "incidentId" in delegated_request.domain_context.payload


@pytest.mark.asyncio
async def test_agent_tree_delegation_executes_a_canonical_child_run():
    gateway = _DelegationGateway()
    adapters = InMemoryAgentAdapters()
    tree = adapters.run_tree
    core = AgentCore(
        model_gateway=gateway,
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
        run_tree_repository=tree,
        root_agent_id="operations-root-agent",
        preset=AgentPreset(
            id="operations",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
            context_provider=_IncidentContext(),
            component_bindings=_context_binding(),
            delegation_policy=DelegationPolicy(),
            prompt_sections=(PromptSection(
                name="identity",
                text=OPERATIONS_IDENTITY,
            ),),
        ),
    )
    try:
        handle = await core.submit(_request())
        result = await handle.wait()
        descendants = await tree.list_descendants(handle.run_id)
        root_events = await adapters.outputs.list_events(
            handle.run_id,
            after_sequence=0,
        )
        child_events = await adapters.outputs.list_events(
            descendants[0].run_id,
            after_sequence=0,
        )
        journal = await adapters.outputs.list_root_events(
            handle.run_id,
            after_root_sequence=0,
        )
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert result.final_response.startswith("Status: degraded")
    assert len(descendants) == 1
    assert descendants[0].status.value == "done"
    assert descendants[0].run_id != handle.run_id
    assert descendants[0].parent_run_id == handle.run_id
    assert child_events[0].run_id == descendants[0].run_id
    assert [event.root_sequence for event in journal] == list(
        range(1, len(journal) + 1)
    )
    assert {event.root_run_id for event in journal} == {handle.run_id}
    assert all(event.agent_id and event.source_event_key for event in journal)
    assert all(event in journal for event in (*root_events, *child_events))
    assert root_events[0].payload["agentPreset"]["snapshotVersion"] == 5
    assert root_events[0].payload["agentPreset"]["composition"][
        "agentTree"
    ]["protocolVersion"] == 1


def test_agent_tree_rejects_the_legacy_delegation_write_authority():
    adapters = InMemoryAgentAdapters()
    with pytest.raises(ValueError, match="legacy delegation lifecycle"):
        AgentCore(
            model_gateway=_OperationsGateway(),
            run_repository=adapters.runs,
            output_repository=adapters.outputs,
            output_publisher=adapters.publisher,
            delegation_policy=DelegationPolicy(),
            delegation_repository=adapters.delegations,
            run_tree_repository=adapters.run_tree,
        )


@pytest.mark.asyncio
async def test_agent_core_host_commands_continue_a_canonical_child_agent():
    root_gate = asyncio.Event()

    class _HostCommandGateway:
        async def stream(self, messages, invocation, signal=None):
            del invocation, signal
            is_child = any(
                message.content == "Host child." for message in messages
            )

            async def chunks():
                if not is_child:
                    await root_gate.wait()
                yield ModelStreamChunk(
                    content_delta=(
                        "host child done" if is_child else "host root done"
                    ),
                    finish_reason=ModelFinishReason.STOP,
                )

            return ModelStream(chunks=chunks(), model="operations-model")

        async def complete(self, messages, invocation, signal=None):
            del invocation, signal
            if any(message.content == "Host child." for message in messages):
                content = "host child done"
            else:
                await root_gate.wait()
                content = "host root done"
            return ModelCompletion(
                message=AgentMessage(
                    role=MessageRole.ASSISTANT,
                    content=content,
                ),
                model="operations-model",
                finish_reason=ModelFinishReason.STOP,
            )

    adapters = InMemoryAgentAdapters()
    core = AgentCore(
        model_gateway=_HostCommandGateway(),
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
        run_tree_repository=adapters.run_tree,
        root_agent_id="host-command-root-agent",
        preset=AgentPreset(
            id="host-commands",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
            delegation_policy=DelegationPolicy(max_parallel=1),
        ),
    )
    try:
        handle = await core.submit(_request())
        child = (await core.spawn_agents(SpawnAgentsCommand(
            parent_run_id=handle.run_id,
            idempotency_key="host-spawn",
            children=(ChildAgentSpec(
                name="host-child",
                title="Host child",
                instruction="Host child.",
                objective="Run once.",
            ),),
        ))).items[0]
        assert (await core.join_agent_runs(
            handle.run_id,
            (child.run.run_id,),
        )).state == "ready"
        continued = await core.continue_agent(ContinueAgentCommand(
            requester_run_id=handle.run_id,
            idempotency_key="host-continue",
            agent_id=child.agent.agent_id,
            expected_context_version=1,
            message="Run again.",
        ))
        assert (await core.join_agent_runs(
            handle.run_id,
            (continued.run.run_id,),
        )).state == "ready"
        assert (
            await adapters.run_tree.get_agent(child.agent.agent_id)
        ).context_version == 2
        root_gate.set()
        result = await handle.wait()
    finally:
        root_gate.set()
        await core.close()

    assert result.status is RunStatus.DONE
    assert result.final_response == "host root done"


@pytest.mark.asyncio
async def test_root_completion_is_rejected_before_final_when_child_is_pending():
    root_gate = asyncio.Event()

    class _PendingChildGateway:
        async def stream(self, messages, invocation, signal=None):
            del messages, invocation, signal

            async def chunks():
                await root_gate.wait()
                yield ModelStreamChunk(
                    content_delta="must not become final",
                    finish_reason=ModelFinishReason.STOP,
                )

            return ModelStream(chunks=chunks(), model="operations-model")

        async def complete(self, messages, invocation, signal=None):
            del messages, invocation, signal
            await root_gate.wait()
            return ModelCompletion(
                message=AgentMessage(
                    role=MessageRole.ASSISTANT,
                    content="must not become final",
                ),
                model="operations-model",
                finish_reason=ModelFinishReason.STOP,
            )

    adapters = InMemoryAgentAdapters()
    core = AgentCore(
        model_gateway=_PendingChildGateway(),
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
        run_tree_repository=adapters.run_tree,
        root_agent_id="quiescence-root-agent",
        preset=AgentPreset(
            id="quiescence",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
            delegation_policy=DelegationPolicy(max_parallel=1),
        ),
    )
    try:
        handle = await core.submit(_request())
        child = (await core.spawn_agents(SpawnAgentsCommand(
            parent_run_id=handle.run_id,
            idempotency_key="pending-child",
            children=(ChildAgentSpec(
                name="pending",
                title="Pending",
                instruction="Remain queued.",
                objective="Prevent premature Root completion.",
            ),),
        ))).items[0]
        root_gate.set()
        result = await handle.wait()
        events = await adapters.outputs.list_events(
            handle.run_id,
            after_sequence=0,
        )
        root_tree_run = await adapters.run_tree.get_run(handle.run_id)
        child_tree_run = await adapters.run_tree.get_run(child.run.run_id)
    finally:
        root_gate.set()
        await core.close()

    assert result.status is RunStatus.FAILED
    assert result.error == "root_run_not_quiescent"
    assert root_tree_run.status.value == "failed"
    assert child_tree_run.status.value == "canceled"
    assert not any(
        event.payload.get("eventType") == "run.completed" for event in events
    )
    assert not any(
        event.channel.value == "final" and event.visibility.value == "public"
        for event in events
    )


@pytest.mark.asyncio
async def test_new_agent_core_rebinds_and_executes_a_committed_child_once():
    executions = 0
    now_ms = 100

    class _RecoveryGateway:
        async def stream(self, messages, invocation, signal=None):
            nonlocal executions
            del messages, invocation, signal
            executions += 1

            async def chunks():
                yield ModelStreamChunk(
                    content_delta="recovered child done",
                    finish_reason=ModelFinishReason.STOP,
                )

            return ModelStream(chunks=chunks(), model="operations-model")

        async def complete(self, messages, invocation, signal=None):
            del messages, invocation, signal
            return ModelCompletion(
                message=AgentMessage(
                    role=MessageRole.ASSISTANT,
                    content="recovered child done",
                ),
                model="operations-model",
                finish_reason=ModelFinishReason.STOP,
            )

    adapters = InMemoryAgentAdapters(agent_tree_clock_ms=lambda: now_ms)
    root, _ = await adapters.outputs.begin_run_lifecycle(
        RunCreateParams(
            session_id=None,
            prompt="recover",
            mode="agent",
            requested_run_id="recovered-root",
            agent_id="recovered-root-agent",
        ),
        AgentEvent(type="run.started"),
    )
    await adapters.run_tree.begin_root(BeginRootAgentCommand(
        run_id=root.run_id,
        agent_id="recovered-root-agent",
        name="root",
        title="Root",
        instruction="Own recovery.",
        objective="Recover the Child Run.",
        capability_grant=AgentCapabilityGrant(
            can_spawn_agents=True,
            max_parallel_runs=1,
            allowed_models=("operations-model",),
        ),
        idempotency_key="recovered-root-begin",
    ))
    child = (await adapters.run_tree.spawn_agents(SpawnAgentsCommand(
        parent_run_id=root.run_id,
        idempotency_key="committed-before-crash",
        children=(ChildAgentSpec(
            name="recovered-child",
            title="Recovered child",
            instruction="Recover safely.",
            objective="Execute exactly once.",
        ),),
    ))).items[0]
    core = AgentCore(
        model_gateway=_RecoveryGateway(),
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
        run_tree_repository=adapters.run_tree,
        root_agent_id="recovered-root-agent",
        preset=AgentPreset(
            id="recovery",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
            delegation_policy=DelegationPolicy(max_parallel=1),
        ),
    )
    original_complete = adapters.run_tree.complete_run
    crashed = False

    async def crash_after_canonical_commit(run_id, **options):
        nonlocal crashed
        if run_id == child.run.run_id and not crashed:
            crashed = True
            raise OSError("worker stopped before Tree terminal commit")
        return await original_complete(run_id, **options)

    adapters.run_tree.complete_run = crash_after_canonical_commit
    try:
        recovery_request = _request()
        with pytest.raises(OSError):
            await core.recover_agent_tree_root(
                root.run_id,
                recovery_request,
            )
        assert (await adapters.runs.get(child.run.run_id)).status is RunStatus.DONE
        assert (await adapters.run_tree.get_run(root.run_id)).status.value == "waiting"
        now_ms = 30_100
        adapters.run_tree.complete_run = original_complete
        aggregate = await core.recover_agent_tree_root(
            root.run_id,
            recovery_request,
        )
        replay = await core.recover_agent_tree_root(
            root.run_id,
            recovery_request,
        )
        continued = await core.continue_agent(ContinueAgentCommand(
            requester_run_id=root.run_id,
            idempotency_key="resume-without-cursor",
            agent_id=child.agent.agent_id,
            expected_context_version=1,
            message="This execution loses its in-flight cursor.",
        ))
        await adapters.run_tree.mark_waiting(root.run_id)
        abandoned = await adapters.run_tree.claim_run(
            continued.run.run_id,
            owner_id="crashed-worker",
            lease_duration_ms=10,
        )
        assert abandoned is not None
        with bind_agent_run_lease(
            abandoned.run_id,
            abandoned.lease_owner_id or "",
            abandoned.lease_epoch,
        ):
            await adapters.outputs.begin_run_lifecycle(
                RunCreateParams(
                    session_id=None,
                    prompt="interrupted",
                    mode="agent",
                    requested_run_id=abandoned.run_id,
                    root_run_id=root.run_id,
                    agent_id=abandoned.agent_id,
                    parent_run_id=abandoned.parent_run_id,
                    lease_owner_id=abandoned.lease_owner_id,
                    lease_epoch=abandoned.lease_epoch,
                ),
                AgentEvent(type="run.started"),
            )
        now_ms += 10
        gap = await core.recover_agent_tree_root(
            root.run_id,
            recovery_request,
        )
        resumable = await core.continue_agent(ContinueAgentCommand(
            requester_run_id=root.run_id,
            idempotency_key="resume-from-model-ready-checkpoint",
            agent_id=child.agent.agent_id,
            expected_context_version=1,
            message="Continue from the committed tool boundary.",
        ))
        await adapters.run_tree.mark_waiting(root.run_id)
        checkpointed = await adapters.run_tree.claim_run(
            resumable.run.run_id,
            owner_id="checkpointed-worker",
            lease_duration_ms=10,
        )
        assert checkpointed is not None
        child_canonical = await adapters.runs.get(child.run.run_id)
        with bind_agent_run_lease(
            checkpointed.run_id,
            checkpointed.lease_owner_id or "",
            checkpointed.lease_epoch,
        ):
            await adapters.outputs.begin_run_lifecycle(
                RunCreateParams(
                    session_id=None,
                    prompt="checkpointed",
                    mode="agent",
                    requested_run_id=checkpointed.run_id,
                    root_run_id=root.run_id,
                    agent_id=checkpointed.agent_id,
                    parent_run_id=checkpointed.parent_run_id,
                    lease_owner_id=checkpointed.lease_owner_id,
                    lease_epoch=checkpointed.lease_epoch,
                    agent_preset_snapshot=(
                        child_canonical.agent_preset_snapshot
                    ),
                ),
                AgentEvent(type="run.started"),
            )
            await adapters.runs.commit(
                checkpointed.run_id,
                RunCommit(execution_checkpoint=AgentExecutionCheckpoint(
                    run_id=checkpointed.run_id,
                    next_round=2,
                    round_limit=6,
                    messages=(
                        AgentMessage(
                            role=MessageRole.SYSTEM,
                            content="Recover safely.",
                        ),
                        AgentMessage(
                            role=MessageRole.USER,
                            content=(
                                "Continue from the committed tool boundary."
                            ),
                        ),
                        AgentMessage(
                            role=MessageRole.ASSISTANT,
                            content=None,
                            tool_calls=(ToolCall(
                                id="committed-call",
                                name="readDocs",
                                arguments_json="{}",
                            ),),
                        ),
                        AgentMessage(
                            role=MessageRole.TOOL,
                            content="committed result",
                            tool_call_id="committed-call",
                        ),
                    ),
                )),
            )
        now_ms += 10
        resumed = await core.recover_agent_tree_root(
            root.run_id,
            recovery_request,
        )
        with pytest.raises(ContractViolationError) as binding_error:
            await core.bind_agent_tree_root(
                root.run_id,
                replace(recovery_request, mode="different"),
            )
        assert binding_error.value.code == "run_identity_conflict"
    finally:
        adapters.run_tree.complete_run = original_complete
        await core.close()

    assert aggregate.state == "ready"
    assert replay == aggregate
    assert gap.state == "blocked"
    assert resumed.state == "blocked"
    assert executions == 2
    assert (await adapters.run_tree.get_run(child.run.run_id)).status.value == "done"
    canonical_gap = await adapters.runs.get(continued.run.run_id)
    tree_gap = await adapters.run_tree.get_run(continued.run.run_id)
    assert canonical_gap.status is RunStatus.FAILED
    assert canonical_gap.error == "agent_run_resume_checkpoint_missing"
    assert tree_gap.status.value == "failed"
    assert tree_gap.error_code == "agent_run_resume_checkpoint_missing"
    assert (
        await adapters.runs.get(resumable.run.run_id)
    ).status is RunStatus.DONE
    assert (
        await adapters.run_tree.get_run(resumable.run.run_id)
    ).status.value == "done"


@pytest.mark.asyncio
async def test_agent_tree_allows_bounded_recursive_child_runs():
    adapters = InMemoryAgentAdapters()
    tree = adapters.run_tree
    core = AgentCore(
        model_gateway=_RecursiveDelegationGateway(),
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
        run_tree_repository=tree,
        root_agent_id="recursive-root-agent",
        preset=AgentPreset(
            id="recursive",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
            context_provider=_IncidentContext(),
            component_bindings=_context_binding(),
            delegation_policy=DelegationPolicy(
                allow_recursive_delegation=True,
                max_depth=2,
                max_parallel=1,
            ),
        ),
    )
    try:
        handle = await core.submit(_request())
        result = await handle.wait()
        descendants = await tree.list_descendants(handle.run_id)
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert result.final_response == "root done"
    assert [run.status.value for run in descendants] == ["done", "done"]
    assert [
        (await tree.get_agent(run.agent_id)).depth for run in descendants
    ] == [1, 2]
    assert descendants[1].parent_run_id == descendants[0].run_id
    checkpoint = (
        await adapters.runs.get(descendants[0].run_id)
    ).execution_checkpoint
    assert checkpoint is not None
    assert checkpoint.phase == "model_ready"
    assert checkpoint.next_round == 2
    checkpoint_events = tuple(
        event
        for event in await adapters.outputs.list_root_events(
            handle.run_id,
            after_root_sequence=0,
        )
        if event.payload.get("eventType")
        == "agent.execution_checkpointed"
    )
    assert len(checkpoint_events) == 1
    assert checkpoint_events[0].visibility.value == "private"
    assert checkpoint_events[0].run_id == descendants[0].run_id
