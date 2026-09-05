"""Run one Agent exclusively from an installed PurrA distribution."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from importlib.metadata import version
from pathlib import Path

import purra
from purra.api import (
    AgentCapabilityGrant,
    AgentCore,
    AgentPlanner,
    PlanningStreamParser,
    PLANNING_STREAM_SCHEMA,
    current_planning_context,
    AgentExecutionCheckpoint,
    AgentPreset,
    AgentTreeExecutionResult,
    AgentTreeRunStatus,
    AgentTreeRunSupervisor,
    BeginRootAgentCommand,
    ChildAgentSpec,
    ContinueAgentCommand,
    InMemoryAgentAdapters,
    InMemoryRunTreeRepository,
    RunCommandService,
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
    ArtifactStatus,
)
from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    DomainContext,
    ExecutionState,
    MessageRole,
    ModelCompletion,
    ModelFinishReason,
    ModelRequest,
    ModelStream,
    ModelStreamActivity,
    ModelStreamActivityKind,
    ModelStreamActivitySupport,
    ModelStreamChunk,
    RuntimeLimits,
    PlanningConstraints,
    PlanningMode,
    ToolPlanningRequirement,
    ModelTransportDiagnostics,
    RunStatus,
)
from purra.model_protocol import generic_capability_snapshot
from purra.retrieval import RetrievalHit, RetrieverTool
from purra.tools import InMemoryToolCatalog


async def _chunks():
    yield ModelStreamActivity(ModelStreamActivityKind.WORKING)
    yield ModelStreamChunk(
        content_delta="installed PurrA is runnable",
        finish_reason=ModelFinishReason.STOP,
    )


class _Gateway:
    async def stream(self, messages, invocation, signal=None):
        planning = any("planning component" in message.content for message in messages)
        async def planned():
            yield ModelStreamChunk(content_delta=json.dumps({"v": 1, "type": "progress", "text": "I will check the scope."}) + "\n")
            yield ModelStreamChunk(content_delta=json.dumps({"v": 1, "type": "plan", "plan": {"needsTodos": False, "reason": "PRIVATE_PLAN"}}) + "\n", finish_reason=ModelFinishReason.STOP)
        return ModelStream(
            chunks=planned() if planning else _chunks(),
            model="smoke-model",
            applied_generation_limit=invocation.output_budget.max_generation_tokens,
            activity_support=ModelStreamActivitySupport.WORKING,
        )

    async def complete(self, messages, invocation, signal=None):
        del messages, signal
        return ModelCompletion(
            message=AgentMessage(
                role=MessageRole.ASSISTANT,
                content="installed PurrA is runnable",
            ),
            model="smoke-model",
            applied_generation_limit=invocation.output_budget.max_generation_tokens,
            finish_reason=ModelFinishReason.STOP,
        )


class _InstalledRetriever:
    async def retrieve(self, request, signal=None):
        del signal
        return (RetrievalHit(
            id="installed-hit",
            content=f"installed retrieval: {request.query}",
            source="installed-smoke",
            version=1,
        ),)


async def _run() -> None:
    package_path = Path(purra.__file__).resolve()
    assert "site-packages" in package_path.parts, package_path
    assert version("purra") == "0.5.0"
    assert PlanningMode.AUTO.value == "auto"
    assert ToolPlanningRequirement.REQUIRED.value == "required"

    retriever_tool = RetrieverTool(
        retriever=_InstalledRetriever(),
        name="searchInstalledKnowledge",
        description="Search installed knowledge.",
        scope={"namespace": "installed-smoke"},
    )
    assert InMemoryToolCatalog((retriever_tool.registration,)).names == {
        "searchInstalledKnowledge"
    }
    retrieval_result = await retriever_tool.registration.handler(
        ExecutionState(run_id="installed-retrieval-run"),
        {"query": "ready"},
    )
    assert json.loads(retrieval_result.content)["hits"][0] == {
        "id": "installed-hit",
        "content": "installed retrieval: ready",
        "source": "installed-smoke",
        "version": 1,
        "untrusted": True,
        "metadata": {},
    }

    adapters = InMemoryAgentAdapters()
    core = AgentCore(
        model_gateway=_Gateway(),
        run_repository=adapters.runs,
        output_repository=adapters.outputs,
        output_publisher=adapters.publisher,
        preset=AgentPreset(
            runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
            id="installed-smoke",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
        ),
    )
    request = AgentRunRequest(
        messages=(AgentMessage(
            role=MessageRole.USER,
            content="Prove the installed Agent can run.",
        ),),
        model=ModelRequest(
            provider="smoke",
            model="smoke-model",
            capability_snapshot=replace(
                generic_capability_snapshot(),
                profile_id="smoke:model",
                max_generation_tokens=1_024,
            ),
            max_generation_tokens=256,
        ),
        domain_context=DomainContext(namespace="smoke"),
        context_window=8_192,
    )
    try:
        handle = await core.submit(request)
        result = await handle.wait()
        events = await adapters.outputs.list_events(
            handle.run_id,
            after_sequence=0,
        )
    finally:
        await core.close()

    assert result.status is RunStatus.DONE
    assert result.final_response == "installed PurrA is runnable"
    snapshot = events[0].payload["agentPreset"]
    assert snapshot["snapshotVersion"] == 5
    assert snapshot["composition"]["agentTree"] == {
        "protocolVersion": 1,
        "enabled": False,
    }
    runtime_limits = snapshot["composition"]["runtimeLimits"]
    assert runtime_limits["providerActivityIdleTimeoutMs"] == 30_000
    assert runtime_limits["providerProgressIdleTimeoutMs"] == 60_000
    assert runtime_limits["providerInvocationTimeoutMs"] == 300_000
    execution_checkpoint = AgentExecutionCheckpoint(
        run_id="installed-checkpoint",
        next_round=2,
        round_limit=6,
        messages=(AgentMessage(
            role=MessageRole.USER,
            content="resume",
        ),),
        pending_tool_input_retries=(("call-invalid", "readThing"),),
    )
    assert AgentExecutionCheckpoint.from_mapping(
        execution_checkpoint.to_mapping()
    ) == execution_checkpoint

    assert PLANNING_STREAM_SCHEMA == "purra.planning-stream/v1"
    assert current_planning_context() is None
    assert all(value is None for value in ModelTransportDiagnostics().to_mapping().values())
    parser = PlanningStreamParser()
    parser.feed('{"v":1,"type":"plan","plan":{}}\n')
    assert parser.finish() == {}
    class Policy:
        def planning_constraints(self, request, capabilities): return PlanningConstraints(allow_model_only_fallback=False)
    planned_storage = InMemoryAgentAdapters()
    gateway = _Gateway()
    planned_core = AgentCore(model_gateway=gateway, planner=AgentPlanner(gateway), planning_policy=Policy(),
        run_repository=planned_storage.runs, output_repository=planned_storage.outputs,
        output_publisher=planned_storage.publisher, runtime_limits=RuntimeLimits(max_run_generation_tokens=None))
    try:
        planned_handle = await planned_core.submit(replace(
            request,
            context_window=32768,
            planning_mode=PlanningMode.PLANNED,
        ))
        public = [e async for e in planned_handle.subscribe()]
        assert (await planned_handle.wait()).status is RunStatus.DONE
        assert any(e.kind.value == "planning.progress" for e in public)
        assert "PRIVATE_PLAN" not in str(public)
        assert public == [e async for e in planned_handle.subscribe()]
    finally:
        await planned_core.close()

    tree = InMemoryRunTreeRepository()

    class _InstalledTreeExecutor:
        commands: RunCommandService | None = None
        parallel_active = 0
        parallel_peak = 0
        parallel_started = asyncio.Event()

        async def execute(self, run, agent, checkpoint, signal=None):
            del checkpoint, signal
            if agent.name in {"child", "peer"} and run.previous_run_id is None:
                self.parallel_active += 1
                self.parallel_peak = max(
                    self.parallel_peak,
                    self.parallel_active,
                )
                if self.parallel_active == 2:
                    self.parallel_started.set()
                await self.parallel_started.wait()
                await asyncio.sleep(0)
                self.parallel_active -= 1
            if agent.name == "child":
                assert self.commands is not None
                nested = await self.commands.spawn_agents(SpawnAgentsCommand(
                    parent_run_id=run.run_id,
                    idempotency_key="installed-nested",
                    lease_owner_id=run.lease_owner_id,
                    lease_epoch=run.lease_epoch,
                    children=(ChildAgentSpec(
                        name="grandchild",
                        title="Grandchild",
                        instruction="Finish.",
                        objective="Finish nested work.",
                    ),),
                ))
                assert (await self.commands.join_runs(
                    run.run_id,
                    (nested.items[0].run.run_id,),
                    lease_owner_id=run.lease_owner_id,
                    lease_epoch=run.lease_epoch,
                )).state == "ready"
            return AgentTreeExecutionResult(
                status=AgentTreeRunStatus.DONE,
                result={"agent": agent.name},
                content_ref=f"memory://{run.run_id}",
                fingerprint=f"fingerprint:{run.run_id}",
            )

    tree_executor = _InstalledTreeExecutor()
    tree_supervisor = AgentTreeRunSupervisor(
        repository=tree,
        executor=tree_executor,
    )
    tree_commands = RunCommandService(tree, tree_supervisor)
    tree_executor.commands = tree_commands
    tree_root = await tree_commands.begin_root(BeginRootAgentCommand(
        run_id="installed-tree-root",
        agent_id="installed-tree-agent",
        name="root",
        title="Root",
        instruction="Own the smoke test.",
        objective="Run two levels.",
        capability_grant=AgentCapabilityGrant(
            can_spawn_agents=True,
            max_parallel_runs=2,
        ),
        idempotency_key="installed-tree-begin",
    ))
    tree_child = await tree_commands.spawn_agents(SpawnAgentsCommand(
        parent_run_id=tree_root.run_id,
        idempotency_key="installed-tree-spawn",
        children=(
            ChildAgentSpec(
                name="child",
                title="Child",
                instruction="Delegate once.",
                objective="Run child work.",
            ),
            ChildAgentSpec(
                name="peer",
                title="Peer",
                instruction="Run beside Child.",
                objective="Prove parallel execution.",
            ),
        ),
    ))
    assert (await tree_commands.join_runs(
        tree_root.run_id,
        tuple(item.run.run_id for item in tree_child.items),
    )).state == "ready"
    assert tree_executor.parallel_peak == 2
    continued = await tree_commands.continue_agent(ContinueAgentCommand(
        requester_run_id=tree_root.run_id,
        idempotency_key="installed-tree-continue",
        agent_id=tree_child.items[0].agent.agent_id,
        expected_context_version=1,
        message="Run the installed Child Agent again.",
    ))
    assert (await tree_commands.join_runs(
        tree_root.run_id,
        (continued.run.run_id,),
    )).state == "ready"
    assert len(await tree.list_descendants(tree_root.run_id)) == 5

    lifecycle = ArtifactLifecycle(
        adapters.artifacts,
        id_factory=lambda: "installed-artifact-smoke",
    )
    artifact = await lifecycle.begin(ArtifactCreateCommand(
        namespace="smoke",
        kind="report",
        owner_id="installed-smoke",
        owner_ref=ArtifactOwnerRef("run", "installed-artifact-run"),
        created_by_run_id="installed-artifact-run",
        expected_item_count=1,
    ))
    grant = await ArtifactAccessController(adapters.artifact_claims).authorize(
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
            run_id=artifact.created_by_run_id,
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
    receipt = await lifecycle.append(ArtifactAppendCommand(
        artifact_id=artifact.id,
        expected_revision=artifact.revision,
        sequence=artifact.next_sequence,
        batch_id="installed-batch",
        idempotency_key="installed-append",
        items=({"installed": True},),
        write_lease=lease,
        coverage_keys=("installed",),
    ))
    finalized = await lifecycle.finalize(ArtifactFinalizeCommand(
        artifact_id=artifact.id,
        expected_revision=receipt.committed_revision,
        write_lease=lease,
        expected_item_count=1,
        expected_coverage_keys=("installed",),
        resource_ref="memory://installed-artifact",
    ))
    assert finalized.status is ArtifactStatus.FINALIZED


if __name__ == "__main__":
    asyncio.run(_run())
