import asyncio
import json
from dataclasses import replace
import pytest
from purra.api import (
    AgentComponentBinding,
    AgentCore,
    AgentPreset,
    ExecutionProfile,
    UserInputRequired,
)
from purra.contracts import (AgentMessage, AgentRunRequest, DomainContext, ModelRequest, ModelStream,
    ModelStreamChunk, ToolCallDelta, ModelTokenUsage, RuntimeLimits, PlanningResult, WorkPlan, WorkStep)
from purra.agent_tree_policy import AgentTreePolicy
from purra.model_protocol import generic_capability_snapshot
from purra.tools import InMemoryToolCatalog
from purra_sqlite import SqliteAgentAdapters
from purra_interaction import SqliteClarification

QUESTION = {"questions": [{"id": "detail", "prompt": "Choose a detail", "choices": ["yes", "no"], "allowFreeform": False}]}

def plan(ask=True, suffix=""):
    steps = [WorkStep(id="ask" + suffix, title="Ask", type="read", executor="tool", capability_names=("request_user_input",))] if ask else []
    steps.append(WorkStep(id="finish" + suffix, title="Finish", type="review", executor="model", depends_on=("ask" + suffix,) if ask else ()))
    return PlanningResult(kind="planned", work_plan=WorkPlan(title="Clarify and finish", steps=tuple(steps)))

class Planner:
    def __init__(self, ask=True): self.calls = 0; self.revisions = []; self.ask = ask
    async def create_plan(self, *args, **kwargs): self.calls += 1; return plan(self.ask)
    async def revise_plan(self, request, capabilities, turn, *args, **kwargs):
        self.revisions.append(turn.revision); return plan(False)

class Gateway:
    def __init__(self, script): self.calls = []; self.script = script
    async def complete(self, *args, **kwargs): raise AssertionError("stream expected")
    async def stream(self, messages, invocation, signal=None):
        self.calls.append(messages)
        result = self.script(messages, len(self.calls))
        async def chunks():
            if isinstance(result, tuple):
                name, args = result
                yield ModelStreamChunk(tool_call_deltas=(ToolCallDelta(index=0, id=f"call-{sum(len(m.tool_calls) for m in messages) + 1}", name=name, arguments_fragment=json.dumps(args)),), finish_reason="tool_calls", usage=ModelTokenUsage(input_tokens=2, generation_tokens=2))
            else: yield ModelStreamChunk(content_delta=result, finish_reason="stop", usage=ModelTokenUsage(input_tokens=2, generation_tokens=2))
        return ModelStream(
            chunks=chunks(),
            model="fixture",
            applied_generation_limit=invocation.output_budget.max_generation_tokens,
        )

def answered(messages): return any(isinstance(m.content, str) and "Answers to requested" in m.content for m in messages)

def compose(path, script, planner=None, tree=False, replan=False):
    storage = SqliteAgentAdapters(path, scope="modes")
    interaction = SqliteClarification(storage)
    if replan:
        original = interaction.registration.handler
        async def question(*args, **kwargs):
            return replace(await original(*args, **kwargs), planning_disposition="replan")
        interaction.registration = replace(interaction.registration, handler=question)
    gateway = Gateway(script)
    profile = ExecutionProfile(planner=planner)
    bindings = (
        {"planner": AgentComponentBinding("interaction.planner", "1")}
        if planner is not None
        else {}
    )
    core = AgentCore(model_gateway=gateway, run_repository=storage.runs, output_repository=storage.outputs,
        output_publisher=storage.publisher, execution_lease_store=storage.leases,
        run_tree_repository=storage.run_tree if tree else None,
        preset=AgentPreset(
            id="interaction-modes",
            revision="1",
            tool_catalog=InMemoryToolCatalog((interaction.registration,)),
            runtime_limits=RuntimeLimits(max_run_generation_tokens=1000),
            execution_profile=profile,
            component_bindings=bindings,
            agent_tree_policy=(
                AgentTreePolicy(allow_recursive_agents=True) if tree else None
            ),
        ))
    request = AgentRunRequest(messages=(AgentMessage("user", "Root task"),), model=ModelRequest("fixture", "fixture", replace(generic_capability_snapshot(), max_generation_tokens=32)),
        domain_context=DomainContext("fixture"), context_window=65536, tools_enabled=True)
    return storage, interaction, gateway, core, request

@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["auto", "planned", "promoted", "remaining"])
async def test_modes_restart_keep_plan_and_auto_activation(tmp_path, mode):
    def initial(messages, count):
        if mode == "promoted" and count == 1: return "request_plan", {}
        return "request_user_input", QUESTION
    planner = Planner(mode != "remaining") if mode != "auto" else None
    storage, interaction, gateway, core, request = compose(tmp_path / "modes.db", initial, planner)
    request = replace(request, planning_mode="planned" if mode == "planned" else "auto")
    handle = await interaction.submit(core, request)
    with pytest.raises(UserInputRequired) as paused: await handle.wait()
    checkpoint = (await storage.runs.get(handle.run_id)).execution_checkpoint
    assert checkpoint.execution_profile == ("planned" if mode in {"planned", "promoted"} else "auto")
    if mode in {"planned", "promoted"}:
        assert (await storage.runs.get(handle.run_id)).steps[0].status.value == "done"
    await core.close(); storage.close()
    def resumed(messages, count):
        assert answered(messages)
        if mode == "remaining" and count == 1: return "request_remaining_plan", {}
        return "Finished"
    planner = Planner(False) if planner else None
    storage, interaction, gateway, core, _ = compose(tmp_path / "modes.db", resumed, planner)
    try:
        await interaction.answer(paused.value.request_id, revision=1, key="answer", answers={"detail": "yes"})
        resumed_handle = await interaction.resume(core, paused.value.request_id)
        result = await resumed_handle.wait()
        assert result.status.value == "done", result
        assert resumed_handle.run_id == handle.run_id
        if planner: assert planner.calls == (1 if mode == "remaining" else 0)
    finally: await core.close(); storage.close()


def tree_script(messages, count):
    instruction = next((m.content for m in messages if m.role.value == "system" and m.attributes.get("agentId")), "root")
    delegated = any(m.role.value == "tool" and '"pendingRunIds"' in m.content for m in messages)
    if instruction in {"root", "middle"}:
        if not delegated:
            names = ["middle", "sibling"] if instruction == "root" else ["leaf"]
            return "delegateToAgents", {"children": [{"name": name, "title": name, "instruction": name, "objective": name} for name in names]}
        for m in messages:
            if m.role.value == "tool" and '"pendingRunIds"' in m.content: assert not json.loads(m.content)["pendingRunIds"]
        return "Tree finished"
    return "Child finished" if answered(messages) else ("request_user_input", QUESTION)

@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_nested_tree_questions_restart_and_cancel(tmp_path, cancel):
    path = tmp_path / "tree.db"
    storage, interaction, gateway, core, request = compose(path, tree_script, tree=True)
    handle = await interaction.submit(core, request)
    with pytest.raises(UserInputRequired): await asyncio.wait_for(handle.wait(), 8)
    questions = await interaction.list_waiting()
    assert len(questions) == 2
    descendants = await storage.run_tree.list_descendants(handle.run_id)
    assert len(descendants) == 3 and all(run.status.value == "waiting" and run.lease_owner_id is None for run in descendants)
    await core.close(); storage.close()
    storage, interaction, gateway, core, _ = compose(path, tree_script, tree=True)
    try:
        if cancel:
            assert await interaction.cancel(questions[0]["id"])
            assert (await storage.runs.get(handle.run_id)).status.value == "canceled"
            assert all(run.status.value == "canceled" for run in await storage.run_tree.list_descendants(handle.run_id))
            assert not await interaction.list_waiting()
        else:
            for q in questions: await interaction.answer(q["id"], revision=1, key="answer", answers={"detail": "yes"})
            resumed = await interaction.resume(core, questions[0]["id"])
            result = await asyncio.wait_for(resumed.wait(), 8)
            assert result.status.value == "done", result
            assert resumed.run_id == handle.run_id
            assert all(run.status.value == "done" for run in await storage.run_tree.list_descendants(handle.run_id))
            assert not await interaction.list_pending()
    finally: await core.close(); storage.close()


@pytest.mark.asyncio
async def test_replanning_revision_survives_two_input_waits(tmp_path):
    class RevisingPlanner(Planner):
        async def revise_plan(self, request, capabilities, turn, *args, **kwargs):
            self.revisions.append(turn.revision)
            return plan(turn.revision == 1, str(turn.revision))
    def script(messages, count):
        answers = sum("Answers to requested" in m.content for m in messages)
        return ("request_user_input", QUESTION) if answers < 2 else "Finished"
    path = tmp_path / "revisions.db"
    planner = RevisingPlanner()
    storage, interaction, gateway, core, request = compose(path, script, planner, replan=True)
    request = replace(request, planning_mode="planned")
    handle = await interaction.submit(core, request)
    for revision in (0, 1):
        with pytest.raises(UserInputRequired) as pause: await handle.wait()
        checkpoint = (await storage.runs.get(handle.run_id)).execution_checkpoint
        assert checkpoint.dynamic_replan_pending and checkpoint.planning_state["revision"] == revision
        assert planner.revisions == ([] if revision == 0 else [1])
        await core.close(); storage.close()
        planner = RevisingPlanner()
        storage, interaction, gateway, core, _ = compose(path, script, planner, replan=True)
        await interaction.answer(pause.value.request_id, revision=1, key="a", answers={"detail": "yes"})
        handle = await interaction.resume(core, pause.value.request_id)
    try:
        result = await handle.wait()
        assert result.status.value == "done", result
        assert planner.calls == 0 and planner.revisions == [2]
    finally: await core.close(); storage.close()


@pytest.mark.asyncio
async def test_planned_root_and_auto_promoted_children_resume_together(tmp_path):
    class TreePlanner(Planner):
        async def create_plan(self, request, *args, **kwargs):
            self.calls += 1
            instruction = next((m.content for m in request.messages if m.attributes.get("agentId")), "root")
            if instruction not in {"root", "middle"}: return plan()
            return PlanningResult(kind="planned", work_plan=WorkPlan(title="Delegate", steps=(
                WorkStep(id="delegate", title="Delegate", type="write", executor="tool", capability_names=("delegateToAgents",)),
                WorkStep(id="finish", title="Finish", type="review", executor="model", depends_on=("delegate",)),
            )))
        async def revise_plan(self, request, capabilities, turn, *args, **kwargs):
            for message in turn.messages:
                if message.role.value == "tool":
                    value = json.loads(message.content)
                    if value.get("tool") == "delegateToAgents":
                        assert not json.loads(value["excerpt"])["pendingRunIds"]
            self.revisions.append(turn.revision)
            return plan(False, "-revised")
    def make_script():
        seen = set()
        def script(messages, count):
            instruction = next((m.content for m in messages if m.role.value == "system" and m.attributes.get("agentId")), "root")
            if instruction != "root" and instruction not in seen and not answered(messages) and not any(m.role.value == "tool" for m in messages):
                seen.add(instruction); return "request_plan", {}
            return tree_script(messages, count)
        return script
    path = tmp_path / "planned-tree.db"
    planner = TreePlanner()
    storage, interaction, gateway, core, request = compose(path, make_script(), planner, tree=True)
    handle = await interaction.submit(core, replace(request, planning_mode="planned"))
    with pytest.raises(UserInputRequired): await handle.wait()
    pending = await interaction.list_waiting()
    assert len(pending) == 2
    assert planner.calls == 4
    await core.close(); storage.close()
    planner = TreePlanner()
    storage, interaction, gateway, core, _ = compose(path, make_script(), planner, tree=True)
    try:
        for row in pending: await interaction.answer(row["id"], revision=1, key="a", answers={"detail": "yes"})
        resumed = await interaction.resume(core, pending[0]["id"])
        result = await resumed.wait()
        assert result.status.value == "done", result
        assert planner.calls == 0 and planner.revisions == [1, 1]
        assert all(run.status.value == "done" for run in await storage.run_tree.list_descendants(handle.run_id))
    finally: await core.close(); storage.close()
