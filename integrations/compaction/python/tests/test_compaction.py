import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from purra.contracts import AgentMessage, AgentRunRequest, ModelRequest, ModelCompletion, DomainContext
from purra.context_orchestration import ContextCompressionCoordinator
from purra.context_orchestration.contracts import ContextCompressionRequest
from purra.context_orchestration.ledger import ContextCompactionBudget, ContextCompactionPhase
from purra_compaction import SemanticCompaction
from purra.model_protocol import generic_capability_snapshot

SUMMARY = {k: [] for k in ("goals", "constraints", "decisions", "completed", "open_questions", "evidence")}


class Tasks:
    calls = 0
    def __init__(self, content=None, finish="stop"):
        self.content, self.finish = json.dumps(SUMMARY) if content is None else content, finish
    async def complete(self, messages, call, signal):
        self.calls += 1
        assert "private-secret" not in str(messages)
        assert call.result_capacity_target_tokens == 256
        assert call.output_budget.max_generation_tokens == 256
        return SimpleNamespace(completion=ModelCompletion(AgentMessage("assistant", self.content), "fixture", finish_reason=self.finish))


def request():
    return AgentRunRequest(messages=(AgentMessage("system", "keep instructions"),
        AgentMessage("user", "old question " * 500), AgentMessage("assistant", "old answer", reasoning="private-secret"),
        AgentMessage("user", "latest question")), model=ModelRequest("fixture", "fixture", replace(generic_capability_snapshot(), max_generation_tokens=256)), domain_context=DomainContext("test"))


@pytest.mark.asyncio
async def test_compaction_preserves_instructions_latest_user_and_does_not_mutate_input():
    tasks = Tasks()
    core = ContextCompressionCoordinator(SemanticCompaction(tasks, max_summary_tokens=256, keep_recent_messages=1))
    source = request()
    # Enough context pressure to invoke the hook, without overflowing the summarizer.
    result = await core.prepare(replace(source, context_window=16384))
    assert result.outcome == "summarized" and tasks.calls == 1
    assert result.request.messages[0] == source.messages[0]
    assert result.request.messages[-1] == source.messages[-1]
    assert len(source.messages) == 4
    assert result.request.messages[1].origin.value == "host_context"


@pytest.mark.asyncio
@pytest.mark.parametrize("content,finish", [("not json", "stop"), (json.dumps(SUMMARY), "length"), ('{"goals":[]}', "stop")])
async def test_invalid_summary_never_replaces_source(content, finish):
    core = ContextCompressionCoordinator(SemanticCompaction(Tasks(content, finish), max_summary_tokens=256, keep_recent_messages=1))
    with pytest.raises(Exception, match="semantic_compaction"):
        await core.prepare(replace(request(), context_window=16384))


@pytest.mark.asyncio
async def test_below_threshold_does_not_call_model():
    tasks = Tasks()
    await ContextCompressionCoordinator(SemanticCompaction(tasks)).prepare(request())
    assert tasks.calls == 0
