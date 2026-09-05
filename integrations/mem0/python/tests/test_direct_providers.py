import asyncio
import json

import pytest

from purra.contracts import AgentMessage, ModelCompletion, ModelTokenUsage
from purra_mem0 import MemoryBudget, MemoryProviders, EmbeddingResult, MemoryError
from purra_mem0._journal import Journal
from purra_mem0.providers import ProviderExecution
from purra_mem0.direct_providers import DirectLlm, DirectEmbedder


@pytest.fixture
def execution(tmp_path):
    async def create():
        calls = []
        async def complete(messages, target, signal):
            calls.append(("llm", tuple((m.role, m.content) for m in messages), target))
            return ModelCompletion(message=AgentMessage("assistant", '{"memory":[]}'), model="fixture",
                finish_reason="stop", applied_generation_limit=target, usage=ModelTokenUsage(10, 2))
        async def embed(texts, signal):
            calls.append(("embedding", tuple(texts)))
            return EmbeddingResult([[float(i), 1.] for i, _ in enumerate(texts)], 4)
        budget = MemoryBudget("direct", 3, 3, 10000, 100, 32)
        journal = Journal(str(tmp_path / "direct.db"), "direct")
        journal.budget(budget.key, budget.limits)
        return ProviderExecution(MemoryProviders(budget, complete, embed), journal, 2, 10, 10, 10000), calls, journal
    return create


@pytest.mark.asyncio
async def test_native_messages_and_batch_keep_one_admission(execution):
    bound, calls, journal = await execution()
    try:
        def work():
            assert json.loads(DirectLlm().generate_response([
                {"role": "system", "content": "policy"}, {"role": "user", "content": "中文"}],
                response_format={"type": "json_object"})) == {"memory": []}
            assert DirectEmbedder().embed_batch(["a", "b"], "search") == [[0., 1.], [1., 1.]]
        await asyncio.to_thread(bound.run, work)
        assert calls == [("llm", (("system", "policy"), ("user", "中文")), 32), ("embedding", ("a", "b"))]
        assert journal.usage(budget="direct")["llm_calls"] == 1 and journal.usage(budget="direct")["embedding_calls"] == 1
    finally:
        journal.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [
    lambda: DirectLlm().generate_response([{"role": "user", "content": []}]),
    lambda: DirectLlm().generate_response([{"role": "user", "content": "x"}], tools=[{}]),
    lambda: DirectLlm().generate_response([{"role": "user", "content": "x"}], unknown=True),
    lambda: DirectLlm().generate_response([{"role": "tool", "content": "x"}]),
    lambda: DirectLlm().generate_response([{"role": "user", "content": "x"}], response_format={"type": "json_schema"}),
    lambda: DirectEmbedder().embed_batch(["x", None]),
    lambda: DirectEmbedder().embed("x", "unknown"),
])
async def test_protocol_denial_survives_sdk_swallow_and_fallback(execution, invalid):
    bound, calls, journal = await execution()
    try:
        def work():
            try:
                invalid()
            except MemoryError:
                pass
            with pytest.raises(MemoryError, match="memory_provider_contract"):
                DirectEmbedder().embed("fallback")
            return "SDK claimed success"
        with pytest.raises(MemoryError, match="memory_provider_contract"):
            await asyncio.to_thread(bound.run, work)
        assert calls == []
    finally:
        journal.close()


@pytest.mark.asyncio
async def test_stop_and_unbound_fail_before_callback(execution):
    with pytest.raises(MemoryError, match="memory_provider_unbound"):
        DirectEmbedder().embed("unbound")
    bound, calls, journal = await execution()
    try:
        def work():
            bound.stop("memory_cancelled")
            DirectEmbedder().embed("cancelled")
        with pytest.raises(MemoryError, match="memory_cancelled"):
            await asyncio.to_thread(bound.run, work)
        assert not calls
    finally:
        journal.close()
