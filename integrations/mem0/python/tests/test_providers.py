import asyncio
import json
import os
import subprocess
import sys
from dataclasses import replace

import pytest

from purra.contracts import AgentMessage, ModelCompletion, ModelTokenUsage, ModelRequest
from purra.model_execution import AgentModelTaskRunner
from purra.model_invocation import AgentModelInvocationManager, ModelInvocationContext
from purra.model_protocol import generic_capability_snapshot
from purra.retrieval import RetrievalRequest, RetrievalError
from purra_mem0 import EmbeddingResult, Mem0Memory, MemoryBudget, MemoryProviders, MemoryScope, run_model
from purra_mem0 import MemoryError, MemoryRef
from purra_mem0.providers import ManagedMem0Client, current_execution
from test_memory import Sdk, SOURCE


class ProviderSdk(Sdk):
    """Inject swallowed errors at the SDK boundary; real SDK is tested separately."""
    def add(self, messages, **options):
        execution = current_execution()
        if not options["infer"]:
            execution.invoke("embedding", [messages])
        else:
            execution.invoke("embedding", [messages[0]["content"]])
            response = execution.invoke("llm", [AgentMessage(role="user", content=messages[0]["content"])])
            texts = json.loads(response)["memory"]
            messages = []
            for record in texts:
                try:
                    execution.invoke("embedding", [record["text"]])
                    messages.append({"role": "user", "content": record["text"]})
                except Exception:
                    # Emulate SDK's fallback trying again, then returning a partial result.
                    try:
                        execution.invoke("embedding", [record["text"]])
                    except Exception:
                        pass
        return super().add(messages, **options)

    def search(self, query, **options):
        current_execution().invoke("embedding", [query])
        return super().search(query, **options)


def budget(**changes):
    return replace(MemoryBudget("job", 2, 8, 50_000, 64, 32), **changes)


async def complete(messages, cap, signal):
    return ModelCompletion(message=AgentMessage(role="assistant", content=json.dumps({"memory": [{"text": "中文", "entities": []}]})),
                           model="test", finish_reason="stop", applied_output_limit=cap, usage=ModelTokenUsage(10, 5))


async def embed(texts, signal):
    return EmbeddingResult([[1.0, 0.0] for _ in texts], input_tokens=sum(map(len, texts)))


@pytest.fixture
def managed(tmp_path):
    sdk = ProviderSdk()
    instances = []

    def create(*, limits=None, completion=complete, embedding=embed, **options):
        memory = Mem0Memory(client=ManagedMem0Client(sdk, 2), scope=MemoryScope("u", "p"),
                            journal_path=str(tmp_path / "journal.db"), allow_inference=True,
                            providers=MemoryProviders(limits or budget(), completion, embedding), **options)
        instances.append(memory)
        return memory

    yield create, sdk
    for memory in instances:
        memory.close()


@pytest.mark.asyncio
async def test_durable_admission_receipts_replay_and_reads(managed):
    create, sdk = managed
    memory = create(limits=budget(max_embedding_calls=3))
    receipt = await memory.extract([{"role": "user", "content": "说中文"}], source=SOURCE, key="extract")
    assert receipt.usage.llm_calls == 1 and receipt.usage.embedding_calls == 2
    assert receipt.usage.reported_input_tokens == 15 and receipt.usage.reported_output_tokens == 5
    assert receipt.usage.reserved_output_tokens == 32 and receipt.usage.unreported_calls == 0
    memory.close()
    restored = create(limits=budget(max_embedding_calls=3))
    assert await restored.extract([{"role": "user", "content": "说中文"}], source=SOURCE, key="extract") == receipt
    await restored.retrieve(RetrievalRequest(query="中文", limit=2))
    with pytest.raises(RetrievalError):
        await restored.retrieve(RetrievalRequest(query="中文", limit=2))
    assert restored.budget_usage().embedding_calls == 3
    with pytest.raises(Exception, match="memory_budget_conflict"):
        create(limits=budget(max_embedding_calls=4))


@pytest.mark.asyncio
@pytest.mark.parametrize("limits", [budget(max_llm_calls=0), budget(max_output_tokens=31), budget(max_input_chars=3)])
async def test_denial_happens_before_llm_callback(managed, limits):
    create, _ = managed
    calls = []
    async def model(*args):
        calls.append(1)
        return await complete(*args)
    memory = create(limits=limits, completion=model)
    with pytest.raises(Exception, match="memory_budget_exceeded"):
        await memory.extract([{"role": "user", "content": "中文"}], source=SOURCE, key="denied")
    assert not calls and memory.operation("denied").state == "unknown"


@pytest.mark.asyncio
async def test_swallowed_denial_cannot_commit_or_reconcile_extraction(managed):
    create, _ = managed
    memory = create(limits=budget(max_embedding_calls=1))
    with pytest.raises(Exception, match="memory_budget_exceeded"):
        await memory.extract([{"role": "user", "content": "中文"}], source=SOURCE, key="partial")
    assert memory.operation("partial").usage.embedding_calls == 1
    assert (await memory.list(state="pending")).items == ()
    with pytest.raises(Exception, match="memory_reconciliation_required"):
        await memory.reconcile("partial", writer_stopped=True)
    assert (await memory.discard_extraction("partial", writer_stopped=True)).state == "discarded"


@pytest.mark.asyncio
async def test_source_revocation_stops_subsequent_sdk_provider_calls(managed):
    create, _ = managed
    async def revoke_during_completion(*args):
        await memory.revoke_source(SOURCE.id, key="withdraw")
        return await complete(*args)
    memory = create(completion=revoke_during_completion)
    with pytest.raises(MemoryError, match="memory_source_revoked"):
        await memory.extract([{"role": "user", "content": "中文"}], source=SOURCE, key="interrupted")
    usage = memory.operation("interrupted").usage
    assert usage.llm_calls == usage.embedding_calls == 1
    assert memory.operation("withdraw").usage.llm_calls == 0
    assert (await memory.list(state="pending")).items == ()
    with pytest.raises(MemoryError, match="memory_reconciliation_required"):
        await memory.reconcile("interrupted", writer_stopped=True)
    await memory.discard_extraction("interrupted", writer_stopped=True)


@pytest.mark.asyncio
async def test_lost_provider_verdict_cannot_be_reconciled_from_ids_alone(managed, monkeypatch):
    create, _ = managed
    memory = create()
    def unavailable(key):
        raise OSError("journal unavailable after SDK IDs")
    monkeypatch.setattr(memory._journal, "verify_providers", unavailable)
    with pytest.raises(Exception, match="memory_sdk_error"):
        await memory.extract([{"role": "user", "content": "中文"}], source=SOURCE, key="verdict")
    assert memory.operation("verdict").ids
    with pytest.raises(Exception, match="memory_reconciliation_required"):
        await memory.reconcile("verdict", writer_stopped=True)
    assert (await memory.discard_extraction("verdict", writer_stopped=True)).state == "discarded"


@pytest.mark.asyncio
@pytest.mark.parametrize("abort", [False, True])
async def test_stop_blocks_late_calls_and_preserves_late_usage(managed, abort):
    create, _ = managed
    started, release, signal = asyncio.Event(), asyncio.Event(), asyncio.Event()
    async def slow(texts, cancel):
        started.set()
        await release.wait()  # Deliberately ignores cancellation like some Providers.
        assert cancel.is_set()
        return await embed(texts, cancel)
    memory = create(embedding=slow, timeout_seconds=0.03 if not abort else 2)
    task = asyncio.create_task(memory.extract([{"role": "user", "content": "中文"}], source=SOURCE, key="late", signal=signal))
    await started.wait()
    if abort:
        signal.set()
    with pytest.raises(Exception, match="memory_cancelled" if abort else "memory_timeout"):
        await task
    assert memory.operation("late").usage.unsettled_calls == 1
    release.set()
    await memory.drain()
    usage = memory.operation("late").usage
    assert usage.unsettled_calls == 0 and usage.llm_calls == 0 and usage.reported_input_tokens == 2


@pytest.mark.asyncio
async def test_concurrent_readers_share_quota_and_unknown_is_not_zero(managed):
    create, _ = managed
    async def unknown(texts, signal):
        return EmbeddingResult([[1, 0] for _ in texts])
    first = create(limits=budget(max_embedding_calls=1), embedding=unknown)
    second = create(limits=budget(max_embedding_calls=1), embedding=unknown)
    results = await asyncio.gather(first.retrieve(RetrievalRequest(query="a", limit=1)),
                                   second.retrieve(RetrievalRequest(query="b", limit=1)), return_exceptions=True)
    assert sum(isinstance(x, RetrievalError) for x in results) == 1
    assert first.budget_usage().unreported_calls == 1 and second.budget_usage().embedding_calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_cap", [False, True])
async def test_invalid_extraction_and_output_cap_fail_closed(managed, invalid_cap):
    create, _ = managed
    async def invalid(messages, cap, signal):
        if invalid_cap:
            return replace(await complete(messages, cap, signal), applied_output_limit=cap + 1)
        return replace(await complete(messages, cap, signal), message=AgentMessage(role="assistant", content="not json"))
    memory = create(completion=invalid)
    with pytest.raises(Exception, match="memory_provider_contract" if invalid_cap else "memory_invalid_extraction"):
        await memory.extract([{"role": "user", "content": "中文"}], source=SOURCE, key="invalid")
    assert memory.operation("invalid").usage.reported_output_tokens == 5


@pytest.mark.asyncio
async def test_embedding_shape_validation_and_callback_failures_are_sticky(managed):
    create, _ = managed
    async def wrong_shape(texts, signal):
        return EmbeddingResult([[float("nan"), 0]], input_tokens=2)
    memory = create(embedding=wrong_shape)
    with pytest.raises(Exception, match="memory_provider_contract"):
        await memory.add("中文", source=SOURCE, key="shape")
    assert memory.operation("shape").usage.embedding_calls == 1
    assert (await memory.list()).items == ()


@pytest.mark.asyncio
async def test_input_envelope_counts_unicode_codepoints(managed):
    create, _ = managed
    memory = create(limits=budget(max_input_chars=2))
    result = await memory.add("猫😺", source=SOURCE, key="unicode")
    assert result.usage.input_chars == 2
    with pytest.raises(RetrievalError):
        await memory.retrieve(RetrievalRequest(query="a", limit=1))


@pytest.mark.asyncio
async def test_run_bridge_uses_existing_reservation_and_usage_settlement(managed):
    create, _ = managed
    reservations, settlements = [], []
    class Repository:
        async def reserve_model_attempt(self, *args):
            reservations.append(args)
        async def settle_model_attempt(self, *args):
            settlements.append(args)
    class Gateway:
        async def stream(self, *args, **kwargs):
            raise AssertionError("memory uses completion")
        async def complete(self, messages, invocation, signal=None):
            assert reservations and invocation.max_call_output_tokens == 32
            result = await complete(messages, 32, signal)
            if messages[0].content.startswith("Review a pending memory"):
                result = replace(result, message=AgentMessage(role="assistant", content='{"relations":[{"item":"0","kind":"duplicate"}]}'))
            return result
    request = ModelRequest(provider="test", model="test", capability_snapshot=replace(generic_capability_snapshot(), max_call_output_tokens=32))
    runner = AgentModelTaskRunner(AgentModelInvocationManager(Gateway(), budget_repository=Repository()), ModelInvocationContext(run_id="existing-run"))
    memory = create(completion=run_model(runner, request))
    await memory.add("中文", source=SOURCE, key="existing")
    receipt = await memory.extract([{"role": "user", "content": "中文"}], source=SOURCE, key="run")
    review = await memory.review(MemoryRef(receipt.ids[0], 1), key="run-review")
    assert len(reservations) == len(settlements) == 2 and review.review.proposal.kind == "duplicate"
    assert reservations[0][0] == settlements[0][0] == "existing-run"
    assert settlements[0][-1].output_tokens == receipt.usage.reported_output_tokens == 5
    assert settlements[1][0] == "existing-run" and review.usage.reported_output_tokens == 5


def test_crash_after_reservation_does_not_restore_quota(tmp_path):
    path = str(tmp_path / "journal.db")
    script = '''
import os,sys
from purra_mem0._journal import Journal
from purra_mem0.providers import MemoryBudget
j=Journal(sys.argv[1], 'scope')
b=MemoryBudget('job', 0, 1, 20, 0, 1)
j.budget(b.key,b.limits)
j.admit('job',None,'embedding',2,0)
os._exit(17)
'''
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    assert subprocess.run([sys.executable, "-c", script, path], env=env, check=False).returncode == 17
    from purra_mem0._journal import Journal
    journal = Journal(path, "scope")
    try:
        assert journal.usage(budget="job")["unsettled_calls"] == 1
        with pytest.raises(Exception, match="memory_budget_exceeded"):
            journal.admit("job", None, "embedding", 2, 0)
    finally:
        journal.close()
