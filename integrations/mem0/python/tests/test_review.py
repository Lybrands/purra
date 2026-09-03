import asyncio
import json
import os
import subprocess
import sys
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from purra.contracts import AgentMessage
from purra_mem0 import MemoryError, MemoryRef, MemoryResolution, MemorySource
from test_providers import managed, budget, complete
from test_memory import setup

FIXTURE = json.loads((Path(__file__).resolve().parents[2] / "fixtures/review.json").read_text())
LIMITS = budget(max_llm_calls=8, max_embedding_calls=32, max_output_tokens=4096, max_call_output_tokens=512)


def classifier(kinds, calls, raw=None):
    async def model(messages, cap, signal):
        base = await complete(messages, cap, signal)
        if messages[0].content.startswith("Review a pending memory"):
            calls.append(messages)
            body = raw if raw is not None else json.dumps({"relations": [{"item": str(i), "kind": k} for i, k in enumerate(kinds)]})
            return replace(base, message=AgentMessage(role="assistant", content=body))
        return base
    return model


async def seed(memory, peers=1):
    old = []
    for i in range(peers):
        op = await memory.add(f"既有资料 {i}", source=MemorySource(f"old-{i}", "1"), key=f"old-{i}")
        old.append(MemoryRef(op.ids[0], 1))
    op = await memory.extract([{"role": "user", "content": "这次请详细解释。"}], source=MemorySource("candidate", "1"), key="candidate")
    return MemoryRef(op.ids[0], 1), old


@pytest.mark.asyncio
@pytest.mark.parametrize("case", FIXTURE["cases"], ids=lambda c: c["name"])
async def test_review_is_budgeted_advice_with_durable_replay_and_guarded_execution(managed, case):
    create, sdk = managed
    calls = []
    model = classifier(case["kinds"], calls)
    memory = create(limits=LIMITS, completion=model)
    candidate, old = await seed(memory, len(case["kinds"]))
    epoch = memory.epoch
    result = await memory.review(candidate, key="review", instructions="临时例外不能覆盖长期偏好。")
    assert result.state == "complete" and result.ids == () and memory.epoch == epoch
    assert result.usage.llm_calls == bool(old) and result.usage.embedding_calls == 1
    assert result.review.candidate == candidate and [m.item for m in result.review.matches] == old
    assert [m.kind for m in result.review.matches] == case["kinds"]
    assert (await memory.get(candidate.id, include_inactive=True)).state == "pending"
    proposal = result.review.proposal
    assert (None if proposal is None else proposal.kind) == case["proposal"]
    if old:
        assert "untrusted data" in calls[0][0].content and "临时例外" in calls[0][0].content
        payload = json.loads(calls[0][1].content)
        assert payload["candidate"]["source"]["id"] == "candidate" and payload["related"][0]["item"] == "0"
    memory.close()
    memory = create(limits=LIMITS, completion=model)
    before = len(sdk.calls)
    assert await memory.review(candidate, key="review", instructions="临时例外不能覆盖长期偏好。") == result
    assert len(sdk.calls) == before and memory.operation("review") == result
    with pytest.raises(MemoryError, match="memory_idempotency_conflict"):
        await memory.review(candidate, key="review", instructions="changed policy")
    plan_text = json.dumps(json.loads(memory._journal.db.execute("SELECT plan FROM purra_mem0_ops WHERE key='review'").fetchone()[0]), ensure_ascii=False)
    assert "临时例外" not in plan_text and "既有资料" not in plan_text and "中文" not in plan_text
    if proposal is not None:
        applied = await memory.resolve(proposal, key="apply")
        assert applied.resolution.review_key == "review" and applied.resolution == proposal
        assert memory.epoch == epoch + 1
        if proposal.kind == "conflict":
            assert await memory.get(candidate.id) is None
        else:
            assert await memory.get(proposal.keep) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", FIXTURE["invalid"])
async def test_review_rejects_incomplete_or_invented_model_output(managed, raw):
    create, _ = managed
    memory = create(limits=LIMITS, completion=classifier([], [], raw))
    candidate, _ = await seed(memory)
    epoch = memory.epoch
    with pytest.raises(MemoryError, match="memory_invalid_review"):
        await memory.review(candidate, key="invalid")
    op = memory.operation("invalid")
    assert op.state == "failed" and op.review is None and op.usage.llm_calls == 1
    assert op.usage.reported_output_tokens == 5 and memory.epoch == epoch
    await memory.add("still writable", source=MemorySource("next", "1"), key="next")


@pytest.mark.asyncio
async def test_budget_denial_happens_before_review_model_and_does_not_strand_writer(managed):
    create, _ = managed
    calls = []
    memory = create(limits=replace(LIMITS, max_llm_calls=1), completion=classifier(["duplicate"], calls))
    candidate, _ = await seed(memory)
    with pytest.raises(MemoryError, match="memory_budget_exceeded"):
        await memory.review(candidate, key="denied")
    assert not calls and memory.operation("denied").state == "failed"
    assert memory.operation("denied").usage.llm_calls == 0
    await memory.add("next", source=MemorySource("next", "1"), key="next")


@pytest.mark.asyncio
@pytest.mark.parametrize("event", ["withdraw", "tamper", "cancel", "timeout"])
async def test_changes_or_stop_during_review_never_publish_late_advice(managed, event):
    create, sdk = managed
    started, release, cancel = asyncio.Event(), asyncio.Event(), asyncio.Event()
    regular = classifier(["duplicate"], [])
    async def model(messages, cap, signal):
        if messages[0].content.startswith("Review a pending memory"):
            started.set()
            await release.wait()
        return await regular(messages, cap, signal)
    memory = create(limits=LIMITS, completion=model, timeout_seconds=0.2 if event == "timeout" else 3)
    candidate, old = await seed(memory)
    task = asyncio.create_task(memory.review(candidate, key="slow", signal=cancel))
    try:
        await started.wait()
        if event == "withdraw":
            await create(limits=LIMITS).revoke_source("old-0", key="withdraw")
        elif event == "tamper":
            sdk.rows[old[0].id]["memory"] = "changed outside adapter"
        elif event == "cancel":
            cancel.set()
        if event not in ("timeout", "cancel"):
            release.set()
        with pytest.raises(MemoryError):
            await task
    finally:
        release.set()
        await memory.drain()
    op = memory.operation("slow")
    assert op.state == "failed" and op.review is None
    assert op.usage.unsettled_calls == 0 and op.usage.reported_output_tokens == 5
    assert (await memory.get(candidate.id, include_inactive=True)).state == "pending"


@pytest.mark.asyncio
async def test_bound_execution_checks_even_independent_comparisons_and_blocks_foreign_refs(managed):
    create, _ = managed
    memory = create(limits=LIMITS, completion=classifier(["duplicate", "independent"], []))
    candidate, old = await seed(memory, 2)
    review = (await memory.review(candidate, key="review")).review
    assert old[1] not in review.proposal.items
    with pytest.raises(MemoryError, match="memory_review_mismatch"):
        await memory.resolve(MemoryResolution("independent", (old[0],), old[0].id, "review"), key="foreign")
    await memory.update(old[1].id, "new fact", source=MemorySource("old-1", "2"), version=1, key="change")
    with pytest.raises(MemoryError, match="memory_context_stale"):
        await memory.resolve(review.proposal, key="apply")
    assert memory.operation("apply") is None


@pytest.mark.asyncio
async def test_review_expiry_is_rechecked_before_bound_execution(managed, monkeypatch):
    create, _ = managed
    memory = create(limits=LIMITS, completion=classifier(["independent"], []))
    candidate, old = await seed(memory)
    expires = (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    await memory.update(old[0].id, "short lived", source=MemorySource("old-0", "1"), version=1, key="expires", expires_at=expires)
    proposal = (await memory.review(candidate, key="review")).review.proposal
    import purra_mem0._journal as module
    class Future(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime.now(tz) + timedelta(minutes=2)
    monkeypatch.setattr(module, "datetime", Future)
    with pytest.raises(MemoryError, match="memory_context_stale"):
        await memory.resolve(proposal, key="apply")


@pytest.mark.asyncio
async def test_review_requires_managed_pending_source_and_bounded_whole_input(managed, setup):
    _, raw_create = setup
    with pytest.raises(MemoryError, match="memory_review_requires_managed"):
        await raw_create().review(MemoryRef("x", 1), key="raw")
    create, _ = managed
    memory = create(limits=LIMITS, completion=classifier(["independent"], []), max_input_chars=100)
    candidate, old = await seed(memory)
    with pytest.raises(MemoryError, match="memory_review_candidate_state"):
        await memory.review(old[0], key="active")
    with pytest.raises(MemoryError, match="memory_review_input_too_large"):
        await memory.review(candidate, key="oversize")
    assert memory.operation("oversize").usage.llm_calls == 0


@pytest.mark.asyncio
async def test_crashed_review_is_abandoned_without_provider_retry(managed, tmp_path):
    create, _ = managed
    memory = create(limits=LIMITS)
    candidate, _ = await seed(memory)
    script = '''
import os,sys
from purra_mem0._journal import Journal
from purra_mem0 import MemoryScope
j=Journal(sys.argv[1],MemoryScope('u','p').namespace)
j.begin('crashed','opaque',{'kind':'review','target':None,'meta':None,'budget':'job','review_epoch':j.epoch(),'review_refs':[{'id':sys.argv[2],'version':1}]})
# Simulates process death after provider admission: quota is still charged.
j.admit('job','crashed','llm',10,512)
os._exit(17)
'''
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    result = subprocess.run([sys.executable, "-c", script, str(tmp_path / "journal.db"), candidate.id], env=env, capture_output=True, timeout=10)
    assert result.returncode == 17, result.stderr
    receipt = await memory.reconcile("crashed", writer_stopped=True)
    assert receipt.state == "failed" and receipt.review is None and receipt.usage.unsettled_calls == 1
    assert (await memory.get(candidate.id, include_inactive=True)).state == "pending"
    await memory.add("recovered", source=MemorySource("next", "1"), key="next")

@pytest.mark.asyncio
async def test_withdrawal_during_search_prevents_review_prompt_dispatch(managed):
    from test_providers import embed
    create, _ = managed
    phase, calls = False, []
    async def embedding(texts, signal):
        if phase:
            await create(limits=LIMITS).revoke_source("old-0", key="withdraw")
        return await embed(texts, signal)
    memory = create(limits=LIMITS, completion=classifier(["duplicate"], calls), embedding=embedding)
    candidate, _ = await seed(memory)
    phase = True
    with pytest.raises(MemoryError, match="memory_context_stale"):
        await memory.review(candidate, key="review")
    assert not calls and memory.operation("review").usage.llm_calls == 0
