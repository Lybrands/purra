import asyncio
import copy
import os
import subprocess
import sys
import threading
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from purra.contracts import AgentRunRequest, ContextBudget, DomainContext, ModelRequest
from purra.evidence import ContextEvidenceReceipt, CONTEXT_EVIDENCE_RECEIPTS_KEY
from purra.retrieval import RetrievalError, RetrievalRequest
from purra_mem0 import Mem0Memory, MemoryContext, MemoryScope, MemorySource, MemoryError
from test_memory import Sdk

SOURCE = MemorySource("来源😺", "01")
REQUEST = RetrievalRequest(query="中文", limit=1)


@pytest.fixture
def setup(tmp_path):
    sdk, instances = Sdk(), []
    def create(**options):
        memory = Mem0Memory(client=sdk, scope=options.pop("scope", MemoryScope("u", "p")),
                            journal_path=str(tmp_path / "journal.db"), **options)
        instances.append(memory)
        return memory
    yield sdk, create, tmp_path
    for memory in instances:
        memory.close()


def evidence(hit):
    return ContextEvidenceReceipt(evidence_id=hit.metadata["evidenceId"], context_block="memory",
                                  source=hit.source, item_id=hit.id, version=hit.version)


@pytest.mark.asyncio
async def test_revision_revocation_is_durable_scoped_and_blocks_reingestion(setup):
    sdk, create, _ = setup
    memory = create(allow_inference=True)
    original = await memory.add("old", source=SOURCE, key="old")
    good, = (await memory.add("new", source=MemorySource(SOURCE.id, "1"), key="new")).ids
    pending = await memory.extract([{"role": "user", "content": "pending"}], source=SOURCE, key="pending")
    receipt = evidence((await memory.retrieve(REQUEST))[0])
    epoch, calls = memory.epoch, copy.deepcopy(sdk.calls)
    revoked = await memory.revoke_source(SOURCE.id, revision=SOURCE.revision, key="revoke")
    assert revoked.state == "complete" and revoked.ids == ()
    assert revoked.usage.embedding_calls == revoked.usage.llm_calls == revoked.usage.unreported_calls == 0
    assert sdk.calls == calls and memory.epoch == epoch + 1
    assert await memory.revoke_source(SOURCE.id, revision=SOURCE.revision, key="revoke") == revoked
    await memory.revoke_source(SOURCE.id, revision=SOURCE.revision, key="revoke-again")
    assert memory.epoch == epoch + 1
    assert memory.is_source_revoked(SOURCE) and not memory.is_source_revoked(MemorySource(SOURCE.id, "1"))
    assert not create(scope=MemoryScope("other", "p")).is_source_revoked(SOURCE)
    assert await memory.get(original.ids[0], include_inactive=True) is None
    assert await memory.get(pending.ids[0], include_inactive=True) is None
    assert (await memory.list(state="pending")).items == ()
    assert [r.id for r in (await memory.list(limit=1)).items] == [good]  # filter before pagination
    assert [h.id for h in await memory.retrieve(REQUEST)] == [good]  # bounded overfetch
    with pytest.raises(MemoryError, match="memory_context_stale"):
        await memory.validate_evidence([receipt])
    calls = copy.deepcopy(sdk.calls)
    for kind, work in [
        ("add", lambda: memory.add("old", source=SOURCE, key="blocked-add")),
        ("extract", lambda: memory.extract([{"role": "user", "content": "old"}], source=SOURCE, key="blocked-extract")),
        ("update", lambda: memory.update(good, "old", source=SOURCE, version=1, key="blocked-update")),
        ("state", lambda: memory.set_state(original.ids[0], "active", version=1, key="blocked-state")),
    ]:
        with pytest.raises(MemoryError, match="memory_source_revoked"):
            await work()
        receipt = memory.operation("blocked-" + kind)
        if kind == "state":
            assert receipt is None  # Control changes either commit atomically or remain absent.
        else:
            assert receipt.state == "failed"
    assert sdk.calls == calls
    assert await memory.add("old", source=SOURCE, key="old") == original  # receipt replay, not visibility
    memory.close()
    restored = create()
    assert restored.is_source_revoked(SOURCE)
    assert await restored.revoke_source(SOURCE.id, revision=SOURCE.revision, key="revoke") == revoked
    with pytest.raises(MemoryError, match="memory_idempotency_conflict"):
        await restored.revoke_source(SOURCE.id, key="revoke")
    assert await restored.history(original.ids[0])  # explicit host audit, not erasure
    await restored.delete(original.ids[0], version=1, key="cleanup")
    assert original.ids[0] not in sdk.rows


@pytest.mark.asyncio
async def test_full_source_revocation_covers_future_and_literal_star_is_not_wildcard(setup):
    _, create, _ = setup
    memory = create()
    await memory.revoke_source("source", revision="*", key="literal-star")
    assert memory.is_source_revoked(MemorySource("source", "*"))
    assert not memory.is_source_revoked(MemorySource("source", "later"))
    await memory.revoke_source("source", key="all")
    assert memory.is_source_revoked(MemorySource("source", "future"))
    with pytest.raises(MemoryError, match="memory_source_revoked"):
        await memory.add("future", source=MemorySource("source", "future"), key="new")


@pytest.mark.asyncio
async def test_explicit_correction_uses_new_source_and_invalidates_old_evidence(setup):
    _, create, _ = setup
    memory = create()
    item, = (await memory.add("旧事实", source=SOURCE, key="old")).ids
    old = evidence((await memory.retrieve(REQUEST))[0])
    await memory.revoke_source(SOURCE.id, revision=SOURCE.revision, key="invalid")
    # A host-approved replacement is independent evidence, not a revival of the old revision.
    replacement = MemorySource(SOURCE.id, "corrected")
    await memory.update(item, "纠正后的事实", source=replacement, version=1, key="correct")
    assert (await memory.get(item)).source == replacement
    with pytest.raises(MemoryError, match="memory_context_stale"):
        await memory.validate_evidence([old])
    current = evidence((await memory.retrieve(REQUEST))[0])
    await memory.validate_evidence([current])
    await memory.set_state(item, "disabled", version=2, key="disable")
    with pytest.raises(MemoryError, match="memory_context_stale"):
        await memory.validate_evidence([current])


@pytest.mark.asyncio
async def test_revocation_does_not_wait_for_or_release_unknown_writer(setup):
    sdk, create, _ = setup
    memory = create(timeout_seconds=0.02)
    sdk.gate = threading.Event()
    try:
        with pytest.raises(MemoryError, match="memory_timeout"):
            await memory.add("late", source=SOURCE, key="late")
        assert sdk.started.is_set()
        await memory.revoke_source(SOURCE.id, key="withdraw")
        assert memory.operation("late").state == "running"
        with pytest.raises(MemoryError, match="memory_write_busy"):
            await memory.add("other", source=MemorySource("other", "1"), key="busy")
    finally:
        sdk.gate.set()
        await memory.drain()
    result = memory.operation("late")
    assert result.state == "complete" and await memory.get(result.ids[0]) is None
    assert await memory.retrieve(REQUEST) == ()


@pytest.mark.asyncio
async def test_reconciliation_of_lost_write_cannot_resurrect_revoked_source(setup):
    sdk, create, _ = setup
    memory = create()
    sdk.fail_add = True
    with pytest.raises(MemoryError):
        await memory.add("lost", source=SOURCE, key="lost")
    await memory.revoke_source(SOURCE.id, key="withdraw")
    recovered = await memory.reconcile("lost", writer_stopped=True)
    assert recovered.state == "complete" and await memory.get(recovered.ids[0]) is None
    assert (await memory.list()).items == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["get", "list", "retrieve"])
async def test_get_and_search_fail_closed_when_revoked_during_sdk_read(setup, monkeypatch, kind):
    sdk, create, _ = setup
    memory = create()
    item, = (await memory.add("private", source=SOURCE, key="add")).ids
    entered, released = threading.Event(), threading.Event()
    original = sdk.get
    def paused(item_id):
        result = original(item_id)
        entered.set()
        released.wait(2)
        return result
    monkeypatch.setattr(sdk, "get", paused)
    task = asyncio.create_task(memory.get(item) if kind == "get" else memory.list() if kind == "list" else memory.retrieve(REQUEST))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        await create().revoke_source(SOURCE.id, key="withdraw")
    finally:
        released.set()
    with pytest.raises(RetrievalError if kind == "retrieve" else MemoryError):
        await task


@pytest.mark.asyncio
async def test_evidence_is_store_scoped_and_expiry_does_not_need_epoch_change(setup):
    sdk, create, path = setup
    memory = create()
    expires = (datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    await memory.add("temporary", source=SOURCE, key="temp", expires_at=expires)
    receipt = evidence((await memory.retrieve(REQUEST))[0])
    await memory.validate_evidence([receipt])
    other = Mem0Memory(client=sdk, scope=MemoryScope("u", "p"), journal_path=str(path / "other.db"))
    try:
        with pytest.raises(MemoryError, match="memory_context_stale"):
            await other.validate_evidence([receipt])
    finally:
        other.close()
    with pytest.raises(MemoryError, match="memory_context_stale"):
        await memory.validate_evidence([replace(receipt, source="foreign")])
    # Advancing the read clock proves expiry is checked independently of epoch.
    import purra_mem0.memory as module
    original = module.datetime
    class Future(original):
        @classmethod
        def now(cls, tz=None):
            return original.now(tz) + timedelta(seconds=120)
    epoch = memory.epoch
    module.datetime = Future
    try:
        with pytest.raises(MemoryError, match="memory_context_stale"):
            await memory.validate_evidence([receipt])
        assert memory.epoch == epoch
    finally:
        module.datetime = original


@pytest.mark.asyncio
async def test_context_rechecks_evidence_after_retrieval(setup, monkeypatch):
    _, create, _ = setup
    memory = create()
    await memory.add("中文", source=SOURCE, key="add")
    context = MemoryContext(memory=memory, query=lambda _: "中文")
    budget = ContextBudget(window_tokens=4000, output_reserve_tokens=1000, safety_reserve_tokens=0,
                           runtime_reserve_tokens=0, provider_input_tokens=3000, context_allocations={"memory": 200})
    request = AgentRunRequest(messages=(), model=ModelRequest("test", "test"), domain_context=DomainContext("test"))
    block, = (await context.build_context(request, budget)).blocks
    raw, = block.host_metadata[CONTEXT_EVIDENCE_RECEIPTS_KEY]
    await memory.validate_evidence([ContextEvidenceReceipt(evidence_id=raw["evidenceId"], context_block="memory",
                                                          source=raw["source"], item_id=raw["itemId"], version=raw["version"])])
    original = memory.retrieve
    async def revoke_after_retrieval(*args, **kwargs):
        hits = await original(*args, **kwargs)
        await memory.revoke_source(SOURCE.id, key="withdraw")
        return hits
    monkeypatch.setattr(memory, "retrieve", revoke_after_retrieval)
    with pytest.raises(MemoryError, match="memory_context_stale"):
        await context.build_context(request, budget)


def test_revocation_receipt_survives_process_exit(setup):
    _, create, path = setup
    program = '''
import asyncio,os,sys
from purra_mem0 import Mem0Memory,MemoryScope
class Sdk:
    def add(self,*a,**k): raise AssertionError('no SDK call')
    get=get_all=search=update=delete=history=add
m=Mem0Memory(client=Sdk(),scope=MemoryScope('u','p'),journal_path=sys.argv[1])
asyncio.run(m.revoke_source('来源😺',key='withdraw'))
os._exit(17)
'''
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}
    result = subprocess.run([sys.executable, "-c", program, str(path / "journal.db")], env=env, capture_output=True, timeout=10)
    assert result.returncode == 17, result.stderr
    memory = create()
    assert memory.is_source_revoked(SOURCE) and memory.operation("withdraw").state == "complete"
