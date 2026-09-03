import asyncio
import copy
import json
import os
import subprocess
import sys
import threading

import pytest

from purra.evidence import ContextEvidenceReceipt
from purra.retrieval import RetrievalRequest
from purra_mem0 import Mem0Memory, MemoryError, MemoryRef, MemoryResolution, MemoryScope, MemorySource
from test_memory import FIXTURE, setup

REQUEST = RetrievalRequest(query="偏好", limit=8)


async def pair(memory, texts=("old", "new")):
    a = await memory.add(texts[0], source=MemorySource("a", "1"), key="a")
    b = await memory.extract([{"role": "user", "content": texts[1]}], source=MemorySource("b", "1"), key="b")
    return (MemoryRef(a.ids[0], 1), MemoryRef(b.ids[0], 1))


@pytest.mark.asyncio
@pytest.mark.parametrize("case", FIXTURE["resolutions"])
async def test_atomic_resolution_shared_contract_and_replay(setup, case):
    sdk, create = setup
    memory = create(allow_inference=True)
    refs = await pair(memory, case["texts"])
    hit, = await memory.retrieve(REQUEST)
    old = ContextEvidenceReceipt(evidence_id=hit.metadata["evidenceId"], context_block="memory", source=hit.source, item_id=hit.id, version=hit.version)
    decision = MemoryResolution(case["kind"], refs, None if case["keep"] is None else refs[case["keep"]].id)
    rows, histories, epoch = copy.deepcopy(sdk.rows), copy.deepcopy(sdk.histories), memory.epoch
    receipt = await memory.resolve(decision, key="decision")
    assert receipt.state == "complete" and receipt.resolution == decision
    assert sdk.rows == rows and sdk.histories == histories  # no partial multi-SDK mutation
    assert memory.epoch == epoch + 1
    assert [h.id for h in await memory.retrieve(REQUEST)] == ([] if decision.keep is None else [decision.keep])
    assert [r.id for r in (await memory.list(limit=1)).items] == ([] if decision.keep is None else [decision.keep])
    for ref in refs:
        record = await memory.get(ref.id, include_inactive=True)
        assert record.version == 2 and record.resolution_key == "decision"
        assert record.source.id == ("a" if ref == refs[0] else "b")
        assert record.state == ("active" if record.id == decision.keep else "disabled")
    assert len((await memory.list(state="disabled")).items) == (2 if decision.keep is None else 1)
    with pytest.raises(MemoryError, match="memory_context_stale"):
        await memory.validate_evidence([old])
    calls = len(sdk.calls)
    assert await memory.resolve(decision, key="decision") == receipt
    assert len(sdk.calls) == calls and memory.epoch == epoch + 1
    memory.close()
    reopened = create()
    assert reopened.operation("decision") == receipt
    assert (await reopened.get(refs[0].id, include_inactive=True)).resolution_key == "decision"
    with pytest.raises(MemoryError, match="memory_idempotency_conflict"):
        await reopened.resolve(MemoryResolution("duplicate", refs, refs[1 if case["keep"] == 0 else 0].id), key="decision")


@pytest.mark.asyncio
async def test_resolution_preserves_cas_source_withdrawal_and_later_correction(setup):
    sdk, create = setup
    memory = create(allow_inference=True)
    refs = await pair(memory)
    await memory.resolve(MemoryResolution("supersede", refs, refs[1].id), key="replace")
    with pytest.raises(MemoryError, match="memory_version_conflict"):
        await memory.set_state(refs[0].id, "active", version=1, key="stale")
    await memory.revoke_source("b", key="withdraw")
    assert not await memory.retrieve(REQUEST)  # never silently fall back to old claim
    with pytest.raises(MemoryError, match="memory_source_revoked"):
        await memory.resolve(MemoryResolution("duplicate", tuple(MemoryRef(r.id, 2) for r in refs), refs[0].id), key="invalid")
    sdk.fail_update = True
    with pytest.raises(MemoryError):
        await memory.update(refs[1].id, "corrected", source=MemorySource("c", "1"), version=2, key="correct")
    assert memory.operation("correct").state == "unknown"
    await memory.reconcile("correct", writer_stopped=True)
    current = await memory.get(refs[1].id)
    assert current.version == 3 and current.resolution_key is None and current.source.id == "c"
    assert memory.operation("replace").resolution.kind == "supersede"
    await memory.delete(refs[0].id, version=2, key="delete")
    assert await memory.history(refs[0].id)


@pytest.mark.asyncio
async def test_conflict_can_be_reviewed_again_without_inventing_merged_text(setup):
    _, create = setup
    memory = create(allow_inference=True)
    refs = await pair(memory)
    await memory.resolve(MemoryResolution("conflict", refs), key="quarantine")
    assert not await memory.retrieve(REQUEST)
    reviewed = tuple(MemoryRef(r.id, 2) for r in refs)
    await memory.resolve(MemoryResolution("supersede", reviewed, refs[1].id), key="accept")
    hit, = await memory.retrieve(REQUEST)
    assert hit.id == refs[1].id and hit.content == "new" and hit.version == 3


@pytest.mark.asyncio
async def test_invalid_foreign_expired_and_stale_resolution_leave_no_partial_state(setup):
    sdk, create = setup
    memory = create(allow_inference=True)
    refs = await pair(memory)
    for decision in [MemoryResolution("conflict", (refs[0], MemoryRef("foreign", 1))),
                     MemoryResolution("conflict", (refs[0], MemoryRef(refs[1].id, 2)))]:
        with pytest.raises(MemoryError):
            await memory.resolve(decision, key="invalid")
        assert memory.operation("invalid") is None and (await memory.get(refs[0].id)).version == 1
    other = create(scope=MemoryScope("other", "project"))
    with pytest.raises(MemoryError, match="memory_not_found"):
        await other.resolve(MemoryResolution("conflict", refs), key="foreign")
    expired = await memory.add("expired", source=MemorySource("expired", "1"), key="expired", expires_at="2020-01-01T00:00:00Z")
    with pytest.raises(MemoryError, match="memory_context_stale"):
        await memory.resolve(MemoryResolution("conflict", (refs[0], MemoryRef(expired.ids[0], 1))), key="expired-review")
    for make in [lambda: MemoryResolution("duplicate", refs), lambda: MemoryResolution("conflict", refs, refs[0].id),
                 lambda: MemoryResolution("duplicate", (refs[0], refs[0]), refs[0].id), lambda: MemoryRef("x", True)]:
        with pytest.raises(ValueError):
            make()
    sdk.rows[refs[1].id]["memory"] = "external tampering"
    with pytest.raises(MemoryError, match="memory_record_changed"):
        await memory.resolve(MemoryResolution("conflict", refs), key="tampered")


@pytest.mark.asyncio
async def test_resolution_rolls_back_every_record_and_receipt_on_sql_failure(setup):
    _, create = setup
    memory = create(allow_inference=True)
    refs = await pair(memory)
    epoch = memory.epoch
    memory._journal.db.execute("""CREATE TRIGGER fault BEFORE UPDATE ON purra_mem0_items
        WHEN OLD.id='id-0002' BEGIN SELECT RAISE(ABORT,'injected fault'); END""")
    decision = MemoryResolution("supersede", refs, refs[1].id)
    with pytest.raises(MemoryError, match="memory_sdk_error"):
        await memory.resolve(decision, key="atomic")
    assert memory.operation("atomic") is None and memory.epoch == epoch
    assert [(await memory.get(r.id, include_inactive=True)).version for r in refs] == [1, 1]
    memory._journal.db.execute("DROP TRIGGER fault")
    assert (await memory.resolve(decision, key="atomic")).state == "complete"


@pytest.mark.asyncio
async def test_competing_resolutions_only_one_can_commit(setup):
    _, create = setup
    memory = create(allow_inference=True)
    refs = await pair(memory)
    outputs = await asyncio.gather(*(create().resolve(MemoryResolution("duplicate", refs, refs[i % 2].id), key=f"review-{i}") for i in range(6)), return_exceptions=True)
    assert sum(not isinstance(r, Exception) for r in outputs) == 1
    assert len(await memory.retrieve(REQUEST)) == 1
    assert [(await memory.get(r.id, include_inactive=True)).version for r in refs] == [2, 2]


@pytest.mark.asyncio
@pytest.mark.parametrize("event", ["withdraw", "write", "cancel"])
async def test_changes_during_sdk_verification_prevent_resolution(setup, monkeypatch, event):
    sdk, create = setup
    memory = create(allow_inference=True)
    refs = await pair(memory)
    entered, release, stop = threading.Event(), threading.Event(), asyncio.Event()
    original = sdk.get
    def read(item_id):
        if item_id == refs[1].id:
            entered.set()
            release.wait(3)
        return original(item_id)
    monkeypatch.setattr(sdk, "get", read)
    task = asyncio.create_task(memory.resolve(MemoryResolution("supersede", refs, refs[1].id), key="review", signal=stop))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        if event == "withdraw":
            await create().revoke_source("a", key="withdraw")
        elif event == "write":
            await create().update(refs[0].id, "changed", source=MemorySource("a", "2"), version=1, key="change")
        else:
            stop.set()
        release.set()
        with pytest.raises(MemoryError):
            await task
    finally:
        release.set()
        await memory.drain()
    assert memory.operation("review") is None
    assert (await memory.get(refs[1].id, include_inactive=True)).state == "pending"


@pytest.mark.asyncio
async def test_unknown_sdk_writer_blocks_resolution_without_releasing_its_fence(setup):
    sdk, create = setup
    memory = create(allow_inference=True)
    refs = await pair(memory)
    sdk.fail_add = True
    with pytest.raises(MemoryError):
        await memory.add("uncertain", source=MemorySource("c", "1"), key="lost")
    with pytest.raises(MemoryError, match="memory_write_busy"):
        await memory.resolve(MemoryResolution("conflict", refs), key="review")
    assert memory.operation("lost").state == "unknown" and memory.operation("review") is None
    await memory.reconcile("lost", writer_stopped=True)
    assert (await memory.resolve(MemoryResolution("conflict", refs), key="review")).state == "complete"


@pytest.mark.asyncio
async def test_resolution_commit_survives_process_exit(setup, tmp_path):
    sdk, create = setup
    memory = create(allow_inference=True)
    refs = await pair(memory)
    snapshot = tmp_path / "sdk.json"
    snapshot.write_text(json.dumps(sdk.rows))
    program = '''
import asyncio,json,os,sys
from purra_mem0 import Mem0Memory,MemoryScope,MemoryRef,MemoryResolution
class Sdk:
    def get(self,id): return json.load(open(sys.argv[2])).get(id)
    def forbidden(self,*a,**k): raise AssertionError('no mutations')
    add=get_all=search=update=delete=history=forbidden
m=Mem0Memory(client=Sdk(),scope=MemoryScope('user','project'),journal_path=sys.argv[1])
asyncio.run(m.resolve(MemoryResolution('supersede',(MemoryRef('id-0001',1),MemoryRef('id-0002',1)),'id-0002'),key='durable'))
os._exit(17)
'''
    result = subprocess.run([sys.executable, "-c", program, str(tmp_path / "journal.db"), str(snapshot)],
                            env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)}, capture_output=True, timeout=10)
    assert result.returncode == 17, result.stderr
    assert create().operation("durable").resolution.keep == refs[1].id
    assert [h.id for h in await memory.retrieve(REQUEST)] == [refs[1].id]
