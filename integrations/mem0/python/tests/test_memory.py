from __future__ import annotations

import asyncio
import copy
import json
import threading
from pathlib import Path
import os
import subprocess
import sys

import pytest

from purra.contracts import AgentRunRequest, ContextBudget, DomainContext, ExecutionState, ModelRequest
from purra.evidence import CONTEXT_EVIDENCE_RECEIPTS_KEY
from purra_mem0 import Mem0Memory, MemoryContext, MemoryError, MemoryScope, MemorySource
from purra.retrieval import RetrievalError, RetrievalRequest, RetrieverTool


class Sdk:
    """Fault-injectable SDK boundary, not evidence of semantic retrieval quality."""
    def __init__(self):
        self.rows, self.histories, self.calls = {}, {}, []
        self.next_id = 0
        self.fail_add = False
        self.fail_update = False
        self.gate = None
        self.started = threading.Event()
        self.search_override = None

    def add(self, messages, **options):
        self.calls.append(("add", copy.deepcopy(options)))
        self.started.set()
        if self.gate is not None:
            self.gate.wait(2)
        texts = [m["content"] for m in messages] if options["infer"] else [messages]
        result = []
        for text in texts:
            self.next_id += 1
            item_id = f"id-{self.next_id:04d}"
            self.rows[item_id] = {"id": item_id, "memory": text, "user_id": options["user_id"],
                                  "run_id": options["run_id"], "metadata": copy.deepcopy(options["metadata"])}
            self.histories[item_id] = [{"event": "ADD", "new_memory": text}]
            result.append({"id": item_id})
        if self.fail_add:
            self.fail_add = False
            raise RuntimeError("secret provider payload")
        return {"results": result}

    def get(self, item_id):
        self.calls.append(("get", item_id))
        return copy.deepcopy(self.rows.get(item_id))

    def get_all(self, *, filters, top_k):
        self.calls.append(("get_all", copy.deepcopy(filters)))
        rows = [r for r in self.rows.values() if all(r.get(k, r["metadata"].get(k)) == v for k, v in filters.items())]
        return {"results": copy.deepcopy(rows[:top_k])}

    def search(self, query, *, filters, top_k):
        self.calls.append(("search", copy.deepcopy(filters)))
        return self.search_override or self.get_all(filters=filters, top_k=top_k)

    def update(self, item_id, *, text, metadata):
        self.rows[item_id]["memory"] = text
        self.rows[item_id]["metadata"].update(metadata)
        self.histories[item_id].append({"event": "UPDATE", "new_memory": text})
        if self.fail_update:
            self.fail_update = False
            raise RuntimeError("secret provider payload")

    def delete(self, item_id):
        self.rows.pop(item_id)
        self.histories[item_id].append({"event": "DELETE"})

    def history(self, item_id):
        return copy.deepcopy(self.histories[item_id])


SOURCE = MemorySource("conversation:1", "3")
FIXTURE = json.loads((Path(__file__).resolve().parents[2] / "fixtures/memory.json").read_text())


@pytest.fixture
def setup(tmp_path):
    client = Sdk()
    instances = []
    def create(**options):
        memory = Mem0Memory(client=client, scope=options.pop("scope", MemoryScope("user", "project")),
                            journal_path=str(tmp_path / "journal.db"), **options)
        instances.append(memory)
        return memory
    yield client, create
    for memory in instances:
        memory.close()


@pytest.mark.asyncio
async def test_crud_version_history_and_durable_replay(setup):
    sdk, create = setup
    memory = create()
    result = await memory.add("我喜欢简短回复。", source=SOURCE, key="add")
    item_id, = result.ids
    assert result.state == "complete" and result.usage == "unknown"
    assert (await memory.get(item_id)).source == SOURCE
    assert (await memory.list()).items[0].version == 1
    epoch = memory.epoch
    await memory.update(item_id, "请详细解释复杂问题。", source=SOURCE, version=1, key="update")
    with pytest.raises(MemoryError, match="memory_version_conflict"):
        await memory.update(item_id, "stale", source=SOURCE, version=1, key="stale")
    assert memory.operation("stale").state == "failed"
    with pytest.raises(MemoryError, match="memory_context_stale"):
        memory.assert_epoch(epoch)
    assert (await memory.history(item_id))[-1]["new_memory"] == "请详细解释复杂问题。"
    memory.close()
    restored = create()
    assert await restored.add("我喜欢简短回复。", source=SOURCE, key="add") == result
    assert len(sdk.rows) == 1
    with pytest.raises(MemoryError, match="memory_idempotency_conflict"):
        await restored.add("changed", source=SOURCE, key="add")
    await restored.delete(item_id, version=2, key="delete")
    assert await restored.get(item_id, include_inactive=True) is None
    assert (await restored.list()).items == ()
    assert (await restored.history(item_id))[-1]["event"] == "DELETE"
    assert await restored.add("我喜欢简短回复。", source=SOURCE, key="add") == result
    assert not sdk.rows  # replay never regenerates a deleted memory


@pytest.mark.asyncio
async def test_candidates_are_never_retrieved_until_host_activates(setup):
    sdk, create = setup
    memory = create(allow_inference=True)
    candidate = await memory.extract([{"role": "user", "content": "项目使用中文。"}], source=SOURCE, key="extract")
    item_id, = candidate.ids
    assert await memory.get(item_id) is None
    assert (await memory.get(item_id, include_inactive=True)).state == "pending"
    assert len((await memory.list(state="pending")).items) == 1
    request = RetrievalRequest(query="语言", limit=8, run_id="transient-run")
    assert await memory.retrieve(request) == ()
    await memory.set_state(item_id, "active", version=1, key="approve")
    hit, = await memory.retrieve(request)
    assert hit.untrusted and hit.version == 2 and hit.metadata["inferred"]
    await memory.set_state(item_id, "disabled", version=2, key="disable")
    assert await memory.retrieve(request) == ()
    add_options = next(v for k, v in sdk.calls if k == "add")
    assert add_options["run_id"] != request.run_id  # source session, not Agent Run lifetime


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", [MemoryScope("other", "project"), MemoryScope("user", "other"), MemoryScope("user", "project", "other")])
async def test_all_entry_points_are_scoped(setup, scope):
    sdk, create = setup
    owner, outsider = create(), create(scope=scope)
    item_id, = (await owner.add("private", source=SOURCE, key="add")).ids
    call_count = len(sdk.calls)
    assert await outsider.get(item_id, include_inactive=True) is None
    assert len(sdk.calls) == call_count  # no foreign ID probe sent to SDK
    assert (await outsider.list()).items == ()
    assert await outsider.retrieve(RetrievalRequest(query="private", limit=2)) == ()
    for work in [outsider.update(item_id, "bad", source=SOURCE, version=1, key="update"),
                 outsider.delete(item_id, version=1, key="delete"), outsider.history(item_id),
                 outsider.set_state(item_id, "disabled", version=1, key="state")]:
        with pytest.raises(MemoryError, match="memory_not_found"):
            await work
    assert (await owner.get(item_id)).text == "private"


@pytest.mark.asyncio
async def test_search_filters_are_not_treated_as_authorization(setup):
    sdk, create = setup
    owner, outsider = create(), create(scope=MemoryScope("foreign", "project"))
    item_id, = (await outsider.add("private", source=SOURCE, key="add")).ids
    sdk.search_override = {"results": [copy.deepcopy(sdk.rows[item_id])]}
    with pytest.raises(RetrievalError):
        await owner.retrieve(RetrievalRequest(query="private", limit=2))
    with pytest.raises(RetrievalError, match="scope"):
        await owner.retrieve(RetrievalRequest(query="private", limit=2, scope={"user": "foreign"}))


@pytest.mark.asyncio
async def test_expiry_and_external_changes_fail_closed(setup):
    sdk, create = setup
    memory = create()
    item_id, = (await memory.add("expired", source=SOURCE, key="expired", expires_at="2000-01-01T00:00:00Z")).ids
    assert await memory.get(item_id) is None
    assert await memory.retrieve(RetrievalRequest(query="expired", limit=2)) == ()
    assert (await memory.get(item_id, include_inactive=True)).text == "expired"
    await memory.update(item_id, "corrected expired", source=SOURCE, version=1, key="update")
    assert await memory.get(item_id) is None
    sdk.rows[item_id]["memory"] = "external overwrite"
    with pytest.raises(MemoryError, match="memory_record_changed"):
        await memory.get(item_id, include_inactive=True)


@pytest.mark.asyncio
async def test_lost_response_fences_retry_and_reconciles_after_restart(setup):
    sdk, create = setup
    memory = create()
    sdk.fail_add = True
    with pytest.raises(MemoryError, match="memory_sdk_error") as caught:
        await memory.add("private", source=SOURCE, key="add")
    assert "secret" not in str(caught.value)
    assert memory.operation("add").state == "unknown"
    assert await memory.retrieve(RetrievalRequest(query="private", limit=2)) == ()
    with pytest.raises(MemoryError, match="memory_write_busy"):
        await memory.add("more", source=SOURCE, key="another")
    with pytest.raises(MemoryError, match="memory_operation_unresolved"):
        await memory.add("private", source=SOURCE, key="add")
    memory.close()
    restored = create()
    with pytest.raises(MemoryError, match="memory_writer_not_stopped"):
        await restored.reconcile("add")
    result = await restored.reconcile("add", writer_stopped=True)
    assert result.state == "complete" and len(result.ids) == 1 and len(sdk.rows) == 1


@pytest.mark.asyncio
async def test_unknown_update_hidden_and_reconciled(setup):
    sdk, create = setup
    memory = create()
    item_id, = (await memory.add("original", source=SOURCE, key="add")).ids
    sdk.fail_update = True
    with pytest.raises(MemoryError):
        await memory.update(item_id, "corrected", source=SOURCE, version=1, key="update")
    with pytest.raises(MemoryError, match="memory_write_busy"):
        await memory.get(item_id)
    await memory.reconcile("update", writer_stopped=True)
    assert (await memory.get(item_id)).text == "corrected"


@pytest.mark.asyncio
async def test_extraction_interrupted_before_receipt_is_not_blindly_accepted(setup):
    sdk, create = setup
    memory = create(allow_inference=True)
    sdk.fail_add = True
    with pytest.raises(MemoryError):
        await memory.extract([{"role": "user", "content": "candidate"}], source=SOURCE, key="extract")
    with pytest.raises(MemoryError, match="memory_reconciliation_required"):
        await memory.reconcile("extract", writer_stopped=True)
    assert await memory.retrieve(RetrievalRequest(query="candidate", limit=2)) == ()
    assert (await memory.discard_extraction("extract", writer_stopped=True)).state == "discarded"
    assert not sdk.rows
    await memory.add("safe", source=SOURCE, key="next")


@pytest.mark.asyncio
async def test_timeout_does_not_cancel_or_duplicate_sdk_write(setup):
    sdk, create = setup
    sdk.gate = threading.Event()
    memory = create(timeout_seconds=0.02)
    try:
        with pytest.raises(MemoryError, match="memory_timeout"):
            await memory.add("slow", source=SOURCE, key="add")
        assert memory.operation("add").state == "running"
        with pytest.raises(MemoryError, match="memory_operations_in_flight"):
            memory.close()
        with pytest.raises(MemoryError, match="memory_writer_not_stopped"):
            await memory.reconcile("add", writer_stopped=True)
        other = create()
        with pytest.raises(MemoryError, match="memory_write_busy"):
            await other.add("competing", source=SOURCE, key="other")
    finally:
        sdk.gate.set()
        await memory.drain()
    assert memory.operation("add").state == "complete"
    assert len(sdk.rows) == 1


@pytest.mark.asyncio
async def test_cancel_before_dispatch_and_after_dispatch(setup):
    sdk, create = setup
    memory = create()
    signal = asyncio.Event()
    signal.set()
    with pytest.raises(MemoryError, match="memory_cancelled"):
        await memory.add("no", source=SOURCE, key="before", signal=signal)
    assert memory.operation("before") is None and not sdk.calls
    signal.clear()
    sdk.gate = threading.Event()
    task = asyncio.create_task(memory.add("yes", source=SOURCE, key="after", signal=signal))
    try:
        await asyncio.to_thread(sdk.started.wait, 1)
        signal.set()
        with pytest.raises(MemoryError, match="memory_cancelled"):
            await task
    finally:
        sdk.gate.set()
        await memory.drain()
    assert memory.operation("after").state == "complete"


@pytest.mark.asyncio
async def test_tool_and_context_reuse_budget_and_receipts(setup):
    _, create = setup
    memory = create()
    small, = (await memory.add("简短回复", source=SOURCE, key="small")).ids
    await memory.add("很长的内容" * 100, source=SOURCE, key="large")
    tool = RetrieverTool(retriever=memory, name="recall", description="Recall memory.")
    result = await tool.registration.handler(ExecutionState(run_id="run"), {"query": "偏好"})
    assert len(json.loads(result.content)["hits"]) == 2
    context = MemoryContext(memory=memory, query=lambda _: "偏好", count_tokens=lambda text: len(text.encode()), limit=8)
    budget = ContextBudget(window_tokens=4000, output_reserve_tokens=1000, safety_reserve_tokens=0,
                           runtime_reserve_tokens=0, provider_input_tokens=3000, context_allocations={"memory": 200})
    bundle = await context.build_context(AgentRunRequest(messages=(), model=ModelRequest("test", "test"), domain_context=DomainContext("test")), budget)
    block, = bundle.blocks
    assert block.untrusted and block.token_count <= 200
    assert [row["id"] for row in json.loads(block.content)] == [small]
    receipts = block.host_metadata[CONTEXT_EVIDENCE_RECEIPTS_KEY]
    assert len(receipts) == 1 and receipts[0]["itemId"] == small


@pytest.mark.asyncio
async def test_inference_and_validation_are_explicit(setup):
    _, create = setup
    memory = create()
    with pytest.raises(MemoryError, match="memory_inference_disabled"):
        await memory.extract([{"role": "user", "content": "text"}], source=SOURCE, key="extract")
    with pytest.raises(ValueError):
        await memory.add("text", source=SOURCE, key="add", expires_at="2026-01-01")
    assert memory.operation("add") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("case", FIXTURE["scopes"])
async def test_shared_scope_encoding(setup, case):
    sdk, create = setup
    memory = create(scope=MemoryScope(**case["scope"]))
    await memory.add("fact", source=SOURCE, key="add")
    assert next(v for k, v in sdk.calls if k == "add")["user_id"] == case["namespace"]


@pytest.mark.asyncio
@pytest.mark.parametrize("expires", FIXTURE["invalid_expiry"])
async def test_shared_invalid_expiry(setup, expires):
    sdk, create = setup
    memory = create()
    with pytest.raises(ValueError):
        await memory.add("fact", source=SOURCE, key="add", expires_at=expires)
    assert not sdk.calls


@pytest.mark.asyncio
async def test_pagination_and_sdk_overlimit(setup):
    sdk, create = setup
    memory = create(max_results=1)
    first, = (await memory.add("first", source=SOURCE, key="1")).ids
    second, = (await memory.add("second", source=SOURCE, key="2")).ids
    assert [r.id for r in (await memory.list(limit=1)).items] == [first]
    assert [r.id for r in (await memory.list(limit=1, after=first)).items] == [second]
    sdk.search_override = {"results": list(copy.deepcopy(sdk.rows).values())}
    with pytest.raises(RetrievalError):
        await memory.retrieve(RetrievalRequest(query="fact", limit=1))


@pytest.mark.asyncio
async def test_process_crash_after_sdk_write_recovers_without_reexecution(tmp_path):
    journal = tmp_path / "journal.db"
    snapshot = tmp_path / "sdk.json"
    program = '''
import asyncio, json, os, sys
from pathlib import Path
from purra_mem0 import Mem0Memory, MemoryScope, MemorySource
from test_memory import Sdk
class CrashSdk(Sdk):
    def add(self, messages, **options):
        super().add(messages, **options)
        Path(sys.argv[2]).write_text(json.dumps(self.rows))
        os._exit(17)
memory = Mem0Memory(client=CrashSdk(), scope=MemoryScope("u", "p"), journal_path=sys.argv[1])
asyncio.run(memory.add("crash-safe", source=MemorySource("source", "1"), key="crash"))
'''
    root = Path(__file__).resolve().parents[4]
    env = {**os.environ, "PYTHONPATH": os.pathsep.join(map(str, [root / "src", root / "integrations/mem0/python/src", Path(__file__).parent]))}
    crashed = subprocess.run([sys.executable, "-c", program, str(journal), str(snapshot)], env=env, capture_output=True, timeout=10)
    assert crashed.returncode == 17, crashed.stderr.decode()
    client = Sdk()
    client.rows = json.loads(snapshot.read_text())
    memory = Mem0Memory(client=client, scope=MemoryScope("u", "p"), journal_path=str(journal))
    try:
        assert memory.operation("crash").state == "running"
        with pytest.raises(MemoryError, match="memory_write_busy"):
            await memory.add("competing", source=SOURCE, key="other")
        recovered = await memory.reconcile("crash", writer_stopped=True)
        assert recovered.state == "complete"
        assert (await memory.get(recovered.ids[0])).text == "crash-safe"
        assert not any(k == "add" for k, _ in client.calls)
    finally:
        await memory.drain()
        memory.close()


@pytest.mark.asyncio
async def test_cancelling_drain_does_not_release_a_running_worker(setup):
    sdk, create = setup
    sdk.gate = threading.Event()
    memory = create(timeout_seconds=0.02)
    try:
        with pytest.raises(MemoryError, match="memory_timeout"):
            await memory.add("slow", source=SOURCE, key="add")
        drain = asyncio.create_task(memory.drain())
        await asyncio.sleep(0)
        drain.cancel()
        with pytest.raises(asyncio.CancelledError):
            await drain
        with pytest.raises(MemoryError, match="memory_operations_in_flight"):
            memory.close()
    finally:
        sdk.gate.set()
        await memory.drain()
    assert memory.operation("add").state == "complete"


@pytest.mark.asyncio
@pytest.mark.parametrize('scenario', ['valid', 'empty', 'invented_quote', 'unknown_id', 'unknown_relation', 'extra_field', 'too_many', 'duplicate', 'stale', 'revoked', 'cancelled', 'oversize'])
async def test_relation_proposals_are_bounded_read_only_and_revalidated(setup, scenario):
    from purra_mem0 import MemoryRef, propose_memory_relations
    sdk, create = setup
    memory = create()
    a = (await memory.add('Alice maintains PURRA.', source=SOURCE, key='a')).ids[0]
    b = (await memory.add('PURRA is a framework.', source=SOURCE, key='b')).ids[0]
    refs = [MemoryRef(a, 1), MemoryRef(b, 1)]
    signal = asyncio.Event()
    epoch = memory.epoch
    entered = []

    async def extract(payload, received_signal):
        entered.append(payload)
        assert received_signal is signal
        assert payload['relations'] == ('maintains',)
        with pytest.raises(TypeError):
            payload['records'][0]['text'] = 'changed'
        item = {'from': a, 'to': b, 'relation': 'maintains', 'fromQuote': 'Alice', 'toQuote': 'PURRA'}
        if scenario == 'empty': return []
        if scenario == 'invented_quote': item['fromQuote'] = 'Bob'
        if scenario == 'unknown_id': item['to'] = 'outside-scope'
        if scenario == 'unknown_relation': item['relation'] = 'owns'
        if scenario == 'extra_field': item['confidence'] = 1
        if scenario == 'too_many': return [item] * 33
        if scenario == 'duplicate': return [item, item]
        if scenario == 'stale': await memory.update(a, 'Changed', source=SOURCE, version=1, key='changed')
        if scenario == 'revoked': await memory.revoke_source(SOURCE.id, revision=SOURCE.revision, key='revoke')
        if scenario == 'cancelled': signal.set()
        return [item]

    if scenario in ('valid', 'empty'):
        result = await propose_memory_relations(memory, refs, relations=['maintains'], extract=extract, signal=signal)
        assert len(result) == (1 if scenario == 'valid' else 0)
        if result:
            assert result[0].from_ref == refs[0] and result[0].to_ref == refs[1]
            assert result[0].from_source == SOURCE
        assert memory.epoch == epoch
        assert (await memory.links(a)).items == ()
    else:
        with pytest.raises((MemoryError, ValueError)) as failure:
            await propose_memory_relations(memory, refs, relations=['maintains'], extract=extract,
                max_input_chars=1 if scenario == 'oversize' else 32000, signal=signal)
        if scenario != 'oversize':
            expected = 'memory_context_stale' if scenario in ('stale', 'revoked') else 'memory_cancelled' if scenario == 'cancelled' else 'memory_invalid_relation_proposal'
            assert failure.value.code == expected
    assert len(entered) == (0 if scenario == 'oversize' else 1)


@pytest.mark.asyncio
async def test_relation_extractor_failures_and_preflight_do_not_call_or_retry(setup):
    from purra_mem0 import MemoryRef, propose_memory_relations
    _, create = setup
    memory = create()
    a = (await memory.add('A', source=SOURCE, key='a')).ids[0]
    b = (await memory.add('B', source=SOURCE, key='b')).ids[0]
    refs = [MemoryRef(a, 1), MemoryRef(b, 1)]
    calls = []
    async def fail(payload, signal):
        calls.append(payload)
        raise RuntimeError('host extractor failed')
    for changes in ({'relations': 'rel'}, {'relations': ['rel', 'rel']}, {'max_proposals': True}, {'max_input_chars': 0}):
        with pytest.raises((TypeError, ValueError)):
            await propose_memory_relations(memory, refs, extract=fail, **{'relations': ['rel'], **changes})
    with pytest.raises(MemoryError, match='memory_relation_stale'):
        await propose_memory_relations(memory, [MemoryRef(a, 2), refs[1]], relations=['rel'], extract=fail)
    assert calls == []
    with pytest.raises(RuntimeError, match='host extractor failed'):
        await propose_memory_relations(memory, refs, relations=['rel'], extract=fail)
    assert len(calls) == 1
    assert (await memory.links(a)).items == ()


@pytest.mark.asyncio
@pytest.mark.parametrize('scenario', ['combined', 'empty', 'zero_budget', 'cancelled', 'stale', 'malformed', 'failed'])
async def test_host_context_selection_reuses_authorized_assembly(setup, scenario):
    from purra_mem0 import MemoryRef
    sdk, create = setup
    memory = create()
    a = (await memory.add('First fact', source=SOURCE, key='a')).ids[0]
    b = (await memory.add('Related fact', source=SOURCE, key='b')).ids[0]
    await memory.link(MemoryRef(a, 1), MemoryRef(b, 1), 'related', key='edge')
    calls = []
    signal = asyncio.Event()
    request = AgentRunRequest(messages=(), model=ModelRequest('test', 'test'), domain_context=DomainContext('test'))
    async def select_ids(received, stop):
        assert received is request and stop is signal
        calls.append('select')
        if scenario == 'failed': raise RuntimeError('host policy failed')
        if scenario == 'malformed': return a
        if scenario == 'empty': return []
        if scenario == 'cancelled': signal.set(); return [a]
        if scenario == 'stale':
            await memory.revoke_source(SOURCE.id, key='withdraw')
            return [a]
        hits = await memory.retrieve(RetrievalRequest(query='fact', limit=8), stop)
        links = await memory.links(a, direction='outgoing', valid_only=True)
        return [links.items[0].to_ref.id, hits[0].id, links.items[0].to_ref.id, 'missing']
    context = MemoryContext(memory=memory, select_ids=select_ids)
    budget = ContextBudget(window_tokens=4000, output_reserve_tokens=1000, safety_reserve_tokens=0,
        runtime_reserve_tokens=0, provider_input_tokens=3000,
        context_allocations={'memory': 0 if scenario == 'zero_budget' else 1000})
    if scenario in ('combined', 'empty', 'zero_budget'):
        result = await context.build_context(request, budget, signal)
        if scenario == 'combined':
            block, = result.blocks
            assert [row['id'] for row in json.loads(block.content)] == [b, a]
            assert block.untrusted
            assert [r['itemId'] for r in block.host_metadata[CONTEXT_EVIDENCE_RECEIPTS_KEY]] == [b, a]
            assert len([c for c in sdk.calls if c[0] == 'search']) == 1
        else: assert result.blocks == ()
    else:
        expected = {'cancelled': 'memory_cancelled', 'stale': 'memory_context_stale',
                    'malformed': 'ids must be a sequence', 'failed': 'host policy failed'}[scenario]
        with pytest.raises((MemoryError, TypeError, RuntimeError), match=expected):
            await context.build_context(request, budget, signal)
        assert not [c for c in sdk.calls if c[0] == 'search']
    assert len(calls) == (0 if scenario == 'zero_budget' else 1)


def test_context_requires_one_host_selection_mode(setup):
    _, create = setup
    memory = create()
    for options in ({}, {'query': lambda _: '', 'select_ids': lambda *_: []}, {'select_ids': 1}):
        with pytest.raises(TypeError): MemoryContext(memory=memory, **options)


@pytest.mark.asyncio
async def test_context_selection_snapshots_host_ids_before_async_read(setup):
    from purra_mem0 import assemble_memory_context
    _, create = setup
    memory = create()
    a = (await memory.add('A', source=SOURCE, key='a')).ids[0]
    ids = [a, 'missing']
    original = memory.select
    async def read(selected, **kwargs):
        ids.clear()
        return await original(selected, **kwargs)
    memory.select = read
    result = await assemble_memory_context(memory, ids, 1000)
    assert result.included == (a,)
    assert result.missing == ('missing',)


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['query', 'selection'])
async def test_context_rejects_unsupported_allowance_before_host_work(setup, mode):
    _, create = setup
    memory = create()
    calls = []
    def query(request):
        calls.append('query')
        return 'fact'
    async def select_ids(request, signal):
        calls.append('selection')
        return []
    context = MemoryContext(memory=memory, **({'query': query} if mode == 'query' else {'select_ids': select_ids}))
    budget = ContextBudget(window_tokens=2_000_000, output_reserve_tokens=0,
        safety_reserve_tokens=0, runtime_reserve_tokens=0, provider_input_tokens=2_000_000,
        context_allocations={'memory': 1_000_001})
    with pytest.raises(ValueError, match='invalid context allowance'):
        await context.build_context(object(), budget)
    assert calls == []


@pytest.mark.asyncio
async def test_assembly_rejects_noncallable_counter_before_storage_read(setup):
    from purra_mem0 import assemble_memory_context
    _, create = setup
    memory = create()
    calls = []
    async def select(*args, **kwargs):
        calls.append('read')
        return ()
    memory.select = select
    with pytest.raises(TypeError, match='count_tokens'):
        await assemble_memory_context(memory, [], 100, count_tokens=7)
    assert calls == []
