import pytest

from purra_mem0 import MemoryError, MemorySource
from test_memory import SOURCE, setup


@pytest.mark.asyncio
async def test_pending_metadata_and_control_changes_survive_restart_without_sdk_writes(setup):
    sdk, create = setup
    memory = create()
    operation = await memory.add("未确认事实", source=SOURCE, key="create", state="pending",
                                 metadata={"kind": "plot", "pinned": False})
    item_id, = operation.ids
    assert await memory.get(item_id) is None
    item = await memory.get(item_id, include_inactive=True)
    assert item.state == "pending" and not item.inferred and item.created_at
    assert sdk.rows[item_id]["metadata"]["purra_state"] == "pending"
    with pytest.raises(TypeError):
        item.metadata["pinned"] = True
    before = len(sdk.histories[item_id])
    annotated = await memory.annotate(item_id, {"kind": "plot", "pinned": True}, version=1, key="pin")
    await memory.set_state(item_id, "active", version=2, key="accept")
    assert len(sdk.histories[item_id]) == before
    current = await memory.get(item_id)
    assert current.metadata["pinned"] is True and current.version == 3
    memory.close()
    restored = create()
    assert (await restored.get(item_id)).metadata["pinned"] is True
    assert await restored.annotate(item_id, {"pinned": True, "kind": "plot"}, version=1, key="pin") == annotated
    with pytest.raises(MemoryError, match="memory_idempotency_conflict"):
        await restored.annotate(item_id, {"pinned": False}, version=1, key="pin")
    await restored.update(item_id, "确认后的更正", source=MemorySource(SOURCE.id, "4"), version=3, key="correct")
    corrected = await restored.get(item_id)
    assert corrected.metadata["pinned"] is True and corrected.created_at == item.created_at
    await restored.set_state(item_id, "disabled", reason="archived", version=4, key="archive")
    assert (await restored.get(item_id, include_inactive=True)).reason == "archived"
    assert await restored.get(item_id) is None


@pytest.mark.asyncio
async def test_filtered_pages_advance_even_when_a_scan_finds_no_matches(setup):
    _, create = setup
    memory = create()
    ids = []
    for index in range(5):
        ids.extend((await memory.add(f"记录 {index}", source=SOURCE, key=f"add:{index}",
                                    metadata={"kind": "plot" if index == 4 else "other"})).ids)
    first = await memory.list(filters={"kind": "plot"}, limit=1, scan_limit=2)
    assert first.items == () and first.next == ids[1]
    second = await memory.list(filters={"kind": ["plot"]}, limit=1, scan_limit=2, after=first.next)
    assert second.items == () and second.next == ids[3]
    final = await memory.list(filters={"kind": "plot"}, limit=1, scan_limit=2, after=second.next)
    assert [record.id for record in final.items] == [ids[4]] and final.next is None
    assert (await memory.list(source="other", query="记录")).items == ()


@pytest.mark.asyncio
async def test_control_rejects_stale_versions_revocation_and_unknown_writer(setup):
    sdk, create = setup
    memory = create()
    item_id, = (await memory.add("事实", source=SOURCE, key="add")).ids
    await memory.annotate(item_id, {"kind": "plot"}, version=1, key="classify")
    with pytest.raises(MemoryError, match="memory_version_conflict"):
        await memory.set_state(item_id, "disabled", version=1, key="stale")
    sdk.fail_add = True
    with pytest.raises(MemoryError):
        await memory.add("unknown", source=SOURCE, key="unknown")
    with pytest.raises(MemoryError, match="memory_write_busy"):
        await memory.annotate(item_id, {}, version=2, key="blocked")
    await memory.reconcile("unknown", writer_stopped=True)
    await memory.revoke_source(SOURCE.id, key="revoke")
    with pytest.raises(MemoryError, match="memory_source_revoked"):
        await memory.set_state(item_id, "active", version=2, key="revoked")


@pytest.mark.asyncio
@pytest.mark.parametrize("metadata", [{"purra_scope": "foreign"}, {"x": float("nan")},
                                    {"x": []}, {"__proto__": "unsafe"}, {"n": 2**53}])
async def test_metadata_rejects_control_fields_and_non_scalar_values(setup, metadata):
    sdk, create = setup
    memory = create()
    with pytest.raises(ValueError):
        await memory.add("fact", source=SOURCE, key="invalid", metadata=metadata)
    assert sdk.calls == [] and memory.operation("invalid") is None

@pytest.mark.asyncio
async def test_links_preserve_visibility_and_audit_stale_or_revoked_endpoints(setup):
    from purra_mem0 import MemoryRef, MemoryScope
    sdk, create = setup
    memory = create()
    a, = (await memory.add("事实 A", source=SOURCE, key="a")).ids
    b, = (await memory.add("事实 B", source=SOURCE, key="b")).ids
    refs = (MemoryRef(a, 1), MemoryRef(b, 1))
    before = {item_id: len(rows) for item_id, rows in sdk.histories.items()}
    operation = await memory.link(*refs, "supports", key="link", note="人工确认")
    assert operation.usage.embedding_calls == operation.usage.llm_calls == 0
    assert (await memory.get(a)).version == 1
    assert {item_id: len(rows) for item_id, rows in sdk.histories.items()} == before
    assert (await memory.links(a)).items[0].valid
    memory.close()
    restored = create()
    assert await restored.link(*refs, "supports", key="link", note="人工确认") == operation
    outsider = create(scope=MemoryScope("foreign", "project"))
    assert not (await outsider.links(a)).items
    with pytest.raises(MemoryError, match="memory_not_found"):
        await outsider.link(*refs, "supports", key="foreign")
    await restored.annotate(a, {"pinned": True}, version=1, key="pin")
    assert not (await restored.links(a)).items[0].valid
    with pytest.raises(MemoryError, match="memory_version_conflict"):
        await restored.link(*refs, "supports", key="stale")
    await restored.link(MemoryRef(a, 2), refs[1], "relates_to", key="link-2")
    page = await restored.links(a, limit=1)
    assert page.next == "link" and not page.items[0].valid
    assert (await restored.links(a, limit=1, after=page.next)).items[0].valid
    await restored.revoke_source(SOURCE.id, key="withdraw")
    assert all(not item.valid for item in (await restored.links(a)).items)


@pytest.mark.asyncio
async def test_explicit_selection_context_reports_whole_deferred_and_missing_records(setup):
    from purra_mem0 import assemble_memory_context
    sdk, create = setup
    memory = create()
    large, = (await memory.add("长" * 3000, source=SOURCE, key="large")).ids
    small, = (await memory.add("短事实", source=SOURCE, key="small", metadata={"kind": "plot"})).ids
    pending, = (await memory.add("待审", source=SOURCE, key="pending", state="pending")).ids
    before = len(sdk.calls)
    result = await assemble_memory_context(memory, (large, small, pending, "missing", small), 300)
    assert result.included == (small,) and result.deferred == (large,) and result.missing == (pending, "missing")
    assert len(result.receipts) == 1 and result.receipts[0].item_id == small
    assert '短事实' in result.block.content and '长' not in result.block.content
    assert all(call[0] == "get" for call in sdk.calls[before:])
    epoch = memory.epoch
    await memory.set_state(small, "disabled", version=1, key="disable")
    with pytest.raises(MemoryError, match="memory_context_stale"):
        await assemble_memory_context(memory, (small,), 300, expected_epoch=epoch)


@pytest.mark.asyncio
async def test_shared_management_filter_contract(setup):
    import json
    from pathlib import Path
    spec = json.loads((Path(__file__).resolve().parents[2] / "fixtures/memory.json").read_text())["management"]
    _, create = setup
    memory = create()
    ids = [(await memory.add(f"事实 {index}", source=SOURCE, key=f"item:{index}", metadata=metadata)).ids[0]
           for index, metadata in enumerate(spec["metadata"])]
    for case in spec["filters"]:
        result = await memory.list(filters=case["value"])
        assert [item.id for item in result.items] == [ids[index] for index in case["indices"]]
        assert result.next is None

@pytest.mark.asyncio
async def test_external_metadata_type_changes_cannot_masquerade_as_verified_payload(setup):
    sdk, create = setup
    memory = create()
    item_id, = (await memory.add("事实", source=SOURCE, key="add", metadata={"pinned": True})).ids
    sdk.rows[item_id]["metadata"]["purra_metadata"]["pinned"] = 1
    with pytest.raises(MemoryError, match="memory_record_changed"):
        await memory.get(item_id)
