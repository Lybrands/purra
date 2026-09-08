from unittest.mock import patch

import pytest
from purra.storage import StorageMetadataCache, StorageSession, dump_storage_value, load_storage_value


def test_metadata_cache_reuses_only_validation_and_preserves_canonical_state():
    body = StorageSession().export_snapshot()
    cache = StorageMetadataCache()
    with patch('purra.storage.metadata.StorageSession', wraps=StorageSession) as constructor:
        first = cache.open(body)
        first.extra['cursor'] = {'revision': 1}
        updated = first.export_snapshot()
        second = cache.open(updated)
        assert second.extra == {'cursor': {'revision': 1}}
        second.extra['cursor']['revision'] = 2
        assert first.extra['cursor']['revision'] == 1
        assert constructor.call_count == 1
    before, after = load_storage_value(body), load_storage_value(updated)
    assert {k: v for k, v in before.items() if k != 'extra'} == {k: v for k, v in after.items() if k != 'extra'}
    assert StorageSession(updated).extra == {'cursor': {'revision': 1}}


@pytest.mark.parametrize('section', ['schema', 'groups', 'claims', 'leases', 'extra'])
def test_metadata_cache_rejects_corruption_after_warmup(section):
    body = StorageSession().export_snapshot(); cache = StorageMetadataCache(); cache.open(body)
    saved = load_storage_value(body); saved[section] = []
    with pytest.raises(ValueError): cache.open(dump_storage_value(saved))
    assert cache.open(body).extra == {}


def test_metadata_cache_distinguishes_boolean_and_integer_and_revalidates_changes():
    body = StorageSession().export_snapshot(); cache = StorageMetadataCache(); cache.open(body)
    saved = load_storage_value(body)
    saved['groups']['run']['run_count'] = False
    with pytest.raises(ValueError): cache.open(dump_storage_value(saved))
    saved['groups']['run']['run_count'] = 1
    with patch('purra.storage.metadata.StorageSession', wraps=StorageSession) as constructor:
        cache.open(dump_storage_value(saved)); assert constructor.call_count == 1


@pytest.mark.parametrize('body', [
    '["map",[]]',
    '["value",null]',
    '["map",[[["value","extra"],["map",[]]],[["value","extra"],["map",[]]]]]',
])
def test_metadata_cache_rejects_invalid_envelopes(body):
    with pytest.raises(ValueError): StorageMetadataCache().open(body)


@pytest.mark.asyncio
async def test_metadata_roundtrip_preserves_run_receipts_and_rejects_changed_root():
    from purra.contracts import RunCreateParams, ToolCall, ToolHandlerResult
    from purra.events import AgentEvent
    session = StorageSession()
    run = await session.runs.begin(RunCreateParams(None, 'metadata', None), AgentEvent('run.started'))
    call = ToolCall('call', 'write', '{}')
    session.save_tool_receipt((run.run_id, call.id), call, ToolHandlerResult('written', effect_state='committed'))
    cache = StorageMetadataCache(); metadata = cache.open(session.export_snapshot())
    metadata.extra['recoveryScheduleRevision'] = 1
    restored = StorageSession(metadata.export_snapshot())
    assert (await restored.runs.get(run.run_id)) == (await session.runs.get(run.run_id))
    assert restored.get_tool_receipt((run.run_id, call.id)) == session.get_tool_receipt((run.run_id, call.id))
    state = load_storage_value(metadata.export_snapshot())
    from dataclasses import replace
    record = state['groups']['run']['runs'][run.run_id]
    state['groups']['run']['runs'][run.run_id] = replace(record, params=replace(record.params, root_run_id='missing'))
    with pytest.raises(ValueError): cache.open(dump_storage_value(state))
