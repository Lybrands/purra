"""Versioned adapter state is independent of ambient imports and private fields."""
import asyncio
from dataclasses import dataclass
import json
import subprocess
import sys

import pytest

from purra.contracts import RunCreateParams, ToolCall, ToolHandlerResult
from purra.events import AgentEvent
from purra.storage import StorageSession, dump_storage_value, load_storage_value


@pytest.mark.asyncio
async def test_state_survives_fresh_interpreter_and_excludes_private_cache(tmp_path):
    session = StorageSession()
    run = await session.runs.begin(RunCreateParams(None, "persist", None), AgentEvent("run.started"))
    session.save_tool_receipt((run.run_id, "call"), ToolCall("call", "lookup", "{}"), ToolHandlerResult("found"))
    # A new in-memory cache must not silently become persisted data.
    session.runs._state.unrelated_cache = object()
    body = session.export_snapshot()
    assert "unrelated_cache" not in body and "purra.adapters.memory" not in body
    path = tmp_path / "state.json"
    path.write_text(body)
    result = subprocess.run([sys.executable, "-c", '''
import asyncio, sys
from pathlib import Path
from purra.storage import StorageSession
s = StorageSession(Path(sys.argv[1]).read_text())
assert s.running_run_ids() == (sys.argv[2],)
assert s.get_tool_receipt((sys.argv[2], "call"))[1].content == "found"
assert asyncio.run(s.runs.get(sys.argv[2])).status.value == "running"
print("restored")
''', str(path), run.run_id], capture_output=True, text=True, check=True)
    assert result.stdout.strip() == "restored"


def test_codec_uses_only_explicit_record_ids_even_if_other_dataclasses_are_imported():
    @dataclass
    class Unregistered:
        field: str
    with pytest.raises(ValueError, match="unsupported storage value"):
        dump_storage_value(Unregistered("value"))
    encoded = json.loads(dump_storage_value(ToolCall("call", "lookup", "{}")))
    assert encoded[1] == "ToolCall"
    encoded[1] = "purra.contracts.ToolCall"
    with pytest.raises(ValueError):
        load_storage_value(json.dumps(encoded))


@pytest.mark.parametrize("change", [
    lambda s: s.update(schema="purra.storage-state/python/v0"),
    lambda s: s["groups"]["run"].pop("runs"),
    lambda s: s["groups"]["run"].update(private_cache={}),
    lambda s: s["groups"]["run"].update(runs=[]),
    lambda s: s["groups"]["run"].update(run_count=-1),
    lambda s: s.update(leases=[]),
])
def test_rejects_unsupported_schema_or_storage_fields(change):
    state = load_storage_value(StorageSession().export_snapshot())
    change(state)
    with pytest.raises(ValueError):
        StorageSession(dump_storage_value(state))


@pytest.mark.parametrize("body", [
    '["record","ToolCall",{}]',
    '["value",{}]',
    '["map",[[["value","same"],["value",1]],[["value","same"],["value",2]]]]',
    '["record","ToolCall",{"id":["value","a"],"id":["value","b"]}]',
    '["enum","RunStatus","unknown"]',
])
def test_rejects_malformed_tagged_values(body):
    with pytest.raises(ValueError):
        load_storage_value(body)
