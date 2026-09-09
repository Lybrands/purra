from dataclasses import replace
import pytest
from purra.agent_execution_checkpoint import AgentExecutionCheckpoint, AgentToolExecutionCheckpoint
from purra.contracts import AgentMessage, ToolCall
from purra.storage.codec import dump_storage_value, load_storage_value


def checkpoint():
    return AgentToolExecutionCheckpoint(run_id='run',next_round=1,round_limit=3,
        messages=(AgentMessage('user','write fixture'),),
        assistant=AgentMessage('assistant','before tool',reasoning='private reasoning',
            tool_calls=(ToolCall('call','write','{"value":1}'),),provider_data={'continuation':'opaque'}),
        invocation_id='invocation',model_budget_key='budget',allowed_tool_names=('write',))


def test_tool_ready_roundtrip_keeps_real_continuation_and_legacy_v2_contract():
    saved=checkpoint()
    assert AgentToolExecutionCheckpoint.from_mapping(saved.to_mapping())==saved
    assert load_storage_value(dump_storage_value(saved))==saved
    assert saved.assistant.provider_data['continuation']=='opaque'
    assert saved.next_round==1
    with pytest.raises(ValueError):AgentExecutionCheckpoint.from_mapping(saved.to_mapping())
    old=AgentExecutionCheckpoint(run_id='run',next_round=1,round_limit=3,messages=saved.messages)
    assert AgentExecutionCheckpoint.from_mapping(old.to_mapping())==old


@pytest.mark.parametrize('change',[
    {'phase':'model_ready'},{'schema_version':2},{'invocation_id':''},{'model_budget_key':''},
    {'allowed_tool_names':('other',)},{'next_round':3},
    {'assistant':AgentMessage('assistant','no call')},
    {'assistant':AgentMessage('assistant','',tool_calls=(ToolCall('a','write','{}'),ToolCall('b','write','{}')))},
])
def test_tool_ready_rejects_invalid_or_unsupported_boundaries(change):
    with pytest.raises((TypeError,ValueError)):replace(checkpoint(),**change)


def test_clarification_update_cannot_rewrite_pending_tool_boundary():
    from purra.interaction import is_input_checkpoint_update
    current=checkpoint()
    updated=replace(current,input_revision=1,messages=(*current.messages,AgentMessage('user','changed',attributes={'inputRequestId':'fixture'})))
    assert not is_input_checkpoint_update(current,updated)
