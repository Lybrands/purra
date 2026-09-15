import asyncio, json
import pytest
from purra.agent_tree import *
from purra.agent_tree_execution import RunCommandService
from purra.api import AgentCore, AgentPreset, AgentTreePolicy, InMemoryAgentAdapters, AgentCoreRunOptions
from purra.contracts import ModelStream, ModelStreamChunk, ModelFinishReason, ToolCallDelta, RuntimeLimits, ResponseValidationResult
from purra.tools import InMemoryToolCatalog
from test_second_host_conformance import _request

@pytest.mark.asyncio
@pytest.mark.parametrize("restore", [False, True])
async def test_agent_discovery_survives_continuation(restore):
    from purra.storage import StorageSession
    storage = StorageSession()
    repo = storage.run_tree
    commands = RunCommandService(repo)
    grant=AgentCapabilityGrant(can_spawn_agents=True, max_parallel_runs=3)
    root=await repo.begin_root(BeginRootAgentCommand(run_id='r',agent_id='a',name='root',title='Root',instruction='Own',objective='Work',capability_grant=grant,idempotency_key='r'))
    async def spawn(parent,name):
        return (await repo.spawn_agents(SpawnAgentsCommand(parent_run_id=parent.run_id,lease_owner_id=parent.lease_owner_id,lease_epoch=parent.lease_epoch if parent.lease_owner_id else None,idempotency_key=name,children=(ChildAgentSpec(name=name,title=name,instruction='Own this capability',objective='Work',capability_grant=grant),)))).items[0]
    a=await spawn(root,'specialist')
    sibling=await spawn(root,'sibling')
    ar=await repo.claim_run(a.run.run_id,owner_id='w',lease_duration_ms=30000)
    b=await spawn(ar,'reviewer')
    br=await repo.claim_run(b.run.run_id,owner_id='w',lease_duration_ms=30000)
    async def finish(run):
        await repo.complete_run(run.run_id,expected_context_version=0,result='ok',content_ref='memory://ok',fingerprint='ok',lease_owner_id=run.lease_owner_id,lease_epoch=run.lease_epoch)
    await finish(br); await finish(ar)
    a2=await commands.continue_agent(ContinueAgentCommand(requester_run_id='r',agent_id=a.agent.agent_id,expected_context_version=1,message='Next',idempotency_key='a2'))
    a2r=await repo.claim_run(a2.run.run_id,owner_id='w',lease_duration_ms=30000)
    if restore:
        storage = StorageSession(storage.export_snapshot())
        repo = storage.run_tree
        commands = RunCommandService(repo)
    visible=(await commands.agents.list(a2r.run_id))["agents"]
    b2=await commands.continue_agent(ContinueAgentCommand(requester_run_id=a2r.run_id,lease_owner_id=a2r.lease_owner_id,lease_epoch=a2r.lease_epoch,agent_id=b.agent.agent_id,expected_context_version=1,message='Next review',idempotency_key='b2'))
    assert [item['agentId'] for item in visible] == [b.agent.agent_id]
    assert b2.agent.agent_id == b.agent.agent_id
    from purra.errors import ContractViolationError
    for inaccessible in (root.agent_id, sibling.agent.agent_id):
        with pytest.raises(ContractViolationError) as rejected:
            await commands.agents.describe(a2r.run_id, inaccessible)
        assert rejected.value.code == 'agent_scope_violation' 

@pytest.mark.asyncio
async def test_parent_output_validator_is_not_inherited():
    checked=[]; adapters=InMemoryAgentAdapters()
    class Validator:
        def validate(self,*,content,messages):
            checked.append(content)
            return ResponseValidationResult() if content=='ROOT FINAL' else ResponseValidationResult(violation_code='root_format_required',repair_guidance='Produce ROOT FINAL')
    class Gateway:
        async def complete(self,*a,**k): raise AssertionError()
        async def stream(self,messages,invocation,signal=None):
            async def chunks():
                if any(m.content=='SPECIALIST' for m in messages):
                    yield ModelStreamChunk(content_delta='child evidence',finish_reason=ModelFinishReason.STOP)
                elif any(m.role.value=='tool' for m in messages):
                    yield ModelStreamChunk(content_delta='ROOT FINAL',finish_reason=ModelFinishReason.STOP)
                else:
                    args={'children':[{'name':'review','title':'Review','instruction':'SPECIALIST','objective':'Find evidence'}]}
                    yield ModelStreamChunk(tool_call_deltas=(ToolCallDelta(index=0,id='spawn',type='function',name='delegateToAgents',arguments_fragment=json.dumps(args)),),finish_reason=ModelFinishReason.TOOL_CALLS)
            return ModelStream(chunks=chunks(),model='operations-model',applied_generation_limit=invocation.output_budget.max_generation_tokens)
    core=AgentCore(model_gateway=Gateway(),run_repository=adapters.runs,output_repository=adapters.outputs,output_publisher=adapters.publisher,run_tree_repository=adapters.run_tree,preset=AgentPreset(id='review',revision='1',tool_catalog=InMemoryToolCatalog(()),runtime_limits=RuntimeLimits(max_run_generation_tokens=None),agent_tree_policy=AgentTreePolicy()))
    try:
        h=await core.submit(_request(),options=AgentCoreRunOptions(response_validators=(Validator(),)))
        result=await asyncio.wait_for(h.wait(),5)
        assert result.status.value == 'done', result.error
        assert checked == ['ROOT FINAL']
    finally: await core.close()


@pytest.mark.asyncio
async def test_inventory_pages_and_result_receipts_remain_bounded():
    from purra.agent_tree_tool import AgentToolContext, build_agent_tree_tools
    from purra.contracts import ExecutionState, ToolCall, ToolExecutionLimits
    repo = InMemoryRunTreeRepository()
    commands = RunCommandService(repo)
    await repo.begin_root(BeginRootAgentCommand(run_id='root', agent_id='root-agent', name='root', title='Root',
        instruction='Own', objective='Work', capability_grant=AgentCapabilityGrant(can_spawn_agents=True), idempotency_key='root'))
    runs = []
    for index in range(15):
        receipt = await commands.spawn_agents(SpawnAgentsCommand(parent_run_id='root', idempotency_key=str(index),
            children=(ChildAgentSpec(name=f'{index:02d}'+'n'*62, title='t'*120, instruction='i'*4000, objective='work'),)))
        run = await repo.claim_run(receipt.items[0].run.run_id, owner_id='worker', lease_duration_ms=30000)
        await repo.complete_run(run.run_id, expected_context_version=0, result='ok', content_ref='memory://ok', fingerprint='ok',
            lease_owner_id=run.lease_owner_id, lease_epoch=run.lease_epoch)
        runs.append(run)
    tools = {item.schema.name: item for item in build_agent_tree_tools(AgentToolContext(commands, AgentTreePolicy()))}
    state = ExecutionState(run_id='root')
    ids, cursor = [], None
    while True:
        args = {'limit': 4, **({'after': cursor} if cursor else {})}
        result = await tools['listAgents'].call_handler(state, args, ToolCall(id='list',name='listAgents',arguments_json=json.dumps(args)))
        assert len(result.content) < ToolExecutionLimits().max_result_chars
        page = json.loads(result.content)
        assert all('instruction' not in item for item in page['agents'])
        ids.extend(item['agentId'] for item in page['agents'])
        cursor = page['nextCursor']
        if cursor is None: break
    assert len(ids) == len(set(ids)) == 15
    details = await commands.agents.describe('root', ids[0], detailed=True)
    assert len(details['instruction']) == 4000
    result = await tools['receiveAgentResults'].call_handler(state, {'runIds':[runs[0].run_id]},
        ToolCall(id='receive',name='receiveAgentResults',arguments_json='{}'))
    payload=json.loads(result.content)
    assert len(payload['agents']) == 1
    assert len(payload['results']) == 1
    assert len(result.content) < 2000
    await commands.results.close()


def test_child_options_inherit_only_execution_constraints():
    from types import SimpleNamespace
    from purra.engine.agent_context import child_run_options
    from purra.contracts import ResponseConstraints, RunBinding
    parent = AgentCoreRunOptions(
        require_tool_call=True, response_constraints=ResponseConstraints(exact_top_level_item_count=2),
        binding=RunBinding(namespace='test',aggregate_id='parent',command_id='parent'),
        deadline_at_ms=9000000000000, default_context_window_tokens=16000)
    run=SimpleNamespace(run_id='child',root_run_id='root',agent_id='agent',parent_run_id='root',lease_owner_id='worker',lease_epoch=1)
    child=child_run_options(parent, run, SimpleNamespace(capability_grant=AgentCapabilityGrant()), None)
    assert child.deadline_at_ms == parent.deadline_at_ms
    assert child.default_context_window_tokens == parent.default_context_window_tokens
    assert child.require_tool_call is None
    assert child.binding is None
    assert child.response_constraints == ResponseConstraints()
    assert child.response_transaction_policy.public_presentation.value == 'none'


def test_retired_window_configuration_is_not_accepted():
    with pytest.raises(TypeError):
        AgentTreePolicy(result_window_ms=2000)
