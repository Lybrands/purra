import asyncio
import json
from pathlib import Path

import pytest
from purra.contracts import ExecutionState, ToolBatchRequest, ToolCall, ToolExecutionLimits, ToolHandlerResult, ToolPolicy, ToolSchema
from purra.ports import ToolRegistration
from purra.tools import CoreToolExecutor, InMemoryToolCatalog

FIXTURE = json.loads((Path(__file__).parents[1]/'conformance/fixtures/tool_concurrency.json').read_text())


class Sink:
    def __init__(self, fail=False): self.events=[]; self.fail=fail
    async def emit(self,event):
        self.events.append(event)
        if self.fail: raise RuntimeError('observer failed')


class Harness:
    def __init__(self, *, concurrency=2, safe=True, failing=None, scope=None, sink=None):
        self.gates=[asyncio.Event() for _ in FIXTURE['calls']]
        self.started=[asyncio.Event() for _ in self.gates]
        self.active=0; self.peak=0; self.starts=[]; self.scopes=[]
        async def authorize(state,args,signal):
            self.scopes.append(args['index'])
            if scope: return await scope(args['index'],self.scopes.count(args['index']))
        async def handler(state,args,signal):
            index=args['index'];self.starts.append(index);self.active+=1;self.peak=max(self.peak,self.active)
            self.started[index].set()
            try:
                await self.gates[index].wait()
                return ToolHandlerResult(str(index),error_code='read_failed' if index==failing else None,effect_state='not_started')
            finally:self.active-=1
        registration=ToolRegistration(ToolSchema('read','Read',{'type':'object','properties':{'index':{'type':'integer'}},'required':['index'],'additionalProperties':False}),handler,ToolPolicy(mode='read',title='Read'),scope_validator=authorize,concurrency_safe=safe)
        self.executor=CoreToolExecutor(InMemoryToolCatalog((registration,)),limits=ToolExecutionLimits(max_concurrency=concurrency))
        self.request=ToolBatchRequest('run',tuple(ToolCall(str(i),'read',json.dumps({'index':i})) for i in FIXTURE['calls']),frozenset({'read'}),ExecutionState())
        self.signal=asyncio.Event();self.sink=sink or Sink()
    def run(self):return asyncio.create_task(self.executor.execute_batch(self.request,self.sink,self.signal))
    async def wait(self,index):await asyncio.wait_for(self.started[index].wait(),1)
    def release(self,index):self.gates[index].set()


@pytest.mark.parametrize('value',FIXTURE['invalidLimits'])
def test_invalid_concurrency(value):
    with pytest.raises(ValueError):ToolExecutionLimits(max_concurrency=value)


@pytest.mark.asyncio
@pytest.mark.parametrize('concurrency,safe,expected',[(1,True,1),(2,False,1),(2,True,2)])
async def test_opt_in_bound_and_order(concurrency,safe,expected):
    h=Harness(concurrency=concurrency,safe=safe);running=h.run()
    await h.wait(0)
    if expected==2:
        await h.wait(1)
        assert h.scopes[:4]==FIXTURE['calls']
        h.release(1);await h.wait(2);h.release(2);await h.wait(3);h.release(3);h.release(0)
    else:
        assert h.starts==[0]
        for i in FIXTURE['calls']:await h.wait(i);h.release(i)
    result=await asyncio.wait_for(running,1)
    assert h.peak==expected and h.active==0
    assert [row.tool_call_id for row in result.results]==list(map(str,FIXTURE['calls']))
    assert [row.content for row in result.results]==list(map(str,FIXTURE['calls']))
    assert result.outcome.value=='completed'
    indices=[event.payload['index'] for event in h.sink.events]
    assert sorted(indices)==FIXTURE['calls']
    if expected==2:assert indices[0]==1


@pytest.mark.asyncio
async def test_failure_closes_queue_and_keeps_inflight_success():
    h=Harness(failing=1);running=h.run();await h.wait(0);await h.wait(1)
    h.release(1)
    while not h.sink.events:await asyncio.sleep(0)
    h.release(0);result=await asyncio.wait_for(running,1)
    assert h.starts==[0,1] and h.active==0
    assert result.results[0].error is None and result.results[0].content=='0'
    assert [r.error for r in result.results]==[None,'read_failed',FIXTURE['skippedError'],FIXTURE['skippedError']]
    assert result.outcome.value=='failed'


@pytest.mark.asyncio
async def test_last_scope_denial_starts_nothing():
    async def scope(index,count):return 'denied' if index==3 else None
    h=Harness(scope=scope);result=await h.run()
    assert not h.starts and h.scopes==FIXTURE['calls']
    assert result.outcome.value=='rejected'


@pytest.mark.asyncio
async def test_dispatch_revalidates_scope():
    async def scope(index,count):return 'revoked' if index==2 and count>1 else None
    h=Harness(scope=scope);running=h.run();await h.wait(0);await h.wait(1);h.release(1)
    while h.scopes.count(2)<2:await asyncio.sleep(0)
    h.release(0);result=await asyncio.wait_for(running,1)
    assert h.starts==[0,1]
    assert result.results[2].error=='tool_scope_violation'


@pytest.mark.asyncio
async def test_cancel_drains_handlers_and_never_starts_queued_calls():
    h=Harness();running=h.run();await h.wait(0);await h.wait(1);h.signal.set()
    result=await asyncio.wait_for(running,1)
    assert h.starts==[0,1] and h.active==0
    assert result.outcome.value=='canceled'
    assert len(result.results)==len(FIXTURE['calls'])
    count=len(h.sink.events);await asyncio.sleep(0);assert len(h.sink.events)==count


@pytest.mark.asyncio
async def test_observer_failure_is_fatal_and_drains_siblings():
    h=Harness(sink=Sink(fail=True));running=h.run();await h.wait(0);await h.wait(1);h.release(1)
    with pytest.raises(RuntimeError,match='observer failed'):await asyncio.wait_for(running,1)
    assert h.active==0 and h.starts==[0,1]


@pytest.mark.asyncio
async def test_lease_fence_failure_prevents_dispatch_and_remains_fatal():
    from purra.errors import ContractViolationError
    from purra.operations import AgentOperationController
    class Output:
        async def accept_operation_event(self,event):
            raise ContractViolationError('stale lease',code='agent_run_lease_lost')
    h=Harness();h.executor._operations=AgentOperationController(Output())
    with pytest.raises(ContractViolationError) as error:await asyncio.wait_for(h.run(),1)
    assert error.value.code=='agent_run_lease_lost'
    assert h.starts==[] and h.active==0


@pytest.mark.asyncio
async def test_outer_task_cancellation_drains_sibling_tasks():
    h=Harness();running=h.run();await h.wait(0);await h.wait(1);running.cancel()
    with pytest.raises(asyncio.CancelledError):await running
    assert h.active==0 and h.starts==[0,1]
