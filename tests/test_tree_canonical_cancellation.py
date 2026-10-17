"""Tree cancellation and canonical Child lifecycle share one storage transition."""
from datetime import datetime,timezone
import pytest
from purra.adapters import InMemoryAgentAdapters
from purra.agent_tree import AgentCapabilityGrant,BeginRootAgentCommand,ChildAgentSpec,SpawnAgentsCommand
from purra.agent_tree.lease import bind_agent_run_lease
from purra.contracts import RunCreateParams,RunStatus
from purra.events import AgentEvent
from purra.errors import ContractViolationError
from purra.output import AgentOutputEventDraft,OutputSource,OutputChannel,OutputEventKind,OutputVisibility,RunLifecycleOutputDraft,OutputStreamSpec,AgentOutputIntent,OutputCommitMode
from purra.ports import RunCommit
from purra.storage import StorageSession


async def fixture():
    session=StorageSession();a=session
    await a.outputs.begin_run_lifecycle(RunCreateParams(None,'root',None,requested_run_id='root',root_run_id='root',agent_id='root-agent'),AgentEvent('run.started'))
    await a.run_tree.begin_root(BeginRootAgentCommand(run_id='root',agent_id='root-agent',name='root',title='Root',instruction='Own work',objective='Test cancellation',idempotency_key='root',capability_grant=AgentCapabilityGrant(can_spawn_agents=True)))
    items=(await a.run_tree.spawn_agents(SpawnAgentsCommand(parent_run_id='root',idempotency_key='spawn',children=tuple(ChildAgentSpec(name=n,title=n,instruction='Wait for a source',objective='Read') for n in ['a','b','queued'])))).items
    await a.run_tree.mark_waiting('root');claims=[]
    for item in items[:2]:
        c=await a.run_tree.claim_run(item.run.run_id,owner_id='worker-'+item.agent.name,lease_duration_ms=30000);claims.append(c)
        with bind_agent_run_lease(c.run_id,c.lease_owner_id,c.lease_epoch):
            await a.outputs.begin_run_lifecycle(RunCreateParams(None,'child',None,requested_run_id=c.run_id,root_run_id='root',agent_id=c.agent_id,parent_run_id='root',lease_owner_id=c.lease_owner_id,lease_epoch=c.lease_epoch),AgentEvent('run.started'))
    return session,a,claims,items[2].run


@pytest.mark.asyncio
@pytest.mark.parametrize('transition',['cancel','ancestor_failure'])
async def test_cancel_is_atomic_durable_and_keeps_late_workers_fenced(transition):
    session,a,claims,queued=await fixture()
    if transition=='cancel':await a.run_tree.cancel_subtree('root')
    else:await a.run_tree.fail_run('root','synthetic_failure')
    assert (await a.runs.get('root')).status is RunStatus.RUNNING  # Root lifecycle belongs to its controller.
    for c in claims:
        assert (await a.runs.get(c.run_id)).status is RunStatus.CANCELED
        assert (await a.run_tree.get_run(c.run_id)).status.value=='canceled'
        with bind_agent_run_lease(c.run_id,c.lease_owner_id,c.lease_epoch):
            with pytest.raises(ContractViolationError):await a.runs.reserve_model_attempt(c.run_id,'late-send')
            with pytest.raises(ContractViolationError):await a.outputs.append_event(AgentOutputEventDraft(run_id=c.run_id,turn_id=None,output_stream_id=None,invocation_id=None,source_event_key='late',source=OutputSource.RUNTIME,kind=OutputEventKind.RUNTIME,channel=OutputChannel.DIAGNOSTIC,visibility=OutputVisibility.PRIVATE,payload={},occurred_at=datetime.now(timezone.utc)))
    assert session.get_run_info(queued.run_id) is None
    before=session.export_snapshot();_,journal=session.output_delta()
    assert await a.run_tree.cancel_subtree('root')==()
    assert session.export_snapshot()==before
    restored=StorageSession(before);restored.restore_output_events(sorted(journal,key=lambda e:e.root_sequence))
    assert all(restored.get_run_info(c.run_id).status is RunStatus.CANCELED for c in claims)
    assert sum(e.kind is OutputEventKind.RUN_LIFECYCLE and e.payload.get('status')=='canceled' for e in journal)==2


@pytest.mark.asyncio
async def test_one_conflicting_child_lifecycle_keeps_entire_subtree_unchanged():
    session,a,claims,_=await fixture();c=claims[1]
    with bind_agent_run_lease(c.run_id,c.lease_owner_id,c.lease_epoch):
        await a.outputs.append_event(AgentOutputEventDraft(run_id=c.run_id,turn_id=None,output_stream_id=None,invocation_id=None,source_event_key=f'run:{c.run_id}:canceled',source=OutputSource.RUNTIME,kind=OutputEventKind.RUNTIME,channel=OutputChannel.DIAGNOSTIC,visibility=OutputVisibility.PRIVATE,payload={},occurred_at=datetime.now(timezone.utc)))
    before=session.export_snapshot();_,events=session.output_delta()
    with pytest.raises(ContractViolationError):await a.run_tree.cancel_subtree('root')
    assert session.export_snapshot()==before and session.output_delta()[1]==events


@pytest.mark.asyncio
async def test_completed_child_is_preserved_and_open_child_stream_is_aborted():
    session,a,claims,_=await fixture();done,waiting=claims
    with bind_agent_run_lease(done.run_id,done.lease_owner_id,done.lease_epoch):
        await a.outputs.commit_run_lifecycle(done.run_id,RunCommit(terminal_status=RunStatus.DONE,final_response='original',events=(AgentEvent('run.completed',run_id=done.run_id),)),RunLifecycleOutputDraft(source_event_key=f'run:{done.run_id}:done',status=RunStatus.DONE,payload={'status':'done'},occurred_at=datetime.now(timezone.utc)))
        await a.run_tree.complete_run(done.run_id,expected_context_version=0,result={'content':'original'},content_ref='original',fingerprint='original',lease_owner_id=done.lease_owner_id,lease_epoch=done.lease_epoch)
    with bind_agent_run_lease(waiting.run_id,waiting.lease_owner_id,waiting.lease_epoch):
        await a.outputs.open_stream(OutputStreamSpec(output_stream_id='unfinished',run_id=waiting.run_id,turn_id=None,invocation_id='inv',intent=AgentOutputIntent.FINAL_PUBLIC,commit_mode=OutputCommitMode.LIVE))
    await a.run_tree.cancel_subtree('root')
    assert (await a.runs.get(done.run_id)).status is RunStatus.DONE
    assert (await a.runs.get(done.run_id)).final_response=='original'
    assert (await a.runs.get(waiting.run_id)).status is RunStatus.CANCELED
    _,events=session.output_delta()
    assert sum(e.kind is OutputEventKind.STREAM_ABORTED for e in events)==1
