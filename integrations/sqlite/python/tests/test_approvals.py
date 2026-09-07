import asyncio
import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest
from purra.approvals import ApprovalIntent, ApprovalDecisionCommand
from purra.contracts import RunCreateParams, ToolCall
from purra.events import AgentEvent
from purra.ports import RunCommit
from purra.structured import json_identity_digest
from purra_sqlite import SqliteAgentAdapters

FIXTURE = json.loads((Path(__file__).parents[4] / "conformance/fixtures/approval_records.json").read_text())


async def begin(storage, run_id="run-1", deadline=None):
    return await storage.runs.begin(RunCreateParams(None, "fixture", None, requested_run_id=run_id, deadline_at_ms=deadline), AgentEvent("run.started"))


def intent(run_id="run-1"):
    return ApprovalIntent.from_mapping({**FIXTURE["intent"], "runId": run_id, "rootRunId": run_id, "presetFingerprint": json_identity_digest({})})


def command(record, key="decision", decision="approve"):
    return ApprovalDecisionCommand(record.approval_id, record.revision, record.intent.digest, key, decision)


@pytest.mark.asyncio
async def test_explicit_activation_preserves_history_and_fences_preopened_legacy_writer(tmp_path):
    path=tmp_path/'db'
    storage=SqliteAgentAdapters(path,scope='fixture')
    legacy=sqlite3.connect(path,isolation_level=None)
    try:
        await begin(storage)
        await storage.runs.commit('run-1',RunCommit(terminal_status='done',final_response='done', events=(AgentEvent('run.completed', {}, 'run-1'),)))
        before=legacy.execute("SELECT body FROM purra_state WHERE sdk='python'").fetchone()[0]
        before_events=await storage.outputs.list_events("run-1",after_sequence=0)
        await storage.enable_approvals()
        assert await storage.outputs.list_events("run-1",after_sequence=0)==before_events
        assert legacy.execute("SELECT body FROM purra_state WHERE sdk='python'").fetchone()[0]==before
        assert legacy.execute('SELECT 1 FROM purra_state WHERE version != 4 LIMIT 1').fetchone()
        with pytest.raises(sqlite3.IntegrityError,match='unsupported SQLite storage version'):
            legacy.execute("INSERT INTO purra_state VALUES('new', 'python', 4, '{}')")
        with pytest.raises(sqlite3.IntegrityError,match='unsupported SQLite storage version'):
            legacy.execute("UPDATE purra_state SET version=4 WHERE sdk='python'")
        assert (await storage.runs.get('run-1')).final_response=='done'
        await storage.enable_approvals()
        storage.close();storage=SqliteAgentAdapters(path,scope='fixture')
        assert (await storage.runs.get('run-1')).status.value=='done'
    finally:legacy.close();storage.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('blocker',['active_run','claim','foreign_sdk'])
async def test_activation_rejects_all_scope_blockers_atomically(tmp_path,blocker):
    path=tmp_path/'db';storage=SqliteAgentAdapters(path,scope='other');active=SqliteAgentAdapters(path,scope='active')
    try:
        if blocker=='active_run':await begin(active)
        elif blocker=='claim':
            async with active._transaction(with_journal=False) as session:session.claims[('run','call')]=ToolCall('call','write','{}')
        else:
            active._db.execute("INSERT INTO purra_state VALUES('foreign','typescript',4,'{}')")
        before=active._db.execute('SELECT * FROM purra_state ORDER BY scope,sdk').fetchall()
        with pytest.raises(ValueError,match='approval_activation_'):await storage.enable_approvals()
        assert active._db.execute('SELECT * FROM purra_state ORDER BY scope,sdk').fetchall()==before
        assert not active._db.execute("SELECT 1 FROM sqlite_master WHERE name='purra_approvals'").fetchone()
    finally:active.close();storage.close()


@pytest.mark.asyncio
async def test_reopen_replay_conflict_expiry_and_read_only_projection(tmp_path):
    path=tmp_path/'db';storage=SqliteAgentAdapters(path,scope='fixture');clock=[1000]
    try:
        store=storage.approval_store(authorize=lambda *args:True,clock_ms=lambda:clock[0])
        with pytest.raises(Exception,match='Approval operation') as denied:await store.create(intent(),expires_at_ms=2000)
        assert denied.value.code=='approval_storage_not_enabled'
        await storage.enable_approvals();await begin(storage)
        record=await store.create(intent(),expires_at_ms=2000)
        clock[0]=1100
        assert await store.create(intent(),expires_at_ms=2000)==record
        with pytest.raises(Exception) as conflict:await store.create(replace(intent(),scope_revision='changed'),expires_at_ms=2000)
        assert conflict.value.code=='approval_intent_conflict'
        cmd=command(record);receipt=await store.decide(cmd,principal_id='host')
        storage.close();storage=SqliteAgentAdapters(path,scope='fixture')
        store=storage.approval_store(authorize=lambda *args:True,clock_ms=lambda:clock[0])
        assert await store.decide(cmd,principal_id='host')==receipt
        with pytest.raises(Exception) as conflict:await store.decide(replace(cmd,decision='reject'),principal_id='host')
        assert conflict.value.code=='approval_command_conflict'
        with pytest.raises(Exception) as conflict:await store.decide(cmd,principal_id='different')
        assert conflict.value.code=='approval_command_conflict'
        clock[0]=2000;changes=storage._db.total_changes
        assert (await store.get(record.approval_id)).status=='approved'
        assert len(await store.list_pending())==1
        assert storage._db.total_changes==changes
        assert (await store.refresh(record.approval_id)).status=='expired'
        clock[0]=1200
        assert (await store.refresh(record.approval_id)).status=='expired'
        assert await store.decide(cmd,principal_id='host')==receipt
    finally:storage.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('change',['expire','cancel'])
async def test_authorizer_runs_outside_transaction_and_rechecks_after_wait(tmp_path,change):
    path=tmp_path/'db';storage=SqliteAgentAdapters(path,scope='fixture');other=SqliteAgentAdapters(path,scope='fixture');clock=[1000]
    entered=asyncio.Event();release=asyncio.Event()
    async def authorize(*args):entered.set();await release.wait();return True
    try:
        await storage.enable_approvals();await begin(storage)
        store=storage.approval_store(authorize=authorize,clock_ms=lambda:clock[0])
        record=await store.create(intent(),expires_at_ms=2000)
        task=asyncio.create_task(store.decide(command(record),principal_id='host'))
        await asyncio.wait_for(entered.wait(),1)
        async with other._transaction(with_journal=False):other.extra['proofOfUnlockedWriter']=True
        if change=='expire':clock[0]=2000
        else:await other.leases.request_cancellation('run-1')
        release.set()
        with pytest.raises(Exception) as denied:await task
        assert denied.value.code==('approval_expired' if change=='expire' else 'approval_canceled')
        assert (await store.get(record.approval_id)).status==('expired' if change=='expire' else 'canceled')
    finally:release.set();other.close();storage.close()


@pytest.mark.asyncio
async def test_two_connections_have_one_decision_winner_and_denial_is_not_truthy(tmp_path):
    path=tmp_path/'db';storage=SqliteAgentAdapters(path,scope='fixture');other=SqliteAgentAdapters(path,scope='fixture')
    try:
        await storage.enable_approvals();await begin(storage)
        first=storage.approval_store(authorize=lambda *args:True,clock_ms=lambda:1000)
        second=other.approval_store(authorize=lambda *args:True,clock_ms=lambda:1000)
        record=await first.create(intent(),expires_at_ms=2000)
        denied=storage.approval_store(authorize=lambda *args:'approved',clock_ms=lambda:1000)
        with pytest.raises(Exception) as error:await denied.decide(command(record),principal_id='model-output')
        assert error.value.code=='approval_authorization_denied'
        results=await asyncio.gather(first.decide(command(record,'a'),principal_id='host'),second.decide(command(record,'b','reject'),principal_id='host'),return_exceptions=True)
        assert sum(isinstance(row,Exception) for row in results)==1
        assert next(row for row in results if isinstance(row,Exception)).code=='approval_revision_conflict'
        assert (await first.get(record.approval_id)).revision==2
        assert not storage._claims
    finally:other.close();storage.close()


@pytest.mark.asyncio
async def test_decision_storage_failure_rolls_back_and_scope_isolated(tmp_path):
    path=tmp_path/'db';storage=SqliteAgentAdapters(path,scope='fixture');other=SqliteAgentAdapters(path,scope='other')
    try:
        await storage.enable_approvals();await begin(storage)
        store=storage.approval_store(authorize=lambda *args:True,clock_ms=lambda:1000)
        record=await store.create(intent(),expires_at_ms=2000)
        with pytest.raises(Exception) as missing:
            await other.approval_store(authorize=lambda *args:True).get(record.approval_id)
        assert missing.value.code=='approval_not_found'
        storage._db.execute("CREATE TRIGGER fail_approval_update BEFORE UPDATE ON purra_approvals BEGIN SELECT RAISE(ABORT, 'fixture persistence failure'); END")
        with pytest.raises(sqlite3.IntegrityError,match='fixture persistence failure'):
            await store.decide(command(record),principal_id='host')
        assert await store.get(record.approval_id)==record
        storage._db.execute('DROP TRIGGER fail_approval_update')
        assert (await store.decide(command(record),principal_id='host'))['revision']==2
    finally:other.close();storage.close()


@pytest.mark.asyncio
async def test_root_configuration_deadline_and_authorization_failure(tmp_path):
    storage=SqliteAgentAdapters(tmp_path/'db',scope='fixture')
    try:
        await storage.enable_approvals();await begin(storage,deadline=1500)
        def fail_authorization(*args):raise RuntimeError('private authorization failure')
        store=storage.approval_store(authorize=fail_authorization,clock_ms=lambda:1000)
        for changed,code in [(replace(intent(),root_run_id='other'),'approval_run_conflict'),(replace(intent(),preset_fingerprint='changed'),'approval_configuration_mismatch')]:
            with pytest.raises(Exception) as failure:await store.create(changed,expires_at_ms=2000)
            assert failure.value.code==code
        record=await store.create(intent(),expires_at_ms=2000)
        assert record.expires_at_ms==1500
        with pytest.raises(Exception) as failure:await store.decide(command(record),principal_id='host')
        assert failure.value.code=='approval_authorization_failed'
        assert 'private' not in str(failure.value)
        assert await store.get(record.approval_id)==record
    finally:storage.close()


@pytest.mark.asyncio
async def test_creation_replay_uses_json_identity_not_python_value_equality(tmp_path):
    storage=SqliteAgentAdapters(tmp_path/'db',scope='fixture')
    try:
        await storage.enable_approvals();await begin(storage)
        store=storage.approval_store(authorize=lambda *args:True,clock_ms=lambda:1000)
        numeric=replace(intent(),arguments={'value':1})
        boolean=replace(intent(),arguments={'value':True})
        assert numeric.arguments==boolean.arguments
        assert numeric.digest!=boolean.digest
        await store.create(numeric,expires_at_ms=2000)
        with pytest.raises(Exception) as conflict:await store.create(boolean,expires_at_ms=2000)
        assert conflict.value.code=='approval_intent_conflict'
    finally:storage.close()
