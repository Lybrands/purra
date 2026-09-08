"""Independent-process recovery, using only synthetic effects in a temporary directory."""
import asyncio
import json
import os
from pathlib import Path
import sys
import time

import pytest

from purra.api import AgentCoreRunOptions, RecoveryWorker
from purra.contracts import ToolHandlerResult
# Only test fixtures are added; installed checks still import SDKs from site-packages.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_approval_resume import ApprovalHost, approved_host


def emit(**value):
    print(json.dumps(value), flush=True)


async def release_barrier():
    assert await asyncio.to_thread(sys.stdin.readline) == 'continue\n'


async def child(path, effect_path, expiry, mode):
    class ProcessHost(ApprovalHost):
        async def lookup(self, state, arguments, signal=None):
            self.tool_calls += 1
            # The append deliberately has no deduplication: Core must prevent repeats.
            with open(effect_path, 'a') as effect:
                effect.write('write\n')
                effect.flush()
                os.fsync(effect.fileno())
            if mode == 'exit_after_effect':
                emit(stage='effect')
                os._exit(74)
            if mode == 'owner':
                emit(stage='effect')
                await release_barrier()
            return ToolHandlerResult(content='42', effect_state='committed')

    host = ProcessHost(path)
    host.expiry = int(expiry)
    cursor = host.storage.recovery_cursor('competitor' if mode == 'competitor' else 'worker')
    resume_errors = []

    async def discover():
        ids = await cursor.discover()
        emit(stage='discovered', ids=ids)
        return ids

    async def inspect(run_id):
        report = await host.storage.inspect_recovery(run_id)
        if mode == 'force_resume':
            return {'blockers': []}
        if mode == 'competitor':
            emit(stage='inspected', blockers=report['blockers'])
            await release_barrier()
        return report

    async def resume(run_id):
        try:
            handle = await host.core.resume(run_id, host.request,
                options=AgentCoreRunOptions(tool_checkpoint_handler=host.boundary))
            result = await handle.wait()
            assert result.status.value == 'done'
        except Exception as error:
            resume_errors.append(getattr(error, 'code', type(error).__name__))
            raise

    async def acknowledge(ids):
        if mode == 'exit_before_ack':
            emit(stage='before_ack', ids=ids, tools=host.tool_calls)
            os._exit(73)
        await cursor.acknowledge(ids)

    try:
        result = await RecoveryWorker(discover=discover, inspect=inspect,
            resume=resume, acknowledge=acknowledge).run_once()
        emit(stage='result', actions=[r.action for r in result],
            reasons=[list(r.reasons) for r in result], errors=resume_errors,
            tools=host.tool_calls, models=host.model_calls)
    finally:
        await host.close()


async def start(path, effect, expiry, mode):
    # -I is retained by installed-artifact checks; source checks inherit PYTHONPATH.
    args = [sys.executable, *(['-I'] if sys.flags.isolated else []), str(Path(__file__).resolve()),
            str(path), str(effect), str(expiry), mode]
    return await asyncio.create_subprocess_exec(*args, stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)


async def event(process, stage):
    line = await asyncio.wait_for(process.stdout.readline(), 15)
    assert line, f'child exited before {stage}'
    value = json.loads(line)
    assert value['stage'] == stage, value
    return value


async def finish(process, code=0):
    stdout, stderr = await asyncio.wait_for(process.communicate(), 15)
    assert process.returncode == code, (stdout.decode(), stderr.decode())
    assert not stderr, stderr.decode()


async def cleanup(processes):
    for process in processes:
        if process.returncode is None:
            process.kill()
        await asyncio.wait_for(process.communicate(), 5)


@pytest.mark.asyncio
async def test_stale_inspection_in_competing_process_cannot_repeat_write(tmp_path):
    path, effect = tmp_path / 'db', tmp_path / 'effects'
    host, _, record = await approved_host(path)
    expiry = host.expiry
    await host.close()
    children = []
    try:
        competitor = await start(path, effect, expiry, 'competitor'); children.append(competitor)
        assert (await event(competitor, 'discovered'))['ids'] == [record.intent.run_id]
        assert not (await event(competitor, 'inspected'))['blockers']
        owner = await start(path, effect, expiry, 'owner'); children.append(owner)
        await event(owner, 'discovered')
        await event(owner, 'effect')
        competitor.stdin.write(b'continue\n'); await competitor.stdin.drain()
        result = await event(competitor, 'result')
        assert result['actions'] == ['failed']
        assert result['errors'] == ['run_lease_conflict']
        assert result['tools'] == result['models'] == 0
        await finish(competitor)
        owner.stdin.write(b'continue\n'); await owner.stdin.drain()
        assert (await event(owner, 'result'))['actions'] == ['settled']
        await finish(owner)
        assert effect.read_text() == 'write\n'
    finally:
        await cleanup(children)


@pytest.mark.asyncio
async def test_process_exit_before_ack_replays_terminal_candidate_without_execution(tmp_path):
    path, effect = tmp_path / 'db', tmp_path / 'effects'
    host, _, record = await approved_host(path)
    expiry = host.expiry
    await host.close()
    children = []
    try:
        first = await start(path, effect, expiry, 'exit_before_ack'); children.append(first)
        await event(first, 'discovered')
        before = await event(first, 'before_ack')
        assert before['ids'] == [record.intent.run_id] and before['tools'] == 1
        await finish(first, 73)
        host = ApprovalHost(path)
        try:
            assert (await host.storage.runs.get(record.intent.run_id)).status.value == 'done'
            async with host.storage.transaction() as session:
                assert 'worker' not in session.extra.get('recoveryCursors', {})
                assert session.extra['approvalExecutions'][record.approval_id]['state'] == 'complete'
                assert not session.claims
        finally:
            await host.close()
        restarted = await start(path, effect, expiry, 'restart'); children.append(restarted)
        assert (await event(restarted, 'discovered'))['ids'] == [record.intent.run_id]
        result = await event(restarted, 'result')
        assert result['actions'] == ['blocked']
        assert result['tools'] == result['models'] == 0
        await finish(restarted)
        assert effect.read_text() == 'write\n'
        host = ApprovalHost(path)
        try:
            async with host.storage.transaction() as session:
                assert session.extra['recoveryCursors']['worker']['revision'] == 1
        finally:
            await host.close()
    finally:
        await cleanup(children)



@pytest.mark.asyncio
async def test_effect_exit_retains_claim_after_real_lease_expiry_and_reconciles(tmp_path):
    path, effect = tmp_path / 'db', tmp_path / 'effects'
    host, _, record = await approved_host(path)
    expiry = host.expiry
    await host.close()
    children = []
    key = (record.intent.run_id, record.intent.tool_call_id)
    try:
        crashed = await start(path, effect, expiry, 'exit_after_effect'); children.append(crashed)
        await event(crashed, 'discovered'); await event(crashed, 'effect'); await finish(crashed, 74)
        host = ApprovalHost(path)
        try:
            lease = await host.storage.leases.get(record.intent.run_id)
            assert lease.owner_id is not None
            async with host.storage.transaction() as session:
                claim = session.claims[key]
                assert session.get_tool_receipt(key) is None
            with pytest.raises(ValueError, match='approval_reconciliation_requires_idle_run'):
                await host.storage.reconcile_tool(record.intent.run_id, claim,
                    result=ToolHandlerResult('42', effect_state='committed'))
            delay = max(0, (lease.expires_at_ms - time.time() * 1000) / 1000) + 0.05
            assert delay < 35
            # Wait for the actual persisted lease; do not rewrite expiry or fake time.
            await asyncio.sleep(delay)
            assert (await host.storage.leases.get(record.intent.run_id)).expires_at_ms < time.time() * 1000
        finally:
            await host.close()
        blocked = await start(path, effect, expiry, 'restart'); children.append(blocked)
        await event(blocked, 'discovered'); report = await event(blocked, 'result'); await finish(blocked)
        assert report['actions'] == ['blocked'] and 'tool_effect_unknown' in report['reasons'][0]
        assert report['tools'] == report['models'] == 0
        forced = await start(path, effect, expiry, 'force_resume'); children.append(forced)
        await event(forced, 'discovered'); report = await event(forced, 'result'); await finish(forced)
        assert report['errors'] == ['tool_effect_unknown']
        assert report['tools'] == report['models'] == 0
        host = ApprovalHost(path)
        try:
            async with host.storage.transaction() as session:
                assert session.claims[key] == claim
                assert session.get_tool_receipt(key) is None
            # This synthetic file is independent evidence of the committed effect.
            assert effect.read_text() == 'write\n'
            await host.storage.reconcile_tool(record.intent.run_id, claim,
                result=ToolHandlerResult('42', effect_state='committed'))
        finally:
            await host.close()
        recovered = await start(path, effect, expiry, 'restart'); children.append(recovered)
        await event(recovered, 'discovered'); report = await event(recovered, 'result'); await finish(recovered)
        assert report['actions'] == ['settled'] and report['tools'] == 0
        assert effect.read_text() == 'write\n'
        host = ApprovalHost(path)
        try:
            assert (await host.storage.runs.get(record.intent.run_id)).status.value == 'done'
            async with host.storage.transaction() as session:
                assert key not in session.claims
                assert session.get_tool_receipt(key) is not None
        finally:
            await host.close()
    finally:
        await cleanup(children)

if __name__ == '__main__':
    asyncio.run(child(*sys.argv[1:]))
