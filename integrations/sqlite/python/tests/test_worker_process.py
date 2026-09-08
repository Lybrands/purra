"""Independent-process recovery, using only synthetic effects in a temporary directory."""
import asyncio
import json
import os
from pathlib import Path
import sys

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


if __name__ == '__main__':
    asyncio.run(child(*sys.argv[1:]))
