from dataclasses import replace
import time
import pytest
from purra.api import ModelRouteCandidate, AgentCoreRunOptions
from purra.model_protocol import generic_capability_snapshot
from purra.approvals import ApprovalRequired, ApprovalDecisionCommand
from test_approval_resume import ApprovalHost


@pytest.mark.asyncio
async def test_route_reopen_preserves_identity_and_rejects_drift(tmp_path):
    route = ModelRouteCandidate('host-model', '1', 'config-1', replace(generic_capability_snapshot(), max_generation_tokens=128))
    path = tmp_path / 'route.db'
    host = ApprovalHost(path, model_route=route)
    expiry = int(time.time() * 1000) + 60000
    host.expiry = expiry
    try:
        await host.storage.enable_approvals()
        handle = await host.core.submit(host.request, options=AgentCoreRunOptions(tool_checkpoint_handler=host.boundary))
        with pytest.raises(ApprovalRequired): await handle.wait()
        run_id = handle.run_id
        saved = await host.storage.runs.get(run_id)
        assert saved.agent_preset_snapshot['composition']['modelRoute']['bindingId'] == 'host-model'
    finally:
        await host.close()
    for current in [None, replace(route, revision='2'), replace(route, config_identity='config-2')]:
        host = ApprovalHost(path, model_route=current); host.expiry = expiry
        try:
            with pytest.raises(Exception) as rejected:
                resumed = await host.core.resume(run_id, host.request, options=AgentCoreRunOptions(tool_checkpoint_handler=host.boundary))
                await resumed.wait()
            assert getattr(rejected.value, 'code', None) == 'agent_preset_mismatch'
            assert (host.model_calls, host.tool_calls) == (0, 0)
        finally: await host.close()
    host = ApprovalHost(path, model_route=route); host.expiry = expiry
    try:
        with pytest.raises(Exception) as rejected:
            changed = replace(host.request, model=replace(host.request.model, model='other'))
            resumed = await host.core.resume(run_id, changed, options=AgentCoreRunOptions(tool_checkpoint_handler=host.boundary))
            await resumed.wait()
        assert getattr(rejected.value, 'code', None) == 'agent_preset_mismatch'
        record = (await host.approvals.list_pending(run_id=run_id))[0]
        assert record.expires_at_ms == expiry
        await host.approvals.decide(ApprovalDecisionCommand(record.approval_id, record.revision, record.intent.digest, 'approve', 'approve'), principal_id='host')
        resumed = await host.core.resume(run_id, host.request, options=AgentCoreRunOptions(tool_checkpoint_handler=host.boundary))
        assert (await resumed.wait()).status.value == 'done'
        assert host.tool_calls == 1
    finally: await host.close()
