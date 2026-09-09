from dataclasses import replace
import time
import asyncio
import pytest
from purra.api import ModelRouteCandidate, AgentCoreRunOptions, resolve_model_route
from purra.api import ModelRouteBinding, ModelRouteRegistry
from purra.model_protocol import TaskCapabilityRequirements
from purra.model_protocol import generic_capability_snapshot
from purra.approvals import ApprovalRequired, ApprovalDecisionCommand
from test_approval_resume import ApprovalHost


@pytest.mark.asyncio
async def test_route_reopen_preserves_identity_and_rejects_drift(tmp_path):
    route = ModelRouteCandidate('host-model', '1', 'config-1', replace(generic_capability_snapshot(), max_generation_tokens=128),
                                policy_id='ordered', policy_revision='1')
    path = tmp_path / 'route.db'
    async def create(selected): return ApprovalHost(path, model_route=selected)
    registry = ModelRouteRegistry([ModelRouteBinding(route, create)])
    host = await registry.create_new(['host-model'], TaskCapabilityRequirements('default'))
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
    for current in [None, replace(route, revision='2'), replace(route, config_identity='config-2'), replace(route, policy_revision='2')]:
        host = ApprovalHost(path, model_route=current); host.expiry = expiry
        try:
            with pytest.raises(Exception) as rejected:
                resumed = await host.core.resume(run_id, host.request, options=AgentCoreRunOptions(tool_checkpoint_handler=host.boundary))
                await resumed.wait()
            assert getattr(rejected.value, 'code', None) == 'agent_preset_mismatch'
            assert (host.model_calls, host.tool_calls) == (0, 0)
        finally: await host.close()
    probe = ApprovalHost(path)
    try:
        saved_route = (await probe.storage.runs.get(run_id)).agent_preset_snapshot['composition']['modelRoute']
        resolved = resolve_model_route([replace(route, policy_revision='2')], saved_route, ['host-model'])
        assert resolved.policy_revision == '1'
    finally: await probe.close()
    rebuilt_registry = ModelRouteRegistry([ModelRouteBinding(replace(route, policy_revision='2'), create)])
    host = await rebuilt_registry.create_recovery(saved_route, ['host-model']); host.expiry = expiry
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


@pytest.mark.asyncio
async def test_two_routed_runs_overlap_without_model_identity_leak(tmp_path):
    entered = []
    both = asyncio.Event()
    hosts = []

    class ConcurrentHost(ApprovalHost):
        async def stream(self, messages, invocation, signal=None):
            entered.append(invocation.request.model)
            if len(entered) == 2: both.set()
            await asyncio.wait_for(both.wait(), 5)
            assert invocation.request.model == self.request.model.model
            return await super().stream(messages, invocation, signal)

    async def create(route):
        host = ConcurrentHost(tmp_path / 'shared.db', model_route=route)
        host.request = replace(host.request, tools_enabled=False,
            model=replace(host.request.model, model=route.binding_id))
        hosts.append(host)
        return host

    capabilities = replace(generic_capability_snapshot(), max_generation_tokens=128)
    registry = ModelRouteRegistry([ModelRouteBinding(ModelRouteCandidate(name, '1', name, capabilities), create)
                                   for name in ['model-a', 'model-b']])
    try:
        first, second = await asyncio.gather(*[registry.create_new([name], TaskCapabilityRequirements('default'))
                                              for name in ['model-a', 'model-b']])
        handles = await asyncio.gather(first.core.submit(first.request), second.core.submit(second.request))
        results = await asyncio.gather(*(handle.wait() for handle in handles))
        assert all(result.status.value == 'done' for result in results)
        assert sorted(entered) == ['model-a', 'model-b']
        assert handles[0].run_id != handles[1].run_id
        for host, handle in zip([first, second], handles):
            saved = await host.storage.runs.get(handle.run_id)
            assert saved.agent_preset_snapshot['composition']['modelRoute']['bindingId'] == host.request.model.model
            assert (host.model_calls, host.tool_calls) == (1, 0)
    finally:
        for host in hosts: await host.close()
