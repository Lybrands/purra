"""A host model-ready callback preserves the live recovery budget."""
from dataclasses import replace

import pytest

from purra.api import AgentCoreRunOptions
from purra.contracts import PlanningMode, RunStatus, ToolHandlerResult, ToolPolicy, ToolSchema
from purra.ports import ToolRegistration
from purra.tools import InMemoryToolCatalog
from test_standalone_agent_conformance import _Context, _ScriptedToolGateway, _core, _request


@pytest.mark.asyncio
@pytest.mark.parametrize('case', ['recover', 'exhaust', 'tamper'])
async def test_live_checkpoint_callback_keeps_consumed_recovery_attempts(case):
    calls = []

    async def read(state, arguments, signal=None):
        calls.append(arguments)
        return ToolHandlerResult(content='synthetic fact', effect_state='not_started')

    catalog = InMemoryToolCatalog(tuple(ToolRegistration(
        schema=ToolSchema(name=name, description='Read a synthetic fact.', parameters={
            'type':'object', 'properties':{'id':{'type':'string'}},
            'required':['id'] if name=='readById' else []}),
        handler=read, policy=ToolPolicy(mode='read',title=name),
    ) for name in ('readById','readCurrent')))
    gateway = _ScriptedToolGateway(['readById', 'readById' if case=='exhaust' else 'readCurrent', None])
    checkpoints = []

    async def boundary(checkpoint):
        checkpoints.append(checkpoint)
        if case=='tamper' and checkpoint.recovery_attempts:
            return replace(checkpoint, recovery_attempts=())
        return checkpoint

    core = _core(gateway=gateway, context=_Context(), tool_catalog=catalog)
    try:
        result = await (await core.submit(replace(_request(), tools_enabled=True,context_window=32768,planning_mode=PlanningMode.REACTIVE),
            options=AgentCoreRunOptions(checkpoint_handler=boundary))).wait()
    finally:
        await core.close()
    consumed = [checkpoint.recovery_attempts for checkpoint in checkpoints if checkpoint.recovery_attempts]
    assert consumed and all(sum(item[2] for item in attempts)==1 for attempts in consumed),result
    if case=='recover':
        assert result.status is RunStatus.DONE
        assert len(gateway.invocations)==3 and len(calls)==1
    else:
        assert result.status is RunStatus.FAILED
        assert result.error == ('runtime_exception' if case=='tamper' else 'invalid_tool_arguments_schema')
        assert len(gateway.invocations)==(1 if case=='tamper' else 2)
        assert not calls
