"""Installed Core/SQLite/MCP checks against an independent synthetic writer."""
import asyncio
from dataclasses import replace
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import time

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from purra.api import AgentCore, AgentCoreRunOptions, AgentPreset
from purra.approvals import ApprovalIntent, ApprovalDecisionCommand, ApprovalRequired
from purra.contracts import AgentMessage, AgentRunRequest, DomainContext, ModelRequest, ModelStream, ModelStreamChunk, RuntimeLimits, ToolCallDelta, ToolPolicy
from purra.errors import ContractViolationError
from purra.model_protocol import generic_capability_snapshot
from purra.structured import json_identity_digest
from purra.tools import InMemoryToolCatalog
from purra_mcp import McpCatalogMonitor, McpWriteToolBinding, McpToolLimits, discover_mcp_write_tools
from purra_sqlite import SqliteAgentAdapters


class Host:
    def __init__(self, path, registrations, expiry):
        self.storage=SqliteAgentAdapters(path,scope='stdio-write')
        self.approvals=self.storage.approval_store(authorize=lambda principal,*_:principal=="fixture-host")
        self.calls=0; self.expiry=expiry; self.identity=registrations[0].approval_binding
        self.request=AgentRunRequest(messages=(AgentMessage('user','Write the synthetic value'),),
            model=ModelRequest('fixture','fixture',replace(generic_capability_snapshot(),max_generation_tokens=128)),
            domain_context=DomainContext('stdio-write'),context_window=65536,tools_enabled=True,planning_mode='reactive')
        self.core=AgentCore(model_gateway=self,run_repository=self.storage.runs,output_repository=self.storage.outputs,
            output_publisher=self.storage.publisher,execution_lease_store=self.storage.leases,
            approval_gateway=self.approvals.gateway(),tool_idempotency_gateway=self.storage.idempotency,
            preset=AgentPreset(id='stdio-write',revision='1',runtime_limits=RuntimeLimits(max_run_generation_tokens=None),tool_catalog=InMemoryToolCatalog(registrations)))
        self.options=AgentCoreRunOptions(tool_checkpoint_handler=self.boundary,tool_checkpoint_names=frozenset({'write'}))
    async def complete(self,*args,**kwargs): raise AssertionError('stream required')
    async def stream(self,messages,invocation,signal=None):
        self.calls+=1
        async def chunks():
            if any(m.role.value=='tool' for m in messages) or not invocation.tools:
                yield ModelStreamChunk(content_delta='done',finish_reason='stop')
            else:
                yield ModelStreamChunk(tool_call_deltas=(ToolCallDelta(index=0,id='write-1',name='write',arguments_fragment='{"value":42}'),),finish_reason='tool_calls')
        return ModelStream(chunks=chunks(),model='fixture',applied_generation_limit=invocation.output_budget.max_generation_tokens)
    async def boundary(self,checkpoint):
        call=checkpoint.assistant.tool_calls[0];run=await self.storage.runs.get(checkpoint.run_id);identity=self.identity
        intent=ApprovalIntent(run_id=checkpoint.run_id,root_run_id=checkpoint.run_id,tool_call_id=call.id,tool_name=call.name,
            arguments=json.loads(call.arguments_json),preset_fingerprint=json_identity_digest(run.agent_preset_snapshot),
            binding_id=identity['bindingId'],binding_revision=identity['bindingRevision'],scope_id=identity['scopeId'],scope_revision=identity['scopeRevision'],effect=identity['effect'])
        await self.approvals.prepare(checkpoint,intent,expires_at_ms=self.expiry)
    async def close(self):
        await self.core.close();self.storage.close()


async def check(server,mode):
    with TemporaryDirectory(prefix='purra-stdio-write-') as directory:
        path=Path(directory);monitor=McpCatalogMonitor();host=None
        def ledger():return [json.loads(line) for line in (path/'remote.jsonl').read_text().splitlines()]
        params=StdioServerParameters(command=sys.executable,args=['-I',str(server),directory,mode])
        try:
            async with stdio_client(params) as streams:
                async with ClientSession(*streams,message_handler=monitor.on_message) as client:
                    initialized=await client.initialize();monitor.accept_protocol_version(initialized.protocolVersion)
                    async def scope(*args):return None
                    catalog=await discover_mcp_write_tools(client,'synthetic',{'write':McpWriteToolBinding('write',ToolPolicy('confirm','Write fixture','write'),scope,
                        'synthetic-writer','1','temporary-fixture','1','write')},monitor=monitor,limits=McpToolLimits(timeout_ms=2000))
                    expiry=int(time.time()*1000)+60000
                    host=Host(path/'run.db',catalog.registrations,expiry);await host.storage.enable_approvals()
                    handle=await host.core.submit(host.request,options=host.options)
                    try:await handle.wait();raise AssertionError('approval wait required')
                    except ApprovalRequired:pass
                    run_id=handle.run_id;assert host.calls==1 and [r['event'] for r in ledger()]==['started']
                    await host.close();host=Host(path/'run.db',catalog.registrations,expiry)
                    try:await (await host.core.resume(run_id,host.request,options=host.options)).wait();raise AssertionError('approval still pending')
                    except ApprovalRequired:pass
                    assert host.calls==0 and [r['event'] for r in ledger()]==['started']
                    record=(await host.approvals.list_pending())[0]
                    await host.approvals.decide(ApprovalDecisionCommand(record.approval_id,record.revision,record.intent.digest,'approve','approve'),principal_id='fixture-host')
                    result=await (await host.core.resume(run_id,host.request,options=host.options)).wait()
                    assert result.status.value==('done' if mode=='success' else 'failed')
                    async with host.storage.transaction() as session:
                        receipt=session.get_tool_receipt((run_id,'write-1'));unknown=(run_id,'write-1') in session.claims
                        assert (receipt is not None)==(mode=='success') and unknown==(mode!='success')
                    await host.close();host=Host(path/'run.db',catalog.registrations,expiry)
                    before=ledger()
                    try:await (await host.core.resume(run_id,host.request,options=host.options)).wait();raise AssertionError('terminal/unknown resume must fail')
                    except ContractViolationError as error:assert error.code=="run_terminal"
                    assert host.calls==0 and ledger()==before
        finally:
            if host is not None:await host.close()
            monitor.close()
        rows=ledger();pid=rows[0]['pid']
        try:os.kill(pid,0)
        except ProcessLookupError:pass
        else:raise AssertionError('fixture process still alive')
        events=[r['event'] for r in rows];assert events.count('received')==1
        writes=0 if mode=='exit-before' else 1
        assert events.count('written')==writes and (path/'value.txt').exists()==bool(writes)
        if writes:assert (path/'value.txt').read_text()=='42'
        return {'scenario':mode,'protocol':initialized.protocolVersion,'remoteCalls':1,'remoteWrites':writes,
            'effect':'committed' if mode=='success' else 'unknown','duplicateDispatch':False,'resumeBlocker':'run_terminal','serverExited':True,'events':events}


async def main():
    import purra,purra_sqlite,purra_mcp
    for module in [purra,purra_sqlite,purra_mcp]:assert Path(module.__file__).is_relative_to(sys.prefix),module.__file__
    server=Path(sys.argv[1]).resolve()
    results=[await check(server,mode) for mode in ['success','response-loss','exit-before','exit-after','error-after']]
    print(json.dumps({'sdk':'python','model':'scripted fixture; no real Provider','checks':results},indent=2))

if __name__=='__main__':asyncio.run(main())
