"""Hosts may reserve public progress for execution receipts, without losing bytes/usage."""
import pytest
from dataclasses import replace
from purra.model_protocol import FeatureSupport
from purra.api import AgentCore, AgentCoreRunOptions, AgentPreset, InMemoryAgentAdapters
from purra.contracts import ModelStream, ModelStreamChunk, ModelFinishReason, ModelTokenUsage, RuntimeLimits, RunStatus
from purra.output import AgentOutputProcessor
from purra.tools import InMemoryToolCatalog
from tests.test_standalone_agent_conformance import _request


@pytest.mark.asyncio
@pytest.mark.parametrize('publish',[False,True])
async def test_progress_visibility_does_not_change_answer_or_actual_usage(publish):
    class Gateway:
        async def complete(self,*args,**kwargs): raise AssertionError('stream required')
        async def stream(self,messages,invocation,signal=None):
            async def chunks():
                yield ModelStreamChunk(content_delta='Original answer',progress_delta='Already saved all targets',
                    finish_reason=ModelFinishReason.STOP,usage=ModelTokenUsage(input_tokens=2,generation_tokens=3))
            return ModelStream(chunks=chunks(),model='portable-model',applied_generation_limit=invocation.output_budget.max_generation_tokens)
    adapters=InMemoryAgentAdapters()
    processor=AgentOutputProcessor(adapters.outputs,adapters.publisher,publish_provider_progress=publish)
    core=AgentCore(model_gateway=Gateway(),run_repository=adapters.runs,output_repository=adapters.outputs,
        output_publisher=adapters.publisher,output_processor=processor,
        preset=AgentPreset(id='progress-visibility',revision='1',tool_catalog=InMemoryToolCatalog(()),
            runtime_limits=RuntimeLimits(max_run_generation_tokens=None)))
    try:
        request=_request()
        snapshot=request.model.capability_snapshot
        request=replace(request, model=replace(request.model, capability_snapshot=replace(snapshot, protocol=replace(snapshot.protocol, public_progress=FeatureSupport.SUPPORTED))))
        result=await(await core.submit(request,options=AgentCoreRunOptions())).wait()
        assert result.status is RunStatus.DONE and result.final_response=='Original answer'
        events=await adapters.outputs.list_events(result.run_id,after_sequence=0,limit=1000)
        assert any(e.kind.value=='agent.progress' and e.visibility.value=='public' for e in events)==publish
        assert any(e.kind.value=='provider.usage' and e.payload['generationTokens']==3 for e in events)
        assert any(e.kind.value=='provider.delta_batch' and 'Already saved all targets' in str(e.payload)
            and e.visibility.value!='public' for e in events)
    finally: await core.close()
