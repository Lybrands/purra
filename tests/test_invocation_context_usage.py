"""Context occupancy follows each physical invocation, independently of billing."""
from dataclasses import replace

import pytest

from purra.api import AgentComponentBinding, AgentCore, AgentPreset, ContextStrategy, ExecutionProfile, InMemoryAgentAdapters
from purra.context_budget import estimate_agent_messages_tokens, estimate_tool_schema_tokens
from purra.context_orchestration import ConversationCompactionResult
from purra.contracts import (
    AgentMessage, ContextBundle, ModelFinishReason, ModelStream, ModelStreamChunk,
    ModelTokenUsage, RuntimeLimits, ToolCallDelta, ToolHandlerResult, ToolPolicy, ToolSchema,
)
from purra.ports import ToolRegistration
from purra.tools import InMemoryToolCatalog
from test_second_host_conformance import _request


class _Context:
    async def build_context(self, request, budget, signal=None):
        return ContextBundle()

    async def build_planning_context(self, request, budget, signal=None):
        return ContextBundle()

    async def build_task_context(self, request, budget, task, signal=None):
        return ContextBundle()


class _Gateway:
    def __init__(self):
        self.calls = []

    async def complete(self, *args, **kwargs):
        raise AssertionError('stream required')

    async def stream(self, messages, invocation, signal=None):
        self.calls.append((tuple(messages), invocation))
        number = len(self.calls)

        async def chunks():
            if number == 1:
                yield ModelStreamChunk(tool_call_deltas=(ToolCallDelta(
                    index=0, id='read-1', name='readSource', arguments_fragment='{}',
                ),), usage=ModelTokenUsage(11000, 20), finish_reason=ModelFinishReason.TOOL_CALLS)
            else:
                yield ModelStreamChunk(content_delta='Evidence checked.',
                    usage=ModelTokenUsage(29000, 40), finish_reason=ModelFinishReason.STOP)
        return ModelStream(chunks=chunks(), model='operations-model',
            applied_generation_limit=invocation.output_budget.max_generation_tokens)


@pytest.mark.asyncio
async def test_later_tool_round_replaces_first_round_usage_with_paired_estimate():
    async def read(state, arguments, signal=None):
        return ToolHandlerResult('source material ' * 100)
    adapters = InMemoryAgentAdapters()
    gateway = _Gateway()
    core = AgentCore(model_gateway=gateway, run_repository=adapters.runs,
        output_repository=adapters.outputs, output_publisher=adapters.publisher,
        preset=AgentPreset(id='context-usage', revision='1', context_provider=_Context(),
            component_bindings={'contextProvider': AgentComponentBinding('test.context', '1')},
            runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
            tool_catalog=InMemoryToolCatalog((ToolRegistration(
                schema=ToolSchema(name='readSource', description='Read source', parameters={'type': 'object'}),
                policy=ToolPolicy(mode='read', title='Read source'), handler=read),))))
    try:
        handle = await core.submit(_request())
        result = await handle.wait()
        assert result.status.value == 'done', result.error
        events = await adapters.outputs.list_events(handle.run_id, after_sequence=0)
    finally:
        await core.close()
    runtime = [event.payload for event in events if event.kind.value == 'runtime.event']
    usages = [item['data'] for item in runtime if item['eventType'] == 'context.usage_recorded']
    prepared = [item['data'] for item in runtime if item['eventType'] == 'context.budgeted'
                and item['data'].get('usageScope') == 'invocation_input']
    assert [item['actualInputTokens'] for item in usages] == [11000, 29000]
    assert [item['contextRound'] for item in usages] == [1, 2]
    assert len(prepared) == len(gateway.calls) == 2
    for budget, usage, (messages, invocation) in zip(prepared, usages, gateway.calls, strict=True):
        estimate = estimate_agent_messages_tokens(messages) + estimate_tool_schema_tokens(invocation.tools)
        assert budget['estimatedInputTokens'] + budget['toolSchemaTokens'] == estimate
        assert usage['inputTokenEstimateAtUsage'] == estimate
    assert prepared[1]['runtimeReserveTokens'] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize('strategy', [ContextStrategy.SINGLE_PASS, ContextStrategy.STAGED])
async def test_history_compactor_runs_for_single_pass_and_reactive_hosts(strategy):
    class Compactor:
        def __init__(self):
            self.calls = []
        async def prepare(self, request, signal=None, **options):
            self.calls.append((request, options))
            if request.metadata.get('contextCompressionScope') == 'runtime':
                return ConversationCompactionResult(request, 'unchanged')
            return ConversationCompactionResult(replace(request, messages=(
                AgentMessage(role='user', content='validated history summary'), request.messages[-1],
            )), 'summarized')
    class Gateway(_Gateway):
        async def stream(self, messages, invocation, signal=None):
            assert any(m.content == 'validated history summary' for m in messages)
            assert any(m.content == 'current request' for m in messages)
            async def chunks():
                yield ModelStreamChunk(content_delta='done', finish_reason=ModelFinishReason.STOP)
            return ModelStream(chunks=chunks(), model='operations-model',
                applied_generation_limit=invocation.output_budget.max_generation_tokens)
    compactor = Compactor()
    adapters = InMemoryAgentAdapters()
    core = AgentCore(model_gateway=Gateway(), run_repository=adapters.runs,
        output_repository=adapters.outputs, output_publisher=adapters.publisher,
        preset=AgentPreset(id='summary', revision='1', context_provider=_Context(),
            component_bindings={'contextProvider': AgentComponentBinding('test.context', '1'),
                'conversationCompactor': AgentComponentBinding('test.compactor', '1')},
            conversation_compactor=compactor, execution_profile=ExecutionProfile(context_strategy=strategy),
            runtime_limits=RuntimeLimits(max_run_generation_tokens=None), tool_catalog=InMemoryToolCatalog(())))
    request = replace(_request(), tools_enabled=False, messages=(
        AgentMessage(role='user', content='old request'), AgentMessage(role='assistant', content='old answer'),
        AgentMessage(role='user', content='current request')))
    try:
        handle = await core.submit(request)
        result = await handle.wait()
        assert result.status.value == 'done', result.error
    finally:
        await core.close()
    canonical = [r for r, _ in compactor.calls if r.metadata.get('contextCompressionScope') != 'runtime']
    assert len(canonical) == 1
    assert [m.content for m in canonical[0].messages][-3:] == ['old request', 'old answer', 'current request']
    assert request.messages[-3].content == 'old request'


class _GrowthGateway(_Gateway):
    def __init__(self, fail_phase=None):
        super().__init__()
        self.fail_phase = fail_phase
        self.failed = False
        self.phases = []

    async def stream(self, messages, invocation, signal=None):
        from purra.errors import ModelGatewayError

        self.calls.append((tuple(messages), invocation))
        phase = 'later' if any(m.role.value == 'tool' for m in messages) else 'initial'
        self.phases.append(phase)
        if phase == self.fail_phase and not self.failed:
            self.failed = True
            raise ModelGatewayError('synthetic interrupted opening',
                code='upstream_stream_interrupted', retryable=True)

        async def chunks():
            if phase == 'initial':
                yield ModelStreamChunk(tool_call_deltas=(ToolCallDelta(
                    index=0, id='growth-read', name='readSource', arguments_fragment='{}',
                ),), usage=ModelTokenUsage(10, 2), finish_reason=ModelFinishReason.TOOL_CALLS)
            else:
                yield ModelStreamChunk(content_delta='Synthetic evidence checked.',
                    usage=ModelTokenUsage(20, 2), finish_reason=ModelFinishReason.STOP)
        return ModelStream(chunks=chunks(), model='operations-model',
            applied_generation_limit=invocation.output_budget.max_generation_tokens)


async def _run_growth_case(*, initial_chars=300_000, result_chars=50_000, fail_phase=None):
    from purra.contracts import ModelRequest
    from purra.model_protocol import generic_capability_snapshot

    reads = []

    async def read(state, arguments, signal=None):
        reads.append(True)
        return ToolHandlerResult('y' * result_chars)

    adapters = InMemoryAgentAdapters()
    gateway = _GrowthGateway(fail_phase)
    core = AgentCore(model_gateway=gateway, run_repository=adapters.runs,
        output_repository=adapters.outputs, output_publisher=adapters.publisher,
        preset=AgentPreset(id='growth-budget', revision='1', context_provider=_Context(),
            component_bindings={'contextProvider': AgentComponentBinding('test.context', '1')},
            runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
            tool_catalog=InMemoryToolCatalog((ToolRegistration(
                schema=ToolSchema(name='readSource', description='Read synthetic source',
                    parameters={'type': 'object'}),
                policy=ToolPolicy(mode='read', title='Read synthetic source'), handler=read),))))
    request = replace(_request(), context_window=200_000,
        model=ModelRequest(provider='operations', model='operations-model',
            capability_snapshot=replace(generic_capability_snapshot(),
                context_window_tokens=200_000, max_generation_tokens=131_072)),
        messages=(AgentMessage(role='user', content='x' * initial_chars),))
    try:
        handle = await core.submit(request)
        result = await handle.wait()
        events = await adapters.outputs.list_events(handle.run_id, after_sequence=0)
    finally:
        await core.close()
    prepared = [event.payload['data'] for event in events
        if event.kind.value == 'runtime.event'
        and event.payload['eventType'] == 'context.budgeted'
        and event.payload['data'].get('usageScope') == 'invocation_input']
    return result, prepared, gateway, reads


@pytest.mark.asyncio
@pytest.mark.parametrize('fail_phase', [None, 'initial', 'later'])
async def test_growth_reserve_is_released_for_later_rounds_and_preserved_for_retries(fail_phase):
    result, prepared, gateway, reads = await _run_growth_case(fail_phase=fail_phase)
    assert result.status.value == 'done', result.error
    assert len(reads) == 1
    assert len(prepared) == len(gateway.calls) == (2 if fail_phase is None else 3)
    assert gateway.phases[0] == 'initial' and gateway.phases[-1] == 'later'
    for budget, phase, (messages, invocation) in zip(
            prepared, gateway.phases, gateway.calls, strict=True):
        assert budget['runtimeReserveTokens'] == (20_000 if phase == 'initial' else 0)
        message_tokens = estimate_agent_messages_tokens(messages)
        schema_tokens = estimate_tool_schema_tokens(invocation.tools)
        remaining = (budget['windowTokens'] - message_tokens - schema_tokens
            - budget['safetyReserveTokens'] - budget['runtimeReserveTokens'])
        assert invocation.output_budget.max_generation_tokens == min(131_072, remaining)
        assert budget['overflowTokens'] == 0
    # This fits the later-round budget but cannot fit if the first-round
    # growth reserve is incorrectly retained by the invocation manager.
    last = prepared[-1]
    assert (last['estimatedInputTokens'] + last['toolSchemaTokens']
        + last['safetyReserveTokens'] + 20_000) > last['windowTokens']


@pytest.mark.asyncio
@pytest.mark.parametrize('initial_chars,result_chars,expected_calls', [
    (400_000, 50_000, 0),
    (300_000, 64_000, 1),
])
async def test_real_context_overflow_remains_fail_closed(initial_chars, result_chars, expected_calls):
    result, _, gateway, reads = await _run_growth_case(
        initial_chars=initial_chars, result_chars=result_chars)
    assert result.status.value == 'failed'
    assert ('context_overflow' in result.error
        or result.error == 'protected_messages_exceed_compression_budget')
    assert len(gateway.calls) == expected_calls
    assert len(reads) == expected_calls
