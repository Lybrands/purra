"""Exact Provider prose publication, using the real Core and in-memory outbox."""
from dataclasses import replace
import pytest

from purra.api import AgentCoreRunOptions, InMemoryAgentAdapters
from purra.contracts import ResponseValidationResult, RunStatus
from purra.output import PublicPresentationMode, ResponseTransactionMode, ResponseTransactionPolicy
from tests.test_standalone_agent_conformance import _Gateway, _Context, _core, _request


@pytest.mark.asyncio
@pytest.mark.parametrize('large', [False, True])
async def test_validated_answer_is_published_exactly_once_without_another_generation(large):
    answer = '合成正文🙂\n' * (6000 if large else 1)
    gateway, adapters, checked = _Gateway(answer), InMemoryAgentAdapters(), []
    class Validator:
        def validate(self, *, content, messages):
            checked.append(content)
            return ResponseValidationResult()
    original = _request()
    request = replace(original, context_window=200000,
        model=replace(original.model, max_generation_tokens=64000,
            capability_snapshot=replace(original.model.capability_snapshot,
                context_window_tokens=200000, max_generation_tokens=64000)))
    core = _core(gateway=gateway, context=_Context(), adapters=adapters)
    try:
        result = await (await core.submit(request, options=AgentCoreRunOptions(
            response_validators=(Validator(),), response_transaction_policy=ResponseTransactionPolicy(
                mode=ResponseTransactionMode.VALIDATED_RESULT,
                public_presentation=PublicPresentationMode.VERBATIM)))).wait()
        assert result.status is RunStatus.DONE, result
        assert result.final_response == answer
        assert checked == [answer] and len(gateway.invocations) == 1
        events = await adapters.outputs.list_events(result.run_id, after_sequence=0, limit=1000)
        public = [event for event in events if event.channel.value == 'final' and event.visibility.value == 'public']
        assert ''.join(event.payload.get('delta', '') for event in public) == answer
        assert all(len(event.payload.get('delta', '').encode('utf-8')) <= 16384 for event in public)
        assert events[-1].payload['status'] == 'done'
        assert await adapters.outputs.load_validated_result(result.run_id) == answer
    finally:
        await core.close()


@pytest.mark.parametrize('fault', ['digest', 'stream', 'invocation', 'text', 'foreign', 'foreign_invocation', 'tools', 'reasoning', 'incomplete', 'planning'])
def test_publication_rejects_unbound_or_incomplete_source(fault):
    from datetime import datetime, timezone
    from types import SimpleNamespace
    from purra.contracts import OutputTextReference
    from purra.errors import ContractViolationError
    from purra.output import AgentOutputIntent, OutputChannel, OutputEventKind, OutputSource, OutputVisibility
    from purra.output.verbatim import verbatim_publication_drafts
    text = 'synthetic complete answer'
    reference = OutputTextReference.for_text('run', 'stream', 'invocation', text)
    spec = SimpleNamespace(run_id='run', output_stream_id='stream', invocation_id='invocation',
        turn_id='turn', intent=AgentOutputIntent.STRUCTURED_PRIVATE, output_protocol=None)
    event = SimpleNamespace(run_id='run', output_stream_id='stream', invocation_id='invocation', source=OutputSource.PROVIDER,
        channel=OutputChannel.DIAGNOSTIC, visibility=OutputVisibility.PRIVATE,
        kind=OutputEventKind.PROVIDER_CONTENT_DELTA, payload={'delta':text})
    status, finish = 'committed', 'stop'
    if fault == 'digest': reference = replace(reference, content_digest='sha256:'+'0'*64)
    elif fault == 'stream': reference = replace(reference, output_stream_id='other')
    elif fault == 'invocation': reference = replace(reference, invocation_id='other')
    elif fault == 'text': text += ' rewritten'
    elif fault == 'foreign': event.run_id = 'other'
    elif fault == 'foreign_invocation': event.invocation_id = 'other'
    elif fault == 'tools': event.kind = OutputEventKind.PROVIDER_TOOL_CALL_DELTA
    elif fault == 'reasoning': event.kind = OutputEventKind.PROVIDER_REASONING_DELTA
    elif fault == 'incomplete': finish = 'length'
    elif fault == 'planning': spec.output_protocol = 'planning'
    with pytest.raises(ContractViolationError):
        verbatim_publication_drafts(reference, spec=spec, status=status, finish_reason=finish,
            events=(event,), text=text, occurred_at=datetime.now(timezone.utc))


@pytest.mark.asyncio
async def test_rejected_candidate_is_never_public():
    class Reject:
        def validate(self, **kwargs):
            return ResponseValidationResult(violation_code='synthetic_reject', repair_guidance='Fix answer')
    gateway, adapters = _Gateway('SYNTHETIC_REJECTED_ANSWER'), InMemoryAgentAdapters()
    core = _core(gateway=gateway, context=_Context(), adapters=adapters)
    try:
        result = await (await core.submit(_request(), options=AgentCoreRunOptions(
            response_validators=(Reject(),), response_transaction_policy=ResponseTransactionPolicy(
                mode=ResponseTransactionMode.VALIDATED_RESULT,
                public_presentation=PublicPresentationMode.VERBATIM)))).wait()
        assert result.status is RunStatus.FAILED
        events = await adapters.outputs.list_events(result.run_id, after_sequence=0, limit=1000)
        assert not any(e.channel.value == 'final' and e.visibility.value == 'public' for e in events)
    finally:
        await core.close()


@pytest.mark.asyncio
async def test_publication_budget_failure_keeps_memory_commit_atomic():
    from purra.contracts import RuntimeLimits
    from purra.errors import ContractViolationError
    answer = '合成正文🙂\n' * 6000
    adapters = InMemoryAgentAdapters()
    core = _core(gateway=_Gateway(answer), context=_Context(), adapters=adapters,
        runtime_limits=RuntimeLimits(max_run_generation_tokens=None,
            max_provider_output_bytes=len(answer.encode('utf-8')) * 3 // 2))
    original = _request()
    request = replace(original, context_window=200000, model=replace(original.model,
        max_generation_tokens=64000, capability_snapshot=replace(original.model.capability_snapshot,
            context_window_tokens=200000, max_generation_tokens=64000)))
    try:
        result = await (await core.submit(request, options=AgentCoreRunOptions(
            response_transaction_policy=ResponseTransactionPolicy(mode=ResponseTransactionMode.VALIDATED_RESULT,
                public_presentation=PublicPresentationMode.VERBATIM)))).wait()
        assert result.status is RunStatus.FAILED and result.error == 'runtime_budget_exceeded'
        events = await adapters.outputs.list_events(result.run_id, after_sequence=0, limit=1000)
        assert any(e.kind.value == 'stream.committed' for e in events), 'budget must fail at publication'
        assert not any(e.channel.value == 'final' and e.visibility.value == 'public' for e in events)
        with pytest.raises(ContractViolationError):
            await adapters.outputs.load_validated_result(result.run_id)
    finally:
        await core.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('decision', ['suppress', 'rewrite'])
async def test_verbatim_still_requires_public_output_policy_before_atomic_commit(decision):
    from purra.api import AgentCore, AgentPreset
    from purra.contracts import ModelStreamChunk, RuntimeLimits
    from purra.output import AgentOutputProcessor
    from purra.tools import InMemoryToolCatalog
    class Policy:
        async def authorize_provider_chunk(self, spec, chunk):
            if spec.intent.value == 'final_public':
                return None if decision == 'suppress' else ModelStreamChunk(content_delta='rewritten')
            return chunk
    adapters = InMemoryAgentAdapters()
    processor = AgentOutputProcessor(adapters.outputs, adapters.publisher, policy=Policy())
    core = AgentCore(model_gateway=_Gateway('SYNTHETIC_ANSWER'), run_repository=adapters.runs,
        output_repository=adapters.outputs, output_publisher=adapters.publisher, output_processor=processor,
        preset=AgentPreset(id='policy', revision='1', tool_catalog=InMemoryToolCatalog(()),
            runtime_limits=RuntimeLimits(max_run_generation_tokens=None)))
    try:
        result = await (await core.submit(_request(), options=AgentCoreRunOptions(
            response_transaction_policy=ResponseTransactionPolicy(mode=ResponseTransactionMode.VALIDATED_RESULT,
                public_presentation=PublicPresentationMode.VERBATIM)))).wait()
        assert result.status is RunStatus.FAILED and result.error == 'output_policy_violation'
        events = await adapters.outputs.list_events(result.run_id, after_sequence=0, limit=1000)
        assert not any(e.channel.value == 'final' and e.visibility.value == 'public' for e in events)
        assert not any(e.kind.value == 'run.validated_result' for e in events)
    finally:
        await core.close()
