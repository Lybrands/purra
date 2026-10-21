"""Presentation retries and execution completion are separate contracts."""
from __future__ import annotations

import pytest

from purra.contracts import AgentRunResult, ModelFinishReason, ModelStreamChunk, RunStatus
from purra.errors import ContractViolationError, ModelGatewayError
from purra.model_invocation import ModelInvocationContext
from tests.test_response_transaction import _types, _Facts, _Stream, _model_request


@pytest.mark.asyncio
@pytest.mark.parametrize('failure,expected_calls', [
    ('length', 1), ('contract', 1), ('untyped', 1),
    ('transient_before_output', 2), ('transient_after_output', 1), ('missing_finish_after_output', 1),
])
async def test_only_explicit_transient_failure_before_public_output_retries(failure, expected_calls):
    Transaction, Fact, Bundle, Presentation, Mode, Policy = _types()
    class Manager:
        calls = 0
        async def stream(self, messages, call, context, signal):
            self.calls += 1
            attempt = self.calls
            async def chunks():
                if attempt > 1:
                    yield ModelStreamChunk(content_delta='completed', finish_reason=ModelFinishReason.STOP)
                    return
                if failure == 'length':
                    yield ModelStreamChunk(content_delta='partial', finish_reason=ModelFinishReason.LENGTH)
                elif failure == 'contract':
                    raise ContractViolationError('synthetic invalid contract')
                elif failure == 'untyped':
                    raise RuntimeError('synthetic unknown failure')
                elif failure == 'transient_before_output':
                    raise ModelGatewayError('synthetic connection interruption', retryable=True)
                else:
                    yield ModelStreamChunk(content_delta='partial')
                    if failure == 'transient_after_output':
                        raise ModelGatewayError('synthetic read interruption', retryable=True)
            return _Stream(chunks())
    manager = Manager()
    transaction = Transaction(manager,
        policy=Policy(mode=Mode.VALIDATED_RESULT, public_presentation=Presentation.MODEL_LIVE),
        facts_provider=_Facts(Bundle(facts=(Fact('summary', 'private execution completed'),))))
    kwargs = dict(request=_model_request(), context=ModelInvocationContext(run_id='presentation-only'))
    committed = AgentRunResult(run_id='presentation-only', status=RunStatus.DONE)
    if failure == 'transient_before_output':
        assert await transaction.present(committed, **kwargs) == 'completed'
    else:
        with pytest.raises((ModelGatewayError, ContractViolationError, RuntimeError)):
            await transaction.present(committed, **kwargs)
    assert manager.calls == expected_calls


@pytest.mark.parametrize('status,presentation', [
    (RunStatus.FAILED, {'status': 'failed', 'errorCode': 'length'}),
    (RunStatus.DONE, {'status': 'failed'}),
    (RunStatus.DONE, {'status': 'completed', 'errorCode': 'length'}),
    (RunStatus.DONE, {'status': 'failed', 'errorCode': ''}),
    (RunStatus.DONE, {'status': 'failed', 'errorCode': 'PRIVATE exception text'}),
    (RunStatus.DONE, {'status': 'failed', 'errorCode': 'code' * 50}),
    (RunStatus.DONE, {'status': 'failed', 'errorCode': 123}),
    (RunStatus.DONE, {'status': 'pending'}),
    (RunStatus.DONE, {'status': 'completed', 'content': 'private result'}),
])
def test_public_presentation_metadata_is_closed_and_cannot_include_private_content(status, presentation):
    with pytest.raises(ValueError):
        AgentRunResult(run_id='metadata', status=status, presentation=presentation)


def test_presentation_metadata_is_frozen():
    metadata = {'status': 'failed', 'errorCode': 'model_output_truncated'}
    result = AgentRunResult(run_id='metadata', status=RunStatus.DONE, presentation=metadata)
    metadata['errorCode'] = 'changed'
    assert result.presentation == {'status': 'failed', 'errorCode': 'model_output_truncated'}
    with pytest.raises(TypeError):
        result.presentation['status'] = 'completed'


@pytest.mark.asyncio
@pytest.mark.parametrize('fail_presentation', [False, True])
async def test_completed_execution_persists_private_result_and_independent_public_outcome(fail_presentation):
    from dataclasses import replace
    from purra.api import AgentCoreRunOptions, ExecutionProfile
    from purra.adapters.memory import InMemoryAgentAdapters
    from purra.contracts import ModelStream, PlanningMode
    from purra.context_strategies import ContextStrategy
    from purra.output import PublicPresentationMode, ResponseTransactionMode, ResponseTransactionPolicy
    from tests.test_standalone_agent_conformance import (
        _Gateway, _Context, _Dispatcher, _Planner, _AlwaysPlan, _Admission, _DurableFacts, _core, _request,
    )
    class Gateway(_Gateway):
        async def stream(self, messages, invocation, signal=None):
            if not fail_presentation:
                return await super().stream(messages, invocation, signal)
            self.messages.append(tuple(messages))
            self.invocations.append(invocation)
            async def chunks():
                yield ModelStreamChunk(content_delta='incomplete public answer', finish_reason=ModelFinishReason.LENGTH)
            return ModelStream(chunks=chunks(), model='portable-model',
                applied_generation_limit=invocation.output_budget.max_generation_tokens)
    gateway, dispatcher, adapters = Gateway('public synthesis'), _Dispatcher(), InMemoryAgentAdapters()
    core = _core(gateway=gateway, context=_Context(), adapters=adapters,
        profile=ExecutionProfile(planner=_Planner(), planning_policy=_AlwaysPlan(),
            context_strategy=ContextStrategy.STAGED, task_admission_evaluator=_Admission(),
            long_task_dispatcher=dispatcher))
    try:
        result = await (await core.submit(replace(_request(), planning_mode=PlanningMode.PLANNED),
            options=AgentCoreRunOptions(response_transaction_policy=ResponseTransactionPolicy(
                mode=ResponseTransactionMode.VALIDATED_RESULT, public_presentation=PublicPresentationMode.MODEL_LIVE),
                committed_result_facts_provider=_DurableFacts()))).wait()
    finally:
        await core.close()
    assert result.status is RunStatus.DONE and result.error is None
    assert result.final_response == ('' if fail_presentation else 'public synthesis')
    assert result.presentation == ({'status': 'failed', 'errorCode': 'model_output_truncated'}
        if fail_presentation else {'status': 'completed'})
    assert dispatcher.dispatches == 1 and len(gateway.invocations) == 1
    private = await adapters.outputs.load_validated_result(result.run_id)
    assert private == 'portable durable answer'
    events = await adapters.outputs.list_events(result.run_id, after_sequence=0)
    terminal = [event for event in events if event.kind.value == 'run.lifecycle' and event.payload.get('status') == 'done']
    assert len(terminal) == 1 and terminal[0].payload['presentation'] == result.presentation
    assert sum(event.kind.value == 'run.validated_result' for event in events) == 1
    assert private not in str(terminal[0].payload)


@pytest.mark.asyncio
@pytest.mark.parametrize('failure_type', ['cancel', 'deadline'])
@pytest.mark.parametrize('visible', [False, True])
async def test_presentation_preserves_typed_control_exceptions_after_visible_output(failure_type, visible):
    from purra.cancellation import ExecutionDeadlineExceeded, OperationCanceled
    error_type = OperationCanceled if failure_type == 'cancel' else ExecutionDeadlineExceeded
    Transaction, Fact, Bundle, Presentation, Mode, Policy = _types()
    class Manager:
        calls = 0
        async def stream(self, messages, call, context, signal):
            self.calls += 1
            async def chunks():
                if visible:
                    yield ModelStreamChunk(content_delta='partial')
                raise error_type('synthetic control stop')
            return _Stream(chunks())
    manager = Manager()
    transaction = Transaction(manager,
        policy=Policy(mode=Mode.VALIDATED_RESULT, public_presentation=Presentation.MODEL_LIVE),
        facts_provider=_Facts(Bundle(facts=(Fact('summary', 'completed execution'),))))
    with pytest.raises(error_type):
        await transaction.present(AgentRunResult(run_id='control', status=RunStatus.DONE),
            request=_model_request(), context=ModelInvocationContext(run_id='control'))
    assert manager.calls == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('finish', ['error', 'stop'])
@pytest.mark.parametrize('reason', ['request_canceled', 'run_deadline_exceeded'])
async def test_stopped_signal_wins_over_presentation_error_or_success(finish, reason):
    from purra.cancellation import ExecutionDeadlineExceeded, ExecutionStopSignal, OperationCanceled
    stop = ExecutionStopSignal()
    Transaction, Fact, Bundle, Presentation, Mode, Policy = _types()
    class Manager:
        calls = 0
        async def stream(self, messages, call, context, signal):
            self.calls += 1
            async def chunks():
                yield ModelStreamChunk(content_delta='partial')
                stop.set(reason)
                if finish == 'error':
                    raise ModelGatewayError('synthetic interrupted stream', retryable=True)
                yield ModelStreamChunk(finish_reason=ModelFinishReason.STOP)
            return _Stream(chunks())
    manager = Manager()
    transaction = Transaction(manager,
        policy=Policy(mode=Mode.VALIDATED_RESULT, public_presentation=Presentation.MODEL_LIVE),
        facts_provider=_Facts(Bundle(facts=(Fact('summary', 'completed execution'),))))
    try:
        with pytest.raises(ExecutionDeadlineExceeded if reason.endswith('_deadline_exceeded') else OperationCanceled):
            await transaction.present(AgentRunResult(run_id='signal', status=RunStatus.DONE),
                request=_model_request(), context=ModelInvocationContext(run_id='signal'), signal=stop)
    finally:
        stop.close()
    assert manager.calls == 1


def _durable_presentation_core(facts, gateway=None):
    from purra.api import AgentCoreRunOptions, ExecutionProfile
    from purra.adapters.memory import InMemoryAgentAdapters
    from purra.context_strategies import ContextStrategy
    from purra.output import PublicPresentationMode, ResponseTransactionMode, ResponseTransactionPolicy
    from tests.test_standalone_agent_conformance import (
        _Gateway, _Context, _Dispatcher, _Planner, _AlwaysPlan, _Admission, _core,
    )
    gateway, dispatcher, adapters = gateway or _Gateway('public synthesis'), _Dispatcher(), InMemoryAgentAdapters()
    core = _core(gateway=gateway, context=_Context(), adapters=adapters,
        profile=ExecutionProfile(planner=_Planner(), planning_policy=_AlwaysPlan(),
            context_strategy=ContextStrategy.STAGED, task_admission_evaluator=_Admission(),
            long_task_dispatcher=dispatcher))
    options = AgentCoreRunOptions(response_transaction_policy=ResponseTransactionPolicy(
        mode=ResponseTransactionMode.VALIDATED_RESULT, public_presentation=PublicPresentationMode.MODEL_LIVE),
        committed_result_facts_provider=facts)
    return core, options, gateway, dispatcher, adapters


@pytest.mark.asyncio
@pytest.mark.parametrize('failure_type', ['cancel', 'deadline'])
async def test_presentation_control_exception_settles_the_run_without_done(failure_type):
    from dataclasses import replace
    from purra.cancellation import ExecutionDeadlineExceeded, OperationCanceled
    from purra.contracts import PlanningMode
    from tests.test_standalone_agent_conformance import _request
    class Facts:
        async def facts_for(self, run_id, result):
            raise (OperationCanceled('synthetic cancellation') if failure_type == 'cancel'
                else ExecutionDeadlineExceeded('synthetic deadline'))
    core, options, gateway, dispatcher, adapters = _durable_presentation_core(Facts())
    try:
        result = await (await core.submit(replace(_request(), planning_mode=PlanningMode.PLANNED), options=options)).wait()
    finally:
        await core.close()
    assert result.status is (RunStatus.CANCELED if failure_type == 'cancel' else RunStatus.FAILED)
    assert result.error == ('request_canceled' if failure_type == 'cancel' else 'execution_deadline_exceeded')
    assert result.presentation is None and result.final_response == ''
    assert dispatcher.dispatches == 1 and not gateway.invocations
    with pytest.raises(ContractViolationError):
        await adapters.outputs.load_validated_result(result.run_id)
    events = await adapters.outputs.list_events(result.run_id, after_sequence=0)
    assert not any(event.kind.value == 'run.lifecycle' and event.payload.get('status') == 'done' for event in events)


@pytest.mark.asyncio
@pytest.mark.parametrize('facts_failure', [False, True])
async def test_manual_stop_during_facts_preparation_does_not_complete_execution(facts_failure):
    import asyncio
    from dataclasses import replace
    from purra.contracts import PlanningMode
    from tests.test_standalone_agent_conformance import _DurableFacts, _request
    entered, release = asyncio.Event(), asyncio.Event()
    class Facts(_DurableFacts):
        async def facts_for(self, run_id, result):
            entered.set()
            await release.wait()
            if facts_failure:
                raise RuntimeError('synthetic presentation failure after stop')
            return await super().facts_for(run_id, result)
    core, options, gateway, dispatcher, adapters = _durable_presentation_core(Facts())
    try:
        handle = await core.submit(replace(_request(), planning_mode=PlanningMode.PLANNED), options=options)
        await asyncio.wait_for(entered.wait(), 5)
        await handle.cancel('author_cancelled')
        release.set()
        result = await asyncio.wait_for(handle.wait(), 5)
    finally:
        release.set()
        await core.close()
    assert result.status is RunStatus.CANCELED and result.presentation is None
    assert dispatcher.dispatches == 1 and not gateway.invocations
    events = await adapters.outputs.list_events(result.run_id, after_sequence=0)
    assert not any(event.kind.value == 'run.lifecycle' and event.payload.get('status') == 'done' for event in events)


@pytest.mark.asyncio
@pytest.mark.parametrize('code,expected', [
    ('model_output_truncated', 'model_output_truncated'),
    ('provider.error:quota-limited', 'provider.error:quota-limited'),
    ('PRIVATE exception body https://private.invalid', 'completion_projection_failed'),
    ('x' * 200, 'completion_projection_failed'),
    (None, 'completion_projection_failed'),
])
async def test_presentation_failure_exposes_only_a_bounded_stable_error_code(code, expected):
    from dataclasses import replace
    from purra.contracts import PlanningMode
    from tests.test_standalone_agent_conformance import _request
    class Facts:
        async def facts_for(self, run_id, result):
            error = RuntimeError('PRIVATE exception message')
            error.code = code
            raise error
    core, options, gateway, dispatcher, adapters = _durable_presentation_core(Facts())
    try:
        result = await (await core.submit(replace(_request(), planning_mode=PlanningMode.PLANNED), options=options)).wait()
    finally:
        await core.close()
    assert result.status is RunStatus.DONE and result.error is None
    assert result.presentation == {'status': 'failed', 'errorCode': expected}
    assert result.final_response == '' and dispatcher.dispatches == 1 and not gateway.invocations
    assert await adapters.outputs.load_validated_result(result.run_id) == 'portable durable answer'
    events = await adapters.outputs.list_events(result.run_id, after_sequence=0)
    terminal = [event for event in events if event.kind.value == 'run.lifecycle' and event.payload.get('status') == 'done']
    assert len(terminal) == 1 and terminal[0].payload['presentation'] == result.presentation
    assert 'PRIVATE' not in str(terminal[0].payload)
