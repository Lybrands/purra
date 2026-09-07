from __future__ import annotations

import asyncio
from dataclasses import replace
import time

import pytest

from purra.contracts import (
    AgentMessage,
    ModelCompletion,
    ModelFinishReason,
    ModelRequest,
    ModelStream,
    ModelStreamChunk,
    ModelTokenUsage,
    ReasoningMode,
    RuntimeLimits,
    ToolCallDelta,
    ToolSchema,
)
from purra.errors import ContractViolationError, UnsupportedModelFeatureError
from purra.evidence import ContextEvidenceReceipt
from purra.model_invocation.evidence import bind_model_input_evidence
from purra.cancellation import ExecutionDeadlineExceeded
from purra.model_protocol import (
    GenerationBudgetSource,
    InvocationOutputBudget,
    ReasoningUsageDetail,
    ResultCapacitySource,
    ThinkingTokenAccounting,
    generic_capability_snapshot,
    resolve_invocation_output_budget,
)
from purra.operations import (
    AgentOperationController,
    OperationKind,
    OperationStatus,
)
from purra.output import (
    AgentOutputIntent,
    OutputCommitMode,
)


def _types():
    try:
        from purra.model_invocation import (
            AgentModelCall,
            AgentModelInvocationManager,
            ModelInvocationContext,
        )
    except ModuleNotFoundError as error:
        pytest.fail(f"model invocation manager is missing: {error}")
    return AgentModelCall, AgentModelInvocationManager, ModelInvocationContext


def _request() -> ModelRequest:
    return ModelRequest(
        provider="test",
        model="model",
        capability_snapshot=replace(
            generic_capability_snapshot(),
            profile_id="test:model",
            max_generation_tokens=200,
        ),
    )


class _Gateway:
    def __init__(self):
        self.calls = []

    async def stream(self, messages, invocation, signal=None):
        del signal
        self.calls.append((tuple(messages), invocation))

        async def chunks():
            yield ModelStreamChunk(content_delta="甲乙")
            yield ModelStreamChunk(finish_reason=ModelFinishReason.STOP)

        return ModelStream(
            chunks=chunks(),
            model="model",
            applied_generation_limit=invocation.output_budget.max_generation_tokens,
        )

    async def complete(self, messages, invocation, signal=None):
        raise AssertionError("complete should not be called")


class _CompletionGateway(_Gateway):
    async def complete(self, messages, invocation, signal=None):
        del messages, signal
        return ModelCompletion(
            message=AgentMessage(role="assistant", content='{"plan":true}'),
            model="model",
            applied_generation_limit=invocation.output_budget.max_generation_tokens,
            finish_reason=ModelFinishReason.STOP,
        )


class _NeverReturningGateway(_Gateway):
    async def complete(self, messages, invocation, signal=None):
        del messages
        assert signal is not None
        await signal.wait()
        await asyncio.Event().wait()

    async def stream(self, messages, invocation, signal=None):
        del messages
        assert signal is not None

        async def chunks():
            while True:
                await signal.wait()
                yield ModelStreamChunk(content_delta="late")

        return ModelStream(
            chunks=chunks(),
            model="model",
            applied_generation_limit=invocation.output_budget.max_generation_tokens,
        )


class _ChunkGateway(_Gateway):
    def __init__(self, chunks):
        super().__init__()
        self._chunks = tuple(chunks)

    async def stream(self, messages, invocation, signal=None):
        del messages, signal

        async def chunks():
            for chunk in self._chunks:
                yield chunk

        return ModelStream(
            chunks=chunks(),
            model="model",
            applied_generation_limit=invocation.output_budget.max_generation_tokens,
        )


class _Observer:
    def __init__(self):
        self.opened = []
        self.accepted = []
        self.finished = []
        self.aborted = []

    async def open_model_stream(self, receipt, spec):
        self.opened.append((receipt, spec))

    async def accept_provider_chunk(self, output_stream_id, chunk):
        self.accepted.append((output_stream_id, chunk))

    async def finish_model_stream(self, output_stream_id, finish_reason):
        self.finished.append((output_stream_id, finish_reason))

    async def abort_model_stream(self, output_stream_id, error_code):
        self.aborted.append((output_stream_id, error_code))


class _OperationOutput:
    def __init__(self):
        self.events = []

    async def accept_operation_event(self, event):
        self.events.append(event)
        return event


class _FailingOpenObserver(_Observer):
    async def open_model_stream(self, receipt, spec):
        del receipt, spec
        raise OSError("journal unavailable")

    async def abort_model_stream(self, output_stream_id, error_code):
        del output_stream_id, error_code
        raise ContractViolationError("cannot abort an unopened stream")


class _RejectingEvidenceValidator:
    def __init__(self):
        self.calls = []

    async def validate_evidence(self, receipts, *, signal=None):
        self.calls.append((tuple(receipts), signal))
        raise ContractViolationError(
            "external evidence is stale",
            code="external_evidence_stale",
        )


@pytest.mark.asyncio
async def test_live_full_text_validation_is_rejected_before_gateway_call():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    gateway = _Gateway()
    manager = AgentModelInvocationManager(gateway, output_observer=_Observer())
    call = AgentModelCall(
        request=_request(),
        output_intent=AgentOutputIntent.FINAL_PUBLIC,
        commit_mode=OutputCommitMode.LIVE,
        requires_full_text_validation=True,
        reasoning_mode=ReasoningMode.DISABLED,
    )

    with pytest.raises(ContractViolationError, match="full-text validation"):
        await manager.stream(
            (AgentMessage(role="user", content="answer"),),
            call,
            ModelInvocationContext(
                run_id="run-1",
                turn_id="turn-1",
                requested_reasoning_mode=ReasoningMode.DISABLED,
            ),
        )

    assert gateway.calls == []


@pytest.mark.asyncio
async def test_public_chunk_is_observed_before_runtime_consumes_it():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    gateway = _Gateway()
    observer = _Observer()
    manager = AgentModelInvocationManager(gateway, output_observer=observer)
    call = AgentModelCall(
        request=_request(),
        output_intent=AgentOutputIntent.FINAL_PUBLIC,
        commit_mode=OutputCommitMode.LIVE,
        reasoning_mode=ReasoningMode.DISABLED,
    )

    managed = await manager.stream(
        (AgentMessage(role="user", content="answer"),),
        call,
        ModelInvocationContext(
            run_id="run-1",
            turn_id="turn-1",
            requested_reasoning_mode=ReasoningMode.DISABLED,
        ),
    )
    chunk = await anext(managed.chunks)

    assert observer.accepted == [(managed.receipt.output_stream_id, chunk)]
    assert observer.opened[0][0] == managed.receipt
    assert observer.opened[0][1].run_id == "run-1"
    assert observer.opened[0][1].turn_id == "turn-1"


@pytest.mark.asyncio
async def test_undeclared_provider_progress_is_rejected_before_persistence():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    observer = _Observer()
    manager = AgentModelInvocationManager(
        _ChunkGateway((ModelStreamChunk(progress_delta="正在核对范围"),)),
        output_observer=observer,
    )
    managed = await manager.stream(
        (AgentMessage(role="user", content="answer"),),
        AgentModelCall(
            request=_request(),
            output_intent=AgentOutputIntent.FINAL_PUBLIC,
            commit_mode=OutputCommitMode.LIVE,
        ),
        ModelInvocationContext(run_id="run-1", turn_id="turn-1"),
    )

    with pytest.raises(
        ContractViolationError,
        match="undeclared public progress",
    ):
        await anext(managed.chunks)

    assert observer.accepted == []
    assert observer.aborted[0][1] == "model_gateway_contract_violation"


@pytest.mark.asyncio
async def test_stream_contract_failure_settles_usage_from_the_rejected_chunk():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    usage = ModelTokenUsage(input_tokens=11, generation_tokens=7)

    class Repository:
        def __init__(self):
            self.settlements = []

        async def reserve_model_attempt(self, *args):
            return None

        async def settle_model_attempt(self, *args):
            self.settlements.append(args)

    repository = Repository()
    manager = AgentModelInvocationManager(
        _ChunkGateway((ModelStreamChunk(
            progress_delta="undeclared",
            usage=usage,
        ),)),
        budget_repository=repository,
    )
    managed = await manager.stream(
        (),
        AgentModelCall(
            request=_request(),
            output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
            commit_mode=OutputCommitMode.PRIVATE,
        ),
        ModelInvocationContext(run_id="run-rejected-usage"),
    )

    with pytest.raises(ContractViolationError):
        await anext(managed.chunks)

    assert repository.settlements[0][-1] == usage


@pytest.mark.asyncio
async def test_length_completion_fails_but_still_settles_reported_usage():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    usage = ModelTokenUsage(input_tokens=11, generation_tokens=200)

    class Repository:
        def __init__(self):
            self.settlements = []

        async def reserve_model_attempt(self, *args):
            return None

        async def settle_model_attempt(self, *args):
            self.settlements.append(args)

    class LengthGateway(_CompletionGateway):
        async def complete(self, messages, invocation, signal=None):
            del messages, signal
            return ModelCompletion(
                message=AgentMessage(role="assistant", content="truncated"),
                model="model",
                applied_generation_limit=invocation.max_generation_tokens,
                finish_reason=ModelFinishReason.LENGTH,
                usage=usage,
            )

    repository = Repository()
    manager = AgentModelInvocationManager(
        LengthGateway(),
        budget_repository=repository,
    )
    with pytest.raises(Exception) as captured:
        await manager.complete(
            (),
            AgentModelCall(
                request=_request(),
                output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
                commit_mode=OutputCommitMode.PRIVATE,
            ),
            ModelInvocationContext(run_id="run-length-usage"),
        )

    assert getattr(captured.value, "code", None) == "model_output_truncated"
    assert repository.settlements[0][-1] == usage


@pytest.mark.asyncio
async def test_receipt_fingerprints_model_input_tools_and_context_provenance():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    manager = AgentModelInvocationManager(_Gateway(), output_observer=_Observer())
    context = AgentMessage(
        role="system",
        content="trusted context",
        attributes={"context_name": "canon"},
        host_metadata={
            "context_evidence_receipts": [{
                "evidenceId": "canon:1",
                "source": "canon",
                "itemId": "book-1",
                "version": 2,
            }],
        },
    )
    managed = await manager.stream(
        (context, AgentMessage(role="user", content="answer")),
        AgentModelCall(
            request=_request(),
            output_intent=AgentOutputIntent.FINAL_PUBLIC,
            commit_mode=OutputCommitMode.LIVE,
            reasoning_mode=ReasoningMode.DISABLED,
            tools=(ToolSchema(
                name="lookup",
                description="Look up a fact.",
                parameters={"type": "object", "properties": {}},
            ),),
        ),
        ModelInvocationContext(
            run_id="run-1",
            turn_id="turn-1",
            requested_reasoning_mode=ReasoningMode.DISABLED,
        ),
    )

    receipt = managed.receipt

    assert len(receipt.input_fingerprint) == 64
    assert len(receipt.tool_schema_fingerprint) == 64
    assert [item.to_mapping() for item in receipt.context_evidence] == [{
        "evidenceId": "canon:1",
        "contextBlock": "canon",
        "source": "canon",
        "itemId": "book-1",
        "version": 2,
    }]
    assert receipt.to_mapping()["callParameters"][0]["toolNames"] == ["lookup"]


@pytest.mark.asyncio
async def test_request_fingerprint_includes_the_user_generation_ceiling():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    manager = AgentModelInvocationManager(_Gateway())
    fingerprints = []
    for ceiling in (None, 100):
        managed = await manager.stream(
            (AgentMessage(role="user", content="same"),),
            AgentModelCall(
                request=replace(_request(), max_generation_tokens=ceiling),
                output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
                commit_mode=OutputCommitMode.PRIVATE,
            ),
            ModelInvocationContext(run_id="run-fingerprint"),
        )
        fingerprints.append(managed.receipt.input_fingerprint)
        await managed.chunks.aclose()

    assert fingerprints[0] != fingerprints[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ("stream", "complete"))
async def test_external_evidence_is_revalidated_before_every_gateway_call(method):
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    gateway = _CompletionGateway()
    validator = _RejectingEvidenceValidator()
    observer = _Observer()
    manager = AgentModelInvocationManager(
        gateway,
        output_observer=observer,
        evidence_validator=validator,
    )
    evidence = ContextEvidenceReceipt(
        evidence_id="mem0:store:item-1:3",
        context_block="memory",
        source="mem0/scope",
        item_id="item-1",
        version=3,
    )
    call = AgentModelCall(
        request=_request(),
        output_intent=(
            AgentOutputIntent.FINAL_PUBLIC
            if method == "stream"
            else AgentOutputIntent.STRUCTURED_PRIVATE
        ),
        commit_mode=(
            OutputCommitMode.LIVE
            if method == "stream"
            else OutputCommitMode.PRIVATE
        ),
    )

    with bind_model_input_evidence((evidence,)):
        with pytest.raises(ContractViolationError) as captured:
            await getattr(manager, method)(
                (AgentMessage(role="user", content="answer"),),
                call,
                ModelInvocationContext(run_id="run-evidence"),
            )

    assert captured.value.code == "external_evidence_stale"
    assert len(validator.calls) == 1
    assert validator.calls[0][0] == (evidence,)
    assert gateway.calls == []
    assert observer.opened == []


@pytest.mark.asyncio
async def test_provider_chunks_are_forwarded_once_and_finish_after_terminal():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    observer = _Observer()
    manager = AgentModelInvocationManager(_Gateway(), output_observer=observer)
    managed = await manager.stream(
        (),
        AgentModelCall(
            request=_request(),
            output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
            commit_mode=OutputCommitMode.PRIVATE,
            reasoning_mode=ReasoningMode.DISABLED,
        ),
        ModelInvocationContext(
            run_id="run-1",
            requested_reasoning_mode=ReasoningMode.DISABLED,
        ),
    )

    chunks = [chunk async for chunk in managed.chunks]

    assert [item[1] for item in observer.accepted] == chunks
    assert observer.finished == [
        (managed.receipt.output_stream_id, ModelFinishReason.STOP)
    ]
    assert observer.aborted == []


@pytest.mark.asyncio
async def test_every_provider_attempt_has_one_authoritative_model_operation():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    operation_output = _OperationOutput()
    manager = AgentModelInvocationManager(
        _Gateway(),
        output_observer=_Observer(),
        operation_controller=AgentOperationController(operation_output),
    )
    managed = await manager.stream(
        (),
        AgentModelCall(
            request=_request(),
            output_intent=AgentOutputIntent.FINAL_PUBLIC,
            commit_mode=OutputCommitMode.LIVE,
            reasoning_mode=ReasoningMode.DISABLED,
        ),
        ModelInvocationContext(
            run_id="run-1",
            requested_reasoning_mode=ReasoningMode.DISABLED,
        ),
    )

    assert [event.kind for event in operation_output.events] == [
        OperationKind.MODEL,
    ]
    await anext(managed.chunks)
    terminal = await anext(managed.chunks)

    assert terminal.finish_reason is ModelFinishReason.STOP
    assert len(operation_output.events) == 2
    started, finished = operation_output.events
    assert finished.operation_id == started.operation_id
    assert finished.invocation_id == managed.receipt.invocation_id
    assert finished.status is OperationStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_stream_open_failure_still_closes_model_operation_once():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    operation_output = _OperationOutput()
    controller = AgentOperationController(operation_output)
    manager = AgentModelInvocationManager(
        _Gateway(),
        output_observer=_FailingOpenObserver(),
        operation_controller=controller,
    )

    with pytest.raises(OSError, match="journal unavailable"):
        await manager.stream(
            (),
            AgentModelCall(
                request=_request(),
                output_intent=AgentOutputIntent.FINAL_PUBLIC,
                commit_mode=OutputCommitMode.LIVE,
                reasoning_mode=ReasoningMode.DISABLED,
            ),
            ModelInvocationContext(
                run_id="run-1",
                requested_reasoning_mode=ReasoningMode.DISABLED,
            ),
        )

    assert len(operation_output.events) == 2
    assert operation_output.events[-1].status is OperationStatus.FAILED
    assert controller.running_operation_ids == ()


@pytest.mark.asyncio
async def test_private_completion_is_observed_before_stream_commit():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    observer = _Observer()
    manager = AgentModelInvocationManager(
        _CompletionGateway(),
        output_observer=observer,
    )

    completed = await manager.complete(
        (),
        AgentModelCall(
            request=_request(),
            output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
            commit_mode=OutputCommitMode.PRIVATE,
            requires_full_text_validation=True,
            reasoning_mode=ReasoningMode.DISABLED,
        ),
        ModelInvocationContext(
            run_id="run-1",
            requested_reasoning_mode=ReasoningMode.DISABLED,
        ),
    )

    assert observer.accepted == [(
        completed.receipt.output_stream_id,
        ModelStreamChunk(content_delta='{"plan":true}'),
    )]
    assert observer.finished == [(
        completed.receipt.output_stream_id,
        ModelFinishReason.STOP,
    )]


@pytest.mark.asyncio
async def test_stream_rejects_a_missing_applied_generation_limit_acknowledgment():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()

    class MissingAcknowledgmentGateway(_Gateway):
        async def stream(self, messages, invocation, signal=None):
            del messages, invocation, signal

            async def chunks():
                yield ModelStreamChunk(finish_reason=ModelFinishReason.STOP)

            return ModelStream(chunks=chunks(), model="model")

    manager = AgentModelInvocationManager(MissingAcknowledgmentGateway())
    with pytest.raises(ContractViolationError) as captured:
        await manager.stream(
            (),
            AgentModelCall(
                request=_request(),
                output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
                commit_mode=OutputCommitMode.PRIVATE,
            ),
            ModelInvocationContext(run_id="run-missing-output-limit-ack"),
        )

    assert captured.value.code == "model_gateway_contract_violation"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("applied_generation_limit", "usage"),
    (
        (199, None),
        (200, ModelTokenUsage(input_tokens=1, generation_tokens=201)),
    ),
)
async def test_completion_rejects_a_false_generation_limit_acknowledgment(
    applied_generation_limit,
    usage,
):
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()

    class FalseAcknowledgmentGateway(_CompletionGateway):
        async def complete(self, messages, invocation, signal=None):
            del messages, invocation, signal
            return ModelCompletion(
                message=AgentMessage(role="assistant", content="done"),
                model="model",
                applied_generation_limit=applied_generation_limit,
                finish_reason=ModelFinishReason.STOP,
                usage=usage,
            )

    manager = AgentModelInvocationManager(FalseAcknowledgmentGateway())
    with pytest.raises(ContractViolationError) as captured:
        await manager.complete(
            (),
            AgentModelCall(
                request=_request(),
                output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
                commit_mode=OutputCommitMode.PRIVATE,
            ),
            ModelInvocationContext(run_id="run-false-output-limit-ack"),
        )

    assert captured.value.code == "model_gateway_contract_violation"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("snapshot_changes", "usage"),
    (
        (
            {"reasoning_usage_detail": ReasoningUsageDetail.REQUIRED},
            ModelTokenUsage(input_tokens=1, generation_tokens=2),
        ),
        (
            {"thinking_token_accounting": ThinkingTokenAccounting.INCLUDED},
            ModelTokenUsage(
                input_tokens=1,
                generation_tokens=2,
                reasoning_tokens=3,
            ),
        ),
    ),
)
async def test_completion_rejects_inconsistent_reasoning_usage(
    snapshot_changes,
    usage,
):
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    request = replace(
        _request(),
        capability_snapshot=replace(
            _request().capability_snapshot,
            **snapshot_changes,
        ),
    )

    class InconsistentUsageGateway(_CompletionGateway):
        async def complete(self, messages, invocation, signal=None):
            del messages, signal
            return ModelCompletion(
                message=AgentMessage(role="assistant", content="done"),
                model="model",
                applied_generation_limit=(
                    invocation.output_budget.max_generation_tokens
                ),
                finish_reason=ModelFinishReason.STOP,
                usage=usage,
            )

    manager = AgentModelInvocationManager(InconsistentUsageGateway())
    with pytest.raises(ContractViolationError) as captured:
        await manager.complete(
            (),
            AgentModelCall(
                request=request,
                output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
                commit_mode=OutputCommitMode.PRIVATE,
            ),
            ModelInvocationContext(run_id="run-inconsistent-reasoning-usage"),
        )

    assert captured.value.code == "model_gateway_contract_violation"


@pytest.mark.asyncio
async def test_required_reasoning_usage_rejects_an_entirely_missing_usage_report():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    request = replace(
        _request(),
        capability_snapshot=replace(
            _request().capability_snapshot,
            reasoning_usage_detail=ReasoningUsageDetail.REQUIRED,
        ),
    )

    manager = AgentModelInvocationManager(_CompletionGateway())
    with pytest.raises(ContractViolationError) as captured:
        await manager.complete(
            (),
            AgentModelCall(
                request=request,
                output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
                commit_mode=OutputCommitMode.PRIVATE,
            ),
            ModelInvocationContext(run_id="run-missing-required-usage"),
        )

    assert captured.value.code == "model_gateway_contract_violation"


@pytest.mark.asyncio
async def test_actual_context_remainder_is_the_provider_generation_limit():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    request = ModelRequest(
        provider="test",
        model="model",
        capability_snapshot=replace(
            generic_capability_snapshot(),
            context_window_tokens=20_000,
            max_generation_tokens=18_000,
        ),
    )

    class RecordingGateway(_CompletionGateway):
        async def complete(self, messages, invocation, signal=None):
            self.calls.append((tuple(messages), invocation))
            return await super().complete(messages, invocation, signal)

    short_gateway = RecordingGateway()
    long_gateway = RecordingGateway()
    context = ModelInvocationContext(
        run_id="run-dynamic-context",
        context_window_tokens=20_000,
        safety_reserve_tokens=100,
        runtime_reserve_tokens=100,
    )
    call = AgentModelCall(
        request=request,
        output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
        commit_mode=OutputCommitMode.PRIVATE,
    )
    short = await AgentModelInvocationManager(short_gateway).complete(
        (AgentMessage(role="user", content="short"),),
        call,
        context,
    )
    long = await AgentModelInvocationManager(long_gateway).complete(
        (AgentMessage(role="user", content="长" * 4_000),),
        call,
        context,
    )

    short_limit = short.receipt.output_budget.max_generation_tokens
    long_limit = long.receipt.output_budget.max_generation_tokens
    assert short_limit == 18_000
    assert long_limit < short_limit
    assert short_gateway.calls[0][1].max_generation_tokens == short_limit
    assert long_gateway.calls[0][1].max_generation_tokens == long_limit


@pytest.mark.asyncio
async def test_result_capacity_target_does_not_become_the_provider_limit():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    request = ModelRequest(
        provider="test",
        model="model",
        capability_snapshot=replace(
            generic_capability_snapshot(),
            context_window_tokens=200_000,
            max_generation_tokens=100_000,
        ),
    )
    budget = resolve_invocation_output_budget(
        request.capability_snapshot,
        max_generation_tokens=None,
        result_capacity_target_tokens=16_384,
        result_capacity_source=ResultCapacitySource.WORKFLOW_POLICY,
    )

    class RecordingGateway(_CompletionGateway):
        async def complete(self, messages, invocation, signal=None):
            self.calls.append((tuple(messages), invocation))
            return await super().complete(messages, invocation, signal)

    gateway = RecordingGateway()
    completed = await AgentModelInvocationManager(gateway).complete(
        (AgentMessage(role="user", content="draft"),),
        AgentModelCall(
            request=request,
            output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
            commit_mode=OutputCommitMode.PRIVATE,
            output_budget=budget,
        ),
        ModelInvocationContext(run_id="run-result-capacity"),
    )

    assert completed.receipt.output_budget.result_capacity_target_tokens == 16_384
    assert completed.receipt.output_budget.max_generation_tokens == 100_000
    assert gateway.calls[0][1].max_generation_tokens == 100_000


@pytest.mark.asyncio
async def test_caller_cannot_disguise_a_workflow_cap_as_context_capacity():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    request = _request()
    forged = InvocationOutputBudget(
        max_generation_tokens=100,
        generation_source=GenerationBudgetSource.CONTEXT_CAPACITY,
        profile_max_generation_tokens=200,
    )

    class RecordingGateway(_CompletionGateway):
        async def complete(self, messages, invocation, signal=None):
            self.calls.append((tuple(messages), invocation))
            return await super().complete(messages, invocation, signal)

    gateway = RecordingGateway()
    completed = await AgentModelInvocationManager(gateway).complete(
        (),
        AgentModelCall(
            request=request,
            output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
            commit_mode=OutputCommitMode.PRIVATE,
            output_budget=forged,
        ),
        ModelInvocationContext(run_id="run-no-workflow-cap"),
    )

    assert completed.receipt.output_budget.max_generation_tokens == 200
    assert gateway.calls[0][1].max_generation_tokens == 200


@pytest.mark.asyncio
async def test_result_capacity_target_must_fit_the_actual_context_remainder():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    request = ModelRequest(
        provider="test",
        model="model",
        capability_snapshot=replace(
            generic_capability_snapshot(),
            context_window_tokens=10_000,
            max_generation_tokens=8_000,
        ),
    )
    budget = resolve_invocation_output_budget(
        request.capability_snapshot,
        max_generation_tokens=None,
        result_capacity_target_tokens=4_000,
        result_capacity_source=ResultCapacitySource.WORKFLOW_POLICY,
    )

    with pytest.raises(UnsupportedModelFeatureError) as captured:
        await AgentModelInvocationManager(_CompletionGateway()).complete(
            (AgentMessage(role="user", content="长" * 7_000),),
            AgentModelCall(
                request=request,
                output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
                commit_mode=OutputCommitMode.PRIVATE,
                output_budget=budget,
            ),
            ModelInvocationContext(run_id="run-capacity-does-not-fit"),
        )

    assert captured.value.code == "model_result_capacity_incompatible"


def test_reasoning_usage_unknown_is_distinct_from_reported_zero():
    unknown = ModelTokenUsage(input_tokens=1, generation_tokens=2)
    reported_zero = ModelTokenUsage(
        input_tokens=1,
        generation_tokens=2,
        reasoning_tokens=0,
    )

    assert unknown.reasoning_tokens is None
    assert reported_zero.reasoning_tokens == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["complete", "stream"])
@pytest.mark.parametrize("call_mode, context_mode", [
    (call_mode, context_mode)
    for call_mode in ReasoningMode
    for context_mode in ReasoningMode
    if call_mode is not context_mode
])
async def test_reasoning_mode_conflict_is_rejected_before_gateway_call(
    method, call_mode, context_mode,
):
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    gateway = _Gateway()
    manager = AgentModelInvocationManager(gateway)

    with pytest.raises(ContractViolationError) as captured:
        await getattr(manager, method)(
            (),
            AgentModelCall(
                request=_request(),
                output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
                commit_mode=OutputCommitMode.PRIVATE,
                reasoning_mode=call_mode,
            ),
            ModelInvocationContext(
                run_id="run-conflict",
                requested_reasoning_mode=context_mode,
            ),
        )

    assert captured.value.code == "model_reasoning_mode_conflict"
    assert gateway.calls == []


@pytest.mark.asyncio
async def test_completion_uses_one_absolute_invocation_deadline():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    observer = _Observer()
    manager = AgentModelInvocationManager(
        _NeverReturningGateway(),
        output_observer=observer,
        invocation_timeout_ms=100,
    )

    with pytest.raises(ExecutionDeadlineExceeded) as exceeded:
        await manager.complete(
            (),
            AgentModelCall(
                request=_request(),
                output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
                commit_mode=OutputCommitMode.PRIVATE,
            ),
            ModelInvocationContext(run_id="run-deadline"),
        )

    assert exceeded.value.code == "model_invocation_deadline_exceeded"
    assert len(observer.aborted) == 1


@pytest.mark.asyncio
async def test_stream_chunk_reads_share_the_creation_deadline():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    observer = _Observer()
    manager = AgentModelInvocationManager(
        _NeverReturningGateway(),
        output_observer=observer,
        invocation_timeout_ms=100,
    )
    managed = await manager.stream(
        (),
        AgentModelCall(
            request=_request(),
            output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
            commit_mode=OutputCommitMode.PRIVATE,
        ),
        ModelInvocationContext(run_id="run-stream-deadline"),
    )

    with pytest.raises(ExecutionDeadlineExceeded) as exceeded:
        await anext(managed.chunks)

    assert exceeded.value.code == "model_invocation_deadline_exceeded"
    assert len(observer.aborted) == 1


@pytest.mark.asyncio
async def test_shorter_parent_run_deadline_preserves_its_reason_code():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    observer = _Observer()
    manager = AgentModelInvocationManager(
        _NeverReturningGateway(),
        output_observer=observer,
        invocation_timeout_ms=1_000,
    )

    with pytest.raises(ExecutionDeadlineExceeded) as exceeded:
        await manager.complete(
            (),
            AgentModelCall(
                request=_request(),
                output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
                commit_mode=OutputCommitMode.PRIVATE,
            ),
            ModelInvocationContext(
                run_id="run-parent-deadline",
                deadline_at_ms=int(time.time() * 1000) + 50,
            ),
        )

    assert exceeded.value.code == "run_deadline_exceeded"
    assert len(observer.aborted) == 1


@pytest.mark.asyncio
async def test_stream_limit_rejects_the_first_exceeding_fragment_before_output():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    observer = _Observer()
    manager = AgentModelInvocationManager(
        _ChunkGateway((
            ModelStreamChunk(content_delta="ab"),
            ModelStreamChunk(content_delta="cd"),
        )),
        output_observer=observer,
        runtime_limits=RuntimeLimits(max_run_generation_tokens=None, max_stream_content_chars=3),
    )
    managed = await manager.stream(
        (),
        AgentModelCall(
            request=_request(),
            output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
            commit_mode=OutputCommitMode.PRIVATE,
        ),
        ModelInvocationContext(run_id="run-stream-limit"),
    )

    assert (await anext(managed.chunks)).content_delta == "ab"
    with pytest.raises(Exception) as exceeded:
        await anext(managed.chunks)

    assert getattr(exceeded.value, "code", None) == "model_stream_limit_exceeded"
    assert [chunk.content_delta for _, chunk in observer.accepted] == ["ab"]


@pytest.mark.asyncio
async def test_late_tool_name_rechecks_the_registration_specific_limit():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    observer = _Observer()
    manager = AgentModelInvocationManager(
        _ChunkGateway((
            ModelStreamChunk(tool_call_deltas=(ToolCallDelta(
                index=0,
                id="call-1",
                arguments_fragment="1234",
            ),)),
            ModelStreamChunk(tool_call_deltas=(ToolCallDelta(
                index=0,
                name="narrow",
            ),)),
        )),
        output_observer=observer,
        max_tool_argument_chars=10,
    )
    managed = await manager.stream(
        (),
        AgentModelCall(
            request=_request(),
            output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
            commit_mode=OutputCommitMode.PRIVATE,
            tools=(ToolSchema(
                name="narrow",
                description="Narrow tool.",
                parameters={"type": "object", "properties": {}},
            ),),
        ),
        ModelInvocationContext(
            run_id="run-tool-limit",
            tool_argument_limits={"narrow": 3},
        ),
    )

    await anext(managed.chunks)
    with pytest.raises(Exception) as exceeded:
        await anext(managed.chunks)

    assert getattr(exceeded.value, "code", None) == "model_stream_limit_exceeded"
    assert len(observer.accepted) == 1


@pytest.mark.asyncio
async def test_unconsumed_managed_stream_closes_scope_and_invocation_once():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    output = _Observer()
    operations = _OperationOutput()
    manager = AgentModelInvocationManager(_Gateway(), output_observer=output,
        operation_controller=AgentOperationController(operations))
    stream = await manager.stream((), AgentModelCall(request=_request(),
        output_intent=AgentOutputIntent.STRUCTURED_PRIVATE, commit_mode=OutputCommitMode.PRIVATE),
        ModelInvocationContext(run_id='unconsumed'))
    await stream.chunks.aclose()
    await stream.chunks.aclose()
    assert len(output.aborted) == 1
    assert len(operations.events) == 2
    assert operations.events[-1].status is OperationStatus.CANCELED
