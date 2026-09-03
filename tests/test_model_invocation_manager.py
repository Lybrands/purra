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
from purra.errors import ContractViolationError
from purra.evidence import ContextEvidenceReceipt
from purra.model_invocation.evidence import bind_model_input_evidence
from purra.cancellation import ExecutionDeadlineExceeded
from purra.model_protocol import generic_capability_snapshot
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
            max_call_output_tokens=200,
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
            applied_output_limit=invocation.output_limit.max_tokens,
        )

    async def complete(self, messages, invocation, signal=None):
        raise AssertionError("complete should not be called")


class _CompletionGateway(_Gateway):
    async def complete(self, messages, invocation, signal=None):
        del messages, signal
        return ModelCompletion(
            message=AgentMessage(role="assistant", content='{"plan":true}'),
            model="model",
            applied_output_limit=invocation.output_limit.max_tokens,
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
            applied_output_limit=invocation.output_limit.max_tokens,
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
            applied_output_limit=invocation.output_limit.max_tokens,
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
async def test_stream_rejects_a_missing_applied_output_limit_acknowledgment():
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
    ("applied_output_limit", "usage"),
    (
        (199, None),
        (200, ModelTokenUsage(input_tokens=1, output_tokens=201)),
    ),
)
async def test_completion_rejects_a_false_output_limit_acknowledgment(
    applied_output_limit,
    usage,
):
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()

    class FalseAcknowledgmentGateway(_CompletionGateway):
        async def complete(self, messages, invocation, signal=None):
            del messages, invocation, signal
            return ModelCompletion(
                message=AgentMessage(role="assistant", content="done"),
                model="model",
                applied_output_limit=applied_output_limit,
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
async def test_reasoning_mode_conflict_is_rejected_before_gateway_call():
    AgentModelCall, AgentModelInvocationManager, ModelInvocationContext = _types()
    gateway = _CompletionGateway()
    manager = AgentModelInvocationManager(gateway)

    with pytest.raises(ContractViolationError) as captured:
        await manager.complete(
            (),
            AgentModelCall(
                request=_request(),
                output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
                commit_mode=OutputCommitMode.PRIVATE,
                reasoning_mode=ReasoningMode.DISABLED,
            ),
            ModelInvocationContext(
                run_id="run-conflict",
                requested_reasoning_mode=ReasoningMode.ENABLED,
            ),
        )

    assert captured.value.code == "model_reasoning_mode_conflict"


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
        runtime_limits=RuntimeLimits(max_run_output_tokens=None, max_stream_content_chars=3),
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
