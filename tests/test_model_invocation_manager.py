from __future__ import annotations

from dataclasses import replace

import pytest

from purra.contracts import (
    AgentMessage,
    ModelCompletion,
    ModelFinishReason,
    ModelRequest,
    ModelStream,
    ModelStreamChunk,
    ReasoningMode,
    ToolSchema,
)
from purra.errors import ContractViolationError
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
            max_output_tokens=200,
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

        return ModelStream(chunks=chunks(), model="model")

    async def complete(self, messages, invocation, signal=None):
        raise AssertionError("complete should not be called")


class _CompletionGateway(_Gateway):
    async def complete(self, messages, invocation, signal=None):
        del messages, invocation, signal
        return ModelCompletion(
            message=AgentMessage(role="assistant", content='{"plan":true}'),
            model="model",
            finish_reason=ModelFinishReason.STOP,
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
            ModelInvocationContext(run_id="run-1", turn_id="turn-1"),
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
        ModelInvocationContext(run_id="run-1", turn_id="turn-1"),
    )
    chunk = await anext(managed.chunks)

    assert observer.accepted == [(managed.receipt.output_stream_id, chunk)]
    assert observer.opened[0][0] == managed.receipt
    assert observer.opened[0][1].run_id == "run-1"
    assert observer.opened[0][1].turn_id == "turn-1"


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
        ModelInvocationContext(run_id="run-1", turn_id="turn-1"),
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
        ModelInvocationContext(run_id="run-1"),
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
        ModelInvocationContext(run_id="run-1"),
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
            ModelInvocationContext(run_id="run-1"),
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
        ModelInvocationContext(run_id="run-1"),
    )

    assert observer.accepted == [(
        completed.receipt.output_stream_id,
        ModelStreamChunk(content_delta='{"plan":true}'),
    )]
    assert observer.finished == [(
        completed.receipt.output_stream_id,
        ModelFinishReason.STOP,
    )]
