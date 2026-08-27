from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest

from purra.contracts import (
    AgentMessage,
    AgentRunResult,
    ModelFinishReason,
    ModelRequest,
    ModelStreamChunk,
    RunStatus,
)
from purra.model_invocation import ModelInvocationContext
from purra.model_protocol import generic_capability_snapshot
from purra.api import AgentCoreRunOptions


def _types():
    try:
        from purra.output import (
            AgentResponseTransaction,
            PublicFact,
            PublicFactBundle,
            PublicPresentationMode,
            ResponseTransactionMode,
            ResponseTransactionPolicy,
        )
    except ImportError as error:
        pytest.fail(f"response transaction contracts are missing: {error}")
    return (
        AgentResponseTransaction,
        PublicFact,
        PublicFactBundle,
        PublicPresentationMode,
        ResponseTransactionMode,
        ResponseTransactionPolicy,
    )


def _model_request() -> ModelRequest:
    return ModelRequest(
        provider="test",
        model="model",
        capability_snapshot=replace(
            generic_capability_snapshot(),
            profile_id="test:model",
            max_call_output_tokens=1_024,
        ),
    )


class _Stream:
    def __init__(self, chunks, *, model="model") -> None:
        self.chunks = chunks
        self.receipt = type("Receipt", (), {"model": model})()


class _Manager:
    def __init__(self) -> None:
        self.calls = []
        self.release = asyncio.Event()
        self.first_chunk_seen = asyncio.Event()
        self.presentation_failures = 0
        self.delay_first_public = False
        self.private_responses = []

    async def stream(self, messages, call, context, signal=None):
        del signal
        self.calls.append((tuple(messages), call, context))
        call_number = len(self.calls)

        async def chunks():
            if (
                call.output_intent.value == "final_public"
                and call_number == 1
                and self.delay_first_public
            ):
                self.first_chunk_seen.set()
                yield ModelStreamChunk(content_delta="第一块")
                await self.release.wait()
                yield ModelStreamChunk(
                    content_delta="第二块",
                    finish_reason=ModelFinishReason.STOP,
                )
                return
            if (
                call.output_intent.value == "final_public"
                and self.presentation_failures > 0
            ):
                self.presentation_failures -= 1
                raise RuntimeError("presentation failed")
            content = (
                "公开结论"
                if call.output_intent.value == "final_public"
                else (
                    self.private_responses.pop(0)
                    if self.private_responses
                    else "私有候选"
                )
            )
            yield ModelStreamChunk(
                content_delta=content,
                finish_reason=ModelFinishReason.STOP,
            )

        return _Stream(chunks())


class _Committer:
    def __init__(self) -> None:
        self.calls = []

    async def commit_candidate(self, run_id, candidate):
        self.calls.append((run_id, candidate))
        return AgentRunResult(
            run_id=run_id,
            status=RunStatus.DONE,
            final_response=candidate,
        )


class _Facts:
    def __init__(self, bundle) -> None:
        self.bundle = bundle
        self.calls = []

    async def facts_for(self, run_id, result):
        self.calls.append((run_id, result))
        return self.bundle


@pytest.mark.asyncio
async def test_direct_answer_streams_before_provider_finish():
    (
        Transaction,
        _PublicFact,
        _PublicFactBundle,
        _PresentationMode,
        TransactionMode,
        Policy,
    ) = _types()
    manager = _Manager()
    manager.delay_first_public = True
    transaction = Transaction(
        manager,
        policy=Policy(mode=TransactionMode.DIRECT_LIVE),
    )

    result_task = asyncio.create_task(transaction.execute_direct(
        (AgentMessage(role="user", content="answer"),),
        request=_model_request(),
        context=ModelInvocationContext(run_id="run-1"),
    ))
    await manager.first_chunk_seen.wait()

    assert not result_task.done()
    assert manager.calls[0][1].output_intent.value == "final_public"
    assert manager.calls[0][1].commit_mode.value == "live"
    manager.release.set()
    result = await result_task
    assert result.final_response == "第一块第二块"


@pytest.mark.asyncio
async def test_validated_candidate_is_private_until_commit():
    (
        Transaction,
        PublicFact,
        PublicFactBundle,
        PresentationMode,
        TransactionMode,
        Policy,
    ) = _types()
    manager = _Manager()
    committer = _Committer()
    facts = _Facts(PublicFactBundle(facts=(
        PublicFact("resultSummary", "8 episodes reviewed"),
    )))
    transaction = Transaction(
        manager,
        policy=Policy(
            mode=TransactionMode.VALIDATED_RESULT,
            public_presentation=PresentationMode.NONE,
        ),
        facts_provider=facts,
    )

    result = await transaction.execute_validated(
        (AgentMessage(role="user", content="review"),),
        committer,
        request=_model_request(),
        context=ModelInvocationContext(run_id="run-1"),
    )

    assert result.status is RunStatus.DONE
    assert committer.calls == [("run-1", "私有候选")]
    assert manager.calls[0][1].output_intent.value == "structured_private"
    assert manager.calls[0][1].commit_mode.value == "gated"
    assert facts.calls == []
    assert len(manager.calls) == 1


@pytest.mark.asyncio
async def test_public_presentation_uses_only_committed_facts_and_no_tools():
    (
        Transaction,
        PublicFact,
        PublicFactBundle,
        PresentationMode,
        TransactionMode,
        Policy,
    ) = _types()
    manager = _Manager()
    facts = _Facts(PublicFactBundle(
        facts=(PublicFact("resultSummary", "review completed"),),
        resource_refs=("resource://review/public-result",),
    ))
    transaction = Transaction(
        manager,
        policy=Policy(
            mode=TransactionMode.VALIDATED_RESULT,
            public_presentation=PresentationMode.MODEL_LIVE,
        ),
        facts_provider=facts,
    )
    committed = AgentRunResult(
        run_id="run-1",
        status=RunStatus.DONE,
        final_response="private body",
    )

    response = await transaction.present(
        committed,
        request=_model_request(),
        context=ModelInvocationContext(run_id="run-1"),
    )

    messages, call, _context = manager.calls[0]
    assert response == "公开结论"
    assert messages == facts.bundle.as_messages()
    assert call.tools == ()
    assert call.tool_choice.value == "none"
    assert call.output_intent.value == "final_public"
    assert facts.calls == [("run-1", committed)]


@pytest.mark.asyncio
async def test_presentation_retry_does_not_repeat_candidate_commit():
    (
        Transaction,
        PublicFact,
        PublicFactBundle,
        PresentationMode,
        TransactionMode,
        Policy,
    ) = _types()
    manager = _Manager()
    manager.presentation_failures = 1
    committer = _Committer()
    facts = _Facts(PublicFactBundle(facts=(
        PublicFact("resultSummary", "committed"),
    )))
    transaction = Transaction(
        manager,
        policy=Policy(
            mode=TransactionMode.VALIDATED_RESULT,
            public_presentation=PresentationMode.MODEL_LIVE,
        ),
        facts_provider=facts,
        max_presentation_attempts=2,
    )

    result = await transaction.execute_validated(
        (AgentMessage(role="user", content="review"),),
        committer,
        request=_model_request(),
        context=ModelInvocationContext(run_id="run-1"),
    )

    assert result.final_response == "公开结论"
    assert committer.calls == [("run-1", "私有候选")]
    assert len(facts.calls) == 1
    assert [call.output_intent.value for _, call, _ in manager.calls] == [
        "structured_private",
        "final_public",
        "final_public",
    ]


@pytest.mark.asyncio
async def test_private_candidate_repair_commits_only_accepted_candidate_once():
    (
        Transaction,
        _PublicFact,
        _PublicFactBundle,
        PresentationMode,
        TransactionMode,
        Policy,
    ) = _types()
    manager = _Manager()
    manager.private_responses = ["invalid", "accepted"]
    committer = _Committer()

    class Validator:
        def validate(self, *, content, messages):
            del messages
            from purra.contracts import ResponseValidationResult

            if content == "accepted":
                return ResponseValidationResult()
            return ResponseValidationResult(
                violation_code="fixture.invalid",
                repair_guidance="Return the accepted fixture.",
            )

    transaction = Transaction(
        manager,
        policy=Policy(
            mode=TransactionMode.VALIDATED_RESULT,
            public_presentation=PresentationMode.NONE,
        ),
        validators=(Validator(),),
    )

    result = await transaction.execute_validated(
        (AgentMessage(role="user", content="review"),),
        committer,
        request=_model_request(),
        context=ModelInvocationContext(run_id="run-1"),
    )

    assert result.final_response == "accepted"
    assert committer.calls == [("run-1", "accepted")]
    assert manager.calls[1][0][-2].content == "invalid"
    assert "Return the accepted fixture" in manager.calls[1][0][-1].content


def test_public_fact_bundle_rejects_internal_ids_and_unbounded_body():
    (
        _Transaction,
        PublicFact,
        PublicFactBundle,
        _PresentationMode,
        _TransactionMode,
        _Policy,
    ) = _types()

    with pytest.raises(ValueError, match="forbidden"):
        PublicFactBundle(facts=(
            PublicFact("reviewId", "review_internal_123"),
        ))
    with pytest.raises(ValueError, match="forbidden|size"):
        PublicFactBundle(facts=(
            PublicFact("contentText", "x" * 20_000),
        ))
    with pytest.raises(ValueError, match="resource reference"):
        PublicFactBundle(
            facts=(PublicFact("resultSummary", "done"),),
            resource_refs=("review_internal_123",),
        )


def test_run_options_reject_incompatible_response_transactions():
    (
        _Transaction,
        PublicFact,
        PublicFactBundle,
        PresentationMode,
        TransactionMode,
        Policy,
    ) = _types()

    class Validator:
        def validate(self, *, content, messages):
            del content, messages

    with pytest.raises(ValueError, match="full-text validation"):
        AgentCoreRunOptions(
            response_validators=(Validator(),),
            response_transaction_policy=Policy(
                mode=TransactionMode.DIRECT_LIVE,
            ),
        )
    with pytest.raises(ValueError, match="facts provider"):
        AgentCoreRunOptions(response_transaction_policy=Policy(
            mode=TransactionMode.VALIDATED_RESULT,
            public_presentation=PresentationMode.MODEL_LIVE,
        ))

    inferred = AgentCoreRunOptions(response_validators=(Validator(),))
    assert inferred.resolved_response_transaction_policy.mode is (
        TransactionMode.VALIDATED_RESULT
    )

    class Facts:
        async def facts_for(self, run_id, result):
            del run_id, result
            return PublicFactBundle(facts=(PublicFact("summary", "done"),))

    configured = AgentCoreRunOptions(
        response_transaction_policy=Policy(
            mode=TransactionMode.VALIDATED_RESULT,
            public_presentation=PresentationMode.MODEL_LIVE,
        ),
        committed_result_facts_provider=Facts(),
    )
    assert configured.committed_result_facts_provider is not None
