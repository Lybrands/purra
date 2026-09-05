from __future__ import annotations

import asyncio
from dataclasses import fields, replace

import pytest

import purra.contracts as contracts
from purra.cancellation import ExecutionDeadlineExceeded
from purra.contracts import (
    AgentMessage,
    ModelRequest,
    ModelStream,
    ModelStreamChunk,
    RuntimeLimits,
)
from purra.errors import ContractViolationError
from purra.model_invocation import (
    AgentModelCall,
    AgentModelInvocationManager,
    ModelInvocationContext,
)
from purra.model_protocol import generic_capability_snapshot
from purra.output import AgentOutputIntent, OutputCommitMode


def test_model_stream_activity_contract_exists():
    assert hasattr(contracts, "ModelStreamActivity")
    assert hasattr(contracts, "ModelStreamActivityKind")
    assert hasattr(contracts, "ModelStreamActivitySupport")
    assert hasattr(contracts, "ModelStreamItem")


def test_runtime_limits_separate_activity_progress_and_absolute_bounds():
    names = {field.name for field in fields(RuntimeLimits)}
    assert {
        "provider_activity_idle_timeout_ms",
        "provider_progress_idle_timeout_ms",
        "provider_invocation_timeout_ms",
    } <= names
    limits = RuntimeLimits(max_run_generation_tokens=None)
    assert limits.provider_activity_idle_timeout_ms == 30_000
    assert limits.provider_progress_idle_timeout_ms == 60_000
    assert limits.provider_invocation_timeout_ms == 300_000


class _Observer:
    def __init__(self) -> None:
        self.accepted = []
        self.aborted = []
        self.finished = []

    async def open_model_stream(self, receipt, spec):
        del receipt, spec

    async def accept_provider_chunk(self, output_stream_id, chunk):
        self.accepted.append((output_stream_id, chunk))

    async def finish_model_stream(self, output_stream_id, finish_reason):
        self.finished.append((output_stream_id, finish_reason))

    async def abort_model_stream(self, output_stream_id, error_code):
        self.aborted.append((output_stream_id, error_code))


class _TimedGateway:
    def __init__(self, *, progressing: bool) -> None:
        self.progressing = progressing

    async def stream(self, messages, invocation, signal=None):
        del messages

        async def chunks():
            if self.progressing:
                while True:
                    await asyncio.sleep(0.005)
                    yield ModelStreamChunk(content_delta="x")
            assert signal is not None
            await signal.wait()
            yield ModelStreamChunk(content_delta="late")

        return ModelStream(
            chunks=chunks(),
            model="model",
            applied_generation_limit=invocation.output_budget.max_generation_tokens,
        )

    async def complete(self, messages, invocation, signal=None):
        del messages, invocation, signal
        raise AssertionError("complete should not be called")


def _call() -> AgentModelCall:
    request = ModelRequest(
        provider="test",
        model="model",
        capability_snapshot=replace(
            generic_capability_snapshot(),
            profile_id="test:model",
            max_generation_tokens=256,
        ),
    )
    return AgentModelCall(
        request=request,
        output_intent=AgentOutputIntent.STRUCTURED_PRIVATE,
        commit_mode=OutputCommitMode.PRIVATE,
    )


async def _current_deadline_outcome(*, progressing: bool):
    observer = _Observer()
    manager = AgentModelInvocationManager(
        _TimedGateway(progressing=progressing),
        output_observer=observer,
        invocation_timeout_ms=30,
    )
    managed = await manager.stream(
        (AgentMessage(role="user", content="test"),),
        _call(),
        ModelInvocationContext(run_id=(
            "run-progressing" if progressing else "run-silent"
        )),
    )
    with pytest.raises(ExecutionDeadlineExceeded) as exceeded:
        async for _ in managed.chunks:
            pass
    return exceeded.value.code, observer


@pytest.mark.asyncio
async def test_current_absolute_only_policy_cannot_classify_liveness():
    silent_code, silent = await _current_deadline_outcome(progressing=False)
    progressing_code, progressing = await _current_deadline_outcome(
        progressing=True,
    )

    assert silent_code == progressing_code == "model_invocation_deadline_exceeded"
    assert silent.accepted == []
    assert len(progressing.accepted) > 0
    assert len(silent.aborted) == len(progressing.aborted) == 1


class _ActivityGateway:
    def __init__(self, *, support, rows, tail_delay: float = 0.0) -> None:
        self.support = support
        self.rows = tuple(rows)
        self.tail_delay = tail_delay
        self.closed = 0

    async def stream(self, messages, invocation, signal=None):
        del messages, signal

        async def chunks():
            try:
                for delay, item in self.rows:
                    await asyncio.sleep(delay)
                    yield item
                if self.tail_delay:
                    await asyncio.sleep(self.tail_delay)
            finally:
                self.closed += 1

        return ModelStream(
            chunks=chunks(),
            model="model",
            applied_generation_limit=invocation.output_budget.max_generation_tokens,
            activity_support=self.support,
        )

    async def complete(self, messages, invocation, signal=None):
        del messages, invocation, signal
        raise AssertionError("complete should not be called")


async def _consume(gateway, limits):
    observer = _Observer()
    gateway.observer = observer
    manager = AgentModelInvocationManager(
        gateway,
        output_observer=observer,
        invocation_timeout_ms=limits.provider_invocation_timeout_ms,
        runtime_limits=limits,
    )
    managed = await manager.stream(
        (AgentMessage(role="user", content="test"),),
        _call(),
        ModelInvocationContext(run_id="run-liveness"),
    )
    values = []
    async for chunk in managed.chunks:
        values.append(chunk)
    return values, observer


@pytest.mark.asyncio
async def test_semantic_only_stream_uses_only_absolute_boundary():
    gateway = _ActivityGateway(
        support=contracts.ModelStreamActivitySupport.SEMANTIC_ONLY,
        rows=((0.03, ModelStreamChunk(
            content_delta="done",
            finish_reason="stop",
        )),),
    )
    chunks, observer = await _consume(
        gateway,
        RuntimeLimits(max_run_generation_tokens=None,
            provider_activity_idle_timeout_ms=5,
            provider_progress_idle_timeout_ms=10,
            provider_invocation_timeout_ms=80,
        ),
    )

    assert [chunk.content_delta for chunk in chunks] == ["done"]
    assert len(observer.finished) == 1
    assert observer.aborted == []
    assert gateway.closed == 1


@pytest.mark.asyncio
async def test_working_activity_renews_both_leases_without_entering_output():
    working = contracts.ModelStreamActivity(
        contracts.ModelStreamActivityKind.WORKING,
    )
    gateway = _ActivityGateway(
        support=contracts.ModelStreamActivitySupport.WORKING,
        rows=(
            (0.004, working),
            (0.004, working),
            (0.004, working),
            (0.004, ModelStreamChunk(content_delta="ok", finish_reason="stop")),
        ),
    )
    chunks, observer = await _consume(
        gateway,
        RuntimeLimits(max_run_generation_tokens=None,
            provider_activity_idle_timeout_ms=7,
            provider_progress_idle_timeout_ms=7,
            provider_invocation_timeout_ms=80,
            max_stream_chunks=1,
        ),
    )

    assert [chunk.content_delta for chunk in chunks] == ["ok"]
    assert len(observer.accepted) == 1
    assert len(observer.finished) == 1
    assert gateway.closed == 1


@pytest.mark.asyncio
async def test_undeclared_activity_fails_before_output_with_stable_code():
    gateway = _ActivityGateway(
        support=contracts.ModelStreamActivitySupport.SEMANTIC_ONLY,
        rows=((0, contracts.ModelStreamActivity(
            contracts.ModelStreamActivityKind.TRANSPORT,
        )),),
    )

    with pytest.raises(ContractViolationError) as captured:
        await _consume(gateway, RuntimeLimits(max_run_generation_tokens=None, provider_invocation_timeout_ms=80))

    assert captured.value.code == "model_stream_activity_unsupported"
    assert gateway.observer.accepted == []
    assert len(gateway.observer.aborted) == 1
    assert gateway.closed == 1


@pytest.mark.asyncio
async def test_transport_only_activity_expires_progress_and_settles_once():
    transport = contracts.ModelStreamActivity(
        contracts.ModelStreamActivityKind.TRANSPORT,
    )
    gateway = _ActivityGateway(
        support=contracts.ModelStreamActivitySupport.TRANSPORT,
        rows=tuple((0.004, transport) for _ in range(20)),
    )

    with pytest.raises(ExecutionDeadlineExceeded) as exceeded:
        await _consume(
            gateway,
            RuntimeLimits(max_run_generation_tokens=None,
                provider_activity_idle_timeout_ms=8,
                provider_progress_idle_timeout_ms=18,
                provider_invocation_timeout_ms=100,
            ),
        )

    assert exceeded.value.code == "model_progress_deadline_exceeded"
    assert len(gateway.observer.aborted) == 1
    assert gateway.observer.finished == []
    assert gateway.closed == 1


@pytest.mark.asyncio
async def test_declared_stream_silence_expires_activity():
    gateway = _ActivityGateway(
        support=contracts.ModelStreamActivitySupport.TRANSPORT,
        rows=(),
        tail_delay=0.1,
    )

    with pytest.raises(ExecutionDeadlineExceeded) as exceeded:
        await _consume(
            gateway,
            RuntimeLimits(max_run_generation_tokens=None,
                provider_activity_idle_timeout_ms=12,
                provider_progress_idle_timeout_ms=40,
                provider_invocation_timeout_ms=100,
            ),
        )

    assert exceeded.value.code == "model_activity_deadline_exceeded"
    assert len(gateway.observer.aborted) == 1
    assert gateway.observer.finished == []
    assert gateway.closed == 1


@pytest.mark.asyncio
async def test_continuous_progress_stops_at_absolute_boundary_once():
    gateway = _ActivityGateway(
        support=contracts.ModelStreamActivitySupport.WORKING,
        rows=tuple(
            (0.004, ModelStreamChunk(content_delta="x"))
            for _ in range(30)
        ),
    )

    with pytest.raises(ExecutionDeadlineExceeded) as exceeded:
        await _consume(
            gateway,
            RuntimeLimits(max_run_generation_tokens=None,
                provider_activity_idle_timeout_ms=10,
                provider_progress_idle_timeout_ms=10,
                provider_invocation_timeout_ms=30,
            ),
        )

    assert exceeded.value.code == "model_invocation_deadline_exceeded"
    assert len(gateway.observer.aborted) == 1
    assert gateway.observer.finished == []
    assert gateway.closed == 1
