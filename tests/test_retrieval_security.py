from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from purra.contracts import (
    ExecutionState,
    ToolBatchRequest,
    ToolCall,
    ToolExecutionLimits,
)
from purra.retrieval import (
    RetrievalError,
    RetrievalHit,
    RetrievalRequest,
    RetrieverTool,
)
from purra.tools import InMemoryToolCatalog
from purra.tools.executor import CoreToolExecutor


FIXTURE = json.loads(
    (Path(__file__).parents[1] / "conformance" / "fixtures" / "retrieval.json")
    .read_text(encoding="utf-8")
)


class _FailingRetriever:
    def __init__(self, error: Exception) -> None:
        self.error = error
        self.calls = 0

    async def retrieve(self, request, signal=None):
        del request, signal
        self.calls += 1
        raise self.error


class _InvalidRetriever:
    async def retrieve(self, request, signal=None):
        del request, signal
        return ({"id": "not-a-hit"},)


class _OversizedRetriever:
    async def retrieve(self, request, signal=None):
        del request, signal
        return (RetrievalHit(
            id="large",
            content="x" * 1_000,
            source="fixture",
        ),)


@pytest.mark.asyncio
async def test_model_cannot_supply_host_owned_retrieval_fields() -> None:
    retriever = _FailingRetriever(AssertionError("handler must not run"))
    registration = RetrieverTool(
        retriever=retriever,
        name="search_knowledge",
        description="Search configured knowledge.",
    ).registration

    invalid_arguments = [
        *FIXTURE["forbiddenModelArguments"],
        {"query": ""},
        {"query": "x" * (FIXTURE["defaults"]["maxQueryChars"] + 1)},
    ]
    for arguments in invalid_arguments:
        result = await _execute(registration, arguments=arguments)
        assert result.results[0].error == "invalid_tool_arguments_schema"
    assert retriever.calls == 0


@pytest.mark.asyncio
async def test_unknown_retrieval_error_is_left_to_the_existing_tool_boundary() -> None:
    registration = RetrieverTool(
        retriever=_FailingRetriever(RuntimeError("secret backend failure")),
        name="search_knowledge",
        description="Search configured knowledge.",
    ).registration

    result = await _execute(registration)
    assert result.results[0].error == "tool_execution_failed"
    assert "secret backend failure" not in result.results[0].content


@pytest.mark.asyncio
@pytest.mark.parametrize("code", FIXTURE["stableErrors"])
async def test_known_retrieval_error_keeps_code_and_hides_details(code) -> None:
    retriever = _FailingRetriever(RetrievalError(
        "secret-token at private/path",
        code=code,
        details={"scope": "private-project"},
    ))
    registration = RetrieverTool(
        retriever=retriever,
        name="search_knowledge",
        description="Search configured knowledge.",
    ).registration

    batch = await _execute(registration)
    result = batch.results[0]

    assert result.error == code
    assert "secret-token" not in result.content
    assert "private/path" not in result.content
    assert "private-project" not in result.content


@pytest.mark.asyncio
async def test_invalid_and_oversized_results_fail_closed() -> None:
    invalid = RetrieverTool(
        retriever=_InvalidRetriever(),
        name="invalid_search",
        description="Search configured knowledge.",
    ).registration
    oversized = RetrieverTool(
        retriever=_OversizedRetriever(),
        name="oversized_search",
        description="Search configured knowledge.",
        max_result_chars=120,
    ).registration

    invalid_result = (await _execute(invalid)).results[0]
    oversized_result = (await _execute(oversized)).results[0]

    assert invalid_result.error == "invalid_retrieval_result"
    assert oversized_result.error == "retrieval_result_too_large"
    assert "x" * 50 not in oversized_result.content


@pytest.mark.asyncio
async def test_retriever_cannot_bypass_the_existing_tool_allowlist() -> None:
    retriever = _FailingRetriever(AssertionError("must not run"))
    registration = RetrieverTool(
        retriever=retriever,
        name="search_knowledge",
        description="Search configured knowledge.",
    ).registration

    result = await _execute(registration, allowed=False)

    assert result.results[0].error == "tool_not_authorized"
    assert retriever.calls == 0


@pytest.mark.asyncio
async def test_application_authorization_covers_direct_and_tool_retrieval() -> None:
    # This application-owned authority is deliberately separate from model input.
    bindings = {"authorized-run": "project-1"}
    reads = []

    class AuthorizedRetriever:
        async def retrieve(self, request, signal=None):
            namespace = bindings.get(request.run_id)
            if namespace is None:
                raise RetrievalError("No binding", code="retrieval_scope_unavailable")
            if request.scope.get("namespace") != namespace:
                raise RetrievalError("Wrong scope", code="retrieval_access_denied")
            reads.append(namespace)
            return ()

    retriever = AuthorizedRetriever()
    for row in FIXTURE["scopeCases"]:
        before = len(reads)
        request = RetrievalRequest(
            query="canon", limit=8, run_id=row["runId"], scope=row["scope"],
        )
        if row["errorCode"] is None:
            assert await retriever.retrieve(request) == ()
        else:
            with pytest.raises(RetrievalError) as raised:
                await retriever.retrieve(request)
            assert raised.value.code == row["errorCode"]

        registration = RetrieverTool(
            retriever=retriever,
            name="search_knowledge",
            description="Search authorized knowledge.",
            scope=row["scope"],
        ).registration
        result = await _execute(registration, state=ExecutionState(
            run_id=row["runId"],
            domain={"namespace": "project-1", "run_id": "authorized-run"},
        ))
        assert result.results[0].error == row["errorCode"]
        assert len(reads) - before == (2 if row["errorCode"] is None else 0)


@pytest.mark.asyncio
async def test_canceled_retrieval_discards_late_results() -> None:
    signal = asyncio.Event()
    started = asyncio.Event()
    cleaned_up = asyncio.Event()

    class LateRetriever:
        async def retrieve(self, request, received_signal=None):
            assert received_signal is signal
            started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                # A backend may race cancellation and still produce a result.
                return (RetrievalHit(id="late", content="late evidence", source="fixture"),)
            finally:
                cleaned_up.set()

    registration = RetrieverTool(
        retriever=LateRetriever(),
        name="search_knowledge",
        description="Search configured knowledge.",
    ).registration
    async with asyncio.timeout(2):
        running = asyncio.create_task(_execute(registration, signal=signal))
        try:
            await started.wait()
            signal.set()
            result = await running
        finally:
            signal.set()
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)

    assert result.outcome.value == "canceled"
    assert result.error == "tool_execution_canceled"
    assert cleaned_up.is_set()
    assert all("late evidence" not in item.content for item in result.results)


@pytest.mark.asyncio
async def test_global_tool_budget_remains_authoritative() -> None:
    registration = RetrieverTool(
        retriever=_OversizedRetriever(),
        name="search_knowledge",
        description="Search configured knowledge.",
        max_result_chars=2_000,
    ).registration
    result = await _execute(registration, limits=ToolExecutionLimits(max_result_chars=120))

    assert result.results[0].error == "tool_result_too_large"
    assert "x" * 50 not in result.results[0].content


async def _execute(
    registration, *, arguments=None, state=None, signal=None,
    allowed=True, limits=ToolExecutionLimits(),
):
    state = state if state is not None else ExecutionState(run_id="run-1")

    class Sink:
        async def emit(self, event):
            pass

    return await CoreToolExecutor(
        InMemoryToolCatalog((registration,)), limits=limits,
    ).execute_batch(
        ToolBatchRequest(
            run_id=state.run_id,
            invocation_id="retrieval-invocation",
            calls=(ToolCall(
                id="retrieval-call",
                name=registration.schema.name,
                arguments_json=json.dumps(arguments if arguments is not None else {"query": "canon"}),
            ),),
            allowed_tool_names=frozenset({registration.schema.name}) if allowed else frozenset(),
            state=state,
        ),
        Sink(),
        signal,
    )
