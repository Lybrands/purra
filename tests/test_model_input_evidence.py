from __future__ import annotations

from dataclasses import replace

import pytest

from purra.agent_execution_checkpoint import AgentExecutionCheckpoint
from purra.context_budget import allocate_context_budget
from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    DomainContext,
    ModelCompletion,
    ModelFinishReason,
    ModelRequest,
    ModelStream,
    ModelStreamChunk,
    RuntimeLimits,
    RuntimeOutcome,
    ToolBatchOutcome,
    ToolBatchResult,
    ToolCallDelta,
    ToolCallResult,
    ToolEffectState,
    ToolSchema,
)
from purra.evidence import ContextEvidenceReceipt, RunEvidenceStore
from purra.errors import ContractViolationError
from purra.model_execution import AgentModelTask, AgentModelTaskRunner
from purra.model_invocation import (
    AgentModelInvocationManager,
    ModelInvocationContext,
)
from purra.model_protocol import generic_capability_snapshot
from purra.runtime import AgentRuntime


def _request(messages: tuple[AgentMessage, ...]) -> AgentRunRequest:
    return AgentRunRequest(
        messages=messages,
        model=ModelRequest(
            provider="fixture",
            model="fixture",
            capability_snapshot=replace(
                generic_capability_snapshot(),
                profile_id="fixture:model",
                max_call_output_tokens=128,
            ),
        ),
        domain_context=DomainContext(namespace="fixture"),
        context_window=16_000,
        tools_enabled=True,
    )


def _receipt() -> ContextEvidenceReceipt:
    return ContextEvidenceReceipt(
        evidence_id="mem0:store:memory-1:3",
        context_block="memory",
        source="mem0/scope",
        item_id="memory-1",
        version=3,
    )


def _evidence_message(content: str = "remembered fact") -> AgentMessage:
    receipt = _receipt()
    return AgentMessage(
        role="system",
        content=content,
        attributes={"context_name": receipt.context_block},
        host_metadata={
            "context_evidence_receipts": [{
                "evidenceId": receipt.evidence_id,
                "source": receipt.source,
                "itemId": receipt.item_id,
                "version": receipt.version,
            }],
        },
    )


class _RejectingValidator:
    def __init__(self) -> None:
        self.calls: list[tuple[ContextEvidenceReceipt, ...]] = []

    async def validate_evidence(self, receipts, *, signal=None):
        del signal
        self.calls.append(tuple(receipts))
        raise ContractViolationError(
            "external evidence is stale",
            code="external_evidence_stale",
        )


class _CountingGateway:
    def __init__(self) -> None:
        self.calls = 0

    async def stream(self, messages, invocation, signal=None):
        del messages, signal
        self.calls += 1

        async def chunks():
            yield ModelStreamChunk(
                content_delta="answer",
                finish_reason=ModelFinishReason.STOP,
            )

        return ModelStream(
            chunks=chunks(),
            model="fixture",
            applied_output_limit=invocation.output_limit.max_tokens,
        )

    async def complete(self, messages, invocation, signal=None):
        del messages, signal
        self.calls += 1
        return ModelCompletion(
            message=AgentMessage(role="assistant", content="compressed"),
            model="fixture",
            applied_output_limit=invocation.output_limit.max_tokens,
            finish_reason=ModelFinishReason.STOP,
        )


class _ToolThenAnswerGateway(_CountingGateway):
    async def stream(self, messages, invocation, signal=None):
        del messages, signal
        self.calls += 1
        assert self.calls == 1, "stale evidence must block the second Provider call"

        async def chunks():
            yield ModelStreamChunk(
                tool_call_deltas=(ToolCallDelta(
                    index=0,
                    id="call-1",
                    name="search_memory",
                    arguments_fragment='{"query":"memory"}',
                ),),
                finish_reason=ModelFinishReason.TOOL_CALLS,
            )

        return ModelStream(
            chunks=chunks(),
            model="fixture",
            applied_output_limit=invocation.output_limit.max_tokens,
        )


class _EvidenceToolGateway:
    async def execute_batch(self, request, event_sink, signal=None):
        del event_sink, signal
        call, = request.calls
        return ToolBatchResult(
            results=(ToolCallResult(
                tool_call_id=call.id,
                tool_name=call.name,
                content='{"hits":[]}',
                context_evidence=(_receipt(),),
            ),),
            outcome=ToolBatchOutcome.COMPLETED,
            effect_state=ToolEffectState.NOT_STARTED,
        )


@pytest.mark.asyncio
async def test_tool_evidence_is_revalidated_before_the_followup_model_round():
    gateway = _ToolThenAnswerGateway()
    validator = _RejectingValidator()
    runtime = AgentRuntime(
        model_gateway=gateway,
        tool_execution_gateway=_EvidenceToolGateway(),
        evidence_validator=validator,
    )
    schema = ToolSchema(
        name="search_memory",
        description="Search memory.",
        parameters={
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
            "additionalProperties": False,
        },
    )

    updates = [item async for item in runtime.run(
        _request((AgentMessage(role="user", content="remember"),)),
        tools=(schema,),
        run_id="run-tool-evidence",
        scope_tools_to_observer=False,
    )]

    result = updates[-1]
    assert result.outcome is RuntimeOutcome.FAILED
    assert gateway.calls == 1
    assert validator.calls == [(_receipt(),)]


@pytest.mark.asyncio
async def test_checkpoint_evidence_is_revalidated_before_recovery_dispatch():
    store = RunEvidenceStore()
    store.record_context_messages((_evidence_message(),))
    checkpoint = AgentExecutionCheckpoint(
        run_id="run-recovery",
        next_round=1,
        round_limit=4,
        messages=(AgentMessage(role="user", content="continue"),),
        evidence_state=store.checkpoint_mapping(),
    )
    gateway = _CountingGateway()
    validator = _RejectingValidator()
    runtime = AgentRuntime(
        model_gateway=gateway,
        evidence_validator=validator,
    )

    updates = [item async for item in runtime.run(
        _request((AgentMessage(role="user", content="unused"),)),
        run_id=checkpoint.run_id,
        resume_checkpoint=checkpoint,
    )]

    assert updates[-1].outcome is RuntimeOutcome.FAILED
    assert gateway.calls == 0
    assert validator.calls == [(_receipt(),)]


class _ModelCompactor:
    def __init__(self, tasks: AgentModelTaskRunner) -> None:
        self._tasks = tasks

    async def prepare(
        self,
        request,
        signal=None,
        *,
        on_compaction_started=None,
        budget=None,
        operation_scope=None,
    ):
        del on_compaction_started, budget, operation_scope
        await self._tasks.complete(
            (AgentMessage(role="user", content="compress"),),
            AgentModelTask(request=request.model),
            signal,
        )
        raise AssertionError("rejecting validator must stop compaction")


@pytest.mark.asyncio
async def test_runtime_compaction_model_task_inherits_canonical_evidence():
    gateway = _CountingGateway()
    validator = _RejectingValidator()
    manager = AgentModelInvocationManager(
        gateway,
        runtime_limits=RuntimeLimits(max_run_output_tokens=None),
        evidence_validator=validator,
    )
    tasks = AgentModelTaskRunner(
        manager,
        ModelInvocationContext(run_id="run-compression"),
    )
    runtime = AgentRuntime(
        model_gateway=gateway,
        model_manager=manager,
        context_compressor=_ModelCompactor(tasks),
    )
    evidence = _receipt()
    context = _evidence_message()
    request = _request((context, AgentMessage(role="user", content="answer")))
    budget = allocate_context_budget(
        window_tokens=request.context_window,
        output_reserve_tokens=128,
    )

    with pytest.raises(ContractViolationError) as captured:
        async for _item in runtime.run(
            request,
            run_id="run-compression",
            context_budget=budget,
        ):
            pass

    assert captured.value.code == "external_evidence_stale"
    assert gateway.calls == 0
    assert validator.calls == [(evidence,)]
