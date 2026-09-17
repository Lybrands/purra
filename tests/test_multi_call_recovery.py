"""混发读写工具的多调用批次按可恢复失败回传，让模型拆批重发。"""

from __future__ import annotations

import json

import pytest

from purra.contracts import (
    ExecutionState,
    ToolBatchRequest,
    ToolCall,
    ToolHandlerResult,
    ToolPolicy,
    ToolSchema,
)
from purra.ports import ToolRegistration
from purra.recovery import RecoveryLedger
from purra.runtime.tool_recovery import (
    ToolRecoveryDisposition,
    resolve_tool_recovery,
)
from purra.tools import CoreToolExecutor, InMemoryToolCatalog


class _Sink:
    def __init__(self) -> None:
        self.events = []

    async def emit(self, event):
        self.events.append(event)


async def _unexpected_handler(state, arguments, signal=None):
    raise AssertionError("handler must not run for a rejected batch")


def _mixed_catalog():
    return InMemoryToolCatalog((
        ToolRegistration(
            schema=ToolSchema(
                name="readThing",
                description="Read a thing.",
                parameters={"type": "object", "properties": {}},
            ),
            handler=_unexpected_handler,
            policy=ToolPolicy(mode="read", title="Read thing"),
        ),
        ToolRegistration(
            schema=ToolSchema(
                name="mutateThing",
                description="Mutate a thing.",
                parameters={"type": "object", "properties": {}},
            ),
            handler=_unexpected_handler,
            policy=ToolPolicy(mode="propose", title="Mutate thing"),
        ),
    ))


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["propose", "confirm"])
async def test_mixed_read_write_batch_fails_recoverably_before_handlers(mode):
    catalog = InMemoryToolCatalog((
        ToolRegistration(
            schema=ToolSchema(
                name="readThing",
                description="Read a thing.",
                parameters={"type": "object", "properties": {}},
            ),
            handler=_unexpected_handler,
            policy=ToolPolicy(mode="read", title="Read thing"),
        ),
        ToolRegistration(
            schema=ToolSchema(
                name="mutateThing",
                description="Mutate a thing.",
                parameters={"type": "object", "properties": {}},
            ),
            handler=_unexpected_handler,
            policy=ToolPolicy(mode=mode, title="Mutate thing"),
        ),
    ))
    sink = _Sink()

    result = await CoreToolExecutor(catalog).execute_batch(
        ToolBatchRequest(
            "run",
            (
                ToolCall("call-a", "readThing", json.dumps({})),
                ToolCall("call-b", "mutateThing", json.dumps({})),
            ),
            frozenset({"readThing", "mutateThing"}),
            ExecutionState(),
        ),
        sink,
    )

    # 可恢复失败：不进任何 handler，逐调用结果携带同一错误码与恢复指引。
    assert result.outcome.value == "failed"
    assert result.error == "multi_call_batch_requires_read_only_tools"
    assert [item.error for item in result.results] == [
        "multi_call_batch_requires_read_only_tools",
        "multi_call_batch_requires_read_only_tools",
    ]
    assert "Re-issue the rejected calls" in result.results[0].content
    assert sink.events == []


def test_multi_call_batch_error_is_retryable_model_input():
    from purra.contracts import ToolBatchOutcome, ToolBatchResult, ToolCallResult, ToolEffectState

    failure = ToolBatchResult(
        results=(
            ToolCallResult("call-a", "readThing", '{"errorCode":"x"}', error="multi_call_batch_requires_read_only_tools"),
            ToolCallResult("call-b", "mutateThing", '{"errorCode":"x"}', error="multi_call_batch_requires_read_only_tools"),
        ),
        outcome=ToolBatchOutcome.FAILED,
        error="multi_call_batch_requires_read_only_tools",
        effect_state=ToolEffectState.NOT_STARTED,
    )

    resolution = resolve_tool_recovery(
        failure,
        requested_names={"readThing", "mutateThing"},
        planning_available=False,
        recovery_ledger=RecoveryLedger(),
        input_recovery_epoch=0,
        remaining_model_rounds=10,
        round_number=3,
        cancellation_requested=False,
    )

    # 走有界 RETRY_MODEL：回传给模型拆批重发；超限后由账本拒绝终态。
    assert resolution.disposition is ToolRecoveryDisposition.RETRY_MODEL
    assert resolution.messages, "retry guidance must accompany the retry"


def test_multi_call_batch_retry_is_bounded_by_the_ledger():
    from purra.contracts import ToolBatchOutcome, ToolBatchResult, ToolCallResult, ToolEffectState
    from purra.recovery import RecoveryPolicy

    def make_failure():
        return ToolBatchResult(
            results=(
                ToolCallResult("call-a", "readThing", "{}", error="multi_call_batch_requires_read_only_tools"),
            ),
            outcome=ToolBatchOutcome.FAILED,
            error="multi_call_batch_requires_read_only_tools",
            effect_state=ToolEffectState.NOT_STARTED,
        )

    ledger = RecoveryLedger(RecoveryPolicy())
    dispositions = []
    for round_number in range(1, 8):
        resolution = resolve_tool_recovery(
            make_failure(),
            requested_names={"readThing"},
            planning_available=False,
            recovery_ledger=ledger,
            # FAILED 轮不递增 input epoch（与 orchestrator 一致），
            # 连续失败复用同一 scope，账本按此累计并最终拒绝。
            input_recovery_epoch=0,
            remaining_model_rounds=max(0, 20 - round_number),
            round_number=round_number,
            cancellation_requested=False,
        )
        dispositions.append(resolution.disposition)
        if resolution.disposition is ToolRecoveryDisposition.REJECT:
            break

    assert ToolRecoveryDisposition.RETRY_MODEL in dispositions
    assert dispositions[-1] is ToolRecoveryDisposition.REJECT, "ledger must bound retries"
