"""Run-lifecycle context compaction phases."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import replace
from typing import Any

from purra.cancellation import OperationCanceled, await_with_cancellation
from purra.context_budget import estimate_agent_messages_tokens
from purra.context_orchestration import (
    ContextCompactionBudget,
    ContextCompactionPhase,
    ConversationCompactionResult,
)
from purra.contracts import AgentRunRequest, ContextBudget, ExecutionPlan, StepExecutor, TraceRecord
from purra.engine.context_phase import assemble_messages
from purra.errors import ContractViolationError
from purra.events import AgentEvent
from purra.json_values import thaw_json_mapping
from purra.operations import OperationScope
from purra.ports import CancellationSignal, ConversationCompactor
from purra.run_controller import AgentRunController


PublishEvent = Callable[[str | None, AgentEvent], Awaitable[None]]


async def compact_before_planning(
    *,
    compactor: ConversationCompactor,
    request: AgentRunRequest,
    budget: ContextBudget,
    controller: AgentRunController,
    publish: PublishEvent,
    signal: CancellationSignal | None,
) -> tuple[AgentRunRequest, dict[str, Any], tuple[AgentEvent, ...]]:
    started = asyncio.Event()
    started_payload: dict[str, Any] = {}

    async def notify(payload: Mapping[str, Any]) -> None:
        started_payload.update(dict(payload))
        started.set()

    task = asyncio.create_task(await_with_cancellation(
        compactor.prepare(
            request,
            signal,
            on_compaction_started=notify,
            budget=ContextCompactionBudget(
                phase=ContextCompactionPhase.PRE_PLANNING,
                provider_input_tokens=budget.provider_input_tokens,
                context_tokens=sum(budget.context_allocations.values()),
                context_tokens_are_resolved=False,
                output_reserve_tokens=budget.output_reserve_tokens,
            ),
            operation_scope=OperationScope(run_id=controller.run_id),
        ),
        signal,
    ))
    wait_started = asyncio.create_task(started.wait())
    result: ConversationCompactionResult | None = None
    events: list[AgentEvent] = []
    diagnostics: dict[str, Any] = {"outcome": "not_configured"}
    try:
        await asyncio.wait((task, wait_started), return_when=asyncio.FIRST_COMPLETED)
        if started.is_set():
            event = AgentEvent(
                type="conversation.compaction.started",
                run_id=controller.run_id,
                payload={
                    "status": "running",
                    "phase": "pre_planning",
                    "postPlanning": False,
                    **started_payload,
                },
            )
            await publish(controller.run_id, event)
            events.append(event)
        candidate = await task
        if not isinstance(candidate, ConversationCompactionResult):
            raise ContractViolationError(
                "conversation compactor returned an invalid result"
            )
        result = candidate
    except OperationCanceled:
        raise
    except Exception as error:
        diagnostics = {"outcome": "failed_open", "phase": "pre_planning"}
        await _record_compaction_error(
            controller,
            stage="conversation_compaction",
            error=error,
            phase="pre_planning",
        )
    finally:
        await _close_compaction_tasks(task, wait_started)

    if result is not None:
        request = result.request
        diagnostics = {
            **thaw_json_mapping(result.diagnostics),
            "outcome": result.outcome,
            "phase": "pre_planning",
            "compactedTurnCount": result.compacted_turn_count,
            "retainedRawTurnCount": result.retained_raw_turn_count,
            "summaryVersion": result.compression_state_version,
        }
        await controller.record_trace(TraceRecord(
            stage="conversation_compaction",
            outcome=result.outcome,
            details={
                key: value for key, value in diagnostics.items()
                if key != "outcome"
            },
        ))
    if started.is_set():
        event = _completed_event(controller, result, phase="pre_planning")
        await publish(controller.run_id, event)
        events.append(event)
    return request, diagnostics, tuple(events)


async def compact_after_planning(
    *,
    compactor: ConversationCompactor,
    source_request: AgentRunRequest,
    request: AgentRunRequest,
    budget: ContextBudget,
    context_blocks: Sequence,
    plan: ExecutionPlan | None,
    selected_names: frozenset[str],
    controller: AgentRunController,
    publish: PublishEvent,
    signal: CancellationSignal | None,
) -> tuple[AgentRunRequest, dict[str, Any], tuple[AgentEvent, ...]]:
    diagnostics: dict[str, Any] = {
        "outcome": "not_configured",
        "plannedStepCount": len(plan.steps) if plan is not None else 0,
        "plannedToolCount": (
            sum(step.executor is StepExecutor.TOOL for step in plan.steps)
            if plan is not None else 0
        ),
        "selectedToolCount": len(selected_names),
    }
    resolved_tokens = estimate_agent_messages_tokens(
        assemble_messages((), context_blocks, plan)
    )
    started = asyncio.Event()
    started_payload: dict[str, Any] = {}

    async def notify(payload: Mapping[str, Any]) -> None:
        started_payload.update(dict(payload))
        started.set()

    compact_request = replace(
        source_request,
        metadata={**source_request.metadata, **request.metadata},
    )
    task = asyncio.create_task(await_with_cancellation(
        compactor.prepare(
            compact_request,
            signal,
            budget=ContextCompactionBudget(
                phase=ContextCompactionPhase.POST_PLANNING,
                provider_input_tokens=budget.provider_input_tokens,
                context_tokens=resolved_tokens,
                context_tokens_are_resolved=True,
                output_reserve_tokens=budget.output_reserve_tokens,
                planned_step_count=diagnostics["plannedStepCount"],
                planned_tool_count=diagnostics["plannedToolCount"],
                selected_tool_count=len(selected_names),
            ),
            on_compaction_started=notify,
            operation_scope=OperationScope(run_id=controller.run_id),
        ),
        signal,
    ))
    wait_started = asyncio.create_task(started.wait())
    result: ConversationCompactionResult | None = None
    failed = False
    events: list[AgentEvent] = []
    try:
        await asyncio.wait((task, wait_started), return_when=asyncio.FIRST_COMPLETED)
        if started.is_set():
            event = AgentEvent(
                type="conversation.compaction.started",
                run_id=controller.run_id,
                payload={
                    "status": "running",
                    "phase": "post_planning",
                    "postPlanning": True,
                    **started_payload,
                },
            )
            await publish(controller.run_id, event)
            events.append(event)
        candidate = await task
        if not isinstance(candidate, ConversationCompactionResult):
            raise ContractViolationError(
                "conversation compactor returned an invalid post-planning result"
            )
        result = replace(
            candidate,
            request=replace(
                candidate.request,
                metadata={**request.metadata, **candidate.request.metadata},
            ),
        )
    except OperationCanceled:
        raise
    except Exception as error:
        failed = True
        await _record_compaction_error(
            controller,
            stage="post_planning_context_optimization",
            error=error,
            phase=None,
        )
    finally:
        await _close_compaction_tasks(task, wait_started)

    if result is not None:
        request = result.request
        diagnostics = {
            **diagnostics,
            **thaw_json_mapping(result.diagnostics),
            "outcome": result.outcome,
            "postPlanning": True,
            "resolvedContextTokens": resolved_tokens,
            "compactedTurnCount": result.compacted_turn_count,
            "retainedRawTurnCount": result.retained_raw_turn_count,
            "summaryVersion": result.compression_state_version,
        }
        await controller.record_trace(TraceRecord(
            stage="post_planning_context_optimization",
            outcome=result.outcome,
            details=diagnostics,
        ))
    elif failed:
        diagnostics = {
            **diagnostics,
            "outcome": "failed_open",
            "postPlanning": True,
            "resolvedContextTokens": resolved_tokens,
        }
    if started.is_set():
        event = _completed_event(controller, result, phase="post_planning")
        await publish(controller.run_id, event)
        events.append(event)
    return request, diagnostics, tuple(events)


async def _close_compaction_tasks(task: asyncio.Task, wait_started: asyncio.Task) -> None:
    if not wait_started.done():
        wait_started.cancel()
    if not task.done():
        task.cancel()
    with suppress(asyncio.CancelledError):
        await wait_started
    with suppress(asyncio.CancelledError, Exception):
        await task


async def _record_compaction_error(
    controller: AgentRunController,
    *,
    stage: str,
    error: Exception,
    phase: str | None,
) -> None:
    await controller.record_trace(TraceRecord(
        stage=stage,
        outcome="failed_open",
        details={
            "errorType": type(error).__name__,
            **({"phase": phase} if phase is not None else {}),
        },
    ))


def _completed_event(
    controller: AgentRunController,
    result: ConversationCompactionResult | None,
    *,
    phase: str,
) -> AgentEvent:
    return AgentEvent(
        type="conversation.compaction.completed",
        run_id=controller.run_id,
        payload={
            "status": (
                "completed"
                if result is not None and result.outcome.startswith("compacted")
                else "failed"
            ),
            "phase": phase,
            "postPlanning": phase == "post_planning",
            "outcome": result.outcome if result is not None else "failed_open",
            "compactedTurnCount": result.compacted_turn_count if result else 0,
            "retainedRawTurnCount": result.retained_raw_turn_count if result else 0,
            "summaryVersion": result.compression_state_version if result else None,
        },
    )


__all__: list[str] = []
