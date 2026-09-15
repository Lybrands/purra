"""Durable checkpoint resume and long-task continuation for AgentCore."""

from __future__ import annotations

from purra.engine.orchestration_support import (
    _run_result,
    _runtime_result_from_durable,
    _uses_validated_result,
)
from dataclasses import (
    replace,
)
from purra.contracts import (
    AgentRunRequest,
    AgentRunResult,
    MessageRole,
)
from purra.engine.canonical_sink import BufferedEventSink as _BufferedEventSink
from purra.engine.durable_execution import (
    DurableExecutionCompletion,
)
from purra.engine.options import (
    AgentCoreRunOptions,
    DurableTaskContinuation,
)
from purra.engine.task_orchestration import (
    TaskOrchestrationCapability,
)
from purra.errors import (
    ContractViolationError,
)
from purra.events import (
    AgentEvent,
)
from purra.evidence import (
    RunEvidenceStore,
)
from purra.json_values import (
    thaw_json_mapping,
)
from purra.model_execution import (
    AgentModelTaskRunner,
)
from purra.ports import (
    CancellationSignal,
)
from purra.run_controller import (
    AgentRunController,
)
from typing import (
    AsyncIterator,
)
import json


async def _resume_checkpointed_run(
    core,
    request: AgentRunRequest,
    options: AgentCoreRunOptions,
    controller: AgentRunController,
    sink: _BufferedEventSink,
    model_tasks: AgentModelTaskRunner,
    output_budget,
    signal: CancellationSignal | None,
) -> AsyncIterator[AgentEvent | AgentRunResult]:
    checkpoint = options.agent_execution_checkpoint
    if core._run_tree_repository is not None:
        checkpoint = await core._resume_child_runs(
            checkpoint,
            options,
            controller,
            signal,
        )
        options = replace(options, agent_execution_checkpoint=checkpoint)
    runtime_result, runtime_events = await core._resume_runtime(
        request=request,
        options=options,
        controller=controller,
        sink=sink,
        model_tasks=model_tasks,
        output_budget=output_budget,
        signal=signal,
    )
    for event in runtime_events:
        yield event
    await core._settle_runtime_result(
        request=request,
        options=options,
        controller=controller,
        result=runtime_result,
        output_budget=output_budget,
        signal=signal,
    )
    for event in sink.drain():
        yield event
    yield _run_result(
        controller,
        model=(
            runtime_result.model
            if runtime_result is not None
            else request.model.model
        ),
    )


async def _resume_child_runs(core, checkpoint, options, controller, signal):
    calls = {call.id for message in checkpoint.messages for call in message.tool_calls if call.name in {"delegateToAgents", "receiveAgentResults", "continueAgent"}}
    messages = list(checkpoint.messages)
    evidence = RunEvidenceStore.from_checkpoint_mapping(checkpoint.evidence_state)
    required_failure = False
    for index, message in enumerate(messages):
        if message.role is not MessageRole.TOOL or message.tool_call_id not in calls:
            continue
        try: payload = json.loads(message.content)
        except (ValueError, TypeError): continue
        if not isinstance(payload, dict) or payload.get("state") != "pending" or not payload.get("pendingRunIds"):
            continue
        aggregate = await core.join_agent_runs(checkpoint.run_id, tuple(payload["pendingRunIds"]), signal,
            lease_owner_id=options.agent_tree_lease_owner_id, lease_epoch=options.agent_tree_lease_epoch)
        if aggregate.pending_run_ids:
            continue
        failures = sorted(set(payload["requiredFailures"]) | set(aggregate.required_failures))
        required_failure = required_failure or bool(failures)
        messages[index] = replace(message, content=json.dumps({
            **({"runIds": payload["runIds"]} if "runIds" in payload else {}),
            "state": "blocked" if failures else "ready", "pendingRunIds": [], "requiredFailures": failures,
            "results": [*payload["results"], *(thaw_json_mapping(row) for row in aggregate.results)],
        }, ensure_ascii=False, separators=(",", ":")))
        evidence.resolve_child_runs(
            message.tool_call_id,
            messages[index].content,
        )
    if tuple(messages) != checkpoint.messages:
        checkpoint = replace(checkpoint, messages=tuple(messages), input_revision=checkpoint.input_revision + 1,
                             evidence_state=evidence.checkpoint_mapping())
        await controller.save_execution_checkpoint(checkpoint)
    if required_failure:
        raise ContractViolationError(
            "A required Child Run failed",
            code="required_child_run_failed",
        )
    return checkpoint


async def _continue_durable_run(
    core,
    *,
    request: AgentRunRequest,
    options: AgentCoreRunOptions,
    controller: AgentRunController,
    sink: _BufferedEventSink,
    continuation: DurableTaskContinuation,
    output_budget,
    signal: CancellationSignal | None,
) -> AsyncIterator[AgentEvent | AgentRunResult]:
    orchestration = core._task_orchestration or TaskOrchestrationCapability(
        None,
        None,
    )
    updates = orchestration.continue_durable(
        controller,
        request,
        continuation,
        sink,
        signal,
        defer_successful_completion=_uses_validated_result(options),
    )
    async for update in core._settle_durable_updates(
        updates,
        request=request,
        options=options,
        controller=controller,
        sink=sink,
        output_budget=output_budget,
        signal=signal,
    ):
        yield update


async def _settle_durable_updates(
    core,
    updates: AsyncIterator[AgentEvent | DurableExecutionCompletion],
    *,
    request: AgentRunRequest,
    options: AgentCoreRunOptions,
    controller: AgentRunController,
    sink: _BufferedEventSink,
    output_budget,
    signal: CancellationSignal | None,
) -> AsyncIterator[AgentEvent | AgentRunResult]:
    durable_result = None
    async for update in updates:
        if isinstance(update, DurableExecutionCompletion):
            durable_result = _runtime_result_from_durable(
                update,
                run_id=controller.run_id,
                model=request.model.model,
            )
        else:
            yield update
    if durable_result is not None:
        await core._settle_runtime_result(
            request=request,
            options=options,
            controller=controller,
            result=durable_result,
            output_budget=output_budget,
            signal=signal,
        )
        for event in sink.drain():
            yield event
    yield _run_result(controller)
