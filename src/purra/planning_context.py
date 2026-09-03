"""Core-owned planning scope, available to custom Planners without wrappers."""

from __future__ import annotations

from typing import Any
from purra.ports import CancellationSignal

from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from time import perf_counter

from purra.cancellation import OperationCanceled, raise_if_stopped
from purra.operations import AgentOperationController, OperationDisplay, OperationKind, OperationScope
from purra.planning_stream import PlanningScope


@dataclass(slots=True)
class PlanningContext:
    scope: PlanningScope
    # Core binds AgentModelInvocationManager; keep this context carrier below it
    # in the dependency graph instead of importing its implementation here.
    model_manager: Any
    signal: CancellationSignal | None
    model_attempts: int = 0
    validation_started: float | None = None
    error_code: str | None = None

    def fail(self, code: str) -> None:
        self.error_code = code


_CURRENT: ContextVar[PlanningContext | None] = ContextVar("purra_planning_context", default=None)


def current_planning_context() -> PlanningContext | None:
    """None outside Core planning. Never share a captured context across Runs."""
    return _CURRENT.get()


@asynccontextmanager
async def planning_operation(*, controller, operations, model_manager, revision, signal):
    import asyncio

    class ControllerSink:
        async def accept_operation_event(self, event):
            await controller.record_event(
                "operation.started" if hasattr(event, "started_at") else "operation.finished",
                {"operationId": event.operation_id, "revision": revision,
                 "display": dict(event.display),
                 **({"kind": "planning", "startedAt": event.started_at.isoformat()}
                    if hasattr(event, "started_at") else
                    {"status": event.status.value, "finishedAt": event.finished_at.isoformat(),
                     "durationMs": event.duration_ms, "errorCode": event.error_code})},
            )

    operations = operations or AgentOperationController(ControllerSink())
    raise_if_stopped(signal)
    receipt = await operations.start(OperationKind.PLANNING, OperationScope(
        run_id=controller.run_id,
        display=OperationDisplay(label_key="agent.operation.planning", label_params={"revision": revision}),
    ))
    context = PlanningContext(PlanningScope(controller.run_id, receipt.operation_id, revision), model_manager, signal)
    token = _CURRENT.set(context)
    error = None
    try:
        yield context
        raise_if_stopped(signal)
    except BaseException as caught:
        error = caught
        raise
    finally:
        _CURRENT.reset(token)
        display = OperationDisplay(label_key="agent.operation.planning", label_params={
            "revision": revision, "modelAttempts": context.model_attempts,
            "repairCount": max(0, context.model_attempts - 1),
            "validationMs": (None if context.validation_started is None else
                             max(0, round((perf_counter() - context.validation_started) * 1000))),
        })
        code = context.error_code or getattr(error, "code", None) or "planning_failed"
        try:
            if isinstance(error, (OperationCanceled, asyncio.CancelledError)):
                await operations.cancel(receipt.operation_id, "request_canceled", display)
            elif error is not None or context.error_code:
                await operations.fail(receipt.operation_id, code, display)
            else:
                await operations.succeed(receipt.operation_id, display)
        except BaseException:
            if error is None:
                raise


__all__ = ["PlanningContext", "current_planning_context"]
