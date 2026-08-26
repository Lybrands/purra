"""Run persistence adapter that makes canonical lifecycle output authoritative."""

from __future__ import annotations

from datetime import datetime, timezone

from purra.contracts import RunCreateParams, RunId, RunStatus
from purra.events import AgentEvent, CoreEventType
from purra.output.contracts import RunLifecycleOutputDraft
from purra.output.processor import AgentOutputProcessor
from purra.ports import RunBeginResult, RunCommit, RunRepository


class CanonicalRunRepository:
    """Commit controller-owned lifecycle state through OutputProcessor."""

    def __init__(
        self,
        repository: RunRepository,
        output: AgentOutputProcessor,
    ) -> None:
        if not isinstance(repository, RunRepository):
            raise TypeError("canonical run repository requires a RunRepository")
        if not isinstance(output, AgentOutputProcessor):
            raise TypeError("canonical run repository requires OutputProcessor")
        self._repository = repository
        self._output = output

    async def begin(
        self,
        params: RunCreateParams,
        started_event: AgentEvent,
    ) -> RunBeginResult:
        return await self._output.begin_run_lifecycle(params, started_event)

    async def get(self, run_id: RunId):
        return await self._repository.get(run_id)

    async def commit(
        self,
        run_id: RunId,
        commit: RunCommit,
    ) -> tuple[AgentEvent, ...]:
        if commit.terminal_status is None:
            return await self._repository.commit(run_id, commit)
        status = RunStatus(commit.terminal_status)
        terminal_type = {
            RunStatus.DONE: CoreEventType.RUN_COMPLETED,
            RunStatus.BLOCKED: CoreEventType.RUN_BLOCKED,
            RunStatus.FAILED: CoreEventType.RUN_FAILED,
            RunStatus.CANCELED: CoreEventType.RUN_CANCELED,
        }[status]
        terminal_event = next(
            event
            for event in commit.events
            if event.type == terminal_type
        )
        await self._output.accept_run_lifecycle_event(
            commit,
            RunLifecycleOutputDraft(
                source_event_key=f"run:{run_id}:{status.value}",
                status=status,
                payload=terminal_event.payload,
                occurred_at=datetime.now(timezone.utc),
            ),
        )
        # The terminal commit's public structured events were journaled in the
        # same transaction. Do not feed them through the live sink a second time.
        return ()

    async def bind_conversation(self, run_id, conversation_id):
        return await self._repository.bind_conversation(run_id, conversation_id)

    async def append_event(self, run_id, event):
        return await self._repository.append_event(run_id, event)

    async def append_trace(self, run_id, trace):
        return await self._repository.append_trace(run_id, trace)

    async def reserve_model_attempt(self, run_id, invocation_id):
        return await self._repository.reserve_model_attempt(run_id, invocation_id)

    async def settle_model_attempt(self, run_id, invocation_id, usage):
        return await self._repository.settle_model_attempt(
            run_id,
            invocation_id,
            usage,
        )


__all__ = ["CanonicalRunRepository"]
