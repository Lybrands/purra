"""One-Run delegation coordination for isolated Agent executions."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol, runtime_checkable
from uuid import uuid4

from purra.cancellation import OperationCanceled, await_with_cancellation
from purra.contracts import (
    AgentDelegation,
    DelegationContextMode,
    DelegationAggregation,
    RunId,
    RuntimeOutcome,
)
from purra.json_values import freeze_json_mapping
from purra.normalization import required_text
from purra.operations import (
    AgentOperationController,
    OperationDisplay,
    OperationKind,
    OperationScope,
)
from purra.output import DelegationOutputEvent
from purra.output.processor import AgentOutputProcessor
from purra.ports import CancellationSignal, DelegationRepository


@dataclass(frozen=True, slots=True)
class DelegatedAgentRequest:
    """One Agent execution inside an existing Run."""

    run_id: RunId
    batch_id: str
    delegation_id: str
    agent_name: str
    agent_title: str
    agent_instruction: str
    objective: str
    input_payload: Mapping[str, Any] = field(default_factory=dict)
    context_mode: DelegationContextMode = DelegationContextMode.ISOLATED

    def __post_init__(self) -> None:
        for name, label in (
            ("run_id", "delegated execution run id"),
            ("batch_id", "delegation batch id"),
            ("delegation_id", "delegation id"),
            ("agent_name", "delegated Agent name"),
            ("agent_title", "delegated Agent title"),
            ("agent_instruction", "delegated Agent instruction"),
            ("objective", "delegation objective"),
        ):
            object.__setattr__(self, name, required_text(getattr(self, name), label))
        object.__setattr__(
            self,
            "input_payload",
            freeze_json_mapping(self.input_payload),
        )
        object.__setattr__(
            self,
            "context_mode",
            DelegationContextMode(self.context_mode),
        )


@dataclass(frozen=True, slots=True)
class DelegatedAgentResult:
    """Terminal result of a delegated execution, not a second Run result."""

    outcome: RuntimeOutcome
    content: str = ""
    error_code: str | None = None

    def __post_init__(self) -> None:
        outcome = RuntimeOutcome(self.outcome)
        error_code = str(self.error_code or "").strip() or None
        if outcome is RuntimeOutcome.COMPLETED and error_code is not None:
            raise ValueError("completed delegated execution cannot carry an error")
        if outcome is not RuntimeOutcome.COMPLETED and error_code is None:
            raise ValueError("unfinished delegated execution requires an error code")
        object.__setattr__(self, "outcome", outcome)
        object.__setattr__(self, "content", str(self.content or ""))
        object.__setattr__(self, "error_code", error_code)


@runtime_checkable
class DelegatedAgentExecutor(Protocol):
    async def execute(
        self,
        request: DelegatedAgentRequest,
        signal: CancellationSignal | None = None,
    ) -> DelegatedAgentResult: ...


class DelegationCoordinator:
    """Own one-shot delegated executions inside their existing Run."""

    def __init__(
        self,
        *,
        repository: DelegationRepository,
        executor: DelegatedAgentExecutor,
        output_processor: AgentOutputProcessor,
        operation_controller: AgentOperationController,
        max_parallel: int = 3,
    ) -> None:
        for method in (
            "create",
            "start",
            "complete",
            "fail",
            "cancel",
            "cancel_batch",
            "aggregate_batch",
        ):
            if not callable(getattr(repository, method, None)):
                raise TypeError(
                    "delegation coordinator requires a repository with "
                    f"{method}()"
                )
        if not isinstance(executor, DelegatedAgentExecutor):
            raise TypeError("delegation coordinator requires an Agent executor")
        self._repository = repository
        self._executor = executor
        self._output = output_processor
        self._operations = operation_controller
        self._capacity = asyncio.Semaphore(max(1, int(max_parallel)))
        self._active: dict[tuple[RunId, str], set[asyncio.Task[object]]] = {}

    async def create(
        self,
        *,
        run_id: RunId,
        batch_id: str,
        agent_name: str,
        agent_title: str,
        agent_instruction: str,
        objective: str,
        input_payload: Mapping[str, Any] | None = None,
        context_mode: DelegationContextMode = DelegationContextMode.ISOLATED,
        required: bool = True,
        priority: int = 0,
    ) -> AgentDelegation:
        delegation = await self._repository.create(
            run_id=run_id,
            batch_id=batch_id,
            agent_name=agent_name,
            agent_title=agent_title,
            agent_instruction=agent_instruction,
            objective=objective,
            input_payload=input_payload,
            context_mode=DelegationContextMode(context_mode),
            required=required,
            priority=priority,
        )
        await self._emit_status(delegation, "queued")
        return delegation

    async def execute_batch(
        self,
        delegations: Sequence[AgentDelegation],
        signal: CancellationSignal | None = None,
    ) -> tuple[DelegatedAgentResult, ...]:
        items = tuple(delegations)
        if not items:
            return ()
        run_id = items[0].run_id
        batch_id = items[0].batch_id
        if any(
            item.run_id != run_id or item.batch_id != batch_id
            for item in items
        ):
            raise ValueError("delegation batch must belong to one Run")

        tasks = tuple(
            asyncio.create_task(self._execute_one(item, signal))
            for item in items
        )
        key = (run_id, batch_id)
        active = self._active.setdefault(key, set())
        active.update(tasks)
        try:
            return tuple(await asyncio.gather(*tasks))
        finally:
            active.difference_update(tasks)
            if not active:
                self._active.pop(key, None)

    async def cancel_batch(self, run_id: RunId, batch_id: str) -> int:
        key = (
            required_text(run_id, "delegation Run id"),
            required_text(batch_id, "delegation batch id"),
        )
        canceled = await self._repository.cancel_batch(*key)
        tasks = tuple(self._active.get(key, ()))
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        return canceled

    async def aggregate_batch(
        self,
        run_id: RunId,
        batch_id: str,
    ) -> DelegationAggregation:
        return await self._repository.aggregate_batch(run_id, batch_id)

    async def close(self) -> None:
        tasks = tuple(
            task
            for active in self._active.values()
            for task in active
        )
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._active.clear()

    async def _execute_one(
        self,
        delegation: AgentDelegation,
        signal: CancellationSignal | None,
    ) -> DelegatedAgentResult:
        operation_id: str | None = None
        try:
            async with self._capacity:
                started = await self._repository.start(
                    delegation.id,
                    run_id=delegation.run_id,
                    batch_id=delegation.batch_id,
                )
                if started is None:
                    raise RuntimeError("delegation could not start")
                operation = await self._operations.start(
                    OperationKind.DELEGATION,
                    OperationScope(
                        run_id=delegation.run_id,
                        display=OperationDisplay(
                            label_key="agent.operation.delegation",
                            label_params={
                                "agentName": delegation.agent_name,
                                "delegationId": delegation.id,
                            },
                        ),
                    ),
                )
                operation_id = operation.operation_id
                await self._emit_status(started, "running")
                result = await await_with_cancellation(
                    self._executor.execute(
                        DelegatedAgentRequest(
                            run_id=delegation.run_id,
                            batch_id=delegation.batch_id,
                            delegation_id=delegation.id,
                            agent_name=delegation.agent_name,
                            agent_title=delegation.agent_title,
                            agent_instruction=delegation.agent_instruction,
                            objective=delegation.objective,
                            input_payload=delegation.input_payload,
                            context_mode=delegation.context_mode,
                        ),
                        signal,
                    ),
                    signal,
                )
                if not isinstance(result, DelegatedAgentResult):
                    raise TypeError(
                        "delegated Agent executor returned an invalid result"
                    )
                await self._settle(delegation, result, operation_id)
                return result
        except OperationCanceled:
            await self._cancel(delegation, operation_id)
            return DelegatedAgentResult(
                outcome=RuntimeOutcome.CANCELED,
                error_code="delegation_canceled",
            )
        except asyncio.CancelledError:
            await self._cancel(delegation, operation_id)
            raise
        except Exception as error:
            code = str(getattr(error, "code", "") or type(error).__name__)
            await self._repository.fail(
                delegation.id,
                run_id=delegation.run_id,
                batch_id=delegation.batch_id,
                error=code,
            )
            if operation_id is not None:
                await self._operations.fail(operation_id, code)
            await self._emit_status(
                delegation,
                "failed",
                error_code=code,
            )
            return DelegatedAgentResult(
                outcome=RuntimeOutcome.FAILED,
                error_code=code,
            )

    async def _cancel(
        self,
        delegation: AgentDelegation,
        operation_id: str | None,
    ) -> None:
        await self._repository.cancel(
            delegation.id,
            run_id=delegation.run_id,
            batch_id=delegation.batch_id,
            reason="delegation_canceled",
        )
        if operation_id is not None:
            await self._operations.cancel(
                operation_id,
                "delegation_canceled",
            )
        await self._emit_status(
            delegation,
            "canceled",
            error_code="delegation_canceled",
        )

    async def _settle(
        self,
        delegation: AgentDelegation,
        result: DelegatedAgentResult,
        operation_id: str,
    ) -> None:
        if result.outcome is RuntimeOutcome.COMPLETED:
            settled = await self._repository.complete(
                delegation.id,
                run_id=delegation.run_id,
                batch_id=delegation.batch_id,
                result_summary=result.content,
            )
            if not settled:
                raise RuntimeError("delegation result could not be persisted")
            await self._operations.succeed(operation_id)
            await self._emit_status(delegation, "done")
            return
        if result.outcome is RuntimeOutcome.CANCELED:
            await self._repository.cancel(
                delegation.id,
                run_id=delegation.run_id,
                batch_id=delegation.batch_id,
                reason=result.error_code or "delegation_canceled",
            )
            await self._operations.cancel(
                operation_id,
                result.error_code or "delegation_canceled",
            )
            await self._emit_status(
                delegation,
                "canceled",
                error_code=result.error_code,
            )
            return
        await self._repository.fail(
            delegation.id,
            run_id=delegation.run_id,
            batch_id=delegation.batch_id,
            error=result.error_code or "delegation_failed",
        )
        await self._operations.fail(
            operation_id,
            result.error_code or "delegation_failed",
        )
        await self._emit_status(
            delegation,
            "failed",
            error_code=result.error_code,
        )

    async def _emit_status(
        self,
        delegation: AgentDelegation,
        status: str,
        *,
        error_code: str | None = None,
    ) -> None:
        await self._output.accept_delegation_event(DelegationOutputEvent(
            event_id=f"delegation-{uuid4().hex}",
            run_id=delegation.run_id,
            batch_id=delegation.batch_id,
            delegation_id=delegation.id,
            status=status,
            agent_name=delegation.agent_name,
            agent_title=delegation.agent_title,
            objective=delegation.objective,
            error_code=error_code,
            occurred_at=datetime.now(timezone.utc),
        ))


__all__ = [
    "DelegatedAgentExecutor",
    "DelegatedAgentRequest",
    "DelegatedAgentResult",
    "DelegationContextMode",
    "DelegationCoordinator",
]
