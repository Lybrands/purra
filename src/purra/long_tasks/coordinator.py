"""Generic coordinator for checkpointed long-task execution units."""

from __future__ import annotations

import asyncio

from purra.cancellation import ExecutionStopSignal, stop_reason
from purra.errors import ContractViolationError
from purra.long_tasks.contracts import LongTaskStatus
from purra.long_tasks.ports import LongTaskRepository, LongTaskUnitRunner
from purra.ports import CancellationSignal
from purra.recovery import (
    FailureCategory,
    FailureDisposition,
    FailureSignal,
    decide_failure,
)


class LongTaskCoordinator:
    def __init__(
        self,
        repository: LongTaskRepository,
        *,
        worker_id: str,
        lease_duration_ms: int = 300_000,
        retry_backoff_ms: tuple[int, ...] = (),
        idle_poll_ms: int = 100,
    ) -> None:
        self._repository = repository
        self._worker_id = str(worker_id or "").strip()
        if not self._worker_id:
            raise ValueError("long task coordinator worker_id is required")
        self._lease_duration_ms = int(lease_duration_ms)
        if self._lease_duration_ms <= 0:
            raise ValueError("long task coordinator lease must be positive")
        self._retry_backoff_ms = tuple(
            max(0, int(value)) for value in retry_backoff_ms
        )
        self._idle_poll_ms = max(1, int(idle_poll_ms))

    async def run(
        self,
        task_id: str,
        runner: LongTaskUnitRunner,
        signal: CancellationSignal | None = None,
    ):
        task = await self._require(task_id)
        task_stop = ExecutionStopSignal(
            signal,
            deadline_at_ms=task.deadline_at_ms,
            deadline_code="long_task_deadline_exceeded",
        )
        signal = task_stop
        if task.cancellation_requested_at_ms is not None:
            try:
                return await self._repository.cancel(task.id)
            finally:
                task_stop.close()
        if task.status is LongTaskStatus.PENDING:
            task = await self._repository.start(
                task.id,
                expected_revision=task.revision,
            )
        active: dict[asyncio.Task, object] = {}
        try:
            while task.status is LongTaskStatus.RUNNING:
                if signal is not None and signal.is_set():
                    if stop_reason(signal) == "long_task_deadline_exceeded":
                        await self._cancel_active(active)
                        return await self._repository.expire_deadline(task.id)
                    return await self._stop_active(task.id, active)

                task = await self._require(task.id)
                if task.cancellation_requested_at_ms is not None:
                    return await self._stop_active(task.id, active)
                if task.status is not LongTaskStatus.RUNNING:
                    break

                while len(active) < task.max_parallelism:
                    unit = await self._repository.claim_ready_unit(
                        task.id,
                        worker_id=self._worker_id,
                        lease_duration_ms=self._lease_duration_ms,
                    )
                    if unit is None:
                        break
                    execution = asyncio.create_task(
                        self._run_claimed_unit(task, unit, runner, signal)
                    )
                    active[execution] = unit

                if not active:
                    task = await self._repository.finalize_if_complete(task.id)
                    if task.status is LongTaskStatus.RUNNING:
                        await self._wait_for_idle_work(signal)
                        continue
                    return task

                done, _ = await asyncio.wait(
                    tuple(active),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for execution in done:
                    active.pop(execution, None)
                    # _run_claimed_unit owns persistence and normalizes races;
                    # surfacing an unexpected infrastructure exception here is
                    # safer than silently abandoning the remaining branches.
                    await execution
                task = await self._require(task.id)
                if task.status is not LongTaskStatus.RUNNING and active:
                    await self._cancel_active(active)
            return task
        except asyncio.CancelledError:
            return await self._stop_active(task.id, active)
        finally:
            task_stop.close()
            if active:
                await self._cancel_active(active)

    async def _run_claimed_unit(self, task, unit, runner, signal):
        try:
            local_stop = asyncio.Event()
            combined_signal = _CombinedCancellationSignal(signal, local_stop)
            result = await self._run_with_heartbeat(
                task,
                unit,
                runner,
                combined_signal,
                local_stop,
            )
            try:
                settled = await self._repository.complete_unit(
                    task.id,
                    unit.id,
                    worker_id=self._worker_id,
                    lease_epoch=unit.lease_epoch,
                    result=result,
                )
            except ContractViolationError as error:
                if _is_lease_lost(error):
                    return await self._require(task.id)
                raise
            await self._notify_settled(runner, task.id)
            return settled
        except asyncio.CancelledError:
            return await self._checkpoint_interrupted(
                task.id,
                unit.id,
                unit.lease_epoch,
            )
        except ContractViolationError as error:
            if _is_lease_lost(error):
                return await self._require(task.id)
            if error.code == "long_task_deadline_exceeded":
                return await self._require(task.id)
            raise
        except Exception as error:
            current = await self._require(task.id)
            if current.status is not LongTaskStatus.RUNNING:
                return current
            if signal is not None and signal.is_set():
                return await self._checkpoint_interrupted(
                    task.id,
                    unit.id,
                    unit.lease_epoch,
                )
            classifier = getattr(runner, "classify_unit_failure", None)
            failure = None
            if callable(classifier):
                try:
                    candidate = classifier(task, unit, error)
                    if isinstance(candidate, FailureSignal):
                        failure = candidate
                except Exception:
                    failure = None
            if failure is None:
                failure = FailureSignal(
                    category=FailureCategory.BUSINESS_INVARIANT,
                    code=_error_code(error),
                    retryable=False,
                )
            decision = decide_failure(
                failure,
                attempts_remaining=max(0, unit.max_attempts - unit.attempt),
            )
            if decision.disposition is FailureDisposition.SPLIT_PART:
                splitter = getattr(runner, "split_unit", None)
                split = None
                if callable(splitter):
                    try:
                        split = splitter(task, unit, error)
                    except Exception:
                        split = None
                if split is not None and split.children:
                    settled = await self._repository.expand_unit(
                        task.id,
                        unit.id,
                        worker_id=self._worker_id,
                        lease_epoch=unit.lease_epoch,
                        split=split,
                        decision=decision,
                    )
                    await self._notify_settled(runner, task.id)
                    return settled
                decision = decide_failure(
                    FailureSignal(
                        category=FailureCategory.PROTOCOL_INCOMPATIBLE,
                        code="model_task_mode_incompatible",
                        retryable=False,
                        scope=failure.scope,
                    ),
                    attempts_remaining=0,
                )
            try:
                settled = await self._repository.settle_unit_failure(
                    task.id,
                    unit.id,
                    worker_id=self._worker_id,
                    lease_epoch=unit.lease_epoch,
                    decision=decision,
                )
            except ContractViolationError as lease_error:
                if _is_lease_lost(lease_error):
                    return await self._require(task.id)
                if lease_error.code == "long_task_deadline_exceeded":
                    return await self._require(task.id)
                raise
            await self._notify_settled(runner, task.id)
            return settled

    async def _run_with_heartbeat(
        self,
        task,
        unit,
        runner,
        signal,
        local_stop: asyncio.Event,
    ):
        execution = asyncio.create_task(
            self._execute_claimed_unit(task, unit, runner, signal)
        )
        heartbeat = asyncio.create_task(self._heartbeat(task.id, unit))
        try:
            done, _ = await asyncio.wait(
                {execution, heartbeat},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if heartbeat in done:
                error = heartbeat.exception()
                local_stop.set()
                if not execution.done():
                    execution.cancel()
                await asyncio.gather(execution, return_exceptions=True)
                if error is None:
                    raise RuntimeError("long task heartbeat stopped unexpectedly")
                raise error
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            return await execution
        finally:
            if not heartbeat.done():
                heartbeat.cancel()
            if not execution.done():
                execution.cancel()
            await asyncio.gather(heartbeat, execution, return_exceptions=True)

    async def _execute_claimed_unit(self, task, unit, runner, signal):
        if unit.attempt > 1 and unit.error_code:
            await self._wait_before_retry(unit.attempt - 1, signal)
        if signal.is_set():
            raise asyncio.CancelledError
        return await runner.run_unit(task, unit, signal)

    async def _heartbeat(self, task_id: str, unit) -> None:
        interval = max(1, self._lease_duration_ms // 3) / 1000
        while True:
            await asyncio.sleep(interval)
            await self._repository.renew_unit_lease(
                task_id,
                unit.id,
                worker_id=self._worker_id,
                lease_epoch=unit.lease_epoch,
                lease_duration_ms=self._lease_duration_ms,
            )

    async def _checkpoint_interrupted(
        self,
        task_id: str,
        unit_id: str,
        lease_epoch: int,
    ):
        current = await self._require(task_id)
        if current.status is not LongTaskStatus.RUNNING:
            return current
        try:
            return await self._repository.interrupt_unit(
                task_id,
                unit_id,
                worker_id=self._worker_id,
                lease_epoch=lease_epoch,
                reason_code="execution_interrupted",
            )
        except ContractViolationError as error:
            if _is_lease_lost(error):
                return current
            if error.code == "long_task_deadline_exceeded":
                return await self._require(task_id)
            raise

    async def _notify_settled(self, runner, task_id: str) -> None:
        callback = getattr(runner, "on_unit_settled", None)
        if callable(callback):
            await callback(task_id)

    async def _stop_active(self, task_id: str, active: dict):
        await self._cancel_active(active)
        current = await self._require(task_id)
        if current.status in {
            LongTaskStatus.PAUSED,
            LongTaskStatus.CANCELED,
        }:
            return current
        if current.status is LongTaskStatus.RUNNING:
            if current.cancellation_requested_at_ms is not None:
                return await self._repository.cancel(task_id)
            return await self._repository.pause(task_id)
        return current

    @staticmethod
    async def _cancel_active(active: dict) -> None:
        pending = tuple(active)
        active.clear()
        for execution in pending:
            if not execution.done():
                execution.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    async def _wait_before_retry(self, attempt: int, signal) -> None:
        if not self._retry_backoff_ms:
            return
        index = min(max(0, int(attempt) - 1), len(self._retry_backoff_ms) - 1)
        delay_ms = self._retry_backoff_ms[index]
        if delay_ms <= 0:
            return
        if signal is None:
            await asyncio.sleep(delay_ms / 1000)
            return
        try:
            await asyncio.wait_for(signal.wait(), timeout=delay_ms / 1000)
        except TimeoutError:
            return

    async def _wait_for_idle_work(self, signal) -> None:
        delay = self._idle_poll_ms / 1000
        if signal is None:
            await asyncio.sleep(delay)
            return
        try:
            await asyncio.wait_for(signal.wait(), timeout=delay)
        except TimeoutError:
            return

    async def _require(self, task_id: str):
        task = await self._repository.load(str(task_id or "").strip())
        if task is None:
            raise LookupError("long task does not exist")
        return task


def _error_code(error: Exception) -> str:
    code = str(getattr(error, "code", "") or "").strip()
    return (code or str(error) or type(error).__name__)[:240]


class _CombinedCancellationSignal:
    def __init__(self, parent, local: asyncio.Event) -> None:
        self._parent = parent
        self._local = local

    def is_set(self) -> bool:
        return self._local.is_set() or bool(
            self._parent is not None and self._parent.is_set()
        )

    async def wait(self) -> bool:
        if self.is_set():
            return True
        local_waiter = asyncio.create_task(self._local.wait())
        if self._parent is None:
            return await local_waiter
        parent_waiter = asyncio.create_task(self._parent.wait())
        try:
            done, _ = await asyncio.wait(
                {local_waiter, parent_waiter},
                return_when=asyncio.FIRST_COMPLETED,
            )
            return any(bool(waiter.result()) for waiter in done)
        finally:
            for waiter in (local_waiter, parent_waiter):
                if not waiter.done():
                    waiter.cancel()
            await asyncio.gather(
                local_waiter,
                parent_waiter,
                return_exceptions=True,
            )


def _is_lease_lost(error: BaseException) -> bool:
    return getattr(error, "code", None) == "long_task_unit_lease_lost"


__all__ = ["LongTaskCoordinator"]
