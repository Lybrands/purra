"""Host-driven recovery scans over persisted Runs, without a second queue."""
import asyncio

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, Protocol


@dataclass(frozen=True, slots=True)
class RecoveryWorkerResult:
    run_id: str
    action: Literal["blocked", "settled", "failed"]
    reasons: tuple[str, ...] = ()


class RecoverySchedule(Protocol):
    async def ready(self, run_id: str) -> int | None: ...
    async def settle(self, run_id: str, revision: int, failed: bool) -> bool: ...


class RecoveryWorker:
    """Run one serial scan; the host supplies discovery and the public resume call.

    `resume` must await the Run handle's outcome, including approval waits. The
    host owns request/configuration reconstruction and schedules later scans.
    A clean inspection is only a hint: public resume must acquire ownership and
    revalidate all execution gates. No claim or approval is repaired here.
    """

    def __init__(
        self, *,
        discover: Callable[[], Awaitable[Sequence[str]]],
        inspect: Callable[[str], Awaitable[Mapping[str, Any]]],
        resume: Callable[[str], Awaitable[object]],
        schedule: RecoverySchedule | None = None,
        max_runs_per_scan: int | None = None,
    ) -> None:
        if max_runs_per_scan is not None and (type(max_runs_per_scan) is not int or not 0 < max_runs_per_scan <= 2147483647):
            raise ValueError("max_runs_per_scan must be a positive 32-bit integer")
        self._limit = max_runs_per_scan
        self._queue = []
        self._phase = "idle"
        self._last_scan = None
        self._discover = discover
        self._inspect = inspect
        self._resume = resume
        self._schedule = schedule
        self._active = False
        self._serving = False
        self._wake = asyncio.Event()

    def diagnostics(self):
        """Process-local observations, with no Run IDs or execution authority."""
        return {"schemaVersion": 1, "authority": "diagnosis_only",
                "phase": self._phase, "serving": self._serving,
                "maxRunsPerScan": self._limit,
                "lastScan": dict(self._last_scan) if self._last_scan is not None else None}

    def wake(self) -> None:
        """Notify on the worker event loop after committing a host state change."""
        self._wake.set()

    async def run(
        self, *, stop: asyncio.Event, poll_interval_ms: int = 1000,
        max_backoff_ms: int = 30000,
        on_scan: Callable[[tuple[RecoveryWorkerResult, ...]], Awaitable[None]] | None = None,
    ) -> None:
        """Scan until stop; drain the current callback without canceling its Run."""
        for value in (poll_interval_ms, max_backoff_ms):
            if type(value) is not int or not 0 < value <= 2147483647:
                raise ValueError("worker intervals must be positive 32-bit integers")
        if max_backoff_ms < poll_interval_ms:
            raise ValueError("worker backoff must cover polling interval")
        if self._serving or self._active:
            raise RuntimeError("recovery_worker_scan_active")
        self._serving = True
        delay = poll_interval_ms
        try:
            while not stop.is_set():
                self._wake.clear()
                report = await self._scan(stop.is_set)
                if on_scan is not None:
                    self._phase = "observing"
                    await on_scan(report)
                if stop.is_set():
                    break
                delay = min(max_backoff_ms, delay * 2) if any(r.action == "failed" for r in report) else poll_interval_ms
                self._phase = "waiting"
                wake_task = asyncio.create_task(self._wake.wait())
                stop_task = asyncio.create_task(stop.wait())
                try:
                    await asyncio.wait((wake_task, stop_task), timeout=delay / 1000,
                                       return_when=asyncio.FIRST_COMPLETED)
                finally:
                    for task in (wake_task, stop_task):
                        task.cancel()
                    await asyncio.gather(wake_task, stop_task, return_exceptions=True)
        finally:
            self._serving = False
            self._phase = "idle"

    async def run_once(self) -> tuple[RecoveryWorkerResult, ...]:
        if self._serving or self._active:
            raise RuntimeError("recovery_worker_scan_active")
        return await self._scan(lambda: False)

    async def _scan(self, stopped: Callable[[], bool]) -> tuple[RecoveryWorkerResult, ...]:
        self._active = True
        results = []
        candidates = visited = 0
        outcome = "interrupted"
        try:
            self._phase = "discovering"
            discovered = list(dict.fromkeys(await self._discover()))
            present = set(discovered)
            self._queue = [key for key in self._queue if key in present]
            queued = set(self._queue)
            self._queue.extend(key for key in discovered if key not in queued)
            candidates = len(self._queue)
            batch = self._queue[:self._limit] if self._limit is not None else list(self._queue)
            for run_id in batch:
                if stopped():
                    break
                visited += 1
                self._phase = "scheduling"
                revision = await self._schedule.ready(run_id) if self._schedule is not None else 0
                if revision is None:
                    results.append(RecoveryWorkerResult(run_id, "blocked", ("retry_not_due",)))
                    continue
                stage = "inspection_failed"
                try:
                    self._phase = "inspecting"
                    report = await self._inspect(run_id)
                    reasons = tuple(report["blockers"])
                    if reasons:
                        results.append(RecoveryWorkerResult(run_id, "blocked", reasons))
                    else:
                        if stopped():
                            break
                        stage = "resume_failed"
                        self._phase = "resuming"
                        await self._resume(run_id)
                        results.append(RecoveryWorkerResult(run_id, "settled"))
                except Exception:
                    # Exception text may contain host credentials or tool arguments.
                    results.append(RecoveryWorkerResult(run_id, "failed", (stage,)))
                if self._schedule is not None:
                    self._phase = "settling"
                    await self._schedule.settle(run_id, revision, results[-1].action == "failed")
            outcome = "stopped" if stopped() else "complete"
            return tuple(results)
        except Exception:
            outcome = "failed"
            raise
        finally:
            self._queue = self._queue[visited:] + self._queue[:visited]
            self._last_scan = {"outcome": outcome, "candidates": candidates, "visited": visited,
                               "deferred": candidates - visited,
                               **{action: sum(r.action == action for r in results) for action in ("blocked", "settled", "failed")}}
            self._active = False
            self._phase = "idle"
