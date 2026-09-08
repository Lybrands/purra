"""Host-driven recovery scans over persisted Runs, without a second queue."""
import asyncio

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class RecoveryWorkerResult:
    run_id: str
    action: Literal["blocked", "settled", "failed"]
    reasons: tuple[str, ...] = ()


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
    ) -> None:
        self._discover = discover
        self._inspect = inspect
        self._resume = resume
        self._active = False
        self._serving = False
        self._wake = asyncio.Event()

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
                    await on_scan(report)
                if stop.is_set():
                    break
                delay = min(max_backoff_ms, delay * 2) if any(r.action == "failed" for r in report) else poll_interval_ms
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

    async def run_once(self) -> tuple[RecoveryWorkerResult, ...]:
        if self._serving or self._active:
            raise RuntimeError("recovery_worker_scan_active")
        return await self._scan(lambda: False)

    async def _scan(self, stopped: Callable[[], bool]) -> tuple[RecoveryWorkerResult, ...]:
        self._active = True
        try:
            results = []
            for run_id in dict.fromkeys(await self._discover()):
                if stopped():
                    break
                stage = "inspection_failed"
                try:
                    report = await self._inspect(run_id)
                    reasons = tuple(report["blockers"])
                    if reasons:
                        results.append(RecoveryWorkerResult(run_id, "blocked", reasons))
                        continue
                    if stopped():
                        break
                    stage = "resume_failed"
                    await self._resume(run_id)
                    results.append(RecoveryWorkerResult(run_id, "settled"))
                except Exception:
                    # Exception text may contain host credentials or tool arguments.
                    results.append(RecoveryWorkerResult(run_id, "failed", (stage,)))
            return tuple(results)
        finally:
            self._active = False
