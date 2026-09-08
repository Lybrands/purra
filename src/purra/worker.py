"""Host-driven recovery scans over persisted Runs, without a second queue."""
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

    async def run_once(self) -> tuple[RecoveryWorkerResult, ...]:
        if self._active:
            raise RuntimeError("recovery_worker_scan_active")
        self._active = True
        try:
            results = []
            for run_id in dict.fromkeys(await self._discover()):
                stage = "inspection_failed"
                try:
                    report = await self._inspect(run_id)
                    reasons = tuple(report["blockers"])
                    if reasons:
                        results.append(RecoveryWorkerResult(run_id, "blocked", reasons))
                        continue
                    stage = "resume_failed"
                    await self._resume(run_id)
                    results.append(RecoveryWorkerResult(run_id, "settled"))
                except Exception:
                    # Exception text may contain host credentials or tool arguments.
                    results.append(RecoveryWorkerResult(run_id, "failed", (stage,)))
            return tuple(results)
        finally:
            self._active = False
