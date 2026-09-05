"""Persistence and durable coordination ports."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from purra.contracts import RunExecutionLease, RunId
from purra.run_recovery import RunRecoverySnapshot
from purra.run_control import (
    OrphanRunCandidate,
    RunActivitySnapshot,
    RunCancellationReceipt,
)
from purra.ports.run_lifecycle import (
    CONTROLLER_OWNED_RUN_EVENT_TYPES,
    TERMINAL_RUN_EVENT_TYPES,
    RunBeginResult,
    RunCommit,
    RunRepository,
    validate_run_commit_lifecycle,
)
from purra.ports.projection import DomainEventProjector


@runtime_checkable
class ExecutionLeaseStore(Protocol):
    async def claim(
        self,
        run_id: RunId,
        owner_id: str,
        *,
        lease_duration_ms: int,
    ) -> bool: ...

    async def renew(
        self,
        run_id: RunId,
        owner_id: str,
        *,
        lease_duration_ms: int,
    ) -> bool: ...

    async def release(self, run_id: RunId, owner_id: str) -> bool: ...

    async def request_cancellation(self, run_id: RunId) -> bool: ...

    async def get(self, run_id: RunId) -> RunExecutionLease | None: ...


@runtime_checkable
class RunRecoveryStore(Protocol):
    async def load(
        self,
        run_id: RunId,
        *,
        after_event_id: int = 0,
        limit: int = 100,
    ) -> RunRecoverySnapshot | None: ...


@runtime_checkable
class RunControlStore(ExecutionLeaseStore, Protocol):
    """Atomic persistence required to control Runs without a live handle."""

    async def claim_for_cancellation(
        self,
        run_id: RunId,
        owner_id: str,
        *,
        lease_duration_ms: int,
        timestamp_ms: int | None = None,
    ) -> bool: ...

    async def load_cancellation_receipt(
        self,
        run_id: RunId,
    ) -> RunCancellationReceipt | None: ...

    async def fence_cancellation(
        self,
        run_id: RunId,
    ) -> RunCancellationReceipt: ...

    async def complete_cancellation(
        self,
        run_id: RunId,
        *,
        terminalized: bool,
    ) -> RunCancellationReceipt: ...

    async def list_orphans(
        self,
        *,
        timestamp_ms: int | None = None,
        after_restart: bool = False,
    ) -> tuple[OrphanRunCandidate, ...]: ...

    async def claim_orphan(
        self,
        candidate: OrphanRunCandidate,
        owner_id: str,
        *,
        lease_duration_ms: int,
        timestamp_ms: int | None = None,
        after_restart: bool = False,
    ) -> bool: ...

    async def inspect_activity(
        self,
        *,
        run_ids: tuple[RunId, ...] = (),
        task_ids: tuple[str, ...] = (),
    ) -> RunActivitySnapshot: ...


__all__ = [name for name in globals() if not name.startswith("_")]
