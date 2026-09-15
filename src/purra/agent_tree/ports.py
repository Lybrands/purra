"""Storage port for recursive Agent Run trees."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol, runtime_checkable

from purra.agent_tree.contracts import (
    AgentNode,
    AgentRunAggregation,
    AgentTreeRun,
    BeginRootAgentCommand,
    ContextCheckpoint,
    ContinueAgentCommand,
    ContinueAgentReceipt,
    SpawnAgentsCommand,
    SpawnAgentsReceipt,
)

@runtime_checkable
class RunTreeRepository(Protocol):
    async def begin_root(self, command: BeginRootAgentCommand) -> AgentTreeRun: ...
    async def spawn_agents(self, command: SpawnAgentsCommand) -> SpawnAgentsReceipt: ...
    async def continue_agent(
        self,
        command: ContinueAgentCommand,
    ) -> ContinueAgentReceipt: ...
    async def claim_run(
        self,
        run_id: str,
        *,
        owner_id: str = "run-tree-supervisor",
        lease_duration_ms: int = 30_000,
    ) -> AgentTreeRun | None: ...
    async def renew_run_lease(
        self,
        run_id: str,
        *,
        owner_id: str,
        lease_epoch: int,
        lease_duration_ms: int,
    ) -> AgentTreeRun: ...
    async def require_run_claim(
        self,
        run_id: str,
        *,
        lease_owner_id: str | None = None,
        lease_epoch: int | None = None,
    ) -> None: ...
    async def mark_waiting(
        self,
        run_id: str,
        *,
        lease_owner_id: str | None = None,
        lease_epoch: int | None = None,
    ) -> AgentTreeRun: ...
    async def release_waiting(
        self,
        run_id: str,
        *,
        lease_owner_id: str | None = None,
        lease_epoch: int | None = None,
    ) -> AgentTreeRun: ...
    async def suspend_run(self, run_id: str, *, lease_owner_id: str, lease_epoch: int) -> AgentTreeRun: ...
    async def complete_run(
        self,
        run_id: str,
        *,
        expected_context_version: int,
        result: Any,
        content_ref: str,
        fingerprint: str,
        lease_owner_id: str | None = None,
        lease_epoch: int | None = None,
    ) -> AgentTreeRun: ...
    async def fail_run(
        self,
        run_id: str,
        error_code: str,
        *,
        lease_owner_id: str | None = None,
        lease_epoch: int | None = None,
    ) -> AgentTreeRun: ...
    async def cancel_subtree(self, run_id: str) -> tuple[str, ...]: ...
    async def aggregate_runs(
        self,
        requester_run_id: str,
        run_ids: Sequence[str],
    ) -> AgentRunAggregation: ...
    async def close_agent(self, agent_id: str) -> AgentNode: ...
    async def get_agent(self, agent_id: str) -> AgentNode: ...
    async def get_run(self, run_id: str) -> AgentTreeRun: ...
    async def get_checkpoint(self, checkpoint_id: str) -> ContextCheckpoint: ...
    async def list_runnable(self, root_run_id: str) -> tuple[AgentTreeRun, ...]: ...
    async def list_descendants(self, run_id: str) -> tuple[AgentTreeRun, ...]: ...
    async def list_agent_descendants(self, agent_id: str, *, after: str | None = None, limit: int = 21) -> tuple[AgentNode, ...]: ...
