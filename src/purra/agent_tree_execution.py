"""Command and execution boundary for recursive Agent Runs."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from typing import Any, Protocol, runtime_checkable
from uuid import uuid4

from purra.agent_tree import (
    AgentNode,
    AgentCapabilityGrant,
    AgentRunAggregation,
    AgentTreeRun,
    AgentTreeRunStatus,
    BeginRootAgentCommand,
    ContinueAgentCommand,
    ContinueAgentReceipt,
    ContextCheckpoint,
    RunTreeRepository,
    SpawnAgentsCommand,
    SpawnAgentsReceipt,
)
from purra.cancellation import OperationCanceled, await_with_cancellation, is_canceled
from purra.agent_tree.delivery import deliver_agent_results
from purra.agent_tree.query import AgentTreeQuery
from purra.agent_tree.receiver import AgentResultReceiver
from purra.agent_tree.lease import bind_agent_run_lease
from purra.interaction import UserInputRequired
from purra.approvals import ApprovalRequired
from purra.errors import ContractViolationError
from purra.normalization import positive_int, required_text
from purra.ports import CancellationSignal


@dataclass(frozen=True, slots=True)
class AgentTreeExecutionResult:
    """Terminal material returned by the one shared Run executor."""

    status: AgentTreeRunStatus
    result: Any = None
    content_ref: str | None = None
    fingerprint: str | None = None
    error_code: str | None = None

    def __post_init__(self) -> None:
        status = AgentTreeRunStatus(self.status)
        if status not in {
            AgentTreeRunStatus.DONE,
            AgentTreeRunStatus.FAILED,
            AgentTreeRunStatus.CANCELED,
        }:
            raise ValueError("Agent tree executor must return a terminal status")
        error = str(self.error_code or "").strip() or None
        if status is AgentTreeRunStatus.DONE:
            if error is not None:
                raise ValueError("completed Agent Run cannot carry an error")
            object.__setattr__(
                self,
                "content_ref",
                required_text(self.content_ref, "Agent Run content reference"),
            )
            object.__setattr__(
                self,
                "fingerprint",
                required_text(self.fingerprint, "Agent Run context fingerprint"),
            )
        elif error is None:
            raise ValueError("unfinished Agent Run requires an error code")
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "error_code", error)


@runtime_checkable
class AgentTreeRunExecutor(Protocol):
    """Execute Root and Child Runs through the same runtime composition."""

    async def execute(
        self,
        run: AgentTreeRun,
        agent: AgentNode,
        checkpoint: ContextCheckpoint | None,
        signal: CancellationSignal | None = None,
    ) -> AgentTreeExecutionResult: ...


@runtime_checkable
class AgentTreeRunCoordinator(Protocol):
    async def execute_and_join(
        self,
        requester_run_id: str,
        run_ids: tuple[str, ...],
        signal: CancellationSignal | None = None,
        *,
        lease_owner_id: str | None = None,
        lease_epoch: int | None = None,
        delivery_signal: CancellationSignal | None = None,
    ) -> AgentRunAggregation: ...


class _AgentTreeJoinCanceled(asyncio.CancelledError):
    code = "child_run_join_canceled"


class _AgentTreeSchedulingCapability:
    """Claim queued Runs, settle them once, and resume their waiting parent."""

    def __init__(
        self,
        *,
        repository: RunTreeRepository,
        executor: AgentTreeRunExecutor,
        owner_id: str | None = None,
        lease_duration_ms: int = 30_000,
        deliver_results=None,
    ) -> None:
        if not isinstance(repository, RunTreeRepository):
            raise TypeError("Agent tree supervisor requires RunTreeRepository")
        if not isinstance(executor, AgentTreeRunExecutor):
            raise TypeError("Agent tree supervisor requires one Run executor")
        self._repository = repository
        self._executor = executor
        self._deliver_results = deliver_results
        self._owner_id = required_text(
            owner_id or f"agent-tree-supervisor-{uuid4().hex}",
            "Agent tree supervisor owner id",
        )
        self._lease_duration_ms = positive_int(
            lease_duration_ms,
            "Agent tree lease duration",
        )

    async def execute_and_join(self, requester_run_id, run_ids, signal=None, *,
                               lease_owner_id=None, lease_epoch=None,
                               delivery_signal=None):
        requester = await self._repository.get_run(requester_run_id)
        if self._deliver_results is not None and requester.root_run_id == requester_run_id:
            return await deliver_agent_results(
                lambda notify: self._execute_and_join(requester_run_id, run_ids, signal,
                    lease_owner_id=lease_owner_id, lease_epoch=lease_epoch, notify=notify),
                lambda results: self._deliver_results(requester_run_id, results, delivery_signal or signal),
            )
        return await self._execute_and_join(
            requester_run_id, run_ids, signal,
            lease_owner_id=lease_owner_id, lease_epoch=lease_epoch,
        )

    async def _execute_and_join(
        self,
        requester_run_id: str,
        run_ids: tuple[str, ...],
        signal: CancellationSignal | None = None,
        *,
        lease_owner_id: str | None = None,
        lease_epoch: int | None = None,
        notify=None,
    ) -> AgentRunAggregation:
        requester_id = required_text(requester_run_id, "requester Run id")
        target_ids = tuple(dict.fromkeys(
            required_text(item, "Child Run id") for item in run_ids
        ))
        if not target_ids:
            return await self._repository.aggregate_runs(requester_id, ())

        requester = await self._repository.get_run(requester_id)
        for identity in target_ids:
            target = await self._repository.get_run(identity)
            for dependency_id in target.dependency_run_ids:
                dependency = await self._repository.get_run(dependency_id)
                if not dependency.terminal and dependency_id not in target_ids:
                    raise ContractViolationError("Join must include unfinished dependencies", code="agent_dependency_join_incomplete")
        if requester.status is AgentTreeRunStatus.WAITING:
            await self._repository.require_run_claim(
                requester_id,
                lease_owner_id=lease_owner_id,
                lease_epoch=lease_epoch,
            )
        else:
            requester = await self._mark_waiting(
                requester_id,
                lease_owner_id=lease_owner_id,
                lease_epoch=lease_epoch,
            )
        root_run_id = requester.root_run_id
        pending = set(target_ids)
        active: dict[asyncio.Task[None], str] = {}
        attempted: set[str] = set()
        joined = False
        try:
            while pending:
                if is_canceled(signal):
                    raise _AgentTreeJoinCanceled(
                        "Child Run join was canceled"
                    )
                aggregate = await self._repository.aggregate_runs(
                    requester_id,
                    target_ids,
                )
                if notify is not None:
                    await notify(aggregate)
                pending = set(aggregate.pending_run_ids)
                if not pending:
                    joined = True
                    return aggregate
                if (await self._repository.get_run(requester_id)).status is AgentTreeRunStatus.RUNNING:
                    await self._mark_waiting(requester_id,
                        lease_owner_id=lease_owner_id, lease_epoch=lease_epoch)
                for candidate in await self._repository.list_runnable(root_run_id):
                    if candidate.run_id not in pending or candidate.run_id in attempted:
                        continue
                    claimed = await self._repository.claim_run(
                        candidate.run_id,
                        owner_id=self._owner_id,
                        lease_duration_ms=self._lease_duration_ms,
                    )
                    if claimed is None:
                        continue
                    async def execute_and_notify(run):
                        await self._execute_claimed(run, signal)
                        if notify is not None:
                            current = await self._repository.aggregate_runs(requester_id, target_ids)
                            await notify(replace(current, results=tuple(item for item in current.results if item["runId"] == run.run_id)))
                    task = asyncio.create_task(execute_and_notify(claimed))
                    active[task] = claimed.run_id
                    attempted.add(claimed.run_id)
                if not active:
                    blocked = set(attempted)
                    changed = True
                    while changed:
                        changed = False
                        for identity in pending - blocked:
                            candidate = await self._repository.get_run(identity)
                            if any(dependency in blocked for dependency in candidate.dependency_run_ids):
                                blocked.add(identity)
                                changed = True
                    if pending <= blocked:
                        joined = True
                        return aggregate
                    # Other joins can occupy the shared root capacity. Their
                    # completion is progress even when this join has no worker.
                    if any(item.status is AgentTreeRunStatus.RUNNING
                           for item in await self._repository.list_descendants(root_run_id)):
                        await asyncio.sleep(0.01)
                        continue
                    raise ContractViolationError(
                        "Child Run scheduler made no progress",
                        code="agent_run_scheduler_stalled",
                    )
                try:
                    done, _ = await await_with_cancellation(
                        asyncio.wait(
                            tuple(active),
                            return_when=asyncio.FIRST_COMPLETED,
                        ),
                        signal,
                    )
                except OperationCanceled:
                    raise _AgentTreeJoinCanceled(
                        "Child Run join was canceled"
                    ) from None
                for task in done:
                    run_id = active.pop(task)
                    await task
                    pending.discard(run_id)
            aggregate = await self._repository.aggregate_runs(
                requester_id,
                target_ids,
            )
            if notify is not None:
                await notify(aggregate)
            joined = True
            return aggregate
        except asyncio.CancelledError as error:
            for run_id in target_ids:
                await self._repository.cancel_subtree(run_id)
            for task in active:
                task.cancel()
            if active:
                await asyncio.gather(*active, return_exceptions=True)
            joined = True
            if isinstance(error, _AgentTreeJoinCanceled):
                raise
            raise _AgentTreeJoinCanceled(
                "Child Run join was canceled"
            ) from None
        finally:
            current = await self._repository.get_run(requester_id)
            if joined and current.status is AgentTreeRunStatus.WAITING:
                try:
                    await self._repository.release_waiting(requester_id,
                        lease_owner_id=lease_owner_id, lease_epoch=lease_epoch)
                except ContractViolationError as error:
                    if error.code not in {"agent_run_state_conflict", "agent_capacity_exceeded"}:
                        raise
                    await self._repository.require_run_claim(requester_id,
                        lease_owner_id=lease_owner_id, lease_epoch=lease_epoch)
                    if (await self._repository.get_run(requester_id)).terminal:
                        raise


    async def _mark_waiting(self, run_id, *, lease_owner_id=None, lease_epoch=None):
        try:
            return await self._repository.mark_waiting(run_id,
                lease_owner_id=lease_owner_id, lease_epoch=lease_epoch)
        except ContractViolationError as error:
            if error.code != "agent_run_state_conflict":
                raise
            await self._repository.require_run_claim(run_id,
                lease_owner_id=lease_owner_id, lease_epoch=lease_epoch)
            current = await self._repository.get_run(run_id)
            if current.status is not AgentTreeRunStatus.WAITING:
                raise
            return current

    async def _execute_claimed(
        self,
        run: AgentTreeRun,
        signal: CancellationSignal | None,
    ) -> None:
        agent = await self._repository.get_agent(run.agent_id)
        checkpoint = (
            await self._repository.get_checkpoint(agent.context_checkpoint_id)
            if agent.context_checkpoint_id is not None
            else None
        )
        expected_context_version = agent.context_version
        try:
            dependencies = [await self._repository.get_run(identity) for identity in run.dependency_run_ids]
            if any(dependency.status is not AgentTreeRunStatus.DONE for dependency in dependencies):
                raise ContractViolationError("A required predecessor did not complete", code="agent_dependency_failed")
            result = await self._execute_with_heartbeat(
                run,
                agent,
                checkpoint,
                signal,
            )
            if not isinstance(result, AgentTreeExecutionResult):
                raise TypeError("Agent tree executor returned an invalid result")
        except asyncio.CancelledError:
            await self._repository.cancel_subtree(run.run_id)
            raise
        except (UserInputRequired, ApprovalRequired):
            await self._repository.suspend_run(run.run_id, lease_owner_id=run.lease_owner_id, lease_epoch=run.lease_epoch)
            return
        except Exception as error:
            await self._repository.fail_run(
                run.run_id,
                str(getattr(error, "code", "") or "agent_execution_failed"),
                lease_owner_id=run.lease_owner_id,
                lease_epoch=run.lease_epoch,
            )
            return
        if result.status is AgentTreeRunStatus.DONE:
            await self._repository.complete_run(
                run.run_id,
                expected_context_version=expected_context_version,
                result=result.result,
                content_ref=result.content_ref or "",
                fingerprint=result.fingerprint or "",
                lease_owner_id=run.lease_owner_id,
                lease_epoch=run.lease_epoch,
            )
        elif result.status is AgentTreeRunStatus.CANCELED:
            await self._repository.cancel_subtree(run.run_id)
        else:
            await self._repository.fail_run(
                run.run_id,
                result.error_code or "agent_run_failed",
                lease_owner_id=run.lease_owner_id,
                lease_epoch=run.lease_epoch,
            )

    async def _execute_with_heartbeat(
        self,
        run: AgentTreeRun,
        agent: AgentNode,
        checkpoint: ContextCheckpoint | None,
        signal: CancellationSignal | None,
    ) -> AgentTreeExecutionResult:
        with bind_agent_run_lease(run.run_id, required_text(run.lease_owner_id, "Agent Run lease owner"), run.lease_epoch):
            execution = asyncio.create_task(self._executor.execute(
                run, agent, checkpoint, signal,
            ))
        heartbeat = asyncio.create_task(self._heartbeat(run))
        try:
            done, _ = await asyncio.wait(
                (execution, heartbeat),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if heartbeat in done:
                error = heartbeat.exception()
                execution.cancel()
                await asyncio.gather(execution, return_exceptions=True)
                if error is not None:
                    raise error
                raise ContractViolationError(
                    "Agent Run heartbeat stopped",
                    code="agent_run_lease_lost",
                )
            return await execution
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

    async def _heartbeat(self, run: AgentTreeRun) -> None:
        if run.lease_owner_id is None:
            return
        interval = max(0.001, self._lease_duration_ms / 3_000)
        while True:
            await asyncio.sleep(interval)
            await self._repository.renew_run_lease(
                run.run_id,
                owner_id=run.lease_owner_id,
                lease_epoch=run.lease_epoch,
                lease_duration_ms=self._lease_duration_ms,
            )


class AgentTreeRunSupervisor:
    """Standalone owner for hosts using RunCommandService without AgentCore."""

    def __init__(
        self,
        *,
        repository: RunTreeRepository,
        executor: AgentTreeRunExecutor,
        deliver_results=None,
        owner_id: str | None = None,
        lease_duration_ms: int = 30_000,
    ) -> None:
        self._scheduling = _AgentTreeSchedulingCapability(
            repository=repository,
            executor=executor,
            deliver_results=deliver_results,
            owner_id=owner_id,
            lease_duration_ms=lease_duration_ms,
        )

    async def execute_and_join(
        self,
        requester_run_id: str,
        run_ids: tuple[str, ...],
        signal: CancellationSignal | None = None,
        *,
        lease_owner_id: str | None = None,
        lease_epoch: int | None = None,
        delivery_signal: CancellationSignal | None = None,
    ) -> AgentRunAggregation:
        return await self._scheduling.execute_and_join(
            requester_run_id,
            run_ids,
            signal,
            lease_owner_id=lease_owner_id,
            lease_epoch=lease_epoch,
            delivery_signal=delivery_signal,
        )


class RunCommandService:
    """The only public command path that creates or links Agent Runs."""

    def __init__(
        self,
        repository: RunTreeRepository,
        supervisor: AgentTreeRunCoordinator | None = None,
    ) -> None:
        if not isinstance(repository, RunTreeRepository):
            raise TypeError("Run command service requires RunTreeRepository")
        if supervisor is not None and not isinstance(
            supervisor,
            AgentTreeRunCoordinator,
        ):
            raise TypeError("Run command service supervisor is invalid")
        self._repository = repository
        self._supervisor = supervisor
        self.agents = AgentTreeQuery(repository)
        self.results = AgentResultReceiver(repository, self.join_runs)

    async def begin_root(self, command: BeginRootAgentCommand) -> AgentTreeRun:
        return await self._repository.begin_root(command)

    async def spawn_agents(
        self,
        command: SpawnAgentsCommand,
    ) -> SpawnAgentsReceipt:
        return await self._repository.spawn_agents(command)

    async def compile_child_grant(
        self,
        parent_run_id: str,
        *,
        can_spawn_agents: bool,
        allowed_tools: tuple[str, ...] | None = None,
    ) -> AgentCapabilityGrant:
        if not isinstance(can_spawn_agents, bool):
            raise TypeError("child spawn authority must be boolean")
        parent_run = await self._repository.get_run(parent_run_id)
        parent = await self._repository.get_agent(parent_run.agent_id)
        selected_tools = (
            parent.capability_grant.allowed_tools
            if allowed_tools is None
            else tuple(
                name
                for name in allowed_tools
                if name in parent.capability_grant.allowed_tools
            )
        )
        return replace(
            parent.capability_grant,
            can_spawn_agents=(
                parent.capability_grant.can_spawn_agents and can_spawn_agents
            ),
            allowed_tools=selected_tools,
        )

    async def continue_agent(
        self,
        command: ContinueAgentCommand,
    ) -> ContinueAgentReceipt:
        return await self._repository.continue_agent(command)

    async def join_runs(
        self,
        requester_run_id: str,
        run_ids: tuple[str, ...],
        signal: CancellationSignal | None = None,
        *,
        lease_owner_id: str | None = None,
        lease_epoch: int | None = None,
        delivery_signal: CancellationSignal | None = None,
    ) -> AgentRunAggregation:
        if self._supervisor is None:
            return await self._repository.aggregate_runs(
                requester_run_id,
                run_ids,
            )
        return await self._supervisor.execute_and_join(
            requester_run_id,
            run_ids,
            signal,
            lease_owner_id=lease_owner_id,
            lease_epoch=lease_epoch,
            delivery_signal=delivery_signal,
        )

    async def receive_runs(
        self, requester_run_id: str, run_ids: tuple[str, ...],
        signal: CancellationSignal | None = None, *,
        after_run_ids: tuple[str, ...] = (),
        lease_owner_id: str | None = None, lease_epoch: int | None = None,
    ) -> AgentRunAggregation:
        return await self.results.receive(
            requester_run_id, run_ids, signal, after_run_ids=after_run_ids,
            lease_owner_id=lease_owner_id, lease_epoch=lease_epoch,
        )

    async def cancel_run(self, run_id: str) -> tuple[str, ...]:
        return await self._repository.cancel_subtree(run_id)

    async def close_agent(self, agent_id: str) -> AgentNode:
        return await self._repository.close_agent(agent_id)


__all__ = [
    "AgentTreeExecutionResult",
    "AgentTreeRunExecutor",
    "AgentTreeRunCoordinator",
    "AgentTreeRunSupervisor",
    "RunCommandService",
]
