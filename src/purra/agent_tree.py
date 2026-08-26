"""Stable recursive Agent identities and immutable Run chains."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import StrEnum
from hashlib import sha256
import json
import time
from typing import Any, Protocol, runtime_checkable

from purra.errors import ContractViolationError
from purra.json_values import freeze_json_mapping, thaw_json_mapping
from purra.normalization import (
    non_negative_int,
    positive_int,
    required_text,
    unique_text_tuple,
)


class AgentNodeState(StrEnum):
    ACTIVE = "active"
    CLOSED = "closed"


class AgentTreeRunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    WAITING = "waiting"
    DONE = "done"
    FAILED = "failed"
    CANCELED = "canceled"


_ACTIVE_RUN_STATUSES = frozenset({
    AgentTreeRunStatus.QUEUED,
    AgentTreeRunStatus.RUNNING,
    AgentTreeRunStatus.WAITING,
})
_TERMINAL_RUN_STATUSES = frozenset({
    AgentTreeRunStatus.DONE,
    AgentTreeRunStatus.FAILED,
    AgentTreeRunStatus.CANCELED,
})


@dataclass(frozen=True, slots=True)
class AgentCapabilityGrant:
    """Authority that a parent may only preserve or narrow for a child."""

    can_spawn_agents: bool = False
    max_depth: int = 3
    max_children_per_call: int = 3
    max_agents_per_root: int = 16
    max_parallel_runs: int = 3
    allowed_tools: tuple[str, ...] = ()
    allowed_models: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.can_spawn_agents, bool):
            raise TypeError("can_spawn_agents must be boolean")
        for name in (
            "max_depth",
            "max_children_per_call",
            "max_agents_per_root",
            "max_parallel_runs",
        ):
            object.__setattr__(
                self,
                name,
                positive_int(getattr(self, name), name.replace("_", " ")),
            )
        object.__setattr__(
            self,
            "allowed_tools",
            unique_text_tuple(self.allowed_tools),
        )
        object.__setattr__(
            self,
            "allowed_models",
            unique_text_tuple(self.allowed_models),
        )

    def authorize_child(
        self,
        requested: AgentCapabilityGrant | None,
    ) -> AgentCapabilityGrant:
        child = requested or self
        if not isinstance(child, AgentCapabilityGrant):
            raise TypeError("child Agent capability grant is invalid")
        if child.can_spawn_agents and not self.can_spawn_agents:
            _raise("agent_capability_escalation", "child cannot gain spawn authority")
        for name in (
            "max_depth",
            "max_children_per_call",
            "max_agents_per_root",
            "max_parallel_runs",
        ):
            if getattr(child, name) > getattr(self, name):
                _raise(
                    "agent_capability_escalation",
                    f"child cannot increase {name}",
                )
        if set(child.allowed_tools) - set(self.allowed_tools):
            _raise("agent_capability_escalation", "child cannot gain tools")
        if set(child.allowed_models) - set(self.allowed_models):
            _raise("agent_capability_escalation", "child cannot gain models")
        return child

    def to_mapping(self) -> dict[str, object]:
        return {
            "canSpawnAgents": self.can_spawn_agents,
            "maxDepth": self.max_depth,
            "maxChildrenPerCall": self.max_children_per_call,
            "maxAgentsPerRoot": self.max_agents_per_root,
            "maxParallelRuns": self.max_parallel_runs,
            "allowedTools": list(self.allowed_tools),
            "allowedModels": list(self.allowed_models),
        }


@dataclass(frozen=True, slots=True)
class AgentNode:
    agent_id: str
    root_agent_id: str
    parent_agent_id: str | None
    depth: int
    created_by_run_id: str
    created_by_call_id: str
    name: str
    title: str
    instruction: str
    capability_grant: AgentCapabilityGrant
    context_version: int = 0
    context_checkpoint_id: str | None = None
    latest_run_id: str | None = None
    state: AgentNodeState = AgentNodeState.ACTIVE

    def __post_init__(self) -> None:
        for name, label in (
            ("agent_id", "Agent id"),
            ("root_agent_id", "root Agent id"),
            ("created_by_run_id", "Agent creator Run id"),
            ("created_by_call_id", "Agent creator call id"),
            ("name", "Agent name"),
            ("title", "Agent title"),
            ("instruction", "Agent instruction"),
        ):
            object.__setattr__(self, name, required_text(getattr(self, name), label))
        if self.parent_agent_id is not None:
            object.__setattr__(
                self,
                "parent_agent_id",
                required_text(self.parent_agent_id, "parent Agent id"),
            )
        depth = non_negative_int(self.depth, "Agent depth")
        if (self.parent_agent_id is None) != (depth == 0):
            raise ValueError("only a root Agent may have depth zero")
        object.__setattr__(self, "depth", depth)
        if not isinstance(self.capability_grant, AgentCapabilityGrant):
            raise TypeError("Agent capability grant is required")
        object.__setattr__(
            self,
            "context_version",
            non_negative_int(self.context_version, "Agent context version"),
        )
        if self.context_checkpoint_id is not None:
            object.__setattr__(
                self,
                "context_checkpoint_id",
                required_text(self.context_checkpoint_id, "context checkpoint id"),
            )
        if self.latest_run_id is not None:
            object.__setattr__(
                self,
                "latest_run_id",
                required_text(self.latest_run_id, "latest Agent Run id"),
            )
        object.__setattr__(self, "state", AgentNodeState(self.state))


@dataclass(frozen=True, slots=True)
class AgentTreeRun:
    run_id: str
    agent_id: str
    root_run_id: str
    parent_run_id: str | None
    previous_run_id: str | None
    spawn_batch_id: str | None
    objective: str
    input_payload: Mapping[str, Any] = field(default_factory=dict)
    required: bool = True
    priority: int = 0
    status: AgentTreeRunStatus = AgentTreeRunStatus.QUEUED
    result: Any = None
    error_code: str | None = None
    created_sequence: int = 0
    lease_owner_id: str | None = None
    lease_epoch: int = 0
    lease_expires_at_ms: int | None = None

    def __post_init__(self) -> None:
        for name, label in (
            ("run_id", "Agent Run id"),
            ("agent_id", "Agent Run Agent id"),
            ("root_run_id", "root Run id"),
            ("objective", "Agent Run objective"),
        ):
            object.__setattr__(self, name, required_text(getattr(self, name), label))
        for name, label in (
            ("parent_run_id", "parent Run id"),
            ("previous_run_id", "previous Run id"),
            ("spawn_batch_id", "spawn batch id"),
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, required_text(value, label))
        object.__setattr__(
            self,
            "input_payload",
            freeze_json_mapping(self.input_payload),
        )
        if not isinstance(self.required, bool):
            raise TypeError("child Agent required must be boolean")
        if not isinstance(self.priority, int) or isinstance(self.priority, bool):
            raise TypeError("Agent Run priority must be an integer")
        object.__setattr__(self, "status", AgentTreeRunStatus(self.status))
        error_code = str(self.error_code or "").strip() or None
        if self.status is AgentTreeRunStatus.DONE and error_code is not None:
            raise ValueError("completed Agent Run cannot carry an error")
        if self.status in {
            AgentTreeRunStatus.FAILED,
            AgentTreeRunStatus.CANCELED,
        } and error_code is None:
            raise ValueError("failed or canceled Agent Run requires an error code")
        object.__setattr__(self, "error_code", error_code)
        object.__setattr__(
            self,
            "created_sequence",
            non_negative_int(self.created_sequence, "Agent Run sequence"),
        )
        if self.lease_owner_id is not None:
            object.__setattr__(
                self,
                "lease_owner_id",
                required_text(self.lease_owner_id, "Agent Run lease owner"),
            )
        object.__setattr__(
            self,
            "lease_epoch",
            non_negative_int(self.lease_epoch, "Agent Run lease epoch"),
        )
        object.__setattr__(
            self,
            "lease_expires_at_ms",
            (
                None
                if self.lease_expires_at_ms is None
                else positive_int(
                    self.lease_expires_at_ms,
                    "Agent Run lease expiry",
                )
            ),
        )
        if (self.lease_owner_id is None) != (self.lease_expires_at_ms is None):
            raise ValueError("Agent Run lease owner and expiry must match")

    @property
    def terminal(self) -> bool:
        return self.status in _TERMINAL_RUN_STATUSES


@dataclass(frozen=True, slots=True)
class ContextCheckpoint:
    checkpoint_id: str
    agent_id: str
    version: int
    previous_checkpoint_id: str | None
    source_run_id: str
    content_ref: str
    fingerprint: str

    def __post_init__(self) -> None:
        for name, label in (
            ("checkpoint_id", "context checkpoint id"),
            ("agent_id", "context checkpoint Agent id"),
            ("source_run_id", "context checkpoint Run id"),
            ("content_ref", "context checkpoint content reference"),
            ("fingerprint", "context checkpoint fingerprint"),
        ):
            object.__setattr__(self, name, required_text(getattr(self, name), label))
        object.__setattr__(
            self,
            "version",
            positive_int(self.version, "context checkpoint version"),
        )
        if self.previous_checkpoint_id is not None:
            object.__setattr__(
                self,
                "previous_checkpoint_id",
                required_text(
                    self.previous_checkpoint_id,
                    "previous context checkpoint id",
                ),
            )


@dataclass(frozen=True, slots=True)
class ChildAgentSpec:
    name: str
    title: str
    instruction: str
    objective: str
    input_payload: Mapping[str, Any] = field(default_factory=dict)
    required: bool = True
    priority: int = 0
    capability_grant: AgentCapabilityGrant | None = None

    def __post_init__(self) -> None:
        for name, label in (
            ("name", "child Agent name"),
            ("title", "child Agent title"),
            ("instruction", "child Agent instruction"),
            ("objective", "child Agent objective"),
        ):
            object.__setattr__(self, name, required_text(getattr(self, name), label))
        object.__setattr__(
            self,
            "input_payload",
            freeze_json_mapping(self.input_payload),
        )
        if not isinstance(self.required, bool):
            raise TypeError("child Agent required must be boolean")
        if not isinstance(self.priority, int) or isinstance(self.priority, bool):
            raise TypeError("child Agent priority must be an integer")
        if (
            self.capability_grant is not None
            and not isinstance(self.capability_grant, AgentCapabilityGrant)
        ):
            raise TypeError("child Agent capability grant is invalid")

    def to_mapping(self) -> dict[str, object]:
        return {
            "name": self.name,
            "title": self.title,
            "instruction": self.instruction,
            "objective": self.objective,
            "input": thaw_json_mapping(self.input_payload),
            "required": self.required,
            "priority": self.priority,
            "capabilityGrant": (
                self.capability_grant.to_mapping()
                if self.capability_grant is not None
                else None
            ),
        }


@dataclass(frozen=True, slots=True)
class BeginRootAgentCommand:
    run_id: str
    agent_id: str
    name: str
    title: str
    instruction: str
    objective: str
    capability_grant: AgentCapabilityGrant
    idempotency_key: str

    def __post_init__(self) -> None:
        for name, label in (
            ("run_id", "root Run id"),
            ("agent_id", "root Agent id"),
            ("name", "root Agent name"),
            ("title", "root Agent title"),
            ("instruction", "root Agent instruction"),
            ("objective", "root Run objective"),
            ("idempotency_key", "root Agent idempotency key"),
        ):
            object.__setattr__(self, name, required_text(getattr(self, name), label))
        if not isinstance(self.capability_grant, AgentCapabilityGrant):
            raise TypeError("root Agent capability grant is required")


@dataclass(frozen=True, slots=True)
class SpawnAgentsCommand:
    parent_run_id: str
    idempotency_key: str
    children: tuple[ChildAgentSpec, ...]
    lease_owner_id: str | None = None
    lease_epoch: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "parent_run_id",
            required_text(self.parent_run_id, "parent Run id"),
        )
        object.__setattr__(
            self,
            "idempotency_key",
            required_text(self.idempotency_key, "spawn idempotency key"),
        )
        children = tuple(self.children)
        if not children or any(not isinstance(item, ChildAgentSpec) for item in children):
            raise ValueError("spawn command requires child Agent specs")
        if len({item.name for item in children}) != len(children):
            raise ValueError("spawned Agent names must be unique in one command")
        object.__setattr__(self, "children", children)
        _normalize_command_lease(self)


@dataclass(frozen=True, slots=True)
class ContinueAgentCommand:
    requester_run_id: str
    idempotency_key: str
    agent_id: str
    expected_context_version: int
    message: str
    required: bool = True
    priority: int = 0
    lease_owner_id: str | None = None
    lease_epoch: int | None = None

    def __post_init__(self) -> None:
        for name, label in (
            ("requester_run_id", "continuation requester Run id"),
            ("idempotency_key", "continuation idempotency key"),
            ("agent_id", "continued Agent id"),
            ("message", "continuation message"),
        ):
            object.__setattr__(self, name, required_text(getattr(self, name), label))
        object.__setattr__(
            self,
            "expected_context_version",
            non_negative_int(
                self.expected_context_version,
                "expected Agent context version",
            ),
        )
        if not isinstance(self.required, bool):
            raise TypeError("continuation required must be boolean")
        if not isinstance(self.priority, int) or isinstance(self.priority, bool):
            raise TypeError("continuation priority must be an integer")
        _normalize_command_lease(self)


@dataclass(frozen=True, slots=True)
class SpawnedAgent:
    agent: AgentNode
    run: AgentTreeRun


@dataclass(frozen=True, slots=True)
class SpawnAgentsReceipt:
    batch_id: str
    items: tuple[SpawnedAgent, ...]
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class ContinueAgentReceipt:
    agent: AgentNode
    run: AgentTreeRun
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class AgentRunAggregation:
    state: str
    pending_run_ids: tuple[str, ...]
    required_failures: tuple[str, ...]
    results: tuple[Mapping[str, Any], ...]


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


class InMemoryRunTreeRepository:
    """Atomic reference adapter; hosts may implement the same port durably."""

    def __init__(
        self,
        *,
        transaction_lock: asyncio.Lock | None = None,
        clock_ms=None,
    ) -> None:
        self._lock = transaction_lock or asyncio.Lock()
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1000))
        if not callable(self._clock_ms):
            raise TypeError("Agent tree clock must be callable")
        self._agents: dict[str, AgentNode] = {}
        self._runs: dict[str, AgentTreeRun] = {}
        self._checkpoints: dict[str, ContextCheckpoint] = {}
        self._spawn_receipts: dict[
            tuple[str, str],
            tuple[str, SpawnAgentsReceipt],
        ] = {}
        self._continue_receipts: dict[
            tuple[str, str],
            tuple[str, ContinueAgentReceipt],
        ] = {}
        self._root_digests: dict[str, str] = {}
        self._sequence = 0
        self._agent_sequence = 0
        self._run_sequence = 0
        self._batch_sequence = 0
        self._checkpoint_sequence = 0

    async def begin_root(self, command: BeginRootAgentCommand) -> AgentTreeRun:
        if not isinstance(command, BeginRootAgentCommand):
            raise TypeError("begin_root requires BeginRootAgentCommand")
        async with self._lock:
            digest = _digest({
                "agentId": command.agent_id,
                "name": command.name,
                "title": command.title,
                "instruction": command.instruction,
                "objective": command.objective,
                "capabilityGrant": command.capability_grant.to_mapping(),
                "idempotencyKey": command.idempotency_key,
            })
            existing_run = self._runs.get(command.run_id)
            if existing_run is not None:
                if self._root_digests.get(command.run_id) != digest:
                    _raise(
                        "child_spawn_idempotency_conflict",
                        "root Run id was reused with different input",
                    )
                return existing_run
            agent = self._agents.get(command.agent_id)
            if agent is None:
                agent = AgentNode(
                    agent_id=command.agent_id,
                    root_agent_id=command.agent_id,
                    parent_agent_id=None,
                    depth=0,
                    created_by_run_id=command.run_id,
                    created_by_call_id=command.idempotency_key,
                    name=command.name,
                    title=command.title,
                    instruction=command.instruction,
                    capability_grant=command.capability_grant,
                )
            else:
                if (
                    agent.parent_agent_id is not None
                    or agent.state is AgentNodeState.CLOSED
                    or agent.capability_grant != command.capability_grant
                ):
                    _raise("agent_scope_violation", "root Agent cannot be rebound")
                if self._has_active_run(agent.agent_id):
                    _raise("agent_busy", "root Agent already has an active Run")
            run = AgentTreeRun(
                run_id=command.run_id,
                agent_id=agent.agent_id,
                root_run_id=command.run_id,
                parent_run_id=None,
                previous_run_id=agent.latest_run_id,
                spawn_batch_id=None,
                objective=command.objective,
                status=AgentTreeRunStatus.RUNNING,
                created_sequence=self._next_sequence(),
            )
            self._runs[run.run_id] = run
            self._root_digests[run.run_id] = digest
            self._agents[agent.agent_id] = replace(agent, latest_run_id=run.run_id)
            return run

    async def spawn_agents(
        self,
        command: SpawnAgentsCommand,
    ) -> SpawnAgentsReceipt:
        if not isinstance(command, SpawnAgentsCommand):
            raise TypeError("spawn_agents requires SpawnAgentsCommand")
        async with self._lock:
            digest = _digest([item.to_mapping() for item in command.children])
            key = (command.parent_run_id, command.idempotency_key)
            replay = self._spawn_receipts.get(key)
            if replay is not None:
                if replay[0] != digest:
                    _raise(
                        "child_spawn_idempotency_conflict",
                        "spawn key was reused with different input",
                    )
                return replace(replay[1], replayed=True)
            parent_run = self._require_active_run(command.parent_run_id)
            self._require_claim_unlocked(
                parent_run,
                command.lease_owner_id,
                command.lease_epoch,
            )
            parent_agent = self._require_active_agent(parent_run.agent_id)
            grant = parent_agent.capability_grant
            if not grant.can_spawn_agents:
                _raise("agent_capability_escalation", "Agent cannot create children")
            if parent_agent.depth >= grant.max_depth:
                _raise("agent_depth_exceeded", "maximum Agent depth was reached")
            if len(command.children) > grant.max_children_per_call:
                _raise("agent_capacity_exceeded", "too many children in one call")
            participating = {
                run.agent_id
                for run in self._runs.values()
                if run.root_run_id == parent_run.root_run_id
            }
            if len(participating) + len(command.children) > grant.max_agents_per_root:
                _raise("agent_capacity_exceeded", "Root Agent capacity was exceeded")

            self._batch_sequence += 1
            batch_id = f"agent-batch-{self._batch_sequence}"
            items = []
            for spec in command.children:
                child_grant = grant.authorize_child(spec.capability_grant)
                self._agent_sequence += 1
                self._run_sequence += 1
                agent_id = f"agent-{self._agent_sequence}"
                run_id = f"agent-run-{self._run_sequence}"
                agent = AgentNode(
                    agent_id=agent_id,
                    root_agent_id=parent_agent.root_agent_id,
                    parent_agent_id=parent_agent.agent_id,
                    depth=parent_agent.depth + 1,
                    created_by_run_id=parent_run.run_id,
                    created_by_call_id=command.idempotency_key,
                    name=spec.name,
                    title=spec.title,
                    instruction=spec.instruction,
                    capability_grant=child_grant,
                    latest_run_id=run_id,
                )
                run = AgentTreeRun(
                    run_id=run_id,
                    agent_id=agent_id,
                    root_run_id=parent_run.root_run_id,
                    parent_run_id=parent_run.run_id,
                    previous_run_id=None,
                    spawn_batch_id=batch_id,
                    objective=spec.objective,
                    input_payload=spec.input_payload,
                    required=spec.required,
                    priority=spec.priority,
                    created_sequence=self._next_sequence(),
                )
                self._agents[agent_id] = agent
                self._runs[run_id] = run
                items.append(SpawnedAgent(agent=agent, run=run))
            receipt = SpawnAgentsReceipt(batch_id=batch_id, items=tuple(items))
            self._spawn_receipts[key] = (digest, receipt)
            return receipt

    async def continue_agent(
        self,
        command: ContinueAgentCommand,
    ) -> ContinueAgentReceipt:
        if not isinstance(command, ContinueAgentCommand):
            raise TypeError("continue_agent requires ContinueAgentCommand")
        async with self._lock:
            digest = _digest({
                "agentId": command.agent_id,
                "contextVersion": command.expected_context_version,
                "message": command.message,
                "required": command.required,
                "priority": command.priority,
            })
            key = (command.requester_run_id, command.idempotency_key)
            replay = self._continue_receipts.get(key)
            if replay is not None:
                if replay[0] != digest:
                    _raise(
                        "child_spawn_idempotency_conflict",
                        "continuation key was reused with different input",
                    )
                return replace(replay[1], replayed=True)
            requester = self._require_active_run(command.requester_run_id)
            self._require_claim_unlocked(
                requester,
                command.lease_owner_id,
                command.lease_epoch,
            )
            target = self._require_active_agent(command.agent_id)
            requester_agent = self._require_active_agent(requester.agent_id)
            if requester_agent.root_agent_id != target.root_agent_id:
                _raise("agent_scope_violation", "continued Agent belongs to another tree")
            if not self._is_ancestor(requester_agent.agent_id, target.agent_id):
                _raise("agent_scope_violation", "requester is not an Agent ancestor")
            if target.context_version != command.expected_context_version:
                _raise("agent_context_conflict", "Agent context version changed")
            if self._has_active_run(target.agent_id):
                _raise("agent_busy", "Agent already has an active Run")
            participating = {
                run.agent_id
                for run in self._runs.values()
                if run.root_run_id == requester.root_run_id
            }
            root = self._require_active_agent(requester_agent.root_agent_id)
            if (
                target.agent_id not in participating
                and len(participating) >= root.capability_grant.max_agents_per_root
            ):
                _raise("agent_capacity_exceeded", "Root Agent capacity was exceeded")
            self._run_sequence += 1
            run = AgentTreeRun(
                run_id=f"agent-run-{self._run_sequence}",
                agent_id=target.agent_id,
                root_run_id=requester.root_run_id,
                parent_run_id=requester.run_id,
                previous_run_id=target.latest_run_id,
                spawn_batch_id=None,
                objective=command.message,
                required=command.required,
                priority=command.priority,
                created_sequence=self._next_sequence(),
            )
            next_agent = replace(target, latest_run_id=run.run_id)
            self._agents[target.agent_id] = next_agent
            self._runs[run.run_id] = run
            receipt = ContinueAgentReceipt(agent=next_agent, run=run)
            self._continue_receipts[key] = (digest, receipt)
            return receipt

    async def claim_run(
        self,
        run_id: str,
        *,
        owner_id: str = "run-tree-supervisor",
        lease_duration_ms: int = 30_000,
    ) -> AgentTreeRun | None:
        owner = required_text(owner_id, "Agent Run lease owner")
        duration = positive_int(lease_duration_ms, "Agent Run lease duration")
        async with self._lock:
            run = self._require_run(run_id)
            now = self._now_ms()
            reclaimable = (
                run.status is AgentTreeRunStatus.RUNNING
                and run.lease_expires_at_ms is not None
                and now >= run.lease_expires_at_ms
            )
            if run.status is not AgentTreeRunStatus.QUEUED and not reclaimable:
                return None
            root = self._require_active_agent(
                self._require_run(run.root_run_id).agent_id
            )
            active = sum(
                item.root_run_id == run.root_run_id
                and item.status is AgentTreeRunStatus.RUNNING
                and not self._lease_expired_unlocked(item, now)
                for item in self._runs.values()
            )
            if active >= root.capability_grant.max_parallel_runs:
                return None
            claimed = replace(
                run,
                status=AgentTreeRunStatus.RUNNING,
                lease_owner_id=owner,
                lease_epoch=run.lease_epoch + 1,
                lease_expires_at_ms=now + duration,
            )
            self._runs[run.run_id] = claimed
            return claimed

    async def renew_run_lease(
        self,
        run_id: str,
        *,
        owner_id: str,
        lease_epoch: int,
        lease_duration_ms: int,
    ) -> AgentTreeRun:
        owner = required_text(owner_id, "Agent Run lease owner")
        epoch = non_negative_int(lease_epoch, "Agent Run lease epoch")
        duration = positive_int(lease_duration_ms, "Agent Run lease duration")
        async with self._lock:
            run = self._require_active_run(run_id)
            self._require_claim_unlocked(run, owner, epoch)
            renewed = replace(
                run,
                lease_expires_at_ms=self._now_ms() + duration,
            )
            self._runs[run.run_id] = renewed
            return renewed

    async def require_run_claim(
        self,
        run_id: str,
        *,
        lease_owner_id: str | None = None,
        lease_epoch: int | None = None,
    ) -> None:
        async with self._lock:
            self._require_claim_unlocked(
                self._require_run(run_id),
                lease_owner_id,
                lease_epoch,
            )

    async def mark_waiting(
        self,
        run_id: str,
        *,
        lease_owner_id: str | None = None,
        lease_epoch: int | None = None,
    ) -> AgentTreeRun:
        async with self._lock:
            run = self._require_run(run_id)
            if run.status is not AgentTreeRunStatus.RUNNING:
                _raise("agent_run_state_conflict", "Agent Run state changed")
            self._require_claim_unlocked(run, lease_owner_id, lease_epoch)
            waiting = replace(run, status=AgentTreeRunStatus.WAITING)
            self._runs[run.run_id] = waiting
            return waiting

    async def release_waiting(
        self,
        run_id: str,
        *,
        lease_owner_id: str | None = None,
        lease_epoch: int | None = None,
    ) -> AgentTreeRun:
        async with self._lock:
            run = self._require_run(run_id)
            if run.status is not AgentTreeRunStatus.WAITING:
                _raise("agent_run_state_conflict", "Agent Run state changed")
            self._require_claim_unlocked(run, lease_owner_id, lease_epoch)
            root = self._require_active_agent(
                self._require_run(run.root_run_id).agent_id
            )
            active = sum(
                item.root_run_id == run.root_run_id
                and item.status is AgentTreeRunStatus.RUNNING
                and not self._lease_expired_unlocked(item)
                for item in self._runs.values()
            )
            if active >= root.capability_grant.max_parallel_runs:
                _raise(
                    "agent_capacity_exceeded",
                    "no root execution slot is available",
                )
            resumed = replace(run, status=AgentTreeRunStatus.RUNNING)
            self._runs[run.run_id] = resumed
            return resumed

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
    ) -> AgentTreeRun:
        async with self._lock:
            run = self._require_run(run_id)
            if run.status is not AgentTreeRunStatus.RUNNING:
                _raise("agent_run_state_conflict", "only a running Run may complete")
            self._require_claim_unlocked(run, lease_owner_id, lease_epoch)
            if any(
                not item.terminal
                and self._is_causal_descendant(run.run_id, item.run_id)
                for item in self._runs.values()
                if item.root_run_id == run.root_run_id
            ):
                _raise("root_run_not_quiescent", "Run has unfinished children")
            agent = self._require_active_agent(run.agent_id)
            if agent.context_version != expected_context_version:
                _raise("agent_context_conflict", "Agent context version changed")
            self._checkpoint_sequence += 1
            checkpoint = ContextCheckpoint(
                checkpoint_id=f"context-{self._checkpoint_sequence}",
                agent_id=agent.agent_id,
                version=agent.context_version + 1,
                previous_checkpoint_id=agent.context_checkpoint_id,
                source_run_id=run.run_id,
                content_ref=content_ref,
                fingerprint=fingerprint,
            )
            completed = replace(
                run,
                status=AgentTreeRunStatus.DONE,
                result=result,
                lease_owner_id=None,
                lease_expires_at_ms=None,
            )
            self._runs[run.run_id] = completed
            self._checkpoints[checkpoint.checkpoint_id] = checkpoint
            self._agents[agent.agent_id] = replace(
                agent,
                context_version=checkpoint.version,
                context_checkpoint_id=checkpoint.checkpoint_id,
            )
            return completed

    async def fail_run(
        self,
        run_id: str,
        error_code: str,
        *,
        lease_owner_id: str | None = None,
        lease_epoch: int | None = None,
    ) -> AgentTreeRun:
        error = required_text(error_code, "Agent Run error code")
        async with self._lock:
            run = self._require_run(run_id)
            if run.status not in _ACTIVE_RUN_STATUSES:
                return run
            self._require_claim_unlocked(run, lease_owner_id, lease_epoch)
            failed = replace(
                run,
                status=AgentTreeRunStatus.FAILED,
                error_code=error,
                lease_owner_id=None,
                lease_expires_at_ms=None,
            )
            self._runs[run.run_id] = failed
            for item in self._runs.values():
                if (
                    item.status in _ACTIVE_RUN_STATUSES
                    and self._is_causal_descendant(run.run_id, item.run_id)
                ):
                    self._runs[item.run_id] = replace(
                        item,
                        status=AgentTreeRunStatus.CANCELED,
                        error_code="ancestor_run_failed",
                        lease_owner_id=None,
                        lease_expires_at_ms=None,
                    )
            return failed

    async def cancel_subtree(self, run_id: str) -> tuple[str, ...]:
        async with self._lock:
            root = self._require_run(run_id)
            pending = [root.run_id]
            ordered: list[str] = []
            while pending:
                current = pending.pop()
                ordered.append(current)
                pending.extend(
                    item.run_id
                    for item in self._runs.values()
                    if (
                        item.root_run_id == root.root_run_id
                        and item.parent_run_id == current
                    )
                )
            canceled = []
            for current in ordered:
                run = self._runs[current]
                if run.status in _ACTIVE_RUN_STATUSES:
                    self._runs[current] = replace(
                        run,
                        status=AgentTreeRunStatus.CANCELED,
                        error_code="agent_run_canceled",
                        lease_owner_id=None,
                        lease_expires_at_ms=None,
                    )
                    canceled.append(current)
            return tuple(canceled)

    async def aggregate_runs(
        self,
        requester_run_id: str,
        run_ids: Sequence[str],
    ) -> AgentRunAggregation:
        async with self._lock:
            requester = self._require_run(requester_run_id)
            ids = tuple(dict.fromkeys(
                required_text(item, "joined Agent Run id") for item in run_ids
            ))
            rows = tuple(self._require_run(item) for item in ids)
            if any(
                row.root_run_id != requester.root_run_id
                or not self._is_causal_descendant(requester.run_id, row.run_id)
                for row in rows
            ):
                _raise("child_run_scope_violation", "joined Run escaped requester")
            pending = tuple(
                row.run_id for row in rows if row.status in _ACTIVE_RUN_STATUSES
            )
            failures = tuple(
                row.run_id
                for row in rows
                if row.required
                and row.status in {
                    AgentTreeRunStatus.FAILED,
                    AgentTreeRunStatus.CANCELED,
                }
            )
            return AgentRunAggregation(
                state="pending" if pending else "blocked" if failures else "ready",
                pending_run_ids=pending,
                required_failures=failures,
                results=tuple(
                    freeze_json_mapping({
                        "agentId": row.agent_id,
                        "runId": row.run_id,
                        "status": row.status.value,
                        "result": row.result,
                        "errorCode": row.error_code,
                    })
                    for row in rows
                    if row.terminal
                ),
            )

    async def close_agent(self, agent_id: str) -> AgentNode:
        async with self._lock:
            agent = self._require_active_agent(agent_id)
            if self._has_active_run(agent.agent_id):
                _raise("agent_busy", "Agent has an active Run")
            closed = replace(agent, state=AgentNodeState.CLOSED)
            self._agents[agent.agent_id] = closed
            return closed

    async def get_agent(self, agent_id: str) -> AgentNode:
        async with self._lock:
            return self._require_agent(agent_id)

    async def get_run(self, run_id: str) -> AgentTreeRun:
        async with self._lock:
            return self._require_run(run_id)

    async def get_checkpoint(self, checkpoint_id: str) -> ContextCheckpoint:
        checkpoint = required_text(checkpoint_id, "context checkpoint id")
        async with self._lock:
            try:
                return self._checkpoints[checkpoint]
            except KeyError as error:
                raise ContractViolationError(
                    "context checkpoint does not exist",
                    code="agent_context_checkpoint_not_found",
                ) from error

    async def list_runnable(self, root_run_id: str) -> tuple[AgentTreeRun, ...]:
        root = required_text(root_run_id, "root Run id")
        async with self._lock:
            now = self._now_ms()
            return tuple(sorted(
                (
                    run
                    for run in self._runs.values()
                    if (
                        run.root_run_id == root
                        and (
                            run.status is AgentTreeRunStatus.QUEUED
                            or (
                                run.status is AgentTreeRunStatus.RUNNING
                                and self._lease_expired_unlocked(run, now)
                            )
                        )
                    )
                ),
                key=lambda item: (-item.priority, item.created_sequence),
            ))

    async def list_descendants(self, run_id: str) -> tuple[AgentTreeRun, ...]:
        parent = required_text(run_id, "ancestor Run id")
        async with self._lock:
            self._require_run(parent)
            return tuple(sorted(
                (
                    run
                    for run in self._runs.values()
                    if self._is_causal_descendant(parent, run.run_id)
                ),
                key=lambda item: item.created_sequence,
            ))

    async def _transition(
        self,
        run_id: str,
        expected: set[AgentTreeRunStatus],
        status: AgentTreeRunStatus,
    ) -> AgentTreeRun:
        async with self._lock:
            run = self._require_run(run_id)
            if run.status not in expected:
                _raise("agent_run_state_conflict", "Agent Run state changed")
            changed = replace(run, status=status)
            self._runs[run.run_id] = changed
            return changed

    def _require_agent(self, agent_id: str) -> AgentNode:
        agent = required_text(agent_id, "Agent id")
        try:
            return self._agents[agent]
        except KeyError as error:
            raise ContractViolationError(
                "Agent does not exist",
                code="agent_not_found",
            ) from error

    def _require_active_agent(self, agent_id: str) -> AgentNode:
        agent = self._require_agent(agent_id)
        if agent.state is AgentNodeState.CLOSED:
            _raise("agent_closed", "Agent is closed")
        return agent

    def _require_run(self, run_id: str) -> AgentTreeRun:
        run = required_text(run_id, "Agent Run id")
        try:
            return self._runs[run]
        except KeyError as error:
            raise ContractViolationError(
                "Agent Run does not exist",
                code="child_run_not_found",
            ) from error

    def _require_active_run(self, run_id: str) -> AgentTreeRun:
        run = self._require_run(run_id)
        if run.status not in {
            AgentTreeRunStatus.RUNNING,
            AgentTreeRunStatus.WAITING,
        }:
            _raise("root_run_not_active", "Agent Run is not active")
        return run

    def require_run_claim_unlocked(
        self,
        run_id: str,
        lease_owner_id: str | None,
        lease_epoch: int | None,
    ) -> None:
        """Validate under the shared transaction lock used by memory adapters."""
        self._require_claim_unlocked(
            self._require_run(run_id),
            lease_owner_id,
            lease_epoch,
        )

    def _require_claim_unlocked(
        self,
        run: AgentTreeRun,
        lease_owner_id: str | None,
        lease_epoch: int | None,
    ) -> None:
        owner = str(lease_owner_id or "").strip() or None
        epoch = (
            None
            if lease_epoch is None
            else non_negative_int(lease_epoch, "Agent Run lease epoch")
        )
        if run.lease_owner_id is None:
            if owner is None and epoch is None:
                return
            _raise("agent_run_lease_lost", "Agent Run has no matching lease")
        if (
            owner != run.lease_owner_id
            or epoch != run.lease_epoch
            or self._lease_expired_unlocked(run)
        ):
            _raise("agent_run_lease_lost", "Agent Run lease is stale or expired")

    def _lease_expired_unlocked(
        self,
        run: AgentTreeRun,
        now_ms: int | None = None,
    ) -> bool:
        return (
            run.status in {
                AgentTreeRunStatus.RUNNING,
                AgentTreeRunStatus.WAITING,
            }
            and run.lease_expires_at_ms is not None
            and (self._now_ms() if now_ms is None else now_ms)
            >= run.lease_expires_at_ms
        )

    def _now_ms(self) -> int:
        return non_negative_int(self._clock_ms(), "Agent tree clock")

    def _has_active_run(self, agent_id: str) -> bool:
        return any(
            run.agent_id == agent_id and run.status in _ACTIVE_RUN_STATUSES
            for run in self._runs.values()
        )

    def _is_ancestor(self, ancestor_id: str, descendant_id: str) -> bool:
        current = self._require_agent(descendant_id)
        while True:
            if current.agent_id == ancestor_id:
                return True
            if current.parent_agent_id is None:
                return False
            current = self._require_agent(current.parent_agent_id)

    def _is_causal_descendant(self, parent_run_id: str, run_id: str) -> bool:
        current = self._require_run(run_id)
        while current.parent_run_id is not None:
            if current.parent_run_id == parent_run_id:
                return True
            current = self._require_run(current.parent_run_id)
        return False

    def _next_sequence(self) -> int:
        self._sequence += 1
        return self._sequence


def _digest(value: object) -> str:
    return sha256(json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


def _normalize_command_lease(command: object) -> None:
    owner = str(getattr(command, "lease_owner_id", None) or "").strip() or None
    epoch_value = getattr(command, "lease_epoch", None)
    epoch = (
        None
        if epoch_value is None
        else non_negative_int(epoch_value, "Agent Run lease epoch")
    )
    if (owner is None) != (epoch is None):
        raise ValueError("Agent Run lease owner and epoch must be provided together")
    object.__setattr__(command, "lease_owner_id", owner)
    object.__setattr__(command, "lease_epoch", epoch)


def _raise(code: str, message: str) -> None:
    raise ContractViolationError(message, code=code)


__all__ = [
    "AgentCapabilityGrant",
    "AgentNode",
    "AgentNodeState",
    "AgentRunAggregation",
    "AgentTreeRun",
    "AgentTreeRunStatus",
    "BeginRootAgentCommand",
    "ChildAgentSpec",
    "ContextCheckpoint",
    "ContinueAgentCommand",
    "ContinueAgentReceipt",
    "InMemoryRunTreeRepository",
    "RunTreeRepository",
    "SpawnAgentsCommand",
    "SpawnAgentsReceipt",
    "SpawnedAgent",
]
