"""Stable recursive Agent identities and immutable Run chains."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import StrEnum
from graphlib import CycleError, TopologicalSorter
from typing import Any

from purra.errors import ContractViolationError
from purra.json_values import freeze_json_mapping, thaw_json_mapping
from purra.normalization import (
    non_negative_int,
    positive_int,
    required_text,
    unique_text_tuple,
)

def _dependency_names(values):
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        raise TypeError("dependencies must be a sequence of names")
    names = tuple(required_text(value, "dependency") for value in values)
    if len(set(names)) != len(names):
        raise ValueError("dependencies must be unique")
    return names


def validate_stored_dependencies(runs: Mapping[str, "AgentTreeRun"]) -> None:
    for identity, run in runs.items():
        if identity != run.run_id:
            raise ValueError("stored dependency Run identity mismatch")
        for dependency_id in run.dependency_run_ids:
            dependency = runs.get(dependency_id)
            if (dependency is None or run.parent_run_id is None
                    or dependency.parent_run_id != run.parent_run_id
                    or dependency.root_run_id != run.root_run_id):
                raise ValueError("stored dependency is outside its sibling scope")
    try:
        tuple(TopologicalSorter({identity: run.dependency_run_ids for identity, run in runs.items()}).static_order())
    except CycleError as error:
        raise ValueError("stored dependency graph is cyclic") from error


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
    dependency_run_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "dependency_run_ids", _dependency_names(self.dependency_run_ids))
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
    depends_on: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "depends_on", _dependency_names(self.depends_on))
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
            "dependsOn": list(self.depends_on),
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
        names = {item.name for item in children}
        if any(set(item.depends_on) - names for item in children):
            raise ValueError("Child dependency must name a sibling in the spawn command")
        try:
            tuple(TopologicalSorter({item.name: item.depends_on for item in children}).static_order())
        except CycleError as error:
            raise ValueError("Child dependencies must be acyclic") from error
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
    "SpawnAgentsCommand",
    "SpawnAgentsReceipt",
    "SpawnedAgent",
    "validate_stored_dependencies",
]


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
