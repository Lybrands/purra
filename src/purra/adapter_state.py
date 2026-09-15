"""Shared data state for reference adapters and transaction-scoped storage.

Repositories own behavior and locks; snapshots encode only persistent fields.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, fields
from typing import Any, Generic, TypeVar, TYPE_CHECKING

_Node = TypeVar("_Node")
_Run = TypeVar("_Run")
_Checkpoint = TypeVar("_Checkpoint")
_Spawn = TypeVar("_Spawn")
_Continue = TypeVar("_Continue")

if TYPE_CHECKING:
    from purra.adapter_records import StoredRun, StoredStream
    from purra.adapter_records import StoredLongTask
    from purra.contracts import ToolCall, ToolHandlerResult
    from purra.output.contracts import AgentOutputEvent
    from purra.artifacts.contracts import ArtifactRecord, ArtifactBatch, ArtifactBatchReceipt
    from purra.artifacts.continuity import ArtifactWriteClaim


@dataclass
class RunState:
    runs: dict[str, StoredRun] = field(default_factory=dict)
    streams: dict[str, StoredStream] = field(default_factory=dict)
    stream_by_invocation: dict[str, str] = field(default_factory=dict)
    sequences: dict[str, int] = field(default_factory=dict)
    root_sequences: dict[str, int] = field(default_factory=dict)
    published_sequences: dict[str, int] = field(default_factory=dict)
    tool_receipts: dict[tuple[str, str], tuple[ToolCall, ToolHandlerResult]] = field(default_factory=dict)
    run_count: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock, metadata={"persistent": False})
    changed: asyncio.Condition = field(default_factory=asyncio.Condition, metadata={"persistent": False})
    output_events: dict[str, list[AgentOutputEvent]] = field(default_factory=dict, metadata={"persistent": False})
    root_output_events: dict[str, list[AgentOutputEvent]] = field(default_factory=dict, metadata={"persistent": False})
    events_by_source_key: dict[str, AgentOutputEvent] = field(default_factory=dict, metadata={"persistent": False})
    tool_inflight: dict[tuple[str, str], tuple[ToolCall, asyncio.Task[ToolHandlerResult]]] = field(default_factory=dict, metadata={"persistent": False})
    run_tree_authority: Any = field(default=None, metadata={"persistent": False})

    def next_run_id(self) -> str:
        self.run_count += 1
        return f"memory-run-{self.run_count}"


@dataclass
class AgentTreeState(Generic[_Node, _Run, _Checkpoint, _Spawn, _Continue]):
    agents: dict[str, _Node] = field(default_factory=dict)
    runs: dict[str, _Run] = field(default_factory=dict)
    checkpoints: dict[str, _Checkpoint] = field(default_factory=dict)
    spawn_receipts: dict[tuple[str, str], tuple[str, _Spawn]] = field(default_factory=dict)
    continue_receipts: dict[tuple[str, str], tuple[str, _Continue]] = field(default_factory=dict)
    root_digests: dict[str, str] = field(default_factory=dict)
    sequence: int = 0
    agent_sequence: int = 0
    run_sequence: int = 0
    batch_sequence: int = 0
    checkpoint_sequence: int = 0


@dataclass
class ArtifactState:
    artifacts: dict[str, ArtifactRecord] = field(default_factory=dict)
    owners: dict[tuple[str, str, str, str, str], str] = field(default_factory=dict)
    batches: dict[str, list[ArtifactBatch]] = field(default_factory=dict)
    receipts: dict[tuple[str, str], ArtifactBatchReceipt] = field(default_factory=dict)
    receipt_digests: dict[tuple[str, str], str] = field(default_factory=dict)
    claims: dict[str, ArtifactWriteClaim] = field(default_factory=dict)
    updated_at_ms: dict[str, int] = field(default_factory=dict)


@dataclass
class LongTaskState:
    tasks: dict[str, StoredLongTask] = field(default_factory=dict)


@dataclass
class AdapterState:
    run: RunState = field(default_factory=RunState)
    tree: AgentTreeState = field(default_factory=AgentTreeState)
    artifact: ArtifactState = field(default_factory=ArtifactState)
    task: LongTaskState = field(default_factory=LongTaskState)

    def to_groups(self) -> dict[str, dict[str, object]]:
        groups = {}
        for item in fields(self):
            group = getattr(self, item.name)
            groups[item.name] = {
                entry.name: getattr(group, entry.name)
                for entry in fields(group) if entry.metadata.get("persistent", True)
            }
        return groups

    @classmethod
    def from_groups(cls, groups: object) -> AdapterState:
        state = cls()
        if not isinstance(groups, dict) or set(groups) != {item.name for item in fields(state)}:
            raise ValueError("invalid storage groups")
        for item in fields(state):
            group = getattr(state, item.name)
            values = groups[item.name]
            names = {entry.name for entry in fields(group) if entry.metadata.get("persistent", True)}
            if not isinstance(values, dict) or set(values) != names:
                raise ValueError("invalid storage fields")
            for name, value in values.items():
                expected = getattr(group, name)
                if type(value) is not type(expected) or (type(expected) is int and value < 0):
                    raise ValueError("invalid storage field type")
            setattr(state, item.name, type(group)(**values))
        return state
