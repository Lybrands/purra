"""Stable Core event and inbound command envelopes."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping

from purra.contracts import ApprovalDecision, RunId
from purra.normalization import required_text
from purra.json_values import freeze_json_mapping


class CoreEventType(StrEnum):
    RUN_STARTED = "run.started"
    RUN_TODOS_UPDATED = "run.todos_updated"
    RUN_TODO_UPDATED = "run.todo_updated"
    MODEL_CALL_RECORDED = "model.call_recorded"
    HOST_PLANNED_TOOL_DISPATCHED = "tool.host_planned_dispatched"
    TOOL_CALLS_STARTED = "tool.calls_started"
    TOOL_CALL_COMPLETED = "tool.call_completed"
    TOOL_RESULTS = "tool.results"
    TOOL_ROUND_COMPLETED = "tool.round_completed"
    AGENT_EXECUTION_CHECKPOINTED = "agent.execution_checkpointed"
    APPROVAL_REQUESTED = "approval.requested"
    APPROVAL_RESOLVED = "approval.resolved"
    DELEGATION_CREATED = "delegation.created"
    DELEGATION_CLAIMED = "delegation.claimed"
    DELEGATION_COMPLETED = "delegation.completed"
    DELEGATION_FAILED = "delegation.failed"
    DELEGATION_CANCELED = "delegation.canceled"
    CONTEXT_BUDGETED = "context.budgeted"
    CONTEXT_USAGE_RECORDED = "context.usage_recorded"
    TASK_ADMISSION_DECIDED = "task.admission_decided"
    LONG_TASK_DISPATCHED = "long_task.dispatched"
    LONG_TASK_PROGRESS = "long_task.progress"
    RUN_COMPLETED = "run.completed"
    RUN_BLOCKED = "run.blocked"
    RUN_FAILED = "run.failed"
    RUN_CANCELED = "run.canceled"


class CoreCommandType(StrEnum):
    APPROVAL_RESOLVE = "approval.resolve"
    RUN_CANCEL = "run.cancel"


@dataclass(frozen=True, slots=True)
class AgentEvent:
    """An event envelope; ``type`` remains open for domain-owned effects."""

    type: str
    payload: Mapping[str, Any] = field(default_factory=dict)
    run_id: RunId | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "type", required_text(self.type, "event type"))
        object.__setattr__(self, "payload", freeze_json_mapping(self.payload))


@dataclass(frozen=True, slots=True)
class AgentCommand:
    type: CoreCommandType
    payload: Mapping[str, Any] = field(default_factory=dict)
    run_id: RunId | None = None

    def __post_init__(self) -> None:
        command_type = (
            self.type
            if isinstance(self.type, CoreCommandType)
            else CoreCommandType(str(self.type))
        )
        object.__setattr__(self, "type", command_type)
        if not str(self.run_id or "").strip():
            raise ValueError(f"{command_type} command requires a run id")
        payload = dict(self.payload)
        if command_type is CoreCommandType.APPROVAL_RESOLVE:
            if not str(payload.get("approval_id") or "").strip():
                raise ValueError("approval.resolve requires approval_id")
            try:
                decision = ApprovalDecision(str(payload.get("decision") or ""))
            except ValueError:
                raise ValueError("approval.resolve requires decision")
            payload["decision"] = decision.value
        object.__setattr__(self, "payload", freeze_json_mapping(payload))


__all__ = [
    "AgentCommand",
    "AgentEvent",
    "CoreCommandType",
    "CoreEventType",
]
