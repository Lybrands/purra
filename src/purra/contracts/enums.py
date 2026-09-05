"""Foundational identifiers and enums shared across PurrA contracts."""

from __future__ import annotations

from enum import StrEnum
from typing import Literal, TypeAlias


RunId: TypeAlias = str
SessionId: TypeAlias = str | int


class RunStatus(StrEnum):
    RUNNING = "running"
    DONE = "done"
    BLOCKED = "blocked"
    FAILED = "failed"
    CANCELED = "canceled"


TerminalRunStatus: TypeAlias = Literal[
    RunStatus.DONE,
    RunStatus.BLOCKED,
    RunStatus.FAILED,
    RunStatus.CANCELED,
]


class StepStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    BLOCKED = "blocked"
    FAILED = "failed"


class StepExecutor(StrEnum):
    MODEL = "model"
    TOOL = "tool"


class StepType(StrEnum):
    READ = "read"
    ANALYZE = "analyze"
    WRITE = "write"
    REVIEW = "review"
    CONFIRM = "confirm"


class ToolExecutionMode(StrEnum):
    READ = "read"
    PROPOSE = "propose"
    CONFIRM = "confirm"


class ToolEffectState(StrEnum):
    """Whether a failed tool batch could already have changed host state."""

    NOT_STARTED = "not_started"
    COMMITTED = "committed"
    UNKNOWN = "unknown"


class ToolRiskLevel(StrEnum):
    READ = "read"
    WRITE = "write"
    DESTRUCTIVE = "destructive"


class ApprovalDecision(StrEnum):
    APPROVE = "approve"
    REJECT = "reject"


class ApprovalStatus(StrEnum):
    APPROVED = "approved"
    REJECTED = "rejected"
    TIMED_OUT = "timed_out"
    CANCELED = "canceled"
    UNAVAILABLE = "unavailable"


class MessageRole(StrEnum):
    SYSTEM = "system"
    DEVELOPER = "developer"
    USER = "user"
    ASSISTANT = "assistant"
    TOOL = "tool"


class MessageOrigin(StrEnum):
    """Internal provenance that provider-shaped mappings cannot set."""

    CALLER = "caller"
    HOST_CONTEXT = "host_context"
    MODEL = "model"
    HOST_TOOL_RESULT = "host_tool_result"


class ToolChoiceMode(StrEnum):
    NONE = "none"
    AUTO = "auto"
    REQUIRED = "required"


class ReasoningMode(StrEnum):
    DEFAULT = "default"
    ENABLED = "enabled"
    DISABLED = "disabled"


class ModelFinishReason(StrEnum):
    STOP = "stop"
    LENGTH = "length"
    TOOL_CALLS = "tool_calls"
    FILTERED = "filtered"
    OTHER = "other"


class ToolBatchOutcome(StrEnum):
    PROGRESSED = "progressed"
    COMPLETED = "completed"
    DECLINED = "declined"
    CANCELED = "canceled"
    REJECTED = "rejected"
    FAILED = "failed"


class ToolStepDisposition(StrEnum):
    """Whether a successful tool result satisfies the active plan step."""

    CONTINUE = "continue"
    COMPLETE = "complete"


class ToolPlanningDisposition(StrEnum):
    """Whether a successful host tool result invalidates the future plan."""

    KEEP_PLAN = "keep_plan"
    REPLAN = "replan"


class ToolPlanningRequirement(StrEnum):
    """Whether a runtime tool may execute before a governed plan exists."""

    OPTIONAL = "optional"
    REQUIRED = "required"


class RuntimeOutcome(StrEnum):
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class PlanningMode(StrEnum):
    """Per-Run strategy for adaptive, direct, or governed execution."""

    AUTO = "auto"
    REACTIVE = "reactive"
    PLANNED = "planned"


class PlanningKind(StrEnum):
    PLANNED = "planned"
    DIRECT_RESPONSE = "direct_response"
