"""Immutable contracts for every PurrA-owned Provider invocation."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field

from purra.contracts import (
    ModelCompletion,
    ModelRequest,
    ModelStreamChunk,
    ReasoningMode,
    RunId,
    ToolChoiceMode,
    ToolSchema,
)
from purra.json_values import freeze_json_mapping
from purra.json_values import thaw_json_mapping
from purra.evidence import ContextEvidenceReceipt
from purra.model_protocol import (
    InvocationOutputLimit,
    resolve_invocation_output_limit,
)
from purra.normalization import optional_positive_int, optional_text, required_text
from purra.output.contracts import AgentOutputIntent, OutputCommitMode


@dataclass(frozen=True, slots=True)
class ModelInvocationContext:
    run_id: RunId
    turn_id: str | None = None
    deadline_at_ms: int | None = None
    deadline_code: str = "run_deadline_exceeded"
    attempt_source_key: str | None = None
    tool_argument_limits: Mapping[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", required_text(self.run_id, "run id"))
        object.__setattr__(self, "turn_id", optional_text(self.turn_id))
        object.__setattr__(
            self,
            "attempt_source_key",
            optional_text(self.attempt_source_key),
        )
        object.__setattr__(
            self,
            "deadline_at_ms",
            optional_positive_int(
                self.deadline_at_ms,
                "model invocation parent deadline_at_ms",
            ),
        )
        object.__setattr__(
            self,
            "deadline_code",
            required_text(self.deadline_code, "model invocation parent deadline code"),
        )
        limits = {
            required_text(name, "tool argument limit name"): int(limit)
            for name, limit in self.tool_argument_limits.items()
        }
        if any(limit <= 0 for limit in limits.values()):
            raise ValueError("tool argument limits must be positive")
        object.__setattr__(self, "tool_argument_limits", freeze_json_mapping(limits))


@dataclass(frozen=True, slots=True)
class AgentModelCall:
    request: ModelRequest
    output_intent: AgentOutputIntent
    commit_mode: OutputCommitMode
    requires_full_text_validation: bool = False
    reasoning_mode: ReasoningMode = ReasoningMode.DEFAULT
    output_limit: InvocationOutputLimit | None = None
    tools: tuple[ToolSchema, ...] = ()
    tool_choice: ToolChoiceMode = ToolChoiceMode.NONE

    def __post_init__(self) -> None:
        if not isinstance(self.request, ModelRequest):
            raise TypeError("agent model call requires a ModelRequest")
        intent = AgentOutputIntent(self.output_intent)
        commit_mode = OutputCommitMode(self.commit_mode)
        if intent in {
            AgentOutputIntent.EXECUTION_PUBLIC,
            AgentOutputIntent.FINAL_PUBLIC,
        } and commit_mode is not OutputCommitMode.LIVE:
            raise ValueError("public output intent must be live")
        if intent in {
            AgentOutputIntent.STRUCTURED_PRIVATE,
            AgentOutputIntent.REASONING_PRIVATE,
        } and commit_mode is OutputCommitMode.LIVE:
            raise ValueError("private output intent cannot be live")
        if not isinstance(self.requires_full_text_validation, bool):
            raise TypeError("full-text validation flag must be a boolean")
        limit = self.output_limit or resolve_invocation_output_limit(
            self.request.capability_snapshot,
            self.request.options.get("max_tokens"),
        )
        if not isinstance(limit, InvocationOutputLimit):
            raise TypeError("agent model call requires an InvocationOutputLimit")
        tools = tuple(self.tools)
        tool_choice = ToolChoiceMode(self.tool_choice)
        if not tools and tool_choice is ToolChoiceMode.REQUIRED:
            raise ValueError("required tool choice needs at least one tool")
        object.__setattr__(self, "output_intent", intent)
        object.__setattr__(self, "commit_mode", commit_mode)
        object.__setattr__(self, "reasoning_mode", ReasoningMode(self.reasoning_mode))
        object.__setattr__(self, "output_limit", limit)
        object.__setattr__(self, "tools", tools)
        object.__setattr__(self, "tool_choice", tool_choice)


@dataclass(frozen=True, slots=True)
class ModelInvocationReceipt:
    invocation_id: str
    output_stream_id: str
    run_id: RunId
    turn_id: str | None
    model: str
    output_intent: AgentOutputIntent
    commit_mode: OutputCommitMode
    output_limit: InvocationOutputLimit
    input_fingerprint: str
    tool_schema_fingerprint: str
    budget_key: str | None = None
    context_evidence: tuple[ContextEvidenceReceipt, ...] = field(
        default_factory=tuple
    )
    call_parameters: tuple[Mapping[str, object], ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "invocation_id",
            required_text(self.invocation_id, "invocation id"),
        )
        object.__setattr__(
            self,
            "output_stream_id",
            required_text(self.output_stream_id, "output stream id"),
        )
        object.__setattr__(self, "run_id", required_text(self.run_id, "run id"))
        object.__setattr__(self, "turn_id", optional_text(self.turn_id))
        object.__setattr__(self, "model", required_text(self.model, "model"))
        object.__setattr__(
            self,
            "output_intent",
            AgentOutputIntent(self.output_intent),
        )
        object.__setattr__(self, "commit_mode", OutputCommitMode(self.commit_mode))
        if not isinstance(self.output_limit, InvocationOutputLimit):
            raise TypeError("invocation receipt requires an output limit")
        object.__setattr__(
            self,
            "input_fingerprint",
            required_text(self.input_fingerprint, "model input fingerprint"),
        )
        object.__setattr__(
            self,
            "tool_schema_fingerprint",
            required_text(
                self.tool_schema_fingerprint,
                "model tool schema fingerprint",
            ),
        )
        object.__setattr__(self, "budget_key", optional_text(self.budget_key))
        evidence = tuple(self.context_evidence)
        if not all(isinstance(item, ContextEvidenceReceipt) for item in evidence):
            raise TypeError(
                "invocation context evidence must be ContextEvidenceReceipt values"
            )
        object.__setattr__(self, "context_evidence", evidence)
        object.__setattr__(
            self,
            "call_parameters",
            tuple(freeze_json_mapping(item) for item in self.call_parameters),
        )

    def to_mapping(self) -> dict[str, object]:
        return {
            "invocationId": self.invocation_id,
            "outputStreamId": self.output_stream_id,
            "runId": self.run_id,
            "turnId": self.turn_id,
            "model": self.model,
            "outputIntent": self.output_intent.value,
            "commitMode": self.commit_mode.value,
            "outputLimit": self.output_limit.to_mapping(),
            "inputFingerprint": self.input_fingerprint,
            "toolSchemaFingerprint": self.tool_schema_fingerprint,
            "budgetKey": self.budget_key or self.invocation_id,
            "contextEvidence": [
                item.to_mapping() for item in self.context_evidence
            ],
            "callParameters": [
                thaw_json_mapping(item) for item in self.call_parameters
            ],
        }


@dataclass(frozen=True, slots=True)
class ManagedInvocationStream:
    chunks: AsyncIterator[ModelStreamChunk]
    receipt: ModelInvocationReceipt


@dataclass(frozen=True, slots=True)
class ManagedInvocationCompletion:
    completion: ModelCompletion
    receipt: ModelInvocationReceipt


__all__ = [name for name in globals() if not name.startswith("_")]
