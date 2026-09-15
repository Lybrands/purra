"""Run contracts: intents, leases, provenance, limits, and results."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping

from purra.contracts.enums import (
    RunId,
    RunStatus,
    RuntimeOutcome,
    SessionId,
    StepStatus,
)
from purra.contracts.host import RunBinding
from purra.json_values import freeze_json_mapping
from purra.normalization import (
    non_negative_int,
    optional_non_negative_int,
    optional_positive_int,
    optional_text as _optional_text,
    positive_int,
    required_text,
)

@dataclass(frozen=True, slots=True)
class RunExecutionIntent:
    """Immutable user and host intent shared by every attempt of one Run."""

    requested_reasoning_mode: Literal["default", "enabled", "disabled"]
    output_contract: str
    tool_protocol_contract: str
    recovery_policy_id: str
    capability_snapshot_digest: str
    requested_user_max_generation_tokens: int | None
    result_capacity_target_tokens: int | None

    def __post_init__(self) -> None:
        mode = str(self.requested_reasoning_mode or "").strip().lower()
        if mode not in {"default", "enabled", "disabled"}:
            raise ValueError(
                "requested reasoning mode must be default, enabled or disabled"
            )
        object.__setattr__(self, "requested_reasoning_mode", mode)
        for name in (
            "output_contract",
            "tool_protocol_contract",
            "recovery_policy_id",
        ):
            object.__setattr__(self, name, required_text(
                getattr(self, name), f"run execution intent {name}"
            ))
        digest = str(self.capability_snapshot_digest or "").strip().lower()
        if len(digest) != 64 or any(
            char not in "0123456789abcdef" for char in digest
        ):
            raise ValueError(
                "run execution intent capability snapshot must be a SHA-256 digest"
            )
        object.__setattr__(self, "capability_snapshot_digest", digest)
        for name in (
            "requested_user_max_generation_tokens",
            "result_capacity_target_tokens",
        ):
            object.__setattr__(
                self,
                name,
                optional_positive_int(
                    getattr(self, name),
                    f"run execution intent {name}",
                ),
            )


@dataclass(frozen=True, slots=True)
class RunProvenance:
    """Immutable, non-secret identity of the model request behind a Run."""

    model_provider: str
    model_name: str
    context_window: int
    endpoint_digest: str
    request_profile_digest: str
    capability_snapshot: Mapping[str, Any] = field(default_factory=dict)
    execution_intent: RunExecutionIntent | None = None

    def __post_init__(self) -> None:
        for name in ("model_provider", "model_name"):
            object.__setattr__(self, name, required_text(
                getattr(self, name), f"run provenance {name}"
            ))
        object.__setattr__(self, "context_window", positive_int(
            self.context_window, "run provenance context_window"
        ))
        for name in ("endpoint_digest", "request_profile_digest"):
            value = str(getattr(self, name) or "").strip().lower()
            if len(value) != 64 or any(
                char not in "0123456789abcdef"
                for char in value
            ):
                raise ValueError(f"run provenance {name} must be a SHA-256 digest")
            object.__setattr__(self, name, value)
        object.__setattr__(
            self,
            "capability_snapshot",
            freeze_json_mapping(self.capability_snapshot),
        )
        if self.execution_intent is not None and not isinstance(
            self.execution_intent,
            RunExecutionIntent,
        ):
            raise TypeError(
                "run provenance execution_intent must be a RunExecutionIntent"
            )
        if self.capability_snapshot and self.execution_intent is not None:
            snapshot_digest = str(
                self.capability_snapshot.get("digest") or ""
            ).strip().lower()
            if snapshot_digest != self.execution_intent.capability_snapshot_digest:
                raise ValueError(
                    "run provenance capability snapshot digest does not match intent"
                )


@dataclass(frozen=True, slots=True)
class RunExecutionLease:
    run_id: RunId
    status: RunStatus
    owner_id: str | None = None
    expires_at_ms: int | None = None
    heartbeat_at_ms: int | None = None
    attempt: int = 0
    cancellation_requested_at_ms: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", required_text(
            self.run_id, "execution lease run id"
        ))
        object.__setattr__(self, "status", RunStatus(self.status))
        object.__setattr__(self, "owner_id", _optional_text(self.owner_id))
        object.__setattr__(self, "attempt", max(0, int(self.attempt)))


@dataclass(frozen=True, slots=True)
class RunCreateParams:
    session_id: SessionId | None
    prompt: str
    mode: str | None
    provenance: RunProvenance | None = None
    binding: RunBinding | None = None
    turn_id: str | None = None
    deadline_at_ms: int | None = None
    runtime_limits: "RuntimeLimits" = field(
        default_factory=lambda: RuntimeLimits(
            max_run_generation_tokens=None,
        )
    )
    agent_preset_snapshot: Mapping[str, Any] = field(default_factory=dict)
    requested_user_max_generation_tokens: int | None = None
    result_capacity_target_tokens: int | None = None
    selected_context_window_tokens: int | None = None
    requested_run_id: RunId | None = None
    root_run_id: RunId | None = None
    agent_id: str | None = None
    parent_run_id: RunId | None = None
    lease_owner_id: str | None = None
    lease_epoch: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "prompt", str(self.prompt or ""))
        object.__setattr__(self, "mode", _optional_text(self.mode))
        object.__setattr__(
            self,
            "requested_run_id",
            _optional_text(self.requested_run_id),
        )
        for name in (
            "root_run_id",
            "agent_id",
            "parent_run_id",
            "lease_owner_id",
        ):
            object.__setattr__(self, name, _optional_text(getattr(self, name)))
        object.__setattr__(
            self,
            "lease_epoch",
            (
                None
                if self.lease_epoch is None
                else non_negative_int(self.lease_epoch, "Run lease_epoch")
            ),
        )
        if (self.lease_owner_id is None) != (self.lease_epoch is None):
            raise ValueError("Run lease owner and epoch must be provided together")
        if self.parent_run_id is not None and self.root_run_id is None:
            raise ValueError("Child Run scope requires root_run_id")
        object.__setattr__(self, "turn_id", _optional_text(self.turn_id))
        object.__setattr__(
            self,
            "deadline_at_ms",
            optional_positive_int(self.deadline_at_ms, "Run deadline_at_ms"),
        )
        for name in (
            "requested_user_max_generation_tokens",
            "result_capacity_target_tokens",
            "selected_context_window_tokens",
        ):
            object.__setattr__(
                self,
                name,
                optional_positive_int(
                    getattr(self, name),
                    f"Run {name}",
                ),
            )
        if not isinstance(self.runtime_limits, RuntimeLimits):
            raise TypeError("Run runtime_limits must be RuntimeLimits")
        if self.provenance is not None and not isinstance(
            self.provenance,
            RunProvenance,
        ):
            raise TypeError("run provenance must be a RunProvenance value")
        if self.binding is not None and not isinstance(self.binding, RunBinding):
            raise TypeError("run binding must be a RunBinding value")
        object.__setattr__(
            self,
            "agent_preset_snapshot",
            freeze_json_mapping(self.agent_preset_snapshot),
        )


@dataclass(frozen=True, slots=True)
class TaskStepUpdate:
    step_id: str
    status: StepStatus
    result_summary: str | None = None
    error: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "step_id", required_text(
            self.step_id, "task step update id"
        ))
        object.__setattr__(self, "status", StepStatus(self.status))


@dataclass(frozen=True, slots=True)
class TraceRecord:
    stage: str
    outcome: str
    details: Mapping[str, Any] = field(default_factory=dict)
    duration_ms: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "stage", required_text(
            self.stage, "trace stage"
        ))
        object.__setattr__(self, "outcome", required_text(
            self.outcome, "trace outcome"
        ))
        object.__setattr__(self, "details", freeze_json_mapping(self.details))
        object.__setattr__(self, "duration_ms", optional_non_negative_int(
            self.duration_ms, "trace duration"
        ))


@dataclass(frozen=True, slots=True)
class AgentRunResult:
    run_id: RunId
    status: RunStatus
    final_response: str = ""
    error: str | None = None
    model: str | None = None
    validated_result: str | None = None

    def __post_init__(self) -> None:
        run_id = required_text(self.run_id, "agent run result run id")
        status = RunStatus(self.status)
        if status is RunStatus.RUNNING:
            raise ValueError("agent run result must be terminal")
        object.__setattr__(self, "run_id", run_id)
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "final_response", str(self.final_response or ""))
        object.__setattr__(self, "error", _optional_text(self.error))
        object.__setattr__(self, "model", _optional_text(self.model))
        object.__setattr__(
            self,
            "validated_result",
            (
                None
                if self.validated_result is None
                else str(self.validated_result)
            ),
        )


@dataclass(frozen=True, slots=True)
class RuntimeLimits:
    """Bound stalled execution separately from monotonic partial progress.

    ``max_model_rounds`` is the base budget for planned transitions,
    corrections, and the final response. A ``PROGRESSED`` tool result has a
    stronger contract: it committed valid partial work while keeping the same
    plan step active. Such rounds may unlock the separately bounded progress
    allowance without turning malformed or stalled loops into unbounded runs.

    ``max_run_generation_tokens`` covers all model invocations charged
    to one Run. ``None`` is an explicit choice to leave that cumulative token
    budget without a finite ceiling; it is not a per-invocation model limit.
    """

    max_run_generation_tokens: int | None
    max_model_rounds: int = 6
    max_progress_rounds: int = 32
    provider_activity_idle_timeout_ms: int | None = 30_000
    provider_progress_idle_timeout_ms: int | None = 60_000
    provider_invocation_timeout_ms: int | None = 300_000
    root_run_timeout_ms: int | None = 900_000
    max_model_invocation_attempts: int = 64
    max_input_tokens: int | None = None
    max_reasoning_tokens: int | None = None
    max_provider_output_events: int = 10_000
    max_provider_output_bytes: int = 8 * 1024 * 1024
    max_stream_content_chars: int = 1_000_000
    max_stream_reasoning_chars: int = 1_000_000
    max_stream_chunks: int = 100_000

    def __post_init__(self) -> None:
        object.__setattr__(self, "max_model_rounds", positive_int(
            self.max_model_rounds, "max model rounds"
        ))
        object.__setattr__(
            self,
            "max_progress_rounds",
            non_negative_int(self.max_progress_rounds, "max progress rounds"),
        )
        for name in (
            "max_model_invocation_attempts",
            "max_provider_output_events",
            "max_provider_output_bytes",
            "max_stream_content_chars",
            "max_stream_reasoning_chars",
            "max_stream_chunks",
        ):
            object.__setattr__(
                self,
                name,
                positive_int(getattr(self, name), name.replace("_", " ")),
            )
        for name in (
            "provider_activity_idle_timeout_ms",
            "provider_progress_idle_timeout_ms",
            "provider_invocation_timeout_ms",
            "root_run_timeout_ms",
        ):
            object.__setattr__(
                self,
                name,
                optional_positive_int(getattr(self, name), name.replace("_", " ")),
            )
        for name in (
            "max_input_tokens",
            "max_run_generation_tokens",
            "max_reasoning_tokens",
        ):
            object.__setattr__(
                self,
                name,
                optional_positive_int(getattr(self, name), name.replace("_", " ")),
            )


@dataclass(frozen=True, slots=True)
class AgentRuntimeResult:
    run_id: RunId | None
    outcome: RuntimeOutcome
    final_response: str
    model: str
    round_count: int
    error_code: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "outcome", RuntimeOutcome(self.outcome))
        object.__setattr__(self, "final_response", str(self.final_response or ""))
        object.__setattr__(self, "model", str(self.model or ""))
        object.__setattr__(self, "round_count", max(0, int(self.round_count)))
        object.__setattr__(self, "error_code", _optional_text(self.error_code))
