"""Tool contracts: calls, schemas, policies, batches, and results."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping, MutableMapping

from purra.contracts.enums import (
    ApprovalStatus,
    RunId,
    ToolBatchOutcome,
    ToolEffectState,
    ToolExecutionMode,
    ToolPlanningDisposition,
    ToolRiskLevel,
    ToolStepDisposition,
)
from purra.json_values import freeze_json_mapping
from purra.normalization import (
    optional_text as _optional_text,
    positive_int,
    required_text,
    text_frozenset,
    unique_text_tuple,
)
from purra.contracts.context import ContextEvidenceReceipt
from purra.contracts.tool_paths import tool_data_path as _tool_data_path

@dataclass(frozen=True, slots=True)
class ToolCall:
    id: str
    name: str
    arguments_json: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", required_text(self.id, "tool call id"))
        object.__setattr__(self, "name", required_text(
            self.name, "tool call name"
        ))
        object.__setattr__(self, "arguments_json", str(self.arguments_json or ""))


@dataclass(frozen=True, slots=True)
class ToolSchema:
    name: str
    description: str
    parameters: Mapping[str, Any]
    display_names: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        name = required_text(self.name, "tool schema name")
        display_names: dict[str, str] = {}
        for raw_locale, raw_display_name in self.display_names.items():
            locale = normalize_locale_tag(raw_locale)
            display_name = str(raw_display_name or "").strip()
            if not display_name:
                raise ValueError(
                    f"tool display name for {locale!r} must not be empty"
                )
            if locale in display_names:
                raise ValueError(
                    f"duplicate normalized tool display locale: {locale}"
                )
            display_names[locale] = display_name
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "description", str(self.description or ""))
        object.__setattr__(
            self,
            "parameters",
            freeze_json_mapping(self.parameters),
        )
        object.__setattr__(
            self,
            "display_names",
            freeze_json_mapping(display_names),
        )


def normalize_locale_tag(value: Any) -> str:
    parts = [
        part
        for part in str(value or "").strip().replace("_", "-").split("-")
        if part
    ]
    if not parts or any(not part.isalnum() for part in parts):
        raise ValueError(f"invalid locale tag: {value!r}")
    normalized = [parts[0].lower()]
    for part in parts[1:]:
        if len(part) in {2, 3}:
            normalized.append(part.upper())
        elif len(part) == 4:
            normalized.append(part.title())
        else:
            normalized.append(part.lower())
    return "-".join(normalized)


class ToolPayloadMode(StrEnum):
    """How model-generated data is committed by a registered tool."""

    INLINE = "inline"
    DELTA = "delta"
    BATCH = "batch"
    RESOURCE_REFERENCE = "resource_reference"


@dataclass(frozen=True, slots=True)
class ToolDataContract:
    """Declare authority boundaries without exposing host state to the model.

    Paths use a compact dotted form. ``items[].id`` addresses a property of an
    array item. Model-owned paths must be present in the model-visible JSON
    Schema. Host-bound and host-derived paths must be absent from it; adapters
    bind or calculate those values after model input validation.

    An empty path declaration means every model-visible Schema field is
    model-owned, with no host-bound or host-derived payload fields.
    """

    model_owned_paths: tuple[str, ...] = ()
    host_bound_paths: tuple[str, ...] = ()
    host_derived_paths: tuple[str, ...] = ()
    payload_mode: ToolPayloadMode = ToolPayloadMode.INLINE

    def __post_init__(self) -> None:
        groups: dict[str, tuple[str, ...]] = {}
        for name in (
            "model_owned_paths",
            "host_bound_paths",
            "host_derived_paths",
        ):
            values = tuple(dict.fromkeys(
                _tool_data_path(value)
                for value in getattr(self, name)
            ))
            object.__setattr__(self, name, values)
            groups[name] = values
        object.__setattr__(
            self,
            "payload_mode",
            ToolPayloadMode(self.payload_mode),
        )
        seen: dict[str, str] = {}
        for group, paths in groups.items():
            for path in paths:
                previous = seen.setdefault(path, group)
                if previous != group:
                    raise ValueError(
                        f"tool data path {path!r} has conflicting owners"
                    )


@dataclass(frozen=True, slots=True)
class ToolPolicy:
    mode: ToolExecutionMode
    title: str
    risk_level: ToolRiskLevel = ToolRiskLevel.READ

    def __post_init__(self) -> None:
        object.__setattr__(self, "mode", ToolExecutionMode(self.mode))
        object.__setattr__(self, "title", required_text(
            self.title, "tool policy title"
        ))
        object.__setattr__(self, "risk_level", ToolRiskLevel(self.risk_level))

    @property
    def requires_user_approval(self) -> bool:
        return self.mode is ToolExecutionMode.CONFIRM


class ToolResultProjection(StrEnum):
    """How a completed result may be represented in later model rounds."""

    FULL = "full"
    RECEIPT = "receipt"


@dataclass(frozen=True, slots=True)
class ToolContextContract:
    """Host-owned dependency and evidence lifecycle for one tool.

    ``FULL`` is deliberately the default projection.  A tool result may only
    be compacted to a receipt after its domain contract opts in, preventing a
    context optimization from silently removing evidence.
    """

    prerequisite_tools: tuple[str, ...] = ()
    mandatory_context_keys: tuple[str, ...] = ()
    required_context_blocks: tuple[str, ...] = ()
    evidence_kinds: tuple[str, ...] = ()
    produces: tuple[str, ...] = ()
    result_projection: ToolResultProjection = ToolResultProjection.FULL
    final_projection: ToolResultProjection = ToolResultProjection.FULL

    def __post_init__(self) -> None:
        for field_name in (
            "prerequisite_tools",
            "mandatory_context_keys",
            "required_context_blocks",
            "evidence_kinds",
            "produces",
        ):
            object.__setattr__(
                self,
                field_name,
                unique_text_tuple(getattr(self, field_name)),
            )
        object.__setattr__(
            self,
            "result_projection",
            ToolResultProjection(self.result_projection),
        )
        object.__setattr__(
            self,
            "final_projection",
            ToolResultProjection(self.final_projection),
        )


@dataclass(frozen=True, slots=True)
class DomainEffect:
    """A domain-owned effect emitted without depending on a host transport."""

    type: str
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "type", required_text(
            self.type, "domain effect type"
        ))
        object.__setattr__(self, "payload", freeze_json_mapping(self.payload))


@dataclass(frozen=True, slots=True)
class ToolHandlerResult:
    content: str
    from_cache: bool = False
    effects: tuple[DomainEffect, ...] = ()
    context_evidence: tuple[ContextEvidenceReceipt, ...] = ()
    error_code: str | None = None
    step_disposition: ToolStepDisposition = ToolStepDisposition.COMPLETE
    planning_disposition: ToolPlanningDisposition = (
        ToolPlanningDisposition.KEEP_PLAN
    )
    effect_state: ToolEffectState = ToolEffectState.UNKNOWN

    def __post_init__(self) -> None:
        object.__setattr__(self, "content", str(self.content or ""))
        object.__setattr__(self, "from_cache", bool(self.from_cache))
        object.__setattr__(self, "effects", tuple(self.effects))
        evidence = tuple(self.context_evidence)
        if not all(isinstance(item, ContextEvidenceReceipt) for item in evidence):
            raise TypeError(
                "tool context evidence must contain ContextEvidenceReceipt values"
            )
        object.__setattr__(self, "context_evidence", evidence)
        object.__setattr__(self, "error_code", _optional_text(self.error_code))
        object.__setattr__(
            self,
            "step_disposition",
            ToolStepDisposition(self.step_disposition),
        )
        object.__setattr__(
            self,
            "planning_disposition",
            ToolPlanningDisposition(self.planning_disposition),
        )
        object.__setattr__(
            self,
            "effect_state",
            ToolEffectState(self.effect_state),
        )


@dataclass(frozen=True, slots=True)
class ToolExecutionLimits:
    max_calls_per_batch: int = 8
    # Raw JSON is bounded before parsing only as a configurable memory-safety
    # envelope. Model-visible semantic limits belong to each tool's schema and
    # are enforced after JSON decoding, so escaping and whitespace cannot
    # consume an unrelated 32K workflow budget.
    max_argument_chars: int = 1_000_000
    max_result_chars: int = 64_000
    approval_timeout_seconds: float = 300.0
    approval_summary_chars: int = 420
    max_concurrency: int = 1

    def __post_init__(self) -> None:
        for name in (
            "max_calls_per_batch",
            "max_argument_chars",
            "max_result_chars",
            "approval_summary_chars",
        ):
            object.__setattr__(self, name, positive_int(
                getattr(self, name), name
            ))
        if type(self.max_concurrency) is not int or not 1 <= self.max_concurrency <= 2**53 - 1:
            raise ValueError("max_concurrency must be a positive safe integer")
        timeout = float(self.approval_timeout_seconds)
        if timeout <= 0:
            raise ValueError("approval_timeout_seconds must be positive")
        object.__setattr__(self, "approval_timeout_seconds", timeout)


@dataclass(slots=True)
class ExecutionState:
    """Run-scoped mutable state owned by the active domain adapter.

    Core authorization such as the current tool allow-list must never be stored
    here because a domain handler can mutate this mapping.
    """

    domain: MutableMapping[str, Any] = field(default_factory=dict)
    # Bound by AgentCore after Run creation. It is contextual identity for
    # run-aware host tools, never a source of authorization.
    run_id: RunId | None = None


@dataclass(frozen=True, slots=True)
class ToolBatchRequest:
    run_id: RunId | None
    calls: tuple[ToolCall, ...]
    allowed_tool_names: frozenset[str]
    state: ExecutionState
    invocation_id: str | None = None
    retry_of_tool_call_ids: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "calls", tuple(self.calls))
        object.__setattr__(
            self,
            "allowed_tool_names",
            text_frozenset(self.allowed_tool_names),
        )
        if not self.calls:
            raise ValueError("tool batch requires at least one call")
        object.__setattr__(
            self,
            "invocation_id",
            _optional_text(self.invocation_id),
        )
        call_ids = {call.id for call in self.calls}
        retry_links = {
            required_text(call_id, "retry tool call id"): required_text(
                retry_of,
                "retried tool call id",
            )
            for call_id, retry_of in self.retry_of_tool_call_ids.items()
        }
        if not retry_links.keys() <= call_ids:
            raise ValueError("tool retry links must target calls in the batch")
        if any(call_id == retry_of for call_id, retry_of in retry_links.items()):
            raise ValueError("a tool call cannot retry itself")
        object.__setattr__(
            self,
            "retry_of_tool_call_ids",
            freeze_json_mapping(retry_links),
        )


@dataclass(frozen=True, slots=True)
class ToolCallResult:
    tool_call_id: str
    tool_name: str
    content: str
    from_cache: bool = False
    approval_status: ApprovalStatus | None = None
    error: str | None = None
    effects: tuple[DomainEffect, ...] = ()
    context_evidence: tuple[ContextEvidenceReceipt, ...] = ()
    step_disposition: ToolStepDisposition = ToolStepDisposition.COMPLETE
    planning_disposition: ToolPlanningDisposition = (
        ToolPlanningDisposition.KEEP_PLAN
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "tool_call_id", required_text(
            self.tool_call_id, "tool result call id"
        ))
        object.__setattr__(self, "tool_name", required_text(
            self.tool_name, "tool result name"
        ))
        object.__setattr__(self, "content", str(self.content or ""))
        if self.approval_status is not None:
            object.__setattr__(
                self,
                "approval_status",
                ApprovalStatus(self.approval_status),
            )
        object.__setattr__(self, "error", _optional_text(self.error))
        object.__setattr__(self, "effects", tuple(self.effects))
        evidence = tuple(self.context_evidence)
        if not all(isinstance(item, ContextEvidenceReceipt) for item in evidence):
            raise TypeError(
                "tool context evidence must contain ContextEvidenceReceipt values"
            )
        object.__setattr__(self, "context_evidence", evidence)
        object.__setattr__(
            self,
            "step_disposition",
            ToolStepDisposition(self.step_disposition),
        )
        object.__setattr__(
            self,
            "planning_disposition",
            ToolPlanningDisposition(self.planning_disposition),
        )


@dataclass(frozen=True, slots=True)
class ToolBatchResult:
    results: tuple[ToolCallResult, ...]
    outcome: ToolBatchOutcome
    error: str | None = None
    cache_hits: tuple[bool, ...] = ()
    effect_state: ToolEffectState = ToolEffectState.UNKNOWN

    def __post_init__(self) -> None:
        object.__setattr__(self, "results", tuple(self.results))
        object.__setattr__(self, "outcome", ToolBatchOutcome(self.outcome))
        object.__setattr__(self, "error", _optional_text(self.error))
        object.__setattr__(self, "cache_hits", tuple(bool(item) for item in self.cache_hits))
        object.__setattr__(
            self,
            "effect_state",
            ToolEffectState(self.effect_state),
        )
        if self.cache_hits and len(self.cache_hits) != len(self.results):
            raise ValueError("tool batch cache hits must align with results")

    @property
    def replan_requested(self) -> bool:
        return any(
            result.planning_disposition is ToolPlanningDisposition.REPLAN
            for result in self.results
        )


def _tool_call_from_mapping(value: Mapping[str, Any]) -> ToolCall:
    function = value.get("function") if isinstance(value.get("function"), Mapping) else {}
    return ToolCall(
        id=str(value.get("id") or ""),
        name=str(value.get("name") or function.get("name") or ""),
        arguments_json=str(
            value.get("arguments_json")
            or function.get("arguments")
            or ""
        ),
    )
