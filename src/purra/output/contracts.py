"""Closed contracts for canonical Agent output.

Public text is deliberately representable only as a Provider-authored event.
Runtime, tool, and domain producers use separate structured contracts.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
import json
import re
from typing import Any

from purra.contracts import (
    AgentMessage,
    DomainEffect,
    MessageRole,
    RunId,
    RunStatus,
)
from purra.json_values import (
    FrozenDict,
    freeze_json_mapping,
    freeze_json_value,
    thaw_json_value,
)
from purra.normalization import optional_text, positive_int, required_text


class AgentOutputIntent(StrEnum):
    EXECUTION_PUBLIC = "execution_public"
    FINAL_PUBLIC = "final_public"
    STRUCTURED_PRIVATE = "structured_private"
    REASONING_PRIVATE = "reasoning_private"


class OutputCommitMode(StrEnum):
    LIVE = "live"
    GATED = "gated"
    PRIVATE = "private"


class OutputSource(StrEnum):
    PROVIDER = "provider"
    RUNTIME = "runtime"
    TOOL = "tool"
    DOMAIN = "domain"


class OutputChannel(StrEnum):
    COMMENTARY = "commentary"
    FINAL = "final"
    OPERATION = "operation"
    LIFECYCLE = "lifecycle"
    ERROR = "error"
    DIAGNOSTIC = "diagnostic"
    DELEGATION = "delegation"


class OutputVisibility(StrEnum):
    PUBLIC = "public"
    PRIVATE = "private"
    DIAGNOSTIC = "diagnostic"


class OutputEventKind(StrEnum):
    STREAM_OPENED = "stream.opened"
    PROVIDER_CONTENT_DELTA = "provider.content_delta"
    PROVIDER_REASONING_DELTA = "provider.reasoning_delta"
    PROVIDER_TOOL_CALL_DELTA = "provider.tool_call_delta"
    PROVIDER_USAGE = "provider.usage"
    STREAM_COMMITTED = "stream.committed"
    STREAM_ABORTED = "stream.aborted"
    OPERATION_STARTED = "operation.started"
    OPERATION_FINISHED = "operation.finished"
    RUN_LIFECYCLE = "run.lifecycle"
    RUN_VALIDATED_RESULT = "run.validated_result"
    TOOL = "tool.event"
    DOMAIN_EFFECT = "domain.effect"
    DELEGATION = "delegation.event"
    RUNTIME = "runtime.event"


TERMINAL_STREAM_ABORT_ERROR_CODE = "run_terminalized"
TERMINAL_STREAM_ABORT_CAUSE = "run_terminal_commit"


class ResponseTransactionMode(StrEnum):
    DIRECT_LIVE = "direct_live"
    VALIDATED_RESULT = "validated_result"


class PublicPresentationMode(StrEnum):
    NONE = "none"
    MODEL_LIVE = "model_live"


@dataclass(frozen=True, slots=True)
class ResponseTransactionPolicy:
    mode: ResponseTransactionMode
    public_presentation: PublicPresentationMode = PublicPresentationMode.NONE

    def __post_init__(self) -> None:
        mode = ResponseTransactionMode(self.mode)
        presentation = PublicPresentationMode(self.public_presentation)
        if (
            mode is ResponseTransactionMode.DIRECT_LIVE
            and presentation is not PublicPresentationMode.NONE
        ):
            raise ValueError(
                "direct-live response cannot add a second presentation"
            )
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "public_presentation", presentation)


@dataclass(frozen=True, slots=True)
class PublicFact:
    key: str
    value: Any

    def __post_init__(self) -> None:
        key = required_text(self.key, "public fact key")
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9]{0,63}", key):
            raise ValueError("public fact key must be lower-camel compatible")
        object.__setattr__(self, "key", key)
        object.__setattr__(self, "value", freeze_json_value(self.value))


@dataclass(frozen=True, slots=True)
class PublicFactBundle:
    facts: tuple[PublicFact, ...]
    resource_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        facts = tuple(self.facts)
        if len(facts) > 32:
            raise ValueError("public fact bundle has too many facts")
        if any(not isinstance(fact, PublicFact) for fact in facts):
            raise TypeError("public fact bundle requires PublicFact values")
        keys = [fact.key for fact in facts]
        if len(keys) != len(set(keys)):
            raise ValueError("public fact keys must be unique")
        for fact in facts:
            _reject_private_fact_fields(fact.key, fact.value)

        refs = tuple(required_text(ref, "public resource reference") for ref in self.resource_refs)
        if len(refs) > 16:
            raise ValueError("public fact bundle has too many resource references")
        for ref in refs:
            if not re.fullmatch(
                r"resource://[a-z][a-z0-9-]{0,31}/[A-Za-z0-9][A-Za-z0-9._~-]{0,127}",
                ref,
            ):
                raise ValueError("invalid public resource reference")

        encoded = json.dumps(
            {
                "facts": [
                    {"key": fact.key, "value": thaw_json_value(fact.value)}
                    for fact in facts
                ],
                "resourceRefs": list(refs),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        if len(encoded) > 8_192:
            raise ValueError("public fact bundle exceeds the size limit")
        object.__setattr__(self, "facts", facts)
        object.__setattr__(self, "resource_refs", refs)

    def as_messages(self) -> tuple[AgentMessage, ...]:
        payload = json.dumps(
            {
                "facts": [
                    {"key": fact.key, "value": thaw_json_value(fact.value)}
                    for fact in self.facts
                ],
                "resourceRefs": list(self.resource_refs),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        return (
            AgentMessage(
                role=MessageRole.SYSTEM,
                content=(
                    "Write the final user-facing response using only the "
                    "committed public facts below. Do not expose identifiers, "
                    "private state, hidden content, or tool calls."
                ),
            ),
            AgentMessage(role=MessageRole.USER, content=payload),
        )


_FORBIDDEN_PUBLIC_FACT_KEYS = frozenset({
    "artifactid",
    "candidatebody",
    "candidateid",
    "contenttext",
    "databaseid",
    "internalid",
    "payload",
    "privatestatus",
    "reportbody",
    "reviewid",
    "revisionid",
    "runid",
    "status",
    "trace",
})


def _reject_private_fact_fields(key: str, value: Any) -> None:
    normalized = re.sub(r"[^a-z0-9]", "", key.casefold())
    if normalized in _FORBIDDEN_PUBLIC_FACT_KEYS:
        raise ValueError(f"forbidden public fact field: {key}")
    if isinstance(value, Mapping):
        for nested_key, nested_value in value.items():
            _reject_private_fact_fields(str(nested_key), nested_value)
    elif isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        for nested in value:
            _reject_private_fact_fields("item", nested)


_PUBLIC_INTENTS = frozenset({
    AgentOutputIntent.EXECUTION_PUBLIC,
    AgentOutputIntent.FINAL_PUBLIC,
})
_TEXT_CHANNELS = frozenset({
    OutputChannel.COMMENTARY,
    OutputChannel.FINAL,
})


@dataclass(frozen=True, slots=True)
class OutputStreamSpec:
    output_stream_id: str
    run_id: RunId
    turn_id: str | None
    invocation_id: str
    intent: AgentOutputIntent
    commit_mode: OutputCommitMode

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "output_stream_id",
            required_text(self.output_stream_id, "output stream id"),
        )
        object.__setattr__(self, "run_id", required_text(self.run_id, "run id"))
        object.__setattr__(self, "turn_id", optional_text(self.turn_id))
        object.__setattr__(
            self,
            "invocation_id",
            required_text(self.invocation_id, "invocation id"),
        )
        intent = AgentOutputIntent(self.intent)
        commit_mode = OutputCommitMode(self.commit_mode)
        if intent in _PUBLIC_INTENTS and commit_mode is not OutputCommitMode.LIVE:
            raise ValueError("public output intent must be live")
        if (
            intent
            in {
                AgentOutputIntent.STRUCTURED_PRIVATE,
                AgentOutputIntent.REASONING_PRIVATE,
            }
            and commit_mode is OutputCommitMode.LIVE
        ):
            raise ValueError("private output intent cannot be live")
        if (
            intent is AgentOutputIntent.REASONING_PRIVATE
            and commit_mode is not OutputCommitMode.PRIVATE
        ):
            raise ValueError(
                "reasoning-private output requires private commit mode"
            )
        object.__setattr__(self, "intent", intent)
        object.__setattr__(self, "commit_mode", commit_mode)


@dataclass(frozen=True, slots=True)
class AgentOutputEventDraft:
    run_id: RunId
    turn_id: str | None
    output_stream_id: str | None
    invocation_id: str | None
    source_event_key: str
    source: OutputSource
    kind: OutputEventKind
    channel: OutputChannel
    visibility: OutputVisibility
    payload: Mapping[str, Any] = field(default_factory=dict)
    occurred_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", required_text(self.run_id, "run id"))
        object.__setattr__(self, "turn_id", optional_text(self.turn_id))
        object.__setattr__(
            self,
            "output_stream_id",
            optional_text(self.output_stream_id),
        )
        object.__setattr__(
            self,
            "invocation_id",
            optional_text(self.invocation_id),
        )
        object.__setattr__(
            self,
            "source_event_key",
            required_text(self.source_event_key, "source event key"),
        )
        source = OutputSource(self.source)
        kind = OutputEventKind(self.kind)
        channel = OutputChannel(self.channel)
        visibility = OutputVisibility(self.visibility)
        _require_aware(self.occurred_at, "occurred_at")
        payload = freeze_json_mapping(self.payload)
        _validate_public_text(
            source=source,
            kind=kind,
            channel=channel,
            visibility=visibility,
            payload=payload,
            output_stream_id=self.output_stream_id,
            invocation_id=self.invocation_id,
        )
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "channel", channel)
        object.__setattr__(self, "visibility", visibility)
        object.__setattr__(self, "payload", payload)

    @classmethod
    def public_text(
        cls,
        *,
        run_id: RunId,
        turn_id: str | None,
        output_stream_id: str,
        invocation_id: str,
        source_event_key: str,
        source: OutputSource,
        channel: OutputChannel,
        delta: str,
        occurred_at: datetime,
    ) -> AgentOutputEventDraft:
        return cls(
            run_id=run_id,
            turn_id=turn_id,
            output_stream_id=output_stream_id,
            invocation_id=invocation_id,
            source_event_key=source_event_key,
            source=source,
            kind=OutputEventKind.PROVIDER_CONTENT_DELTA,
            channel=channel,
            visibility=OutputVisibility.PUBLIC,
            payload={"delta": str(delta)},
            occurred_at=occurred_at,
        )


@dataclass(frozen=True, slots=True)
class AgentOutputEvent:
    event_id: str
    output_stream_id: str | None
    run_id: RunId
    turn_id: str | None
    invocation_id: str | None
    sequence: int
    source: OutputSource
    kind: OutputEventKind
    channel: OutputChannel
    visibility: OutputVisibility
    payload: Mapping[str, Any]
    occurred_at: datetime
    emitted_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "event_id", required_text(self.event_id, "event id")
        )
        object.__setattr__(
            self,
            "output_stream_id",
            optional_text(self.output_stream_id),
        )
        object.__setattr__(self, "run_id", required_text(self.run_id, "run id"))
        object.__setattr__(self, "turn_id", optional_text(self.turn_id))
        object.__setattr__(
            self, "invocation_id", optional_text(self.invocation_id)
        )
        object.__setattr__(
            self, "sequence", positive_int(self.sequence, "sequence")
        )
        source = OutputSource(self.source)
        kind = OutputEventKind(self.kind)
        channel = OutputChannel(self.channel)
        visibility = OutputVisibility(self.visibility)
        _require_aware(self.occurred_at, "occurred_at")
        _require_aware(self.emitted_at, "emitted_at")
        payload = freeze_json_mapping(self.payload)
        _validate_public_text(
            source=source,
            kind=kind,
            channel=channel,
            visibility=visibility,
            payload=payload,
            output_stream_id=self.output_stream_id,
            invocation_id=self.invocation_id,
        )
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "channel", channel)
        object.__setattr__(self, "visibility", visibility)
        object.__setattr__(self, "payload", payload)


@dataclass(frozen=True, slots=True)
class RunLifecycleOutputDraft:
    source_event_key: str
    status: RunStatus
    payload: Mapping[str, Any]
    occurred_at: datetime
    turn_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "source_event_key",
            required_text(self.source_event_key, "source event key"),
        )
        object.__setattr__(self, "status", RunStatus(self.status))
        object.__setattr__(self, "payload", freeze_json_mapping(self.payload))
        object.__setattr__(self, "turn_id", optional_text(self.turn_id))
        _require_aware(self.occurred_at, "occurred_at")


@dataclass(frozen=True, slots=True)
class ToolOutputEvent:
    operation_id: str
    run_id: RunId
    invocation_id: str | None
    tool_call_id: str
    tool_name: str
    status: str
    occurred_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", required_text(self.run_id, "run id"))
        object.__setattr__(
            self,
            "invocation_id",
            optional_text(self.invocation_id),
        )
        for attribute, label in (
            ("operation_id", "operation id"),
            ("tool_call_id", "tool call id"),
            ("tool_name", "tool name"),
            ("status", "tool status"),
        ):
            object.__setattr__(
                self,
                attribute,
                required_text(getattr(self, attribute), label),
            )
        _require_aware(self.occurred_at, "occurred_at")


@dataclass(frozen=True, slots=True)
class DomainEffectOutput:
    effect_id: str
    run_id: RunId
    effect: DomainEffect
    occurred_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "effect_id",
            required_text(self.effect_id, "effect id"),
        )
        object.__setattr__(self, "run_id", required_text(self.run_id, "run id"))
        if not isinstance(self.effect, DomainEffect):
            raise TypeError("domain effect output requires a DomainEffect")
        _require_aware(self.occurred_at, "occurred_at")


@dataclass(frozen=True, slots=True)
class RuntimeOutputEvent:
    event_id: str
    run_id: RunId
    event_type: str
    payload: Mapping[str, Any]
    occurred_at: datetime
    turn_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "event_id",
            required_text(self.event_id, "runtime output event id"),
        )
        object.__setattr__(self, "run_id", required_text(self.run_id, "run id"))
        object.__setattr__(
            self,
            "event_type",
            required_text(self.event_type, "runtime output event type"),
        )
        object.__setattr__(self, "payload", freeze_json_mapping(self.payload))
        object.__setattr__(self, "turn_id", optional_text(self.turn_id))
        _require_aware(self.occurred_at, "occurred_at")


@dataclass(frozen=True, slots=True)
class DelegationOutputEvent:
    event_id: str
    run_id: RunId
    batch_id: str
    delegation_id: str
    status: str
    agent_name: str
    agent_title: str | None
    objective: str
    error_code: str | None
    occurred_at: datetime

    def __post_init__(self) -> None:
        for attribute, label in (
            ("event_id", "delegation output event id"),
            ("run_id", "delegation Root Run id"),
            ("batch_id", "delegation batch id"),
            ("delegation_id", "delegation id"),
            ("agent_name", "delegation agent name"),
            ("objective", "delegation objective"),
        ):
            object.__setattr__(
                self,
                attribute,
                required_text(getattr(self, attribute), label),
            )
        status = required_text(self.status, "delegation status")
        if status not in {
            "queued",
            "running",
            "done",
            "failed",
            "canceled",
        }:
            raise ValueError("invalid delegation output status")
        object.__setattr__(self, "status", status)
        object.__setattr__(
            self,
            "agent_title",
            optional_text(self.agent_title),
        )
        object.__setattr__(
            self,
            "error_code",
            optional_text(self.error_code),
        )
        _require_aware(self.occurred_at, "occurred_at")


def _validate_public_text(
    *,
    source: OutputSource,
    kind: OutputEventKind,
    channel: OutputChannel,
    visibility: OutputVisibility,
    payload: FrozenDict,
    output_stream_id: str | None,
    invocation_id: str | None,
) -> None:
    if (
        visibility is not OutputVisibility.PUBLIC
        or channel not in _TEXT_CHANNELS
        or (
            kind is not OutputEventKind.PROVIDER_CONTENT_DELTA
            and "delta" not in payload
        )
    ):
        return
    if source is not OutputSource.PROVIDER:
        raise ValueError("public text requires provider source")
    if kind is not OutputEventKind.PROVIDER_CONTENT_DELTA:
        raise ValueError("public text requires provider content delta")
    if not output_stream_id or not invocation_id:
        raise ValueError("public text requires stream and invocation ids")
    if not isinstance(payload.get("delta"), str):
        raise ValueError("public text requires a string delta")


def _require_aware(value: datetime, field_name: str) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    if value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")


__all__ = [name for name in globals() if not name.startswith("_")]
