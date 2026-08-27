"""Typed, provider-neutral model protocol capability snapshots."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from hashlib import sha256

from purra.normalization import optional_positive_int, positive_int, required_text


class ReasoningControl(StrEnum):
    SELECTABLE = "selectable"
    ALWAYS_ENABLED = "always_enabled"
    UNAVAILABLE = "unavailable"


class ReasoningReplayPolicy(StrEnum):
    REQUIRED = "required"
    FORBIDDEN = "forbidden"
    IGNORED = "ignored"


class FeatureSupport(StrEnum):
    SUPPORTED = "supported"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"


class ThinkingTokenAccounting(StrEnum):
    INCLUDED = "included"
    SEPARATE = "separate"
    UNKNOWN = "unknown"


class AssistantContentWithToolCalls(StrEnum):
    REQUIRED = "required"
    OPTIONAL = "optional"
    FORBIDDEN = "forbidden"


@dataclass(frozen=True, slots=True)
class ModelProtocolCapabilities:
    """Immutable facts used by Core without inspecting provider identities."""

    reasoning_control: ReasoningControl = ReasoningControl.SELECTABLE
    reasoning_replay: ReasoningReplayPolicy = ReasoningReplayPolicy.IGNORED
    tool_calling: FeatureSupport = FeatureSupport.SUPPORTED
    required_tool_choice: FeatureSupport = FeatureSupport.SUPPORTED
    parallel_tool_calls: FeatureSupport = FeatureSupport.SUPPORTED
    streaming: FeatureSupport = FeatureSupport.SUPPORTED
    cancellation: FeatureSupport = FeatureSupport.SUPPORTED
    assistant_content_with_tool_calls: AssistantContentWithToolCalls = (
        AssistantContentWithToolCalls.OPTIONAL
    )
    json_schema_level: str = "unknown"
    stream_finish_semantics: str = "normalized"
    usage_semantics: str = "normalized"

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "reasoning_control",
            ReasoningControl(self.reasoning_control),
        )
        object.__setattr__(
            self,
            "reasoning_replay",
            ReasoningReplayPolicy(self.reasoning_replay),
        )
        for name in (
            "tool_calling",
            "required_tool_choice",
            "parallel_tool_calls",
            "streaming",
            "cancellation",
        ):
            object.__setattr__(self, name, FeatureSupport(getattr(self, name)))
        object.__setattr__(
            self,
            "assistant_content_with_tool_calls",
            AssistantContentWithToolCalls(
                self.assistant_content_with_tool_calls
            ),
        )
        for name in (
            "json_schema_level",
            "stream_finish_semantics",
            "usage_semantics",
        ):
            value = str(getattr(self, name) or "").strip()
            if not value:
                raise ValueError(f"model protocol {name} is required")
            object.__setattr__(self, name, value)

    @classmethod
    def conservative(cls) -> "ModelProtocolCapabilities":
        return cls(
            reasoning_control=ReasoningControl.UNAVAILABLE,
            reasoning_replay=ReasoningReplayPolicy.FORBIDDEN,
            tool_calling=FeatureSupport.UNKNOWN,
            required_tool_choice=FeatureSupport.UNKNOWN,
            parallel_tool_calls=FeatureSupport.UNKNOWN,
        )

    def to_mapping(self) -> dict[str, str]:
        return {
            "reasoningControl": self.reasoning_control.value,
            "reasoningReplay": self.reasoning_replay.value,
            "toolCalling": self.tool_calling.value,
            "requiredToolChoice": self.required_tool_choice.value,
            "parallelToolCalls": self.parallel_tool_calls.value,
            "streaming": self.streaming.value,
            "cancellation": self.cancellation.value,
            "assistantContentWithToolCalls": (
                self.assistant_content_with_tool_calls.value
            ),
            "jsonSchemaLevel": self.json_schema_level,
            "streamFinishSemantics": self.stream_finish_semantics,
            "usageSemantics": self.usage_semantics,
        }

    def digest(self) -> str:
        payload = json.dumps(
            self.to_mapping(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return sha256(payload).hexdigest()

    def reasoning_mode_is_supported(self, mode: object) -> bool:
        value = str(getattr(mode, "value", mode) or "").strip().lower()
        if self.reasoning_control is ReasoningControl.SELECTABLE:
            return value in {"default", "enabled", "disabled"}
        if self.reasoning_control is ReasoningControl.ALWAYS_ENABLED:
            return value in {"default", "enabled"}
        return value in {"default", "disabled"}


@dataclass(frozen=True, slots=True)
class ModelOutputCapabilities:
    """Objective model output limits and token-accounting facts."""

    max_output_tokens: int | None = None
    thinking_token_accounting: ThinkingTokenAccounting = (
        ThinkingTokenAccounting.UNKNOWN
    )

    def __post_init__(self) -> None:
        object.__setattr__(self, "max_output_tokens", optional_positive_int(
            self.max_output_tokens, "model max output tokens"
        ))
        object.__setattr__(
            self,
            "thinking_token_accounting",
            ThinkingTokenAccounting(self.thinking_token_accounting),
        )

    def to_mapping(self) -> dict[str, object]:
        return {
            "maxOutputTokens": self.max_output_tokens,
            "thinkingTokenAccounting": self.thinking_token_accounting.value,
        }


@dataclass(frozen=True, slots=True)
class ModelCapabilitySnapshot:
    """Versioned immutable model facts captured before a Run is created."""

    schema_version: int
    profile_id: str
    provider_protocol: str
    context_window_tokens: int
    max_output_tokens: int | None
    thinking_token_accounting: ThinkingTokenAccounting
    protocol: ModelProtocolCapabilities
    actionable: bool = True
    source: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "schema_version", positive_int(
            self.schema_version, "capability snapshot schema version"
        ))
        object.__setattr__(self, "profile_id", required_text(
            self.profile_id, "capability snapshot profile id"
        ))
        object.__setattr__(self, "provider_protocol", required_text(
            self.provider_protocol, "capability snapshot provider protocol"
        ))
        object.__setattr__(self, "context_window_tokens", positive_int(
            self.context_window_tokens, "capability snapshot context window"
        ))
        object.__setattr__(self, "max_output_tokens", optional_positive_int(
            self.max_output_tokens, "capability snapshot max output tokens"
        ))
        object.__setattr__(
            self,
            "thinking_token_accounting",
            ThinkingTokenAccounting(self.thinking_token_accounting),
        )
        if not isinstance(self.protocol, ModelProtocolCapabilities):
            raise TypeError(
                "capability snapshot protocol must be ModelProtocolCapabilities"
            )
        object.__setattr__(self, "actionable", bool(self.actionable))
        normalized_source = str(self.source or "").strip() or None
        object.__setattr__(self, "source", normalized_source)

    @property
    def output(self) -> ModelOutputCapabilities:
        return ModelOutputCapabilities(
            max_output_tokens=self.max_output_tokens,
            thinking_token_accounting=self.thinking_token_accounting,
        )

    def to_mapping(self, *, include_digest: bool = False) -> dict[str, object]:
        value: dict[str, object] = {
            "schemaVersion": self.schema_version,
            "profileId": self.profile_id,
            "providerProtocol": self.provider_protocol,
            "contextWindowTokens": self.context_window_tokens,
            "maxOutputTokens": self.max_output_tokens,
            "thinkingTokenAccounting": self.thinking_token_accounting.value,
            "protocol": self.protocol.to_mapping(),
            "actionable": self.actionable,
            "source": self.source,
        }
        if include_digest:
            value["digest"] = self.digest()
        return value

    def digest(self) -> str:
        payload = json.dumps(
            self.to_mapping(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return sha256(payload).hexdigest()


def generic_capability_snapshot() -> ModelCapabilitySnapshot:
    return ModelCapabilitySnapshot(
        schema_version=1,
        profile_id="generic",
        provider_protocol="custom",
        context_window_tokens=200_000,
        max_output_tokens=None,
        thinking_token_accounting=ThinkingTokenAccounting.UNKNOWN,
        protocol=ModelProtocolCapabilities(),
        actionable=True,
    )


__all__ = [
    "AssistantContentWithToolCalls",
    "FeatureSupport",
    "ModelCapabilitySnapshot",
    "ModelOutputCapabilities",
    "ModelProtocolCapabilities",
    "ReasoningControl",
    "ReasoningReplayPolicy",
    "ThinkingTokenAccounting",
    "generic_capability_snapshot",
]
