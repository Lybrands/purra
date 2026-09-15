"""Model-loop contracts: messages, requests, invocations, and streams."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, AsyncIterator, Mapping, TypeAlias

from purra.contracts.enums import (
    MessageOrigin,
    ToolChoiceMode,
    MessageRole,
    ModelFinishReason,
    PlanningMode,
    ReasoningMode,
    SessionId,
)
from purra.json_values import (
    freeze_json_mapping,
    freeze_json_value,
    thaw_json_mapping,
    thaw_json_value,
)
from purra.model_protocol.capabilities import (
    ModelCapabilitySnapshot,
    ModelProtocolCapabilities,
    generic_capability_snapshot,
)
from purra.model_protocol.diagnostics import ModelTransportDiagnostics
from purra.model_protocol.output_limits import InvocationOutputBudget
from purra.normalization import (
    non_negative_int,
    optional_positive_int,
    optional_text as _optional_text,
    required_text,
)
from purra.structured import StructuredOutputContract
from purra.contracts.tools import ToolCall, ToolSchema, _tool_call_from_mapping

@dataclass(frozen=True, slots=True)
class AgentMessage:
    """Provider-neutral message with lossless extra protocol fields."""

    role: MessageRole
    content: Any = None
    reasoning: str | None = None
    tool_calls: tuple[ToolCall, ...] = ()
    tool_call_id: str | None = None
    origin: MessageOrigin = MessageOrigin.CALLER
    attributes: Mapping[str, Any] = field(default_factory=dict)
    host_metadata: Mapping[str, Any] = field(default_factory=dict)
    provider_data: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        try:
            role = MessageRole(str(self.role or "").strip())
        except ValueError:
            raise ValueError("unsupported message role")
        object.__setattr__(self, "role", role)
        from purra.media import parse_static_image_content
        if parse_static_image_content(self.content) is not None and role is not MessageRole.USER:
            raise ValueError("Static images require user role")
        object.__setattr__(self, "content", freeze_json_value(self.content))
        object.__setattr__(self, "reasoning", _optional_text(self.reasoning))
        object.__setattr__(self, "tool_calls", tuple(self.tool_calls))
        object.__setattr__(self, "tool_call_id", _optional_text(self.tool_call_id))
        object.__setattr__(self, "origin", MessageOrigin(self.origin))
        if role is MessageRole.TOOL and not self.tool_call_id:
            raise ValueError("tool message requires tool_call_id")
        object.__setattr__(self, "attributes", freeze_json_mapping(self.attributes))
        object.__setattr__(self, "provider_data", freeze_json_mapping(self.provider_data))
        object.__setattr__(
            self,
            "host_metadata",
            freeze_json_mapping(self.host_metadata),
        )

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "AgentMessage":
        raw = dict(value)
        # Provenance is assigned only by in-process Core composition. A
        # untrusted external mapping can never claim a host origin.
        raw.pop("origin", None)
        raw.pop("host_metadata", None)
        role = raw.pop("role", "")
        content = raw.pop("content", None)
        reasoning = raw.pop("reasoning", raw.pop("reasoning_content", None))
        tool_call_id = raw.pop("tool_call_id", None)
        provider_data = raw.pop("provider_data", {})
        tool_calls = tuple(
            _tool_call_from_mapping(item)
            for item in (raw.pop("tool_calls", ()) or ())
            if isinstance(item, Mapping)
        )
        return cls(
            role=role,  # type: ignore[arg-type]
            content=content,
            reasoning=reasoning,
            tool_calls=tool_calls,
            tool_call_id=tool_call_id,
            attributes=raw,
            provider_data=provider_data,
        )

    def to_mapping(self) -> dict[str, Any]:
        value = thaw_json_mapping(self.attributes)
        value.update({"role": self.role.value, "content": thaw_json_value(self.content)})
        if self.reasoning is not None:
            value["reasoning"] = self.reasoning
        if self.tool_calls:
            value["tool_calls"] = [
                {
                    "id": call.id,
                    "name": call.name,
                    "arguments_json": call.arguments_json,
                }
                for call in self.tool_calls
            ]
        if self.tool_call_id is not None:
            value["tool_call_id"] = self.tool_call_id
        if self.provider_data:
            value["provider_data"] = thaw_json_mapping(self.provider_data)
        return value


@dataclass(frozen=True, slots=True)
class DomainContext:
    """Opaque immutable request data interpreted only by a domain adapter."""

    namespace: str
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "namespace", required_text(
            self.namespace, "domain context namespace"
        ))
        object.__setattr__(self, "payload", freeze_json_mapping(self.payload))


@dataclass(frozen=True, slots=True)
class ModelRequest:
    provider: str
    model: str
    capability_snapshot: ModelCapabilitySnapshot = field(
        default_factory=generic_capability_snapshot
    )
    max_generation_tokens: int | None = None
    options: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "provider", required_text(
            self.provider, "model provider"
        ).lower())
        object.__setattr__(self, "model", required_text(
            self.model, "model name"
        ))
        if not isinstance(self.capability_snapshot, ModelCapabilitySnapshot):
            raise TypeError(
                "model capability snapshot must be ModelCapabilitySnapshot"
            )
        object.__setattr__(
            self,
            "max_generation_tokens",
            optional_positive_int(
                self.max_generation_tokens,
                "model request max_generation_tokens",
            ),
        )
        object.__setattr__(self, "options", freeze_json_mapping(self.options))

    @property
    def profile_id(self) -> str:
        return self.capability_snapshot.profile_id

    @property
    def protocol_capabilities(self) -> ModelProtocolCapabilities:
        return self.capability_snapshot.protocol


@dataclass(frozen=True, slots=True)
class ModelInvocation:
    request: ModelRequest
    tools: tuple[ToolSchema, ...] = ()
    tool_choice: ToolChoiceMode = ToolChoiceMode.AUTO
    output_budget: InvocationOutputBudget | None = None
    reasoning_mode: ReasoningMode = ReasoningMode.DEFAULT

    output_contract: "StructuredOutputContract | None" = None

    def __post_init__(self) -> None:
        if self.output_contract is not None:
            if not isinstance(self.output_contract, StructuredOutputContract):
                raise TypeError("output_contract must be StructuredOutputContract")
            if self.tools:
                raise ValueError("structured output invocations cannot use tools")
        object.__setattr__(self, "tools", tuple(self.tools))
        object.__setattr__(self, "tool_choice", ToolChoiceMode(self.tool_choice))
        object.__setattr__(self, "reasoning_mode", ReasoningMode(self.reasoning_mode))
        if self.output_budget is not None and not isinstance(
            self.output_budget,
            InvocationOutputBudget,
        ):
            raise TypeError(
                "model output budget must be InvocationOutputBudget"
            )
        if not self.tools and self.tool_choice is ToolChoiceMode.REQUIRED:
            raise ValueError("required tool choice needs at least one tool")

    @property
    def max_generation_tokens(self) -> int | None:
        return (
            self.output_budget.max_generation_tokens
            if self.output_budget is not None
            else None
        )


@dataclass(frozen=True, slots=True)
class ToolCallDelta:
    index: int
    id: str | None = None
    type: str | None = None
    name: str | None = None
    arguments_fragment: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "index", non_negative_int(
            self.index, "tool call delta index"
        ))
        object.__setattr__(self, "id", _optional_text(self.id))
        object.__setattr__(self, "type", _optional_text(self.type))
        object.__setattr__(self, "name", _optional_text(self.name))
        object.__setattr__(
            self,
            "arguments_fragment",
            str(self.arguments_fragment or ""),
        )


@dataclass(frozen=True, slots=True)
class ModelTokenUsage:
    """Provider-reported token usage for one completed model request.

    ``input_tokens`` is the complete provider input, including cached input
    tokens when the provider reports those as separate counters.
    """

    input_tokens: int
    generation_tokens: int
    total_tokens: int | None = None
    cached_input_tokens: int = 0
    reasoning_tokens: int | None = None

    def __post_init__(self) -> None:
        for name in (
            "input_tokens",
            "generation_tokens",
            "cached_input_tokens",
        ):
            object.__setattr__(self, name, non_negative_int(
                getattr(self, name), name
            ))
        if self.reasoning_tokens is not None:
            object.__setattr__(self, "reasoning_tokens", non_negative_int(
                self.reasoning_tokens,
                "reasoning_tokens",
            ))
        if self.total_tokens is None:
            object.__setattr__(
                self,
                "total_tokens",
                self.input_tokens + self.generation_tokens,
            )
        else:
            object.__setattr__(self, "total_tokens", non_negative_int(
                self.total_tokens, "total_tokens"
            ))


@dataclass(frozen=True, slots=True)
class ModelStreamChunk:
    content_delta: str = ""
    reasoning_delta: str = ""
    progress_delta: str = ""
    tool_call_deltas: tuple[ToolCallDelta, ...] = ()
    finish_reason: ModelFinishReason | None = None
    usage: ModelTokenUsage | None = None
    # Opaque provider continuation data, emitted only with the terminal chunk.
    provider_data: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        for name in ("content_delta", "reasoning_delta", "progress_delta"):
            if not isinstance(getattr(self, name), str):
                raise TypeError(f"model stream {name.replace('_', ' ')} must be text")
        object.__setattr__(self, "tool_call_deltas", tuple(self.tool_call_deltas))
        if self.finish_reason is not None:
            object.__setattr__(
                self,
                "finish_reason",
                ModelFinishReason(self.finish_reason),
            )
        if self.usage is not None and not isinstance(
            self.usage,
            ModelTokenUsage,
        ):
            raise TypeError("model stream usage must be ModelTokenUsage")
        if self.provider_data is not None:
            if self.finish_reason is None:
                raise ValueError("provider data requires a terminal chunk")
            attributes = freeze_json_mapping(self.provider_data)
            if len(str(thaw_json_mapping(attributes))) > 1_000_000:
                raise ValueError("provider data exceeds the size limit")
            object.__setattr__(self, "provider_data", attributes)


class ModelStreamActivityKind(StrEnum):
    TRANSPORT = "transport"
    WORKING = "working"


class ModelStreamActivitySupport(StrEnum):
    SEMANTIC_ONLY = "semantic_only"
    TRANSPORT = "transport"
    WORKING = "working"




@dataclass(frozen=True, slots=True)
class ModelStreamActivity:
    kind: ModelStreamActivityKind
    transport_diagnostics: ModelTransportDiagnostics | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", ModelStreamActivityKind(self.kind))
        if self.transport_diagnostics is not None and not isinstance(self.transport_diagnostics, ModelTransportDiagnostics):
            raise TypeError("activity transport diagnostics must be typed")


ModelStreamItem: TypeAlias = ModelStreamActivity | ModelStreamChunk


@dataclass(slots=True)
class ModelStream:
    """Provider stream plus the exact output limit applied by the host."""

    chunks: AsyncIterator[ModelStreamItem]
    model: str
    metadata: Mapping[str, Any] = field(default_factory=dict)
    transport_diagnostics: ModelTransportDiagnostics | None = None
    activity_support: ModelStreamActivitySupport = (
        ModelStreamActivitySupport.SEMANTIC_ONLY
    )
    applied_generation_limit: int | None = None

    def __post_init__(self) -> None:
        if self.transport_diagnostics is not None and not isinstance(self.transport_diagnostics, ModelTransportDiagnostics):
            raise TypeError("stream transport diagnostics must be typed")
        self.model = required_text(self.model, "model stream model name")
        self.applied_generation_limit = optional_positive_int(
            self.applied_generation_limit,
            "model stream applied output limit",
        )
        self.metadata = freeze_json_mapping(self.metadata)
        self.activity_support = ModelStreamActivitySupport(self.activity_support)


@dataclass(frozen=True, slots=True)
class ModelCompletion:
    """Provider completion plus the exact output limit applied by the host."""

    message: AgentMessage
    model: str
    finish_reason: ModelFinishReason | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    usage: ModelTokenUsage | None = None
    applied_generation_limit: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "model", required_text(
            self.model, "model completion model name"
        ))
        object.__setattr__(
            self,
            "applied_generation_limit",
            optional_positive_int(
                self.applied_generation_limit,
                "model completion applied output limit",
            ),
        )
        if self.finish_reason is not None:
            object.__setattr__(
                self,
                "finish_reason",
                ModelFinishReason(self.finish_reason),
            )
        object.__setattr__(self, "metadata", freeze_json_mapping(self.metadata))
        if self.usage is not None and not isinstance(self.usage, ModelTokenUsage):
            raise TypeError("model completion usage must be ModelTokenUsage")

@dataclass(frozen=True, slots=True)
class AgentRunRequest:
    messages: tuple[AgentMessage, ...]
    model: ModelRequest
    domain_context: DomainContext
    session_id: SessionId | None = None
    mode: str | None = None
    context_window: int | None = None
    tools_enabled: bool = False
    planning_mode: PlanningMode = PlanningMode.AUTO
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        messages = tuple(self.messages)
        if not all(isinstance(message, AgentMessage) for message in messages):
            raise TypeError("agent run messages must be AgentMessage values")
        if not isinstance(self.model, ModelRequest):
            raise TypeError("agent run model must be ModelRequest")
        if not isinstance(self.domain_context, DomainContext):
            raise TypeError("agent run domain context must be DomainContext")
        object.__setattr__(self, "messages", messages)
        object.__setattr__(self, "mode", _optional_text(self.mode))
        object.__setattr__(self, "tools_enabled", bool(self.tools_enabled))
        object.__setattr__(self, "planning_mode", PlanningMode(self.planning_mode))
        object.__setattr__(self, "metadata", freeze_json_mapping(self.metadata))
        object.__setattr__(self, "context_window", optional_positive_int(
            self.context_window, "context window"
        ))

    def latest_user_text(self) -> str:
        for message in reversed(self.messages):
            if message.role == "user":
                from purra.media import parse_static_image_content
                images = parse_static_image_content(message.content)
                if images is not None:
                    return images["text"]
                return str(message.content or "")
        return ""
