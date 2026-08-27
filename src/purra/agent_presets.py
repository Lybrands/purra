"""Resolved, host-facing composition for one Agent style."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from hashlib import sha256
import json
from types import MappingProxyType
from typing import Any

from purra.context_orchestration import ContextCompressionCoordinator
from purra.delegation import DelegationPolicy

from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    MessageOrigin,
    MessageRole,
    RuntimeLimits,
)
from purra.execution_profiles import ExecutionProfile
from purra.model_execution import AgentModelTaskRunner
from purra.json_values import freeze_json_mapping, thaw_json_mapping
from purra.normalization import required_text
from purra.ports import (
    ContextProvider,
    ConversationCompactor,
    ExecutionStateFactory,
    ToolCatalog,
    ToolRegistration,
)
from purra.recovery import RecoveryPolicy
from purra.planning_policies import ReactivePlanningPolicy, ToolPlanningPolicy


ContextProviderFactory = Callable[[AgentModelTaskRunner], ContextProvider]
ConversationCompactorFactory = Callable[
    [AgentModelTaskRunner], ConversationCompactor
]

_COMPONENT_ROLES = frozenset({
    "contextProvider",
    "conversationCompactor",
    "executionStateFactory",
    "planner",
    "planningPolicy",
    "taskAdmissionEvaluator",
    "longTaskDispatcher",
})


@dataclass(frozen=True, slots=True)
class AgentComponentBinding:
    """Stable host identity for one opaque behavior-affecting component."""

    id: str
    revision: str
    config_digest: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", required_text(self.id, "component id"))
        object.__setattr__(
            self,
            "revision",
            required_text(self.revision, "component revision"),
        )
        digest = str(self.config_digest or "").strip() or None
        object.__setattr__(self, "config_digest", digest)

    def to_mapping(self) -> dict[str, str]:
        return {
            "id": self.id,
            "revision": self.revision,
            **(
                {"configDigest": self.config_digest}
                if self.config_digest is not None
                else {}
            ),
        }


@dataclass(frozen=True, slots=True)
class PromptSection:
    """One trusted, statically ordered identity or behavior section."""

    name: str
    text: str
    order: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", required_text(
            self.name,
            "prompt section name",
        ))
        object.__setattr__(self, "text", required_text(
            self.text,
            "prompt section text",
        ))
        if isinstance(self.order, bool):
            raise TypeError("prompt section order must be an integer")
        object.__setattr__(self, "order", int(self.order))

    def to_message(self, *, preset_identity: str | None = None) -> AgentMessage:
        metadata = {"promptSection": self.name}
        if preset_identity is not None:
            metadata["agentPreset"] = preset_identity
        return AgentMessage(
            role=MessageRole.SYSTEM,
            content=self.text,
            origin=MessageOrigin.HOST_CONTEXT,
            host_metadata=metadata,
        )

    def to_mapping(self) -> dict[str, Any]:
        return {"name": self.name, "order": self.order, "text": self.text}


@dataclass(frozen=True, slots=True)
class AgentPresetSnapshot:
    """Durable identity and exact capability composition of one Preset run."""

    id: str
    revision: str
    fingerprint: str
    composition: Mapping[str, Any]
    snapshot_version: int = 4

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", required_text(self.id, "agent preset id"))
        if self.snapshot_version not in {4, 5}:
            raise ValueError("agent preset snapshot version must be 4 or 5")
        object.__setattr__(
            self,
            "revision",
            required_text(self.revision, "agent preset revision"),
        )
        composition = freeze_json_mapping(self.composition)
        if self.snapshot_version == 5:
            agent_tree = composition.get("agentTree")
            if (
                not isinstance(agent_tree, Mapping)
                or agent_tree.get("protocolVersion") != 1
            ):
                raise ValueError(
                    "agent preset snapshot v5 requires Agent tree protocol v1"
                )
        object.__setattr__(self, "composition", composition)
        expected = _preset_fingerprint(
            self.id,
            self.revision,
            thaw_json_mapping(composition),
        )
        fingerprint = str(self.fingerprint or "").strip().lower()
        if fingerprint != expected:
            raise ValueError("agent preset snapshot fingerprint is invalid")
        object.__setattr__(self, "fingerprint", fingerprint)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "snapshotVersion": self.snapshot_version,
            "id": self.id,
            "revision": self.revision,
            "fingerprint": self.fingerprint,
            "composition": thaw_json_mapping(self.composition),
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "AgentPresetSnapshot":
        snapshot_version = value.get("snapshotVersion")
        if snapshot_version not in {4, 5}:
            raise ValueError("agent preset snapshot version must be 4 or 5")
        composition = value.get("composition")
        if not isinstance(composition, Mapping):
            raise TypeError("agent preset snapshot composition must be an object")
        return cls(
            id=str(value.get("id") or ""),
            revision=str(value.get("revision") or ""),
            fingerprint=str(value.get("fingerprint") or ""),
            composition=composition,
            snapshot_version=snapshot_version,
        )


@dataclass(frozen=True, slots=True)
class AgentPreset:
    """Immutable, fully resolved capabilities for one Agent composition.

    Product routing, database hydration, infrastructure construction and
    process cleanup deliberately stay outside this contract.
    """

    id: str
    revision: str
    tool_catalog: ToolCatalog
    runtime_limits: RuntimeLimits
    execution_profile: ExecutionProfile = field(
        default_factory=ExecutionProfile
    )
    prompt_sections: tuple[PromptSection, ...] = ()
    context_provider: ContextProvider | None = None
    context_provider_factory: ContextProviderFactory | None = None
    conversation_compactor: ConversationCompactor | None = None
    conversation_compactor_factory: ConversationCompactorFactory | None = None
    execution_state_factory: ExecutionStateFactory | None = None
    component_bindings: Mapping[str, AgentComponentBinding] = field(
        default_factory=dict
    )
    delegation_policy: DelegationPolicy | None = None
    recovery_policy: RecoveryPolicy = RecoveryPolicy()

    def __post_init__(self) -> None:
        object.__setattr__(self, "id", required_text(self.id, "agent preset id"))
        object.__setattr__(
            self,
            "revision",
            required_text(self.revision, "agent preset revision"),
        )
        if not isinstance(self.tool_catalog, ToolCatalog):
            raise TypeError("agent preset tool_catalog must implement ToolCatalog")
        if not isinstance(self.execution_profile, ExecutionProfile):
            raise TypeError(
                "agent preset execution_profile must be an ExecutionProfile"
            )
        bindings = dict(self.component_bindings)
        unknown_binding_roles = sorted(set(bindings) - _COMPONENT_ROLES)
        if unknown_binding_roles:
            raise ValueError(
                "unknown Agent component binding role(s): "
                + ", ".join(unknown_binding_roles)
            )
        if any(
            not isinstance(binding, AgentComponentBinding)
            for binding in bindings.values()
        ):
            raise TypeError(
                "agent preset component_bindings must contain "
                "AgentComponentBinding values"
            )
        object.__setattr__(
            self,
            "component_bindings",
            MappingProxyType(bindings),
        )
        sections = tuple(self.prompt_sections)
        if any(not isinstance(section, PromptSection) for section in sections):
            raise TypeError(
                "agent preset prompt_sections must contain PromptSection values"
            )
        names = tuple(section.name for section in sections)
        if len(names) != len(set(names)):
            raise ValueError("agent preset prompt section names must be unique")
        object.__setattr__(
            self,
            "prompt_sections",
            tuple(sorted(
                sections,
                key=lambda section: (section.order, section.name),
            )),
        )
        if (
            self.context_provider is not None
            and self.context_provider_factory is not None
        ):
            raise ValueError(
                "agent preset context provider and factory are mutually exclusive"
            )
        if self.context_provider is not None and not isinstance(
            self.context_provider,
            ContextProvider,
        ):
            raise TypeError(
                "agent preset context_provider must implement ContextProvider"
            )
        if self.context_provider_factory is not None and not callable(
            self.context_provider_factory
        ):
            raise TypeError("agent preset context_provider_factory must be callable")
        if self.execution_state_factory is not None and not isinstance(
            self.execution_state_factory,
            ExecutionStateFactory,
        ):
            raise TypeError(
                "agent preset execution_state_factory must implement "
                "ExecutionStateFactory"
            )
        if (
            self.conversation_compactor is not None
            and self.conversation_compactor_factory is not None
        ):
            raise ValueError(
                "agent preset conversation compactor and factory are mutually "
                "exclusive"
            )
        if self.conversation_compactor is not None and not isinstance(
            self.conversation_compactor,
            ConversationCompactor,
        ):
            raise TypeError(
                "agent preset conversation_compactor must implement "
                "ConversationCompactor"
            )
        if self.conversation_compactor_factory is not None and not callable(
            self.conversation_compactor_factory
        ):
            raise TypeError(
                "agent preset conversation_compactor_factory must be callable"
            )
        if not isinstance(self.runtime_limits, RuntimeLimits):
            raise TypeError("agent preset runtime_limits must be RuntimeLimits")
        if not isinstance(self.recovery_policy, RecoveryPolicy):
            raise TypeError("agent preset recovery_policy must be RecoveryPolicy")
        if self.delegation_policy is not None and not isinstance(
            self.delegation_policy,
            DelegationPolicy,
        ):
            raise TypeError(
                "agent preset delegation_policy must be DelegationPolicy or None"
            )
        self._validate_component_contract()

    @property
    def identity(self) -> str:
        return f"{self.id}@{self.revision}"

    def apply(self, request: AgentRunRequest) -> AgentRunRequest:
        """Materialize trusted prompt sections into the durable request."""

        if not self.prompt_sections:
            return request
        expected = tuple(
            section.to_message(preset_identity=self.identity)
            for section in self.prompt_sections
        )
        prefix = request.messages[:len(expected)]
        if prefix == expected:
            return request
        if any(
            message.host_metadata.get("agentPreset") is not None
            for message in request.messages
        ):
            raise ValueError(
                "request already contains a different AgentPreset composition"
            )
        return replace(
            request,
            messages=(
                *expected,
                *request.messages,
            ),
        )

    def snapshot(
        self,
        request: AgentRunRequest,
        *,
        tool_catalog: ToolCatalog | None = None,
    ) -> AgentPresetSnapshot:
        """Freeze the complete host-declared capability surface for this run."""

        effective_catalog = tool_catalog or self.tool_catalog
        enabled = effective_catalog.enabled_names(request)
        registrations = {
            registration.schema.name: registration
            for registration in effective_catalog.registrations()
        }
        unknown = set(enabled) - registrations.keys()
        if unknown:
            raise ValueError(
                "agent preset enabled unknown tools: " + ", ".join(sorted(unknown))
            )
        composition = {
            "promptSections": [
                section.to_mapping() for section in self.prompt_sections
            ],
            "executionProfile": self.execution_profile.snapshot_mapping(
                self._execution_component_binding
            ),
            "contextProvider": _component_binding(
                "contextProvider",
                self.context_provider,
                self.context_provider_factory,
                self.component_bindings,
            ),
            "conversationCompactor": _compactor_binding(
                self.conversation_compactor,
                self.conversation_compactor_factory,
                self.component_bindings,
            ),
            "executionStateFactory": _component_binding(
                "executionStateFactory",
                self.execution_state_factory,
                None,
                self.component_bindings,
                default_id="purra.execution-state.default",
            ),
            "tools": [
                _tool_registration_mapping(registrations[name])
                for name in sorted(enabled)
            ],
            "runtimeLimits": {
                "maxModelRounds": self.runtime_limits.max_model_rounds,
                "maxProgressRounds": self.runtime_limits.max_progress_rounds,
                "providerActivityIdleTimeoutMs": self.runtime_limits.provider_activity_idle_timeout_ms,
                "providerProgressIdleTimeoutMs": self.runtime_limits.provider_progress_idle_timeout_ms,
                "providerInvocationTimeoutMs": self.runtime_limits.provider_invocation_timeout_ms,
                "rootRunTimeoutMs": self.runtime_limits.root_run_timeout_ms,
                "maxModelInvocationAttempts": self.runtime_limits.max_model_invocation_attempts,
                "maxInputTokens": self.runtime_limits.max_input_tokens,
                "maxRunOutputTokens": (
                    self.runtime_limits.max_run_output_tokens
                ),
                "maxReasoningTokens": self.runtime_limits.max_reasoning_tokens,
                "maxProviderOutputEvents": self.runtime_limits.max_provider_output_events,
                "maxProviderOutputBytes": self.runtime_limits.max_provider_output_bytes,
                "maxStreamContentChars": self.runtime_limits.max_stream_content_chars,
                "maxStreamReasoningChars": self.runtime_limits.max_stream_reasoning_chars,
                "maxStreamChunks": self.runtime_limits.max_stream_chunks,
            },
            "recoveryPolicy": {
                cause.value: attempts
                for cause, attempts in sorted(
                    self.recovery_policy.attempt_limits.items(),
                    key=lambda item: item[0].value,
                )
            },
            "delegation": (
                self.delegation_policy.snapshot_mapping()
                if self.delegation_policy is not None
                else {"enabled": False}
            ),
        }
        return AgentPresetSnapshot(
            id=self.id,
            revision=self.revision,
            fingerprint=_preset_fingerprint(self.id, self.revision, composition),
            composition=composition,
        )

    def require_snapshot(
        self,
        snapshot: AgentPresetSnapshot,
        request: AgentRunRequest,
        *,
        tool_catalog: ToolCatalog | None = None,
    ) -> None:
        """Fail closed when recovery would use a different composition."""

        if not isinstance(snapshot, AgentPresetSnapshot):
            raise TypeError("agent preset recovery requires AgentPresetSnapshot")
        current = self.snapshot(request, tool_catalog=tool_catalog)
        if current != snapshot:
            raise ValueError(
                "agent preset composition does not match the persisted snapshot"
            )

    def _execution_component_binding(
        self,
        role: str,
        value: object | None,
    ) -> Mapping[str, Any]:
        builtin_id = None
        if isinstance(value, ReactivePlanningPolicy):
            builtin_id = "purra.planning.reactive"
        elif isinstance(value, ToolPlanningPolicy):
            builtin_id = "purra.planning.tool"
        return _component_binding(
            role,
            value,
            None,
            self.component_bindings,
            known_builtin=builtin_id is not None,
            default_id=(
                builtin_id
                if builtin_id is not None
                else f"purra.execution-profile.{role}.none"
                if value is None
                else None
            ),
        )

    def _validate_component_contract(self) -> None:
        _component_binding(
            "contextProvider",
            self.context_provider,
            self.context_provider_factory,
            self.component_bindings,
        )
        _compactor_binding(
            self.conversation_compactor,
            self.conversation_compactor_factory,
            self.component_bindings,
        )
        _component_binding(
            "executionStateFactory",
            self.execution_state_factory,
            None,
            self.component_bindings,
            default_id="purra.execution-state.default",
        )
        self.execution_profile.snapshot_mapping(
            self._execution_component_binding
        )

def _fingerprint(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def _preset_fingerprint(
    preset_id: str,
    revision: str,
    composition: Mapping[str, Any],
) -> str:
    return _fingerprint({
        "id": preset_id,
        "revision": revision,
        "composition": composition,
    })


def _component_binding(
    role: str,
    instance: object | None,
    factory: object | None,
    bindings: Mapping[str, AgentComponentBinding],
    *,
    known_builtin: bool = False,
    default_id: str | None = None,
) -> dict[str, Any]:
    selected = factory if factory is not None else instance
    kind = (
        "factory"
        if factory is not None
        else "instance"
        if instance is not None
        else "default"
    )
    if selected is None:
        return {
            "kind": "builtin",
            "id": default_id or f"purra.{role}.default",
            "revision": "1",
        }
    binding = bindings.get(role)
    if binding is None:
        if known_builtin and default_id is not None:
            return {
                "kind": "builtin",
                "id": default_id,
                "revision": "1",
                "type": _component_type(selected),
            }
        raise ValueError(
            f"opaque Agent component {role} requires a component binding"
        )
    return {
        "kind": kind,
        "type": _component_type(selected),
        "binding": binding.to_mapping(),
    }


def _compactor_binding(
    instance: object | None,
    factory: object | None,
    bindings: Mapping[str, AgentComponentBinding],
) -> dict[str, Any]:
    if factory is None and (
        instance is None
        or (
            isinstance(instance, ContextCompressionCoordinator)
            and instance.hook is None
        )
    ):
        coordinator = (
            instance
            if isinstance(instance, ContextCompressionCoordinator)
            else ContextCompressionCoordinator()
        )
        return {
            "kind": "builtin",
            "id": "purra.context.compaction",
            "revision": "1",
            "settings": {
                "triggerRatio": coordinator.settings.trigger_ratio,
                "defaultKeepRecentMessages": (
                    coordinator.settings.default_keep_recent_messages
                ),
            },
        }
    return _component_binding(
        "conversationCompactor",
        instance,
        factory,
        bindings,
    )


def _component_type(value: object) -> str:
    module = getattr(value, "__module__", None)
    qualname = getattr(value, "__qualname__", None)
    if not module or not qualname:
        kind = type(value)
        module, qualname = kind.__module__, kind.__qualname__
    return f"{module}.{qualname}"


def _tool_schema_mapping(schema: object) -> dict[str, Any]:
    return {
        "name": schema.name,
        "description": schema.description,
        "parameters": thaw_json_mapping(schema.parameters),
        "displayNames": thaw_json_mapping(schema.display_names),
    }


def _tool_registration_mapping(
    registration: ToolRegistration,
) -> dict[str, Any]:
    context = registration.context_contract
    data = registration.data_contract
    host_arguments = registration.host_planned_arguments
    return {
        **_tool_schema_mapping(registration.schema),
        "policy": {
            "mode": registration.policy.mode.value,
            "title": registration.policy.title,
            "riskLevel": registration.policy.risk_level.value,
        },
        "contextContract": {
            "prerequisiteTools": list(context.prerequisite_tools),
            "mandatoryContextKeys": list(context.mandatory_context_keys),
            "requiredContextBlocks": list(context.required_context_blocks),
            "evidenceKinds": list(context.evidence_kinds),
            "produces": list(context.produces),
            "resultProjection": context.result_projection.value,
            "finalProjection": context.final_projection.value,
        },
        "dataContract": {
            "modelOwnedPaths": list(data.model_owned_paths),
            "hostBoundPaths": list(data.host_bound_paths),
            "hostDerivedPaths": list(data.host_derived_paths),
            "payloadMode": data.payload_mode.value,
        },
        "cancellationLinearizable": registration.cancellation_linearizable,
        "hostManagedDurability": registration.host_managed_durability,
        "maxArgumentChars": registration.max_argument_chars,
        "planningCapability": (
            _tool_schema_mapping(registration.planning_capability)
            if registration.planning_capability is not None
            else None
        ),
        "hostPlannedArgumentsDigest": (
            _fingerprint(thaw_json_mapping(host_arguments))
            if host_arguments is not None
            else None
        ),
    }


__all__ = [
    "AgentComponentBinding",
    "AgentPreset",
    "AgentPresetSnapshot",
    "ContextProviderFactory",
    "ConversationCompactorFactory",
    "PromptSection",
]
