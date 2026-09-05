from __future__ import annotations

from dataclasses import replace

import pytest

from purra.agent_presets import (
    AgentComponentBinding,
    AgentPreset,
    AgentPresetSnapshot,
    PromptSection,
)
from purra.agent_tree_policy import AgentTreePolicy
from purra.context_orchestration import (
    ContextCompressionCoordinator,
    ContextCompressionSettings,
)
from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    ContextBundle,
    DomainContext,
    ExecutionState,
    MessageOrigin,
    MessageRole,
    ModelRequest,
    RuntimeLimits,
    ToolExecutionMode,
    ToolPolicy,
    ToolRiskLevel,
    ToolSchema,
)
from purra.model_protocol import generic_capability_snapshot
from purra.ports import ToolRegistration
from purra.recovery import RecoveryCause, RecoveryPolicy
from purra.tools import InMemoryToolCatalog


class _ContextProvider:
    async def build_context(self, request, budget, signal=None):
        del request, budget, signal
        return ContextBundle()


class _ExecutionStateFactory:
    def create(self, request):
        del request
        return ExecutionState()


def _request() -> AgentRunRequest:
    return AgentRunRequest(
        messages=(AgentMessage(role="user", content="work"),),
        model=ModelRequest(
            provider="test",
            model="test-model",
            capability_snapshot=replace(
                generic_capability_snapshot(),
                profile_id="test:model",
            ),
        ),
        domain_context=DomainContext(namespace="test.domain"),
    )


def test_preset_applies_ordered_trusted_prompt_sections():
    preset = AgentPreset(
        runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
        id="operations",
        revision="1",
        tool_catalog=InMemoryToolCatalog(()),
        context_provider=_ContextProvider(),
        component_bindings={
            "contextProvider": AgentComponentBinding("test.context", "1"),
        },
        prompt_sections=(
            PromptSection(name="rules", order=100, text="Use evidence."),
            PromptSection(name="identity", order=-100, text="You are PurrA."),
        ),
    )

    request = preset.apply(_request())

    assert [message.content for message in request.messages] == [
        "You are PurrA.",
        "Use evidence.",
        "work",
    ]
    assert all(
        message.origin is MessageOrigin.HOST_CONTEXT
        and message.role is MessageRole.SYSTEM
        for message in request.messages[:2]
    )
    assert preset.apply(request) is request
    with pytest.raises(ValueError, match="different AgentPreset"):
        AgentPreset(
            runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
            id="other",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
            context_provider=_ContextProvider(),
            component_bindings=preset.component_bindings,
            prompt_sections=(PromptSection(name="identity", text="Other."),),
        ).apply(request)


def test_preset_rejects_ambiguous_context_and_duplicate_sections():
    context = _ContextProvider()
    with pytest.raises(ValueError, match="mutually exclusive"):
        AgentPreset(
            runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
            id="operations",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
            context_provider=context,
            context_provider_factory=lambda _tasks: context,
        )
    assert AgentPreset(
        runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
        id="context-free",
        revision="1",
        tool_catalog=InMemoryToolCatalog(()),
    ).context_provider is None
    with pytest.raises(ValueError, match="must be unique"):
        AgentPreset(
            runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
            id="operations",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
            context_provider=context,
            prompt_sections=(
                PromptSection(name="identity", text="one"),
                PromptSection(name="identity", text="two"),
            ),
        )


def test_preset_snapshot_detects_prompt_or_tool_drift():
    context = _ContextProvider()
    preset = AgentPreset(
        runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
        id="operations",
        revision="1",
        tool_catalog=InMemoryToolCatalog(()),
        context_provider=context,
        component_bindings={
            "contextProvider": AgentComponentBinding("test.context", "1"),
        },
        prompt_sections=(PromptSection(name="identity", text="Be calm."),),
    )
    snapshot = preset.snapshot(_request())

    assert AgentPresetSnapshot.from_mapping(snapshot.to_mapping()) == snapshot
    preset.require_snapshot(snapshot, _request())

    changed = AgentPreset(

        runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
        id="operations",
        revision="1",
        tool_catalog=InMemoryToolCatalog(()),
        context_provider=context,
        component_bindings=preset.component_bindings,
        prompt_sections=(PromptSection(name="identity", text="Be terse."),),
    )
    with pytest.raises(ValueError, match="persisted snapshot"):
        changed.require_snapshot(snapshot, _request())


def test_preset_snapshot_covers_agent_tree_configuration():
    disabled = AgentPreset(
        runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
        id="root",
        revision="1",
        tool_catalog=InMemoryToolCatalog(()),
    )
    enabled = AgentPreset(
        runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
        id="root",
        revision="1",
        tool_catalog=InMemoryToolCatalog(()),
        agent_tree_policy=AgentTreePolicy(
            max_children_per_call=2,
            max_parallel_runs=1,
        ),
    )

    disabled_snapshot = disabled.snapshot(_request())
    enabled_snapshot = enabled.snapshot(_request())

    assert disabled_snapshot.composition["agentTree"] == {
        "protocolVersion": 1,
        "enabled": False,
    }
    assert enabled_snapshot.composition["agentTree"] == {
        "protocolVersion": 1,
        "enabled": True,
        "maxChildrenPerCall": 2,
        "maxParallelRuns": 1,
        "maxAgentNameChars": 64,
        "maxTitleChars": 120,
        "maxInstructionChars": 4_000,
        "maxObjectiveChars": 4_000,
        "maxDepth": 3,
        "maxAgentsPerRoot": 16,
        "allowsRecursiveAgents": False,
    }
    assert disabled_snapshot.fingerprint != enabled_snapshot.fingerprint


def test_preset_snapshot_requires_and_uses_opaque_component_bindings():
    with pytest.raises(ValueError, match="contextProvider"):
        AgentPreset(
            runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
            id="portable",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
            context_provider=_ContextProvider(),
        ).snapshot(_request())
    with pytest.raises(ValueError, match="executionStateFactory"):
        AgentPreset(
            runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
            id="portable",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
            execution_state_factory=_ExecutionStateFactory(),
        )

    first = AgentPreset(

        runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
        id="portable",
        revision="1",
        tool_catalog=InMemoryToolCatalog(()),
        context_provider=_ContextProvider(),
        component_bindings={
            "contextProvider": AgentComponentBinding(
                id="host.context",
                revision="1",
                config_digest="sha256:first",
            ),
        },
    )
    second = replace(
        first,
        component_bindings={
            "contextProvider": AgentComponentBinding(
                id="host.context",
                revision="2",
                config_digest="sha256:second",
            ),
        },
    )

    assert first.snapshot(_request()).fingerprint != second.snapshot(
        _request()
    ).fingerprint
    assert first.snapshot(_request()).fingerprint != replace(
        first,
        revision="2",
    ).snapshot(_request()).fingerprint


def test_preset_snapshot_derives_builtin_compaction_settings():
    first = AgentPreset(
        runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
        id="portable",
        revision="1",
        tool_catalog=InMemoryToolCatalog(()),
        conversation_compactor=ContextCompressionCoordinator(
            settings=ContextCompressionSettings(trigger_ratio=0.85),
        ),
    )
    second = replace(
        first,
        conversation_compactor=ContextCompressionCoordinator(
            settings=ContextCompressionSettings(trigger_ratio=0.5),
        ),
    )

    first_snapshot = first.snapshot(_request())
    second_snapshot = second.snapshot(_request())

    assert first_snapshot.fingerprint != second_snapshot.fingerprint
    assert first_snapshot.composition["conversationCompactor"]["settings"] == {
        "triggerRatio": 0.85,
        "defaultKeepRecentMessages": 20,
    }


def test_snapshot_v5_round_trip_rejects_old_or_incomplete_values():
    snapshot = AgentPreset(
        runtime_limits=RuntimeLimits(max_run_generation_tokens=None),
        id="portable",
        revision="1",
        tool_catalog=InMemoryToolCatalog(()),
    ).snapshot(_request())

    assert snapshot.snapshot_version == 5
    assert snapshot.to_mapping()["snapshotVersion"] == 5
    assert AgentPresetSnapshot.from_mapping(snapshot.to_mapping()) == snapshot

    legacy = snapshot.to_mapping()
    legacy.pop("snapshotVersion")
    with pytest.raises(ValueError, match="snapshot version"):
        AgentPresetSnapshot.from_mapping(legacy)

    version_four = snapshot.to_mapping()
    version_four["snapshotVersion"] = 4
    with pytest.raises(ValueError, match="snapshot version"):
        AgentPresetSnapshot.from_mapping(version_four)


def test_explicit_provider_generation_limit_stays_in_snapshot_v5():
    preset = AgentPreset(
        id="bounded-output",
        revision="1",
        tool_catalog=InMemoryToolCatalog(()),
        runtime_limits=RuntimeLimits(max_run_generation_tokens=None, max_provider_output_bytes=1_000_000),
    )

    snapshot = preset.snapshot(_request())
    recovered = AgentPresetSnapshot.from_mapping(snapshot.to_mapping())

    assert snapshot.snapshot_version == 5
    assert recovered.composition["runtimeLimits"]["maxProviderOutputBytes"] == 1_000_000
    assert RuntimeLimits(max_run_generation_tokens=None).max_provider_output_bytes == 8 * 1024 * 1024

async def _tool_handler(state, arguments, signal=None):
    del state, arguments, signal


def _catalog(mode: ToolExecutionMode) -> InMemoryToolCatalog:
    return InMemoryToolCatalog((ToolRegistration(
        schema=ToolSchema(
            name="readStatus",
            description="Read status.",
            parameters={"type": "object", "properties": {}},
        ),
        handler=_tool_handler,
        policy=ToolPolicy(
            mode=mode,
            title="Read status",
            risk_level=ToolRiskLevel.READ,
        ),
    ),))


def test_preset_snapshot_covers_operational_capability_drift():
    original = AgentPreset(
        id="operations",
        revision="1",
        tool_catalog=_catalog(ToolExecutionMode.READ),
        context_provider=_ContextProvider(),
        component_bindings={
            "contextProvider": AgentComponentBinding("test.context", "1"),
        },
        runtime_limits=RuntimeLimits(max_run_generation_tokens=None, max_model_rounds=4),
        recovery_policy=RecoveryPolicy().with_overrides({
            RecoveryCause.EMPTY_MODEL_RESPONSE: 1,
        }),
    )
    snapshot = original.snapshot(_request())
    composition = snapshot.composition

    assert composition["contextProvider"]["kind"] == "instance"
    assert composition["tools"][0]["policy"]["mode"] == "read"
    assert composition["runtimeLimits"]["maxModelRounds"] == 4
    assert composition["runtimeLimits"]["providerActivityIdleTimeoutMs"] == 30_000
    assert composition["runtimeLimits"]["providerProgressIdleTimeoutMs"] == 60_000
    assert composition["runtimeLimits"]["providerInvocationTimeoutMs"] == 300_000
    assert composition["recoveryPolicy"]["empty_model_response"] == 1

    changed = AgentPreset(
        id="operations",
        revision="1",
        tool_catalog=_catalog(ToolExecutionMode.CONFIRM),
        context_provider=_ContextProvider(),
        component_bindings=original.component_bindings,
        runtime_limits=RuntimeLimits(max_run_generation_tokens=None, max_model_rounds=4),
        recovery_policy=original.recovery_policy,
    )
    with pytest.raises(ValueError, match="persisted snapshot"):
        changed.require_snapshot(snapshot, _request())
