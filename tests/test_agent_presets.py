from __future__ import annotations

from dataclasses import replace

import pytest

from purra.agent_presets import (
    AgentPreset,
    AgentPresetSnapshot,
    PromptSection,
)
from purra.contracts import (
    AgentMessage,
    AgentRunRequest,
    ContextBundle,
    DomainContext,
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
        id="operations",
        revision="1",
        tool_catalog=InMemoryToolCatalog(()),
        context_provider=_ContextProvider(),
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
            id="other",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
            context_provider=_ContextProvider(),
            prompt_sections=(PromptSection(name="identity", text="Other."),),
        ).apply(request)


def test_preset_rejects_ambiguous_context_and_duplicate_sections():
    context = _ContextProvider()
    with pytest.raises(ValueError, match="mutually exclusive"):
        AgentPreset(
            id="operations",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
            context_provider=context,
            context_provider_factory=lambda _tasks: context,
        )
    assert AgentPreset(
        id="context-free",
        revision="1",
        tool_catalog=InMemoryToolCatalog(()),
    ).context_provider is None
    with pytest.raises(ValueError, match="must be unique"):
        AgentPreset(
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
        id="operations",
        revision="1",
        tool_catalog=InMemoryToolCatalog(()),
        context_provider=context,
        prompt_sections=(PromptSection(name="identity", text="Be calm."),),
    )
    snapshot = preset.snapshot(_request())

    assert AgentPresetSnapshot.from_mapping(snapshot.to_mapping()) == snapshot
    preset.require_snapshot(snapshot, _request())

    changed = AgentPreset(
        id="operations",
        revision="1",
        tool_catalog=InMemoryToolCatalog(()),
        context_provider=context,
        prompt_sections=(PromptSection(name="identity", text="Be terse."),),
    )
    with pytest.raises(ValueError, match="persisted snapshot"):
        changed.require_snapshot(snapshot, _request())


def test_preset_snapshot_has_no_delegation_configuration():
    root = AgentPreset(
        id="root",
        revision="1",
        tool_catalog=InMemoryToolCatalog(()),
    )

    visible = root.snapshot(_request()).composition

    assert "delegatedAgents" not in visible
    assert "maxParallelDelegations" not in visible


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
        runtime_limits=RuntimeLimits(max_model_rounds=4),
        recovery_policy=RecoveryPolicy().with_overrides({
            RecoveryCause.EMPTY_MODEL_RESPONSE: 1,
        }),
    )
    snapshot = original.snapshot(_request())
    composition = snapshot.composition

    assert composition["contextProvider"]["kind"] == "instance"
    assert composition["tools"][0]["policy"]["mode"] == "read"
    assert composition["runtimeLimits"]["maxModelRounds"] == 4
    assert composition["recoveryPolicy"]["empty_model_response"] == 1

    changed = AgentPreset(
        id="operations",
        revision="1",
        tool_catalog=_catalog(ToolExecutionMode.CONFIRM),
        context_provider=_ContextProvider(),
        runtime_limits=RuntimeLimits(max_model_rounds=4),
        recovery_policy=original.recovery_policy,
    )
    with pytest.raises(ValueError, match="persisted snapshot"):
        changed.require_snapshot(snapshot, _request())
