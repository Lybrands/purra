from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from purra.contracts import AgentMessage, ModelFinishReason, ToolCall
from purra.errors import UnsupportedModelFeatureError
from purra.context_budget import max_generation_tokens_for_context
from purra.json_values import thaw_json_mapping, thaw_json_value
from purra.model_protocol import (
    GenerationBudgetSource,
    ResultCapacitySource,
    classify_model_termination,
    constrain_output_budget_to_context,
    generic_capability_snapshot,
    resolve_invocation_output_budget,
)


_FIXTURE = json.loads(
    (Path(__file__).parents[1] / "conformance" / "fixtures" / "model_protocol.json").read_text()
)


def test_shared_message_and_tool_call_cases() -> None:
    for case in _FIXTURE["messageCases"]:
        message = AgentMessage(
            role=case["role"],
            content=case["content"],
            reasoning=case.get("reasoning"),
            tool_call_id=case.get("toolCallId"),
            tool_calls=tuple(
                ToolCall(
                    id=call["id"],
                    name=call["name"],
                    arguments_json=json.dumps(
                        call["arguments"],
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                )
                for call in case.get("toolCalls", [])
            ),
            attributes=case["attributes"],
        )

        assert message.role.value == case["role"]
        assert thaw_json_value(message.content) == case["content"]
        assert message.reasoning == case.get("reasoning")
        assert message.tool_call_id == case.get("toolCallId")
        assert thaw_json_mapping(message.attributes) == case["attributes"]
        assert [
            {
                "id": call.id,
                "name": call.name,
                "arguments": json.loads(call.arguments_json),
            }
            for call in message.tool_calls
        ] == case.get("toolCalls", [])


def test_shared_model_termination_cases() -> None:
    for case in _FIXTURE["terminationCases"]:
        termination = classify_model_termination(
            ModelFinishReason(case["finishReason"]),
            tool_call_count=case["toolCallCount"],
        )
        assert termination.incomplete is case["incomplete"]
        assert termination.authorizes_tool_calls is case["authorizesToolCalls"]
        assert termination.error_code == case["errorCode"]


def test_shared_output_budget_cases() -> None:
    for case in _FIXTURE["outputBudgetCases"]:
        snapshot = replace(
            generic_capability_snapshot(),
            max_generation_tokens=case["profileMaxGenerationTokens"],
        )
        arguments = {
            "max_generation_tokens": case["maxGenerationTokens"],
            "generation_source": (
                GenerationBudgetSource(case["generationSource"])
                if case["generationSource"] is not None
                else None
            ),
            "result_capacity_target_tokens": case[
                "resultCapacityTargetTokens"
            ],
            "result_capacity_source": (
                ResultCapacitySource(case["resultCapacitySource"])
                if case["resultCapacitySource"] is not None
                else None
            ),
        }
        if case["errorCode"] is not None:
            with pytest.raises(UnsupportedModelFeatureError) as captured:
                resolve_invocation_output_budget(snapshot, **arguments)
            assert captured.value.code == case["errorCode"]
            continue
        budget = resolve_invocation_output_budget(snapshot, **arguments)
        assert budget.to_mapping() == case["outputBudget"]


@pytest.mark.parametrize(
    ("context_window", "expected_generation_tokens", "expected_source"),
    (
        (1_000_000, 393_216, GenerationBudgetSource.MODEL_PROFILE),
        (262_144, 235_930, GenerationBudgetSource.CONTEXT_CAPACITY),
        (32_768, 25_600, GenerationBudgetSource.CONTEXT_CAPACITY),
    ),
)
def test_context_capacity_resolves_generation_without_workflow_clamping(
    context_window,
    expected_generation_tokens,
    expected_source,
) -> None:
    snapshot = replace(
        generic_capability_snapshot(),
        context_window_tokens=context_window,
        max_generation_tokens=393_216,
    )
    budget = resolve_invocation_output_budget(
        snapshot,
        max_generation_tokens=None,
    )

    fitted = constrain_output_budget_to_context(
        budget,
        max_generation_tokens=max_generation_tokens_for_context(
            window_tokens=context_window,
        ),
    )

    assert fitted.max_generation_tokens == expected_generation_tokens
    assert fitted.generation_source is expected_source
    assert fitted.profile_max_generation_tokens == 393_216


def test_context_capacity_rejects_an_unrepresentable_result_target() -> None:
    snapshot = replace(
        generic_capability_snapshot(),
        context_window_tokens=32_768,
        max_generation_tokens=393_216,
    )
    budget = resolve_invocation_output_budget(
        snapshot,
        max_generation_tokens=None,
        result_capacity_target_tokens=30_000,
        result_capacity_source=ResultCapacitySource.WORKFLOW_POLICY,
    )

    with pytest.raises(UnsupportedModelFeatureError) as captured:
        constrain_output_budget_to_context(
            budget,
            max_generation_tokens=max_generation_tokens_for_context(
                window_tokens=32_768,
            ),
        )

    assert captured.value.code == "model_result_capacity_incompatible"
