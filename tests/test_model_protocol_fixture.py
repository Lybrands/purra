from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from purra.contracts import AgentMessage, ModelFinishReason, ToolCall
from purra.errors import UnsupportedModelFeatureError
from purra.json_values import thaw_json_mapping, thaw_json_value
from purra.model_protocol import (
    classify_model_termination,
    generic_capability_snapshot,
    resolve_invocation_output_limit,
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


def test_shared_output_limit_cases() -> None:
    for case in _FIXTURE["outputLimitCases"]:
        snapshot = replace(
            generic_capability_snapshot(),
            max_call_output_tokens=case["profileMaxTokens"],
        )
        if case["errorCode"] is not None:
            with pytest.raises(UnsupportedModelFeatureError) as captured:
                resolve_invocation_output_limit(snapshot, case["userOverride"])
            assert captured.value.code == case["errorCode"]
            continue
        limit = resolve_invocation_output_limit(snapshot, case["userOverride"])
        assert limit.to_mapping() == case["outputLimit"]
