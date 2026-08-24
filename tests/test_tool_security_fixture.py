from __future__ import annotations

import json
from pathlib import Path

from purra.contracts import ToolCall
from purra.json_values import freeze_json_mapping
from purra.tools.security import ParsedToolCall, validate_tool_arguments_schema


_FIXTURE = json.loads(
    (Path(__file__).parents[1] / "conformance" / "fixtures" / "tool_security.json").read_text()
)


def test_shared_tool_argument_schema_cases() -> None:
    for case in _FIXTURE["argumentSchemaCases"]:
        failure = validate_tool_arguments_schema(
            ParsedToolCall(
                call=ToolCall(
                    id="fixture-call",
                    name=case["name"],
                    arguments_json=json.dumps(case["arguments"]),
                ),
                arguments=freeze_json_mapping(case["arguments"]),
            ),
            case["schema"],
        )
        assert (failure.code if failure is not None else None) == case["errorCode"]
