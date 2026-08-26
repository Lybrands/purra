from __future__ import annotations

import pytest

from purra.contracts import (
    ExecutionState,
    ToolBatchRequest,
    ToolCall,
    ToolHandlerResult,
    ToolPolicy,
    ToolSchema,
)
from purra.evaluation import (
    get_core_security_redteam_cases,
    run_security_redteam_cases,
)
from purra.errors import ContractViolationError
from purra.ports import ToolRegistration
from purra.tools import InMemoryToolCatalog
from purra.tools.executor import CoreToolExecutor
from purra.tools.security import ParsedToolCall, validate_tool_arguments_schema


async def _handler(state, arguments, signal=None):
    del state, arguments, signal


def _catalog(parameters):
    return InMemoryToolCatalog((ToolRegistration(
        schema=ToolSchema(
            name="inspectPayload",
            description="Inspect a portable payload.",
            parameters=parameters,
        ),
        handler=_handler,
        policy=ToolPolicy(mode="read", title="Inspect payload"),
    ),))


def test_catalog_rejects_unknown_nested_schema_type():
    with pytest.raises(ContractViolationError, match=r"properties\.payload\.type"):
        _catalog({
            "type": "object",
            "properties": {
                "payload": {"type": "definitely-not-json-schema"},
            },
        })


@pytest.mark.parametrize(
    ("parameters", "expected"),
    (
        (
            {"type": "object", "properties": {}, "patternProperties": {}},
            "unsupported keyword",
        ),
        (
            {"type": "object", "properties": {}, "required": ["x", "x"]},
            "required",
        ),
        (
            {"type": "object", "properties": {"x": {"type": []}}},
            "non-empty",
        ),
        (
            {"type": "object", "properties": {"x": {"anyOf": []}}},
            "anyOf",
        ),
        (
            {"type": "object", "properties": {"x": {"minLength": -1}}},
            "minLength",
        ),
        (
            {
                "type": "object",
                "properties": {"x": {"minimum": 2, "maximum": 1}},
            },
            "minimum",
        ),
        ({"type": "object", "properties": []}, "properties"),
        ({"type": "object", "required": "x"}, "required"),
        ({"type": "object", "additionalProperties": {}}, "additionalProperties"),
        ({"type": "array", "items": []}, "items"),
        ({"type": "object", "anyOf": {}}, "anyOf"),
        ({"type": "object", "oneOf": []}, "oneOf"),
        ({"type": "object", "enum": []}, "enum"),
        ({"type": "string", "title": 1}, "title"),
        ({"type": "string", "description": {}}, "description"),
        ({"type": ["object", "object"]}, "unique"),
        ({"type": "array", "maxItems": True}, "maxItems"),
        ({"type": "number", "maximum": "1"}, "maximum"),
        ({"type": "object", "properties": {"x": {"uniqueItems": "true"}}}, "uniqueItems"),
        ({"type": "object", "properties": {"x": {"uniqueItems": 1}}}, "uniqueItems"),
        ({"type": "object", "properties": {"x": {"uniqueItems": {}}}}, "uniqueItems"),
        ({"type": "object", "properties": {"x": {"uniqueItems": []}}}, "uniqueItems"),
        ({"type": "object", "properties": {"x": {"uniqueItems": None}}}, "uniqueItems"),
    ),
)
def test_catalog_rejects_malformed_or_unsupported_schema(parameters, expected):
    with pytest.raises(ContractViolationError, match=expected):
        _catalog(parameters)


def test_catalog_accepts_the_documented_schema_subset():
    catalog = _catalog({
        "type": "object",
        "title": "Payload",
        "description": "Bounded input",
        "properties": {
            "choice": {
                "anyOf": [
                    {"type": "string", "enum": ["a", "b"]},
                    {"type": "null"},
                ],
            },
            "items": {
                "type": "array",
                "minItems": 1,
                "maxItems": 2,
                "items": {
                    "oneOf": [
                        {"type": "integer", "minimum": 0, "maximum": 3},
                        {"type": "string", "minLength": 1, "maxLength": 4},
                    ],
                },
            },
            "flag": {"type": ["boolean", "null"], "const": True},
            "unique": {"uniqueItems": True},
            "duplicatesAllowed": {"uniqueItems": False},
        },
        "required": ["choice"],
        "additionalProperties": False,
    })

    assert catalog.names == frozenset({"inspectPayload"})


def test_runtime_unknown_type_defense_fails_closed():
    failure = validate_tool_arguments_schema(
        ParsedToolCall(
            call=ToolCall(
                id="call-1",
                name="inspectPayload",
                arguments_json='{"payload":{}}',
            ),
            arguments={"payload": {}},
        ),
        {
            "type": "object",
            "properties": {"payload": {"type": "unknown-runtime-type"}},
        },
    )

    assert failure is not None
    assert failure.code == "invalid_tool_arguments_schema"


@pytest.mark.parametrize(
    ("value", "schema", "valid"),
    (
        ("ok", {"type": "string", "minLength": 2, "maxLength": 2}, True),
        ("x", {"type": "string", "minLength": 2}, False),
        ("xxx", {"type": "string", "maxLength": 2}, False),
        ([1], {"type": "array", "minItems": 1, "maxItems": 1, "items": {"type": "integer"}}, True),
        ([], {"type": "array", "minItems": 1}, False),
        ([1, 2], {"type": "array", "maxItems": 1}, False),
        (["x"], {"type": "array", "items": {"type": "integer"}}, False),
        (2, {"type": "number", "minimum": 1, "maximum": 3}, True),
        (0, {"type": "number", "minimum": 1}, False),
        (4, {"type": "number", "maximum": 3}, False),
        ("a", {"enum": ["a", "b"]}, True),
        ("c", {"enum": ["a", "b"]}, False),
        (True, {"const": True}, True),
        (False, {"const": True}, False),
        (None, {"anyOf": [{"type": "string"}, {"type": "null"}]}, True),
        (1, {"anyOf": [{"type": "string"}, {"type": "null"}]}, False),
        (1, {"oneOf": [{"type": "integer"}, {"type": "number"}]}, False),
        ([1], {"type": "array"}, True),
    ),
)
def test_runtime_enforces_supported_assertions(value, schema, valid):
    failure = validate_tool_arguments_schema(
        ParsedToolCall(
            call=ToolCall(
                id="call-assertion",
                name="inspectPayload",
                arguments_json="{}",
            ),
            arguments={"value": value},
        ),
        {
            "type": "object",
            "properties": {"value": schema},
            "required": ["value"],
            "additionalProperties": False,
        },
    )

    assert (failure is None) is valid


def test_runtime_malformed_items_schema_fails_closed():
    failure = validate_tool_arguments_schema(
        ParsedToolCall(
            call=ToolCall(
                id="call-items",
                name="inspectPayload",
                arguments_json="{}",
            ),
            arguments={"value": [1]},
        ),
        {
            "type": "object",
            "properties": {"value": {"type": "array", "items": None}},
        },
    )

    assert failure is not None
    assert failure.code == "invalid_tool_arguments_schema"


def test_unique_items_failure_has_safe_structured_diagnostics():
    failure = validate_tool_arguments_schema(
        ParsedToolCall(
            call=ToolCall(
                id="call-unique",
                name="inspectPayload",
                arguments_json="{}",
            ),
            arguments={
                "value": [
                    {"secret": "do-not-echo"},
                    {"secret": "do-not-echo"},
                ],
            },
        ),
        {
            "type": "object",
            "properties": {"value": {"uniqueItems": True}},
        },
    )

    assert failure is not None
    assert failure.code == "invalid_tool_arguments_schema"
    assert failure.diagnostics == {
        "stage": "schema_validation",
        "toolName": "inspectPayload",
        "path": "$.value",
        "keyword": "uniqueItems",
        "firstIndex": 0,
        "duplicateIndex": 1,
    }
    assert "do-not-echo" not in failure.message


@pytest.mark.asyncio
async def test_unique_items_failure_rejects_the_whole_batch_before_handlers():
    executions = 0

    async def handler(state, arguments, signal=None):
        nonlocal executions
        del state, arguments, signal
        executions += 1
        return ToolHandlerResult('{"ok":true}')

    executor = CoreToolExecutor(InMemoryToolCatalog((ToolRegistration(
        schema=ToolSchema(
            name="inspectUnique",
            description="Inspect unique values.",
            parameters={
                "type": "object",
                "properties": {
                    "values": {"type": "array", "uniqueItems": True},
                },
                "required": ["values"],
                "additionalProperties": False,
            },
        ),
        handler=handler,
        policy=ToolPolicy(mode="read", title="Inspect unique values"),
    ),)))

    async def sink(event):
        del event

    result = await executor.execute_batch(
        ToolBatchRequest(
            run_id="run-unique",
            invocation_id="invocation-unique",
            calls=(
                ToolCall(
                    id="valid",
                    name="inspectUnique",
                    arguments_json='{"values":["a","b"]}',
                ),
                ToolCall(
                    id="invalid",
                    name="inspectUnique",
                    arguments_json='{"values":["a","a"]}',
                ),
            ),
            allowed_tool_names=frozenset({"inspectUnique"}),
            state=ExecutionState(),
        ),
        sink,
    )

    assert executions == 0
    assert result.outcome.value == "failed"
    assert all(
        item.error == "invalid_tool_arguments_schema"
        for item in result.results
    )


def test_complete_core_security_redteam_catalog_passes():
    report = run_security_redteam_cases(get_core_security_redteam_cases())

    assert report["summary"] == {"total": 5, "passed": 5, "failed": 0}
