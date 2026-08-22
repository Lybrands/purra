"""Strict, provider-neutral tool input/output security helpers."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from purra.contracts import ToolCall, ToolExecutionLimits
from purra.json_values import freeze_json_mapping, thaw_json_mapping
_ERROR_CODE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,127}$")
_BEARER = re.compile(r"(?i)bearer\s+[a-z0-9._~+/=-]+")
_API_KEY = re.compile(r"(?i)\b(?:sk|key)-[a-z0-9_-]{8,}\b")
_WINDOWS_PATH = re.compile(r"[A-Za-z]:\\[^\r\n\"']+")
_POSIX_PATH = re.compile(
    r"(?<![:/A-Za-z0-9])/(?:[^/\s\"'\\]+/)+[^/\s\"'\\]*"
)


@dataclass(frozen=True, slots=True)
class ToolSecurityFailure:
    code: str
    message: str
    diagnostics: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ParsedToolCall:
    call: ToolCall
    arguments: Mapping[str, Any]
    normalized_argument_paths: tuple[str, ...] = ()


def preflight_tool_calls(
    calls: Sequence[ToolCall],
    limits: ToolExecutionLimits,
    *,
    argument_limits: Mapping[str, int] | None = None,
) -> tuple[tuple[ParsedToolCall, ...], ToolSecurityFailure | None]:
    """Validate the complete model-generated batch before any side effect."""

    if len(calls) > limits.max_calls_per_batch:
        return (), ToolSecurityFailure(
            "too_many_tool_calls",
            f"Too many tool calls; maximum is {limits.max_calls_per_batch}.",
        )
    ids = [str(call.id or "").strip() for call in calls]
    if any(not call_id for call_id in ids):
        return (), ToolSecurityFailure(
            "invalid_tool_call_id",
            "Every tool call must have a non-empty id.",
        )
    if len(ids) != len(set(ids)):
        return (), ToolSecurityFailure(
            "duplicate_tool_call_id",
            "Tool call ids must be unique within a batch.",
        )
    if any(not str(call.name or "").strip() for call in calls):
        return (), ToolSecurityFailure(
            "invalid_tool_name",
            "Every tool call must have a function name.",
        )

    parsed: list[ParsedToolCall] = []
    for call in calls:
        tool_limit = (
            argument_limits.get(str(call.name), limits.max_argument_chars)
            if argument_limits is not None
            else limits.max_argument_chars
        )
        arguments, failure = parse_tool_arguments(
            call.arguments_json,
            max_chars=tool_limit,
            tool_name=str(call.name),
        )
        if failure is not None or arguments is None:
            return (), failure
        parsed.append(ParsedToolCall(call=call, arguments=arguments))
    return tuple(parsed), None


def parse_tool_arguments(
    raw: Any,
    *,
    max_chars: int = 1_000_000,
    tool_name: str = "",
) -> tuple[Mapping[str, Any] | None, ToolSecurityFailure | None]:
    normalized_tool_name = str(tool_name or "").strip()
    base_diagnostics = {
        "stage": "transport_preflight",
        **(
            {"toolName": normalized_tool_name}
            if normalized_tool_name
            else {}
        ),
    }
    if not isinstance(raw, str):
        return None, ToolSecurityFailure(
            "invalid_tool_arguments_type",
            "Tool arguments must be a JSON object string.",
            {
                **base_diagnostics,
                "actualType": type(raw).__name__,
            },
        )
    if len(raw) > int(max_chars):
        return None, ToolSecurityFailure(
            "tool_arguments_too_large",
            (
                f"Tool {normalized_tool_name or '<unknown>'} arguments use "
                f"{len(raw)} transport characters, exceeding the "
                f"{int(max_chars)}-character safety limit."
            ),
            {
                **base_diagnostics,
                "actualChars": len(raw),
                "maxChars": int(max_chars),
                "measurement": "raw_json_transport_chars",
            },
        )
    try:
        value = json.loads(raw or "{}")
    except json.JSONDecodeError as error:
        return None, ToolSecurityFailure(
            "invalid_tool_arguments_json",
            (
                f"Tool {normalized_tool_name or '<unknown>'} arguments are not "
                f"valid JSON at line {error.lineno}, column {error.colno}: "
                f"{error.msg}."
            ),
            {
                **base_diagnostics,
                "stage": "json_decode",
                "line": error.lineno,
                "column": error.colno,
                "offset": error.pos,
                "actualChars": len(raw),
                "jsonError": error.msg,
            },
        )
    if not isinstance(value, dict):
        return None, ToolSecurityFailure(
            "invalid_tool_arguments_shape",
            "Tool arguments must decode to a JSON object.",
            {
                **base_diagnostics,
                "stage": "json_shape",
                "actualType": _json_type_name(value),
            },
        )
    try:
        return freeze_json_mapping(value), None
    except (TypeError, ValueError):
        return None, ToolSecurityFailure(
            "invalid_tool_arguments_value",
            "Tool arguments contain an unsupported JSON value.",
            {
                **base_diagnostics,
                "stage": "json_value",
            },
        )


def normalize_tool_arguments_to_schema(
    parsed: ParsedToolCall,
    parameters: Mapping[str, Any],
) -> tuple[ParsedToolCall | None, ToolSecurityFailure | None]:
    """Recover provider-stringified arrays/objects using the registered schema.

    Some OpenAI-compatible providers occasionally serialize an array or object
    property as a JSON-looking string even though the function schema declares
    a structured value.  The top-level arguments remain valid JSON, so ordinary
    preflight parsing cannot detect the shape loss.  Normalize only fields whose
    registered schema explicitly requires an array/object, and fail closed when
    such a string cannot be decoded.

    The fallback quote repair is intentionally narrow: it only escapes a quote
    that cannot legally terminate the current JSON string.  It does not add or
    remove containers, keys, commas, or values.
    """

    value, paths, failure = _normalize_schema_value(
        parsed.arguments,
        parameters,
        path="$",
    )
    if failure is not None:
        return None, failure
    if not isinstance(value, Mapping):
        return None, ToolSecurityFailure(
            "invalid_tool_arguments_schema",
            "Tool arguments do not match the registered object schema.",
        )
    if not paths:
        return parsed, None
    try:
        arguments = freeze_json_mapping(value)
    except (TypeError, ValueError):
        return None, ToolSecurityFailure(
            "invalid_tool_arguments_value",
            "Normalized tool arguments contain an unsupported JSON value.",
        )
    return ParsedToolCall(
        call=parsed.call,
        arguments=arguments,
        normalized_argument_paths=tuple(paths),
    ), None


def validate_tool_arguments_schema(
    parsed: ParsedToolCall,
    parameters: Mapping[str, Any],
) -> ToolSecurityFailure | None:
    """Enforce the bounded JSON-Schema subset exposed to the model.

    Provider-side function schemas guide generation but are not an authority
    boundary. Core validates the normalized value again before scope checks or
    side effects. The supported keywords cover the schemas registered by the
    built-in domains; admission rejects unsupported keywords before runtime.
    """

    violation = _schema_violation(parsed.arguments, parameters, path="$")
    if violation is None:
        return None
    message, details = violation
    return ToolSecurityFailure(
        "invalid_tool_arguments_schema",
        f"Tool {parsed.call.name} {message}",
        {
            "stage": "schema_validation",
            "toolName": parsed.call.name,
            **details,
        },
    )


def _schema_violation(
    value: Any,
    schema: object,
    *,
    path: str,
) -> tuple[str, dict[str, Any]] | None:
    if not isinstance(schema, Mapping):
        return (
            f"argument {path} cannot be checked against a malformed schema.",
            {"path": path, "keyword": "schema"},
        )

    any_of = schema.get("anyOf")
    if _is_json_array(any_of):
        branch_violations = [
            _schema_violation(value, branch, path=path)
            for branch in any_of
        ]
        if not any(violation is None for violation in branch_violations):
            preferred = _preferred_union_violation(
                value,
                any_of,
                branch_violations,
            )
            if preferred is not None:
                return preferred
            return (
                f"argument {path} does not match any allowed schema variant.",
                {
                    "path": path,
                    "keyword": "anyOf",
                    "variantCount": len(any_of),
                },
            )

    one_of = schema.get("oneOf")
    if _is_json_array(one_of):
        branch_violations = [
            _schema_violation(value, branch, path=path)
            for branch in one_of
        ]
        match_count = sum(
            violation is None for violation in branch_violations
        )
        if match_count != 1:
            if match_count == 0:
                preferred = _preferred_union_violation(
                    value,
                    one_of,
                    branch_violations,
                )
                if preferred is not None:
                    return preferred
            return (
                f"argument {path} must match exactly one schema variant.",
                {
                    "path": path,
                    "keyword": "oneOf",
                    "variantCount": len(one_of),
                    "matchedVariants": match_count,
                },
            )

    expected_type = schema.get("type")
    if expected_type is not None and not _matches_json_schema_type(
        value,
        expected_type,
    ):
        expected_label = (
            ", ".join(str(item) for item in expected_type)
            if _is_json_array(expected_type)
            else str(expected_type)
        )
        actual_type = _json_type_name(value)
        return (
            f"argument {path} must be {expected_label}; got {actual_type}.",
            {
                "path": path,
                "keyword": "type",
                "expected": expected_label,
                "actualType": actual_type,
            },
        )

    enum = schema.get("enum")
    if _is_json_array(enum) and not any(
        _json_values_equal(value, candidate) for candidate in enum
    ):
        return (
            f"argument {path} is not one of the allowed values.",
            {
                "path": path,
                "keyword": "enum",
                "allowedValues": list(enum),
            },
        )

    if "const" in schema and not _json_values_equal(value, schema.get("const")):
        return (
            f"argument {path} does not match the required constant value.",
            {
                "path": path,
                "keyword": "const",
            },
        )

    if isinstance(value, str):
        min_length = schema.get("minLength")
        max_length = schema.get("maxLength")
        if _is_schema_integer(min_length) and len(value) < min_length:
            return (
                f"argument {path} is shorter than minLength {min_length} "
                f"(actual {len(value)}).",
                {
                    "path": path,
                    "keyword": "minLength",
                    "actualChars": len(value),
                    "minChars": min_length,
                    "measurement": "decoded_string_chars",
                },
            )
        if _is_schema_integer(max_length) and len(value) > max_length:
            return (
                f"argument {path} exceeds maxLength {max_length} "
                f"(actual {len(value)}).",
                {
                    "path": path,
                    "keyword": "maxLength",
                    "actualChars": len(value),
                    "maxChars": max_length,
                    "measurement": "decoded_string_chars",
                },
            )

    if _is_json_array(value):
        min_items = schema.get("minItems")
        max_items = schema.get("maxItems")
        if _is_schema_integer(min_items) and len(value) < min_items:
            return (
                f"argument {path} has fewer than minItems {min_items} "
                f"(actual {len(value)}).",
                {
                    "path": path,
                    "keyword": "minItems",
                    "actualItems": len(value),
                    "minItems": min_items,
                },
            )
        if _is_schema_integer(max_items) and len(value) > max_items:
            return (
                f"argument {path} exceeds maxItems {max_items} "
                f"(actual {len(value)}).",
                {
                    "path": path,
                    "keyword": "maxItems",
                    "actualItems": len(value),
                    "maxItems": max_items,
                },
            )
        if "items" in schema:
            item_schema = schema.get("items")
            for index, item in enumerate(value):
                violation = _schema_violation(
                    item,
                    item_schema,
                    path=f"{path}[{index}]",
                )
                if violation is not None:
                    return violation

    if isinstance(value, Mapping):
        properties = schema.get("properties")
        declared = properties if isinstance(properties, Mapping) else {}
        required = schema.get("required")
        if _is_json_array(required):
            for required_name in required:
                name = str(required_name)
                if name not in value:
                    required_path = f"{path}.{name}"
                    return (
                        f"argument {required_path} is required.",
                        {
                            "path": required_path,
                            "keyword": "required",
                        },
                    )
        if schema.get("additionalProperties") is False:
            unknown = sorted(str(key) for key in value if key not in declared)
            if unknown:
                return (
                    f"argument {path} contains undeclared properties: "
                    f"{', '.join(unknown)}.",
                    {
                        "path": path,
                        "keyword": "additionalProperties",
                        "unknownProperties": unknown,
                    },
                )
        for key, item in value.items():
            child_schema = declared.get(key)
            if child_schema is None:
                continue
            violation = _schema_violation(
                item,
                child_schema,
                path=f"{path}.{key}",
            )
            if violation is not None:
                return violation

    if _is_json_number(value):
        minimum = schema.get("minimum")
        maximum = schema.get("maximum")
        if _is_json_number(minimum) and value < minimum:
            return (
                f"argument {path} is below minimum {minimum} "
                f"(actual {value}).",
                {
                    "path": path,
                    "keyword": "minimum",
                    "actual": value,
                    "minimum": minimum,
                },
            )
        if _is_json_number(maximum) and value > maximum:
            return (
                f"argument {path} exceeds maximum {maximum} "
                f"(actual {value}).",
                {
                    "path": path,
                    "keyword": "maximum",
                    "actual": value,
                    "maximum": maximum,
                },
            )

    return None


def _matches_json_schema_type(value: Any, expected: Any) -> bool:
    if _is_json_array(expected):
        return any(_matches_json_schema_type(value, item) for item in expected)
    expected_name = str(expected or "")
    return {
        "object": isinstance(value, Mapping),
        "array": _is_json_array(value),
        "string": isinstance(value, str),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "number": _is_json_number(value),
        "boolean": isinstance(value, bool),
        "null": value is None,
    }.get(expected_name, False)


def _json_type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, Mapping):
        return "object"
    if _is_json_array(value):
        return "array"
    if isinstance(value, str):
        return "string"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    return type(value).__name__


def _is_json_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_schema_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _json_values_equal(left: Any, right: Any) -> bool:
    if type(left) is not type(right):
        return False
    return left == right


def _normalize_schema_value(
    value: Any,
    schema: object,
    *,
    path: str,
) -> tuple[Any, list[str], ToolSecurityFailure | None]:
    if not isinstance(schema, Mapping):
        return value, [], None
    union = schema.get("anyOf")
    if not _is_json_array(union):
        union = schema.get("oneOf")
    if _is_json_array(union):
        branch = _select_union_branch(value, union)
        if branch is not None:
            return _normalize_schema_value(value, branch, path=path)
    expected_type = schema.get("type")
    normalized_paths: list[str] = []

    is_structured_type = expected_type == "array" or expected_type == "object"
    if is_structured_type and isinstance(value, str):
        decoded = _decode_stringified_structure(value, expected_type)
        if decoded is None:
            return value, [], ToolSecurityFailure(
                "invalid_tool_arguments_schema",
                f"Tool argument {path} must be a JSON {expected_type}; "
                "the string-encoded value could not be decoded.",
            )
        value = decoded
        normalized_paths.append(path)

    if expected_type == "array" and not _is_json_array(value):
        return value, [], ToolSecurityFailure(
            "invalid_tool_arguments_schema",
            f"Tool argument {path} must be a JSON array.",
        )
    if expected_type == "object" and not isinstance(value, Mapping):
        return value, [], ToolSecurityFailure(
            "invalid_tool_arguments_schema",
            f"Tool argument {path} must be a JSON object.",
        )

    if expected_type == "object" and isinstance(value, Mapping):
        properties = schema.get("properties")
        if not isinstance(properties, Mapping):
            return value, normalized_paths, None
        normalized_object: dict[str, Any] = {}
        for key, item in value.items():
            property_schema = properties.get(key)
            normalized_item, child_paths, failure = _normalize_schema_value(
                item,
                property_schema,
                path=f"{path}.{key}",
            )
            if failure is not None:
                return value, [], failure
            normalized_object[str(key)] = normalized_item
            normalized_paths.extend(child_paths)
        return normalized_object, normalized_paths, None

    if expected_type == "array" and _is_json_array(value):
        item_schema = schema.get("items")
        normalized_array: list[Any] = []
        for index, item in enumerate(value):
            normalized_item, child_paths, failure = _normalize_schema_value(
                item,
                item_schema,
                path=f"{path}[{index}]",
            )
            if failure is not None:
                return value, [], failure
            normalized_array.append(normalized_item)
            normalized_paths.extend(child_paths)
        return normalized_array, normalized_paths, None

    return value, normalized_paths, None


def _select_union_branch(value: Any, branches: Sequence[Any]) -> object | None:
    """Select a discriminated JSON-Schema branch for normalization.

    Built-in tool unions use a singleton ``enum`` or ``const`` discriminator.
    Selecting it before validation preserves the provider compatibility repair
    for nested arrays/objects without guessing between unrelated variants.
    """

    if isinstance(value, Mapping):
        for branch in branches:
            if not isinstance(branch, Mapping):
                continue
            properties = branch.get("properties")
            if not isinstance(properties, Mapping):
                continue
            for key, property_schema in properties.items():
                if key not in value or not isinstance(property_schema, Mapping):
                    continue
                expected = property_schema.get("const")
                if "const" in property_schema and _json_values_equal(
                    value[key],
                    expected,
                ):
                    return branch
                enum = property_schema.get("enum")
                if (
                    _is_json_array(enum)
                    and len(enum) == 1
                    and _json_values_equal(value[key], enum[0])
                ):
                    return branch
    matching = [
        branch
        for branch in branches
        if _schema_violation(value, branch, path="$") is None
    ]
    return matching[0] if len(matching) == 1 else None


def _preferred_union_violation(
    value: Any,
    branches: Sequence[Any],
    violations: Sequence[tuple[str, dict[str, Any]] | None],
) -> tuple[str, dict[str, Any]] | None:
    branch = _select_union_branch(value, branches)
    if branch is None:
        return None
    for index, candidate in enumerate(branches):
        if candidate is branch:
            return violations[index]
    return None


def _decode_stringified_structure(value: str, expected_type: str) -> Any | None:
    text = value.strip()
    opening, closing, expected_python_type = (
        ("[", "]", list)
        if expected_type == "array"
        else ("{", "}", dict)
    )
    if not text.startswith(opening) or not text.endswith(closing):
        return None
    try:
        decoded = json.loads(text)
    except json.JSONDecodeError:
        repaired = _escape_nonterminating_json_quotes(text)
        if repaired == text:
            return None
        try:
            decoded = json.loads(repaired)
        except json.JSONDecodeError:
            return None
    return decoded if isinstance(decoded, expected_python_type) else None


def _escape_nonterminating_json_quotes(value: str) -> str:
    """Escape only quotes that cannot legally close their current JSON string."""

    result: list[str] = []
    in_string = False
    escaped = False
    length = len(value)
    for index, character in enumerate(value):
        if not in_string:
            result.append(character)
            if character == '"':
                in_string = True
            continue
        if escaped:
            result.append(character)
            escaped = False
            continue
        if character == "\\":
            result.append(character)
            escaped = True
            continue
        if character != '"':
            result.append(character)
            continue

        next_index = index + 1
        while next_index < length and value[next_index].isspace():
            next_index += 1
        next_character = value[next_index] if next_index < length else ""
        if not next_character or next_character in ":,}]":
            result.append(character)
            in_string = False
        else:
            result.append('\\"')
    return "".join(result)


def _is_json_array(value: object) -> bool:
    return isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    )


def normalize_error_code(value: Any, *, fallback: str = "tool_execution_failed") -> str:
    text = str(value or "").strip().lower()
    return text if _ERROR_CODE.fullmatch(text) else fallback


def safe_error_content(
    code: str,
    message: str | None = None,
    *,
    diagnostics: Mapping[str, Any] | None = None,
) -> str:
    payload: dict[str, Any] = {
        "success": False,
        "errorCode": normalize_error_code(code),
    }
    if message:
        payload["error"] = sanitize_error_message(message)
    if diagnostics:
        payload["diagnostics"] = _safe_diagnostics(diagnostics)
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _safe_diagnostics(value: Mapping[str, Any]) -> dict[str, Any]:
    """Keep bounded scalar diagnostics without echoing model-generated text."""

    result: dict[str, Any] = {}
    for raw_key, raw_value in value.items():
        key = str(raw_key or "").strip()[:80]
        if not key:
            continue
        if raw_value is None or isinstance(raw_value, (bool, int, float)):
            result[key] = raw_value
        elif isinstance(raw_value, str):
            result[key] = sanitize_error_message(raw_value, max_chars=500)
        elif _is_json_array(raw_value):
            result[key] = [
                sanitize_error_message(item, max_chars=200)
                if isinstance(item, str)
                else item
                for item in list(raw_value)[:30]
                if item is None or isinstance(item, (bool, int, float, str))
            ]
    return result


def sanitize_tool_result(content: Any, *, max_chars: int = 64_000) -> str:
    text = content if isinstance(content, str) else str(content or "")
    if len(text) > int(max_chars):
        return safe_error_content(
            "tool_result_too_large",
            f"Tool result exceeded the {int(max_chars)}-character limit.",
        )
    return _redact_sensitive(text)


def sanitize_error_message(message: Any, *, max_chars: int = 2_000) -> str:
    text = _redact_sensitive(str(message or ""))[: int(max_chars)]
    return text or "Tool execution failed."


def summarize_tool_arguments(
    arguments: Mapping[str, Any],
    *,
    max_chars: int = 420,
) -> str:
    text = json.dumps(
        thaw_json_mapping(arguments),
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
    )
    redacted = _redact_sensitive(text)
    limit = int(max_chars)
    if len(redacted) <= limit:
        return redacted
    marker = "\n…"
    return redacted[: max(0, limit - len(marker))] + marker[:limit]


def _redact_sensitive(text: str) -> str:
    value = _BEARER.sub("Bearer [REDACTED]", str(text or ""))
    value = _API_KEY.sub("[REDACTED_KEY]", value)
    value = _WINDOWS_PATH.sub("[REDACTED_PATH]", value)
    return _POSIX_PATH.sub("[REDACTED_PATH]", value)
