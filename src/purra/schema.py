"""Internal schema inspection and value validation shared by Core boundaries."""

from collections.abc import Mapping, Sequence
from typing import Any

_SCHEMA_TYPES = frozenset({
    "array",
    "boolean",
    "integer",
    "null",
    "number",
    "object",
    "string",
})
_SCHEMA_KEYWORDS = frozenset({
    "additionalProperties",
    "anyOf",
    "const",
    "description",
    "enum",
    "items",
    "maximum",
    "maxItems",
    "maxLength",
    "minimum",
    "minItems",
    "minLength",
    "oneOf",
    "properties",
    "required",
    "title",
    "type",
    "uniqueItems",
})


def schema_contract_violations(
    schema: object,
    *,
    path: str,
) -> tuple[str, ...]:
    """Validate the complete JSON-Schema subset enforced by Core."""

    if not isinstance(schema, Mapping):
        return (f"{path}: schema must be an object",)

    violations: list[str] = []
    unknown = sorted(str(key) for key in schema if key not in _SCHEMA_KEYWORDS)
    if unknown:
        violations.append(
            f"{path}: unsupported keyword(s): {', '.join(unknown)}"
        )

    expected_type = schema.get("type")
    if expected_type is not None:
        if isinstance(expected_type, str):
            type_names = (expected_type,)
        elif _is_schema_array(expected_type):
            type_names = tuple(expected_type)
            if not type_names:
                violations.append(f"{path}.type: type list must be non-empty")
            if any(not isinstance(item, str) for item in type_names):
                violations.append(
                    f"{path}.type: type list must contain only strings"
                )
            if len(type_names) != len(set(type_names)):
                violations.append(
                    f"{path}.type: type list values must be unique"
                )
        else:
            type_names = ()
            violations.append(
                f"{path}.type: type must be a string or non-empty list"
            )
        unsupported_types = sorted(
            str(item) for item in type_names if item not in _SCHEMA_TYPES
        )
        if unsupported_types:
            violations.append(
                f"{path}.type: unsupported type(s): "
                + ", ".join(unsupported_types)
            )

    for keyword in ("title", "description"):
        value = schema.get(keyword)
        if value is not None and not isinstance(value, str):
            violations.append(f"{path}.{keyword}: must be a string")

    properties = schema.get("properties")
    if properties is not None:
        if not isinstance(properties, Mapping):
            violations.append(f"{path}.properties: must be an object")
        else:
            for raw_name, child in properties.items():
                name = str(raw_name)
                violations.extend(schema_contract_violations(
                    child,
                    path=f"{path}.properties.{name}",
                ))

    required = schema.get("required")
    if required is not None:
        if not _is_schema_array(required):
            violations.append(f"{path}.required: must be an array of strings")
        else:
            names = tuple(required)
            if any(not isinstance(name, str) or not name for name in names):
                violations.append(
                    f"{path}.required: values must be non-empty strings"
                )
            if len(names) != len(set(names)):
                violations.append(f"{path}.required: values must be unique")

    additional = schema.get("additionalProperties")
    if additional is not None and not isinstance(additional, bool):
        violations.append(f"{path}.additionalProperties: must be boolean")

    if "uniqueItems" in schema and not isinstance(
        schema.get("uniqueItems"),
        bool,
    ):
        violations.append(f"{path}.uniqueItems: must be boolean")

    if "items" in schema:
        violations.extend(schema_contract_violations(
            schema.get("items"),
            path=f"{path}.items",
        ))

    for keyword in ("anyOf", "oneOf"):
        branches = schema.get(keyword)
        if branches is None:
            continue
        if not _is_schema_array(branches) or not branches:
            violations.append(
                f"{path}.{keyword}: must be a non-empty array of schemas"
            )
            continue
        for index, branch in enumerate(branches):
            violations.extend(schema_contract_violations(
                branch,
                path=f"{path}.{keyword}[{index}]",
            ))

    enum = schema.get("enum")
    if enum is not None and (not _is_schema_array(enum) or not enum):
        violations.append(f"{path}.enum: must be a non-empty array")

    for minimum_name, maximum_name in (
        ("minLength", "maxLength"),
        ("minItems", "maxItems"),
    ):
        minimum = schema.get(minimum_name)
        maximum = schema.get(maximum_name)
        if minimum is not None and not _is_non_negative_integer(minimum):
            violations.append(
                f"{path}.{minimum_name}: must be a non-negative integer"
            )
        if maximum is not None and not _is_non_negative_integer(maximum):
            violations.append(
                f"{path}.{maximum_name}: must be a non-negative integer"
            )
        if (
            _is_non_negative_integer(minimum)
            and _is_non_negative_integer(maximum)
            and minimum > maximum
        ):
            violations.append(
                f"{path}.{minimum_name}: cannot exceed {maximum_name}"
            )

    minimum = schema.get("minimum")
    maximum = schema.get("maximum")
    if minimum is not None and not _is_schema_number(minimum):
        violations.append(f"{path}.minimum: must be a JSON number")
    if maximum is not None and not _is_schema_number(maximum):
        violations.append(f"{path}.maximum: must be a JSON number")
    if (
        _is_schema_number(minimum)
        and _is_schema_number(maximum)
        and minimum > maximum
    ):
        violations.append(f"{path}.minimum: cannot exceed maximum")

    return tuple(violations)


def _is_schema_array(value: object) -> bool:
    return isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    )


def _is_non_negative_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _is_schema_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def schema_violation(
    value: Any,
    schema: object,
    *,
    path: str,
    work: list[int] | None = None,
) -> tuple[str, dict[str, Any]] | None:
    if work is not None:
        work[0] -= 1
        if work[0] < 0:
            raise ValueError("validation work limit exceeded")
    if not isinstance(schema, Mapping):
        return (
            f"argument {path} cannot be checked against a malformed schema.",
            {"path": path, "keyword": "schema"},
        )

    any_of = schema.get("anyOf")
    if _is_json_array(any_of):
        branch_violations = [
            schema_violation(value, branch, path=path, work=work)
            for branch in any_of
        ]
        if not any(violation is None for violation in branch_violations):
            preferred = None if work is not None else _preferred_union_violation(
                value,
                any_of,
                branch_violations,
            )
            if work is None and preferred is not None:
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
            schema_violation(value, branch, path=path, work=work)
            for branch in one_of
        ]
        match_count = sum(
            violation is None for violation in branch_violations
        )
        if match_count != 1:
            if match_count == 0:
                preferred = None if work is not None else _preferred_union_violation(
                    value,
                    one_of,
                    branch_violations,
                )
                if work is None and preferred is not None:
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
        _json_values_equal(value, candidate, work=work) for candidate in enum
    ):
        return (
            f"argument {path} is not one of the allowed values.",
            {
                "path": path,
                "keyword": "enum",
                "allowedValues": list(enum),
            },
        )

    if "const" in schema and not _json_values_equal(value, schema.get("const"), work=work):
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
        if schema.get("uniqueItems") is True:
            for duplicate_index in range(1, len(value)):
                for first_index in range(duplicate_index):
                    if _json_values_equal(
                        value[first_index],
                        value[duplicate_index], work=work,
                    ):
                        return (
                            f"argument {path} violates uniqueItems at indexes "
                            f"{first_index} and {duplicate_index}.",
                            {
                                "path": path,
                                "keyword": "uniqueItems",
                                "firstIndex": first_index,
                                "duplicateIndex": duplicate_index,
                            },
                        )
        if "items" in schema:
            item_schema = schema.get("items")
            for index, item in enumerate(value):
                violation = schema_violation(
                    item,
                    item_schema,
                    path=f"{path}[{index}]",
                    work=work,
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
            violation = schema_violation(
                item,
                child_schema,
                path=f"{path}.{key}",
                work=work,
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


def _json_values_equal(left: Any, right: Any, *, work: list[int] | None = None) -> bool:
    if work is not None:
        work[0] -= 1
        if work[0] < 0:
            raise ValueError("validation work limit exceeded")
    if left is None or right is None:
        return left is right
    if isinstance(left, bool) or isinstance(right, bool):
        return left is right
    if _is_json_number(left) or _is_json_number(right):
        return (
            _is_json_number(left)
            and _is_json_number(right)
            and left == right
        )
    if isinstance(left, str) or isinstance(right, str):
        return (
            isinstance(left, str)
            and isinstance(right, str)
            and left == right
        )
    if _is_json_array(left) or _is_json_array(right):
        return (
            _is_json_array(left)
            and _is_json_array(right)
            and len(left) == len(right)
            and all(
                _json_values_equal(left[index], right[index], work=work)
                for index in range(len(left))
            )
        )
    if isinstance(left, Mapping) or isinstance(right, Mapping):
        return (
            isinstance(left, Mapping)
            and isinstance(right, Mapping)
            and len(left) == len(right)
            and all(
                key in right and _json_values_equal(value, right[key], work=work)
                for key, value in left.items()
            )
        )
    return False


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
        if schema_violation(value, branch, path="$") is None
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


def _is_json_array(value: object) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))
