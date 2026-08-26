"""Fail-closed startup validation for Core tool registrations."""

from __future__ import annotations

import inspect
import json
from collections import Counter
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter
from typing import Any, Iterable, Mapping, Sequence

from purra.contracts import (
    ToolDataContract,
    ToolPayloadMode,
    ToolPolicy,
)
from purra.errors import ContractViolationError
from purra.json_values import thaw_json_mapping
from purra.ports import ToolRegistration


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


@dataclass(frozen=True, slots=True)
class ToolContractReport:
    """Stable registration diagnostics produced before a catalog is used."""

    registered_names: frozenset[str]
    duplicate_names: frozenset[str] = frozenset()
    violations: tuple[str, ...] = ()

    @property
    def is_valid(self) -> bool:
        return not self.duplicate_names and not self.violations

    @property
    def tool_count(self) -> int:
        return len(self.registered_names)

    def describe_violations(self) -> str:
        rows = list(self.violations)
        if self.duplicate_names:
            rows.append(
                "duplicate tool names: " + ", ".join(sorted(self.duplicate_names))
            )
        return "; ".join(rows) or "tool contract is valid"


def inspect_tool_contract(
    registrations: Iterable[ToolRegistration],
) -> ToolContractReport:
    """Inspect a detached registration snapshot without importing adapters."""

    items = tuple(registrations)
    names = tuple(str(item.schema.name or "").strip() for item in items)
    duplicate_names = frozenset(
        name for name, count in Counter(names).items() if name and count > 1
    )
    violations: list[str] = []
    planning_capabilities = {}

    for index, registration in enumerate(items):
        name = names[index]
        label = name or f"registration[{index}]"
        if not name:
            violations.append(f"{label}: tool name is required")
        if not str(registration.schema.description or "").strip():
            violations.append(f"{label}: tool description is required")
        if not isinstance(registration.policy, ToolPolicy):
            violations.append(f"{label}: explicit tool policy is required")
        if not isinstance(registration.cancellation_linearizable, bool):
            violations.append(
                f"{label}: cancellation_linearizable must be boolean"
            )
        capability = registration.planning_capability
        if capability is not None:
            capability_name = capability.name
            if capability_name in names:
                violations.append(
                    f"{label}: planning capability collides with a runtime "
                    f"tool: {capability_name}"
                )
            previous = planning_capabilities.get(capability_name)
            if previous is not None and previous != capability:
                violations.append(
                    f"{label}: planning capability conflicts with another "
                    f"runtime tool: {capability_name}"
                )
            planning_capabilities[capability_name] = capability

        parameters = thaw_json_mapping(registration.schema.parameters)
        violations.extend(_schema_contract_violations(
            parameters,
            path=f"{label}.parameters",
        ))
        if parameters.get("type") != "object":
            violations.append(f"{label}: parameters schema type must be object")
        properties = parameters.get("properties")
        if properties is not None and not isinstance(properties, dict):
            violations.append(f"{label}: schema properties must be an object")
        try:
            json.dumps(parameters, ensure_ascii=False, allow_nan=False)
        except (TypeError, ValueError) as error:
            violations.append(
                f"{label}: parameters schema is not JSON serializable "
                f"({type(error).__name__})"
            )

        if capability is not None:
            capability_parameters = thaw_json_mapping(capability.parameters)
            violations.extend(_schema_contract_violations(
                capability_parameters,
                path=f"{label}.planningCapability.parameters",
            ))
            if capability_parameters.get("type") != "object":
                violations.append(
                    f"{label}: planning capability parameters schema type "
                    "must be object"
                )

        data_contract = registration.data_contract
        if not isinstance(data_contract, ToolDataContract):
            violations.append(f"{label}: explicit tool data contract is invalid")
        else:
            for path in data_contract.model_owned_paths:
                if not _schema_declares_path(parameters, path):
                    violations.append(
                        f"{label}: model-owned path is absent from the "
                        f"model-visible schema: {path}"
                    )
            for path in (
                *data_contract.host_bound_paths,
                *data_contract.host_derived_paths,
            ):
                if _schema_declares_path(parameters, path):
                    violations.append(
                        f"{label}: host-owned path is exposed in the "
                        f"model-visible schema: {path}"
                    )
            if _is_audited_data_contract(data_contract):
                for path in _schema_leaf_paths(parameters):
                    if not any(
                        _owned_path_covers_schema_path(owned, path)
                        for owned in data_contract.model_owned_paths
                    ):
                        violations.append(
                            f"{label}: model-visible path has no declared "
                            f"owner: {path}"
                        )

        if not _is_async_callable(registration.handler):
            violations.append(f"{label}: handler must be async callable")
        if (
            registration.scope_validator is not None
            and not _is_async_callable(registration.scope_validator)
        ):
            violations.append(f"{label}: scope validator must be async callable")
        if registration.cache_probe is not None:
            probe = getattr(registration.cache_probe, "will_hit", None)
            if probe is None or not callable(probe) or inspect.iscoroutinefunction(probe):
                violations.append(f"{label}: cache probe will_hit must be synchronous")

        unknown_prerequisites = set(registration.prerequisite_tools) - set(names)
        if unknown_prerequisites:
            violations.append(
                f"{label}: context contract names unavailable prerequisites: "
                + ", ".join(sorted(unknown_prerequisites))
            )
        if name in registration.prerequisite_tools:
            violations.append(
                f"{label}: context contract cannot require itself"
            )

    dependency_map = {
        names[index]: tuple(registration.prerequisite_tools)
        for index, registration in enumerate(items)
        if names[index]
    }
    cycle = _dependency_cycle(dependency_map)
    if cycle:
        violations.append(
            "tool context prerequisite cycle: " + " -> ".join(cycle)
        )

    return ToolContractReport(
        registered_names=frozenset(name for name in names if name),
        duplicate_names=duplicate_names,
        violations=tuple(violations),
    )


def validate_tool_contract(
    registrations: Iterable[ToolRegistration],
) -> tuple[ToolRegistration, ...]:
    """Return a validated immutable snapshot or stop startup."""

    snapshot = tuple(registrations)
    report = inspect_tool_contract(snapshot)
    if not report.is_valid:
        raise ContractViolationError(report.describe_violations())
    return snapshot


def _is_async_callable(value: object) -> bool:
    return inspect.iscoroutinefunction(value) or inspect.iscoroutinefunction(
        getattr(value, "__call__", None)
    )


def _schema_contract_violations(
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
                violations.extend(_schema_contract_violations(
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
        violations.extend(_schema_contract_violations(
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
            violations.extend(_schema_contract_violations(
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


def _schema_declares_path(parameters: dict, path: str) -> bool:
    current: object = parameters
    for raw_segment in str(path or "").split("."):
        array_item = raw_segment.endswith("[]")
        segment = raw_segment[:-2] if array_item else raw_segment
        if not isinstance(current, dict):
            return False
        properties = current.get("properties")
        if not isinstance(properties, dict) or segment not in properties:
            return False
        current = properties[segment]
        if array_item:
            if not isinstance(current, dict) or current.get("type") != "array":
                return False
            current = current.get("items")
    return True


def _is_audited_data_contract(contract: ToolDataContract) -> bool:
    return bool(
        contract.model_owned_paths
        or contract.host_bound_paths
        or contract.host_derived_paths
        or contract.payload_mode is not ToolPayloadMode.INLINE
    )


def _schema_leaf_paths(schema: dict, prefix: str = "") -> tuple[str, ...]:
    properties = schema.get("properties")
    if not isinstance(properties, dict) or not properties:
        return (prefix,) if prefix else ()
    leaves: list[str] = []
    for raw_name, child in properties.items():
        name = str(raw_name)
        if not isinstance(child, dict):
            leaves.append(_join_schema_path(prefix, name))
            continue
        if child.get("type") == "array":
            array_path = _join_schema_path(prefix, f"{name}[]")
            items = child.get("items")
            if isinstance(items, dict) and isinstance(
                items.get("properties"),
                dict,
            ):
                leaves.extend(_schema_leaf_paths(items, array_path))
            else:
                leaves.append(array_path)
            continue
        child_path = _join_schema_path(prefix, name)
        if isinstance(child.get("properties"), dict):
            leaves.extend(_schema_leaf_paths(child, child_path))
        else:
            leaves.append(child_path)
    return tuple(leaves)


def _join_schema_path(prefix: str, segment: str) -> str:
    return f"{prefix}.{segment}" if prefix else segment


def _owned_path_covers_schema_path(owned: str, schema_path: str) -> bool:
    return bool(
        schema_path == owned
        or schema_path.startswith(f"{owned}.")
        or schema_path.startswith(f"{owned}[]")
    )


def _dependency_cycle(
    dependency_map: dict[str, tuple[str, ...]],
) -> tuple[str, ...]:
    try:
        tuple(TopologicalSorter(dependency_map).static_order())
    except CycleError as error:
        return tuple(str(name) for name in error.args[1])
    return ()
