"""Fail-closed startup validation for Core tool registrations."""

from __future__ import annotations

import inspect
import json
from collections import Counter
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter
from typing import Iterable

from purra.contracts import (
    ToolDataContract,
    ToolPayloadMode,
    ToolPolicy,
)
from purra.errors import ContractViolationError
from purra.json_values import thaw_json_mapping
from purra.ports import ToolRegistration


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
