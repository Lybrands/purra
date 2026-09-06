"""Versioned, bounded object-output contracts; no model calls or coercion."""

from __future__ import annotations

import hashlib
import json
import math
import struct
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from purra.errors import CodedAgentCoreError
from purra.json_values import freeze_json_mapping
from purra.schema import schema_contract_violations, schema_violation

OUTPUT_SCHEMA_PROFILE = "purra.output-schema/v1"
JSON_IDENTITY_PROFILE = "purra.json-identity/v1"
_SAFE_INTEGER = 2**53 - 1


class StructuredOutputError(CodedAgentCoreError):
    """Failure with candidate-free diagnostics; never contains rejected output."""


@dataclass(frozen=True, slots=True)
class StructuredOutputLimits:
    schema_bytes: int = 65_536
    output_bytes: int = 1_048_576
    schema_depth: int = 32
    output_depth: int = 64
    schema_nodes: int = 4_096
    output_nodes: int = 65_536
    validation_steps: int = 100_000

    def __post_init__(self) -> None:
        maxima = (65_536, 1_048_576, 32, 64, 4_096, 65_536, 100_000)
        for name, maximum in zip(self.__dataclass_fields__, maxima):
            value = getattr(self, name)
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError(f"{name} must be a positive integer at most {maximum}")


def _failure(code: str, keyword: str, path: str = "$") -> StructuredOutputError:
    return StructuredOutputError(
        "Structured output contract validation failed",
        code=code,
        details={"path": path if len(path.encode("utf-8")) <= 512 else "$",
                 "keyword": keyword, "reason": "constraint_failed"},
    )


def _snapshot(value: Any, *, depth: int, nodes: int, byte_limit: int) -> Any:
    remaining = nodes
    # Bound detached JSON storage before creating containers; numbers reserve
    # 24 bytes and separators reserve two bytes per child in both languages.
    byte_count = 0
    active: set[int] = set()

    def visit(item: Any, level: int) -> Any:
        nonlocal remaining, byte_count
        remaining -= 1
        if remaining < 0 or level > depth:
            raise ValueError("JSON resource limit exceeded")
        if isinstance(item, str):
            if len(item) > byte_limit:
                raise ValueError("JSON byte limit exceeded")
            byte_count += len(json.dumps(item, ensure_ascii=False).encode("utf-8"))
            result = item
        elif item is None or isinstance(item, bool):
            byte_count += 5
            result = item
        elif isinstance(item, (int, float)):
            number = float(item)
            if not math.isfinite(number) or (number.is_integer() and abs(number) > _SAFE_INTEGER):
                raise ValueError("number outside interoperable range")
            result = int(number) if number.is_integer() else number
            byte_count += 24  # Conservative binary64 JSON spelling bound.
        elif isinstance(item, Mapping) or (isinstance(item, Sequence) and not isinstance(item, (bytes, bytearray))):
            if id(item) in active:
                raise ValueError("cyclic JSON")
            active.add(id(item))
            try:
                if isinstance(item, Mapping):
                    result = {}
                    for key, child in item.items():
                        if not isinstance(key, str):
                            raise ValueError("non-string JSON key")
                        visit(key, level + 1)
                        result[key] = visit(child, level + 1)
                else:
                    result = [visit(child, level + 1) for child in item]
                byte_count += 2 + 2 * len(item)
            finally:
                active.remove(id(item))
        else:
            raise ValueError("unsupported JSON value")
        if byte_count > byte_limit:
            raise ValueError("JSON byte limit exceeded")
        return result

    result = visit(value, 0)
    return result


def _identity(value: Any) -> str:
    if value is None:
        return "n"
    if isinstance(value, bool):
        return "t" if value else "f"
    if isinstance(value, (float, int)):
        return "d" + struct.pack(">d", float(value) if value else 0.0).hex()
    if isinstance(value, str):
        data = value.encode("utf-8")
        return f"s{len(data)}:{data.hex()}"
    if isinstance(value, Mapping):
        return f"o{len(value)}:" + "".join(_identity(key) + _identity(value[key]) for key in sorted(value))
    return f"a{len(value)}:" + "".join(_identity(item) for item in value)


def _digest(value: Any) -> str:
    return hashlib.sha256((JSON_IDENTITY_PROFILE + "\n" + _identity(value)).encode("ascii")).hexdigest()


def _check_schema_nodes(schema: Mapping[str, Any]) -> None:
    # The legacy tool inspector treats null as omission for some keywords.
    # Output schemas require every present keyword to have its declared type.
    for key, value in schema.items():
        if value is None and key != "const":
            raise ValueError("null schema keyword")
    properties = schema.get("properties", {})
    if isinstance(properties, Mapping):
        for child in properties.values():
            if isinstance(child, Mapping):
                _check_schema_nodes(child)
    for key in ("anyOf", "oneOf"):
        branches = schema.get(key, [])
        if isinstance(branches, list):
            for child in branches:
                if isinstance(child, Mapping):
                    _check_schema_nodes(child)
    if isinstance(schema.get("items"), Mapping):
        _check_schema_nodes(schema["items"])


@dataclass(frozen=True, slots=True)
class StructuredOutputContract:
    schema_id: str
    schema_version: str
    schema: Mapping[str, Any]
    mode: Literal["local", "native_required"] = "local"
    schema_profile: str = OUTPUT_SCHEMA_PROFILE
    limits: StructuredOutputLimits = field(default_factory=StructuredOutputLimits)
    schema_digest: str = field(init=False)
    contract_digest: str = field(init=False)

    def __post_init__(self) -> None:
        invalid = False
        try:
            if not all(isinstance(item, str) and len(item) <= 128 and item.strip() and len(item.encode("utf-8")) <= 128
                       for item in (self.schema_id, self.schema_version)):
                raise ValueError("invalid schema identity")
            if self.schema_profile != OUTPUT_SCHEMA_PROFILE:
                raise ValueError("unsupported schema profile")
            if not isinstance(self.limits, StructuredOutputLimits):
                raise ValueError("invalid limits")
            schema = _snapshot(self.schema, depth=self.limits.schema_depth,
                               nodes=self.limits.schema_nodes, byte_limit=self.limits.schema_bytes)
            if not isinstance(schema, dict) or schema.get("type") != "object":
                raise ValueError("root must be object")
            _check_schema_nodes(schema)
            if schema_contract_violations(schema, path="$"):
                raise ValueError("invalid schema")
        except (ValueError, TypeError, OverflowError, RecursionError):
            invalid = True
        if invalid:
            raise _failure("structured_output_schema_invalid", "schema")
        if self.mode not in ("local", "native_required"):
            raise _failure("structured_output_mode_unsupported", "mode")
        object.__setattr__(self, "schema", freeze_json_mapping(schema))
        object.__setattr__(self, "schema_digest", _digest(schema))
        object.__setattr__(self, "contract_digest", _digest({
            "schemaId": self.schema_id, "schemaVersion": self.schema_version,
            "schemaProfile": self.schema_profile, "schemaDigest": self.schema_digest,
            "mode": self.mode, "limits": list(getattr(self.limits, key) for key in self.limits.__dataclass_fields__),
        }))

    def identity(self) -> Mapping[str, Any]:
        return freeze_json_mapping({
            "schemaId": self.schema_id, "schemaVersion": self.schema_version,
            "schemaProfile": self.schema_profile, "schemaDigest": self.schema_digest,
            "contractDigest": self.contract_digest, "mode": self.mode,
        })

    def schema_json(self) -> str:
        from purra.json_values import thaw_json_mapping
        return json.dumps(thaw_json_mapping(self.schema), ensure_ascii=False, separators=(",", ":"))

    def instruction(self) -> str:
        header = "Return exactly one complete JSON object. Do not use tools, markdown fences, or extra text."
        return header + (" JSON Schema: " + self.schema_json() if self.mode == "local" else " Follow the native JSON Schema output format.")

    def parse(self, text: str) -> Mapping[str, Any]:
        """Validate a complete JSON document and return a detached frozen object."""
        invalid = False
        try:
            if not isinstance(text, str) or len(text) > self.limits.output_bytes:
                raise ValueError("invalid document")
            if len(text.encode("utf-8")) > self.limits.output_bytes:
                raise ValueError("document too large")
            # Guard nesting before the native parser allocates a recursive tree.
            level = 0
            quoted = escaped = False
            for char in text:
                if quoted:
                    if escaped:
                        escaped = False
                    elif char == "\\":
                        escaped = True
                    elif char == '"':
                        quoted = False
                elif char == '"':
                    quoted = True
                elif char in "[{":
                    level += 1
                    if level > self.limits.output_depth + 1:
                        raise ValueError("document too deep")
                elif char in "]}":
                    level -= 1

            def pairs(items):
                result = {}
                for key, value in items:
                    if key in result:
                        raise ValueError("duplicate key")
                    result[key] = value
                return result

            def constant(_):
                raise ValueError("non-JSON number")

            value = json.loads(text, object_pairs_hook=pairs, parse_constant=constant)
            value = _snapshot(value, depth=self.limits.output_depth,
                              nodes=self.limits.output_nodes, byte_limit=self.limits.output_bytes)
        except (ValueError, TypeError, OverflowError, RecursionError):
            invalid = True
        if invalid:
            raise _failure("structured_output_invalid_json", "document")
        return self.validate_value(value)

    def validate_value(self, value: Any) -> Mapping[str, Any]:
        """Validate an already-decoded object with the same bounded profile."""
        try:
            value = _snapshot(value, depth=self.limits.output_depth,
                              nodes=self.limits.output_nodes, byte_limit=self.limits.output_bytes)
        except (ValueError, TypeError, OverflowError, RecursionError):
            raise _failure("structured_output_invalid_json", "value") from None
        try:
            violation = schema_violation(value, self.schema, path="$", work=[self.limits.validation_steps])
        except ValueError:
            raise _failure("structured_output_validation_limit_exceeded", "validation_steps") from None
        if violation is not None:
            details = violation[1]
            # Paths derived from undeclared candidate keys never enter diagnostics.
            path = details["path"] if details["keyword"] != "additionalProperties" else "$"
            raise _failure("structured_output_schema_mismatch", details["keyword"], path)
        return freeze_json_mapping(value)


def json_identity_digest(value: Any) -> str:
    """Hash bounded interoperable JSON using purra.json-identity/v1."""
    limits = StructuredOutputLimits()
    normalized = _snapshot(value, depth=limits.output_depth, nodes=limits.output_nodes,
                           byte_limit=limits.output_bytes)
    return _digest(normalized)


__all__ = ["OUTPUT_SCHEMA_PROFILE", "JSON_IDENTITY_PROFILE", "StructuredOutputContract",
           "StructuredOutputLimits", "StructuredOutputError", "json_identity_digest"]
