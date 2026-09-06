"""Read-only tools through a host-owned, initialized official MCP session."""
from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import Any

from mcp import ClientSession, types
from mcp.shared.exceptions import McpError
from purra.cancellation import OperationCanceled, await_with_cancellation, raise_if_stopped
from purra.contracts import ToolHandlerResult, ToolPolicy, ToolSchema
from purra.errors import CodedAgentCoreError
from purra.json_values import freeze_json_mapping, thaw_json_mapping
from purra.ports import ScopeValidator, ToolRegistration
from purra.structured import StructuredOutputContract, StructuredOutputError, StructuredOutputLimits, json_identity_digest


class McpAdapterError(CodedAgentCoreError):
    """A sanitized boundary failure; remote error messages are never attached."""


def _fail(code):
    raise McpAdapterError("MCP tool boundary rejected the operation", code=code)


@dataclass(frozen=True, slots=True)
class McpToolLimits:
    max_pages: int = 32
    max_tools: int = 256
    max_catalog_bytes: int = 1_048_576
    max_schema_bytes: int = 65_536
    max_description_bytes: int = 8_192
    max_result_bytes: int = 65_536
    max_content_blocks: int = 128
    timeout_ms: int = 30_000

    def __post_init__(self):
        maxima = (32, 256, 1_048_576, 65_536, 8_192, 65_536, 128, 300_000)
        for name, maximum in zip(self.__dataclass_fields__, maxima):
            if type(getattr(self, name)) is not int or not 1 <= getattr(self, name) <= maximum:
                raise ValueError(f"{name} must be a positive integer at most {maximum}")


class McpCatalogMonitor:
    """Host-fed protocol/notification state; never owns or closes the session."""
    def __init__(self):
        self._revision = 0
        self._closed = False
        self._protocol = None

    def accept_protocol_version(self, version: str) -> None:
        if version not in {"2025-06-18", "2025-11-25"}:
            self.close()
            _fail("mcp_protocol_unsupported")
        if self._protocol is not None and self._protocol != version:
            self.close()
            _fail("mcp_protocol_conflict")
        self._protocol = version

    async def on_message(self, message) -> None:
        if isinstance(message, Exception):
            self.close()
        elif isinstance(message, types.ServerNotification) and isinstance(message.root, types.ToolListChangedNotification):
            self._revision += 1

    def close(self) -> None:
        self._closed = True

    @property
    def revision(self):
        return self._revision

    @property
    def protocol_version(self):
        return self._protocol

    def check(self, revision: int) -> None:
        if self._closed:
            _fail("mcp_connection_closed")
        if self._protocol is None:
            _fail("mcp_not_initialized")
        if revision != self._revision:
            _fail("mcp_catalog_stale")


@dataclass(frozen=True, slots=True)
class McpToolBinding:
    local_name: str
    policy: ToolPolicy
    scope_validator: ScopeValidator | None
    concurrency_safe: bool = False

    def __post_init__(self):
        if not isinstance(self.local_name, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", self.local_name):
            _fail("mcp_binding_invalid")
        if not isinstance(self.policy, ToolPolicy) or self.policy.mode != "read" or self.policy.risk_level != "read":
            _fail("mcp_binding_invalid")
        if self.scope_validator is not None and not callable(self.scope_validator):
            _fail("mcp_binding_invalid")
        if type(self.concurrency_safe) is not bool:
            _fail("mcp_binding_invalid")


@dataclass(frozen=True, slots=True)
class McpToolSnapshot:
    server_id: str
    protocol_version: str
    revision_digest: str
    entries: tuple[Mapping[str, Any], ...]
    schema_version: int = 1

    def __post_init__(self):
        object.__setattr__(self, "entries", tuple(freeze_json_mapping(e) for e in self.entries))

    def to_mapping(self):
        return {"schemaVersion": self.schema_version, "serverId": self.server_id,
                "protocolVersion": self.protocol_version, "revisionDigest": self.revision_digest,
                "entries": [thaw_json_mapping(e) for e in self.entries]}


@dataclass(frozen=True, slots=True)
class McpToolCatalog:
    snapshot: McpToolSnapshot
    registrations: tuple[ToolRegistration, ...]
    _monitor: McpCatalogMonitor = field(repr=False)
    _revision: int = field(repr=False)

    @property
    def stale(self):
        try:
            self._monitor.check(self._revision)
            return False
        except McpAdapterError:
            return True


def _bytes(value):
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8"))


def _contract(schema, limits):
    try:
        return StructuredOutputContract("mcp.object", "1", schema,
            limits=replace(StructuredOutputLimits(), schema_bytes=limits.max_schema_bytes,
                           output_bytes=limits.max_result_bytes))
    except (StructuredOutputError, TypeError, ValueError):
        _fail("mcp_schema_unsupported")


async def _request(client, request, result_type, limits, signal):
    raise_if_stopped(signal)
    try:
        # Raw official SDK request avoids call_tool's implicit catalog refresh.
        async with asyncio.timeout(limits.timeout_ms / 1000):
            return await await_with_cancellation(client.send_request(request, result_type,
                request_read_timeout_seconds=timedelta(milliseconds=limits.timeout_ms)), signal)
    except OperationCanceled:
        raise
    except TimeoutError:
        _fail("mcp_timeout")
    except McpError as error:
        code = ("mcp_timeout" if error.error.code in {408, -32001} else
                "mcp_transport_error" if error.error.code == types.CONNECTION_CLOSED else "mcp_protocol_error")
    except (ValueError, TypeError):
        code = "mcp_result_invalid"
    except Exception:
        code = "mcp_transport_error"
    _fail(code)


def _result(raw, output, limits):
    if raw.isError:
        _fail("mcp_tool_error")
    if len(raw.content) > limits.max_content_blocks:
        _fail("mcp_result_too_large")
    text = []
    for block in raw.content:
        if not isinstance(block, types.TextContent):
            _fail("mcp_result_unsupported")
        text.append(block.text)
    envelope = {"text": text, "structured": raw.structuredContent}
    try:
        if _bytes(raw.model_dump(mode="json", by_alias=True, exclude_none=True)) > limits.max_result_bytes:
            _fail("mcp_result_too_large")
        value = _contract({"type": "object"}, limits).validate_value(envelope)
        if output is not None:
            output.validate_value(raw.structuredContent)
    except StructuredOutputError:
        _fail("mcp_result_invalid")
    except (ValueError, TypeError, OverflowError, RecursionError):
        _fail("mcp_result_invalid")
    return value


def _registration(client, remote, entry, binding, contract, output, monitor, revision, limits):
    async def scope(state, arguments, signal=None):
        monitor.check(revision)
        raise_if_stopped(signal)
        if binding.scope_validator is not None:
            return await binding.scope_validator(state, arguments, signal)
        return None

    async def run(state, arguments, signal=None):
        try:
            monitor.check(revision)
            try:
                value = contract.validate_value(arguments)
            except StructuredOutputError:
                _fail("mcp_invalid_arguments")
            decision = await scope(state, value, signal)
            if decision is not None and str(decision).strip():
                _fail("mcp_scope_denied")
            monitor.check(revision)
            raise_if_stopped(signal)
            result = await _request(client, types.ClientRequest(types.CallToolRequest(
                params=types.CallToolRequestParams(name=remote, arguments=thaw_json_mapping(value)))),
                types.CallToolResult, limits, signal)
            monitor.check(revision)
            envelope = _result(result, output, limits)
            return ToolHandlerResult(json.dumps(thaw_json_mapping(envelope), ensure_ascii=False, separators=(",", ":")), effect_state="not_started")
        except McpAdapterError as error:
            return ToolHandlerResult('{"text":[],"structured":null}', error_code=error.code, effect_state="not_started")

    return ToolRegistration(ToolSchema(binding.local_name, entry["description"] or binding.policy.title, contract.schema),
        run, binding.policy, scope_validator=scope, argument_contract=contract, concurrency_safe=binding.concurrency_safe,
        max_argument_chars=limits.max_result_bytes)


async def discover_mcp_tools(client: ClientSession, server_id: str,
        bindings: Mapping[str, McpToolBinding], *, monitor: McpCatalogMonitor,
        limits: McpToolLimits = McpToolLimits(), signal=None) -> McpToolCatalog:
    if not isinstance(client, ClientSession) or not isinstance(monitor, McpCatalogMonitor) or not isinstance(limits, McpToolLimits):
        _fail("mcp_binding_invalid")
    if not isinstance(server_id, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", server_id):
        _fail("mcp_binding_invalid")
    if not isinstance(bindings, Mapping) or not bindings or len(bindings) > limits.max_tools:
        _fail("mcp_binding_invalid")
    bound = dict(bindings)
    if any(not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", name)
           or not isinstance(binding, McpToolBinding) for name, binding in bound.items()):
        _fail("mcp_binding_invalid")
    if len({binding.local_name for binding in bound.values()}) != len(bound):
        _fail("mcp_name_conflict")
    revision = monitor.revision
    monitor.check(revision)
    capabilities = client.get_server_capabilities()
    if capabilities is None or capabilities.tools is None:
        _fail("mcp_tools_unavailable")
    seen, cursors, found = set(), set(), {}
    cursor = None
    total_bytes = 0
    for _ in range(limits.max_pages):
        monitor.check(revision)
        result = await _request(client, types.ClientRequest(types.ListToolsRequest(
            params=types.PaginatedRequestParams(cursor=cursor) if cursor is not None else None)),
            types.ListToolsResult, limits, signal)
        monitor.check(revision)
        try:
            total_bytes += _bytes(result.model_dump(mode="json", by_alias=True, exclude_none=True))
        except (ValueError, TypeError, OverflowError, RecursionError):
            _fail("mcp_catalog_invalid")
        if total_bytes > limits.max_catalog_bytes or len(seen) + len(result.tools) > limits.max_tools:
            _fail("mcp_catalog_limit_exceeded")
        for tool in result.tools:
            if tool.name in seen:
                _fail("mcp_name_conflict")
            seen.add(tool.name)
            if tool.name not in bound:
                continue
            binding = bound[tool.name]
            description = tool.description or ""
            if len(description.encode("utf-8")) > limits.max_description_bytes:
                _fail("mcp_catalog_limit_exceeded")
            if tool.execution is not None and tool.execution.taskSupport == "required":
                _fail("mcp_tool_unsupported")
            contract = _contract(tool.inputSchema, limits)
            output = _contract(tool.outputSchema, limits) if tool.outputSchema is not None else None
            entry = {"remoteName": tool.name, "localName": binding.local_name,
                "description": description, "inputSchema": thaw_json_mapping(contract.schema),
                "outputSchema": thaw_json_mapping(output.schema) if output is not None else None,
                "policy": {"mode": "read", "title": binding.policy.title, "riskLevel": "read"},
                "concurrencySafe": binding.concurrency_safe}
            found[tool.name] = (entry, binding, contract, output)
        cursor = result.nextCursor
        if cursor is None:
            break
        if not cursor or len(cursor.encode("utf-8")) > 512 or cursor in cursors:
            _fail("mcp_pagination_invalid")
        cursors.add(cursor)
    else:
        _fail("mcp_catalog_limit_exceeded")
    if found.keys() != bound.keys():
        _fail("mcp_binding_missing")
    ordered = sorted(found.items(), key=lambda item: item[1][0]["localName"])
    entries = tuple(item[1][0] for item in ordered)
    identity = {"schemaVersion": 1, "serverId": server_id, "protocolVersion": monitor.protocol_version, "entries": entries}
    try:
        digest = json_identity_digest(identity)
    except (ValueError, TypeError):
        _fail("mcp_catalog_limit_exceeded")
    snapshot = McpToolSnapshot(server_id, monitor.protocol_version, digest, entries)
    registrations = tuple(_registration(client, remote, entry, binding, contract, output, monitor, revision, limits)
        for remote, (entry, binding, contract, output) in ordered)
    monitor.check(revision)
    return McpToolCatalog(snapshot, registrations, monitor, revision)


__all__ = ["McpAdapterError", "McpToolLimits", "McpCatalogMonitor", "McpToolBinding", "McpToolSnapshot", "McpToolCatalog", "discover_mcp_tools"]
