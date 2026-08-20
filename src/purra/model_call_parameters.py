from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from purra.contracts import AgentMessage, ModelInvocation
_SECRET_FIELD_NAMES = frozenset({
    "apikey",
    "authorization",
    "proxyauthorization",
    "password",
    "secret",
    "accesstoken",
    "refreshtoken",
    "token",
})


def build_model_call_parameters(
    messages: Sequence[AgentMessage],
    invocation: ModelInvocation,
    *,
    provider_options: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    options = _redact(
        provider_options
        if provider_options is not None
        else invocation.request.options
    )
    if isinstance(options, dict):
        options.pop("tools", None)
    result: dict[str, Any] = {
        "provider": invocation.request.provider,
        "model": invocation.request.model,
        "options": options,
        "maxOutputTokens": invocation.max_output_tokens,
        "reasoningMode": invocation.reasoning_mode.value,
        "toolChoice": invocation.tool_choice.value,
        "toolNames": [tool.name for tool in invocation.tools],
        "messageCount": len(messages),
        "messageRoles": [message.role.value for message in messages],
    }
    if invocation.request.profile_id is not None:
        result["profileId"] = invocation.request.profile_id
    capabilities = invocation.request.capability_snapshot.output
    if (
        capabilities.max_output_tokens is not None
        or capabilities.thinking_token_accounting.value != "unknown"
    ):
        result["modelOutputCapabilities"] = capabilities.to_mapping()
    if invocation.output_limit is not None:
        result["outputLimit"] = invocation.output_limit.to_mapping()
    return result


def describe_model_call(
    gateway: object,
    messages: Sequence[AgentMessage],
    invocation: ModelInvocation,
) -> dict[str, Any]:
    describe = getattr(gateway, "describe_invocation", None)
    if callable(describe):
        value = describe(messages, invocation)
        if isinstance(value, Mapping):
            return _redact(value)
    return build_model_call_parameters(messages, invocation)


def _redact(value: Any, *, field_name: str | None = None) -> Any:
    normalized = _normalized_field_name(field_name or "")
    if normalized in _SECRET_FIELD_NAMES:
        return "<redacted>"
    if isinstance(value, Mapping):
        return {
            str(key): _redact(item, field_name=str(key))
            for key, item in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return [_redact(item) for item in value]
    if isinstance(value, str) and normalized.endswith("url"):
        return _redact_url(value)
    return value


def _redact_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return value
    if not parsed.scheme or not parsed.netloc:
        return value
    hostname = parsed.hostname or ""
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"
    netloc = hostname
    if parsed.port is not None:
        netloc = f"{netloc}:{parsed.port}"
    query = urlencode([
        (
            key,
            "<redacted>"
            if _normalized_field_name(key) in _SECRET_FIELD_NAMES
            else item,
        )
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
    ])
    return urlunsplit((
        parsed.scheme,
        netloc,
        parsed.path,
        query,
        parsed.fragment,
    ))


def _normalized_field_name(value: str) -> str:
    return "".join(char for char in value.lower() if char.isalnum())


__all__ = [
    "build_model_call_parameters",
    "describe_model_call",
]
