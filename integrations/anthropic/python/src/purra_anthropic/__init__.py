"""Official Anthropic Messages adapter for PurrA."""

import json
from ._structured import validate as _validate_output

import math

from anthropic import APIError, APIStatusError, AsyncAnthropic
from purra.cancellation import await_with_cancellation, raise_if_stopped
from purra.contracts import (AgentMessage, ModelCompletion, ModelFinishReason, ModelStream,
                            ModelStreamActivity, ModelStreamActivityKind, ModelStreamActivitySupport,
                            ModelStreamChunk, ModelTokenUsage, ToolCall, ToolCallDelta)
from purra.errors import AgentCoreError, ModelGatewayError, UnsupportedModelFeatureError
from purra.json_values import thaw_json_mapping

_REPLAY = "anthropic_message"
_MAX_CONTINUATION_CHARS = 1_000_000


def _text(value):
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError("Anthropic adapter accepts text messages only")
    return value


def _projection(blocks):
    text = []
    calls = []
    for block in blocks:
        kind = block["type"]
        if kind == "text":
            text.append(_text(block["text"]))
        elif kind == "tool_use":
            if not isinstance(block["input"], dict):
                raise ValueError("tool input must be an object")
            calls.append(ToolCall(id=block["id"], name=block["name"], arguments_json=json.dumps(block["input"], ensure_ascii=False)))
        elif kind == "thinking":
            _text(block["thinking"])
            if not isinstance(block.get("signature"), str) or not block["signature"]:
                raise ValueError("thinking continuation requires a signature")
        elif kind == "redacted_thinking":
            if not isinstance(block.get("data"), str) or not block["data"]:
                raise ValueError("redacted thinking requires opaque data")
        else:
            raise ValueError("unsupported Anthropic content block")
    return "".join(text), tuple(calls)


def _call_values(calls):
    return [(c.id, c.name, json.loads(c.arguments_json)) for c in calls]


def _input(messages, model):
    system = []
    rows = []
    for message in messages:
        role = message.role.value
        content = _text(message.content)
        if message.tool_calls and role != "assistant":
            raise ValueError("only assistant messages may carry tool calls")
        if role in ("system", "developer"):
            if content:
                system.append({"type": "text", "text": content})
            continue
        # Consecutive tool results share one user message, preserving parallel calls.
        if role == "tool":
            blocks = [{"type": "tool_result", "tool_use_id": message.tool_call_id, "content": content}]
            role = "user"
        elif role == "assistant":
            replay = thaw_json_mapping(message.provider_data).get(_REPLAY)
            if replay is not None:
                if not isinstance(replay, dict) or replay.get("model") != model or not isinstance(replay.get("content"), list):
                    raise ValueError("Anthropic continuation belongs to a different model")
                blocks = replay["content"]
                if len(json.dumps(blocks, ensure_ascii=False)) > _MAX_CONTINUATION_CHARS:
                    raise ValueError("Anthropic continuation exceeds limit")
                replay_text, replay_calls = _projection(blocks)
                if replay_text != content or _call_values(replay_calls) != _call_values(message.tool_calls):
                    raise ValueError("Anthropic continuation does not match assistant message")
            else:
                blocks = [{"type": "text", "text": content}] if content else []
                for call in message.tool_calls:
                    args = json.loads(call.arguments_json)
                    if not isinstance(args, dict):
                        raise ValueError("tool input must be an object")
                    blocks.append({"type": "tool_use", "id": call.id, "name": call.name, "input": args})
        else:
            blocks = [{"type": "text", "text": content}]
        if not blocks:
            raise ValueError("Anthropic message cannot be empty")
        if rows and rows[-1]["role"] == role:
            rows[-1]["content"].extend(blocks)
        else:
            rows.append({"role": role, "content": blocks})
    return rows, system


def _attributes(blocks, model):
    if not any(b["type"] in ("thinking", "redacted_thinking") for b in blocks):
        return {}
    if len(json.dumps(blocks, ensure_ascii=False)) > _MAX_CONTINUATION_CHARS:
        raise ValueError("Anthropic continuation exceeds limit")
    return {_REPLAY: {"model": model, "content": blocks}}


def _usage(value):
    if not value or value.get("input_tokens") is None or value.get("output_tokens") is None:
        return None
    cached = value.get("cache_read_input_tokens") or 0
    inputs = value["input_tokens"] + cached + (value.get("cache_creation_input_tokens") or 0)
    return ModelTokenUsage(
        input_tokens=inputs,
        generation_tokens=value["output_tokens"],
        cached_input_tokens=cached,
        reasoning_tokens=(value.get("output_tokens_details") or {}).get(
            "thinking_tokens"
        ),
    )


def _finish(reason):
    values = {"end_turn": ModelFinishReason.STOP, "stop_sequence": ModelFinishReason.STOP,
              "tool_use": ModelFinishReason.TOOL_CALLS, "max_tokens": ModelFinishReason.LENGTH,
              "model_context_window_exceeded": ModelFinishReason.LENGTH, "refusal": ModelFinishReason.FILTERED}
    if reason not in values:
        raise ModelGatewayError("Unsupported Anthropic stop reason", code="invalid_model_response")
    return values[reason]


def _error(error):
    if isinstance(error, AgentCoreError):
        return error
    if isinstance(error, APIError):
        status = error.status_code if isinstance(error, APIStatusError) else None
        return ModelGatewayError("Anthropic request failed", code=f"anthropic_http_{status}" if status else "anthropic_transport_error",
                                 retryable=status is None or status in (408, 409, 429) or status >= 500)
    return ModelGatewayError("Invalid Anthropic response", code="invalid_model_response")


class AnthropicMessagesGateway:
    """Text, function tools and signed thinking continuation via the official SDK."""

    def __init__(self, client: AsyncAnthropic | None = None, *, timeout_seconds: float = 60):
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive and finite")
        self._owns_client = client is None
        self._client = (client or AsyncAnthropic()).with_options(max_retries=0, timeout=timeout_seconds)

    def validate_output_contract(self, invocation):
        config = invocation.request.options.get("output_config", {})
        if invocation.output_contract is not None and "format" in config:
            from purra.structured import StructuredOutputError
            raise StructuredOutputError("Output format belongs to the output contract", code="structured_output_mode_unsupported")
        return _validate_output(invocation)

    def _request(self, messages, invocation):
        self.validate_output_contract(invocation)
        if invocation.request.provider != "anthropic":
            raise ValueError("Anthropic gateway requires provider='anthropic'")
        cap = invocation.max_generation_tokens
        if cap is None:
            raise ValueError("Anthropic gateway requires a resolved generation allowance")
        options = thaw_json_mapping(invocation.request.options)
        if set(options) - {"temperature", "top_p", "top_k", "thinking", "output_config"}:
            raise ValueError("unsupported Anthropic model options")
        thinking = options.get("thinking")
        mode = invocation.reasoning_mode.value
        if thinking is not None:
            if not isinstance(thinking, dict) or thinking.get("type") not in ("enabled", "adaptive", "disabled"):
                raise ValueError("invalid Anthropic thinking configuration")
            if thinking["type"] == "enabled" and (type(thinking.get("budget_tokens")) is not int or not 1024 <= thinking["budget_tokens"] < cap):
                raise ValueError("thinking budget must be at least 1024 and below the output limit")
        if mode == "disabled":
            if thinking is not None and thinking["type"] != "disabled":
                raise ValueError("thinking configuration conflicts with disabled reasoning")
            thinking = {"type": "disabled"}
        elif mode == "enabled" and (thinking is None or thinking["type"] == "disabled"):
            raise UnsupportedModelFeatureError("enabled reasoning requires an explicit Anthropic thinking configuration")
        choice = invocation.tool_choice.value
        if choice == "required" and thinking and thinking["type"] != "disabled":
            raise UnsupportedModelFeatureError("Anthropic thinking does not support required tool choice")
        rows, system = _input(messages, invocation.request.model)
        params = dict(model=invocation.request.model, messages=rows, max_tokens=cap,
                      **{key: options[key] for key in ("temperature", "top_p", "top_k", "output_config") if key in options})
        if invocation.output_contract is not None and invocation.output_contract.mode == "native_required":
            params["output_config"] = {**params.get("output_config", {}), "format": {
                "type": "json_schema", "schema": thaw_json_mapping(invocation.output_contract.schema)}}
        if system:
            params["system"] = system
        if thinking is not None:
            params["thinking"] = thinking
        if invocation.tools:
            params["tools"] = [{"name": t.name, "description": t.description, "input_schema": thaw_json_mapping(t.parameters)} for t in invocation.tools]
            params["tool_choice"] = {"type": "any" if choice == "required" else choice}
        return params

    async def complete(self, messages, invocation, signal=None):
        params = self._request(messages, invocation)
        try:
            response = await await_with_cancellation(self._client.messages.create(**params), signal)
            raw = response.model_dump(exclude_none=True)
            content, calls = _projection(raw["content"])
            return ModelCompletion(AgentMessage("assistant", content, tool_calls=calls,
                provider_data=_attributes(raw["content"], params["model"])),
                model=response.model, finish_reason=_finish(response.stop_reason), usage=_usage(raw.get("usage")),
                applied_generation_limit=params["max_tokens"])
        except Exception as error:
            raise _error(error) from None

    async def stream(self, messages, invocation, signal=None):
        params = self._request(messages, invocation)
        async def chunks():
            stream = None
            terminal = False
            private_chars = 0
            empty_tools = set()
            try:
                raise_if_stopped(signal)
                manager = self._client.messages.stream(**params)
                stream = await await_with_cancellation(manager.__aenter__(), signal)
                iterator = stream.__aiter__()
                while True:
                    try:
                        event = await await_with_cancellation(anext(iterator), signal)
                    except StopAsyncIteration:
                        break
                    if event.type == "content_block_start":
                        block = event.content_block
                        if block.type == "tool_use":
                            empty_tools.add(event.index)
                            yield ModelStreamChunk(tool_call_deltas=(ToolCallDelta(index=event.index, id=block.id, name=block.name, type="function"),))
                        elif block.type == "text" and block.text:
                            yield ModelStreamChunk(content_delta=block.text)
                        elif block.type in ("thinking", "redacted_thinking"):
                            private_chars += len(json.dumps(block.model_dump(), ensure_ascii=False))
                            yield ModelStreamActivity(ModelStreamActivityKind.WORKING)
                        elif block.type != "text":
                            raise ValueError("unsupported Anthropic content block")
                    elif event.type == "content_block_delta":
                        delta = event.delta
                        if delta.type == "text_delta":
                            yield ModelStreamChunk(content_delta=delta.text)
                        elif delta.type == "input_json_delta":
                            empty_tools.discard(event.index)
                            yield ModelStreamChunk(tool_call_deltas=(ToolCallDelta(index=event.index, arguments_fragment=delta.partial_json),))
                        elif delta.type in ("thinking_delta", "signature_delta"):
                            private_chars += len(delta.thinking if delta.type == "thinking_delta" else delta.signature)
                            yield ModelStreamActivity(ModelStreamActivityKind.WORKING)
                        else:
                            raise ValueError("unsupported Anthropic content delta")
                    elif event.type == "content_block_stop" and event.index in empty_tools:
                        empty_tools.remove(event.index)
                        yield ModelStreamChunk(tool_call_deltas=(ToolCallDelta(index=event.index, arguments_fragment="{}"),))
                    elif event.type == "message_stop":
                        terminal = True
                    else:
                        yield ModelStreamActivity(ModelStreamActivityKind.TRANSPORT)
                    if private_chars > _MAX_CONTINUATION_CHARS:
                        raise ValueError("Anthropic continuation exceeds limit")
                if not terminal:
                    raise ModelGatewayError("Anthropic stream ended without message_stop", code="upstream_stream_interrupted")
                response = await await_with_cancellation(stream.get_final_message(), signal)
                raw = response.model_dump(exclude_none=True)
                _projection(raw["content"])
                yield ModelStreamChunk(finish_reason=_finish(response.stop_reason), usage=_usage(raw.get("usage")),
                                       provider_data=_attributes(raw["content"], params["model"]))
            except Exception as error:
                raise _error(error) from None
            finally:
                if stream is not None:
                    await stream.close()
        return ModelStream(chunks(), invocation.request.model, applied_generation_limit=params["max_tokens"],
                           activity_support=ModelStreamActivitySupport.WORKING)

    async def close(self):
        if self._owns_client:
            await self._client.close()


__all__ = ["AnthropicMessagesGateway"]
