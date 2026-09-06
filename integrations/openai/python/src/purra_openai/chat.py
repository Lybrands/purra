"""Official OpenAI Chat Completions transport."""

from ._structured import validate as _validate_output

import math

from openai import APIError, APIStatusError, AsyncOpenAI
from purra.cancellation import await_with_cancellation
from purra.contracts import (AgentMessage, ModelCompletion, ModelFinishReason,
                            ModelStream, ModelStreamActivity, ModelStreamActivityKind,
                            ModelStreamActivitySupport, ModelStreamChunk, ModelTokenUsage,
                            ToolCall, ToolCallDelta)
from purra.errors import AgentCoreError, ModelGatewayError
from purra.json_values import thaw_json_mapping


def _text(value):
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError("OpenAI Chat Completions accepts text content only")
    return value


def _usage(value):
    if not value or value.get("prompt_tokens") is None or value.get("completion_tokens") is None:
        return None
    return ModelTokenUsage(
        input_tokens=value["prompt_tokens"], generation_tokens=value["completion_tokens"],
        total_tokens=value.get("total_tokens"),
        cached_input_tokens=(value.get("prompt_tokens_details") or {}).get("cached_tokens", 0),
        reasoning_tokens=(value.get("completion_tokens_details") or {}).get("reasoning_tokens"),
    )


def _finish(reason):
    values = {"stop": ModelFinishReason.STOP, "length": ModelFinishReason.LENGTH,
              "tool_calls": ModelFinishReason.TOOL_CALLS, "content_filter": ModelFinishReason.FILTERED}
    if reason not in values:
        raise ModelGatewayError("Unknown OpenAI finish reason", code="invalid_model_response")
    return values[reason]


def _error(error):
    if isinstance(error, AgentCoreError):
        return error
    if isinstance(error, APIError):
        status = error.status_code if isinstance(error, APIStatusError) else None
        return ModelGatewayError("OpenAI request failed", code=f"openai_http_{status}" if status else "openai_transport_error",
                                 retryable=status is None or status in (408, 409, 429) or status >= 500)
    return ModelGatewayError("Invalid OpenAI Chat Completions response", code="invalid_model_response")


class OpenAIChatCompletionsGateway:
    """Text and function tools; the host supplies model selection and capabilities."""

    def __init__(self, client: AsyncOpenAI | None = None, *, timeout_seconds: float = 60):
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive and finite")
        self._owns_client = client is None
        self._client = (client or AsyncOpenAI()).with_options(max_retries=0, timeout=timeout_seconds)

    def validate_output_contract(self, invocation):
        return _validate_output(invocation)

    def _request(self, messages, invocation):
        self.validate_output_contract(invocation)
        wire = {}
        if invocation.output_contract is not None and invocation.output_contract.mode == "native_required":
            fmt = {"type": "json_schema", "name": "purra_output", "strict": True,
                   "schema": thaw_json_mapping(invocation.output_contract.schema)}
            wire = {"response_format": {"type": "json_schema", "json_schema": {k: v for k, v in fmt.items() if k != "type"}}}
        if invocation.request.provider != "openai":
            raise ValueError("OpenAI gateway requires provider='openai'")
        cap = invocation.max_generation_tokens
        if cap is None:
            raise ValueError("OpenAI gateway requires a resolved generation allowance")
        options = thaw_json_mapping(invocation.request.options)
        if set(options) - {"temperature", "top_p", "reasoning_effort"}:
            raise ValueError("unsupported OpenAI model options")
        effort = options.get("reasoning_effort")
        if invocation.reasoning_mode.value == "disabled" and effort not in (None, "none"):
            raise ValueError("reasoning effort conflicts with disabled reasoning")
        if invocation.reasoning_mode.value == "disabled":
            effort = "none"
        elif invocation.reasoning_mode.value == "enabled" and effort == "none":
            raise ValueError("reasoning effort conflicts with enabled reasoning")
        rows = []
        for message in messages:
            if message.tool_calls and message.role.value != "assistant":
                raise ValueError("only assistant messages may carry tool calls")
            row = {"role": message.role.value, "content": _text(message.content)}
            if message.role.value == "tool":
                row["tool_call_id"] = message.tool_call_id
            if message.tool_calls:
                row["tool_calls"] = [{"id": call.id, "type": "function", "function": {
                    "name": call.name, "arguments": call.arguments_json}} for call in message.tool_calls]
            rows.append(row)
        params = dict(**wire, model=invocation.request.model, messages=rows, max_completion_tokens=cap, store=False,
                      **{key: options[key] for key in ("temperature", "top_p") if key in options})
        if effort is not None:
            params["reasoning_effort"] = effort
        if invocation.tools:
            params["tools"] = [{"type": "function", "function": {"name": t.name, "description": t.description,
                "parameters": thaw_json_mapping(t.parameters), "strict": False}} for t in invocation.tools]
            params["tool_choice"] = invocation.tool_choice.value
        return params

    async def complete(self, messages, invocation, signal=None):
        params = self._request(messages, invocation)
        try:
            response = await await_with_cancellation(self._client.chat.completions.create(**params), signal)
            raw = response.model_dump()
            if len(raw["choices"]) != 1 or raw["choices"][0]["index"] != 0:
                raise ValueError("expected one completion")
            choice = raw["choices"][0]
            message = choice["message"]
            if message.get("role") != "assistant":
                raise ValueError("expected assistant response")
            return ModelCompletion(
                AgentMessage("assistant", _text(message.get("content")), tool_calls=tuple(
                    ToolCall(id=c["id"], name=c["function"]["name"], arguments_json=c["function"]["arguments"])
                    for c in message.get("tool_calls") or ())),
                model=response.model,
                finish_reason=ModelFinishReason.FILTERED if message.get("refusal") else _finish(choice["finish_reason"]),
                usage=_usage(raw.get("usage")), applied_generation_limit=params["max_completion_tokens"],
            )
        except Exception as error:
            raise _error(error) from None

    async def stream(self, messages, invocation, signal=None):
        params = self._request(messages, invocation)
        async def chunks():
            stream = None
            finish = None
            usage = None
            refused = False
            try:
                stream = await await_with_cancellation(self._client.chat.completions.create(
                    **params, stream=True, stream_options={"include_usage": True}), signal)
                iterator = stream.__aiter__()
                while True:
                    try:
                        chunk = await await_with_cancellation(anext(iterator), signal)
                    except StopAsyncIteration:
                        break
                    raw = chunk.model_dump()
                    if raw.get("usage") is not None:
                        usage = _usage(raw["usage"])
                    choices = raw.get("choices", [])
                    if not choices:
                        yield ModelStreamActivity(ModelStreamActivityKind.TRANSPORT)
                        continue
                    if len(choices) != 1 or choices[0].get("index") != 0 or finish is not None:
                        raise ValueError("invalid completion sequence")
                    choice = choices[0]
                    delta = choice["delta"]
                    refused = refused or bool(delta.get("refusal"))
                    calls = tuple(ToolCallDelta(index=c["index"], id=c.get("id"), type=c.get("type"),
                        name=(c.get("function") or {}).get("name"),
                        arguments_fragment=(c.get("function") or {}).get("arguments") or "")
                        for c in delta.get("tool_calls") or ())
                    content = _text(delta.get("content"))
                    if calls or content:
                        yield ModelStreamChunk(content_delta=content, tool_call_deltas=calls)
                    else:
                        yield ModelStreamActivity(ModelStreamActivityKind.TRANSPORT)
                    if choice.get("finish_reason") is not None:
                        finish = _finish(choice["finish_reason"])
                if finish is None:
                    raise ModelGatewayError("OpenAI stream ended without a finish reason", code="upstream_stream_interrupted")
                yield ModelStreamChunk(finish_reason=ModelFinishReason.FILTERED if refused else finish, usage=usage)
            except Exception as error:
                raise _error(error) from None
            finally:
                if stream is not None:
                    await stream.close()
        return ModelStream(chunks(), invocation.request.model, applied_generation_limit=params["max_completion_tokens"],
                           activity_support=ModelStreamActivitySupport.TRANSPORT)

    async def close(self):
        if self._owns_client:
            await self._client.close()
