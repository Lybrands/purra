"""OpenAI Responses adapter; SDK retries are disabled for Core accounting."""

import math

from openai import AsyncOpenAI, APIError, APIStatusError
from purra.cancellation import await_with_cancellation
from purra.contracts import (AgentMessage, MessageRole, ModelCompletion, ModelFinishReason,
                            ModelStream, ModelStreamActivity, ModelStreamActivityKind,
                            ModelStreamActivitySupport, ModelStreamChunk, ModelTokenUsage,
                            ToolCall, ToolCallDelta)
from purra.errors import ModelGatewayError
from purra.json_values import thaw_json_value, thaw_json_mapping

_REPLAY = "openai_reasoning_items"


def _text(value):
    if value is None:
        return ""
    if not isinstance(value, str):
        raise ValueError("OpenAI adapter currently accepts text messages only")
    return value


def _input(messages):
    rows = []
    for message in messages:
        if message.tool_calls and message.role is not MessageRole.ASSISTANT:
            raise ValueError("only assistant messages may carry function calls")
        if message.role is MessageRole.ASSISTANT:
            replay = thaw_json_value(message.provider_data.get(_REPLAY, ()))
            if not isinstance(replay, list) or any(not isinstance(r, dict) or r.get("type") != "reasoning" for r in replay):
                raise ValueError("invalid OpenAI reasoning continuation")
            rows.extend(replay)
        content = _text(message.content)
        if message.role is MessageRole.TOOL:
            rows.append({"type": "function_call_output", "call_id": message.tool_call_id, "output": content})
            continue
        if content or not message.tool_calls:
            rows.append({"role": message.role.value, "content": content})
        rows.extend({"type": "function_call", "call_id": call.id, "name": call.name,
                     "arguments": call.arguments_json} for call in message.tool_calls)
    return rows


def _usage(response):
    usage = response.usage
    if usage is None:
        return None
    input_details = getattr(usage, "input_tokens_details", None)
    output_details = getattr(usage, "output_tokens_details", None)
    return ModelTokenUsage(input_tokens=usage.input_tokens, generation_tokens=usage.output_tokens,
                           total_tokens=usage.total_tokens,
                           cached_input_tokens=(getattr(input_details, "cached_tokens", 0) or 0),
                           reasoning_tokens=getattr(output_details, "reasoning_tokens", None))


def _finish(response):
    if response.status == "incomplete":
        reason = response.incomplete_details.reason if response.incomplete_details else None
        return ModelFinishReason.LENGTH if reason == "max_output_tokens" else ModelFinishReason.FILTERED if reason == "content_filter" else ModelFinishReason.OTHER
    if response.status != "completed":
        raise ModelGatewayError("OpenAI response failed", code="openai_response_failed")
    if any(item.type == "function_call" for item in response.output):
        return ModelFinishReason.TOOL_CALLS
    if any(part.type == "refusal" for item in response.output if item.type == "message" for part in item.content):
        return ModelFinishReason.FILTERED
    return ModelFinishReason.STOP


def _attributes(response):
    items = [item.model_dump(exclude_none=True) for item in response.output if item.type == "reasoning"]
    return {_REPLAY: items} if items else {}


def _error(error):
    status = error.status_code if isinstance(error, APIStatusError) else None
    return ModelGatewayError("OpenAI request failed", code=f"openai_http_{status}" if status else "openai_transport_error",
                             retryable=status is None or status in (408, 409, 429) or status >= 500)


class OpenAIResponsesGateway:
    """Text and function tools, with private encrypted reasoning continuity.

    Pass a host-owned AsyncOpenAI to share its transport. The gateway makes one
    HTTP attempt per managed invocation and reports actual usage/output limits.
    """

    def __init__(self, client: AsyncOpenAI | None = None, *, timeout_seconds: float = 60):
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive and finite")
        self._owns_client = client is None
        self._client = (client or AsyncOpenAI()).with_options(max_retries=0, timeout=timeout_seconds)

    def _request(self, messages, invocation):
        if invocation.request.provider != "openai":
            raise ValueError("OpenAI gateway requires provider='openai'")
        cap = invocation.max_generation_tokens
        if cap is None:
            raise ValueError("OpenAI gateway requires a resolved generation allowance")
        options = thaw_json_mapping(invocation.request.options)
        unknown = set(options) - {"temperature", "top_p", "reasoning_effort"}
        if unknown:
            raise ValueError("unsupported OpenAI model options: " + ", ".join(sorted(unknown)))
        effort = options.get("reasoning_effort")
        if invocation.reasoning_mode.value == "disabled" and effort not in (None, "none"):
            raise ValueError("reasoning effort conflicts with disabled reasoning")
        if invocation.reasoning_mode.value == "disabled":
            effort = "none"
        elif invocation.reasoning_mode.value == "enabled" and effort == "none":
            raise ValueError("reasoning effort conflicts with enabled reasoning")
        return dict(model=invocation.request.model, input=_input(messages),
                    tools=[{"type": "function", "name": t.name, "description": t.description,
                            "parameters": thaw_json_mapping(t.parameters), "strict": False} for t in invocation.tools],
                    tool_choice=invocation.tool_choice.value if invocation.tools else "none",
                    max_output_tokens=cap, store=False, include=["reasoning.encrypted_content"],
                    **({"reasoning": {"effort": effort}} if effort is not None else {}),
                    **{k: options[k] for k in ("temperature", "top_p") if k in options})

    async def complete(self, messages, invocation, signal=None):
        params = self._request(messages, invocation)
        try:
            response = await await_with_cancellation(self._client.responses.create(**params), signal)
        except APIError as error:
            raise _error(error) from None
        return ModelCompletion(
            AgentMessage(MessageRole.ASSISTANT, response.output_text,
                         tool_calls=tuple(ToolCall(id=i.call_id, name=i.name, arguments_json=i.arguments)
                                          for i in response.output if i.type == "function_call"),
                         provider_data=_attributes(response)),
            model=response.model, finish_reason=_finish(response), usage=_usage(response),
            applied_generation_limit=params["max_output_tokens"],
        )

    async def stream(self, messages, invocation, signal=None):
        params = self._request(messages, invocation)
        async def chunks():
            terminal = False
            stream = None
            try:
                stream = await await_with_cancellation(self._client.responses.create(**params, stream=True), signal)
                iterator = stream.__aiter__()
                while True:
                    try:
                        event = await await_with_cancellation(anext(iterator), signal)
                    except StopAsyncIteration:
                        break
                    if event.type == "response.output_text.delta":
                        yield ModelStreamChunk(content_delta=event.delta)
                    elif event.type in ("response.reasoning_text.delta", "response.reasoning_summary_text.delta"):
                        yield ModelStreamActivity(ModelStreamActivityKind.WORKING)
                    elif event.type == "response.output_item.added" and event.item.type == "function_call":
                        yield ModelStreamChunk(tool_call_deltas=(ToolCallDelta(index=event.output_index,
                            id=event.item.call_id, name=event.item.name, type="function",
                            arguments_fragment=event.item.arguments),))
                    elif event.type == "response.function_call_arguments.delta":
                        yield ModelStreamChunk(tool_call_deltas=(ToolCallDelta(index=event.output_index, arguments_fragment=event.delta),))
                    elif event.type in ("response.completed", "response.incomplete", "response.failed"):
                        terminal = True
                        yield ModelStreamChunk(finish_reason=_finish(event.response), usage=_usage(event.response),
                                               provider_data=_attributes(event.response))
                        break
                    elif event.type == "error":
                        raise ModelGatewayError("OpenAI stream failed", code="openai_stream_error")
                    else:
                        yield ModelStreamActivity(ModelStreamActivityKind.TRANSPORT)
                if not terminal:
                    raise ModelGatewayError("OpenAI stream ended without a terminal response", code="upstream_stream_interrupted")
            except APIError as error:
                raise _error(error) from None
            finally:
                if stream is not None:
                    await stream.close()

        return ModelStream(chunks(), invocation.request.model, applied_generation_limit=params["max_output_tokens"],
                           activity_support=ModelStreamActivitySupport.WORKING)

    async def close(self):
        if self._owns_client:
            await self._client.close()


from .chat import OpenAIChatCompletionsGateway

__all__ = ["OpenAIResponsesGateway", "OpenAIChatCompletionsGateway"]
