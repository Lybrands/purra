from __future__ import annotations

from dataclasses import replace

import pytest

from purra.contracts import (
    AgentMessage,
    ExecutionState,
    ModelCompletion,
    ModelFinishReason,
    ModelInvocation,
    InvocationOutputBudget,
    ModelRequest,
    ModelStream,
    ModelStreamChunk,
    ToolBatchRequest,
    ToolCall,
    ToolCallDelta,
    ToolHandlerResult,
    ToolPolicy,
    ToolSchema,
)
from purra.operations import AgentOperationController
from purra.model_protocol import generic_capability_snapshot
from purra.ports import ToolRegistration
from purra.testing import (
    assert_model_gateway_conforms,
    assert_tool_execution_gateway_conforms,
)
from purra.tools.executor import CoreToolExecutor
from purra.tools.registry import InMemoryToolCatalog


class _OperationOutput:
    def __init__(self):
        self.events = []

    async def accept_operation_event(self, event):
        self.events.append(event)
        return event


def _tool() -> ToolSchema:
    return ToolSchema(
        name="readThing",
        description="Read a portable thing",
        parameters={
            "type": "object",
            "properties": {"id": {"type": "string"}},
            "required": ["id"],
            "additionalProperties": False,
        },
    )


def _invocation() -> ModelInvocation:
    return ModelInvocation(
        request=ModelRequest(
            provider="portable",
            model="portable-model",
            capability_snapshot=replace(
                generic_capability_snapshot(),
                max_generation_tokens=256,
            ),
            max_generation_tokens=256,
        ),
        tools=(_tool(),),
        tool_choice="required",
        output_budget=InvocationOutputBudget(
            max_generation_tokens=256,
            generation_source="user",
            profile_max_generation_tokens=256,
            requested_user_max_generation_tokens=256,
        ),
    )


class _PortableModelGateway:
    async def stream(self, messages, invocation, signal=None):
        del messages, signal

        async def chunks():
            yield ModelStreamChunk(tool_call_deltas=(ToolCallDelta(
                index=0,
                id="call-1",
                name="readThing",
                arguments_fragment='{"id":',
            ),))
            yield ModelStreamChunk(
                tool_call_deltas=(ToolCallDelta(
                    index=0,
                    arguments_fragment='"thing-1"}',
                ),),
                finish_reason=ModelFinishReason.TOOL_CALLS,
            )

        return ModelStream(
            chunks=chunks(),
            model="portable-model",
            applied_generation_limit=invocation.output_budget.max_generation_tokens,
        )

    async def complete(self, messages, invocation, signal=None):
        del messages, signal
        return ModelCompletion(
            message=AgentMessage(
                role="assistant",
                tool_calls=(ToolCall(
                    id="call-2",
                    name="readThing",
                    arguments_json='{"id":"thing-2"}',
                ),),
            ),
            model="portable-model",
            applied_generation_limit=invocation.output_budget.max_generation_tokens,
            finish_reason=ModelFinishReason.TOOL_CALLS,
        )


@pytest.mark.asyncio
async def test_model_gateway_passes_stream_and_tool_call_conformance():
    chunks, completion = await assert_model_gateway_conforms(
        gateway=_PortableModelGateway(),
        messages=(AgentMessage(role="user", content="read it"),),
        invocation=_invocation(),
        expected_model="portable-model",
        expected_tool_names=("readThing",),
    )

    assert len(chunks) == 2
    assert completion.message.tool_calls[0].arguments_json == '{"id":"thing-2"}'


@pytest.mark.asyncio
async def test_model_gateway_probe_rejects_stream_without_terminal_reason():
    class InterruptedGateway(_PortableModelGateway):
        async def stream(self, messages, invocation, signal=None):
            del messages, signal

            async def chunks():
                yield ModelStreamChunk(content_delta="unfinished")

            return ModelStream(
                chunks=chunks(),
                model="portable-model",
                applied_generation_limit=invocation.output_budget.max_generation_tokens,
            )

    with pytest.raises(AssertionError):
        await assert_model_gateway_conforms(
            gateway=InterruptedGateway(),
            messages=(),
            invocation=_invocation(),
        )


@pytest.mark.asyncio
async def test_model_gateway_probe_rejects_incomplete_tool_call():
    class IncompleteToolGateway(_PortableModelGateway):
        async def stream(self, messages, invocation, signal=None):
            del messages, signal

            async def chunks():
                yield ModelStreamChunk(
                    tool_call_deltas=(ToolCallDelta(
                        index=0,
                        name="readThing",
                        arguments_fragment='{"id":"thing-1"}',
                    ),),
                    finish_reason=ModelFinishReason.TOOL_CALLS,
                )

            return ModelStream(
                chunks=chunks(),
                model="portable-model",
                applied_generation_limit=invocation.output_budget.max_generation_tokens,
            )

    with pytest.raises(AssertionError, match="missing_tool_call_id"):
        await assert_model_gateway_conforms(
            gateway=IncompleteToolGateway(),
            messages=(),
            invocation=_invocation(),
        )


@pytest.mark.asyncio
async def test_core_tool_executor_passes_shared_host_conformance():
    calls = 0
    operation_output = _OperationOutput()

    async def read_thing(state, arguments, signal=None):
        nonlocal calls
        del signal
        calls += 1
        state.domain["read"] = arguments["id"]
        return ToolHandlerResult('{"ok":true}')

    executor = CoreToolExecutor(
        InMemoryToolCatalog((ToolRegistration(
            schema=_tool(),
            handler=read_thing,
            policy=ToolPolicy(mode="read", title="Read thing"),
            operation_display_params=(
                lambda state, arguments, tool_call: {"episodeNumber": 3}
            ),
        ),)),
        operation_controller=AgentOperationController(operation_output),
    )
    state = ExecutionState()
    result, events = await assert_tool_execution_gateway_conforms(
        gateway=executor,
        request=ToolBatchRequest(
            run_id="run-conformance",
            invocation_id="invocation-conformance",
            calls=(ToolCall(
                id="call-read",
                name="readThing",
                arguments_json='{"id":"thing-1"}',
            ),),
            allowed_tool_names=frozenset({"readThing"}),
            state=state,
            retry_of_tool_call_ids={"call-read": "call-read-invalid"},
        ),
    )

    assert calls == 1
    assert state.domain == {"read": "thing-1"}
    assert result.results[0].content == '{"ok":true}'
    assert events[-1].payload["toolCallId"] == "call-read"
    assert operation_output.events[0].display["labelParams"] == {
        "toolCallId": "call-read",
        "toolName": "readThing",
        "retryOfToolCallId": "call-read-invalid",
        "episodeNumber": 3,
    }


@pytest.mark.asyncio
async def test_invalid_batch_schema_stops_every_handler_before_side_effects():
    calls = 0

    async def read_thing(state, arguments, signal=None):
        nonlocal calls
        del state, arguments, signal
        calls += 1
        return ToolHandlerResult('{"ok":true}')

    executor = CoreToolExecutor(InMemoryToolCatalog((ToolRegistration(
        schema=_tool(),
        handler=read_thing,
        policy=ToolPolicy(mode="read", title="Read thing"),
    ),)))

    async def sink(event):
        del event

    result = await executor.execute_batch(
        ToolBatchRequest(
            run_id="run-fail-closed",
            invocation_id="invocation-fail-closed",
            calls=(
                ToolCall(
                    id="call-valid",
                    name="readThing",
                    arguments_json='{"id":"thing-1"}',
                ),
                ToolCall(
                    id="call-invalid",
                    name="readThing",
                    arguments_json='{"id":42}',
                ),
            ),
            allowed_tool_names=frozenset({"readThing"}),
            state=ExecutionState(),
        ),
        sink,
    )

    assert calls == 0
    assert result.outcome.value == "failed"
    assert all(
        item.error == "invalid_tool_arguments_schema"
        for item in result.results
    )
