"""Deterministic read-only batch example, with no Provider or external service."""
import asyncio
import json
from purra.contracts import ExecutionState, ToolBatchRequest, ToolCall, ToolExecutionLimits, ToolHandlerResult, ToolPolicy, ToolSchema
from purra.ports import ToolRegistration
from purra.tools import CoreToolExecutor, InMemoryToolCatalog


async def main():
    records = {'alpha': {'count': 2}, 'beta': {'count': 3}}
    async def read(state, arguments, signal=None):
        return ToolHandlerResult(json.dumps(records[arguments['key']]), effect_state='not_started')
    async def scope(state, arguments, signal=None):
        return None if arguments['key'] in records else 'Unknown record'
    class Sink:
        async def emit(self, event):
            print(event.type, event.payload['toolCallId'])
    registration = ToolRegistration(
        ToolSchema('readRecord', 'Read an approved local record', {
            'type': 'object', 'properties': {'key': {'type': 'string'}},
            'required': ['key'], 'additionalProperties': False,
        }), read, ToolPolicy(mode='read', title='Read record'),
        scope_validator=scope, concurrency_safe=True,
    )
    executor = CoreToolExecutor(InMemoryToolCatalog((registration,)),
        limits=ToolExecutionLimits(max_concurrency=2))
    batch = ToolBatchRequest(None, tuple(
        ToolCall(key, 'readRecord', json.dumps({'key': key})) for key in records
    ), frozenset({'readRecord'}), ExecutionState())
    result = await executor.execute_batch(batch, Sink())
    assert [row.tool_call_id for row in result.results] == list(records)
    print([json.loads(row.content) for row in result.results])


if __name__ == '__main__':
    asyncio.run(main())
