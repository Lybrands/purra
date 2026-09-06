# Examples

English | [简体中文](README.zh-CN.md)

Runnable examples using local model gateways and in-memory storage. No API key or
network model call is required.

| Example | Python | TypeScript |
| --- | --- | --- |
| Run a tool and return an answer | [quickstart.py](python/quickstart.py) | [quickstart.ts](../typescript/examples/quickstart.ts) |
| Subscribe to planning progress, cancel, and replay events | [planner_streaming.py](python/planner_streaming.py) | [planner-streaming.ts](../typescript/examples/planner-streaming.ts) |
| Structured model task with one bounded repair | [structured_task.py](python/structured_task.py) | [structured-task.ts](../typescript/examples/structured-task.ts) |
| Explicit safe read concurrency | [parallel_tools.py](python/parallel_tools.py) | [parallel-tools.ts](../typescript/examples/parallel-tools.ts) |
| Actual check coverage and read-only recovery inspection | [integration_check.py](python/integration_check.py) | [integration-check.ts](../typescript/examples/integration-check.ts) |

The structured task example uses the low-level runner: its receipt explicitly has
no persistence or Root budget binding. A hosted task must use its Run's runner and
authority. Native Provider behavior is tested separately from this local example.

The optional MCP package includes standalone official-SDK stdio consumers:
[Python](../integrations/mcp/python/scripts/check_installed.py) and
[TypeScript](../integrations/mcp/typescript/scripts/check-installed.mjs).
Install Core and the optional MCP package before running them. They start a local
fixture server and close it; they do not verify a third-party MCP service.

## Python

From the repository root, with Python 3.11+:

```sh
python -m pip install -e .
python examples/python/quickstart.py
python examples/python/planner_streaming.py
python examples/python/structured_task.py
python examples/python/parallel_tools.py
python examples/python/integration_check.py
```

## TypeScript

From the repository root, with Node.js 22+:

```sh
npm --prefix typescript ci
npm --prefix typescript run example
```

The TypeScript command compiles and runs all Core examples.

To use a model service or persistent storage, replace the local adapters with
[optional packages](../integrations/README.md) or application implementations.
The quickstart Retriever reads fixed public data; an application Retriever must
check access to its own data source.
