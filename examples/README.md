# Examples

English | [简体中文](README.zh-CN.md)

Runnable examples using local model gateways and in-memory storage. No API key or
network model call is required.

| Example | Python | TypeScript |
| --- | --- | --- |
| Run a tool and return an answer | [quickstart.py](python/quickstart.py) | [quickstart.ts](../typescript/examples/quickstart.ts) |
| Subscribe to planning progress, cancel, and replay events | [planner_streaming.py](python/planner_streaming.py) | [planner-streaming.ts](../typescript/examples/planner-streaming.ts) |

## Python

From the repository root, with Python 3.11+:

```sh
python -m pip install -e .
python examples/python/quickstart.py
python examples/python/planner_streaming.py
```

## TypeScript

From the repository root, with Node.js 22+:

```sh
npm --prefix typescript ci
npm --prefix typescript run example
```

The TypeScript command compiles and runs both examples.

To use a model service or persistent storage, replace the local adapters with
[optional packages](../integrations/README.md) or application implementations.
The quickstart Retriever reads fixed public data; an application Retriever must
check access to its own data source.
