# PurrA

English | [简体中文](README.zh-CN.md)

PurrA is an Agent runtime for Python and TypeScript. It coordinates model calls,
tool execution, planning, context, and recoverable Runs inside your application.
The application supplies its models, tools, data sources, and access rules.


## 1.0.0 candidate status

This checkout contains an unreleased 1.0.0 candidate. Install the exact local
artifacts from its candidate manifest; registry commands do not identify this candidate.
See the [candidate support and upgrade guide](conformance/release-1.0.md)
for structured tasks, read-only MCP, safe read concurrency, inspection, and 1.x compatibility.

## Install

Python 3.11+:

```sh
pip install purra
```

Node.js 22+ (ESM):

```sh
npm install purra
```

Core has no runtime dependencies. Model SDKs, storage, and memory integrations
are available as [optional packages](integrations/README.md).

## Python quickstart

Provide a model gateway, a `ModelRequest` with the selected model's capability
snapshot, and its context-window size. The gateway can come from an optional
adapter or implement PurrA's `ModelGateway` port.

```python
from purra.api import AgentCore, AgentPreset, InMemoryAgentAdapters
from purra.contracts import (
    AgentMessage, AgentRunRequest, DomainContext, ModelRequest, RuntimeLimits,
)
from purra.tools import InMemoryToolCatalog

async def ask(gateway, model: ModelRequest, context_window: int):
    storage = InMemoryAgentAdapters()
    core = AgentCore(
        model_gateway=gateway,
        preset=AgentPreset(
            id="assistant",
            revision="1",
            tool_catalog=InMemoryToolCatalog(()),
            runtime_limits=RuntimeLimits(max_run_generation_tokens=8192),
        ),
        run_repository=storage.runs,
        output_repository=storage.outputs,
        output_publisher=storage.publisher,
    )
    try:
        run = await core.submit(AgentRunRequest(
            messages=(AgentMessage(role="user", content="Hello"),),
            model=model,
            domain_context=DomainContext(namespace="example"),
            context_window=context_window,
        ))
        result = await run.wait()
        return result.final_response
    finally:
        await core.close()
```

This example stores Runs in memory. Use a persistent repository to retain them
after restart. For a complete example that runs without API credentials, see
[local examples](examples/README.md). For JavaScript and TypeScript, see the
[TypeScript guide](typescript/README.md).

## Runtime capabilities

- **Runs:** submit work, subscribe to committed events, cancel execution, and replay output.
- **Tools:** validate arguments, enforce access and effect policies, and record results.
- **Planning:** select Auto, Reactive, or Planned execution when a planner is configured.
- **Context:** allocate input budgets, retrieve external data, and compress conversation history.
- **Agent trees and long tasks:** delegate work and persist execution progress through repository ports.

The application chooses models and their limits, authorizes data access and tool
side effects, and manages persistent storage. Retrieval sources and tool results
are treated as data rather than instructions.

## Budgets and recovery

`ModelRequest.max_generation_tokens` is an optional user ceiling for one
Provider call and includes every generated token the Provider counts, including
reasoning when the profile declares inclusive accounting.
`RuntimeLimits.max_run_generation_tokens` bounds that quantity cumulatively for
a Run. `None` means no finite cumulative ceiling. A finite budget requires
reported model usage. A workflow's optional `result_capacity_target_tokens` is
only a context-sizing and diagnostic target; it does not reduce the Provider
allowance or guarantee visible output.

Recovery continues from committed checkpoints. The application must restore the
same model and tool configuration and reconcile interrupted external writes
before retrying them. Persistent storage alone does not make those writes replayable.

## Documentation

- [TypeScript guide](typescript/README.md)
- [Examples](examples/README.md)
- [Optional packages](integrations/README.md)
- [Architecture](ARCHITECTURE.md)
- [Cross-language conformance](conformance/README.md)
- [Changelog](CHANGELOG.md)

## License

[MIT](LICENSE)
