# PurrA

English | [简体中文](README.zh-CN.md)

PurrA is an Agent runtime for Python and TypeScript. It coordinates model calls,
tool execution, planning, context, and recoverable Runs inside your application.
The application supplies its models, tools, data sources, and access rules.

## 1.0 release line

The current source version is **1.0.3**. Install matching Core and integration
versions. This update separates Python execution results from public-presentation
outcomes; TypeScript runtime behavior is unchanged. See the [changelog](CHANGELOG.md)
for the changes and their scope.

Both SDKs implement persistent approval waits and recovery for Reactive, Planned
and Auto Root Runs, host-authorized MCP writes, durable receipts, and read-only
recovery inspection. Storage and effect recovery are implemented within the
supported Root single-write boundary. Child write approvals, mixed/multiple-call
approval batches, and general distributed effect recovery are outside that
boundary. See the [approval contract](conformance/durable-approval.md) and
[implementation and acceptance boundaries](conformance/runtime-extension-acceptance.md).

Deterministic tests and selected service checks do not establish real business
MCP, downstream application, cross-platform migration, or production-load
acceptance. Validate the actual model, protocol, tools, and host configuration
used by your application. Review the [upgrade notes](docs/migrations/1.0.md) and
[public compatibility](ARCHITECTURE.md#public-compatibility) before upgrading.

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
- **Structured output:** run schema-bound object tasks with local validation or explicitly supported native constraints.
- **Context:** allocate input budgets, retrieve external data, and compress conversation history.
- **Agent trees and long tasks:** delegate work and persist execution progress through repository ports.
- **Model selection:** select an authorized compatible binding before Run creation and retain it through recovery.
- **Memory:** use the optional Mem0 integration for scoped memory, authorized capture, explicit relations, and host-selected context.
- **Static images:** supply inline PNG, JPEG, or WebP input for text responses through a compatible, explicitly enabled model transport.

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

The optional SQLite integration stores Runs, output, checkpoints, leases, and
receipts. A host-driven recovery worker can discover and inspect persisted Runs
before calling public resume APIs; discovery does not authorize execution or
clear unknown effects. SQLite v4-to-v5 approval activation is an explicit offline
operation, and Python and TypeScript databases are not interchangeable. See
[recovery workers](conformance/recovery-worker.md) and
[SQLite upgrade and rollback](conformance/sqlite-upgrade.md).

## Execution and public replies

In Python's `validated_result` mode with `model_live` presentation,
`AgentRunResult.presentation` records an optional `completed` or `failed` receipt
separately from execution status. If execution succeeds but public presentation
fails, the validated result remains committed, the Run is `done`, and
`final_response` is empty. Hosts should inspect both outcomes and avoid repeating
business execution just to regenerate a reply. Cancellation and execution
deadlines retain their execution semantics; historical Runs and other presentation
modes do not acquire an inferred receipt.

Hosts can also omit public presentation. Python and TypeScript expose different
result APIs, and host-only results must not be forwarded directly to a public
chat UI. The [host policy guide](docs/host-policies.md) documents composition,
defaults, and output visibility.

## Documentation

- Getting started: [TypeScript guide](typescript/README.md), [local examples](examples/README.md), [optional packages](integrations/README.md).
- Execution: [host policies and output](docs/host-policies.md), [planning output](docs/planning-output.md), [structured output](docs/structured-output.md), [tool concurrency](docs/tool-concurrency.md).
- Recovery: [durable approvals](conformance/durable-approval.md), [recovery workers](conformance/recovery-worker.md), [integration and recovery inspection](docs/integration-inspection.md), [acceptance boundaries](conformance/runtime-extension-acceptance.md).
- Models and input: [model routing](conformance/model-routing.md), [static images](docs/static-images.md).
- Memory: [capture and relation contracts](conformance/memory-capture-relations.md), [context selection](integrations/mem0/selection.md), [relation proposals](integrations/mem0/relations.md).
- Upgrades: [0.x to 1.0](docs/migrations/1.0.md), [SQLite v4 to v5](conformance/sqlite-upgrade.md).
- Reference: [architecture and compatibility](ARCHITECTURE.md), [cross-language conformance](conformance/README.md), [changelog](CHANGELOG.md).

## License

[MIT](LICENSE)
