# purra

`purra` is a product-neutral Agent runtime for JavaScript and TypeScript. It is
a native ESM package for Node.js 22+.

## Install

```bash
npm install purra
```

## Quick start

```ts
import { Agent } from "purra";

const agent = new Agent({ model: yourModelGateway });
const run = await agent.submit({
  messages: [{ role: "user", content: "Hello" }],
});

for await (const event of run.events()) {
  console.log(event.kind, event.payload);
}

const result = await run.result;
console.log(result.output);
```

The complete deterministic model-and-tool example is
[`examples/quickstart.ts`](https://github.com/Lybrands/purra/blob/main/typescript/examples/quickstart.ts).

The host supplies the `ModelGateway`. `invoke()` returns a process-local result,
`stream()` exposes provisional events, and `submit()` creates a canonical Run
with replay, cancellation, and budgets. Built-in repositories are in-memory;
inject production implementations for restart safety or multiple workers.

## Tools

Every tool declares an object Schema and effect policy. The runtime admits a
complete batch before any handler starts; side-effecting tools require
idempotency, and `confirm` tools also require host approval.

In a normal Agent run, a model round that receives tool schemas remains private
even when it returns a final answer without calling a tool. PurrA performs one
additional tool-free presentation round and publishes only that answer. The
extra Provider call is subject to the Run deadline and model-attempt/token
budgets; explicit budgets must reserve capacity for it. Provisional deltas from
the tool-capable candidate are not emitted by `Agent.stream()`.

## Child Agents

Enable canonical Child Runs by supplying the Agent tree repository together
with the Run and output adapters:

```ts
import { Agent, InMemoryAgentAdapters } from "purra";

const adapters = new InMemoryAgentAdapters();
const agent = new Agent({
  model: yourModelGateway,
  runRepository: adapters.runs,
  outputPublisher: adapters.outputs,
  agentTree: { repository: adapters.runTree },
});
```

This enables `delegateToAgents` with stable Agent identity, bounded recursion,
structured joins, continuation commands, Root-scoped budgets, and attributed
output. Like Python, canonical Root and Child Runs use validated-result mode and
do not add the normal public-presentation invocation. `recoverAgentTreeRoot()`
rebinds an active Root and scans all persisted descendants. In-memory adapters
are for local execution and tests; restart-safe hosts must implement the same
repository atomicity and lease contracts.

## Composition

Reactive execution is the default; Planned and Durable execution are opt-in.
The host owns Provider adapters, tools, business authorization, and production
persistence. Consumers import only from `purra`; package subpaths are private.
See [Architecture](https://github.com/Lybrands/purra/blob/main/ARCHITECTURE.md).

## Compatibility

The package is pre-1.0. Breaking public-contract changes require a new minor
release. npm and Python use the version encoded by the same Git tag; matching
versions do not imply automatic capability parity.

## Links

- [Repository guide](https://github.com/Lybrands/purra#readme)
- [TypeScript implementation notes](https://github.com/Lybrands/purra/blob/main/typescript/ARCHITECTURE.md)
- [Issue tracker](https://github.com/Lybrands/purra/issues)

## License

[MIT](https://github.com/Lybrands/purra/blob/main/typescript/LICENSE)
