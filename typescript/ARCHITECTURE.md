# TypeScript architecture

This document describes the TypeScript implementation. The framework-wide
ownership and runtime invariants are defined in the
[root architecture document](../ARCHITECTURE.md).

## Package boundary

Consumers import from `purra`. `src/index.ts` is the only public package entry
point; source subpaths are implementation details.

The package is native ESM for Node.js. It does not import Python or mirror the
Python source tree. Shared behavior is verified through host outcomes and
stable contracts rather than identical class names.

## Composition

`Agent` is the host-facing composition root:

- `invoke()` performs one process-local execution and returns its result.
- `stream()` exposes provisional process-local model and tool events.
- `submit()` creates a canonical Run with replay, cancellation, budgets, and
  terminal authority.

Reactive execution is the default. Planning, durable tasks, context budgeting,
response validation, recovery, Artifacts, Agent trees, Operations, and
observability are explicit options around the same Root Run authority.

For Reactive Child Runs, `run/` atomically stores a private model-ready
checkpoint and journal event after each committed tool round. Agent-tree
recovery attaches a new lease holder to that canonical Run and resumes the next
model round. Missing checkpoints, half-finished Provider/tool work, and Planned
execution fail closed instead of being replayed.

## Source ownership

| Path | Owns | Must not own |
| --- | --- | --- |
| `index.ts` | Public exports | Runtime logic |
| `core/` | Agent composition and Reactive sequencing | Provider SDKs, databases, product policy |
| `model/` | Provider-facing types, stream assembly, response validation | Tool handlers and Run policy |
| `tools/` | Tool contracts, Schema checks, admission, approval, idempotency | Product permissions and model-round policy |
| `run/`, `output/` | Run state, budgets, cancellation, canonical events, replay | Provider calls and product lifecycle semantics |
| `context/` | Provider-neutral context budgets, selection, evidence | Domain retrieval policy and canonical history mutation |
| `planning/`, `extensions/` | Planning contracts and optional model-backed helpers | Product prompts, Provider SDKs, durable recovery |
| `durable/` | Task admission, leases, checkpoints, continuation, orphan coordination | Product partitioning, merging, production storage |
| `artifacts/` | Recoverable output lifecycle, revisions, access, writer claims | Run completion and product schemas |
| `agent-tree*.ts` | Stable Agent identity, immutable Child Runs, bounded scheduling, continuation, lease fencing | Product roles, prompts, or persistence choices |
| `delegation/` | Legacy bounded same-Run delegation compatibility | Recursive Child Run authority |
| `operations/` | Optional operation receipts and timing | Run terminal state and product progress |
| `observability/`, `evaluation/` | Read-only projections and deterministic checks | Runtime control and generated content |
| `testing/` | Public contract probes and process-local reference adapters | Provider SDKs and production persistence |
| `shared/` | Coded errors and stable helpers used by multiple capabilities | Feature-specific policy |

## Dependency rules

- Feature packages do not import `core/`; `core/` composes them through public
  contracts.
- `model/` never imports host tool handlers.
- Observability and evaluation cannot mutate runtime state.
- Host callbacks are invoked only through explicit public contracts.
- Provider SDKs, database drivers, product schemas, and UI code stay outside
  this package.

The built-in repositories and publishers are in-memory references. Production
hosts inject implementations that preserve the same atomicity, ordering,
fencing, and cancellation contracts.

## Cross-language alignment

Python is a behavioral reference when both runtimes implement the same host
scenario, not a code-generation source. Alignment requires the same safe
outcome and stable error contract. A matching release version does not, by
itself, claim capability parity.

Cross-language fixtures belong in `conformance/` only for wire representations
consumed by both implementations.

## Example

Run the public package example with:

```bash
npm ci
npm run example
```

Source: [`examples/quickstart.ts`](examples/quickstart.ts).
