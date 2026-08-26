# PurrA architecture

PurrA is a product-neutral execution kernel embedded by a host application. It
owns Agent runtime behavior; it does not own the host's product, Provider, or
deployment choices.

## Ownership

| PurrA owns | The host owns |
| --- | --- |
| Run lifecycle, canonical events, output ordering, cancellation, and recovery decisions | Product requests, domain semantics, and user experience |
| Model and tool execution policy | Provider SDK adapters, credentials, and model selection |
| Tool validation, authorization, approval, and idempotency contracts | Tool implementations and business authorization |
| Deadlines, budgets, leases, checkpoints, and continuation rules | Production persistence, deployment, transport, and billing |
| Optional planning, durable execution, Artifacts, and recursive Child Agent contracts | Product workflows and domain projections |

The built-in adapters are process-local references for tests and examples. A
host that needs restart safety or multi-worker coordination supplies durable
implementations through public ports.

## Runtime model

A complete execution is one Root Run with one canonical event and output
journal:

```text
host request
  -> immutable Agent preset snapshot
  -> Root Run and runtime limits
  -> model invocations and authorized tool batches
  -> persisted canonical events and output
  -> atomic terminal state and final result
  -> publication and replay
```

Model reasoning, provisional Provider data, user-visible commentary, final
output, tool lifecycle, and Artifacts remain separate channels. Publication is
downstream of persistence; subscribers are never the source of Run truth.

## Execution styles

Reactive execution is the default. The model answers directly or requests
currently authorized tools.

Planned execution is explicit. A Planner produces a semantic `WorkPlan`; Core
validates and compiles it before it can grant step-scoped tool authority. The
plan describes intended work but is not itself runtime authority.

Durable execution extends a valid Planned composition with task admission,
leased task units, checkpoints, and authenticated continuation. A continuation
reuses persisted deadlines, budgets, and behavior snapshots instead of silently
starting a new execution.

Child Agent execution is explicit. An `AgentNode` is stable identity; each
execution is a separate immutable `AgentRun`. The tree shares the Root Run's
budget authority and canonical journal, while capability grants may only
narrow down the tree. `delegateToAgents` is a synchronous create-and-wait
facade; hosts may also use the public spawn, join, continue, cancel, and close
commands directly. Root recovery rebinds the execution inputs and scans the
complete persisted descendant set before replaying work.

Reactive Child Runs persist a private `model_ready` checkpoint after a fully
committed tool round. A replacement executor may attach to the same canonical
Run and continue at the next model round only when that checkpoint, its lease,
and its Agent preset snapshot all match. An in-flight Provider stream, an
in-flight tool, and Planned execution remain fail-stop boundaries; PurrA does
not infer or replay their missing state.

## Safety invariants

- Tool schemas are checked at registration. A complete tool batch is admitted
  before any handler starts.
- Provider responses, tool arguments, persisted snapshots, and continuation
  evidence are treated as untrusted at their boundaries.
- Attempts, tokens, output, and absolute deadlines are accounted against the
  Root Run before more work is authorized.
- Child Run events are attributed projections of the Root journal, not a
  second output authority. Lease epochs fence stale Child executors.
- Canonical state is persisted before it is published.
- Cancellation and terminal settlement have one authority; late work cannot
  reopen a terminal Run.
- Side effects require explicit policy, approval where applicable, and
  idempotency or host-managed durability.
- Durable continuation fails closed when its lease, checkpoint, or Agent preset
  snapshot cannot be verified.

These invariants protect execution integrity. They do not determine whether a
business answer is correct; that remains a host responsibility.

## Public boundaries

Python hosts compose complete Runs through `purra.api.AgentCore` and implement
contracts from `purra.contracts`, `purra.ports`, and capability-specific public
modules. `purra.engine`, `purra.runtime`, and their submodules are implementation
details rather than host compatibility surfaces.

JavaScript and TypeScript hosts import from the `purra` package root. Package
subpaths are private. See [TypeScript architecture](typescript/ARCHITECTURE.md)
for its source ownership rules.

Both implementations use dependency inversion:

```text
host adapters -> public contracts and ports <- PurrA runtime
```

Provider SDKs, database drivers, product schemas, and UI code must not become
runtime dependencies.

## Cross-language releases

Python and TypeScript are independent implementations of shared behavioral
contracts. They may use different names and internal structure. Alignment means
that a shared host scenario reaches the same safe outcome and stable error
contract, not that the source trees are mirrors.

The Python and npm packages use the version encoded by the same Git tag.
Matching versions identify one release; they do not imply automatic capability
parity. Shared wire representations live in `conformance/` only when both
runtimes consume the same contract.

## Further reading

- [Runnable examples](examples/README.md)
- [TypeScript implementation notes](typescript/ARCHITECTURE.md)
- [Cross-language fixtures](conformance/README.md)
