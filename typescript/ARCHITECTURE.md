# TypeScript architecture

## Design target

The package implements host-facing Agent use cases in TypeScript. It shares
behavioral requirements with Python where a host needs them, but it does not
mirror Python modules or exports.

## Cross-language contract

Python is the behavioral reference for capabilities implemented in both
languages, not a source-code template. TypeScript keeps native names, async
primitives, and package boundaries. A capability is considered aligned when
the same host scenario reaches the same safe outcome and stable error code.

| Capability | TypeScript alpha | Python authority | Alignment |
| --- | --- | --- | --- |
| Reactive entry | `Agent.invoke()`, `Agent.stream()`, and `Agent.submit()` | `purra.api.AgentCore.submit()` | Same bounded model/tool use case; submit owns an in-memory canonical Run while invoke/stream remain transient |
| Messages | Immutable JSON content, reasoning, attributes, complete tool calls, Prompt sections, and evidence receipts | `purra.contracts.AgentMessage` and `AgentPreset` | Shared model-visible behavior with TS-native composition and fingerprinting |
| Model termination | `stop`, `tool_calls`, `length`, `filtered`, `other` | `purra.model_protocol.classify_model_termination()` | Same tool authorization and fail-closed error codes |
| Model capabilities and limits | Versioned snapshots and resolved per-invocation output limits | `purra.model_protocol` | Same wire values and incompatibility errors |
| Tool batches | Recursive Schema inspection, whole-batch enablement/scope/approval admission, idempotency, effect state, sanitized receipts, and committed lifecycle events | Runtime tool authorization/execution | Same fail-closed batch behavior; submitted Runs persist tool start before invoking a handler |
| Run and cancellation | `RunHandle`, `RunRepository`, `AbortSignal`, cancellation receipts | Run controller/state and persistence ports | In-memory authority covers ordering, budgets, cancellation races, and atomic terminal settlement; storage-neutral orphan recovery owns restart classification and settlement |
| Provider streaming and output | Transient invoke/stream plus submitted private/public canonical channels and replay | `ModelGateway.stream()` and canonical output | Chunk, usage, termination, cancellation, persist-before-publish, and close semantics aligned for the implemented Reactive path |
| Recovery | Request-scoped policy/ledger consumed by Provider, tool, Planned, and response paths | Python `purra.recovery` and runtime recovery paths | Shared decisions, bounded retries/replans, visible-output fencing, side-effect safety, and submitted content-free traces aligned |
| Context | Opt-in budget allocator, Single Pass Agent integration, Staged/task-demand resolver, structural trimming, host compression hook, evidence receipts | Context budget/orchestration/strategies/evidence | Planned uses planning context before TaskSpec and task context only after compilation |
| Managed model tasks | Public standalone runner plus per-execution context/compaction factories; submitted calls reuse Run invocation receipts and budgets | Python `AgentModelTaskRunner` and preset factories | Tool-free completion/streaming, exact output limits, cancellation, empty-output recovery, and optional Operation evidence aligned |
| Operation lifecycle | Canonical start/terminal receipts with monotonic durations and duplicate-terminal fencing | Python `AgentOperationController` | Model-task Operations are optional and cannot replace Run lifecycle authority |
| Planned execution | Explicit host Planner or optional `ModelWorkPlanner`, policy, semantic contracts, compiler, Core execution state, response validation, model-backed judge, and bounded explicit replanning | Planner, planning policy/ports, compiler, response validation, Run state | Reference model calls reuse managed Run authority; every direct, planned, revised, and judged result still passes through existing compiler/validation authority |
| Durable | Explicit admission, Long Task DAG/repository, leases, checkpoints, retries, pause/resume/cancel, recovery snapshots, continuation, orphan recovery | Explicit Python Durable capability | Phase 6 host-facing safety outcomes aligned; built-in storage remains process-local |
| Artifacts | Open/finalized/aborted lifecycle, ordered batches, idempotent receipts, coverage, validation, access, writer claims, recovery candidates, maintenance | Python Artifact lifecycle and durable store ports | Phase 7 host-facing safety outcomes aligned; ownership stays opaque and built-in storage remains process-local |
| Delegation | Root Run-scoped batches, policy bounds, idempotent replay, isolated dynamic execution, read-tool filtering, cancellation, aggregation, lifecycle events | Python `DelegationCoordinator`, `DynamicDelegatedAgentExecutor`, and delegation ports | Phase 8 one-Run outcomes aligned; no child Run, recursive delegation, write authority, or second event stream |
| Observability | Content-free operational, stability, failure, recovery, performance, trend, and regression-gate projections | Python `purra.observability` | Phase 9 frozen evidence, stable codes, confidence, and redaction aligned; reports are read-only |
| Evaluation | Deterministic runtime regression and security red-team suites | Python `purra.evaluation` | Phase 9 public-boundary scenarios aligned |
| Adapter verification | Public lifecycle probes plus one process-local adapter composition | Python `purra.testing` and in-memory adapters | Phase 9 public-port probes complete; production implementations remain host-owned |
| Packaging | ESM JavaScript, declarations, public metadata, and four installer smokes | Independent Python wheel/sdist gate | Phase 12C deterministic package gates complete; credentialed completion and streaming remain future host-project work outside this npm release and are required before stable parity claims |

Shared behavior does not require identical directories, symbols, function
signatures, or implementation details. Cross-language fixtures are added only
when both implementations consume the same wire representation.

Phase 12C re-audited this matrix by host behavior. No unapproved `Partial` or
`Missing` framework capability remains in the alpha matrix. The clean installed
consumer exercises Reactive, Planned reference helpers, managed context model
tasks, Operation evidence, Durable dispatch, Artifacts, delegation, recovery,
observability, and declarations through the package root. Provider SDKs and
production persistence are intentional host-owned boundaries, not placeholder
implementations. Real-Provider evidence is still `NOT RUN`, so this result does
not establish stable cross-language parity.

## Current source tree

```text
src/
  index.ts
  artifacts/
    access.ts
    contracts.ts
    lifecycle.ts
    repository.ts
    types.ts
  core/
    agent.ts
  context/
    budget.ts
    coordinator.ts
    types.ts
  durable/
    contracts.ts
    dispatcher.ts
    orphan.ts
    recovery.ts
    repository.ts
    types.ts
  delegation/
    coordinator.ts
    executor.ts
    policy.ts
    repository.ts
    tool.ts
    types.ts
  evaluation/
    index.ts
  extensions/
    model-tasks.ts
    planning.ts
  model/
    types.ts
    stream.ts
    validation.ts
  observability/
    observation.ts
    reports.ts
    trend.ts
    types.ts
  operations/
    index.ts
  output/
    publisher.ts
    types.ts
  planning/
    compiler.ts
    coordinator.ts
    policies.ts
    response-validation.ts
    state.ts
    types.ts
  recovery/
    index.ts
  run/
    session.ts
    store.ts
    types.ts
  tools/
    types.ts
    catalog.ts
    schema.ts
  shared/
    errors.ts
    fingerprint.ts
  testing/
    adapters.ts
    conformance.ts
```

| Path | Owns | Must not own |
| --- | --- | --- |
| `index.ts` | The single npm public entry point | Runtime logic |
| `artifacts/` | Recoverable output lifecycle, optimistic revisions, ordered batches, coverage, access policy, exclusive write claims, and bounded maintenance | Run/Long Task completion, product schemas, production storage |
| `core/` | Public Agent composition, Reactive sequencing, and selection of transient or submitted execution | Provider SDKs, database drivers, product policy |
| `context/` | Provider-neutral budgets, claims, retrieval strategies, request projections, structural trimming, compression validation, and evidence provenance | Domain retrieval semantics, Prompt diagnostics, canonical conversation mutation |
| `extensions/` | Managed, tool-free model tasks plus optional reference Planner/Judge composition around host-owned policies | Provider SDKs, product prompts/policy, a second Agent or planning runtime |
| `model/` | Provider-facing types, stream aggregation, and validation of untrusted model responses | Agent run policy, tool execution |
| `tools/` | Host tool definitions, Schema inspection, whole-batch admission, policy/approval/scope/idempotency/effect enforcement, sanitized results | Model-round policy, product permissions |
| `run/` | Run snapshots, invocation receipts, budgets, cancellation, terminal authority, repository port, and in-memory repository | Provider calls, domain Run meanings, production database adapters |
| `planning/` | Semantic contracts, host Planner/policy ports, validation/lowering, current-transition authority, response gates, explicit bounded revision | Task admission, tool handlers, Provider SDKs, Durable recovery, product planning prompts |
| `recovery/` | Stable recovery causes/actions/reasons, immutable policy, and one request-scoped decision ledger | Provider SDK retries, tool execution, product fallbacks, durable task retry policy |
| `durable/` | Admission validation, immutable dispatch receipts, task/unit DAG authority, lease fencing, checkpoints, recovery proofs, continuation, and orphan coordination | Product partitioning/merge policy, production storage, Provider SDKs |
| `delegation/` | Root Run-scoped definitions, policy bounds, lifecycle persistence, concurrency, cancellation, isolated execution, read-tool filtering, and result aggregation | Child Runs, write authority, recursive delegation, product Agent catalogs, production storage |
| `observability/` | Content-free read models over canonical evidence, stable failure/recovery codes, performance, stability, trend, and gates | Runtime transitions, prompts, arguments, results, reasoning, generated content |
| `operations/` | Persisted start/terminal receipts and monotonic timing for model, tool, validation, compaction, and delegation work | Run terminal state, product progress semantics, generated content |
| `evaluation/` | Deterministic regression comparisons and security boundary checks | Production monitoring, mutable runtime policy, Provider benchmarks |
| `testing/` | Dependency-free public-port probes and the process-local reference adapter composition | Product fixtures, Provider SDKs, production persistence |
| `shared/` | Coded errors and stable JSON fingerprinting used by more than one feature | Feature-specific helpers or future abstractions |

## Dependency direction

```text
index
  ├─> artifacts ─────────> model, shared
  ├─> observability
  ├─> evaluation ─────────> observability, tools
  ├─> testing ────────────> public ports and in-memory adapters
  └─> core
       ├─> model ────────> shared
       ├─> extensions ───> model, operations, planning, recovery, run, shared
       ├─> context ──────> model, shared
       ├─> tools ────────> model, shared
       ├─> planning ─────> context, model, tools, shared
       ├─> durable ──────> planning, run, model, shared
       ├─> delegation ───> context, model, run, tools, shared
       ├─> run ──────────> model, output, shared
       └─> output ───────> model
```

- `artifacts/`, `context/`, `delegation/`, `durable/`, `evaluation/`, `model/`, `observability/`, `planning/`, `testing/`, `tools/`, `run/`, `output/`, and `shared/` never import `core/`.
- `model/` never imports tool handlers; it sees only Provider-facing tool
  specifications.
- Host callbacks are invoked only through explicit public contracts.
- Package subpaths are not public. Consumers import from `@lybrands/purra`.

## Future directories added only with a caller

These are design slots, not folders to create now:

| Directory | Add when |
| --- | --- |
| `adapters/` | A real Provider or persistence implementation ships with the package |

Provider adapters and production persistence are not implied by this layout.
Each requires its own host use case and acceptance test.
