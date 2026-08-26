# Changelog

## 0.4.0 - 2026-08-25

### Recursive Child Agents

- Add stable `AgentNode` identity and immutable `AgentRun` chains with bounded
  recursive spawn, structured join, context-version continuation, subtree
  cancellation, and capability narrowing.
- Route Python and TypeScript Child Runs through their normal Agent runtime;
  `delegateToAgents` is now a create-and-wait facade over canonical Child Runs
  when Agent tree execution is enabled.
- Add Root-scoped atomic model/output budgets and one attributed canonical
  journal with strictly increasing `root_sequence` values.
- Add lease epochs, heartbeat renewal, stale-writer fencing for commands,
  budgets, output, checkpoints, and terminal commits, plus idempotent recovery
  of committed Child Runs that lost their worker before execution started.
  Root recovery rebinds execution inputs and scans the complete descendant set.
- Add Agent Preset snapshot v5 and shared Python/TypeScript Agent tree fixtures,
  runtime tests, and installed-package recursion/parallelism/continuation smoke
  coverage.
- Reject Root completion before public final output while any descendant remains
  non-terminal, and expose join cancellation as `child_run_join_canceled` in
  both runtimes.

### Provider output repair

- Preserve stable Core failure codes such as `runtime_budget_exceeded` through
  Provider stream handling instead of rewriting them as generic stream errors.
- Keep PUBLIC Provider output bounded by the existing 25 ms batching latency,
  while allowing PRIVATE background output to batch for up to 250 ms.
- Raise the default Provider-output journal budget for new Runs from 1 MiB to
  8 MiB. Explicit host limits and limits restored from persisted snapshots are
  unchanged.
- Keep `provider.delta_batch/v1`, output ordering, visibility, atomic append,
  digest, and terminal-event behavior unchanged.

### Tool-capable output authorization

- Align TypeScript with Python by treating final content from a tool-capable
  model invocation as a private candidate, then running one bounded tool-free
  public-presentation invocation.
- Withhold candidate stream deltas and tool-round commentary, reject any forged
  tool call during presentation, and exclude the private candidate and guidance
  from the returned public message history.
- Match Python validated-result semantics for canonical Agent Trees: Root and
  Child Runs commit their validated result without a presentation invocation.
  Normal Agent presentation remains metered through existing attempt, token,
  deadline, and output budgets.

### Provider liveness and sustained streams

- Replace the 120-second Provider invocation default with a non-renewable
  300-second resource fuse selected from local DeepSeek V4 Flash samples.
- Add opt-in Transport and Provider-working stream activity evidence with
  separate 30-second activity-idle and 60-second progress-idle limits.
  Semantic-only streams and non-stream completions do not arm idle limits.
- Keep activity evidence outside semantic chunks, output journals, stream
  meters, token accounting, and `provider.delta_batch/v1`.
- Persist the resolved timeout policy in Agent Preset snapshot v4. Snapshots
  and durable continuations from v3 or older are no longer accepted.
- Add stable terminal `model_activity_deadline_exceeded` and
  `model_progress_deadline_exceeded` outcomes without automatic retry.

### Tool schema conformance

- Admit and recursively enforce JSON Schema `uniqueItems` in Python and
  TypeScript Core before scope checks, approvals, cache probes, or handlers.
  Provider-side schema validation remains advisory.
- Align the JSON-value equality used by `uniqueItems`, `enum`, and `const`:
  numeric representations compare by mathematical value, booleans stay
  distinct from numbers, arrays compare in order, and object property order is
  irrelevant.
- Python hosts may observe corrected `enum` and `const` results where `1` and
  `1.0` were previously distinguished or nested booleans compared equal to
  numbers. This intentional conformance correction has no compatibility flag,
  wire migration, or persisted-state migration.

### Planner WorkPlan control

- Remove Python's default 1-8 WorkPlan step limit. Hosts may still configure
  an explicit `PlannerLimits.max_steps`; TypeScript retains its existing
  optional `PlanningConstraints.maxSteps` contract.
- Keep tool-step, model-round, output, timeout, approval, and compiled-plan
  authority limits unchanged.
- Reject duplicate normalized step ids and repair revised plans that reuse
  completed step ids instead of silently renaming or dropping them.
- Ask model Planners for the smallest non-redundant user-visible semantic plan
  and record WorkPlan/ExecutionPlan size and repair counts in planning traces.

## 0.3.0 - 2026-08-25

### TypeScript/npm

- Added absolute Run, invocation, and Long Task deadlines with bounded
  cancellation and reason-specific failure codes.
- Added repository-authoritative attempt, input, output, reasoning, output-byte,
  and output-event budgets. Missing Provider usage fails closed when a finite
  token budget is configured.
- Added atomic `RunRepository.appendBatch()`, bounded Provider-delta
  coalescing, and incremental stream limits before accumulation.
- Added lease epochs, renewal heartbeats, and stale-writer fencing to every
  claimed Long Task mutation.
- Added strict full-document Planner JSON parsing and Agent Preset snapshot
  schema version 3.

#### Breaking changes

- `RunBudgetOptions.maxTotalTokens` is replaced by `maxInputTokens`,
  `maxOutputTokens`, and `maxReasoningTokens`; `RunUsage.knownTokens` is
  replaced by separate counters plus `unreportedUsageAttempts`.
- `LongTaskCreateCommand.deadlineAt` and `LongTaskRecord.deadlineAt` are
  replaced by epoch-millisecond `deadlineAtMs`; Long Task budgets now use
  `LongTaskBudgetLimits`.
- Host `RunRepository` adapters must implement atomic `appendBatch()`.
- Agent Preset snapshot schema version 2 is not resumable.

### Python

- Added canonical attempt reservation and usage settlement before Provider
  calls, persisted Run and Long Task budgets, and fail-closed handling of
  missing usage.
- Added absolute Run, invocation, and Long Task deadlines with bounded
  cancellation and stable failure attribution.
- Added Long Task lease epochs, renewal heartbeats, same-worker ABA fencing,
  deadline expiry, and budget-aware claim authority.
- Added atomic output batch append, bounded Provider-delta coalescing,
  incremental stream limits, and snapshot/wire protocol version 3.
- Planner and control-plane JSON now require one complete strict document;
  surrounding prose, duplicate keys, and non-finite numbers are rejected.

#### Breaking changes

- Host Run repositories must persist `RunCreateParams.runtime_limits` and
  `deadline_at_ms`, and implement idempotent `reserve_model_attempt()` and
  `settle_model_attempt()`.
- Host output repositories must implement atomic `append_batch()`.
- Every claimed Long Task mutation requires `lease_epoch`; repositories must
  also implement `renew_unit_lease()` and `expire_deadline()`.
- Agent Preset snapshot schema version 2 is not resumable.

## npm purra 0.1.0-alpha.0 - 2026-08-25

- Added the TypeScript-native Reactive, Planned, Durable, Artifact, delegation,
  observability, evaluation, and public adapter-conformance capabilities.
- Added one ESM package with declarations and clean-consumer installation gates
  for npm, pnpm, Yarn, and Bun.
- Added one bounded recovery authority for interrupted Provider streams,
  malformed or unauthorized tool calls, invalid tool input, failed-step
  replanning, empty output, and response repair. Submitted Runs persist
  content-free recovery decisions before the selected action.
- Added a TypeScript-native canonical Operation lifecycle controller with
  persist-before-transition start/terminal receipts and monotonic duration.
- Added a public managed model-task runner and per-execution context/compaction
  factories; submitted extension calls reuse the same Run receipts and budgets.
- Added optional TypeScript-native model Planner and response-judge helpers;
  host Planner/Judge ports remain supported and Reactive stays the default.
- Re-audited the documented alpha capability matrix and refreshed one exact
  packed candidate through npm, pnpm, Yarn, Bun, and a strict installed-package
  consumer covering the managed extension composition path.
- Production persistence and Provider SDK adapters remain host-owned. Real
  completion and streaming evidence is future host-project work outside this
  npm release and remains required before stable parity claims.

## 0.2.0 - 2026-08-22

- Added fail-closed Agent composition snapshot schema version 2 and explicit
  `AgentComponentBinding` declarations for opaque host behavior.
- Moved delegation policy into the resolved Preset composition and included
  the effective delegation Tool contract in durable snapshots.
- Reject version-1 or incomplete durable snapshots with
  `agent_preset_snapshot_unsupported` instead of guessing an upgrade.
- Recursively validate the documented JSON Schema subset during Tool Catalog
  assembly; unknown types and unsupported assertion keywords now fail closed.
- Replaced accidental facade exports with explicit `__all__` declarations and
  added public evaluation, observability, security red-team, coverage, and
  multi-version CI gates.

### Breaking changes

- Opaque context, compaction, execution-state, planning, task-admission, and
  durable-dispatch components now require an `AgentComponentBinding`.
- Delegation is disabled when `delegation_policy` is `None`; selecting a policy
  requires delegation repository, idempotency, and canonical output
  infrastructure.
- Durable snapshots written before schema version 2 cannot be resumed.
- Durable continuation now requires `AgentCore(preset=...)`; loose composition
  remains available for non-continuation Runs only.

## 0.1.1 - 2026-08-22

- Allow hosts to validate normalized Planner results inside the repair loop.
- Expose the active Run id to durable unit executors during continuation.

## 0.1.0 - 2026-08-21

- Initial public release.
