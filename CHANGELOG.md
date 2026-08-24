# Changelog

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
