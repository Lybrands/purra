# Changelog

## @lybrands/purra 0.1.0-alpha.0 - 2026-08-25

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
