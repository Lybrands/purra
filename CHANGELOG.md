# Changelog

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

## 0.1.0 - 2026-08-21

- Initial public release.
