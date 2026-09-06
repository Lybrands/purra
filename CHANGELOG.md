# Changelog

## Unreleased — 1.0.0 development

- Add versioned strict object output contracts, shared JSON identity, and Run-bound
  structured model tasks with local/native-required modes and explicit bounded repairs.
  OpenAI Responses/Chat and Anthropic mappings use conservative schema dialects;
  selected live model support must be verified separately.
- Add optional host-owned MCP read-only tools with bounded discovery, immutable
  catalog identity, schema/result validation, and fail-closed catalog changes.
- Add explicit bounded concurrency for wholly safe read batches; preserve ordered
  results, actual event timing, cancellation and per-dispatch authorization.
- Add actual integration-check coverage reports and read-only recovery inspection.
  Unknown usage, permissions and effects remain unknown; diagnosis grants no
  execution authority. Include public examples and external package consumers.

## 0.5.0 — 2026-09-06

- Preserve transport-activity events in the opt-in Provider timing sampler
  without treating them as content or progress. Add SQLite multiprocess,
  interrupted-transaction, tool-receipt reconciliation and live-Provider
  verification scripts using temporary synthetic data.
- Validate Python SQLite event bodies against their stored Run/Root identities
  and sequence columns on replay, deferred history reads, pagination and full
  restoration. Reject inconsistent rows before returning or caching them; failed
  replay, settlement and lease acquisition leave persisted state unchanged.
- Use covering SQLite indexes for Root/Run sequence validation and Root headers
  in both SDKs. Avoid event-body table reads and repeated Root-wide child joins,
  retaining count and sequence checks. Existing v3 databases create the indexes
  on opening without rewriting snapshots or events. Execution-write benchmarks
  optionally report journal, state and remaining transaction costs separately.
- Defer Python and TypeScript SQLite output history reads during Run-scoped writes. Validate
  sequence counts in SQL, replay keys through indexes, and buffer new
  events; planning and terminal-state rules still read original evidence when
  needed. Preserve atomic commits, sibling budgets and full lease-recovery
  validation. TypeScript uses persisted counts plus pending events for sequences
  and indexes Root-local keys. Full exports retain complete journals; incremental
  exports include an explicit sequence offset. Storage remains v3.
- Restore only the active Root tree's SQLite output journal for Run-scoped
  execution operations. Preserve unrelated checkpoints, event counts and shared
  child budgets. Python indexes cross-Root event keys and resolves stream lease
  fencing through the owning Run; TypeScript rejects access to unloaded Roots.
  Public transactions and lease acquisition retain full-scope validation.
- Avoid restoring SQLite output history for tool receipts, lease maintenance,
  and Agent tree/Artifact/Long Task repository operations. TypeScript also skips
  unrelated repository hydration. Keep complete state validation on the loaded
  Root tree, lease acquisition and public transactions; storage remains v3.
- Store SQLite canonical output events as incremental rows with Run and Root
  sequence indexes. Event pages and subscription polls use read-only queries
  without loading execution snapshots. Journal additions and execution state
  commit atomically; general repository operations still hydrate scope state.
  SQLite storage v3 rejects v1/v2 snapshots without automatic migration.
- Make durable failure settlement terminal when no concrete recovery action
  remains. Exhausted retries, invalid model output, and systemic protocol
  failures now fail the Long Task and Root Run instead of being mislabeled as
  recoverable pauses. Unknown or already-committed effects without a checkpoint
  also fail without replay; pauses remain reserved for explicit interruption
  recovery rather than error settlement.
- Remove the Python and TypeScript same-Run delegation stack, including its
  repository, coordinator, dynamic executor, lifecycle events, adapters,
  conformance fixture, public exports, and `AgentOptions.delegation` entry.
  Agent Tree is now the sole Child Agent authority through `AgentTreePolicy`;
  `delegateToAgents` accepts `children`, and newly executable Preset snapshots
  are v5 only.
- Added optional Python/TypeScript components: `purra-openai` (Responses and Chat
  Completions, official SDKs Python 3.7.0 / TypeScript 7.9.0), `purra-anthropic`
  (Messages, signed thinking continuation, official SDKs Python 1.3.0 / TypeScript
  0.123.0), `purra-compaction` (one managed semantic
  summary call), `purra-sqlite` (transactional Run/output/operations, budgets,
  checkpoints, leases, tool receipts, Agent trees, Artifacts and Long Tasks), and
  `purra-interaction` (structured questions, persisted user answers and checkpoint
  resume). Native interaction supports Auto/Reactive/Planned and nested Agent trees,
  preserving Run identity, budget, deadline, plan progress and revision state. Waiting
  children release leases; recovery resolves their canonical Child Run results
  before parent execution. Cancellation closes the complete Root scope. A separate callback API supports host-owned
  pre-execution handoff. SQLite combines indexed output journals with bounded
  project execution snapshots.
- Added `MemoryWorkflow` to `purra-mem0`, composing pending extraction, semantic
  review and explicit host-authorized resolution through the existing journal.
- Added private Provider continuation data to messages, terminal stream chunks
  and execution checkpoints. It is included in context budgeting and excluded
  from TypeScript public message projections.

### Planner streaming and public progress

- Replace policy and business-text activation with the breaking three-mode
  contract `auto | reactive | planned`. Auto is the default and uses the first
  ordinary Agent invocation. Before business execution, private sole-call
  `request_plan` can promote the Run. After a committed business tool, private
  `request_remaining_plan` can plan only unfinished work from the checkpoint.
  A planning-required tool promotes before its effect. No separate classifier
  call or legacy alias is retained. Explicit Planned without a Planner fails closed.
- Remove `ReactivePlanningPolicy`, `ToolPlanningPolicy` and policy-owned
  `should_plan` / `shouldPlan` activation. Optional PlanningPolicy objects now
  constrain an activated planning phase, including Auto promotion; no legacy
  alias is retained.
- Use bounded `purra.planning-stream/v1` records in the same managed Provider
  invocation for initial planning, dynamic revision and finite repair in both SDKs.
  Non-streaming Gateways fail explicitly; no extra explanatory model call.
- Persist provider-origin `planning.progress` with exact raw-byte provenance,
  Run/phase/revision/attempt identity, deduplication, replay and existing budgets.
  Keep private plans and reasoning private; publish admitted plan summaries only.
- Core owns `planning` operation lifecycle through validation, compilation and
  admission. Link model operations with `parentOperationId`, preserve Host
  observers, fence late output and close unfinished operations on Run termination.
- Record private first-activity/public-progress/final-plan/validation timings and
  optional typed Adapter HTTP evidence. Do not infer HTTP timing or speedups.
- Add shared protocol/lifecycle fixtures, cancellation/failure tests, public
  examples and independent installed-package checks. No downstream activation or
  real-Provider performance verification is implied.
- Accept one complete final `plan` at a normally terminated Provider EOF without
  requiring trailing LF. Unterminated progress, partial JSON and abnormal stream
  termination still fail closed; Python and TypeScript share the same fixture.

### Native Agent public progress

- Add opt-in Provider-authored progress on ordinary Agent streams, independent
  of answer text and reasoning. Persist its private source delta before the
  public `purra.agent-progress/v1` projection, using existing Run output budgets.
- Keep tool-equipped model output private until its role is known, then use the
  existing tool-free `final_public/live` presentation round so visible final
  responses stream from Provider chunks instead of one committed bulk event.
- Reject undeclared capability use, planning-stream mixing, rewritten content,
  source mismatches, cross-Run output and late terminal writes. Providers without
  this capability emit no native Agent progress.

### Optional Mem0 integration

- Add bounded administration filters and pagination, host metadata and reasons,
  explicit record selection/context assembly, and versioned relations between
  records. These operations reuse the component's scope and lifecycle checks.
- Added opt-in Python/TypeScript Chinese evaluation scripts and nine shared
  synthetic tasks, with a no-memory baseline, strict evidence scoring, bounded
  real-provider transports and metadata-only interrupted-trial reports. SDK smoke
  checks exercise the evaluator with substitutes; real quality is still unverified.
- Added opt-in bounded semantic review of pending candidates through the managed
  model/Run bridge. Strict classifications produce durable advice without changing
  memory; host-authorized proposals automatically carry a review link and require
  full-snapshot revalidation at execution. Budgets, cancellation, crash abandonment
  and replay reuse the existing operation journal. Add single-candidate independent
  resolution; uncertain or ambiguous classifications do not produce a proposal.
- Added explicit duplicate/supersession/conflict resolution with expected versions,
  atomic journal visibility and durable decision receipts in both languages.
  Originals remain separate in Mem0; reads honor effective journal states and
  old evidence invalidates. No implicit approval or generated text union.
- Added durable source revision/whole-ID withdrawal, including future-revision
  denial, blocked reingestion and late-write filtering. Explicit full-text
  correction can use an accepted new source; audit history is retained.
- Revalidate existing Core memory evidence for version, store/scope, lifecycle,
  expiry and source availability. Fresh memory context uses this guard; host
  checkpoint recovery still requires explicit wiring. Search uses bounded
  overfetch to filter revoked vectors without unbounded retries.
- Added opt-in managed OSS providers in both languages: durable pre-call quotas,
  exact output-limit acknowledgments, reported/unknown usage receipts, sticky
  SDK fallback failures, and cancellation that blocks subsequent provider calls.
  Run-bound LLM calls reuse the existing model-task runner; Embedding remains a
  separate accounted capability. Raw SDK mode still reports unknown usage.

- Add separately packaged Python and TypeScript `purra-mem0` components, pinned
  to the tested OSS SDK versions without changing Core's dependencies.
- Add scoped CRUD/history, pending extraction and explicit activation, source
  revisions, expiry filtering, version checks, durable write fencing,
  idempotency and read-back/reconciliation of uncertain SDK writes.
- Reuse RetrieverTool, context allocation and evidence receipts. Keep inferred
  candidates out of recall and unknown SDK usage out of zero-cost accounting.
- Add shared fixtures, failure tests and real-SDK/local-store smoke tests with
  deterministic Providers. Full erasure, deployment billing validation and real
  semantic quality evaluation remain unfinished production gates.

### Retrieval contracts

- Add public Python and TypeScript `Retriever`, `RetrievalRequest`,
  `RetrievalHit`, `RetrievalError`, and `RetrieverTool` contracts.
- Adapt each Retriever through the existing `ToolRegistration` or
  `ToolDefinition` path without adding another Catalog or execution system.
- Keep model-owned input limited to `query`; bind result count, Run identity,
  and authorized scope from the host, and mark all model-visible hits as
  untrusted data.
- Fail closed on invalid or oversized results, preserve source/id/version
  evidence, propagate cancellation, and expose stable sanitized retrieval
  error codes.
- Leave databases, chunking, Embedding, indexing, hybrid search, reranking,
  credentials, and index synchronization to applications or optional
  integration packages.

### TypeScript context recovery

- Restore resolved ContextProvider blocks and their source receipts at Reactive
  Child Run `model_ready` checkpoints without requerying providers or replaying
  completed retrieval tools.
- Reuse normal context projection after recovery, recompute input reserves from
  the rebound model/tools/output limit, and retain spent compaction counts.
  Oversized protected input or invalid compression fails before model invocation.
- Upgrade TypeScript `AgentExecutionCheckpoint` to schema v2 with required
  `context` (`PreparedContextSnapshot` or explicit `null`). Reject v1 and incomplete
  snapshots without migration; Python's unchanged checkpoint format remains v1.

### Planner evidence and summary lifecycle

- Feed resolved single-pass context and recent tool observations into the built-in
  TypeScript Planner. Bound observations to the latest eight, with 4,000-character
  Unicode excerpts, explicit truncation, and Run-scoped full-message references.
- Keep retrieved planning context and observations outside the privileged planning
  contract; preserve complete tool results in canonical messages.
- Retain validated TypeScript compression summaries and provenance across rounds
  and v2 checkpoints. Hooks receive `previousSummary`; omitted `summary` retains,
  an object replaces, and `null` clears. Failed projections keep the old summary.
- Reject private model tasks whose estimated input plus output reserve exceeds
  the model window before invoking the Provider.

### Unambiguous generation-token contracts

- Renamed the per-model-invocation and cumulative Run limits to explicitly
  describe Provider generation, rather than visible output.
- Require public Run creation to state the cumulative generation budget explicitly;
  use `None`/`null` to deliberately select no finite cumulative token limit.
- Require Provider gateways to report the output limit they actually applied.
  Missing or mismatched acknowledgments and reported usage above that limit fail
  with `model_gateway_contract_violation` before output can be committed.
- Do not provide aliases or persisted-state migration for the removed names;
  0.5.0 hosts and snapshots must use the new contract.

## 0.4.1 - 2026-08-27

### Model configuration ownership

- Preserve the Run's exact `default`, `enabled`, or `disabled` reasoning mode
  across planning, execution, private model tasks, response judging,
  delegation, and continuation.
- Reject any internal model call whose reasoning mode differs from its
  immutable Run context before reaching a Provider.
- Stop private response validation from replacing caller model options with a
  framework-selected temperature and option subset.

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

- The former combined Run token setting is replaced by independent input,
  generation, and reasoning token budgets; `RunUsage.knownTokens` is
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
  host Planner/Judge ports remain supported.
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
