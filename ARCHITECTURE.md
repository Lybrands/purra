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

Auto execution is the default. Core adds the private, empty-argument
`request_plan` control to the first ordinary Agent invocation when a Planner and
tool calling are available. A direct answer completes without planning. An
ordinary business tool call executes Reactively and closes later upfront-plan
activation. Core then exposes only private `request_remaining_plan`, which can
plan unfinished work from the committed checkpoint without replaying prior
effects. Either phase control, or selecting a tool declared planning-required,
promotes the same Run into governed planning before the selected required effect.
There is no separate classifier invocation.

Without a Planner or tool calling, Auto does not advertise planning controls
and uses the ordinary model/tool loop. A planning-required tool still needs
Planner admission and cannot fall back to unplanned execution. Avoid assuming
that a direct answer always costs one invocation: the configured response
transaction may also require validation or a tool-free public presentation.
When tools are present, provisional response text remains private and the
tool-free presentation is the only public final stream; Core never promotes the
completed private buffer as one bulk final event.

Reactive execution is an explicit override that forbids Planner activation. A
planning-required tool then fails closed with `planning_required`.

Planned execution is an explicit override. A Planner produces a semantic `WorkPlan`; Core
validates and compiles it before it can grant step-scoped tool authority. The
plan describes intended work but is not itself runtime authority. Configuring a
Planner installs that capability. Python `PlanningMode.PLANNED` and TypeScript
`planningMode: "planned"` force it before the ordinary Agent invocation;
omitting the field selects Auto. Planning policies constrain an activated plan;
they do not classify requests or grant tools.

Configure the Python Planner through `ExecutionProfile.planner` in an
`AgentPreset`, or the direct `AgentCore(planner=...)` composition. TypeScript
uses `planning.planner` or `planning.plannerFactory`. A planning policy is
optional in both SDKs. Full-Run callers must explicitly select the cumulative
generation budget: Python `RuntimeLimits(max_run_generation_tokens=...)` and
TypeScript `submit(request, { budgets: { maxRunGenerationTokens: ... } })`.

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

`AgentTreePolicy` is the only Child Agent policy surface. Python Agent Tree
execution requires that policy inside an `AgentPreset`; `delegateToAgents`
accepts `children`, and every newly persisted Agent preset uses snapshot v5.
The former same-Run delegation repository, executor, events, and v4 snapshot
execution path do not exist in the runtime.

Reactive Child Runs persist a private `model_ready` checkpoint after a fully
committed tool round. A replacement executor may attach to the same canonical
Run and continue at the next model round only when that checkpoint, its lease,
and its Agent preset snapshot all match. An in-flight Provider stream, an
in-flight tool, and Planned execution remain fail-stop boundaries; PurrA does
not infer or replay their missing state.

## Planning output

Managed planning streams emit public `planning.delta` events for every nonempty
Provider content delta. Delivery does not wait for a newline, a valid JSON
record, or the final plan. The framework persists the source chunk and preview
event before publishing it. A blocked next chunk cannot delay the preceding
preview. This is a delivery contract, not a latency guarantee for a Provider
that buffers its own output.

Python subscribers use `handle.subscribe()`; TypeScript subscribers use
`handle.events()`. Select events with `kind == "planning.delta"` and append
`payload.textDelta` verbatim within the same invocation. Public payload fields:

| Field | Meaning |
| --- | --- |
| schemaVersion | `purra.planning-stream/v1` |
| operationId, revision, attempt | Bound planning operation and retry scope |
| sourceChunkIndex | Original framework chunk index; Python starts at 1, TypeScript at 0 |
| textDelta | Exact nonempty content fragment |

Run, stream and invocation identities are carried by the event envelope in
Python; TypeScript includes `invocationId` and `source: "provider"` in the
payload. Chunk indices may have gaps because reasoning-only, usage-only and
finish-only chunks do not produce previews. Use persisted event identity and
sequence for replay deduplication, not the text or contiguous indices.

The preview contains raw planning wire content, including JSON syntax, progress
records and plan records. Fragments may split an escape sequence or JSON token.
Invalid records and failed attempts remain visible as provisional previews.
This deliberately expands the previous public boundary: planning *content* is
previewable, even when final validation later rejects it. Reasoning deltas,
prompts and tool arguments are not projected by this event.

`planning.progress` remains the existing validated, complete progress-record
projection. It can overlap text already present in the raw preview; these are
two views of the same source. Formal plan validation, repair limits, execution
admission and cancellation remain unchanged. A preview never authorizes work.
Track invocation and attempt separately across repairs; do not concatenate a
failed attempt into its replacement.

Python output policies may optionally implement
`async authorize_planning_delta(spec, chunk)`: return the unchanged chunk to
allow a preview or `None` to suppress it. Absent this hook, previews are allowed.
TypeScript output policies can return `null` for a `planning.delta` event.
Rewriting preview content or provenance is rejected. Suppressing this view alone
does not suppress the separate `planning.progress` view.

Memory and SQLite repositories validate exact persisted source bytes, scope,
active invocation and operation, and source-key uniqueness. Each content chunk
requires immediate persistence, increasing event and storage overhead compared
with batching. Preview events consume existing output event/byte budgets; they
do not add model token usage. The existing first-public-progress timing metric
continues to describe complete progress records.


These events belong to the managed Run/output pipeline. Standalone Planner
methods still return a complete validated plan. Custom repositories/processors
must honor the same projection contract. Deterministic gated-chunk tests verify
delivery and replay; they do not establish live Provider or downstream behavior.

## Read-only tool concurrency

Python `ToolExecutionLimits(max_concurrency=2)` and TypeScript
`Agent({ toolLimits: { maxConcurrency: 2 }, ... })` set a **per-batch** in-flight
limit. The default is 1. Registration `concurrency_safe` / `concurrencySafe`
defaults to false and requires both read mode and read risk. Parallel dispatch
requires more than one call and every selected tool to carry that declaration.
Other batches retain their existing serial/policy behavior, including Python's
restrictions on multi-call write batches.

Core validates all arguments and checks every scope in input order before any
parallel handler starts. It checks current scope again at dispatch, uses the
Run cancellation signal, and retains existing canonical operation/event lease
fences. An unbound executor cannot invent a Run lease or authorization proof;
the host must supply those boundaries when using persistence.

Claims are made in input order, up to the configured limit. The first observed
failure closes the dispatch gate. Already-running reads are joined and their
successful results retained; queued calls receive `tool_batch_aborted` with no
write effect. Completion events reflect actual completion order, while returned
results and accumulated context evidence use input order. A skipped call has
no handler execution. A started operation can fail authorization before network
I/O; event start is not proof of an external request.

Cancellation closes the gate and joins active work. Python returns a canceled
batch; TypeScript retains its `AgentCanceledError` convention. Host callbacks
must cooperate with cancellation and finish cleanup: parallel TypeScript waits
for the underlying handler/scope promise to settle, even if cancellation wins
the local race. No SDK can forcibly stop arbitrary host JavaScript or guarantee
remote MCP termination. Observer, canonical persistence and recognized lease
failures propagate; they are not converted into successful tool data. Fatal
failures cancel/drain siblings and prevent late Core event emission after return.

The declaration asserts that the entire host implementation is safe to overlap,
including shared state, clients, resource access and scope callbacks. Read-only
behavior alone does not prove that. The limit does not provide global Root,
process, data-source, tenant or cross-worker scheduling.

Preset-bound configuration identity includes tool argument-contract identity,
concurrency declarations and effective tool limits. Changing them changes the
configuration fingerprint; it does not silently rebind a checkpoint. Hosts
still version their scope and handler implementations through preset revisions.

The shared fixture and latch/barrier tests establish overlap, bounds, ordering,
queue shutdown, revocation, cancellation cleanup and failure propagation.
Official local MCP client/server tests also verify several requests on one
host-owned connection. They do not demonstrate live Provider speedup or reduce
model calls; normal Agent final presentation remains a separate Core phase.
See the runnable [Python example](examples/python/parallel_tools.py) and
[TypeScript example](typescript/examples/parallel-tools.ts).

## Integration reports and recovery inspection

`check_integration` / `checkIntegration` wrap existing conformance assertions. The
report schema is version 1. Component name and version are host-supplied public
labels; `declaredCapabilities` is a snapshot of declarations, not evidence.
Every capability/category pair is `passed`, `failed`, or `not_run`. A pass means
that the supplied assertion returned successfully. Missing checks stay `not_run`.
A failed assertion has a stable `<capability>_nonconforming` code; its exception,
Provider payload and tool output are never copied into the report.

The default enabled category is `deterministic`. `installed_artifact`,
`real_provider_mcp` and `downstream` require explicit `enabled_categories` /
`enabledCategories`. A category label does not create isolation or prove a live
service was used: the host owns truthful classification and an isolated fixture.
Conformance assertions may call models/tools or mutate their fixture stores. In
particular Python's gateway assertion executes both stream and complete; TS's
executes one invocation. Never supply a production adapter under a fixture label.
The wrapper adds no calls, retries or LLM judge. It awaits each check in report
order; cancellation propagates. Use ordinary Python execution, since the existing
Python conformance helpers use assertions.

Runnable examples: `examples/python/integration_check.py` and
`typescript/examples/integration-check.ts`. They exercise existing store assertions
on disposable in-memory stores and then inspect a canonical Run. They use no live
Provider/MCP. Host checks can wrap the structured-output, MCP and concurrency
assertions from the corresponding public APIs; declaring these capabilities alone
does not pass any check. Labels and probe registrations must be trusted host data.

### Read-only diagnosis

Core exports `inspect_recovery(repository, run_id)` /
`inspectRecovery(repository, runId)`. These call only `get` on the existing Run
repository. No new required port method is introduced. The generic reader knows
only status/checkpoint presence; other evidence stays unknown. A missing Run keeps
the repository's normal missing-Run error, rather than inventing a recovery state.

`SqliteAgentAdapters.inspect_recovery(run_id, expected_preset=...)` /
`inspectRecovery(runId, {expectedPreset})` additionally read a single committed
transaction: checkpoint, attempts after the checkpoint, pending tool claims,
execution lease, cancellation, deadline and an optional complete effective preset
comparison. The expected preset is supplied by the host; matching it says nothing
about future configuration or current authorization. Without it configuration is
unknown. SQLite inspection does not claim, resume, reconcile, append events,
serialize state or run any model/tool. Opening the adapter still performs its
normal database initialization; inspection is read-only on an already open adapter.
TS currently restores the selected Root's journal to count post-checkpoint attempts;
its cost grows with that journal. Python uses stored attempt counters. Neither
adapter changes its storage schema for inspection.

Reports contain only enums, counts and fixed reason/action codes, with
`authority: "diagnosis_only"`. They omit Run/tool identifiers, message bodies,
credentials, checkpoint messages, raw errors and private reasoning. The pure
`build_recovery_inspection` / `buildRecoveryInspection` builder allows a custom host
to supply the same normalized observations. It does not verify host evidence.
Missing counts are null, never zero. Unknowns and known blockers are separate;
even an empty blocker list is not a resume permit. Permissions, usage completeness,
external effects outside the adapter and Agent Tree ownership remain unknown in
the supplied SQLite reader. The actual execution path must revalidate all of them,
as well as leases, configuration, budgets and checkpoint freshness.

Python tool claims are associated with a Run, so the count covers that Run's tracked
claims. TS idempotency keys are opaque and have no canonical Run binding: the count
covers the adapter's storage scope and adds `unattributed_tool_effect_unknown` to
`cautions`, not the current Run's `blockers`;
Run-specific effects stay unknown. Zero claims never proves all external effects
safe. Tool reconciliation can remove a pending claim but cannot remove the separate
`run_recovery_requires_reconciliation` blocker for post-checkpoint model attempts.
The inspection API provides no tool-ready cursor or general side-effect recovery.

`suggestedActions` contains advisory inspection, reconciliation and revalidation
steps. None executes work or authorizes a retry. Reports describe a past observation
and are not a second persistent source of Run truth. Existing
`evaluate_agent_run_recovery` and TS historical recovery reports continue to
summarize recorded decisions; this API observes current stored readiness evidence.

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

Full-Run execution uses `AgentCore.submit` / `Agent.submit`. Recovery of a
checkpointed Root uses `resume`; it requires durable execution ownership, the
persisted deadline and budget authority, and the original Agent composition.
Child execution is resumed through its Root scheduler. `continue_agent` /
`continueAgent` creates a subsequent Agent execution through the tree command
contract; it does not bypass checkpoint reconciliation for an interrupted Run.

Hosts handle recovery failures by `error.code`, catching both the `resume`
call and the returned handle's result. Python uses `ContractViolationError`;
TypeScript uses `AgentError`. The following rejection scenarios do not dispatch
a new model or tool call:

| Code | Condition |
| --- | --- |
| `child_run_resume_requires_scheduler` | Direct Root recovery was requested for a Child Run |
| `run_lease_required` | Durable execution ownership is not configured |
| `run_terminal` | The selected Run has already settled |
| `checkpoint_missing` | A running Run has no committed execution checkpoint |
| `agent_preset_mismatch` | The rebound Agent composition differs from the saved composition |
| `agent_execution_checkpoint_conflict` | The selected checkpoint differs from canonical state |
| `run_lease_conflict` | Execution ownership could not be acquired |
| `run_recovery_requires_reconciliation` | The durable adapter found an attempt after the last checkpoint |

Child routing and ownership availability are checked before canonical Root
state; terminal state takes precedence over a missing checkpoint. Subsequent
identity and lease checks may discover changes since the first read. Hosts must
not infer an exhaustive diagnosis from the first reported error, parse exception
messages, or retry an unknown external effect automatically. A custom durable
adapter supplies the same coded acquisition and reconciliation failures through
the execution ownership port. Other validation, budget and Provider errors keep
their capability-specific contracts.

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

`purra-openai` adapts OpenAI Responses and Chat Completions; `purra-anthropic`
adapts Anthropic Messages. Both are optional packages using the public model port.
They translate text, function tools, streaming, usage and private continuation
state. Applications select models, supply verified capabilities and credentials,
and handle other vendors' compatibility rules. Core owns execution and budgets;
the adapters disable SDK retries so each invocation is one HTTP attempt.

Optional components under `integrations/` are separate distributions, outside
the Core source trees and dependency guards. The first is
[`purra-mem0`](integrations/mem0/README.md), which depends only on public PurrA
contracts plus its SDK/storage stack. Its SQLite journal tracks uncertain
memory operations, visibility and optional provider budget reservations. Managed
LLM callbacks reuse the Run-injected model task runner; Embedding usage stays in
the component ledger. Canonical Run events and context evidence remain in the
existing Core/host persistence path. Core never imports it.
Source withdrawal is an independent journal transaction, so uncertain SDK writes
cannot prevent revocation or make late records readable. Existing Core evidence
can be revalidated against live versions, expiry and source policy; the host owns
source-event routing and checkpoint/pre-dispatch wiring. Neither withdrawal nor
logical deletion promises erasure of historical context or SDK audit text.
Explicit duplicate/supersession/conflict decisions verify a bounded set of SDK
records, then atomically change their journal views/versions and decision receipt.
SDK payloads remain unchanged; all reads use effective journal visibility rather
than treating SDK lifecycle metadata as current. Host policy chooses the semantic
decision; the component does not synthesize facts or fabricate merged provenance.
Optional semantic review reuses bounded Mem0 search and the managed model callback
to classify a pending candidate against active peers. It persists advice without
changing visibility. Derived proposals carry their review key; execution verifies
the whole saved snapshot, including peers excluded from the change, then checks
epoch/source/expiry again in the journal transaction. Host policy authorizes the
decision. Review shares the namespace writer fence and provider ledger, with no
second queue or Run journal; abandoned reviews are not replayed after recovery.

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

## Durable adapter state boundary

Core owns `StorageSession` and explicit versioned state schemas; SQLite manages
transactions and journal rows through that boundary. Python exposes it from
`purra.storage`; TypeScript exports it from `purra`. This is a version-pinned
adapter contract, separate from application ports and public output projections.
Python record identifiers do not depend on module paths. Repository state and
canonical output history commit atomically, while lazy history stays transaction-local.
SQLite storage v4 rejects other versions before database initialization writes.
It provides no legacy codec or automatic migration. SDK state schemas, execution
checkpoint versions, preset versions and package versions evolve independently;
Python and TypeScript snapshots are not interchangeable.

## Public compatibility

- Patch releases preserve documented public APIs and behavior. Minor releases
  add capabilities through new exports, optional parameters, or explicitly
  negotiated capabilities; they do not add mandatory methods to existing host ports.
- Public types and documented exports are the supported surface. Private module
  paths, internal state codecs, and generated SDK extension internals are not
  application APIs. A version-pinned storage adapter contract is documented separately.
- Existing event/error meanings are preserved. Where a contract explicitly allows
  unknown fields or codes, consumers must retain an unknown state rather than
  interpreting it as success or permission to resume. Closed enums are not
  implicitly extensible merely because they are represented as strings.
- A breaking public change requires a major version. Removing an existing data
  format or reinterpreting persisted state is not a harmless patch. Storage format
  numbers do not bypass application-visible compatibility commitments.
