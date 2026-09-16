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

Planning previews are persisted before publication. Each content chunk can be
observed before complete JSON arrives; only a validated plan grants execution authority.
See the [detailed contract](docs/planning-output.md).

## Read-only tool concurrency

Read batches may run concurrently only with explicit safety declarations and a
bounded limit. Authorization, cancellation and lease fences remain mandatory.
See the [detailed contract](docs/tool-concurrency.md).

## Integration reports and recovery inspection

Integration reports distinguish tested capabilities from unknowns. Recovery
inspection observes stored evidence without granting permission to resume.
See the [detailed contract](docs/integration-inspection.md).

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
