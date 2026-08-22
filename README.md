# PurrA

[简体中文](README.zh-CN.md) | English

`purra` is an independently packaged, product-neutral Agent framework. It owns contracts,
planning/runtime policy, Run lifecycle, tool authorization, approvals, and
orchestration ports. It must remain independently importable with only the
Python standard library.

PurrA defaults to reactive execution: an Agent can run an ordinary model/tool
loop without a Planner, `TaskSpec`, task admission, `ExecutionRecipe`, durable
task repository, or domain context provider. Planning and durable execution
are explicit first-party capabilities rather than mandatory stages.

Hosts with bounded model-backed hooks use `purra.model_execution`. They declare
the model request, output policy, work-unit count, and reasoning preference;
PurrA alone resolves the provider allowance, constructs `ModelInvocation`, and
classifies the terminal reason. Missing terminal reasons and truncated output
fail closed and are never replayed.

Before every Provider request, PurrA durably opens the output stream and
records a private `stream.opened` receipt. The receipt binds the invocation and
stream identities to redacted call parameters, fingerprints of the exact
model-visible messages and tool schemas, and host-declared context provenance.
It never copies prompt bodies into the output journal; a receipt persistence
failure prevents the Provider call.

A context block declares that provenance through
`ContextBlock.host_metadata["context_evidence_receipts"]`. Each receipt uses
the generic `evidenceId`, `source`, `itemId`, optional `version`, and optional
host metadata fields; PurrA does not interpret product-specific source values.

## Public host contract

PurrA has no universal host object. A host composes existing public contracts:

| Host need | Public import surface | Ownership |
| --- | --- | --- |
| Compose and submit a Run | `purra.api` (`AgentCore`, `AgentPreset`, `AgentComponentBinding`, `AgentCoreRunOptions`, `PromptSection`) | PurrA owns execution once submitted. |
| Describe an input or opaque domain association | `purra.contracts` (`AgentRunRequest`, `RunBinding`, `ExecutionRecipe`) | The host maps product input and interprets its own association. |
| Implement runtime dependencies | `purra.ports` (model, context, tools, Run/output persistence, projectors) | The host provides concrete adapters. |
| Select optional capabilities | `purra.task_admission`, `purra.long_tasks`, `purra.artifacts`, `purra.delegation`, `purra.output` | The host chooses and configures them; PurrA enforces their contracts. |
| Verify an adapter | `purra.testing` | PurrA supplies dependency-free conformance probes. |

`purra.api` is the complete-Run entry point. Hosts must not import
`purra.engine`, `purra.runtime`, or other implementation modules to start a
Run. Product request mapping, transport, credentials, business queries, and
domain projection remain outside PurrA. `RunBinding`, `ExecutionRecipe`, and
`DomainEventProjector` carry host semantics opaquely; PurrA never interprets
their business fields.

### 0.2 compatibility boundary

For the `0.2.x` line, `purra.api` and the owning public modules listed in the
table above are the supported host contract. Compatible additions and fixes
may ship in a patch release; removing or changing an existing public contract
requires the next minor release while PurrA remains pre-1.0. `purra.engine`,
`purra.runtime`, and their implementation submodules are intentionally not
host compatibility surfaces. CI builds both wheel and sdist, installs the
wheel into a clean environment, and runs one Agent through public imports.

The supported top-level host modules for this line are `api`, `artifacts`,
`cancellation`, `context_budget`, `context_orchestration`,
`context_strategies`, `contracts`, `errors`, `evaluation`, `events`,
`evidence`, `json_values`, `long_tasks`, `model_call_parameters`,
`model_execution`, `model_invocation`, `model_protocol`, `normalization`,
`observability`, `orphan_recovery`, `output`, `ports`, `recovery`,
`run_control`, `stream_ownership`, `structured_output`, `task_admission`,
`testing`, and `tools`. Hosts import symbols exported by those module roots;
their implementation submodules are not compatibility surfaces unless this
README explicitly names one.

## Dependency direction

```text
host / domain / infrastructure
             ↓
        application services
             ↓
          purra
```

Core must not import a host transport, product domain, model SDK, database
driver, or concrete persistence adapter. The standalone conformance tests
exercise this dependency direction entirely through public PurrA contracts.

## Module boundaries

`runtime`, `engine`, `contracts`, and `ports` are responsibility packages.
Hosts use the public surfaces above and may use the narrow owning module for a
selected capability:

- Runtime orchestration is separate from model-round accumulation, tool-batch
  streaming, and buffered-response finalization.
- Engine orchestration is separate from options, context assembly, the
  optional planning capability, optional task orchestration, and the lower-level
  durable-execution protocol.
- `ContextStrategy` selects single-pass or staged retrieval independently
  from whether Planning is enabled.
- `ExecutionProfile` freezes the optional Planner, Planning Policy, context
  strategy, and task admission/dispatch selected by the host.
- Contracts expose stable message, planning, context, tool, and run families,
  with foundational enums in `enums.py`.
- The ports package is the stable umbrella for definitions owned by the model,
  context, planning, tools, persistence, and run-lifecycle modules.

Implementation packages remain private details rather than host compatibility
surfaces.

## Agent composition boundary

Core supports product hosts through three product-neutral contracts:
`RunBinding` persists an opaque aggregate/command association,
`ExecutionRecipe` validates a host-compiled mechanical DAG, and
`DomainEventProjector` lets a persistence adapter project a domain effect in
the same commit transaction. Core does not interpret a Binding, author a
product Recipe, or understand a Projector's business result.

Product request DTOs, request/event mapping, authoritative queries, and
business Run lifecycle hooks remain outside Core. The Core Run service and
transport mapper therefore contain only generic contracts and events.

`AgentPreset` is PurrA's complete, already-resolved Agent composition. It owns
a stable id and revision, ordered trusted `PromptSection` values, Context and
Tool ports, `ExecutionProfile`, compaction, runtime limits, and recovery policy.
It never owns product routing, request hydration, repositories, database
queries, provider credentials, or process cleanup.

`AgentCore(preset=...)` materializes the trusted prompt before a Run is
published. It also records an `AgentPresetSnapshot` schema version 2 in
`run.started`. Its fingerprint covers the Preset id/revision, prompt sections,
context and compaction bindings, execution profile, effective enabled Tool
schemas and authorization contracts, delegation policy, runtime limits, and
recovery policy. PurrA derives stable records only for its stateless built-ins
and immutable compaction settings. Every behavior-affecting host component or
factory must have an `AgentComponentBinding` with a stable id, revision, and
optional configuration digest; arbitrary object state and `repr()` output are
never fingerprint sources.
A resumed host can use
`AgentPreset.require_snapshot()` to fail closed rather than silently run old
history under a changed composition. Durable continuation automatically
restores the source Preset snapshot from the canonical Run journal and rejects
drift before starting another model, tool, dispatcher, or delegated invocation.
Version-1 or incomplete snapshots are rejected with
`agent_preset_snapshot_unsupported`; Core never guesses an upgrade from current
process state.

The explicit loose-composition `AgentCore(...)` form remains available for
ordinary Runs, but durable continuation requires a configured `AgentPreset` so
that version-2 composition authority can be recomputed and compared.

## Execution styles

The same Kernel supports three progressively constrained compositions:

- **Reactive** is the `AgentCore` default. It runs the model and authorized
  tools with `ContextStrategy.SINGLE_PASS`, without constructing a Planner or
  invoking staged planning/task context hooks.
- **Planned** explicitly injects both a `WorkPlanner` and a domain
  `PlanningPolicy`, or the public `AgentPlanner` and `ToolPlanningPolicy`.
  This activates `PlanningCapability`, which compiles and validates the plan
  before Core scopes runtime tools to the active transition. A host explicitly
  selects `ContextStrategy.STAGED` when planning and task retrieval are split.
- **Durable** adds `TaskAdmissionEvaluator` and `LongTaskDispatcher` to a
  planned composition. Only then does Core construct
  `TaskOrchestrationCapability`, which owns admission, durable handoff, and
  continuation while reusing the same lower-level durable DAG protocol. The
  host still owns the `ExecutionRecipe`.

Kernel owns non-bypassable model protocol, hard context budgeting, tool
execution safety, authorization, approval, Run/events, cancellation, and
recovery. Whether to plan, what to retrieve, whether to admit durable work,
and how to validate a business result belong to explicit capabilities and the
host composition.

Hosts normally place `ExecutionProfile` inside an `AgentPreset`. The explicit
individual arguments on `AgentCore` remain the low-level Kernel API for hosts
that deliberately assemble capabilities themselves; they cannot be mixed
with a Preset. Provider gateways, approval, persistence, output journals,
leases, and idempotency remain Kernel infrastructure rather than Preset data.

Core never infers or constructs a Planner from enabled tools, a lone
`planner=` argument, or a non-reactive Policy. Planned composition fails at
assembly time unless both Planner and Policy are present. `ToolPlanningPolicy`
is an explicit public choice, not a hidden default.

## Planning context and semantic boundary

Planning and runtime context share `ContextBundle.blocks` and the same hard
budgeting model. Each block produced by `build_planning_context()` retains its
name, content, and `untrusted` marker when projected into the bounded
`planningContext` payload. `ContextBundle.diagnostics` is observability-only
and is never model input; it cannot become an unbudgeted hidden context channel.

Trusted `PromptSection` values constrain both the execution model and the
Planner, preserving the Preset's identity, voice, and working principles during
planning. Ordinary host context and recent dialogue remain untrusted data.

The generic Planner authors only product-neutral `TaskSpec` semantics and
visible steps. It does not interpret Artifact ownership or other product
protocols. A host may describe allowed semantic target fields through
budgeted trusted planning context and validate them at its domain boundary;
Core owns only constraints, tool authority, dependencies, and execution safety.

Planner output and runtime authority are different contracts. A `WorkPlanner`
returns a semantic `WorkPlan` made of stateless `WorkStep` values. Core is the
only component allowed to compile it into an `ExecutionPlan`; host-inserted
prerequisites and private protocol nodes exist only there. `RunStateMachine`
rejects an uncompiled `WorkPlan`.

Runtime reads one derived `ExecutionTransition`, containing the active step and
its current/future tool grants. Todo events project only the original WorkStep
lineage: private execution nodes, runtime tool identifiers, and private
dependencies are not part of the user-visible plan. Delegation is an ordinary
host-authorized tool capability, not a separate plan executor. Durable
mechanical work remains the separate, host-authored `ExecutionRecipe`.

## Portable host conformance

`tests/test_standalone_agent_conformance.py` is the executable third-party host
example. It imports no product application, domain, infrastructure, or model
SDK code. It composes the official stdlib-only `InMemoryAgentAdapters`, then
drives `AgentCore.submit()` through Reactive,
Planned-with-staged-context, and Durable handoff compositions. The package
boundary gate forbids the example from falling back to private `_execute_run`.

```python
from purra.api import (
    AgentComponentBinding,
    AgentCore,
    AgentPreset,
    InMemoryAgentAdapters,
    PromptSection,
)
from purra.tools import InMemoryToolCatalog

adapters = InMemoryAgentAdapters()
agent = AgentCore(
    model_gateway=model_gateway,
    run_repository=adapters.runs,
    output_repository=adapters.outputs,
    output_publisher=adapters.publisher,
    preset=AgentPreset(
        id="portable",
        revision="1",
        tool_catalog=InMemoryToolCatalog(()),
        context_provider=context_provider,
        component_bindings={
            "contextProvider": AgentComponentBinding(
                "host.portable-context",
                "1",
            ),
        },
        prompt_sections=(PromptSection(
            name="identity",
            order=-100,
            text="Be concise, explicit, and calm.",
        ),),
    ),
)
```

The memory adapters preserve Run/output atomic commits, source-event
idempotency, ordered cursors, subscriber wakeups, Artifact writer exclusion,
and DurableTask checkpoints. They are process-local executable specifications:
they can exercise restart recovery semantics but do not survive an actual
process restart or provide multi-process coordination or production durability.

Any durable host adapter can run the same dependency-free contract probe:

```python
from purra.testing import assert_host_adapters_conform

await assert_host_adapters_conform(
    runs=run_repository,
    outputs=output_repository,
    publisher=output_publisher,
    session_id=test_session_id,
)
```

The probe checks Run creation, canonical event ordering, source-key
idempotency, stream terminal fences, post-commit wakeups, and atomic validated
terminal commits. A terminal Run commit is also the final output fence: the
repository atomically aborts every still-open stream and rejects any new stream
for that Run. The host remains responsible for creating and cleaning up its own
database or remote-store fixture.

Context hosts use the same module without adopting a PurrA-specific test
framework:

```python
from purra.testing import (
    assert_context_compression_hook_conforms,
    assert_context_provider_conforms,
)

await assert_context_provider_conforms(
    provider=context_provider,
    request=request,
    budget=budget,
    task_context=task_context,
)
await assert_context_compression_hook_conforms(
    hook=compression_hook,
    request=request,
)
```

The provider probe enforces allocated block budgets, safe host-context message
framing, and Single-pass/Staged retrieval contracts. The compression probe
uses a pressured conversation containing privileged instructions and a
complete tool exchange; Core rejects hooks that remove protected input,
break tool protocol, mutate immutable request scope, or remain over budget.

Task admission and durable execution use two additional probes:

```python
from purra.testing import (
    assert_long_task_dispatcher_conforms,
    assert_task_orchestration_conforms,
)

decision = await assert_task_orchestration_conforms(
    evaluator=task_admission,
    request=request,
    plan=plan,
    dispatcher=long_task_dispatcher,
)
await assert_long_task_dispatcher_conforms(
    dispatcher=long_task_dispatcher,
    request=request,
    plan=plan,
    admission=decision,
)
```

These checks cover all admission modes, exact durable step coverage,
idempotent task handoff, update-envelope restrictions, and continuation from
a typed `RunRecoverySnapshot` plus the original dispatch receipt without
dispatching a second task.

Storage adapters have direct durable-state probes as well:

```python
from purra.testing import (
    assert_artifact_store_conforms,
    assert_long_task_repository_conforms,
)

await assert_artifact_store_conforms(
    artifacts=artifact_repository,
    claims=artifact_claim_repository,
    maintenance=artifact_maintenance_repository,
)
await assert_long_task_repository_conforms(long_task_repository)
```

They check atomic creator bindings, immutable continuation links, exclusive
Artifact claims, CAS/idempotent batches, checkpoint release, restart recovery,
and completed-unit non-replay without importing a product domain.

Run control, delegation, and tool idempotency adapters use the same public
module:

```python
from purra.testing import (
    assert_delegation_repository_conforms,
    assert_execution_lease_store_conforms,
    assert_tool_idempotency_gateway_conforms,
)

await assert_execution_lease_store_conforms(run_control, create_run)
await assert_delegation_repository_conforms(delegations, create_run)
await assert_tool_idempotency_gateway_conforms(tool_idempotency, run_id)
```

Model and tool hosts have matching boundary probes:

```python
from purra.testing import (
    assert_model_gateway_conforms,
    assert_tool_execution_gateway_conforms,
)

await assert_model_gateway_conforms(
    gateway=model_gateway,
    messages=messages,
    invocation=invocation,
)
await assert_tool_execution_gateway_conforms(
    gateway=tool_gateway,
    request=side_effect_free_read_request,
)
```

The model probe checks typed streaming and completion results, one terminal
finish reason, complete JSON tool-call framing, and declared tool names. The
tool probe verifies fail-closed authorization, malformed JSON and unknown-tool
preflight, pre-start cancellation, ordered results, and terminal events. Use a
side-effect-free READ request: the probe executes the valid batch once.

Provider adapters normalize provider wire formats and error types. Core still
owns cancellation arbitration, bounded retry policy, output-limit termination,
and whether a completed tool call may execute; those rules are deliberately
not delegated back to each Provider SDK adapter.

### Identity and a second host

`tests/test_second_host_conformance.py` composes an incident-triage Agent with
no product application, domain, database, or Provider SDK imports.
Its stable identity is an ordered `PromptSection`; its runbook comes from a
`ContextProvider`; current service facts come from a read-only `ToolCatalog`;
and PurrA owns the model/tool loop and the final tool-free public response.

This is the style boundary: identity controls how the Agent reasons and speaks,
while domain context and tools control what it can know and do. PurrA preserves
the trusted instruction across tool rounds, but does not interpret or hard-code
the incident domain. No Persona DSL is required: typed sections are
materialized as provenance-marked system messages before Run publication.

```bash
python -m pytest tests/test_standalone_agent_conformance.py -q
python -m pytest tests/test_second_host_conformance.py -q
```

## Dynamic planning

The initial plan is a tentative roadmap, but successful execution follows that
compiled plan by default. Runtime invokes `DynamicWorkPlanner` only after a
recoverable failure or protocol/authorization drift, or when a host-owned tool
result explicitly returns `ToolPlanningDisposition.REPLAN` because it selected
a branch that changes the valid future path. Normal `PROGRESSED` and
`COMPLETED` batches do not spend another planner model call.

Completed, blocked, and failed execution steps remain immutable history. A
revision produces another `WorkPlan`, which Core validates and compiles before
the controller atomically replaces future authority and publishes the projected
`run.todos_updated` event.
The runtime keeps all request-scoped candidate tool schemas budgeted, but
exposes only the tool authorized by the latest current step. Replanning never
bypasses tool policy, approval, scope validation, or idempotency boundaries.
Authorization rejection and approval decline are not treated as recoverable
tool failures.

`ToolPlanningDisposition` is separate from `ToolStepDisposition`: the former
controls whether future planning authority must be revised, while the latter
only says whether the current step remains active. Both are host-owned tool
result fields and cannot be requested through model arguments.

A successful tool call is not automatically a completed planner step. Tool
adapters return `ToolStepDisposition.CONTINUE` when a bounded commit made
durable progress but the same operation still has work remaining. Core exposes
that as `ToolBatchOutcome.PROGRESSED`, keeps the current capability authorized,
and excludes the step from immutable completion history. Dependent tools become
eligible only after the adapter returns `COMPLETE`.

Runtime limits distinguish this monotonic partial progress from stalled
execution. Each `PROGRESSED` round may unlock one separately bounded progress
round through `RuntimeLimits.max_progress_rounds`; ordinary success, retries,
failures, and malformed output never earn that allowance. Large batch artifacts
therefore do not depend on a domain-specific fixed round count, while Core still
retains a hard total bound.

## Task admission and durable long tasks

The planner remains the only model-backed semantic decision point. It compiles
the user's goal into a `TaskSpec`; it does not estimate execution cost or
decide whether to create a long task. After constraint and authority checks,
but before installing the plan, `TaskOrchestrationCapability` calls an injected
`TaskAdmissionEvaluator`.
Application/domain code resolves that semantic target against authoritative
state and returns `inline`, `durable`, `clarify`, or `reject`. Core therefore
does not need product concepts, and the host does not need a second
intent-model request.

A durable admission must explicitly name every Planner step that the durable
executor will fulfil and include a host-compiled `ExecutionRecipe`. Every
recipe unit maps to one admitted Planner step, while several units may map to
the same step. Core aggregates those unit states and marks the Planner step
complete only after all of its units complete. This prevents an external
workflow from silently bypassing or falsely completing an unrelated step.

`long_tasks` defines product-neutral contracts for durable tasks, dependency
units, leases, checkpoints, retries, pause, resume, and cancellation. The Core
coordinator understands only the unit DAG and execution states. Domain code
owns partitioning, unit inputs, semantic validation, and final merge;
`RecipeLongTaskDispatcher` materializes that recipe as one Long Task, resolves
each unit through a host-owned executor registry, and passes the
completed dependency output references to downstream executors. This supports
static Map-Reduce DAGs without putting product concepts or artifact loading in
PurrA. Task reuse is scoped by namespace, owner, operation kind, session, and
idempotency key; a mismatched recipe fails closed. Pausing releases active
leases for checkpoint resume, completed units are never replayed, and a failed
task receives additional attempts only when the host explicitly authorizes
them. Concurrent coordinators wait on the same leased task instead of falsely
reporting failure.

`OrphanRecoveryCoordinator` owns the product-neutral restart boundary. It
classifies abandoned Runs, defers while a durable task still has a live lease,
claims an interrupted Run with compare-and-swap evidence, and pauses resumable
tasks against their scanned revisions before settlement. The host injects one
`OrphanRunSettlement` callback to atomically persist its lifecycle event and
output projection; PurrA neither queries product tables nor invents a product
terminal record.

`RunRepository` replaces plans only as complete `ExecutionPlan` values; a host
cannot persist a bare step list and silently lose `work_step_ids` or private
protocol nodes. A durable continuation takes its latest plan from a typed
`RunRecoverySnapshot` and its immutable admission from the original
`LongTaskDispatchReceipt`. The application may authenticate and select a
snapshot, but it cannot re-author those two contracts while constructing run
options. The source AgentPreset snapshot must also be reused.

## Context orchestration and compaction

PurrA owns context budgeting, compression timing, hook invocation, and
technical validation. It derives one budget from the model window,
output/runtime reserves, tool schemas, and domain context claims, then checks
pressure at planning boundaries and before every model call. At 85% pressure
Core invokes the configured `ContextCompressionHook`; the hook receives the
complete source view and hard token limits and owns every semantic choice.

Staged providers may additionally implement `TaskContextDemandProvider`.
Their ordinary demand funds only the lightweight planning manifest; after a
`TaskSpec` is compiled, Core resolves supplemental task-specific claims and
rebuilds the final budget before retrieving content. Optional large recovery
or retrieval projections therefore consume no partition for an unrelated
request, and no domain has to encode a fixed local context window.

`context_orchestration` does not own summary schemas, persistence, retention
rules, semantic targets, or fallback summaries. When no hook is installed it
uses only a structural recent-message trimmer (20 messages, then token fitting)
that preserves complete tool exchanges. With a hook installed that fallback
does not run. Core validates privileged instructions, the current user request,
tool-call/result continuity, and the final token count. The application owns
semantic summaries, durable summary state, and any configured fallback chain.

Canonical conversation turns remain the single source used by persistence and
the UI. Compression produces a request-scoped model projection; it never
rewrites that history. A host may store a derived summary artifact with
coverage and digest metadata, inject its repository and summarizer through
application-owned ports, bound semantic compaction passes, and explicitly
select a recent-message emergency projection when a dependency failure would
otherwise leave the request above the hard budget.

## Assistant-output visibility protocol

Core assigns model-stream data to four non-interchangeable semantics:

- `model.reasoning_delta` is raw provider reasoning for DEV diagnostics only. It
  is never conversation UI or a source of business progress.
- `model.content_delta` is unvalidated raw model content that a host may need
  to inspect, such as structured output, and is diagnostic-only as well.
- `assistant.commentary_delta` is user-visible execution narration in the work
  log alongside tools, plan state, and domain progress.
- `assistant.final_delta` is the final natural-language answer, separate from
  both execution narration and artifact content.

Runtime promotes fully accumulated ordinary text to public commentary only
when that model round actually starts tool calls; raw reasoning is never
promoted. A direct response without tools is final output. Deterministic host
progress must emit commentary explicitly instead of borrowing reasoning or
asking the frontend to infer progress from JSON or content fragments. Tool and
plan events stay structured. Artifact bodies are delivered through domain
effects or detail endpoints and are not copied into the conversation.

Transports may map all four event types to distinct stream fields, and a DEV
inspector may subscribe to the raw channels. A normal conversation reducer may
consume only commentary, final, tool, plan, and lifecycle events.
`assistant.commentary_delta` is replayable Run history; per-token raw/final
text events are transport-only, while the terminal Run stores the final answer
separately.

## Model output and tool-data boundaries

`model_protocol` normalizes provider finish reasons. Any provider-declared
length limit makes the round incomplete: partial text cannot become a final
answer, and partial tool calls are neither executed nor written into later
model history. A truncated request is not repeated with the same allowance.
Failure preserves the `tool_call_truncated` or `model_output_truncated` root
cause and safe diagnostics.

Output sizing has three separate authorities. Infrastructure model profiles
declare provider capability ceilings, Application policies estimate and cap
one unit of product work, and `purra.output_budget` resolves the effective
allowance against the context window. Only that resolved allowance may become
a provider `max_tokens` parameter. The resolution, capability ceiling, limiting
factor, actual provider usage, and finish reason are emitted in durable Run
events. Chunked work must add or split execution units instead of increasing a
global model default.

Audited tools declare model-generated, host-bound, and host-derived paths with
`ToolDataContract`. Host-owned paths must stay outside the model-visible JSON
Schema. Large-output tools should prefer `delta`, `batch`, or
`resource_reference`: the model produces only new semantic data while the
host binds identity, revision, lineage, accumulated content, and completion
state.

`ToolSchema.name` remains the immutable protocol identifier. Localized
user-facing labels live separately in the host-owned `display_names` map using
language tags such as `zh-CN` and `en-US`. Core selects the request locale for
planner guidance and model narration, while provider payloads still contain
only the protocol name, description, and parameters. Runtime events carry the
complete label map so the UI can localize deterministically without asking the
model to rename a function.

`ToolExecutionLimits.max_argument_chars` is a configurable raw-JSON transport
safety envelope, not a context allocation or domain data budget. After JSON
decoding and narrowly scoped structured-value recovery, Core enforces the
registered schema again (`required`, types, enums, lengths, item counts,
numeric ranges, and additional properties). This means whitespace and escaped
Unicode cannot consume an unrelated 32K workflow budget, while domains retain
authority over useful semantic size through their versioned schemas. Failures
carry the tool name, validation stage, schema path, actual measurement, and
allowed bound without echoing the rejected payload.

Core admits only this recursive JSON Schema subset: `type` (`object`, `array`,
`string`, `integer`, `number`, `boolean`, `null`, or a non-empty list of those
names), `properties`, `required`, boolean `additionalProperties`, `items`,
`anyOf`, `oneOf`, `enum`, `const`, `minLength`, `maxLength`, `minItems`,
`maxItems`, `minimum`, `maximum`, plus string `title` and `description`
annotations. Malformed schemas and every other assertion keyword are rejected
when the Tool Catalog is assembled. Runtime validation remains fail-closed as
defense in depth, and a rejected batch starts no handler.

## Stability evaluation

`evaluation.stability` derives content-free reliability signals from the
persisted Core event stream. It correlates tool starts, completions, and
results so explicit failures and calls that never reached a terminal result
remain distinguishable. The same report aggregates protocol error codes,
model interruption/retry evidence, context overflow, and compaction fallback
or failure outcomes. Infrastructure may attach storage-level artifact counts,
but it must not copy prompts, tool arguments, or generated content into the
stability report.

`StabilityTrendPolicy` applies caller-owned warning and failure thresholds to
a bounded newest-first Run window. Rate checks wait for a configurable minimum
sample, while consecutive-failure streaks alert immediately because hiding a
hard failure sequence behind a sample gate would be unsafe. The SQLite adapter
projects only trace counters, call identifiers, tool names, and error codes;
historical prompts, arguments, and tool-result content never enter the trend
evaluator.
User-canceled Runs are excluded from the trend window so an intentional abort
does not dilute failure rates or appear as an incomplete-tool regression.

`failure_classification` converts explicit, content-free evidence into stable
cause codes without claiming more certainty than the event stream supports.
Protocol errors, incomplete tool lifecycles, context failures, planner
contract violations, tool-handler failures, and model interruptions retain
separate classifications and remediation keys. A failed Run with no specific
evidence is reported as low-confidence observability debt instead of receiving
an invented cause.

`stability_gate` compares the newest Run window with the immediately preceding
window. It detects rate increases, failure-streak growth, and newly introduced
tool error codes. A host-owned incident catalogue may fix expected
classification codes in its runtime regression harness.

## Controlled recovery policy

`recovery` is the single Runtime decision layer for provider fallback, stream
interruption, truncation, tool-protocol repair, empty responses, response
repair, and tool-input correction. Every candidate action consumes a bounded
Run-scoped budget only after Core verifies cancellation state, remaining model
rounds, visible-output state, and whether tool side effects may have started.

Domain adapters may inject a `RecoveryPolicy` at the composition root, but only
Core can approve and account for attempts. JSON or Schema failures may be
repaired only when `ToolBatchResult.effect_state` proves the whole batch failed
before a handler started. An uncertain write effect blocks replay and failed-
step replanning. Allowed and denied decisions use the existing durable Run
trace path; `observability.recovery` exposes only cause, action, budget, and safety
reason codes, never model text or tool arguments.

## Recoverable artifact lifecycle

`artifacts` provides a domain-neutral `open → finalized/aborted` state
machine. Large results can be committed in ordered batches carrying an
idempotency key, content digest, expected revision, sequence, and coverage
keys. Core checks count, contiguous ordering, and duplicate/missing coverage
before finalization. `ArtifactValidator` keeps domain correctness outside
Core, while `ArtifactRepository` owns the transactional boundary. The SQLite
adapter atomically commits each batch, CAS revision, and replay receipt.

Multiple calls in one model round remain forbidden for ordinary write tools.
Core permits them only when every call targets the same `batch`-mode,
`PROPOSE`, cancellation-linearizable, host-durable artifact tool. The complete
call batch is still preflighted before any write. Domain adapters inject their
own `RuntimeLimits`, so Core's default no longer encodes a product-specific
batch-count assumption.

Planner and runtime tool contracts are now distinct. A registration may map a
stable business-level `planning_capability` onto one or more private runtime
tools. Core validates the public plan first, then deterministically lowers the
capability into its dependency-ordered runtime protocol. Private steps remain
durable and authorized but are omitted from public task-plan SSE; only the
business capability is shown. Host-authenticated continuation state can mark
individual private tools as already satisfied. A WorkPlan that names a private
runtime tool is rejected; public runtime tools without a planning capability
may still be selected directly.

Products may lower a public capability into private begin/append/finalize
tools, but product documents—not this framework—define the artifact kinds,
business operations, source receipts, and domain validation rules.

## Durable tasks and Artifact ownership

`long_tasks` is the single durable task aggregate. It owns task/unit lifecycle,
recovery state, usage and append-only Run bindings (`created`, `continuation`,
or `reference`). Repository creation must atomically persist the task, its units
and the creator binding. PurrA intentionally has no parallel Work Item status
machine.

An Artifact is recoverable output, not a runtime checkpoint or task record. Its
`ArtifactOwnerRef(kind, id)` is an opaque host-owned identity: PurrA never reads
business tables or interprets the owner kind. The creating Run has access by
provenance; cross-Run access is fail-closed unless an injected
`ArtifactAccessAuthorizer` grants it. Every writer, including the creating Run,
must acquire an atomic, expiring claim after lifecycle and revision checks.
Finalization changes only Artifact state and never completes another aggregate.

Artifact maintenance is storage-neutral. Expired or invalid claims are
disposable; open Artifact content is not. Terminal retention remains opt-in,
bounded by `max_purge_artifacts`, and adapters report only content-free status
and claim counts. Scheduling, persistence implementation and host/domain
authorization remain outside PurrA.

Operational reports derived from Runs and events live under `observability`.
Deterministic regression cases and security red-team cases live under
`evaluation`; neither package participates in Runtime state transitions or
checkpoint recovery.

## Reusable persistence ports

- `RunRepository`: atomic Run lifecycle and outbox events.
- `ExecutionLeaseStore`: execution ownership, heartbeat, and durable cancel.
- `DelegationRepository`: Root Run-scoped delegation batches and results.
- `RunRecoveryStore`: typed, cursor-based `RunRecoverySnapshot` values.
- `ApprovalGateway`: one-shot human decisions.
- `ToolIdempotencyGateway`: replay-safe side-effecting tool execution.
- `ContextCompressionHook`: application-owned reduction policy; Core only
  invokes it and validates its result.
- `ArtifactRepository`: recoverable large-result batches with atomic revision,
  ordering, and idempotency receipts.
- `ArtifactValidator`: domain-supplied batch/final validation without putting
  product structure into Core.
- `LongTaskRepository`: durable task/unit lifecycle plus append-only Run
  relationships.
- `ArtifactAccessAuthorizer`: host policy for cross-Run Artifact access.
- `ArtifactClaimRepository`: exclusive, expiring writer ownership for
  Artifacts.
- `ArtifactMaintenanceRepository`: atomic lease cleanup, content-free
  consistency reporting, and explicitly configured terminal retention.

Concrete adapters are assembled by the host composition root.

## One-Run multi-Agent delegation

When an `AgentPreset` selects a `DelegationPolicy` and the required delegation
infrastructure is configured, PurrA exposes `delegateToAgents` as
one ordinary model-facing tool. The parent model defines each task-specific
Agent in that call with `agentName`, `title`, `instruction`, `objective`, and an
optional input object. Creation, invocation, cancellation, result collection,
and aggregation all complete within that single tool lifecycle. Every call
creates a delegation batch inside the same Root Run; it never creates another
Run, Run tree, execution lease, or event stream.

`DelegationCoordinator` owns bounded concurrency, cancellation, persistence,
and lifecycle events. `DynamicDelegatedAgentExecutor` executes the model-defined
Agent in-process. The model owns only its semantic definition; PurrA fixes its
authority. A delegated Agent receives an isolated conversation, the Root Run's
bounded Domain Context and context provider, and only read tools that are
currently enabled for the Root Agent. It cannot see the parent's identity or
private conversation, receive write tools, or call `delegateToAgents`
recursively. A host may replace the executor port, but PurrA still exposes only
one Root Run identity. Results remain batch-scoped, and every event stays
attributed to the Root Run.

`DelegationPolicy` is the single host-supplied limit contract for maximum
delegation count, parallelism, and definition field lengths. It cannot grant
parent context, write tools, or recursive delegation.
`None` disables delegation. The complete policy and effective delegation Tool
schema are part of snapshot version 2; repositories and executors remain
Kernel infrastructure and are not serialized.

Delegation requires both a `DelegationRepository` and a
`ToolIdempotencyGateway`. Retrying the same parent tool call replays its stored
result instead of creating another batch. Delegated tool-call keys are scoped
by `delegation_id`, preventing collisions inside the shared Root Run. An active
model stream is still process-local; work requiring an independent durable
lease belongs to durable task orchestration, not a hidden child Run.

## Adding another adapter

New adapters use the public probes in `purra.testing`. They verify ownership,
cancellation, delegation capacity and batch isolation, checkpoint cursors, and
tool replay semantics without depending on a particular database.

Product context providers, tool catalogs, response policies, and UI mappings
stay outside this package. Neither PurrA nor the host registers a fixed child
role catalog. Agent semantics come from the parent model at the tool boundary;
authority remains a PurrA-owned runtime contract.
