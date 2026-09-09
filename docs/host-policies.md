# Host policies and runtime guarantees

PurrA supplies default execution behavior and public composition ports. Hosts
choose business policy; Core validates the resulting execution. Replacing a
policy does not grant tool access or bypass budget, cancellation, ownership,
checkpoint, or effect-recovery checks.

| Choice | Public composition | Runtime boundary |
| --- | --- | --- |
| Planning activation | Per-request Auto, Reactive or Planned; host may classify requests before submission | Planning-required tools cannot run unplanned; phase controls cannot replay completed effects |
| Plan generation | Python `WorkPlanner` / `ExecutionProfile.planner`; TypeScript `WorkPlanner` / `planning.plannerFactory` | Core compiles and validates tools, dependencies and execution authority |
| Initial model-plan granularity | `PlanningPolicy` constraints, described below | A display preference is not a tool grant; revisions are not padded |
| Model selection before a Run | Host orders authorized route candidates or selects an authorized subset | Compatible saved binding is restored without reselecting on recovery |
| Model choice for the next conversation turn | Host submits a new Run with the chosen binding and retained conversation messages | Each existing Run keeps its binding; changing models during execution is unsupported |
| Memory capture and resolution | Optional Mem0 component; host calls capture with authorization and supplies a resolution policy | Provenance, current authorization and revocation still apply |
| Relation extraction | Host supplies memory revisions, allowed relation names and an extractor to the [optional proposal helper](../integrations/mem0/relations.md) | Source quotes and current revisions are checked; proposals do not authorize writes |
| Memory retrieval composition | Host supplies an ordered-ID callback through [MemoryContext selection](../integrations/mem0/selection.md) | Fresh scoped reads, complete-record budgets and source evidence still apply |
| Delegation | Public spawn/join/continue commands or the default delegation tool; `AgentTreePolicy` limits | Descendant grants narrow and Root budget/ownership remain authoritative |
| Recovery scheduling | Worker discovery, schedule and public-resume callbacks; host owns service lifetime | Discovery/inspection does not grant permission to resume |

## Initial model-plan preference

The built-in model planners default to at least **three visible semantic steps**
for an initial plan. A host may select a different positive safe integer through
its existing planning policy. This does not force unnecessary steps: a direct
response remains valid when allowed by the host's other constraints. An implicit
Respond step does not count; revisions may contain one remaining step. Custom
WorkPlanners need not use this model-planner preference, but still pass the Core
plan compiler and authority checks.

Python:

```python
from purra.contracts import PlanningConstraints

class ReviewPolicy:
    def planning_constraints(self, request, capabilities):
        return PlanningConstraints(min_initial_visible_steps=2)
```

Use this policy as `ExecutionProfile(planner=planner, planning_policy=policy)`.
Preserve other request constraints when refining an existing host policy.

TypeScript:

```typescript
import { ModelWorkPlanner, type ModelTaskRunner } from "purra";

const planning = {
  policy: {
    planningConstraints() {
      return { minInitialVisibleSteps: 2 };
    },
  },
  plannerFactory: (tasks: ModelTaskRunner) => new ModelWorkPlanner(tasks),
};
```

`ModelTaskRunner` and `ModelWorkPlanner` are exported by `purra`. The choice
reaches both the Planner prompt and its output validation. Invalid values fail
before a model request. Auto's private control does not impose a separate fixed
step count. Use explicit Planned mode when the host must activate planning;
changing the preference alone is not a request classifier.

## Identity and recovery

Version host strategy implementations and configuration using existing bindings:
Python `AgentComponentBinding` for `planningPolicy` (and `planner`), TypeScript
`planning.binding`. Change the revision/config identity when behavior changes;
opaque callbacks cannot be inspected for semantic equivalence by Core. A changed
binding must not resume a Run under its old identity. This is a host obligation,
not automatic hashing of closure state.

Committed planning checkpoints preserve the selected constraints. Older
checkpoints without the preference retain the three-step default. Restoring a
checkpoint does not rerun the current host selection policy. These mechanisms do
not enable in-Run model switching or arbitrary policy replacement on an active Run.

## Output choices and current limits

Python exposes `ResponseTransactionPolicy` with `direct_live` or
`validated_result`. A validated result can omit public presentation or request a
model-live presentation from a host facts provider. Direct-live output cannot
also require full-text validation. These are distinct result contracts, not a
switch that republishes private text.

TypeScript exposes `Agent({ responsePresentation: "none", ... })` to return a
host-only result without the extra public-presentation round. The default
`"model_live"` preserves existing behavior. `responseValidation` still runs;
choosing `none` does not imply a business validator has been configured.

```typescript
const agent = new Agent({ model, tools, responsePresentation: "none" });
const handle = await agent.submit(request, {
  budgets: { maxRunGenerationTokens: null },
});
const result = await handle.result; // result.output is host-only data
```

In `none` mode, `handle.events()` still includes public lifecycle, tool and
planning events, but the final result event is private. The host can read it
using `visibility: "all"`; `snapshot().finalOutput` also remains host-only data.
The direct `invoke()` return value is a host result. Transient `stream()` emits
no response-text deltas and marks its final result event `visibility: "private"`.
Do not forward host results or private events directly into a public chat UI.
This setting suppresses response presentation, not every progress/tool event.

The choice is part of the saved Agent composition: switching between `none`
and `model_live` cannot resume the same Run. Omitted and explicit `model_live`
retain the old composition fingerprint. Private final results still commit
atomically with terminal status; an OutputPolicy cannot promote them to public.
Model usage, validation, approval and cancellation follow the same execution
path. No private reasoning is promoted to a public response.

The SDKs now both offer a no-public-presentation choice, with different result
APIs. This does not add Python's general facts-provider/model-live presentation
composition to TypeScript or enable arbitrary host render callbacks.

Default planning activation phases, the presentation boundary and Agent-tree
budget semantics remain constrained. PurrA does not offer arbitrary graph
scheduling. Hosts can replace documented policies, not every internal transition.

## Optional capabilities

Core has no runtime dependency on the Mem0, media, database or worker service
integrations. A text-and-tool host need not configure those capabilities. Choosing
policies does not require adopting additional backends or external services.
