# purra

Business-agnostic, host-embedded Agent runtime for JavaScript and TypeScript.

```sh
npm install purra
pnpm add purra
yarn add purra
bun add purra
```

The npm package is versioned independently from the Python distribution before
1.0. Matching version numbers do not imply parity; the capability matrix in
`ARCHITECTURE.md` defines what this npm version supports. Incompatible public
npm API changes require a new minor version while the package remains pre-1.0.

```ts
import { Agent } from "purra";

const agent = new Agent({ model: yourModelGateway, tools: yourTools });
const run = await agent.submit({
  messages: [{ role: "user", content: "Hello" }],
});
const result = await run.result;

for await (const event of run.events()) {
  // Public, committed events in sequence order.
  console.log(event.kind, event.payload);
}
```

Reactive remains the default. Planned execution is opt-in and requires both a
Planner and a host policy:

```ts
import { Agent, ToolPlanningPolicy } from "purra";

const agent = new Agent({
  model: yourModelGateway,
  tools: yourTools,
  planning: {
    planner: yourWorkPlanner,
    policy: new ToolPlanningPolicy({ maxSteps: 6 }),
  },
});
```

Hosts may keep their own Planner or opt into the Provider-neutral reference
Planner. A factory binds its private planning calls to the current execution:

```ts
import { ModelWorkPlanner } from "purra";

const agent = new Agent({
  model: yourModelGateway,
  tools: yourTools,
  planning: {
    policy: new ToolPlanningPolicy(),
    plannerFactory: (modelTasks) => new ModelWorkPlanner(modelTasks),
  },
});
```

`ModelWorkPlanner` repairs invalid JSON only within its configured fixed budget;
every accepted result still passes through the existing plan compiler and
current-step tool authority. `ModelResponseJudge` similarly wraps a host-owned
prompt/evaluation policy, and is attached through `responseValidation.judgeFactories`.
Reactive composition never constructs either helper.

Durable execution is also explicit. It extends a bound Planned composition with
a task-admission evaluator, Long Task dispatcher, and recovery authenticator.
Durable admission covers every compiled plan step before any task is dispatched.
Authenticated continuation reuses the immutable dispatch receipt and does not
replan or reset persisted deadlines and budgets.

Recoverable Artifacts are a separate host-facing aggregate. They provide
ordered versioned batches, idempotent append receipts, coverage validation,
exclusive expiring writer claims, explicit finalize/abort transitions, and
bounded terminal-state maintenance. Finalizing an Artifact does not complete
its owning Run or Long Task.

One-Run delegation is also opt-in through `delegation`. The built-in
`delegateToAgents` tool creates bounded, idempotent batches inside a submitted
Root Run. Each delegated Agent receives an isolated conversation and only the
Root Run's currently enabled read tools; write tools, recursive delegation,
child Runs, and a second event stream are excluded. Delegated model invocations,
budgets, cancellation, tool lifecycle, and status events remain under the same
Root Run authority.

The Planner returns a semantic `TaskSpec`/`WorkPlan`; Core validates it and
compiles only the current step into runtime tool authority. A tool may expose a
public `planning.capability` whose name differs from its private runtime name.
Tools request a dynamic revision only with an explicit
`planningDisposition: "replan"` receipt; normal progress does not call the
Planner again.

Context budgeting is opt-in and requires a model capability snapshot with a
known context window and output limit:

```ts
const agent = new Agent({
  model: yourModelGateway,
  context: {
    claims: [{ name: "project", desiredTokens: 2_000 }],
    provider: {
      buildContext: async (_request, budget, signal) => ({
        blocks: [await loadProjectContext(budget.contextAllocations.project, signal)],
      }),
    },
  },
});
```

Model-backed context and compaction hooks receive a managed runner from a
factory. Standalone hosts may construct the same runner directly. It exposes
tool-free `complete()` and `streamText()` calls with exact output limits,
bounded empty-response recovery, cancellation, and optional Operation events:

```ts
const agent = new Agent({
  model: yourModelGateway,
  context: {
    claims: [{ name: "derived", desiredTokens: 512 }],
    providerFactory: (modelTasks) => ({
      async buildContext(request, budget, signal) {
        const result = await modelTasks.complete(request.messages, { signal });
        return { blocks: [{ name: "derived", content: String(result.turn.message.content) }] };
      },
    }),
  },
});
```

For `Agent.submit()`, those calls persist invocation receipts and consume model
budgets under the same Run before the Provider is called. A direct provider or
compression hook and its corresponding factory are mutually exclusive.

Version `0.1.0-alpha.0` covers the Reactive model/tool loop plus
explicit Planned and Durable compositions,
cancellation, bounded rounds, immutable JSON messages, normalized finish
reasons, model capability/output-limit contracts, Provider stream consumption,
recursive JSON-Schema inspection, whole-batch tool admission, enablement,
scope, approval, idempotency, effect-state safety, sanitized results, and
bounded Provider/tool/response recovery. `Agent.submit()` owns an in-memory Run and canonical
output journal: it persists invocation receipts before Provider calls, records
private reasoning/raw model evidence separately from public commentary/tool/
plan/final/lifecycle output, supports ordered replay and cancellation, and settles
the final output with the terminal Run state atomically. Attempt, reported-token,
output, and absolute-deadline budgets share the same repository authority.
Optional context composition derives a hard provider-input budget from the
model window, output reserve, tool schemas, runtime/safety reserves, and opaque
host claims. It inserts host context as explicitly trusted or data-only input,
uses request-scoped projections rather than mutating canonical history, and
binds selected evidence receipts to each invocation. Planned runs retrieve
Staged task context only after a valid `TaskSpec` has compiled. The built-in fallback
keeps recent complete turns; an optional host compression hook may add only a
bounded context summary and cannot remove privileged instructions, the current
request, or part of a tool exchange.

`Agent.invoke()` and `Agent.stream()` remain process-local convenience APIs.
`Agent.stream()` exposes provisional model deltas, tool lifecycle events, and
one final result; it never emits reasoning. When response validation is
configured, candidate deltas are withheld until a bounded validation/repair
transaction accepts the result. The package currently ships only in-memory
Run/output/Long Task/Artifact/delegation adapters, so production persistence is
not implied.
Durable ports cover task-unit DAGs, checkpoints, usage, lease fencing, retries,
pause/resume/cancel, authenticated continuation, and storage-neutral orphan
recovery. Artifact ownership remains opaque and domain validation stays in a
Host callback. Domain partitioning, unit inputs, merging, product lifecycle
projection, Provider SDKs, and production persistence remain host-owned or
outside this phase.

Recovery is explicit at composition time and request-scoped at runtime:

```ts
import { Agent, RecoveryPolicy } from "purra";

const agent = new Agent({
  model: yourModelGateway,
  recovery: new RecoveryPolicy().withOverrides({ empty_model_response: 1 }),
});
```

The ledger checks cancellation, remaining model rounds, already-visible output,
and side-effect state before consuming an attempt. Submitted Runs persist an
`agentRunTrace` decision before retrying or replanning. Transient calls use the
same policy without claiming durable evidence.

Operational reports are derived after the fact from canonical Run evidence:

```ts
import {
  classifyAgentRunFailures,
  evaluateAgentRun,
  evaluateAgentRunStability,
} from "purra";

const operational = evaluateAgentRun(runSnapshot, committedEvents);
const stability = evaluateAgentRunStability(committedEvents);
const failures = classifyAgentRunFailures(runSnapshot, committedEvents);
```

These reports use allowlisted counters, codes, durations, and decisions. They
never copy prompts, arguments, tool results, reasoning, or generated content,
and they cannot mutate a Run. `runRuntimeRegressionSuite()` and
`runSecurityRedTeamCases()` provide deterministic regression checks over the
same public boundaries.

`InMemoryAgentAdapters` composes all process-local reference adapters. Hosts can
exercise a production implementation with the exported `assert*Conforms`
helpers for model, context, tool, Run, output, delegation, Long Task, and
Artifact ports. These probes execute real lifecycle transitions; they are not
method-presence checks. Provider SDKs and production persistence remain
host-owned.

Real-Provider verification belongs to a future host project, not this
Provider-neutral package or the current npm release. That host must install the
published artifact through public exports and cover completion, streaming, a
read-tool round, cancellation, and a submitted Run. Missing credentials remain
`NOT RUN`, not a passing result.

The deterministic Phase 12C gate covers the complete documented alpha matrix
through the package root, including the managed model-task, reference Planner,
response-judge, and Operation composition path. This is framework closure, not
a stable parity claim; the external credentialed gate remains outstanding.

`ModelGateway.invoke()` is the completion fallback. A gateway may also expose
`stream()` as an `AsyncIterable`; `Agent.invoke()` then aggregates its content,
reasoning, tool-call deltas, usage, and terminal finish reason while owning
cancellation and iterator cleanup.

Every tool declares a model-visible object Schema and explicit policy. A read
tool returns an explicit no-effect receipt:

```ts
const lookup = {
  name: "lookup",
  description: "Look up a value",
  inputSchema: {
    type: "object",
    properties: { key: { type: "string" } },
    required: ["key"],
    additionalProperties: false,
  },
  policy: { mode: "read", title: "Look up" },
  run: async (input) => {
    const { key } = input as { readonly key: string };
    return {
      content: await hostLookup(key),
      effectState: "not_started",
    };
  },
} as const;
```

`propose` and `confirm` tools require a host idempotency gateway unless they
explicitly declare host-managed durability. `confirm` additionally requires an
approval gateway. An uncertain side effect stops the run and is never replayed.

See [ARCHITECTURE.md](ARCHITECTURE.md) for source ownership and dependency
rules.
