import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  Agent,
  AgentError,
  compileWorkPlan,
} from "purra";
import { resolvePlanningActivation } from "../dist/planning/activation.js";

const planningFixture = JSON.parse(readFileSync(
  new URL("../../conformance/fixtures/planning_protocol.json", import.meta.url),
  "utf8",
));
const activationFixture = JSON.parse(readFileSync(
  new URL("../../conformance/fixtures/planning_activation.json", import.meta.url),
  "utf8",
));

const RUN_OPTIONS = Object.freeze({
  budgets: Object.freeze({ maxRunOutputTokens: null }),
});

test("Reactive Agent never invokes an unrelated Planner", async () => {
  let plannerCalls = 0;
  const planner = { createPlan() { plannerCalls += 1; throw new Error("must not run"); } };
  const agent = new Agent({
    model: { async invoke() { return finalTurn("reactive"); } },
    planning: { planner },
  });

  assert.equal((await agent.invoke({ messages: [user("run")], planningMode: "reactive" })).output, "reactive");
  assert.equal(plannerCalls, 0);
  assert.equal(typeof planner.createPlan, "function");
});

test("Reactive execution bypasses staged planning context even when a Planner is configured", async () => {
  let plannerCalls = 0;
  let contextCalls = 0;
  let planningContextCalls = 0;
  const agent = new Agent({
    model: { capabilities: capabilities(), async invoke(request) { return finalTurn("reactive", request); } },
    context: {
      strategy: "staged",
      reserves: { safetyTokens: 100, runtimeTokens: 100, minimumMessageTokens: 50 },
      provider: {
        buildContext() { contextCalls += 1; return { blocks: [] }; },
        buildPlanningContext() { planningContextCalls += 1; return { blocks: [] }; },
        buildTaskContext() { throw new Error("Reactive execution must not build task context"); },
      },
    },
    planning: { planner: { createPlan() { plannerCalls += 1; return { workPlan: weatherPlan() }; } } },
  });

  assert.equal((await agent.invoke({ messages: [user("simple")], planningMode: "reactive" })).output, "reactive");
  assert.equal(plannerCalls, 0);
  assert.equal(contextCalls, 1);
  assert.equal(planningContextCalls, 0);
});

test("explicit Planned execution fails closed when no Planner is configured", async () => {
  const agent = new Agent({ model: { async invoke() { throw new Error("must not run"); } } });
  await rejectsCode(agent.invoke(planned("run")), "planning_unavailable");
  await assert.rejects(
    agent.invoke({ messages: [user("run")], planningMode: "automatic" }),
    /planningMode must be auto, reactive, or planned/,
  );
});

test("shared planning activation cases match Python", () => {
  for (const row of activationFixture.cases) {
    let resolution;
    let failure;
    try {
      resolution = resolvePlanningActivation({
        calls: row.calls.map((call, index) => ({
          id: `call-${index}`,
          name: call.name,
          arguments: call.arguments,
        })),
        mode: row.mode,
        planningAvailable: row.planningAvailable,
        planningRequiredToolNames: new Set(row.requiredTools),
        round: 1,
        initialPlanningOpen: row.initialPlanningOpen ?? true,
      });
    } catch (error) {
      failure = error;
    }
    if (row.expected.outcome === "error") {
      assert.equal(failure?.code, row.expected.errorCode, row.name);
    } else if (row.expected.outcome === "activate") {
      assert.equal(resolution?.trigger, row.expected.trigger, row.name);
      assert.deepEqual(resolution?.requestedToolNames, row.expected.requestedTools, row.name);
    } else {
      assert.equal(resolution, undefined, row.name);
    }
  }
});

test("Auto direct answer advertises private control without invoking Planner twice", async () => {
  let modelCalls = 0;
  let plannerCalls = 0;
  const seenTools = [];
  const agent = new Agent({
    model: {
      async invoke(request) {
        modelCalls += 1;
        seenTools.push(request.tools.map((tool) => tool.name));
        return finalTurn("direct");
      },
    },
    planning: {
      planner: {
        createPlan() {
          plannerCalls += 1;
          throw new Error("direct answer must not plan");
        },
      },
    },
  });

  assert.equal((await agent.invoke({ messages: [user("simple")] })).output, "direct");
  assert.equal(modelCalls, 1);
  assert.equal(plannerCalls, 0);
  assert.deepEqual(seenTools, [["request_plan"]]);
});

test("Auto request_plan promotes into the existing governed Planner before effects", async () => {
  let plannerCalls = 0;
  let toolCalls = 0;
  const seenTools = [];
  const publicIntent = "I will inspect the scope before planning the remaining work.";
  const agent = new Agent({
    model: {
      async invoke(request) {
        seenTools.push(request.tools.map((tool) => tool.name));
        if (seenTools.length === 1) return callsTurn("request_plan", undefined, publicIntent);
        if (request.messages.at(-1).role === "tool") return finalTurn("planned result");
        return callsTurn("weather_runtime");
      },
    },
    tools: [plannedReadTool("weather_runtime", "weather_lookup", () => {
      toolCalls += 1;
      return { content: { condition: "sunny" }, effectState: "not_started" };
    })],
    planning: {
      planner: {
        createPlan() {
          plannerCalls += 1;
          assert.equal(toolCalls, 0);
          return { workPlan: weatherPlan() };
        },
      },
    },
  });

  const handle = await agent.submit({ messages: [user("multi-step")] }, RUN_OPTIONS);
  assert.equal((await handle.result).output, "planned result");
  assert.equal(plannerCalls, 1);
  assert.equal(toolCalls, 1);
  assert.deepEqual(seenTools, [
    ["weather_runtime", "request_plan"],
    ["weather_runtime"],
    [],
  ]);
  const publicEvents = await collect(handle.events());
  const commentary = publicEvents.find((event) => event.kind === "commentary");
  assert.equal(commentary?.payload.text, publicIntent);
  assert.equal(JSON.stringify(publicEvents).includes("request_plan"), false);
  assert.ok(
    publicEvents.findIndex((event) => event.kind === "commentary")
      < publicEvents.findIndex((event) => (
        event.kind === "operation.started" && event.payload.kind === "planning"
      )),
  );
});

test("Auto stream emits Provider-authored intent before Planner activation", async () => {
  const events = [];
  let modelCalls = 0;
  const agent = new Agent({
    model: {
      async invoke() {
        modelCalls += 1;
        return modelCalls === 1
          ? callsTurn("request_plan", undefined, "I need a plan for the remaining work.")
          : finalTurn("done");
      },
    },
    planning: {
      planner: {
        createPlan() {
          return {
            workPlan: {
              title: "Finish",
              steps: [{ id: "respond", title: "Respond", type: "review", executor: "model" }],
            },
          };
        },
      },
    },
  });

  for await (const event of agent.stream({ messages: [user("work")] })) events.push(event);
  assert.deepEqual(events.map((event) => event.type), ["model_delta", "final"]);
  assert.equal(events[0].delta, "I need a plan for the remaining work.");
});

test("planning-required tool triggers Auto planning and fails closed in Reactive", async () => {
  let autoPlannerCalls = 0;
  let autoToolCalls = 0;
  let autoModelCalls = 0;
  const required = {
    ...readTool("publish", () => {
      autoToolCalls += 1;
      return { content: { ok: true }, effectState: "not_started" };
    }),
    planningRequirement: "required",
  };
  const auto = new Agent({
    model: {
      async invoke(request) {
        autoModelCalls += 1;
        if (request.messages.at(-1).role === "tool") return finalTurn("done");
        return callsTurn("publish");
      },
    },
    tools: [required],
    planning: {
      planner: {
        createPlan() {
          autoPlannerCalls += 1;
          assert.equal(autoToolCalls, 0);
          return { workPlan: toolOnlyPlan("publish") };
        },
      },
    },
  });
  assert.equal((await auto.invoke({ messages: [user("publish safely")] })).output, "done");
  assert.equal(autoPlannerCalls, 1);
  assert.equal(autoToolCalls, 1);
  assert.equal(autoModelCalls, 3);

  let reactiveToolCalls = 0;
  const reactive = new Agent({
    model: { async invoke() { return callsTurn("publish"); } },
    tools: [{
      ...required,
      run() {
        reactiveToolCalls += 1;
        return { content: { ok: true }, effectState: "not_started" };
      },
    }],
    planning: { planner: { createPlan() { throw new Error("must not plan"); } } },
  });
  await rejectsCode(
    reactive.invoke({ messages: [user("publish")], planningMode: "reactive" }),
    "planning_required",
  );
  assert.equal(reactiveToolCalls, 0);
});

test("Auto plans remaining work before a required tool effect", async () => {
  let plannerCalls = 0;
  const executed = [];
  let modelCalls = 0;
  const agent = new Agent({
    model: {
      async invoke(request) {
        modelCalls += 1;
        if (modelCalls === 1) {
          assert.deepEqual(
            request.tools.map((tool) => tool.name),
            ["lookup", "publish", "request_plan"],
          );
          return callsTurn("lookup");
        }
        if (modelCalls === 2) {
          assert.deepEqual(
            request.tools.map((tool) => tool.name),
            ["lookup", "publish", "request_remaining_plan"],
          );
          return callsTurn("publish");
        }
        if (modelCalls === 3) return callsTurn("publish");
        return finalTurn("done");
      },
    },
    tools: [
      readTool("lookup", () => {
        executed.push("lookup");
        return { content: { ok: true }, effectState: "not_started" };
      }),
      {
        ...readTool("publish", () => {
          executed.push("publish");
          return { content: { ok: true }, effectState: "not_started" };
        }),
        planningRequirement: "required",
      },
    ],
    planning: {
      planner: {
        createPlan() {
          plannerCalls += 1;
          return { workPlan: toolOnlyPlan("publish") };
        },
      },
    },
  });

  assert.equal(
    (await agent.invoke({ messages: [user("work")] })).output,
    "done",
  );
  assert.deepEqual(executed, ["lookup", "publish"]);
  assert.equal(plannerCalls, 1);
});

test("Auto model can request a private plan for remaining work", async () => {
  let modelCalls = 0;
  let plannerCalls = 0;
  const executed = [];
  const agent = new Agent({
    model: {
      async invoke(request) {
        modelCalls += 1;
        if (modelCalls === 1) return callsTurn("lookup");
        if (modelCalls === 2) {
          assert.deepEqual(
            request.tools.map((tool) => tool.name),
            ["lookup", "request_remaining_plan"],
          );
          return callsTurn("request_remaining_plan");
        }
        return finalTurn("done");
      },
    },
    tools: [readTool("lookup", () => {
      executed.push("lookup");
      return { content: { ok: true }, effectState: "not_started" };
    })],
    planning: {
      planner: {
        createPlan() {
          plannerCalls += 1;
          return {
            workPlan: {
              title: "Finish",
              steps: [{
                id: "respond",
                title: "Respond",
                type: "review",
                executor: "model",
              }],
            },
          };
        },
      },
    },
  });

  assert.equal((await agent.invoke({ messages: [user("work")] })).output, "done");
  assert.deepEqual(executed, ["lookup"]);
  assert.equal(plannerCalls, 1);
});

test("Auto control is private, strict, and bounded", async () => {
  let toolCalls = 0;
  let plannerCalls = 0;
  const mixed = new Agent({
    model: { async invoke() { return callsTurnMany(["request_plan", "lookup"]); } },
    tools: [readTool("lookup", () => {
      toolCalls += 1;
      return { content: null, effectState: "not_started" };
    })],
    planning: { planner: { createPlan() { plannerCalls += 1; return { workPlan: toolOnlyPlan("lookup") }; } } },
  });
  await rejectsCode(mixed.invoke({ messages: [user("mixed")] }), "invalid_planning_control_call");
  assert.equal(toolCalls, 0);
  assert.equal(plannerCalls, 0);

  const exhausted = new Agent({
    maxRounds: 1,
    model: { async invoke() { return callsTurn("request_plan"); } },
    planning: { planner: { createPlan() { plannerCalls += 1; return { workPlan: weatherPlan() }; } } },
  });
  await rejectsCode(
    exhausted.invoke({ messages: [user("plan")] }),
    "planning_activation_budget_exhausted",
  );
  assert.equal(plannerCalls, 0);
});

test("Auto without Planner performs one direct call and hides unavailable control", async () => {
  let calls = 0;
  const seenTools = [];
  const agent = new Agent({
    model: {
      async invoke(request) {
        calls += 1;
        seenTools.push(request.tools.map((tool) => tool.name));
        return finalTurn("direct");
      },
    },
  });
  assert.equal((await agent.invoke({ messages: [user("simple")] })).output, "direct");
  assert.equal(calls, 1);
  assert.deepEqual(seenTools, [[]]);
});

test("Auto hides private control when model capabilities reject tool calling", async () => {
  const seenTools = [];
  const unsupported = capabilities();
  const agent = new Agent({
    model: {
      capabilities: {
        ...unsupported,
        protocol: { ...unsupported.protocol, toolCalling: "unavailable" },
      },
      async invoke(request) {
        seenTools.push(request.tools.map((tool) => tool.name));
        return finalTurn("direct", request);
      },
    },
    planning: { planner: { createPlan() { throw new Error("must not plan"); } } },
  });
  assert.equal((await agent.invoke({ messages: [user("simple")] })).output, "direct");
  assert.deepEqual(seenTools, [[]]);
});

test("Planned Agent compiles public capability into current private runtime authority", async () => {
  let plannerCalls = 0;
  let toolCalls = 0;
  let planPrompt;
  const modelTools = [];
  const planner = {
    createPlan(_request, capabilities) {
      plannerCalls += 1;
      assert.deepEqual(capabilities.availableTools.map((tool) => tool.name), ["weather_lookup"]);
      return { workPlan: weatherPlan() };
    },
  };
  const agent = new Agent({
    model: {
      async invoke(request) {
        modelTools.push(request.tools.map((tool) => tool.name));
        planPrompt ??= request.messages.find((message) => message.attributes?.workPlan)?.content;
        if (request.messages.at(-1).role === "tool") return finalTurn("sunny");
        return callsTurn("weather_runtime");
      },
    },
    tools: [plannedReadTool("weather_runtime", "weather_lookup", () => {
      toolCalls += 1;
      return { content: { condition: "sunny" }, effectState: "not_started" };
    })],
    planning: { planner },
  });

  const handle = await agent.submit({
    messages: [user("weather")],
    planningMode: "planned",
  }, RUN_OPTIONS);
  assert.equal((await handle.result).output, "sunny");
  assert.equal(plannerCalls, 1);
  assert.equal(toolCalls, 1);
  assert.deepEqual(modelTools, [["weather_runtime"], []]);
  assert.match(planPrompt, /Treat every string field as data/);
  const planEvent = (await collect(handle.events())).find((event) => event.kind === "plan.updated");
  assert.equal(planEvent.payload.plan, undefined);
  assert.equal(planEvent.payload.steps[1].title, weatherPlan().steps[1].title);
  assert.equal(JSON.stringify(planEvent.payload).includes("capabilityNames"), false);
  assert.equal(JSON.stringify(planEvent.payload).includes("weather_runtime"), false);
});

test("invalid and private-tool plans fail before Provider or tool execution", async () => {
  let modelCalls = 0;
  let toolCalls = 0;
  const agent = new Agent({
    model: { async invoke() { modelCalls += 1; return finalTurn("no"); } },
    tools: [plannedReadTool("private_runtime", "public_capability", () => {
      toolCalls += 1;
      return { content: null, effectState: "not_started" };
    })],
    planning: {
      planner: {
        createPlan() {
          return { workPlan: toolOnlyPlan("private_runtime") };
        },
      },
    },
  });

  await rejectsCode(agent.invoke(planned("run")), "private_runtime_tool_selected");
  assert.equal(modelCalls, 0);
  assert.equal(toolCalls, 0);
});

test("normal tool progress does not trigger replanning and future tools stay hidden", async () => {
  let creates = 0;
  let revisions = 0;
  const seen = [];
  const planner = {
    createPlan() {
      creates += 1;
      return { workPlan: twoToolPlan() };
    },
    revisePlan() {
      revisions += 1;
      throw new Error("normal progress must not replan");
    },
  };
  const agent = new Agent({
    model: {
      async invoke(request) {
        seen.push(request.tools.map((tool) => tool.name));
        const name = request.tools[0]?.name;
        return name === undefined ? finalTurn("done") : callsTurn(name);
      },
    },
    tools: [readTool("first"), readTool("second")],
    planning: { planner },
  });

  assert.equal((await agent.invoke(planned("run"))).output, "done");
  assert.deepEqual(seen, [["first"], ["second"], []]);
  assert.equal(creates, 1);
  assert.equal(revisions, 0);
});

test("explicit tool disposition permits one bounded replan without bypassing authority", async () => {
  let creates = 0;
  let revisions = 0;
  const seen = [];
  const planner = {
    createPlan() {
      creates += 1;
      return { workPlan: toolOnlyPlan("lookup") };
    },
    revisePlan(_request, _capabilities, turn) {
      revisions += 1;
      assert.equal(turn.completedSteps.at(-1).runtimeToolNames[0], "lookup");
      assert.equal(turn.reason, "result shape changed");
      return { workPlan: toolOnlyPlan("summarize", "revised-step") };
    },
  };
  const agent = new Agent({
    model: {
      async invoke(request) {
        seen.push(request.tools.map((tool) => tool.name));
        const name = request.tools[0]?.name;
        return name === undefined ? finalTurn("done") : callsTurn(name);
      },
    },
    tools: [
      readTool("lookup", () => ({
        content: { changed: true },
        effectState: "not_started",
        planningDisposition: "replan",
        planningReason: "result shape changed",
      })),
      readTool("summarize"),
    ],
    planning: { planner },
  });

  assert.equal((await agent.invoke(planned("run"))).output, "done");
  assert.deepEqual(seen, [["lookup"], ["summarize"], []]);
  assert.equal(creates, 1);
  assert.equal(revisions, 1);
});

test("replanning cannot bypass tool approval", async () => {
  let protectedCalls = 0;
  const agent = new Agent({
    model: {
      async invoke(request) {
        return callsTurn(request.tools[0].name);
      },
    },
    tools: [
      readTool("lookup", () => ({
        content: null,
        effectState: "not_started",
        planningDisposition: "replan",
        planningReason: "host requests confirmation",
      })),
      {
        name: "protected_write",
        description: "protected write",
        inputSchema: objectSchema(),
        policy: { mode: "confirm", title: "Protected write" },
        hostManagedDurability: true,
        run() {
          protectedCalls += 1;
          return { content: null, effectState: "committed" };
        },
      },
    ],
    planning: {
      planner: {
        createPlan() { return { workPlan: toolOnlyPlan("lookup") }; },
        revisePlan() { return { workPlan: toolOnlyPlan("protected_write", "protected-step") }; },
      },
    },
  });

  await rejectsCode(agent.invoke(planned("run")), "tool_approval_unavailable");
  assert.equal(protectedCalls, 0);
});

test("a revised plan cannot replace completed history", async () => {
  const agent = new Agent({
    model: {
      async invoke(request) { return callsTurn(request.tools[0].name); },
    },
    tools: [
      readTool("lookup", () => ({
        content: null,
        effectState: "not_started",
        planningDisposition: "replan",
        planningReason: "revise",
      })),
      readTool("second"),
    ],
    planning: {
      planner: {
        createPlan() { return { workPlan: toolOnlyPlan("lookup", "same-step") }; },
        revisePlan() { return { workPlan: toolOnlyPlan("second", "same-step") }; },
      },
    },
  });

  await rejectsCode(agent.invoke(planned("run")), "invalid_replan");
});

test("staged task context is selected only after a valid TaskSpec", async () => {
  const order = [];
  let received;
  const agent = new Agent({
    model: {
      capabilities: capabilities(),
      async invoke(request) {
        received = request.messages;
        return request.tools.length === 0
          ? finalTurn("done", request)
          : callsTurn("read", request);
      },
    },
    tools: [readTool("read")],
    context: {
      strategy: "staged",
      reserves: { safetyTokens: 100, runtimeTokens: 100, minimumMessageTokens: 50 },
      provider: {
        buildContext() { throw new Error("single pass must not run"); },
        buildPlanningContext() {
          order.push("planning-context");
          return { blocks: [{ name: "outline", content: "planning facts" }] };
        },
        buildTaskContext(_request, _budget, task) {
          order.push(`task-context:${task.goal}`);
          return { blocks: [{ name: "task", content: "task facts" }] };
        },
      },
    },
    planning: {
      planner: {
        createPlan(_request, planning) {
          order.push(`planner:${planning.planningContext[0].name}`);
          return {
            workPlan: {
              ...toolOnlyPlan("read"),
              taskSpec: { goal: "inspect report" },
            },
          };
        },
      },
    },
  });

  await agent.invoke(planned("run"));
  assert.deepEqual(order, ["planning-context", "planner:outline", "task-context:inspect report"]);
  assert.equal(JSON.stringify(received).includes("task facts"), true);
  assert.equal(JSON.stringify(received).includes("planning facts"), false);
});

test("response validators withhold and repair one bounded candidate", async () => {
  let modelCalls = 0;
  const agent = new Agent({
    model: {
      async invoke() {
        modelCalls += 1;
        return finalTurn(modelCalls === 1 ? "bad" : "accepted");
      },
    },
    responseValidation: {
      validators: [{
        validate({ content }) {
          return content === "accepted"
            ? {}
            : { violationCode: "fixture.invalid", repairGuidance: "Return accepted." };
        },
      }],
      maxAttempts: 2,
    },
  });

  const result = await agent.invoke({ messages: [user("review")] });
  assert.equal(result.output, "accepted");
  assert.equal(JSON.stringify(result.messages).includes("bad"), false);
  assert.equal(JSON.stringify(result.messages).includes("Return accepted"), false);
  assert.equal(modelCalls, 2);
});

test("Planned recovery corrects one missing and one unauthorized tool call before execution", async () => {
  let modelCalls = 0;
  let currentCalls = 0;
  let futureCalls = 0;
  const agent = new Agent({
    model: {
      async invoke() {
        modelCalls += 1;
        if (modelCalls === 1) return finalTurn("omitted");
        if (modelCalls === 2) return callsTurn("future");
        if (modelCalls === 3) return callsTurn("current");
        return finalTurn("done");
      },
    },
    tools: [
      readTool("current", () => {
        currentCalls += 1;
        return { content: "current", effectState: "not_started" };
      }),
      readTool("future", () => {
        futureCalls += 1;
        return { content: "future", effectState: "not_started" };
      }),
    ],
    planning: {
      planner: { createPlan: () => ({ workPlan: toolOnlyPlan("current") }) },
    },
  });

  assert.equal((await agent.invoke(planned("run"))).output, "done");
  assert.equal(modelCalls, 4);
  assert.equal(currentCalls, 1);
  assert.equal(futureCalls, 0);
});

test("a failed Planned read step consumes recovery authority before replanning", async () => {
  let revisions = 0;
  const seenTools = [];
  const agent = new Agent({
    model: {
      async invoke(request) {
        seenTools.push(request.tools.map((tool) => tool.name));
        const selected = request.tools[0]?.name;
        return selected === undefined ? finalTurn("done") : callsTurn(selected);
      },
    },
    tools: [
      readTool("primary", () => ({
        content: null,
        effectState: "not_started",
        errorCode: "fixture_read_failed",
      })),
      readTool("fallback"),
    ],
    planning: {
      planner: {
        createPlan: () => ({ workPlan: toolOnlyPlan("primary", "primary-step") }),
        revisePlan() {
          revisions += 1;
          return { workPlan: toolOnlyPlan("fallback", "fallback-step") };
        },
      },
    },
  });

  assert.equal((await agent.invoke(planned("run"))).output, "done");
  assert.equal(revisions, 1);
  assert.deepEqual(seenTools, [["primary"], ["fallback"], []]);
});

test("compiler inserts registered prerequisites without granting them to Planner", () => {
  const schema = objectSchema();
  for (const row of planningFixture.compileCases) {
    const compiled = compileWorkPlan(toolOnlyPlan(row.planCapability), row.registrations.map((item) => ({
      runtimeName: item.runtimeName,
      runtimeSpec: { name: item.runtimeName, description: item.title, inputSchema: schema },
      ...(item.planningCapability === undefined
        ? {}
        : {
            planningCapability: {
              name: item.planningCapability,
              description: item.title,
              inputSchema: schema,
            },
          }),
      prerequisiteTools: item.prerequisiteTools,
      riskLevel: item.riskLevel,
      title: item.title,
    })));

    assert.deepEqual(
      compiled.executionPlan.steps.map((step) => step.runtimeToolNames[0]),
      row.expectedRuntimeTools,
      row.name,
    );
    assert.deepEqual(compiled.insertedToolNames, row.expectedInsertedTools, row.name);
    assert.deepEqual(compiled.loweredToolNames, row.expectedLoweredTools, row.name);
    assert.deepEqual(
      compiled.executionPlan.steps.map((step) => step.protocolPrivate),
      row.expectedPrivateSteps,
      row.name,
    );
  }
});

function weatherPlan() {
  return {
    title: "Weather",
    goal: "Get weather",
    taskSpec: { goal: "Get weather" },
    steps: [
      { id: "reason", title: "Reason", type: "analyze", executor: "model" },
      {
        id: "lookup",
        title: "Lookup",
        type: "read",
        executor: "tool",
        capabilityNames: ["weather_lookup"],
        dependsOn: ["reason"],
      },
      { id: "respond", title: "Respond", type: "review", executor: "model", dependsOn: ["lookup"] },
    ],
  };
}

function toolOnlyPlan(name, id = "tool-step") {
  return {
    title: "Plan",
    steps: [{ id, title: name, type: "read", executor: "tool", capabilityNames: [name] }],
  };
}

function twoToolPlan() {
  return {
    title: "Two tools",
    steps: [
      { id: "first-step", title: "First", type: "read", executor: "tool", capabilityNames: ["first"] },
      {
        id: "second-step",
        title: "Second",
        type: "read",
        executor: "tool",
        capabilityNames: ["second"],
        dependsOn: ["first-step"],
      },
    ],
  };
}

function plannedReadTool(name, capabilityName, run) {
  return {
    ...readTool(name, run),
    planning: {
      capability: { name: capabilityName, description: `${capabilityName} capability`, inputSchema: objectSchema() },
    },
  };
}

function readTool(name, run = () => ({ content: { ok: true }, effectState: "not_started" })) {
  return {
    name,
    description: `${name} tool`,
    inputSchema: objectSchema(),
    policy: { mode: "read", title: name },
    run,
  };
}

function user(content) {
  return { role: "user", content };
}

function planned(content) {
  return { messages: [user(content)], planningMode: "planned" };
}

function finalTurn(content, request) {
  return {
    message: { role: "assistant", content },
    finishReason: "stop",
    ...(request === undefined
      ? {}
      : { appliedOutputLimit: request.outputLimit?.maxTokens }),
  };
}

function callsTurn(name, request, content = "") {
  return {
    message: {
      role: "assistant",
      content,
      toolCalls: [{ id: `call-${name}`, name, arguments: {} }],
    },
    finishReason: "tool_calls",
    ...(request === undefined
      ? {}
      : { appliedOutputLimit: request.outputLimit?.maxTokens }),
  };
}

function callsTurnMany(names) {
  return {
    message: {
      role: "assistant",
      content: "",
      toolCalls: names.map((name, index) => ({
        id: `call-${name}-${index}`,
        name,
        arguments: {},
      })),
    },
    finishReason: "tool_calls",
  };
}

function objectSchema() {
  return { type: "object", properties: {}, additionalProperties: true };
}

function capabilities() {
  return {
    schemaVersion: 1,
    profileId: "planning-test",
    providerProtocol: "fixture",
    contextWindowTokens: 8_000,
    maxCallOutputTokens: 1_000,
    thinkingTokenAccounting: "included",
    protocol: {
      reasoningControl: "unavailable",
      reasoningReplay: "ignored",
      toolCalling: "supported",
      requiredToolChoice: "unavailable",
      parallelToolCalls: "unavailable",
      streaming: "unavailable",
      cancellation: "supported",
      assistantContentWithToolCalls: "optional",
      jsonSchemaLevel: "fixture",
      streamFinishSemantics: "fixture",
      usageSemantics: "fixture",
    },
  };
}

async function collect(iterable) {
  const result = [];
  for await (const item of iterable) result.push(item);
  return result;
}

async function rejectsCode(promise, code) {
  await assert.rejects(
    promise,
    (error) => error instanceof AgentError && error.code === code,
  );
}
