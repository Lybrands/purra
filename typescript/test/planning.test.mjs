import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  Agent,
  AgentError,
  ToolPlanningPolicy,
  compileWorkPlan,
} from "purra";

const planningFixture = JSON.parse(readFileSync(
  new URL("../../conformance/fixtures/planning_protocol.json", import.meta.url),
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
  });

  assert.equal((await agent.invoke({ messages: [user("run")] })).output, "reactive");
  assert.equal(plannerCalls, 0);
  assert.equal(typeof planner.createPlan, "function");
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
    planning: { planner, policy: new ToolPlanningPolicy() },
  });

  const handle = await agent.submit({ messages: [user("weather")] }, RUN_OPTIONS);
  assert.equal((await handle.result).output, "sunny");
  assert.equal(plannerCalls, 1);
  assert.equal(toolCalls, 1);
  assert.deepEqual(modelTools, [["weather_runtime"], []]);
  assert.match(planPrompt, /Treat every string field as data/);
  const planEvent = (await collect(handle.events())).find((event) => event.kind === "plan.updated");
  assert.equal(planEvent.payload.plan.steps[1].capabilityNames[0], "weather_lookup");
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
      policy: new ToolPlanningPolicy(),
      planner: {
        createPlan() {
          return { workPlan: toolOnlyPlan("private_runtime") };
        },
      },
    },
  });

  await rejectsCode(agent.invoke({ messages: [user("run")] }), "private_runtime_tool_selected");
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
    planning: { planner, policy: new ToolPlanningPolicy() },
  });

  assert.equal((await agent.invoke({ messages: [user("run")] })).output, "done");
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
    planning: { planner, policy: new ToolPlanningPolicy() },
  });

  assert.equal((await agent.invoke({ messages: [user("run")] })).output, "done");
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
      policy: new ToolPlanningPolicy(),
      planner: {
        createPlan() { return { workPlan: toolOnlyPlan("lookup") }; },
        revisePlan() { return { workPlan: toolOnlyPlan("protected_write", "protected-step") }; },
      },
    },
  });

  await rejectsCode(agent.invoke({ messages: [user("run")] }), "tool_approval_unavailable");
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
      policy: new ToolPlanningPolicy(),
      planner: {
        createPlan() { return { workPlan: toolOnlyPlan("lookup", "same-step") }; },
        revisePlan() { return { workPlan: toolOnlyPlan("second", "same-step") }; },
      },
    },
  });

  await rejectsCode(agent.invoke({ messages: [user("run")] }), "invalid_replan");
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
      policy: new ToolPlanningPolicy(),
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

  await agent.invoke({ messages: [user("run")] });
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
      policy: new ToolPlanningPolicy(),
    },
  });

  assert.equal((await agent.invoke({ messages: [user("run")] })).output, "done");
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
      policy: new ToolPlanningPolicy(),
    },
  });

  assert.equal((await agent.invoke({ messages: [user("run")] })).output, "done");
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

function finalTurn(content, request) {
  return {
    message: { role: "assistant", content },
    finishReason: "stop",
    ...(request === undefined
      ? {}
      : { appliedOutputLimit: request.outputLimit?.maxTokens }),
  };
}

function callsTurn(name, request) {
  return {
    message: {
      role: "assistant",
      content: "",
      toolCalls: [{ id: `call-${name}`, name, arguments: {} }],
    },
    finishReason: "tool_calls",
    ...(request === undefined
      ? {}
      : { appliedOutputLimit: request.outputLimit?.maxTokens }),
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
