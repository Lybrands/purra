import assert from "node:assert/strict";
import test from "node:test";

import {
  Agent,
  ModelResponseJudge,
  ModelWorkPlanner,
  ToolPlanningPolicy,
} from "@lybrands/purra";

test("model Planner repairs invalid JSON inside the submitted Run before executing the plan", async () => {
  let plannerRunId;
  let plannerCalls = 0;
  let mainCalls = 0;
  let toolCalls = 0;
  const agent = new Agent({
    model: {
      capabilities: capabilities(),
      async invoke(request) {
        if (isPlannerRequest(request)) {
          plannerCalls += 1;
          return finalTurn(plannerCalls === 1 ? "not json" : JSON.stringify({
            workPlan: toolPlan("lookup", "lookup-step"),
          }));
        }
        mainCalls += 1;
        return mainCalls === 1 ? callsTurn("lookup", "lookup-call") : finalTurn("done");
      },
    },
    tools: [readTool("lookup", () => {
      toolCalls += 1;
      return { content: "fact", effectState: "not_started" };
    })],
    planning: {
      policy: new ToolPlanningPolicy(),
      plannerFactory(modelTasks) {
        plannerRunId = modelTasks.runId;
        return new ModelWorkPlanner(modelTasks, { maxRepairAttempts: 1, maxOutputTokens: 256 });
      },
    },
  });

  const handle = await agent.submit({ messages: [{ role: "user", content: "look it up" }] });
  assert.equal((await handle.result).output, "done");
  assert.equal(plannerRunId, handle.runId);
  assert.equal(plannerCalls, 2);
  assert.equal(mainCalls, 2);
  assert.equal(toolCalls, 1);
  const events = await collect(handle.events({ visibility: "all" }));
  assert.equal(events.filter((event) => event.kind === "invocation.started").length, 4);
  assert.equal(events.filter((event) => event.kind === "plan.updated").length, 1);
});

test("model Planner stops after its fixed repair budget", async () => {
  let plannerCalls = 0;
  let mainCalls = 0;
  const agent = new Agent({
    model: {
      capabilities: capabilities(),
      async invoke(request) {
        if (isPlannerRequest(request)) {
          plannerCalls += 1;
          return finalTurn("still not json");
        }
        mainCalls += 1;
        return finalTurn("unsafe");
      },
    },
    planning: {
      policy: alwaysPlanPolicy(),
      plannerFactory: (modelTasks) => new ModelWorkPlanner(modelTasks, { maxRepairAttempts: 1 }),
    },
  });

  await assert.rejects(
    agent.invoke({ messages: [{ role: "user", content: "plan" }] }),
    (error) => error?.code === "invalid_planner_output",
  );
  assert.equal(plannerCalls, 2);
  assert.equal(mainCalls, 0);
});

test("model Planner keeps direct execution inside the existing plan compiler", async () => {
  let plannerCalls = 0;
  let mainCalls = 0;
  const agent = new Agent({
    model: {
      capabilities: capabilities(),
      async invoke(request) {
        if (isPlannerRequest(request)) {
          plannerCalls += 1;
          return finalTurn(JSON.stringify({ workPlan: directPlan() }));
        }
        mainCalls += 1;
        assert.equal(request.tools.length, 0);
        return finalTurn("direct answer");
      },
    },
    planning: {
      policy: alwaysPlanPolicy(),
      plannerFactory: (modelTasks) => new ModelWorkPlanner(modelTasks),
    },
  });

  assert.equal((await agent.invoke({ messages: [{ role: "user", content: "answer" }] })).output, "direct answer");
  assert.equal(plannerCalls, 1);
  assert.equal(mainCalls, 1);
});

test("model Planner revisions cannot bypass current tool authority", async () => {
  let plannerCalls = 0;
  const mainTools = [];
  const agent = new Agent({
    model: {
      capabilities: capabilities(),
      async invoke(request) {
        if (isPlannerRequest(request)) {
          plannerCalls += 1;
          const name = plannerCalls === 1 ? "lookup" : "summarize";
          return finalTurn(JSON.stringify({ workPlan: toolPlan(name, `${name}-step`) }));
        }
        const name = request.tools[0]?.name;
        mainTools.push(name ?? null);
        return name === undefined ? finalTurn("revised answer") : callsTurn(name, `${name}-call`);
      },
    },
    tools: [
      readTool("lookup", () => ({
        content: "changed",
        effectState: "not_started",
        planningDisposition: "replan",
        planningReason: "result shape changed",
      })),
      readTool("summarize"),
    ],
    planning: {
      policy: new ToolPlanningPolicy(),
      plannerFactory: (modelTasks) => new ModelWorkPlanner(modelTasks),
    },
  });

  assert.equal((await agent.invoke({ messages: [{ role: "user", content: "revise" }] })).output, "revised answer");
  assert.equal(plannerCalls, 2);
  assert.deepEqual(mainTools, ["lookup", "summarize", null]);
});

test("model response judge withholds and repairs a candidate under the same submitted Run", async () => {
  let mainCalls = 0;
  let judgeCalls = 0;
  let judgeRunId;
  const agent = new Agent({
    model: {
      capabilities: capabilities(),
      async invoke(request) {
        if (String(request.messages[0]?.content).startsWith("judge:")) {
          judgeCalls += 1;
          return finalTurn(judgeCalls === 1 ? "reject" : "accept");
        }
        mainCalls += 1;
        return finalTurn(mainCalls === 1 ? "bad candidate" : "good candidate");
      },
    },
    responseValidation: {
      judgeFactories: [(modelTasks) => {
        judgeRunId = modelTasks.runId;
        return new ModelResponseJudge(modelTasks, {
          buildMessages({ content }) {
            return [{ role: "user", content: `judge:${String(content)}` }];
          },
          evaluate({ judgmentContent }) {
            return judgmentContent === "accept"
              ? {}
              : { violationCode: "semantic_rejection", repairGuidance: "Return an acceptable answer." };
          },
        });
      }],
      maxAttempts: 2,
    },
  });

  const handle = await agent.submit({ messages: [{ role: "user", content: "answer" }] });
  assert.equal((await handle.result).output, "good candidate");
  assert.equal(judgeRunId, handle.runId);
  assert.equal(mainCalls, 2);
  assert.equal(judgeCalls, 2);
  const events = await collect(handle.events({ visibility: "all" }));
  assert.equal(events.filter((event) => event.kind === "invocation.started").length, 4);
  assert.equal(events.some((event) => (
    event.kind === "agentRunTrace"
    && event.payload.details?.cause === "response_constraint_semantic"
  )), true);
});

function isPlannerRequest(request) {
  return request.messages.some((message) => message.attributes?.planningContract === true);
}

function alwaysPlanPolicy() {
  return {
    shouldPlan() { return true; },
    planningConstraints() { return { maxSteps: 4 }; },
  };
}

function toolPlan(name, id) {
  return {
    title: "Tool plan",
    steps: [{
      id,
      title: name,
      type: "read",
      executor: "tool",
      capabilityNames: [name],
    }],
  };
}

function directPlan() {
  return {
    title: "Direct response",
    steps: [{ id: "respond", title: "Respond", type: "review", executor: "model" }],
  };
}

function readTool(name, run = () => ({ content: "ok", effectState: "not_started" })) {
  return {
    name,
    description: name,
    inputSchema: { type: "object", properties: {}, additionalProperties: false },
    policy: { mode: "read", title: name },
    run,
  };
}

function callsTurn(name, id) {
  return {
    message: {
      role: "assistant",
      content: "",
      toolCalls: [{ id, name, arguments: {} }],
    },
    finishReason: "tool_calls",
  };
}

function finalTurn(content) {
  return { message: { role: "assistant", content }, finishReason: "stop" };
}

function capabilities() {
  return {
    schemaVersion: 1,
    profileId: "reference-planning-fixture",
    providerProtocol: "custom",
    contextWindowTokens: 16_000,
    maxOutputTokens: 512,
    thinkingTokenAccounting: "unknown",
    protocol: {
      reasoningControl: "selectable",
      reasoningReplay: "ignored",
      toolCalling: "supported",
      requiredToolChoice: "supported",
      parallelToolCalls: "supported",
      streaming: "unavailable",
      cancellation: "supported",
      assistantContentWithToolCalls: "optional",
      jsonSchemaLevel: "unknown",
      streamFinishSemantics: "normalized",
      usageSemantics: "normalized",
    },
  };
}

async function collect(iterable) {
  const values = [];
  for await (const value of iterable) values.push(value);
  return values;
}
