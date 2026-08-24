import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  Agent,
  AgentCanceledError,
  DelegationCoordinator,
  DelegationPolicy,
  InMemoryDelegationRepository,
} from "../dist/index.js";

const fixture = JSON.parse(await readFile(
  new URL("../../conformance/fixtures/delegation_protocol.json", import.meta.url),
  "utf8",
));

test("shared delegation policy and aggregation stay aligned with Python", async () => {
  assert.deepEqual(new DelegationPolicy().snapshot(), fixture.policy);
  for (const scenario of fixture.aggregationCases) {
    let sequence = 0;
    const repository = new InMemoryDelegationRepository(() => `delegation-${++sequence}`);
    const receipt = await repository.createBatch({
      runId: "run-fixture",
      batchId: scenario.name,
      idempotencyKey: scenario.name,
      delegations: scenario.rows.map((row) => ({
        agentName: row.agentName,
        title: row.agentName,
        instruction: "Perform the isolated fixture task.",
        objective: "fixture objective",
        required: row.required,
      })),
    });
    for (const [index, fixtureRow] of scenario.rows.entries()) {
      const row = receipt.delegations[index];
      if (fixtureRow.status === "running" || fixtureRow.status === "done") {
        await repository.start(row.id, row.runId, row.batchId);
      }
      if (fixtureRow.status === "done") {
        await repository.complete(row.id, row.runId, row.batchId, fixtureRow.summary ?? "");
      } else if (fixtureRow.status === "failed") {
        await repository.fail(row.id, row.runId, row.batchId, "fixture_failed");
      } else if (fixtureRow.status === "canceled") {
        await repository.cancel(row.id, row.runId, row.batchId, "fixture_canceled");
      }
    }
    const aggregate = await repository.aggregateBatch("run-fixture", scenario.name);
    assert.equal(aggregate.state, scenario.state, scenario.name);
    assert.equal(aggregate.requiredFailures.length, scenario.requiredFailureCount, scenario.name);
    assert.deepEqual(
      aggregate.results.map((result) => result.agentName),
      scenario.resultAgentNames,
      scenario.name,
    );
  }
});

test("delegation coordinator bounds concurrency, attributes results, and replays once", async () => {
  let sequence = 0;
  let active = 0;
  let maximum = 0;
  let executions = 0;
  const events = [];
  const repository = new InMemoryDelegationRepository(() => `delegation-${++sequence}`);
  const coordinator = new DelegationCoordinator({
    repository,
    policy: new DelegationPolicy({ maxAgentsPerCall: 3, maxParallel: 2 }),
    executor: {
      async execute(request) {
        executions += 1;
        active += 1;
        maximum = Math.max(maximum, active);
        await new Promise((resolve) => setTimeout(resolve, 5));
        active -= 1;
        return { outcome: "completed", content: `result:${request.agentName}` };
      },
    },
  });
  coordinator.bindRun("run-root", (event) => { events.push(event); });
  const definitions = ["facts", "risks", "review"].map((agentName) => ({
    agentName,
    title: agentName,
    instruction: `Act as ${agentName}.`,
    objective: `Complete ${agentName}.`,
  }));

  const first = await coordinator.executeCall({
    runId: "run-root",
    idempotencyKey: "call-1",
    delegations: definitions,
  });
  const replay = await coordinator.executeCall({
    runId: "run-root",
    idempotencyKey: "call-1",
    delegations: definitions,
  });

  assert.equal(maximum, 2);
  assert.equal(executions, 3);
  assert.equal(first.state, "ready");
  assert.deepEqual(replay, first);
  assert.deepEqual(first.results.map((result) => result.agentName), ["facts", "risks", "review"]);
  assert.equal(events.filter((event) => event.status === "queued").length, 3);
  assert.equal(events.filter((event) => event.status === "done").length, 3);
  await assert.rejects(
    coordinator.executeCall({
      runId: "run-root",
      idempotencyKey: "call-1",
      delegations: [{ ...definitions[0], objective: "different" }],
    }),
    (error) => error?.code === "delegation_idempotency_conflict",
  );
  const row = (await repository.listForRun("run-root"))[0];
  await assert.rejects(
    repository.start(row.id, "another-run", row.batchId),
    (error) => error?.code === "delegation_scope_violation",
  );
});

test("canceling a delegation batch aborts only its active execution", async () => {
  let sequence = 0;
  let started;
  const ready = new Promise((resolve) => { started = resolve; });
  const repository = new InMemoryDelegationRepository(() => `delegation-${++sequence}`);
  const coordinator = new DelegationCoordinator({
    repository,
    policy: new DelegationPolicy({ maxAgentsPerCall: 1, maxParallel: 1 }),
    executor: {
      async execute(_request, signal) {
        started();
        await new Promise((resolve, reject) => {
          signal.addEventListener("abort", () => reject(new AgentCanceledError()), { once: true });
        });
      },
    },
  });
  coordinator.bindRun("run-root", () => undefined);
  const execution = coordinator.executeCall({
    runId: "run-root",
    idempotencyKey: "cancel-call",
    delegations: [{
      agentName: "waiter",
      title: "Waiter",
      instruction: "Wait until canceled.",
      objective: "wait",
    }],
  });
  await ready;
  assert.equal(await coordinator.cancelBatch("run-root", "delegation-batch:cancel-call"), 1);
  const aggregate = await execution;
  assert.equal(aggregate.state, "blocked");
  assert.equal((await repository.listForRun("run-root"))[0].status, "canceled");
});

test("Agent delegation stays in one Root Run with isolated context and read tools", async () => {
  const delegatedRequests = [];
  let rootRound = 0;
  let delegatedRound = 0;
  const model = {
    async invoke(request) {
      const delegated = request.messages[0]?.attributes?.delegatedAgentDefinition === "model";
      if (delegated) {
        delegatedRequests.push(request);
        delegatedRound += 1;
        assert.deepEqual(request.tools.map((tool) => tool.name), ["lookup"]);
        assert.equal(JSON.stringify(request.messages).includes("parent-private-secret"), false);
        if (delegatedRound === 1) {
          return {
            message: {
              role: "assistant",
              content: "",
              toolCalls: [{ id: "delegate-lookup", name: "lookup", arguments: { key: "facts" } }],
            },
            finishReason: "tool_calls",
          };
        }
        return {
          message: { role: "assistant", content: "delegated result" },
          finishReason: "stop",
        };
      }
      rootRound += 1;
      if (rootRound === 1) {
        assert.deepEqual(request.tools.map((tool) => tool.name), [
          "lookup",
          "mutate",
          "delegateToAgents",
        ]);
        return {
          message: {
            role: "assistant",
            content: "",
            toolCalls: [{
              id: "delegate-call",
              name: "delegateToAgents",
              arguments: {
                delegations: [{
                  agentName: "researcher",
                  title: "Researcher",
                  instruction: "Use read evidence only.",
                  objective: "collect facts",
                }],
              },
            }],
          },
          finishReason: "tool_calls",
        };
      }
      return { message: { role: "assistant", content: "root result" }, finishReason: "stop" };
    },
  };
  const handle = await new Agent({
    model,
    tools: [{
      name: "lookup",
      description: "Read evidence",
      inputSchema: {
        type: "object",
        properties: { key: { type: "string" } },
        required: ["key"],
        additionalProperties: false,
      },
      policy: { mode: "read", title: "Lookup" },
      run() { return { content: { found: true }, effectState: "not_started" }; },
    }, {
      name: "mutate",
      description: "Write evidence",
      inputSchema: { type: "object", additionalProperties: false },
      policy: { mode: "propose", title: "Mutate" },
      hostManagedDurability: true,
      run() { return { content: { changed: true }, effectState: "committed" }; },
    }],
    delegation: { policy: { maxAgentsPerCall: 1, maxParallel: 1 } },
  }).submit({
    messages: [{ role: "user", content: "parent-private-secret" }],
    enabledTools: ["lookup", "mutate", "delegateToAgents"],
  });
  assert.equal((await handle.result).output, "root result");
  const events = [];
  for await (const event of handle.events({ visibility: "all" })) events.push(event);

  assert.equal(delegatedRequests.length, 2);
  assert.deepEqual(
    events.filter((event) => event.kind === "delegation.status").map((event) => event.payload.status),
    ["queued", "running", "done"],
  );
  assert.equal(events.filter((event) => event.kind === "invocation.started").length, 4);
  assert.equal(events.every((event) => event.runId === handle.runId), true);
  assert.equal(events.some((event) => event.payload.delegationId !== undefined), true);
});

test("delegated Agents resolve managed context factories inside the same Root Run", async () => {
  const factoryRunIds = [];
  let rootRound = 0;
  const model = {
    capabilities: capabilities(),
    async invoke(request) {
      if (request.messages[0]?.content === "derive context") {
        return { message: { role: "assistant", content: "derived fact" }, finishReason: "stop" };
      }
      if (request.messages[0]?.attributes?.delegatedAgentDefinition === "model") {
        return { message: { role: "assistant", content: "delegated result" }, finishReason: "stop" };
      }
      rootRound += 1;
      return rootRound === 1
        ? {
            message: {
              role: "assistant",
              content: "",
              toolCalls: [{
                id: "managed-delegate",
                name: "delegateToAgents",
                arguments: { delegations: [{
                  agentName: "researcher",
                  title: "Researcher",
                  instruction: "Use managed context.",
                  objective: "research",
                }] },
              }],
            },
            finishReason: "tool_calls",
          }
        : { message: { role: "assistant", content: "root result" }, finishReason: "stop" };
    },
  };
  const handle = await new Agent({
    model,
    context: {
      claims: [{ name: "derived", desiredTokens: 64 }],
      providerFactory(modelTasks) {
        factoryRunIds.push(modelTasks.runId);
        return {
          async buildContext() {
            const result = await modelTasks.complete([{ role: "user", content: "derive context" }]);
            return { blocks: [{ name: "derived", content: result.turn.message.content }] };
          },
        };
      },
    },
    delegation: { policy: { maxAgentsPerCall: 1, maxParallel: 1 } },
  }).submit({
    messages: [{ role: "user", content: "delegate" }],
    enabledTools: ["delegateToAgents"],
  });

  assert.equal((await handle.result).output, "root result");
  assert.deepEqual(factoryRunIds, [handle.runId, handle.runId]);
  const events = [];
  for await (const event of handle.events({ visibility: "all" })) events.push(event);
  assert.equal(events.filter((event) => event.kind === "invocation.started").length, 5);
});

test("custom delegated executors receive only enabled read-tool names", async () => {
  let round = 0;
  let delegatedTools;
  const handle = await new Agent({
    model: {
      async invoke() {
        round += 1;
        return round === 1
          ? {
              message: {
                role: "assistant",
                content: "",
                toolCalls: [{
                  id: "custom-delegate",
                  name: "delegateToAgents",
                  arguments: { delegations: [{
                    agentName: "custom",
                    title: "Custom",
                    instruction: "Inspect bounded authority.",
                    objective: "inspect",
                  }] },
                }],
              },
              finishReason: "tool_calls",
            }
          : { message: { role: "assistant", content: "done" }, finishReason: "stop" };
      },
    },
    tools: [{
      name: "read",
      description: "Read",
      inputSchema: { type: "object", additionalProperties: false },
      policy: { mode: "read", title: "Read" },
      run() { return { content: null, effectState: "not_started" }; },
    }, {
      name: "write",
      description: "Write",
      inputSchema: { type: "object", additionalProperties: false },
      policy: { mode: "propose", title: "Write" },
      hostManagedDurability: true,
      run() { return { content: null, effectState: "committed" }; },
    }],
    delegation: {
      policy: { maxAgentsPerCall: 1, maxParallel: 1 },
      executor: {
        async execute(request) {
          delegatedTools = request.enabledTools;
          return { outcome: "completed", content: "bounded" };
        },
      },
    },
  }).submit({
    messages: [{ role: "user", content: "delegate" }],
    enabledTools: ["read", "write", "delegateToAgents"],
  });
  await handle.result;
  assert.deepEqual(delegatedTools, ["read"]);
});

function capabilities() {
  return {
    schemaVersion: 1,
    profileId: "delegation-context-fixture",
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
