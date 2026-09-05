import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { PlanningStreamParser } from "purra";
import { Agent, AgentError, AgentOperationController, ModelWorkPlanner, InMemoryAgentAdapters, ModelTaskRunner } from "purra";

const capabilities = {
  schemaVersion: 2, profileId: "planning-stream-test", providerProtocol: "custom",
  contextWindowTokens: 16000, maxGenerationTokens: 512, thinkingTokenAccounting: "unknown",
  protocol: { reasoningControl: "selectable", reasoningReplay: "ignored", toolCalling: "supported",
    requiredToolChoice: "supported", parallelToolCalls: "supported", streaming: "supported", cancellation: "supported",
    assistantContentWithToolCalls: "optional", jsonSchemaLevel: "unknown", streamFinishSemantics: "normalized", usageSemantics: "normalized" },
};
const plan = { workPlan: { title: "Plan", goal: "PRIVATE_PLAN_MARKER", steps: [
  { id: "answer", title: "Answer", type: "review", executor: "model" },
  { id: "verify", title: "Verify", type: "analyze", executor: "model", dependsOn: ["answer"] },
  { id: "deliver", title: "Deliver", type: "write", executor: "model", dependsOn: ["verify"] },
] } };
const wire = (text) => JSON.stringify(text === undefined ? { v: 1, type: "plan", plan } : { v: 1, type: "progress", text }) + "\n";
const runOptions = { budgets: { maxRunGenerationTokens: null } };
const input = { messages: [{ role: "user", content: "prepare" }], planningMode: "planned" };
const collect = async (stream) => { const events = []; for await (const event of stream) events.push(event); return events; };
const deferred = () => { let resolve; const promise = new Promise((done) => { resolve = done; }); return { promise, resolve }; };

function managed(scripts, options = {}, modelCapabilities = capabilities) {
  const adapters = options.adapters ?? new InMemoryAgentAdapters();
  const state = { opened: 0, closed: 0, planningCalls: 0, executions: 0, planningRequests: [] };
  const model = {
    capabilities: modelCapabilities,
    async invoke() { throw new Error("must use streams"); },
    stream(request, signal) {
      const planning = request.messages.some((message) => message.attributes?.planningContract);
      state.opened += 1;
      if (planning) { state.planningCalls += 1; state.planningRequests.push(request); }
      else state.executions += 1;
      const entry = planning ? scripts.shift() : ["done"];
      const script = typeof entry === "function" ? entry(request) : entry;
      let index = 0;
      let closed = false;
      let cancel;
      const canceled = new Promise((_, reject) => {
        cancel = () => reject(new Error("adapter canceled"));
        signal?.addEventListener("abort", cancel, { once: true });
      });
      void canceled.catch(() => undefined);
      return {
        appliedGenerationLimit: request.outputBudget.maxGenerationTokens,
        activitySupport: "transport",
        [Symbol.asyncIterator]() { return this; },
        async next() {
          if (closed) return { done: true };
          const item = script[index++];
          if (item?.promise) { await Promise.race([item.promise, canceled]); return this.next(); }
          if (item instanceof Error) throw item;
          if (item === undefined) return { value: { finishReason: "stop", usage: { inputTokens: 10, generationTokens: 8 } } };
          return { value: typeof item === "string" ? { contentDelta: item } : item };
        },
        async return() {
          if (!closed) { closed = true; state.closed += 1; signal?.removeEventListener("abort", cancel); }
          return { done: true };
        },
      };
    },
  };
  const agent = new Agent({ model, runRepository: adapters.runs, outputPublisher: adapters.outputs, ...options,
    planning: options.planning ?? { plannerFactory: (tasks) => new ModelWorkPlanner(tasks) },
  });
  return { agent, adapters, state, model };
}

const fixture = JSON.parse(readFileSync(new URL("../../conformance/fixtures/planning_stream.json", import.meta.url), "utf8"));

for (const row of fixture.valid) test(`planning stream boundaries: ${row.name}`, () => {
  for (let boundary = 0; boundary <= row.wire.length; boundary += 1) {
    const parser = new PlanningStreamParser();
    const records = [...parser.feed(row.wire.slice(0, boundary)), ...parser.feed(row.wire.slice(boundary))];
    assert.deepEqual(records.map((record) => record.text), row.texts);
    assert.deepEqual(parser.finish(), row.plan);
    assert.equal(parser.rejectedProgressRecords, row.rejectedProgressRecords ?? 0);
    for (const record of records) {
      assert.equal(JSON.parse(Buffer.from(row.wire).subarray(record.sourceStart, record.sourceEnd).toString()).text, record.text);
    }
  }
});

for (const row of fixture.invalid) test(`planning stream rejects: ${row.name}`, () => {
  const parser = new PlanningStreamParser();
  assert.throws(() => {
    for (const char of row.wire) parser.feed(char);
    parser.finish();
  }, (error) => error.code === "invalid_planning_stream");
});

test("public planning is committed before plan arrival, replayable, and private data never leaks", async () => {
  const gate = deferred();
  const { agent, adapters, state } = managed([[{ reasoningDelta: "PRIVATE_REASONING" }, wire("准备核对证据。"), gate, wire().slice(0, -1)]]);
  const handle = await agent.submit(input, runOptions);
  const seen = [];
  for await (const event of handle.events()) {
    seen.push(event);
    if (event.kind === "planning.progress") {
      assert.equal(state.executions, 0);
      assert.ok((await adapters.runs.listEvents(handle.runId, 0)).some((stored) => stored.eventId === event.eventId));
      assert.equal(event.payload.source, "provider");
      gate.resolve();
    }
  }
  assert.equal((await handle.result).output, "done");
  assert.deepEqual(await collect(handle.events()), seen);
  assert.doesNotMatch(JSON.stringify(seen), /PRIVATE_REASONING|PRIVATE_PLAN_MARKER|capabilityNames/);
  const phases = seen.filter((e) => e.kind === "operation.started" && e.payload.kind === "planning");
  assert.equal(phases.length, 1);
  const terminals = seen.filter((e) => e.kind === "operation.finished" && e.payload.operationId === phases[0].payload.operationId);
  assert.equal(terminals.length, 1);
  assert.equal(terminals[0].payload.status, "succeeded");
  const diagnostic = (await collect(handle.events({ visibility: "all" }))).find((e) => e.kind === "model.diagnostics");
  assert.ok(diagnostic.payload.firstPublicProgressMs <= diagnostic.payload.planReceivedMs);
  assert.equal(diagnostic.payload.httpFirstByteAtMs, null);
  assert.equal(state.closed, state.opened);
});

test("invalid optional progress is omitted without repairing a valid plan", async () => {
  const invalidProgress = JSON.stringify({ v: 1, type: "progress", text: "first\nsecond" }) + "\n";
  const { agent, state } = managed([[invalidProgress, wire()]]);
  const handle = await agent.submit(input, runOptions);
  await handle.result;
  const events = await collect(handle.events({ visibility: "all" }));
  assert.equal(events.filter((event) => event.kind === "planning.progress").length, 0);
  const diagnostics = events.find((event) => event.kind === "model.diagnostics"
    && Object.hasOwn(event.payload, "rejectedPublicProgressRecords"));
  assert.equal(diagnostics.payload.rejectedPublicProgressRecords, 1);
  assert.equal(diagnostics.payload.firstPublicProgressMs, null);
  assert.equal(state.planningCalls, 1);
});

test("repair preserves failed attempt progress and one shared stage without duplicate billing", async () => {
  const { agent, state } = managed([[wire("准备检查范围。")], [wire("准备调整范围。"), wire()]]);
  const handle = await agent.submit(input, runOptions);
  await handle.result;
  const events = await collect(handle.events());
  const progress = events.filter((e) => e.kind === "planning.progress");
  assert.deepEqual(progress.map((e) => e.payload.attempt), [0, 1]);
  assert.equal(new Set(progress.map((e) => e.payload.operationId)).size, 1);
  assert.equal(new Set(progress.map((e) => e.payload.invocationId)).size, 2);
  assert.equal((await handle.snapshot()).usage.modelAttempts, 3);
  assert.equal((await handle.snapshot()).usage.generationTokens, 24);
  assert.equal(state.opened, state.closed);
});

for (const failure of ["envelope", "step type"]) test(`repair uses the rejected ${failure} output and retains valid fields`, async () => {
  const original = structuredClone(plan);
  original.workPlan.taskSpec = { goal: "PRIVATE_TASK", operation: "create", target: { scope: { count: 3 } } };
  const rejected = structuredClone(original);
  if (failure === "step type") rejected.workPlan.steps[0].type = "unknown";
  const rejectedWire = wire("准备核对范围。") + JSON.stringify(failure === "envelope" ? rejected : { v: 1, type: "plan", plan: rejected }) + "\n";
  const { agent, state } = managed([
    [{ reasoningDelta: "PRIVATE_REASONING" }, rejectedWire],
    (request) => {
      const previous = request.messages.at(-2);
      assert.deepEqual(previous, { role: "assistant", content: rejectedWire });
      assert.match(request.messages.at(-1).content, failure === "envelope" ? /unsupported version or envelope/ : /invalid type/);
      const parsed = JSON.parse(previous.content.trim().split("\n").at(-1));
      const repaired = failure === "envelope" ? parsed : parsed.plan;
      if (failure === "step type") repaired.workPlan.steps[0].type = "review";
      assert.deepEqual(repaired, original);
      return [JSON.stringify({ v: 1, type: "plan", plan: repaired }) + "\n"];
    },
  ]);
  const handle = await agent.submit(input, runOptions);
  assert.equal((await handle.result).output, "done");
  assert.equal(state.planningCalls, 2);
  assert.deepEqual(state.planningRequests[1].messages.slice(0, -2), state.planningRequests[0].messages);
  assert.doesNotMatch(JSON.stringify(state.planningRequests), /PRIVATE_REASONING/);
  const events = await collect(handle.events());
  assert.doesNotMatch(JSON.stringify(events), /PRIVATE_PLAN_MARKER|PRIVATE_TASK|PRIVATE_REASONING/);
  const diagnostics = (await collect(handle.events({ visibility: "all" }))).filter((event) => event.kind === "model.diagnostics");
  assert.doesNotMatch(JSON.stringify(diagnostics), /PRIVATE_PLAN_MARKER|PRIVATE_TASK|PRIVATE_REASONING/);
  assert.equal(state.opened, state.closed);
});

test("repair evidence is bounded by Unicode characters and replaces earlier attempts", async () => {
  const { agent, state } = managed([
    ["😀".repeat(65_535) + "\ud83d", "\ude00" + "OMITTED_PRIVATE", { reasoningDelta: "PRIVATE_REASONING" }],
    ["SECOND_PRIVATE"],
    [wire()],
  ], {
    planning: { plannerFactory: (tasks) => new ModelWorkPlanner(tasks, { maxRepairAttempts: 2 }) },
  }, { ...capabilities, contextWindowTokens: 500_000 });
  const handle = await agent.submit(input, runOptions);
  await handle.result;
  assert.equal(state.planningCalls, 3);
  const [first, second, third] = state.planningRequests;
  assert.deepEqual(second.messages.at(-2), { role: "assistant", content: "😀".repeat(65_536) });
  assert.equal([...second.messages.at(-2).content].length, 65_536);
  assert.deepEqual(third.messages.at(-2), { role: "assistant", content: "SECOND_PRIVATE" });
  assert.deepEqual(second.messages.slice(0, -2), first.messages);
  assert.deepEqual(third.messages.slice(0, -2), first.messages);
  assert.doesNotMatch(JSON.stringify([second, third]), /OMITTED_PRIVATE|PRIVATE_REASONING/);
  assert.doesNotMatch(JSON.stringify(await collect(handle.events())), /SECOND_PRIVATE|OMITTED_PRIVATE|PRIVATE_REASONING|😀/);
});

test("rejected Planner content is absent from serialized terminal errors", async () => {
  const { agent, state } = managed([["PRIVATE_INVALID_OUTPUT\n"], ["PRIVATE_INVALID_OUTPUT\n"]]);
  const handle = await agent.submit(input, runOptions);
  await assert.rejects(handle.result, (error) => {
    assert.equal(error.code, "invalid_planning_stream");
    assert.doesNotMatch(String(error), /PRIVATE_INVALID_OUTPUT/);
    assert.doesNotMatch(JSON.stringify(error), /PRIVATE_INVALID_OUTPUT/);
    assert.doesNotMatch(JSON.stringify({ ...error }), /PRIVATE_INVALID_OUTPUT/);
    return true;
  });
  assert.equal(state.planningCalls, 2);
  assert.doesNotMatch(JSON.stringify(await collect(handle.events())), /PRIVATE_INVALID_OUTPUT/);
});

for (const boundary of ["invocation", "operation"]) test(`failed planning ${boundary} persistence prevents repair`, async () => {
  const { model, state } = managed([["PRIVATE_INVALID_OUTPUT\n"], [wire()]]);
  const persistenceError = new AgentError("output_persistence_failed", "Could not persist termination");
  const tasks = new ModelTaskRunner({
    model, runId: "settlement-failure",
    authority: {
      runId: "settlement-failure",
      async openInvocation() { return { invocationId: "invocation" }; },
      async persistChunk() {},
      async persistCompletion() {},
      async settleInvocation() { if (boundary === "invocation") throw persistenceError; },
      async publishRecoveryDecision() {},
    },
    operations: new AgentOperationController({
      acceptOperationEvent(event) {
        if (boundary === "operation" && event.type === "operation.finished") throw persistenceError;
      },
    }),
  });
  const planner = new ModelWorkPlanner(tasks);
  await assert.rejects(planner.createPlan(input, { availableTools: [], planningContext: [], constraints: {} }), (error) => {
    assert.equal(state.planningCalls, 1);
    assert.equal(error.code, "output_persistence_failed");
    assert.ok(error.cause instanceof AggregateError);
    assert.equal(error.cause.errors[0].code, "invalid_planning_stream");
    assert.equal(error.cause.errors[1], persistenceError);
    return true;
  });
  assert.equal(state.planningCalls, 1);
  assert.equal(state.opened, state.closed);
});

for (const position of ["before", "during", "repair"]) test(`planning cancellation: ${position}`, async () => {
  const gate = deferred();
  const scripts = position === "repair" ? [[wire("准备核对。")], [gate, wire("迟到说明"), wire()]]
    : [[...(position === "during" ? [wire("准备核对。")] : []), gate, wire("迟到说明"), wire()]];
  const { agent, state } = managed(scripts);
  const handle = await agent.submit(input, runOptions);
  const result = assert.rejects(handle.result);
  for (let i = 0; i < 200 && state.planningCalls < (position === "repair" ? 2 : 1); i += 1) {
    await new Promise((done) => setTimeout(done, 1));
  }
  await handle.cancel();
  await result;
  const before = await collect(handle.events());
  gate.resolve();
  await new Promise((done) => setTimeout(done, 1));
  assert.deepEqual(await collect(handle.events()), before);
  assert.doesNotMatch(JSON.stringify(before), /迟到说明/);
  assert.equal(state.opened, state.closed);
  const phases = before.filter((e) => e.kind === "operation.started" && e.payload.kind === "planning");
  assert.equal(before.filter((e) => e.kind === "operation.finished" && e.payload.operationId === phases[0].payload.operationId).length, 1);
});

for (const script of [[{ reasoningDelta: "SECRET" }], [wire("只有意图。")], [wire() + wire()], ["unknown\n"]]) {
  test(`incomplete or invalid planning is bounded and never executes: ${JSON.stringify(script).slice(0, 35)}`, async () => {
    const { agent, state } = managed([script, script]);
    const handle = await agent.submit(input, runOptions);
    await assert.rejects(handle.result);
    assert.equal(state.planningCalls, 2);
    assert.equal(state.executions, 0);
    assert.equal(state.closed, state.opened);
    assert.doesNotMatch(JSON.stringify(await collect(handle.events())), /SECRET/);
  });
}

test("non-streaming gateways fail explicitly without fake realtime progress", async () => {
  let calls = 0;
  const tasks = new ModelTaskRunner({ runId: "standalone", model: { capabilities,
    async invoke() { calls += 1; throw new Error(); } } });
  await assert.rejects(tasks.plan([]), (error) => error.code === "model_stream_unavailable");
  assert.equal(calls, 0);
});

test("planning projections reject forgery, raw promotion, cross-Run delivery and terminal appends", async () => {
  const gate = deferred();
  const { agent, adapters } = managed([[wire("准备检查。"), gate, wire()], [wire()]]);
  const first = await agent.submit(input, runOptions);
  try {
    let event;
    for await (const item of first.events()) { if (item.kind === "planning.progress") { event = item; break; } }
    const draft = { sourceKey: event.sourceKey, kind: event.kind, channel: event.channel, visibility: event.visibility, payload: event.payload };
    assert.equal((await adapters.runs.appendEvent(first.runId, draft)).eventId, event.eventId);
    await assert.rejects(adapters.runs.appendEvent(first.runId, { ...draft, payload: { ...draft.payload, text: "forged" } }));
    await assert.rejects(adapters.runs.appendEvent(first.runId, { ...draft, sourceKey: `planning:${draft.payload.invocationId}:2`, payload: { ...draft.payload, recordIndex: 2, sourceStart: 1 } }));
    const raw = (await adapters.runs.listEvents(first.runId, 0)).find((e) => e.kind === "provider.delta_batch");
    await assert.rejects(adapters.runs.appendEvent(first.runId, { ...raw, sourceKey: "promote-raw", visibility: "public" }));
    const { invocationId: _invocation, ...unattributed } = raw.payload;
    await assert.rejects(adapters.runs.appendEvent(first.runId, { ...raw, payload: unattributed, sourceKey: "promote-unattributed", visibility: "public" }));
    const second = await agent.submit(input, runOptions);
    await second.result;
    await assert.rejects(adapters.runs.appendEvent(second.runId, { ...draft, sourceKey: "cross-run" }));
    gate.resolve();
    await first.result;
    await assert.rejects(adapters.runs.appendEvent(first.runId, { ...draft, sourceKey: "late" }));
    assert.equal((await collect(first.events())).filter((e) => e.kind === "planning.progress").length, 1);
  } finally { gate.resolve(); await first.cancel(); }
});

for (const [budgets, runtimeLimits, script] of [
  [{ maxModelAttempts: 1 }, {}, [wire("准备检查。")]],
  [{ maxOutputEvents: 1 }, {}, [wire("准备检查。"), wire()]],
  [{ maxOutputBytes: 20 }, {}, [wire("准备检查。"), wire()]],
  [{}, { maxContentChars: 20 }, [wire()]],
  [{}, { maxChunks: 1 }, [wire("准备检查。"), wire()]],
  [{ maxRunGenerationTokens: 7 }, {}, [wire()]],
]) test(`planning obeys shared budgets ${JSON.stringify({ budgets, runtimeLimits })}`, async () => {
  const { agent, state } = managed([script, script], { runtimeLimits });
  const handle = await agent.submit(input, { budgets: { maxRunGenerationTokens: null, ...budgets } });
  await assert.rejects(handle.result);
  assert.equal(state.planningCalls, 1);
  assert.equal(state.executions, 0);
  assert.equal(state.opened, state.closed);
});

test("silent planning hits the existing absolute invocation deadline without fake progress", async () => {
  const gate = deferred();
  const { agent, state } = managed([[gate]], { runtimeLimits: { invocationTimeoutMs: 15 } });
  const handle = await agent.submit(input, runOptions);
  await assert.rejects(handle.result, (error) => error.code === "model_invocation_deadline_exceeded");
  gate.resolve();
  assert.equal((await collect(handle.events())).filter((e) => e.kind === "planning.progress").length, 0);
  assert.equal(state.opened, state.closed);
});

test("Planner attempt deadline is independent from the Agent runtime timeout", async () => {
  const gate = deferred();
  const { agent, state } = managed([[gate]], {
    runtimeLimits: { invocationTimeoutMs: 1_000 },
    planning: {
      policy: { planningConstraints: () => ({}) },
      plannerFactory: (tasks) => new ModelWorkPlanner(tasks, {
        maxRepairAttempts: 0,
        attemptTimeoutMs: 15,
      }),
    },
  });
  const handle = await agent.submit(input, runOptions);
  await assert.rejects(
    handle.result,
    (error) => error.code === "model_invocation_deadline_exceeded",
  );
  gate.resolve();
  assert.equal(state.planningCalls, 1);
  assert.equal(state.opened, state.closed);
});

for (const boundary of ["persistence", "publisher", "gateway"]) test(`planning closes after ${boundary} failure`, async () => {
  const { agent, adapters, state } = managed([[wire("准备检查。"), ...(boundary === "gateway" ? [new Error("PRIVATE gateway")] : [wire()])]]);
  if (boundary === "persistence") {
    const original = adapters.runs.appendEvent.bind(adapters.runs);
    adapters.runs.appendEvent = async (id, draft, claim) => {
      if (draft.kind === "planning.progress") throw new Error("PRIVATE disk");
      return original(id, draft, claim);
    };
  }
  if (boundary === "publisher") {
    const original = adapters.outputs.publishCommitted.bind(adapters.outputs);
    adapters.outputs.publishCommitted = async (event) => {
      if (event.kind === "planning.progress") throw new Error("PRIVATE publish");
      return original(event);
    };
  }
  const handle = await agent.submit(input, runOptions);
  await assert.rejects(handle.result, (e) => e.code === ({ persistence: "output_persistence_failed", publisher: "output_publish_failed", gateway: "model_stream_error" }[boundary]));
  const events = await collect(handle.events());
  const phase = events.find((e) => e.kind === "operation.started" && e.payload.kind === "planning");
  const ends = events.filter((e) => e.kind === "operation.finished" && e.payload.operationId === phase.payload.operationId);
  assert.equal(ends.length, 1);
  assert.equal(ends[0].payload.status, "failed");
  assert.doesNotMatch(JSON.stringify(events), /PRIVATE/);
  assert.equal(state.executions, 0);
  assert.equal(state.opened, state.closed);
});

test("suppressed progress is not counted as first public output", async () => {
  const { agent } = managed([[wire("准备检查。"), wire()]], { outputPolicy: {
    authorize(event) { return event.kind === "planning.progress" ? null : event; },
  } });
  const handle = await agent.submit(input, runOptions);
  await handle.result;
  const events = await collect(handle.events({ visibility: "all" }));
  assert.equal(events.find((e) => e.kind === "model.diagnostics").payload.firstPublicProgressMs, null);
});

for (const planningMode of ["reactive", "planned"]) test(`custom Planner lifecycle without fake model work: ${planningMode}`, async () => {
  let calls = 0;
  const { agent, state } = managed([], { planning: {
    planner: { async createPlan(request) { calls += 1; assert.ok(request.scope.runId); return plan; } },
  } });
  const handle = await agent.submit({ ...input, planningMode }, runOptions);
  await handle.result;
  const events = await collect(handle.events());
  const planned = Number(planningMode === "planned");
  assert.equal(events.filter((e) => e.kind === 'operation.started' && e.payload.kind === 'planning').length, planned);
  assert.equal(calls, planned);
  assert.equal(state.planningCalls, 0);
  assert.equal(events.filter((e) => e.kind === 'planning.progress').length, 0);
});

test('dynamic planning uses distinct revision scopes and never repeats completed work', async () => {
  let plans = 0, tools = 0;
  const toolPlan = { workPlan: { title: 'Inspect', steps: [
    { id: 'inspect', title: 'Inspect', type: 'read', executor: 'tool', capabilityNames: ['lookup'] },
    { id: 'analyze', title: 'Analyze', type: 'analyze', executor: 'model', dependsOn: ['inspect'] },
    { id: 'review', title: 'Review', type: 'review', executor: 'model', dependsOn: ['analyze'] },
    { id: 'respond', title: 'Respond', type: 'review', executor: 'model', dependsOn: ['review'] },
  ] } };
  const agent = new Agent({
    model: { capabilities,
      async invoke() { throw new Error('must stream'); },
      stream(request) {
        const planning = request.messages.some((m) => m.attributes?.planningContract);
        const revision = plans;
        if (planning) plans += 1;
        return { appliedGenerationLimit: request.outputBudget.maxGenerationTokens,
          async *[Symbol.asyncIterator]() {
            if (planning) {
              yield { contentDelta: wire(revision === 0 ? '准备检查证据。' : '准备组织回答。') };
              yield { contentDelta: JSON.stringify({ v: 1, type: 'plan', plan: revision === 0 ? toolPlan : plan }) + '\n', finishReason: 'stop' };
            } else if (request.tools.length > 0) {
              yield { toolCallDeltas: [{ index: 0, id: 'call-lookup', name: 'lookup', argumentsFragment: '{}' }], finishReason: 'tool_calls' };
            } else yield { contentDelta: 'done', finishReason: 'stop' };
          },
        };
      },
    },
    tools: [{ name: 'lookup', description: 'Read', inputSchema: { type: 'object', properties: {} }, policy: { mode: 'read', title: 'Read' },
      run() { tools += 1; return { content: 'evidence', effectState: 'not_started', planningDisposition: 'replan' }; } }],
    planning: { plannerFactory: (tasks) => new ModelWorkPlanner(tasks) },
  });
  const handle = await agent.submit(input, runOptions);
  assert.equal((await handle.result).output, 'done');
  const progress = (await collect(handle.events())).filter((e) => e.kind === 'planning.progress');
  assert.deepEqual(progress.map((e) => e.payload.revision), [0, 1]);
  assert.equal(new Set(progress.map((e) => e.payload.operationId)).size, 2);
  assert.equal(plans, 2); assert.equal(tools, 1);
});

test('Adapter transport evidence is distinct from logical repair attempts', async () => {
  const { agent } = managed([[{ type: "activity", kind: 'transport', transportDiagnostics: { requestSentAtMs: 1000, firstByteAtMs: 1012, httpAttempts: 2 } }, wire()]]);
  // A transport activity must be declared by the Adapter.
  const handle = await agent.submit(input, runOptions);
  await handle.result;
  const diagnostics = (await collect(handle.events({ visibility: 'all' }))).find((e) => e.kind === 'model.diagnostics');
  assert.equal(diagnostics.payload.sdkHttpAttempts, 2);
  assert.equal(diagnostics.payload.attempt, 0);
  assert.equal(diagnostics.payload.httpFirstByteAtMs, 1012);
  assert.equal(diagnostics.payload.firstPublicProgressMs, null);
});

test('active reasoning cannot renew the absolute planning deadline', async () => {
  let closed = false;
  const { agent } = managed([], { runtimeLimits: { invocationTimeoutMs: 20 }, model: { capabilities,
    async invoke() { throw new Error('must stream'); },
    stream(request) { return { appliedGenerationLimit: request.outputBudget.maxGenerationTokens,
      async *[Symbol.asyncIterator]() {
        try { while (true) { await new Promise((done) => setTimeout(done, 1)); yield { reasoningDelta: 'PRIVATE_ACTIVE' }; } }
        finally { closed = true; }
      },
    }; },
  } });
  const handle = await agent.submit(input, runOptions);
  await assert.rejects(handle.result, (e) => e.code === 'model_invocation_deadline_exceeded');
  assert.equal(closed, true);
  assert.equal((await collect(handle.events())).filter((e) => e.kind === 'planning.progress').length, 0);
});

test('partial and oversized records stay private and close the parser', () => {
  const parser = new PlanningStreamParser();
  assert.deepEqual(parser.feed('{"v":1,"type":"progress","text":"SECRET'), []);
  assert.throws(() => parser.feed('x'.repeat(262144)), (e) => e.code === 'invalid_planning_stream');
  assert.throws(() => parser.feed('"}\n'), (e) => e.code === 'invalid_planning_stream');
});

test('failed phase finish is fenced by one failed terminal operation', async () => {
  const { agent, adapters, state } = managed([[wire('准备检查。'), wire()]]);
  const original = adapters.runs.appendEvent.bind(adapters.runs);
  let failed = false;
  adapters.runs.appendEvent = async (id, draft, claim) => {
    if (draft.kind === 'operation.finished' && draft.payload.parentOperationId === undefined && !failed) {
      failed = true;
      throw new Error('finish storage unavailable');
    }
    return original(id, draft, claim);
  };
  const handle = await agent.submit(input, runOptions);
  await assert.rejects(handle.result);
  const events = await collect(handle.events());
  const phase = events.find((e) => e.kind === 'operation.started' && e.payload.kind === 'planning');
  const ends = events.filter((e) => e.kind === 'operation.finished' && e.payload.operationId === phase.payload.operationId);
  assert.equal(ends.length, 1);
  assert.equal(ends[0].payload.status, 'failed');
  assert.equal(state.executions, 0);
});

test('JSON byte ceiling applies across multiple records', () => {
  const parser = new PlanningStreamParser();
  const record = wire('Intent').trimEnd() + ' '.repeat(250000) + '\n';
  for (let i = 0; i < 4; i++) assert.equal(parser.feed(record).length, 1);
  assert.throws(() => parser.feed(record), (e) => e.code === 'invalid_planning_stream');
});

for (const scenario of fixture.managedRuns) test(`shared managed planning lifecycle: ${scenario.name}`, async () => {
  const scripts = scenario.attempts.map((records, attempt) => records.map((record) => record === 'progress' ? wire(`Intent ${attempt}`) : wire()));
  const { agent, state } = managed(scripts);
  const handle = await agent.submit(input, runOptions);
  if (scenario.status === 'failed') await assert.rejects(handle.result, (e) => e.code === 'invalid_planning_stream');
  else await handle.result;
  assert.equal((await handle.snapshot()).status, scenario.status === 'done' ? 'completed' : 'failed');
  assert.equal(state.planningCalls, scenario.attempts.length);
  const events = await collect(handle.events());
  const progress = events.filter((e) => e.kind === 'planning.progress');
  assert.deepEqual(progress.map((e) => e.payload.attempt), scenario.attempts.map((_, i) => i));
  assert.equal(new Set(progress.map((e) => e.payload.operationId)).size, 1);
  assert.deepEqual(events, await collect(handle.events()));
});

test('cancel settles already-reported planning usage exactly once', async () => {
  const gate = deferred();
  const { agent } = managed([[{ contentDelta: wire('准备检查。'), usage: { inputTokens: 10, generationTokens: 3 } }, gate, wire()]]);
  const handle = await agent.submit(input, runOptions);
  const result = handle.result.catch(() => undefined);
  for await (const event of handle.events()) if (event.kind === 'planning.progress') break;
  await handle.cancel();
  await result;
  gate.resolve();
  const first = await handle.snapshot();
  assert.equal(first.usage.generationTokens, 3);
  assert.equal(first.usage.inputTokens, 10);
  await handle.cancel();
  assert.deepEqual((await handle.snapshot()).usage, first.usage);
});

test('revision repairs a reused completed id without accepting partial work', async () => {
  let calls = 0;
  const tasks = new ModelTaskRunner({ runId: 'unit-revision', model: { capabilities,
    async invoke() { throw new Error('must stream'); },
    stream(request) {
      calls += 1;
      const id = calls === 1 ? 'completed' : 'remaining';
      return { appliedGenerationLimit: request.outputBudget.maxGenerationTokens,
        async *[Symbol.asyncIterator]() { yield { contentDelta: JSON.stringify({ v: 1, type: 'plan', plan: { workPlan: {
          title: 'Remaining', steps: [{ id, title: id, type: 'review', executor: 'model' }],
        } } }) + '\n', finishReason: 'stop' }; },
      };
    },
  } });
  const result = await new ModelWorkPlanner(tasks).revisePlan(input, { availableTools: [], planningContext: [], constraints: {} }, {
    revision: 1, round: 2, remainingModelRounds: 3, messages: [], reason: 'continue', completedSteps: [{ id: 'completed' }],
  });
  assert.equal(calls, 2);
  assert.deepEqual(result.workPlan.steps.map((s) => s.id), ['remaining']);
});
