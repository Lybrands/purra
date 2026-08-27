import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  Agent,
  AgentError,
  InMemoryRunRepository,
  RecoveryLedger,
  RecoveryPolicy,
} from "purra";

const RUN_OPTIONS = Object.freeze({
  budgets: Object.freeze({ maxRunOutputTokens: null }),
});

const fixture = JSON.parse(readFileSync(
  new URL("../../conformance/fixtures/recovery_protocol.json", import.meta.url),
  "utf8",
));

test("shared recovery decisions match Python", () => {
  for (const row of fixture.cases) {
    const ledger = new RecoveryLedger(new RecoveryPolicy(row.policy));
    for (let index = 0; index < (row.preconsume ?? 0); index += 1) {
      assert.equal(ledger.decide(row.request).allowed, true);
    }
    const decision = ledger.decide(row.request);
    assert.deepEqual({
      allowed: decision.allowed,
      reasonCode: decision.reasonCode,
      attempt: decision.attempt,
      maxAttempts: decision.maxAttempts,
    }, row.expected, row.caseId);
  }
});

test("submitted Run persists recovery approval before the retried Provider attempt", async () => {
  const repository = new InMemoryRunRepository();
  let attempts = 0;
  const agent = new Agent({
    runRepository: repository,
    model: {
      async invoke() { throw new Error("stream should be used"); },
      async *stream() {
        attempts += 1;
        if (attempts === 1) return;
        yield { contentDelta: "done", finishReason: "stop" };
      },
    },
  });

  const handle = await agent.submit(
    { messages: [{ role: "user", content: "run" }] },
    RUN_OPTIONS,
  );
  assert.equal((await handle.result).output, "done");
  const events = await collect(handle.events({ visibility: "all" }));
  const traceIndex = events.findIndex((event) => event.kind === "agentRunTrace");
  const secondAttemptIndex = events.findIndex((event, index) => (
    index > traceIndex && event.kind === "invocation.started"
  ));
  assert.equal(attempts, 2);
  assert.equal(traceIndex > 0, true);
  assert.equal(secondAttemptIndex > traceIndex, true);
  assert.deepEqual(events[traceIndex].payload, {
    stage: "recovery_decision",
    outcome: "allowed",
    details: {
      round: 1,
      cause: "provider_stream_interrupted",
      action: "retry_model",
      scope: "run",
      allowed: true,
      reasonCode: "allowed",
      attempt: 1,
      maxAttempts: 1,
      remainingModelRounds: 8,
      minimumRemainingRounds: 1,
      effectState: "not_started",
      mayRepeatSideEffect: false,
    },
  });
});

test("invalid tool input is corrected once before any handler runs", async () => {
  let modelCalls = 0;
  let toolCalls = 0;
  const agent = new Agent({
    model: {
      async invoke() {
        modelCalls += 1;
        if (modelCalls === 1) return toolTurn({ value: "wrong" });
        if (modelCalls === 2) return toolTurn({ value: 1 });
        return finalTurn("done");
      },
    },
    tools: [{
      name: "read",
      description: "Read",
      inputSchema: {
        type: "object",
        properties: { value: { type: "integer" } },
        required: ["value"],
        additionalProperties: false,
      },
      policy: { mode: "read", title: "Read" },
      run() {
        toolCalls += 1;
        return { content: "ok", effectState: "not_started" };
      },
    }],
  });

  assert.equal((await agent.invoke({ messages: [{ role: "user", content: "run" }] })).output, "done");
  assert.equal(modelCalls, 4);
  assert.equal(toolCalls, 1);
});

test("disabled recovery fails without a second model attempt", async () => {
  let modelCalls = 0;
  const agent = new Agent({
    recovery: new RecoveryPolicy({}),
    model: {
      async invoke() {
        modelCalls += 1;
        return finalTurn("");
      },
    },
  });

  await assert.rejects(
    agent.invoke({ messages: [{ role: "user", content: "run" }] }),
    (error) => error instanceof AgentError && error.code === "empty_model_response",
  );
  assert.equal(modelCalls, 1);
});

function toolTurn(args) {
  return {
    message: {
      role: "assistant",
      content: "",
      toolCalls: [{ id: "call-1", name: "read", arguments: args }],
    },
    finishReason: "tool_calls",
  };
}

function finalTurn(content) {
  return { message: { role: "assistant", content }, finishReason: "stop" };
}

async function collect(iterable) {
  const values = [];
  for await (const value of iterable) values.push(value);
  return values;
}
