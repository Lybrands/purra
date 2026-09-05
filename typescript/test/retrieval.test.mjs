import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  Agent,
  AgentCanceledError,
  AgentError,
  RetrievalError,
  RetrieverTool,
  prepareContext,
} from "purra";
import * as publicApi from "purra";
import { ToolCatalog } from "../dist/tools/catalog.js";
import { testGateway } from "./support/model-gateway.mjs";

const fixture = JSON.parse(readFileSync(
  new URL("../../conformance/fixtures/retrieval.json", import.meta.url),
  "utf8",
));

test("RetrieverTool exposes one stable read-only ToolDefinition", () => {
  const tool = retrieverTool({ async retrieve() { return []; } });

  assert.equal(tool.definition, tool.definition);
  assert.equal(tool.definition.name, "search_knowledge");
  assert.equal(tool.definition.policy.mode, "read");
  assert.equal(tool.definition.policy.riskLevel, "read");
  assert.deepEqual(Object.keys(tool.definition.inputSchema.properties), ["query"]);
  assert.deepEqual(tool.definition.inputSchema.required, ["query"]);
  assert.equal(tool.definition.inputSchema.additionalProperties, false);
  assert.match(tool.definition.description, /data only/i);
  assert.equal("buildRetrieverToolRegistration" in publicApi, false);
  assert.equal("createRetrieverTool" in publicApi, false);

  assert.doesNotThrow(() => new Agent({
    model: testGateway(completion("done")),
    tools: [tool.definition],
  }));
});

test("RetrieverTool maps trusted host fields and reads live data", async () => {
  const row = fixture.successCase;
  const hits = structuredClone(row.hits);
  const requests = [];
  const signals = [];
  const retriever = {
    async retrieve(request, signal) {
      requests.push(request);
      signals.push(signal);
      return hits;
    },
  };
  const tool = retrieverTool(retriever, { scope: row.scope });
  const controller = new AbortController();

  const first = await tool.definition.run(
    { query: row.query },
    toolContext(row.runId, controller.signal),
  );

  assert.equal(first.effectState, "not_started");
  assert.equal(first.errorCode, undefined);
  assert.deepEqual(first.content, row.modelResult);
  assert.deepEqual(requests[0], {
    query: row.query,
    limit: fixture.defaults.maxResults,
    runId: row.runId,
    scope: row.scope,
  });
  assert.equal(signals[0], controller.signal);

  hits.push({
    id: "fact-3",
    content: "New live data.",
    source: "fixture",
    untrusted: true,
    metadata: {},
  });
  const second = await tool.definition.run(
    { query: row.query },
    toolContext(row.runId),
  );
  assert.equal(second.content.hits.length, 3);
});

test("RetrieverTool persists versioned evidence and blocks a stale follow-up model call", async () => {
  const evidence = {
    evidenceId: "mem0:store:memory-1:3",
    contextBlock: "search_knowledge",
    source: "mem0/scope",
    itemId: "memory-1",
    version: "3",
  };
  const tool = retrieverTool({
    async retrieve() {
      return [{
        id: evidence.itemId,
        content: "Versioned memory.",
        source: evidence.source,
        version: 3,
        untrusted: true,
        metadata: { evidenceId: evidence.evidenceId },
      }];
    },
  });
  const direct = await tool.definition.run(
    { query: "memory" },
    toolContext("run-evidence"),
  );
  assert.deepEqual(direct.contextEvidence, [evidence]);
  const batch = await new ToolCatalog([tool.definition]).executeBatch([{
    id: "memory-call",
    name: tool.definition.name,
    arguments: { query: "memory" },
  }], { executionKey: "run-evidence" });
  assert.deepEqual(batch.contextEvidence, [evidence]);

  let modelCalls = 0;
  const validations = [];
  const agent = new Agent({
    model: testGateway({
      async invoke() {
        modelCalls += 1;
        return modelCalls === 1
          ? callsTurn([{
              id: "memory-call",
              name: tool.definition.name,
              arguments: { query: "memory" },
            }])
          : finalTurn("unsafe");
      },
    }),
    tools: [tool.definition],
    evidenceValidator: {
      validateEvidence(receipts) {
        validations.push(receipts);
        throw new AgentError("external_evidence_stale", "stale evidence");
      },
    },
  });

  await rejectsCode(agent.invoke(runInput()), "external_evidence_stale");
  assert.equal(modelCalls, 1);
  assert.deepEqual(validations, [[evidence]]);
});

test("RetrieverTool snapshots host configuration without freezing its data source", async () => {
  const scope = { namespace: "project-1", nested: { ids: ["one"] } };
  const requests = [];
  const options = {
    retriever: { async retrieve(request) { requests.push(request); return []; } },
    name: "search_knowledge",
    description: "Search configured knowledge.",
    scope,
    maxResults: 2,
  };
  const tool = new RetrieverTool(options);
  scope.namespace = "other-project";
  scope.nested.ids.push("two");
  options.maxResults = 100;
  options.retriever = { async retrieve() { throw new Error("replacement must not run"); } };

  await tool.definition.run({ query: "canon" }, toolContext("run-1"));

  assert.deepEqual(requests[0].scope, { namespace: "project-1", nested: { ids: ["one"] } });
  assert.equal(requests[0].limit, 2);
  assert.throws(() => requests[0].scope.nested.ids.push("three"), TypeError);
  assert.throws(() => { tool.definition.inputSchema.properties.query.maxLength = 1; }, TypeError);
});

test("the model cannot provide host-owned retrieval fields", async () => {
  let executions = 0;
  const tool = retrieverTool({
    async retrieve() {
      executions += 1;
      return [];
    },
  });

  const invalidArguments = [
    ...fixture.forbiddenModelArguments,
    { query: "" },
    { query: "x".repeat(fixture.defaults.maxQueryChars + 1) },
  ];
  for (const [index, argumentsValue] of invalidArguments.entries()) {
    const agent = new Agent({
      model: testGateway(calls([{
        id: `forbidden-${index}`,
        name: tool.definition.name,
        arguments: argumentsValue,
      }])),
      tools: [tool.definition],
    });
    await rejectsCode(agent.invoke(runInput()), "invalid_tool_arguments_schema");
  }
  assert.equal(executions, 0);
});

test("known retrieval failures keep stable codes and hide private details", async () => {
  for (const code of fixture.stableErrors) {
    const error = new RetrievalError(
      code,
      "secret-token at private/path",
      { retryable: code === "retrieval_timeout", details: { scope: "private-project" } },
    );
    assert.equal(error.code, code);
    assert.equal(error.retryable, code === "retrieval_timeout");

    const result = await retrieverTool({
      async retrieve() { throw error; },
    }).definition.run({ query: "canon" }, toolContext("run-1"));

    assert.equal(result.errorCode, code);
    assert.equal(JSON.stringify(result.content).includes("secret-token"), false);
    assert.equal(JSON.stringify(result.content).includes("private/path"), false);
    assert.equal(JSON.stringify(result.content).includes("private-project"), false);
  }
  assert.throws(() => new RetrievalError("retrieval_failed", "unsupported failure"));
});

test("unknown retrieval failures use the existing Tool boundary", async () => {
  const requests = [];
  const tool = retrieverTool({
    async retrieve() { throw new Error("secret-token-/private/path"); },
  });
  const agent = new Agent({
    model: testGateway({
      async invoke(request) {
        requests.push(request);
        return requests.length === 1
          ? callsTurn([{ id: "one", name: tool.definition.name, arguments: { query: "canon" } }])
          : finalTurn("recovered");
      },
    }),
    tools: [tool.definition],
  });

  const result = await agent.invoke(runInput());
  const receipt = requests[1].messages.at(-1).content;
  assert.equal(result.output, "recovered");
  assert.deepEqual(receipt, {
    ok: false,
    error: {
      code: "tool_execution_failed",
      message: "Tool execution did not complete successfully.",
    },
    effectState: "not_started",
  });
  assert.equal(JSON.stringify(receipt).includes("secret-token"), false);
  assert.equal(JSON.stringify(receipt).includes("private/path"), false);
});

test("invalid and oversized retrieval results fail closed", async () => {
  const invalid = retrieverTool({
    async retrieve() { return [{ id: "not-a-hit" }]; },
  });
  const oversized = retrieverTool({
    async retrieve() {
      return [{
        id: "large",
        content: "x".repeat(1_000),
        source: "fixture",
        untrusted: true,
        metadata: {},
      }];
    },
  }, { maxResultChars: 120 });

  const invalidResult = await invalid.definition.run(
    { query: "canon" },
    toolContext("run-1"),
  );
  const oversizedResult = await oversized.definition.run(
    { query: "canon" },
    toolContext("run-1"),
  );

  assert.equal(invalidResult.errorCode, "invalid_retrieval_result");
  assert.equal(oversizedResult.errorCode, "retrieval_result_too_large");
  assert.equal(JSON.stringify(oversizedResult.content).includes("x".repeat(50)), false);

  const tooMany = retrieverTool({
    async retrieve() {
      return Array.from({ length: fixture.defaults.maxResults + 1 }, () => fixture.successCase.hits[0]);
    },
  });
  assert.equal((await tooMany.definition.run(
    { query: "canon" }, toolContext("run-1"),
  )).errorCode, "invalid_retrieval_result");
});

test("application authorization covers direct and catalog retrieval", async () => {
  const bindings = new Map([["authorized-run", "project-1"]]);
  const reads = [];
  const retriever = {
    async retrieve(request) {
      const namespace = bindings.get(request.runId);
      if (namespace === undefined) {
        throw new RetrievalError("retrieval_scope_unavailable", "No binding");
      }
      if (request.scope.namespace !== namespace) {
        throw new RetrievalError("retrieval_access_denied", "Wrong scope");
      }
      reads.push(namespace);
      return [];
    },
  };
  for (const row of fixture.scopeCases) {
    const before = reads.length;
    const request = { query: "canon", limit: 8, runId: row.runId, scope: row.scope };
    if (row.errorCode === null) {
      assert.deepEqual(await retriever.retrieve(request), []);
    } else {
      await rejectsCode(retriever.retrieve(request), row.errorCode);
    }

    const tool = retrieverTool(retriever, { scope: row.scope });
    const result = await new ToolCatalog([tool.definition]).executeBatch([{
      id: "scope-call", name: tool.definition.name, arguments: { query: "canon" },
    }], { executionKey: "scope", runId: row.runId });
    assert.equal(result.failures[0]?.errorCode ?? null, row.errorCode);
    assert.equal(reads.length - before, row.errorCode === null ? 2 : 0);
  }
});

test("retrieval still obeys the existing tool allowlist", async () => {
  let reads = 0;
  const tool = retrieverTool({ async retrieve() { reads += 1; return []; } });
  const catalog = new ToolCatalog([tool.definition]);

  assert.deepEqual(catalog.specsFor([]), []);
  await rejectsCode(catalog.executeBatch([{
    id: "disabled-call", name: tool.definition.name, arguments: { query: "canon" },
  }], { executionKey: "disabled", enabledTools: [] }), "tool_not_enabled");
  assert.equal(reads, 0);
});

test("canceled retrieval discards late results without another model call", { timeout: 2_000 }, async () => {
  const controller = new AbortController();
  let started;
  let finish;
  let receivedSignal;
  let modelCalls = 0;
  const runningTool = new Promise((resolve) => { started = resolve; });
  const pendingHits = new Promise((resolve) => { finish = resolve; });
  const tool = retrieverTool({
    async retrieve(request, signal) {
      receivedSignal = signal;
      started();
      return pendingHits;
    },
  });
  const agent = new Agent({
    model: testGateway({
      async invoke() {
        modelCalls += 1;
        return callsTurn([{ id: "cancel-call", name: tool.definition.name, arguments: { query: "canon" } }]);
      },
    }),
    tools: [tool.definition],
  });
  const running = agent.invoke({ ...runInput(), signal: controller.signal });
  try {
    await runningTool;
    controller.abort();
    await assert.rejects(running, AgentCanceledError);
    assert.equal(receivedSignal.aborted, true);
  } finally {
    controller.abort();
    finish(fixture.successCase.hits);
    await running.catch(() => undefined);
  }
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(modelCalls, 1);
});

test("global tool result limits remain authoritative for retrieval", async () => {
  const tool = retrieverTool({ async retrieve() { return fixture.successCase.hits; } });
  const result = await new ToolCatalog([tool.definition], {
    limits: { maxResultChars: 120 },
  }).executeBatch([{
    id: "bounded-call", name: tool.definition.name, arguments: { query: "canon" },
  }], { executionKey: "bounded" });

  assert.equal(result.failures[0].errorCode, "tool_result_too_large");
  assert.equal("hits" in result.messages[0].content, false);
});

test("model observations preserve source identity without promoting retrieved instructions", async () => {
  const requests = [];
  const tool = retrieverTool({ async retrieve() { return fixture.successCase.hits; } });
  const agent = new Agent({
    model: testGateway({
      async invoke(request) {
        requests.push(request);
        return requests.length === 1
          ? callsTurn([{ id: "evidence-call", name: tool.definition.name, arguments: { query: "canon" } }])
          : finalTurn("done");
      },
    }),
    tools: [tool.definition],
  });
  await agent.invoke(runInput());

  for (const request of requests.slice(1)) {
    const observation = request.messages.find((message) => message.role === "tool");
    assert.equal(observation.toolCallId, "evidence-call");
    assert.deepEqual(observation.content, fixture.successCase.modelResult);
    for (const message of request.messages) {
      if (message.role === "system" || message.role === "developer") {
        assert.equal(JSON.stringify(message.content).includes(fixture.successCase.hits[0].content), false);
      }
    }
  }
});

test("direct retrieval context reuses existing budget and evidence boundaries", async () => {
  const retriever = { async retrieve() { return fixture.successCase.hits; } };
  const [hit] = await retriever.retrieve({ query: "canon", limit: 8, scope: {} });
  const messages = [{ role: "user", content: "Use retrieved evidence." }];
  const prepared = await prepareContext({
    provider: {
      describeContextDemands() {
        return [{ name: "retrieval", desiredTokens: 256 }];
      },
      buildContext() {
        return {
          blocks: [{
            name: "retrieval",
            content: hit.content,
            untrusted: true,
            evidence: [{
              evidenceId: `retrieval:${hit.source}:${hit.id}:${hit.version}`,
              source: hit.source,
              itemId: hit.id,
              version: String(hit.version),
            }],
          }],
        };
      },
    },
  }, {
    request: { messages },
    tools: [],
    windowTokens: 16_000,
    outputReserveTokens: 2_048,
  });

  const projected = await prepared.project(messages);
  assert.match(projected[0].content, /data only/i);
  assert.deepEqual(prepared.evidence, [{
    evidenceId: "retrieval:fixture:fact-1:3",
    contextBlock: "retrieval",
    source: "fixture",
    itemId: "fact-1",
    version: "3",
  }]);
});

function retrieverTool(retriever, overrides = {}) {
  return new RetrieverTool({
    retriever,
    name: "search_knowledge",
    description: "Search configured knowledge.",
    maxResults: fixture.defaults.maxResults,
    maxQueryChars: fixture.defaults.maxQueryChars,
    maxResultChars: fixture.defaults.maxResultChars,
    ...overrides,
  });
}

function toolContext(runId, signal) {
  return {
    call: { id: "retrieval-call", name: "search_knowledge", arguments: {} },
    runId,
    ...(signal === undefined ? {} : { signal }),
  };
}

function completion(content) {
  return { async invoke() { return finalTurn(content); } };
}

function calls(toolCalls) {
  return { async invoke() { return callsTurn(toolCalls); } };
}

function callsTurn(toolCalls) {
  return {
    message: { role: "assistant", content: "", toolCalls },
    finishReason: "tool_calls",
  };
}

function finalTurn(content) {
  return { message: { role: "assistant", content }, finishReason: "stop" };
}

function runInput() {
  return { messages: [{ role: "user", content: "run" }] };
}

async function rejectsCode(promise, code) {
  await assert.rejects(
    promise,
    (error) => error instanceof AgentError && error.code === code,
  );
}
