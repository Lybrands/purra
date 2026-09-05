import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import OpenAI from "openai";
import { OpenAIResponsesGateway } from "../dist/index.js";

const response = JSON.parse(readFileSync(new URL("../../fixtures/response.json", import.meta.url), "utf8"));
const request = { messages: [{ role: "user", content: "check" }], tools: [{ name: "lookup", description: "lookup", inputSchema: { type: "object", properties: { query: { type: "string" } } } }],
  outputBudget: { maxGenerationTokens: 128, generationSource: "user", profileMaxGenerationTokens: 256,
    requestedUserMaxGenerationTokens: 128, resultCapacityTargetTokens: null, resultCapacitySource: null, nonResultHeadroomTokens: null } };
function gateway(fetch) { return new OpenAIResponsesGateway({ model: "fixture-model", capabilities: {}, client: new OpenAI({ apiKey: "fixture-not-a-key", fetch, maxRetries: 5 }) }); }

test("official SDK maps completion, SSE, encrypted continuity and real usage", async () => {
  const requests = [];
  const model = gateway(async (_url, options) => {
    const body = JSON.parse(options.body); requests.push(body);
    if (!body.stream) return new Response(JSON.stringify(response), { headers: { "content-type": "application/json" } });
    const events = [
      { type: "response.reasoning_summary_text.delta", delta: "private-working-text" },
      { type: "response.output_item.added", output_index: 1, item: { ...response.output[1], arguments: "" } },
      { type: "response.function_call_arguments.delta", output_index: 1, delta: '{"query":"中文"}' },
      { type: "response.completed", response },
    ];
    return new Response(events.map(e => `data: ${JSON.stringify(e)}\n\n`).join(""), { headers: { "content-type": "text/event-stream" } });
  });
  const result = await model.invoke(request);
  assert.equal(result.finishReason, "tool_calls");
  assert.equal(result.usage.reasoningTokens, 6);
  assert.equal(result.message.reasoning, undefined);
  const stream = await model.stream(request), chunks = [];
  for await (const chunk of stream) chunks.push(chunk);
  assert.deepEqual(chunks[0], { type: "activity", kind: "working" });
  assert.equal(JSON.stringify(chunks).includes("private-working-text"), false);
  assert.equal(chunks.at(-1).finishReason, "tool_calls");
  assert.deepEqual(chunks.at(-1).providerData, result.message.providerData);
  await model.invoke({ ...request, messages: [...request.messages, result.message, { role: "tool", toolCallId: "call_fixture", content: "found" }] });
  assert.equal(requests.at(-1).input[1].encrypted_content, "opaque-encrypted-state");
  assert.equal(requests.at(-1).input.at(-1).type, "function_call_output");
  assert.equal(requests.at(-1).max_output_tokens, 128);
  assert.equal(requests.at(-1).store, false);
});

test("SDK retries are disabled and error messages are sanitized", async () => {
  let attempts = 0;
  const model = gateway(async () => { attempts++; return new Response(JSON.stringify({ error: { message: "private-provider-error" } }), { status: 429, headers: { "content-type": "application/json" } }); });
  await assert.rejects(model.invoke(request), { code: "openai_http_429", message: "OpenAI request failed" });
  assert.equal(attempts, 1);
});

test("interrupted streams fail instead of manufacturing completion", async () => {
  const model = gateway(async () => new Response('data: {"type":"response.output_text.delta","delta":"partial"}\n\n', { headers: { "content-type": "text/event-stream" } }));
  await assert.rejects(async () => { for await (const _ of await model.stream(request)) {} }, { code: "upstream_stream_interrupted" });
});

test("an unused stream does not dispatch or leave an open transport", async () => {
  let requests = 0;
  const model = gateway(async () => { requests++; throw Error("must not dispatch"); });
  const stream = await model.stream(request);
  await stream[Symbol.asyncIterator]().return();
  assert.equal(requests, 0);
});
