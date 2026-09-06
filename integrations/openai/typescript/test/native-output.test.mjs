import assert from "node:assert/strict";
import test from "node:test";
import { readFileSync } from "node:fs";
import OpenAI from "openai";
import { ModelTaskRunner, StructuredOutputContract } from "purra";
import { OpenAIResponsesGateway, OpenAIChatCompletionsGateway } from "../dist/index.js";
const schema = { type: "object", properties: { ok: { type: "boolean" } }, required: ["ok"], additionalProperties: false };
const capabilities = {
    schemaVersion: 2,
    profileId: "model-task-fixture",
    providerProtocol: "custom",
    contextWindowTokens: 16_000,
    maxGenerationTokens: 512,
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
      jsonSchemaLevel: "json_schema",
      streamFinishSemantics: "normalized",
      usageSemantics: "normalized",
    },
  };
for (const kind of ["responses", "chat"]) test(`native SDK output: ${kind}`, async () => {
  const requests = [];
  const client = new OpenAI({ apiKey: "fixture-not-a-key", fetch: async (_url, options) => {
    requests.push(JSON.parse(options.body));
    const response = kind === "responses"
      ? JSON.parse(readFileSync(new URL("../../fixtures/response.json", import.meta.url)))
      : JSON.parse(readFileSync(new URL("../../fixtures/chat.json", import.meta.url))).response;
    if (kind === "responses") response.output = [{ type: "message", id: "msg", role: "assistant", status: "completed", content: [{ type: "output_text", text: requests.length === 1 ? '{"ok":true}' : '{"ok":"wrong"}', annotations: [] }] }];
    else response.choices = [{ index: 0, finish_reason: "stop", message: { role: "assistant", content: requests.length === 1 ? '{"ok":true}' : '{"ok":"wrong"}' } }];
    return new Response(JSON.stringify(response), { headers: { "content-type": "application/json" } });
  } });
  const model = new (kind === "responses" ? OpenAIResponsesGateway : OpenAIChatCompletionsGateway)({ client, model: "fixture-model", capabilities });
  const runner = new ModelTaskRunner({ model, runId: "run" });
  const output = await StructuredOutputContract.create({ schemaId: "test", schemaVersion: "1", schema, mode: "native_required" });
  const result = await runner.completeStructured([{ role: "user", content: "check" }], { output });
  assert.deepEqual(result.value, { ok: true });
  assert.equal(result.receipt.outputContract.nativeDialect, "purra.openai-json-schema/v1");
  const fmt = kind === "responses" ? requests[0].text.format : requests[0].response_format.json_schema;
  assert.equal(fmt.strict, true);
  assert.deepEqual(fmt.schema, schema);
  await assert.rejects(runner.completeStructured([{ role: "user", content: "check" }], { output }), { code: "structured_output_schema_mismatch" });
  const bad = await StructuredOutputContract.create({ schemaId: "test", schemaVersion: "2", schema: { ...schema, properties: { ok: { type: "string", minLength: 1 } } }, mode: "native_required" });
  await assert.rejects(runner.completeStructured([], { output: bad, repairAttempts: 3 }), { code: "structured_output_mode_unsupported" });
  assert.equal(requests.length, 2);
});


test("shared native dialect admission is pure", async () => {
  const cases = JSON.parse(readFileSync(new URL("../../../../conformance/fixtures/native_output_schema.json", import.meta.url))).cases;
  const client = new OpenAI({ apiKey: "fixture-not-a-key", fetch: async () => { throw new Error("preflight cannot perform I/O"); } });
  const gateways = [new OpenAIResponsesGateway({ client, model: "fixture-model", capabilities }), new OpenAIChatCompletionsGateway({ client, model: "fixture-model", capabilities })];
  for (const row of cases) {
    const outputContract = await StructuredOutputContract.create({ schemaId: "fixture", schemaVersion: "1", schema: row.schema, mode: "native_required" });
    const request = { messages: [], tools: [], capabilitySnapshot: capabilities, outputContract };
    for (const gateway of gateways) {
      if (row.supported) assert.equal(gateway.validateOutputContract(request), "purra.openai-json-schema/v1", row.name);
      else assert.throws(() => gateway.validateOutputContract(request), { code: "structured_output_mode_unsupported" });
    }
  }
});
