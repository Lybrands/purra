import assert from "node:assert/strict";
import test from "node:test";
import { Agent, InMemoryRunRepository, staticImageContent, estimateMessagesTokens, estimateJsonTokens } from "purra";
import { testGateway } from "./support/model-gateway.mjs";
import { ModelWorkPlanner, prepareContext } from "purra";
import { trimMessagesByTurn } from "../dist/context/budget.js";
import { copyCapabilitySnapshot } from "../dist/model/validation.js";

const dataBase64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aE9sAAAAASUVORK5CYII=";
const image = { mediaType: "image/png", dataBase64, inputTokens: 2000 };
const baseCapabilities = testGateway({}).capabilities;
const imageCapabilities = { ...baseCapabilities, protocol: { ...baseCapabilities.protocol, imageInput: "supported" } };
test("image transport bytes do not inflate the text token estimate", () => {
  const small = staticImageContent("Describe", [{ ...image, dataBase64: "YQ==" }]);
  const large = staticImageContent("Describe", [{ ...image, dataBase64: Buffer.alloc(100_000, 120).toString("base64") }]);
  assert.equal(estimateMessagesTokens([{ role: "user", content: small }]), estimateMessagesTokens([{ role: "user", content: large }]));
  assert.ok(large.images[0].dataBase64.length > 100_000);
});
test("unknown image capability preserves legacy snapshot identity", () => {
  assert.deepEqual(copyCapabilitySnapshot(baseCapabilities), copyCapabilitySnapshot({ ...baseCapabilities, protocol: { ...baseCapabilities.protocol, imageInput: "unknown" } }));
  assert.equal(copyCapabilitySnapshot(imageCapabilities).protocol.imageInput, "supported");
});
for (const imageInput of [undefined, "unknown", "unavailable"]) {
  test(`image request rejects ${imageInput} support before gateway invocation`, async () => {
    let calls = 0;
    const model = testGateway({ capabilities: { ...baseCapabilities, protocol: { ...baseCapabilities.protocol, ...(imageInput === undefined ? {} : { imageInput }) } },
      async invoke() { calls++; throw Error("must not dispatch"); } });
    const agent = new Agent({ model });
    await assert.rejects(agent.invoke({ messages: [{ role: "user", content: staticImageContent("Describe", [image]) }] }), { code: "model_capability_incompatible" });
    assert.equal(calls, 0);
  });
}
test("planner keeps image attachments and stable message associations", async () => {
  const content = staticImageContent("Describe", [image]);
  const request = { messages: [{ role: "user", content }, { role: "assistant", content: "Earlier answer" }, { role: "user", content }] };
  const planner = new ModelWorkPlanner({ async plan(messages, options) {
    const value = messages.find(m => m.attributes?.planningInput).content;
    assert.deepEqual(value.images, [image, image]);
    assert.ok(!value.text.includes(dataBase64));
    const payload = JSON.parse(value.text);
    assert.deepEqual(payload.conversation.filter(m => m.role === "user").map(m => m.content.imageIndexes), [[0], [1]]);
    const plan = { workPlan: { title: "Review", steps: [{ id: "inspect", title: "Inspect image", type: "review", executor: "model" }] } };
    options.validatePlan(plan);
    return { turn: { message: { role: "assistant", content: JSON.stringify(plan) }, finishReason: "stop" } };
  } });
  await planner.createPlan(request, { availableTools: [], constraints: { minInitialVisibleSteps: 1 }, planningContext: [] });
  assert.equal(request.messages[0].content, content);
});

test("turn trimming preserves the complete current image and reports overflow", () => {
  const latest = { role: "user", content: staticImageContent("Describe", [image]) };
  const result = trimMessagesByTurn([{ role: "user", content: "old" }, { role: "assistant", content: "old answer" }, latest], 100);
  assert.deepEqual(result.messages, [latest]);
  assert.equal(result.overflowTokens, estimateMessagesTokens([latest]) - 100);
});

test("custom compression cannot silently replace the current image with text", async () => {
  const message = { role: "user", content: staticImageContent("Describe", [image]) };
  const prepared = await prepareContext({
    provider: { buildContext() { return { blocks: [] }; } },
    triggerRatio: 0.001,
    compression: { compress() { return { messages: [{ role: "user", content: "Describe" }] }; } },
  }, { request: { messages: [message] }, tools: [], windowTokens: 16_000, outputReserveTokens: 512 });
  await assert.rejects(prepared.project([message]), { code: "context_compaction_removed_current_request" });
});
test("image content survives JSON and the public Agent path", async () => {
  const content = staticImageContent("Describe this image", [image]);
  assert.deepEqual(JSON.parse(JSON.stringify(content)), content);
  assert.ok(Object.isFrozen(content.images[0]));
  const message = { role: "user", content };
  const projection = { ...message, content: { ...content, images: content.images.map(image => ({ ...image, dataBase64: "" })) } };
  assert.equal(estimateMessagesTokens([message]), 6 + 2000 + estimateJsonTokens(projection));
  let calls = 0;
  const agent = new Agent({ responsePresentation: "none", runRepository: new InMemoryRunRepository(), model: testGateway({
    capabilities: imageCapabilities,
    async invoke(request) {
      calls++;
      assert.deepEqual(request.messages.find(m => m.role === "user").content, content);
      return { message: { role: "assistant", content: "A small image" }, finishReason: "stop" };
    },
  }) });
  const handle = await agent.submit({ messages: [message], planningMode: "reactive" }, { budgets: { maxRunGenerationTokens: null } });
  assert.equal((await handle.result).output, "A small image");
  assert.equal(calls, 1);
  const events = [];
  for await (const event of handle.events()) events.push(event);
  assert.ok(!JSON.stringify(events).includes(dataBase64));
});

for (const changes of [{ inputTokens: 0 }, { inputTokens: true }, { inputTokens: 1.5 }, { inputTokens: 2 ** 53 },
  { dataBase64: "YR==" }, { dataBase64: "" }, { dataBase64: "https://example.com/image.png" },
  { mediaType: "image/svg+xml" }, { url: "file:///private" }]) {
  test(`invalid image ${JSON.stringify(changes)}`, () => assert.throws(() => staticImageContent("", [{ ...image, ...changes }])));
}
