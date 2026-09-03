import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, writeFileSync, rmSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import test from "node:test";
import { Mem0Memory, MemoryContext } from "../dist/index.js";
import { RetrieverTool, RetrievalError } from "purra";

class Sdk {
  rows = new Map(); histories = new Map(); calls = []; nextId = 0;
  failAdd = false; failUpdate = false; gate; searchOverride;
  async add(messages, options) {
    this.calls.push(["add", structuredClone(options)]);
    if (this.gate) await this.gate;
    const texts = options.infer ? messages.map(m => m.content) : [messages];
    const results = texts.map(text => {
      const id = `id-${String(++this.nextId).padStart(4, "0")}`;
      this.rows.set(id, { id, memory: text, user_id: options.userId, run_id: options.runId, metadata: structuredClone(options.metadata) });
      this.histories.set(id, [{ event: "ADD", new_memory: text }]);
      return { id };
    });
    if (this.failAdd) { this.failAdd = false; throw Error("secret provider payload"); }
    return { results };
  }
  async get(id) { this.calls.push(["get", id]); return structuredClone(this.rows.get(id) ?? null); }
  async getAll({ filters, topK }) {
    this.calls.push(["getAll", structuredClone(filters)]);
    return { results: structuredClone([...this.rows.values()].filter(row => Object.entries(filters)
      .every(([k, v]) => (row[k] ?? row.metadata[k]) === v)).slice(0, topK)) };
  }
  async search(query, options) { this.calls.push(["search", structuredClone(options.filters)]); return this.searchOverride ?? this.getAll(options); }
  async update(id, { text, metadata }) {
    this.rows.get(id).memory = text;
    Object.assign(this.rows.get(id).metadata, metadata);
    this.histories.get(id).push({ event: "UPDATE", new_memory: text });
    if (this.failUpdate) { this.failUpdate = false; throw Error("secret provider payload"); }
  }
  async delete(id) { this.rows.delete(id); this.histories.get(id).push({ event: "DELETE" }); }
  async history(id) { return structuredClone(this.histories.get(id)); }
}
const source = { id: "conversation:1", revision: "3" };
const sharedFixture = JSON.parse(readFileSync(new URL("../../fixtures/memory.json", import.meta.url), "utf8"));
const request = { query: "偏好", limit: 8, scope: {}, runId: "transient-run" };
function fixture(t) {
  const path = mkdtempSync(join(tmpdir(), "purra-mem0-"));
  const client = new Sdk();
  const instances = [];
  const create = (options = {}) => {
    const memory = new Mem0Memory({ client, scope: { user: "user", project: "project" }, journalPath: join(path, "journal.db"), ...options });
    instances.push(memory); return memory;
  };
  t.after(async () => { for (const memory of instances) { await memory.drain(); memory.close(); } rmSync(path, { recursive: true, force: true }); });
  return { client, create, path };
}
const code = expected => error => error.code === expected;

test("Mem0 CRUD, revision checks, persistent idempotency and logical delete", async t => {
  const { client, create } = fixture(t);
  const memory = create();
  const result = await memory.add("我喜欢简短回复。", { source, key: "add" });
  const [id] = result.ids;
  assert.equal(result.state, "complete"); assert.equal(result.usage, "unknown");
  assert.deepEqual((await memory.get(id)).source, source);
  assert.equal((await memory.list()).items[0].version, 1);
  const epoch = memory.epoch;
  await memory.update(id, "请详细解释复杂问题。", { source, version: 1, key: "update" });
  await assert.rejects(memory.update(id, "stale", { source, version: 1, key: "stale" }), code("memory_version_conflict"));
  assert.equal(memory.operation("stale").state, "failed");
  assert.throws(() => memory.assertEpoch(epoch), code("memory_context_stale"));
  assert.equal((await memory.history(id)).at(-1).new_memory, "请详细解释复杂问题。");
  memory.close();
  const restored = create();
  assert.deepEqual(await restored.add("我喜欢简短回复。", { source, key: "add" }), result);
  assert.equal(client.rows.size, 1);
  await assert.rejects(restored.add("changed", { source, key: "add" }), code("memory_idempotency_conflict"));
  await restored.delete(id, { version: 2, key: "delete" });
  assert.equal(await restored.get(id, { includeInactive: true }), undefined);
  assert.deepEqual((await restored.list()).items, []);
  assert.equal((await restored.history(id)).at(-1).event, "DELETE");
  await restored.add("我喜欢简短回复。", { source, key: "add" });
  assert.equal(client.rows.size, 0);
});

test("Mem0 candidates are invisible until activated; scope is immutable", async t => {
  const { client, create } = fixture(t);
  const scope = { user: "user", project: "project" };
  const memory = create({ scope, allowInference: true });
  scope.user = "tampered";
  const [id] = (await memory.extract([{ role: "user", content: "项目使用中文。" }], { source, key: "extract" })).ids;
  assert.equal(await memory.get(id), undefined);
  assert.equal((await memory.get(id, { includeInactive: true })).state, "pending");
  assert.equal((await memory.list({ state: "pending" })).items.length, 1);
  assert.deepEqual(await memory.retrieve(request), []);
  await memory.setState(id, "active", { version: 1, key: "approve" });
  const [hit] = await memory.retrieve(request);
  assert.equal(hit.version, 2); assert.equal(hit.untrusted, true); assert.equal(hit.metadata.inferred, true);
  await memory.setState(id, "disabled", { version: 2, key: "disable" });
  assert.deepEqual(await memory.retrieve(request), []);
  assert.notEqual(client.calls.find(([name]) => name === "add")[1].runId, request.runId);
});

for (const scope of [{ user: "other", project: "project" }, { user: "user", project: "other" }, { user: "user", project: "project", agent: "other" }]) {
  test(`Mem0 every operation is scoped: ${JSON.stringify(scope)}`, async t => {
    const { client, create } = fixture(t);
    const owner = create(), outsider = create({ scope });
    const [id] = (await owner.add("private", { source, key: "add" })).ids;
    const calls = client.calls.length;
    assert.equal(await outsider.get(id, { includeInactive: true }), undefined);
    assert.equal(client.calls.length, calls);
    assert.deepEqual((await outsider.list()).items, []);
    assert.deepEqual(await outsider.retrieve(request), []);
    for (const action of [() => outsider.update(id, "bad", { source, version: 1, key: "update" }),
      () => outsider.delete(id, { version: 1, key: "delete" }), () => outsider.history(id),
      () => outsider.setState(id, "disabled", { version: 1, key: "state" })]) {
      await assert.rejects(action, code("memory_not_found"));
    }
    assert.equal((await owner.get(id)).text, "private");
  });
}

test("Mem0 rejects scope overrides and cross-tenant SDK results", async t => {
  const { client, create } = fixture(t);
  const owner = create(), outsider = create({ scope: { user: "foreign", project: "project" } });
  const [id] = (await outsider.add("private", { source, key: "add" })).ids;
  client.searchOverride = { results: [structuredClone(client.rows.get(id))] };
  await assert.rejects(owner.retrieve(request), RetrievalError);
  await assert.rejects(owner.retrieve({ ...request, scope: { user: "foreign" } }), code("retrieval_access_denied"));
});

test("Mem0 expiry and external mutations fail closed", async t => {
  const { client, create } = fixture(t);
  const memory = create();
  const [id] = (await memory.add("expired", { source, key: "add", expiresAt: "2000-01-01T00:00:00Z" })).ids;
  assert.equal(await memory.get(id), undefined);
  assert.deepEqual(await memory.retrieve(request), []);
  assert.equal((await memory.get(id, { includeInactive: true })).text, "expired");
  await memory.update(id, "corrected expired", { source, version: 1, key: "update" });
  assert.equal(await memory.get(id), undefined);
  client.rows.get(id).memory = "external overwrite";
  await assert.rejects(memory.get(id, { includeInactive: true }), code("memory_record_changed"));
});

test("Mem0 lost response fences writes until verified, including after restart", async t => {
  const { client, create } = fixture(t);
  const memory = create();
  client.failAdd = true;
  await assert.rejects(memory.add("private", { source, key: "add" }), error => error.code === "memory_sdk_error" && !error.message.includes("secret"));
  assert.equal(memory.operation("add").state, "unknown");
  assert.deepEqual(await memory.retrieve(request), []);
  await assert.rejects(memory.add("more", { source, key: "another" }), code("memory_write_busy"));
  await assert.rejects(memory.add("private", { source, key: "add" }), code("memory_operation_unresolved"));
  memory.close();
  const restored = create();
  await assert.rejects(restored.reconcile("add"), code("memory_writer_not_stopped"));
  const result = await restored.reconcile("add", { writerStopped: true });
  assert.equal(result.state, "complete"); assert.equal(result.ids.length, 1); assert.equal(client.rows.size, 1);
});

test("Mem0 unknown updates are hidden and reconciled", async t => {
  const { client, create } = fixture(t); const memory = create();
  const [id] = (await memory.add("original", { source, key: "add" })).ids;
  client.failUpdate = true;
  await assert.rejects(memory.update(id, "corrected", { source, version: 1, key: "update" }));
  await assert.rejects(memory.get(id), code("memory_write_busy"));
  await memory.reconcile("update", { writerStopped: true });
  assert.equal((await memory.get(id)).text, "corrected");
});

test("Mem0 interrupted extraction is not accepted as a complete batch", async t => {
  const { client, create } = fixture(t); const memory = create({ allowInference: true });
  client.failAdd = true;
  await assert.rejects(memory.extract([{ role: "user", content: "candidate" }], { source, key: "extract" }));
  await assert.rejects(memory.reconcile("extract", { writerStopped: true }), code("memory_reconciliation_required"));
  assert.deepEqual(await memory.retrieve(request), []);
  assert.equal((await memory.discardExtraction("extract", { writerStopped: true })).state, "discarded");
  assert.equal(client.rows.size, 0);
  await memory.add("safe", { source, key: "next" });
});

test("Mem0 timeout does not cancel or duplicate a dispatched write", async t => {
  const { client, create } = fixture(t); const memory = create({ timeoutMs: 10 });
  let release; client.gate = new Promise(resolve => { release = resolve; });
  try {
    await assert.rejects(memory.add("slow", { source, key: "add" }), code("memory_timeout"));
    assert.equal(memory.operation("add").state, "running");
    assert.throws(() => memory.close(), code("memory_operations_in_flight"));
    await assert.rejects(memory.reconcile("add", { writerStopped: true }), code("memory_writer_not_stopped"));
    await assert.rejects(create().add("competing", { source, key: "other" }), code("memory_write_busy"));
  } finally { release(); await memory.drain(); }
  assert.equal(memory.operation("add").state, "complete"); assert.equal(client.rows.size, 1);
});

test("Mem0 abort before and after SDK dispatch", async t => {
  const { client, create } = fixture(t); const memory = create();
  const before = new AbortController(); before.abort();
  await assert.rejects(memory.add("no", { source, key: "before", signal: before.signal }), code("memory_cancelled"));
  assert.equal(memory.operation("before"), undefined); assert.equal(client.calls.length, 0);
  let release; client.gate = new Promise(resolve => { release = resolve; });
  const after = new AbortController();
  const task = memory.add("yes", { source, key: "after", signal: after.signal });
  try {
    await new Promise(resolve => setImmediate(resolve)); after.abort();
    await assert.rejects(task, code("memory_cancelled"));
  } finally { release(); await memory.drain(); }
  assert.equal(memory.operation("after").state, "complete");
});

test("Mem0 tool and direct context preserve complete-record budget and receipts", async t => {
  const { create } = fixture(t); const memory = create();
  const [small] = (await memory.add("简短回复", { source, key: "small" })).ids;
  await memory.add("很长的内容".repeat(100), { source, key: "large" });
  const tool = new RetrieverTool({ retriever: memory, name: "recall", description: "Recall memory." });
  const result = await tool.definition.run({ query: "偏好" }, { runId: "run" });
  assert.equal(result.content.hits.length, 2);
  const context = new MemoryContext({ memory, query: () => "偏好", countTokens: text => Buffer.byteLength(text), limit: 8 });
  const bundle = await context.buildContext({ messages: [] }, { contextAllocations: { memory: 200 } });
  const [block] = bundle.blocks;
  assert.equal(block.untrusted, true); assert.ok(block.tokenCount <= 200);
  assert.deepEqual(JSON.parse(block.content).map(row => row.id), [small]);
  assert.equal(block.evidence.length, 1); assert.equal(block.evidence[0].itemId, small);
});

test("Mem0 inference and input validation are explicit", async t => {
  const { create } = fixture(t); const memory = create();
  await assert.rejects(memory.extract([{ role: "user", content: "text" }], { source, key: "extract" }), code("memory_inference_disabled"));
  await assert.rejects(memory.add("text", { source, key: "add", expiresAt: "2026-01-01" }), TypeError);
  assert.equal(memory.operation("add"), undefined);
});

for (const entry of sharedFixture.scopes) {
  test(`Mem0 shared scope encoding: ${JSON.stringify(entry.scope)}`, async t => {
    const { client, create } = fixture(t);
    await create({ scope: entry.scope }).add("fact", { source, key: "add" });
    assert.equal(client.calls.find(([k]) => k === "add")[1].userId, entry.namespace);
  });
}
for (const expiresAt of sharedFixture.invalid_expiry) {
  test(`Mem0 rejects invalid expiry: ${expiresAt}`, async t => {
    const { client, create } = fixture(t);
    await assert.rejects(create().add("fact", { source, key: "add", expiresAt }), TypeError);
    assert.equal(client.calls.length, 0);
  });
}
test("Mem0 list pagination and over-limit SDK results", async t => {
  const { client, create } = fixture(t); const memory = create({ maxResults: 1 });
  const [first] = (await memory.add("first", { source, key: "1" })).ids;
  const [second] = (await memory.add("second", { source, key: "2" })).ids;
  assert.deepEqual((await memory.list({ limit: 1 })).items.map(r => r.id), [first]);
  assert.deepEqual((await memory.list({ limit: 1, after: first })).items.map(r => r.id), [second]);
  client.searchOverride = { results: structuredClone([...client.rows.values()]) };
  await assert.rejects(memory.retrieve({ ...request, limit: 1 }), RetrievalError);
});

test("Mem0 process crash after SDK commit recovers without replay", async t => {
  const { client, create, path } = fixture(t);
  const program = `
    import { writeFileSync } from "node:fs";
    const { Mem0Memory } = await import(process.argv[1]);
    const client = {
      async add(text, options) {
        writeFileSync(process.argv[3], JSON.stringify({id:"crashed-id", memory:text,
          user_id:options.userId, run_id:options.runId, metadata:options.metadata}));
        process.exit(17);
      },
      async get() {}, async getAll() {}, async search() {}, async update() {}, async delete() {}, async history() {},
    };
    const memory = new Mem0Memory({client, scope:{user:"user",project:"project"}, journalPath:process.argv[2]});
    await memory.add("crash-safe", {source:{id:"source",revision:"1"},key:"crash"});
  `;
  const snapshot = join(path, "sdk.json");
  const crashed = spawnSync(process.execPath, ["--input-type=module", "-e", program,
    new URL("../dist/index.js", import.meta.url).href, join(path, "journal.db"), snapshot], { timeout: 10_000, encoding: "utf8" });
  assert.equal(crashed.status, 17, crashed.stderr);
  const row = JSON.parse(readFileSync(snapshot, "utf8")); client.rows.set(row.id, row);
  const memory = create();
  assert.equal(memory.operation("crash").state, "running");
  await assert.rejects(memory.add("competing", {source, key:"other"}), code("memory_write_busy"));
  const recovered = await memory.reconcile("crash", {writerStopped:true});
  assert.equal(recovered.state, "complete");
  assert.equal((await memory.get(recovered.ids[0])).text, "crash-safe");
  assert.ok(!client.calls.some(([name]) => name === "add"));
});

function memoryEvidence(hit) {
  return { evidenceId: hit.metadata.evidenceId, source: hit.source, itemId: hit.id, version: String(hit.version) };
}

test("source revision withdrawal persists, stays scoped, and prevents reingestion", async t => {
  const { client, create } = fixture(t);
  const memory = create({ allowInference: true });
  const retired = { id: "来源😺", revision: "01" };
  const original = await memory.add("old", { source: retired, key: "old" });
  const [good] = (await memory.add("new", { source: { id: retired.id, revision: "1" }, key: "new" })).ids;
  const pending = await memory.extract([{ role: "user", content: "pending" }], { source: retired, key: "pending" });
  const receipt = memoryEvidence((await memory.retrieve({ ...request, limit: 1 }))[0]);
  const epoch = memory.epoch, calls = structuredClone(client.calls);
  const revoked = await memory.revokeSource(retired.id, { revision: retired.revision, key: "revoke" });
  assert.equal(revoked.state, "complete"); assert.deepEqual(revoked.ids, []);
  assert.equal(revoked.usage.llmCalls, 0); assert.equal(revoked.usage.embeddingCalls, 0); assert.equal(revoked.usage.unreportedCalls, 0);
  assert.deepEqual(client.calls, calls); assert.equal(memory.epoch, epoch + 1);
  assert.deepEqual(await memory.revokeSource(retired.id, { revision: retired.revision, key: "revoke" }), revoked);
  await memory.revokeSource(retired.id, { revision: retired.revision, key: "revoke-again" });
  assert.equal(memory.epoch, epoch + 1);
  assert.equal(memory.isSourceRevoked(retired), true);
  assert.equal(memory.isSourceRevoked({ id: retired.id, revision: "1" }), false);
  assert.equal(create({ scope: { user: "other", project: "project" } }).isSourceRevoked(retired), false);
  assert.equal(await memory.get(original.ids[0], { includeInactive: true }), undefined);
  assert.equal(await memory.get(pending.ids[0], { includeInactive: true }), undefined);
  assert.deepEqual((await memory.list({ state: "pending" })).items, []);
  assert.deepEqual((await memory.list({ limit: 1 })).items.map(r => r.id), [good]);
  assert.deepEqual((await memory.retrieve({ ...request, limit: 1 })).map(r => r.id), [good]);
  await assert.rejects(memory.validateEvidence([receipt]), code("memory_context_stale"));
  const beforeRejected = structuredClone(client.calls);
  for (const [kind, action] of [
    ["add", () => memory.add("old", { source: retired, key: "blocked-add" })],
    ["extract", () => memory.extract([{ role: "user", content: "old" }], { source: retired, key: "blocked-extract" })],
    ["update", () => memory.update(good, "old", { source: retired, version: 1, key: "blocked-update" })],
    ["state", () => memory.setState(original.ids[0], "active", { version: 1, key: "blocked-state" })],
  ]) {
    await assert.rejects(action, code("memory_source_revoked"));
    if (kind === "state") assert.equal(memory.operation("blocked-" + kind), undefined);
    else assert.equal(memory.operation("blocked-" + kind).state, "failed");
  }
  assert.deepEqual(client.calls, beforeRejected);
  assert.deepEqual(await memory.add("old", { source: retired, key: "old" }), original);
  memory.close();
  const restored = create();
  assert.equal(restored.isSourceRevoked(retired), true);
  assert.deepEqual(await restored.revokeSource(retired.id, { revision: retired.revision, key: "revoke" }), revoked);
  await assert.rejects(restored.revokeSource(retired.id, { key: "revoke" }), code("memory_idempotency_conflict"));
  assert.ok((await restored.history(original.ids[0])).length); // host audit, not erasure
  await restored.delete(original.ids[0], { version: 1, key: "cleanup" });
  assert.equal(client.rows.has(original.ids[0]), false);
});

test("full source withdrawal covers future revisions; literal star is an opaque version", async t => {
  const { create } = fixture(t); const memory = create();
  await memory.revokeSource("source", { revision: "*", key: "star" });
  assert.equal(memory.isSourceRevoked({ id: "source", revision: "*" }), true);
  assert.equal(memory.isSourceRevoked({ id: "source", revision: "future" }), false);
  await memory.revokeSource("source", { key: "all" });
  assert.equal(memory.isSourceRevoked({ id: "source", revision: "future" }), true);
  await assert.rejects(memory.add("future", { source: { id: "source", revision: "future" }, key: "new" }), code("memory_source_revoked"));
});

test("explicit source correction replaces content and invalidates old evidence", async t => {
  const { create } = fixture(t); const memory = create();
  const [id] = (await memory.add("旧事实", { source, key: "old" })).ids;
  const old = memoryEvidence((await memory.retrieve(request))[0]);
  await memory.revokeSource(source.id, { revision: source.revision, key: "withdraw" });
  const replacement = { id: source.id, revision: "corrected" };
  await memory.update(id, "纠正后的事实", { source: replacement, version: 1, key: "correct" });
  assert.deepEqual((await memory.get(id)).source, replacement);
  await assert.rejects(memory.validateEvidence([old]), code("memory_context_stale"));
  const current = memoryEvidence((await memory.retrieve(request))[0]);
  await memory.validateEvidence([current]);
  await memory.setState(id, "disabled", { version: 2, key: "disable" });
  await assert.rejects(memory.validateEvidence([current]), code("memory_context_stale"));
});

test("withdrawal does not wait for or release an in-flight writer", async t => {
  const { create, client } = fixture(t); const memory = create({ timeoutMs: 20 });
  let release; client.gate = new Promise(resolve => { release = resolve; });
  try {
    await assert.rejects(memory.add("late", { source, key: "late" }), code("memory_timeout"));
    await memory.revokeSource(source.id, { key: "withdraw" });
    assert.equal(memory.operation("late").state, "running");
    await assert.rejects(memory.add("other", { source: { id: "other", revision: "1" }, key: "busy" }), code("memory_write_busy"));
  } finally { release(); await memory.drain(); }
  const result = memory.operation("late");
  assert.equal(result.state, "complete"); assert.equal(await memory.get(result.ids[0]), undefined);
  assert.deepEqual(await memory.retrieve(request), []);
});

test("reconciliation cannot resurrect a revoked source", async t => {
  const { create, client } = fixture(t); const memory = create();
  client.failAdd = true;
  await assert.rejects(memory.add("lost", { source, key: "lost" }), code("memory_sdk_error"));
  await memory.revokeSource(source.id, { key: "withdraw" });
  const recovered = await memory.reconcile("lost", { writerStopped: true });
  assert.equal(recovered.state, "complete"); assert.equal(await memory.get(recovered.ids[0]), undefined);
  assert.deepEqual((await memory.list()).items, []);
});

for (const kind of ["get", "list", "retrieve"]) test(`${kind} fails closed on withdrawal during a read`, async t => {
  const { create, client } = fixture(t); const memory = create();
  const [id] = (await memory.add("private", { source, key: "add" })).ids;
  let entered, release;
  const started = new Promise(resolve => { entered = resolve; });
  const gate = new Promise(resolve => { release = resolve; });
  const original = client.get.bind(client);
  client.get = async itemId => { const result = await original(itemId); entered(); await gate; return result; };
  const task = kind === "get" ? memory.get(id) : kind === "list" ? memory.list() : memory.retrieve(request);
  const rejected = assert.rejects(task, code(kind === "retrieve" ? "retrieval_source_unavailable" : "memory_context_stale"));
  try { await started; await create().revokeSource(source.id, { key: "withdraw" }); }
  finally { release(); }
  await rejected;
});

test("evidence verifies store identity and expiry without an epoch mutation", async t => {
  const { create, client, path } = fixture(t); const memory = create();
  await memory.add("temporary", { source, key: "temp", expiresAt: new Date(Date.now() + 60_000).toISOString() });
  const receipt = memoryEvidence((await memory.retrieve(request))[0]);
  await memory.validateEvidence([receipt]);
  const foreign = new Mem0Memory({ client, scope: { user: "user", project: "project" }, journalPath: join(path, "other.db") });
  try { await assert.rejects(foreign.validateEvidence([receipt]), code("memory_context_stale")); }
  finally { foreign.close(); }
  await assert.rejects(memory.validateEvidence([{ ...receipt, source: "foreign" }]), code("memory_context_stale"));
  const epoch = memory.epoch, now = Date.now();
  t.mock.method(Date, "now", () => now + 120_000);
  await assert.rejects(memory.validateEvidence([receipt]), code("memory_context_stale"));
  assert.equal(memory.epoch, epoch);
});

test("context validates the selected receipts again before returning content", async t => {
  const { create } = fixture(t); const memory = create();
  await memory.add("中文", { source, key: "add" });
  const context = new MemoryContext({ memory, query: () => "中文" });
  const budget = { contextAllocations: { memory: 200 } };
  const { blocks } = await context.buildContext({ messages: [] }, budget);
  await memory.validateEvidence(blocks[0].evidence);
  const original = memory.retrieve.bind(memory);
  memory.retrieve = async (...args) => { const hits = await original(...args); await memory.revokeSource(source.id, { key: "withdraw" }); return hits; };
  await assert.rejects(context.buildContext({ messages: [] }, budget), code("memory_context_stale"));
});

test("source withdrawal and its receipt survive process exit", async t => {
  const { create, path } = fixture(t);
  const program = `
    const { Mem0Memory } = await import(process.argv[1]);
    const noSdk = async () => { throw Error('no SDK call'); };
    const client = {add:noSdk,get:noSdk,getAll:noSdk,search:noSdk,update:noSdk,delete:noSdk,history:noSdk};
    const memory = new Mem0Memory({client,scope:{user:'user',project:'project'},journalPath:process.argv[2]});
    await memory.revokeSource('来源😺',{key:'withdraw'}); process.exit(17);
  `;
  const result = spawnSync(process.execPath, ["--input-type=module", "-e", program, new URL("../dist/index.js", import.meta.url).href, join(path, "journal.db")]);
  assert.equal(result.status, 17, result.stderr?.toString());
  const memory = create();
  assert.equal(memory.isSourceRevoked({ id: "来源😺", revision: "future" }), true);
  assert.equal(memory.operation("withdraw").state, "complete");
});

async function resolutionPair(memory, texts = ["old", "new"]) {
  const a = await memory.add(texts[0], { source: { id: "a", revision: "1" }, key: "a" });
  const b = await memory.extract([{ role: "user", content: texts[1] }], { source: { id: "b", revision: "1" }, key: "b" });
  return [{ id: a.ids[0], version: 1 }, { id: b.ids[0], version: 1 }];
}
for (const scenario of sharedFixture.resolutions) test(`atomic resolution: ${scenario.kind}`, async t => {
  const { client, create } = fixture(t), memory = create({ allowInference: true });
  const items = await resolutionPair(memory, scenario.texts);
  const [hit] = await memory.retrieve(request);
  const oldEvidence = memoryEvidence(hit);
  const resolution = { kind: scenario.kind, items, ...(scenario.keep === null ? {} : { keep: items[scenario.keep].id }) };
  const rows = structuredClone(client.rows), histories = structuredClone(client.histories), epoch = memory.epoch;
  const receipt = await memory.resolve(resolution, { key: "decision" });
  assert.equal(receipt.state, "complete"); assert.deepEqual(receipt.resolution, resolution);
  assert.deepEqual(client.rows, rows); assert.deepEqual(client.histories, histories);
  assert.equal(memory.epoch, epoch + 1);
  const visible = resolution.keep ? [resolution.keep] : [];
  assert.deepEqual((await memory.retrieve(request)).map(h => h.id), visible);
  assert.deepEqual((await memory.list({ limit: 1 })).items.map(r => r.id), visible);
  for (const ref of items) {
    const record = await memory.get(ref.id, { includeInactive: true });
    assert.equal(record.version, 2); assert.equal(record.resolutionKey, "decision");
    assert.equal(record.state, record.id === resolution.keep ? "active" : "disabled");
    assert.equal(record.source.id, ref === items[0] ? "a" : "b");
  }
  assert.equal((await memory.list({ state: "disabled" })).items.length, resolution.keep ? 1 : 2);
  await assert.rejects(memory.validateEvidence([oldEvidence]), { code: "memory_context_stale" });
  const calls = client.calls.length;
  assert.deepEqual(await memory.resolve(resolution, { key: "decision" }), receipt);
  assert.equal(client.calls.length, calls); assert.equal(memory.epoch, epoch + 1);
  memory.close(); const reopened = create();
  assert.deepEqual(reopened.operation("decision"), receipt);
  assert.equal((await reopened.get(items[0].id, { includeInactive: true })).resolutionKey, "decision");
  await assert.rejects(reopened.resolve({ kind: "duplicate", items, keep: items[scenario.keep === 0 ? 1 : 0].id }, { key: "decision" }), { code: "memory_idempotency_conflict" });
});

test("resolution keeps CAS and source withdrawal intact across later correction", async t => {
  const { client, create } = fixture(t), memory = create({ allowInference: true }), items = await resolutionPair(memory);
  await memory.resolve({ kind: "supersede", items, keep: items[1].id }, { key: "replace" });
  await assert.rejects(memory.setState(items[0].id, "active", { version: 1, key: "stale" }), { code: "memory_version_conflict" });
  await memory.revokeSource("b", { key: "withdraw" });
  assert.deepEqual(await memory.retrieve(request), []); // no silent fallback
  await assert.rejects(memory.resolve({ kind: "duplicate", items: items.map(r => ({ ...r, version: 2 })), keep: items[0].id }, { key: "invalid" }), { code: "memory_source_revoked" });
  client.failUpdate = true;
  await assert.rejects(memory.update(items[1].id, "corrected", { source: { id: "c", revision: "1" }, version: 2, key: "correct" }));
  assert.equal(memory.operation("correct").state, "unknown");
  await memory.reconcile("correct", { writerStopped: true });
  const current = await memory.get(items[1].id);
  assert.equal(current.version, 3); assert.equal(current.resolutionKey, undefined); assert.equal(current.source.id, "c");
  assert.equal(memory.operation("replace").resolution.kind, "supersede");
  await memory.delete(items[0].id, { version: 2, key: "delete" });
  assert.ok((await memory.history(items[0].id)).length);
});

test("quarantined conflict can be reviewed without generating a merged fact", async t => {
  const { create } = fixture(t), memory = create({ allowInference: true }), items = await resolutionPair(memory);
  await memory.resolve({ kind: "conflict", items }, { key: "quarantine" });
  assert.deepEqual(await memory.retrieve(request), []);
  await memory.resolve({ kind: "supersede", items: items.map(r => ({ ...r, version: 2 })), keep: items[1].id }, { key: "accept" });
  const [hit] = await memory.retrieve(request);
  assert.equal(hit.id, items[1].id); assert.equal(hit.content, "new"); assert.equal(hit.version, 3);
});

test("invalid, expired, foreign or tampered resolutions cannot partly publish", async t => {
  const { client, create } = fixture(t), memory = create({ allowInference: true }), items = await resolutionPair(memory);
  for (const refs of [[items[0], { id: "foreign", version: 1 }], [items[0], { ...items[1], version: 2 }]]) {
    await assert.rejects(memory.resolve({ kind: "conflict", items: refs }, { key: "invalid" }));
    assert.equal(memory.operation("invalid"), undefined); assert.equal((await memory.get(items[0].id)).version, 1);
  }
  await assert.rejects(create({ scope: { user: "other", project: "project" } }).resolve({ kind: "conflict", items }, { key: "foreign" }), { code: "memory_not_found" });
  const expired = await memory.add("expired", { source: { id: "expired", revision: "1" }, key: "expired", expiresAt: "2020-01-01T00:00:00Z" });
  await assert.rejects(memory.resolve({ kind: "conflict", items: [items[0], { id: expired.ids[0], version: 1 }] }, { key: "expired-review" }), { code: "memory_context_stale" });
  for (const value of [{ kind: "duplicate", items }, { kind: "conflict", items, keep: items[0].id },
    { kind: "duplicate", items: [items[0], items[0]], keep: items[0].id }, { kind: "conflict", items: [items[0], { ...items[1], version: true }] }]) {
    await assert.rejects(memory.resolve(value, { key: "malformed" }), TypeError);
  }
  client.rows.get(items[1].id).memory = "external tampering";
  await assert.rejects(memory.resolve({ kind: "conflict", items }, { key: "tampered" }), { code: "memory_record_changed" });
});

test("SQL failure rolls back the whole resolution, receipt and epoch", async t => {
  const { create, path } = fixture(t), memory = create({ allowInference: true }), items = await resolutionPair(memory);
  const db = new DatabaseSync(join(path, "journal.db")), epoch = memory.epoch;
  try {
    db.exec("CREATE TRIGGER fault BEFORE UPDATE ON purra_mem0_items WHEN OLD.id='id-0002' BEGIN SELECT RAISE(ABORT,'injected'); END");
    const resolution = { kind: "supersede", items, keep: items[1].id };
    await assert.rejects(memory.resolve(resolution, { key: "atomic" }), { code: "memory_sdk_error" });
    assert.equal(memory.operation("atomic"), undefined); assert.equal(memory.epoch, epoch);
    assert.deepEqual(await Promise.all(items.map(async r => (await memory.get(r.id, { includeInactive: true })).version)), [1, 1]);
    db.exec("DROP TRIGGER fault");
    assert.equal((await memory.resolve(resolution, { key: "atomic" })).state, "complete");
  } finally { db.close(); }
});

test("competing resolutions cannot both commit against the same versions", async t => {
  const { create } = fixture(t), memory = create({ allowInference: true }), items = await resolutionPair(memory);
  const outputs = await Promise.allSettled(Array.from({ length: 6 }, (_, i) => create().resolve({ kind: "duplicate", items, keep: items[i % 2].id }, { key: `review-${i}` })));
  assert.equal(outputs.filter(r => r.status === "fulfilled").length, 1);
  assert.equal((await memory.retrieve(request)).length, 1);
  assert.deepEqual(await Promise.all(items.map(async r => (await memory.get(r.id, { includeInactive: true })).version)), [2, 2]);
});

for (const event of ["withdraw", "write", "cancel"]) test(`${event} during SDK verification prevents resolution`, async t => {
  const { client, create } = fixture(t), memory = create({ allowInference: true }), items = await resolutionPair(memory);
  let enter, release;
  const entered = new Promise(r => { enter = r; }), gate = new Promise(r => { release = r; }), stop = new AbortController();
  const original = client.get.bind(client);
  client.get = async id => { if (id === items[1].id) { enter(); await gate; } return original(id); };
  const task = memory.resolve({ kind: "supersede", items, keep: items[1].id }, { key: "review", signal: stop.signal });
  const rejected = assert.rejects(task);
  try {
    await entered;
    if (event === "withdraw") await create().revokeSource("a", { key: "withdraw" });
    else if (event === "write") await create().update(items[0].id, "changed", { source: { id: "a", revision: "2" }, version: 1, key: "change" });
    else stop.abort();
    release(); await rejected;
  } finally { release(); await memory.drain(); }
  assert.equal(memory.operation("review"), undefined);
  assert.equal((await memory.get(items[1].id, { includeInactive: true })).state, "pending");
});

test("unknown SDK writer fences resolution until read-back reconciliation", async t => {
  const { client, create } = fixture(t), memory = create({ allowInference: true }), items = await resolutionPair(memory);
  client.failAdd = true;
  await assert.rejects(memory.add("uncertain", { source: { id: "c", revision: "1" }, key: "lost" }));
  await assert.rejects(memory.resolve({ kind: "conflict", items }, { key: "review" }), { code: "memory_write_busy" });
  assert.equal(memory.operation("lost").state, "unknown"); assert.equal(memory.operation("review"), undefined);
  await memory.reconcile("lost", { writerStopped: true });
  assert.equal((await memory.resolve({ kind: "conflict", items }, { key: "review" })).state, "complete");
});

test("resolution survives process exit without mutating the SDK", async t => {
  const { client, create, path } = fixture(t), memory = create({ allowInference: true }), items = await resolutionPair(memory);
  const snapshot = join(path, "sdk.json"); writeFileSync(snapshot, JSON.stringify(Object.fromEntries(client.rows)));
  const script = `
    import {readFileSync} from 'node:fs';
    import {Mem0Memory} from ${JSON.stringify(new URL("../dist/index.js", import.meta.url).href)};
    const no = async () => {throw Error('no mutations')};
    const client = {get:async id=>JSON.parse(readFileSync(process.argv[2],'utf8'))[id],add:no,getAll:no,search:no,update:no,delete:no,history:no};
    const m = new Mem0Memory({client,scope:{user:'user',project:'project'},journalPath:process.argv[1]});
    await m.resolve({kind:'supersede',items:[{id:'id-0001',version:1},{id:'id-0002',version:1}],keep:'id-0002'},{key:'durable'});
    process.exit(17);`;
  const result = spawnSync(process.execPath, ["--input-type=module", "-e", script, join(path, "journal.db"), snapshot], { encoding: "utf8", timeout: 10_000 });
  assert.equal(result.status, 17, result.stderr);
  assert.equal(create().operation("durable").resolution.keep, items[1].id);
  assert.deepEqual((await memory.retrieve(request)).map(h => h.id), [items[1].id]);
});

test("pending metadata and control changes survive restart without SDK writes", async t => {
  const { client, create } = fixture(t);
  const memory = create();
  const [id] = (await memory.add("未确认事实", { source, key: "create", state: "pending", metadata: { kind: "plot", pinned: false } })).ids;
  assert.equal(await memory.get(id), undefined);
  const item = await memory.get(id, { includeInactive: true });
  assert.equal(item.state, "pending"); assert.equal(item.inferred, false); assert.ok(item.createdAt);
  assert.equal(client.rows.get(id).metadata.purra_state, "pending");
  assert.throws(() => { item.metadata.pinned = true; }, TypeError);
  const before = client.histories.get(id).length;
  const annotated = await memory.annotate(id, { kind: "plot", pinned: true }, { version: 1, key: "pin" });
  await memory.setState(id, "active", { version: 2, key: "accept" });
  assert.equal(client.histories.get(id).length, before);
  assert.equal((await memory.get(id)).version, 3);
  memory.close();
  const restored = create();
  assert.equal((await restored.get(id)).metadata.pinned, true);
  assert.deepEqual(await restored.annotate(id, { pinned: true, kind: "plot" }, { version: 1, key: "pin" }), annotated);
  await assert.rejects(restored.annotate(id, { pinned: false }, { version: 1, key: "pin" }), code("memory_idempotency_conflict"));
  await restored.update(id, "更正", { source: { id: source.id, revision: "4" }, version: 3, key: "correct" });
  assert.equal((await restored.get(id)).metadata.pinned, true);
  assert.equal((await restored.get(id)).createdAt, item.createdAt);
  await restored.setState(id, "disabled", { reason: "archived", version: 4, key: "archive" });
  assert.equal((await restored.get(id, { includeInactive: true })).reason, "archived");
  assert.equal(await restored.get(id), undefined);
});

test("filtered pages advance even when a bounded scan finds no matches", async t => {
  const { create } = fixture(t), memory = create(), ids = [];
  for (let index = 0; index < 5; index++) ids.push(...(await memory.add(`记录 ${index}`, {
    source, key: `add:${index}`, metadata: { kind: index === 4 ? "plot" : "other" },
  })).ids);
  const first = await memory.list({ filters: { kind: "plot" }, limit: 1, scanLimit: 2 });
  assert.deepEqual(first.items, []); assert.equal(first.next, ids[1]);
  const second = await memory.list({ filters: { kind: ["plot"] }, limit: 1, scanLimit: 2, after: first.next });
  assert.deepEqual(second.items, []); assert.equal(second.next, ids[3]);
  const final = await memory.list({ filters: { kind: "plot" }, limit: 1, scanLimit: 2, after: second.next });
  assert.deepEqual(final.items.map(r => r.id), [ids[4]]); assert.equal(final.next, null);
  assert.deepEqual((await memory.list({ source: "other", query: "记录" })).items, []);
});

test("control rejects stale versions, revoked sources, and unknown writers", async t => {
  const { client, create } = fixture(t), memory = create();
  const [id] = (await memory.add("事实", { source, key: "add" })).ids;
  await memory.annotate(id, { kind: "plot" }, { version: 1, key: "classify" });
  await assert.rejects(memory.setState(id, "disabled", { version: 1, key: "stale" }), code("memory_version_conflict"));
  client.failAdd = true;
  await assert.rejects(memory.add("unknown", { source, key: "unknown" }));
  await assert.rejects(memory.annotate(id, {}, { version: 2, key: "blocked" }), code("memory_write_busy"));
  await memory.reconcile("unknown", { writerStopped: true });
  await memory.revokeSource(source.id, { key: "revoke" });
  await assert.rejects(memory.setState(id, "active", { version: 2, key: "revoked" }), code("memory_source_revoked"));
});

for (const metadata of [{ purra_scope: "foreign" }, { x: NaN }, { x: [] }, JSON.parse('{"__proto__":"unsafe"}'), { n: 2 ** 53 }]) {
  test(`metadata rejects control fields and non-scalar values: ${JSON.stringify(metadata)}`, async t => {
    const { client, create } = fixture(t), memory = create();
    await assert.rejects(memory.add("fact", { source, key: "invalid", metadata }), TypeError);
    assert.deepEqual(client.calls, []); assert.equal(memory.operation("invalid"), undefined);
  });
}

test("links preserve visibility and audit stale or revoked endpoints", async t => {
  const { client, create } = fixture(t), memory = create();
  const [a] = (await memory.add("事实 A", { source, key: "a" })).ids;
  const [b] = (await memory.add("事实 B", { source, key: "b" })).ids;
  const from = { id: a, version: 1 }, to = { id: b, version: 1 };
  const before = structuredClone(client.histories);
  const operation = await memory.link(from, to, "supports", { key: "link", note: "人工确认" });
  assert.equal(operation.usage.embeddingCalls, 0); assert.equal(operation.usage.llmCalls, 0);
  assert.equal((await memory.get(a)).version, 1); assert.deepEqual(client.histories, before);
  assert.equal((await memory.links(a)).items[0].valid, true);
  memory.close();
  const restored = create();
  assert.deepEqual(await restored.link(from, to, "supports", { key: "link", note: "人工确认" }), operation);
  const outsider = create({ scope: { user: "foreign", project: "project" } });
  assert.deepEqual((await outsider.links(a)).items, []);
  await assert.rejects(outsider.link(from, to, "supports", { key: "foreign" }), code("memory_not_found"));
  await restored.annotate(a, { pinned: true }, { version: 1, key: "pin" });
  assert.equal((await restored.links(a)).items[0].valid, false);
  await assert.rejects(restored.link(from, to, "supports", { key: "stale" }), code("memory_version_conflict"));
  await restored.link({ id: a, version: 2 }, to, "relates_to", { key: "link-2" });
  const page = await restored.links(a, { limit: 1 });
  assert.equal(page.next, "link"); assert.equal(page.items[0].valid, false);
  assert.equal((await restored.links(a, { limit: 1, after: page.next })).items[0].valid, true);
  await restored.revokeSource(source.id, { key: "withdraw" });
  assert.ok((await restored.links(a)).items.every(link => !link.valid));
});

test("explicit context reports whole deferred and missing records without search", async t => {
  const { assembleMemoryContext } = await import("../dist/index.js");
  const { client, create } = fixture(t), memory = create();
  const [large] = (await memory.add("长".repeat(3000), { source, key: "large" })).ids;
  const [small] = (await memory.add("短事实", { source, key: "small", metadata: { kind: "plot" } })).ids;
  const [pending] = (await memory.add("待审", { source, key: "pending", state: "pending" })).ids;
  const before = client.calls.length;
  const result = await assembleMemoryContext(memory, [large, small, pending, "missing", small], 300);
  assert.deepEqual(result.included, [small]); assert.deepEqual(result.deferred, [large]); assert.deepEqual(result.missing, [pending, "missing"]);
  assert.equal(result.receipts.length, 1); assert.equal(result.receipts[0].itemId, small);
  assert.ok(result.block.content.includes("短事实")); assert.ok(!result.block.content.includes("长"));
  assert.ok(client.calls.slice(before).every(call => call[0] === "get"));
  const epoch = memory.epoch;
  await memory.setState(small, "disabled", { version: 1, key: "disable" });
  await assert.rejects(assembleMemoryContext(memory, [small], 300, { expectedEpoch: epoch }), code("memory_context_stale"));
});


test("shared management filter contract", async t => {
  const spec = sharedFixture.management, { create } = fixture(t), memory = create(), ids = [];
  for (const [index, metadata] of spec.metadata.entries()) ids.push((await memory.add(`事实 ${index}`, { source, key: `item:${index}`, metadata })).ids[0]);
  for (const item of spec.filters) {
    const result = await memory.list({ filters: item.value });
    assert.deepEqual(result.items.map(record => record.id), item.indices.map(index => ids[index]));
    assert.equal(result.next, null);
  }
});

test("external metadata types cannot masquerade as the verified payload", async t => {
  const { client, create } = fixture(t), memory = create();
  const [id] = (await memory.add("事实", { source, key: "add", metadata: { pinned: true } })).ids;
  client.rows.get(id).metadata.purra_metadata.pinned = 1;
  await assert.rejects(memory.get(id), code("memory_record_changed"));
});
