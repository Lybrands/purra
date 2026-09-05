import assert from "node:assert/strict";
import test from "node:test";
import { StorageSession, STORAGE_STATE_SCHEMA, STORAGE_PORT_METHODS, assertRunRepositoryConforms,
  assertArtifactRepositoryConforms, assertLongTaskRepositoryConforms } from "../dist/index.js";
import { encodeStorageState, decodeStorageState } from "../dist/shared/storage-state.js";

test("storage session reopens all repositories and detached output journals", async () => {
  const session = new StorageSession();
  await assertRunRepositoryConforms(session.stores.runs);
  await assertArtifactRepositoryConforms(session.stores.artifacts);
  await assertLongTaskRepositoryConforms(session.stores.longTasks);
  session.extra.tools.receipt = { state: "done", value: "committed" };
  const snapshot = session.exportSnapshot();
  assert.equal(JSON.parse(snapshot.body).schema, STORAGE_STATE_SCHEMA);
  const events = snapshot.journals.flatMap(j => j.events).sort((a,b) => a.rootRunId.localeCompare(b.rootRunId) || a.rootSequence-b.rootSequence);
  const restored = new StorageSession(snapshot.body, "all", events);
  assert.deepEqual(await restored.stores.runs.get("conformance-run-1"), await session.stores.runs.get("conformance-run-1"));
  assert.deepEqual(await restored.stores.runs.listEvents("conformance-run-1", 0), await session.stores.runs.listEvents("conformance-run-1", 0));
  assert.equal(restored.stores.artifacts.exportState(), session.stores.artifacts.exportState());
  assert.equal(restored.stores.longTasks.exportState(), session.stores.longTasks.exportState());
  assert.deepEqual(restored.extra.tools.receipt, session.extra.tools.receipt);
  assert.ok(!STORAGE_PORT_METHODS.runs.includes("importState"));
  assert.ok(STORAGE_PORT_METHODS.runTree.includes("requireRunClaim"));
});

for (const [name, mutate] of [
  ["schema", s => s.schema = "old"],
  ["missing group", s => delete s.stores.runs],
  ["extra group", s => s.stores.cache = "{}"],
  ["extension type", s => s.extra.tools = []],
  ["group schema", s => { const r=JSON.parse(s.stores.runs); r.schema="old"; s.stores.runs=JSON.stringify(r); }],
]) test(`storage rejects invalid ${name}`, () => {
  const state=JSON.parse(new StorageSession().exportSnapshot().body);
  mutate(state);
  assert.throws(() => new StorageSession(JSON.stringify(state)), TypeError);
});

for (const value of [
  ["object", [["runs", ["array", []]]]],
  ["object", [["runs", ["map", []]], ["cache", ["value", 1]]]],
  ["object", [["runs", ["map", []]], ["runs", ["map", []]]]],
  ["object", [["runs", ["map", [[["value","id"],["value",1]],[["value","id"],["value",2]]]]]]],
]) test("storage rejects malformed fields and duplicate keys", () => {
  assert.throws(() => decodeStorageState(JSON.stringify({schema:"test/v1",value}),"test/v1",{runs:new Map()}), TypeError);
});

test("storage codec preserves data keys without prototype mutation", () => {
  const source=JSON.parse('{"__proto__":{"polluted":true},"constructor":"data"}');
  const restored=decodeStorageState(encodeStorageState("test/v1",{rows:new Map([["key",source]])}),"test/v1",{rows:new Map()});
  assert.deepEqual(restored.rows.get("key"),source);
  assert.equal({}.polluted,undefined);
});

test("unrecognized private Run fields are rejected instead of silently dropped", async () => {
  const session = new StorageSession();
  await assertRunRepositoryConforms(session.stores.runs);
  const snapshot = session.exportSnapshot();
  const state = JSON.parse(snapshot.body);
  const schema = "purra.run-state/v1";
  const saved = decodeStorageState(state.stores.runs, schema, { runs: new Map(), rootEvents: new Map(), rootEventsBySourceKey: new Map(), journalCounts: new Map() });
  saved.runs.values().next().value.privateCache = "unexpected";
  state.stores.runs = encodeStorageState(schema, saved);
  assert.throws(() => new StorageSession(JSON.stringify(state)), /storage record fields/);
});
