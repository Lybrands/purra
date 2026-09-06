import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { StructuredOutputContract, StructuredOutputError } from "../dist/index.js";
import { SCHEMA_KEYWORDS } from "../dist/shared/schema.js";

const fixture = JSON.parse(readFileSync(new URL("../../conformance/fixtures/structured_output.json", import.meta.url), "utf8"));
test("shared keyword inventory", () => assert.deepEqual([...SCHEMA_KEYWORDS].sort(), fixture.keywords));
for (const row of fixture.cases) {
  test(`structured profile: ${row.name}`, async () => {
    let value;
    try {
      const contract = await StructuredOutputContract.create({ schemaId: "test", schemaVersion: "1",
        schema: row.schema, mode: row.mode, limits: row.limits });
      if (row.schemaDigest !== undefined) {
        assert.equal(contract.schemaDigest, row.schemaDigest);
        assert.equal(contract.contractDigest, row.contractDigest);
      }
      value = contract.parse(row.text);
    } catch (error) {
      assert.ok(error instanceof StructuredOutputError, String(error));
      assert.equal(error.code, row.code);
      assert.deepEqual(Object.keys(error.details).sort(), ["keyword", "path", "reason"]);
      assert.ok(!JSON.stringify(error.details).includes("secret-candidate"));
      return;
    }
    assert.equal(row.code, null);
    assert.deepEqual(value, JSON.parse(row.text));
  });
}

test("contract snapshots before asynchronous hashing and freezes results", async () => {
  const schema = { type: "object", properties: { v: { type: "array" } } };
  const options = { schemaId: "test", schemaVersion: "1", schema };
  const pending = StructuredOutputContract.create(options);
  schema.properties.v.type = "string";
  options.mode = "native_required";
  const contract = await pending;
  assert.equal(contract.mode, "local");
  assert.throws(() => { contract.schema.properties.v.type = "number"; }, TypeError);
  const value = contract.parse('{"v":[1]}');
  assert.throws(() => value.v.push(2), TypeError);
});

test("schema identity is structural, numeric and mode bound", async () => {
  const create = (schema, mode) => StructuredOutputContract.create({ schemaId: "test", schemaVersion: "1", schema, mode });
  const a = await create({ type: "object", const: { b: 1.0, a: -0 } });
  const b = await create({ const: { a: 0, b: 1 }, type: "object" });
  const c = await create(b.schema, "native_required");
  assert.equal(a.schemaDigest, b.schemaDigest);
  assert.equal(a.contractDigest, b.contractDigest);
  assert.equal(b.schemaDigest, c.schemaDigest);
  assert.notEqual(b.contractDigest, c.contractDigest);
});

test("cycles and huge nesting fail with bounded diagnostics", async () => {
  const schema = { type: "object" };
  schema.properties = schema;
  await assert.rejects(StructuredOutputContract.create({ schemaId: "test", schemaVersion: "1", schema }), { code: "structured_output_schema_invalid" });
  const contract = await StructuredOutputContract.create({ schemaId: "test", schemaVersion: "1", schema: { type: "object" } });
  assert.throws(() => contract.parse('{"v":' + "[".repeat(10000) + "0" + "]".repeat(10000) + "}"), { code: "structured_output_invalid_json" });
});

test('decoded values share strict profile and resource bounds', async () => {
  const { jsonIdentityDigest } = await import('purra');
  const contract = await StructuredOutputContract.create({schemaId:'decoded',schemaVersion:'1',schema:{type:'object',properties:{n:{type:'integer'}},required:['n']}});
  const value = contract.validateValue({n:1});
  assert.deepEqual(value,contract.parse('{"n":1.0}'));
  assert.equal(await jsonIdentityDigest(value),await jsonIdentityDigest({n:1.0}));
  assert.throws(() => contract.validateValue({n:true}),StructuredOutputError);
  const cyclic = {}; cyclic.self = cyclic;
  assert.throws(() => contract.validateValue(cyclic),StructuredOutputError);
  await assert.rejects(jsonIdentityDigest(cyclic));
});
