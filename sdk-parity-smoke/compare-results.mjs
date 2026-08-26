import { readFile } from "node:fs/promises";

const [pythonPath, typescriptPath] = process.argv.slice(2);
if (pythonPath === undefined || typescriptPath === undefined) {
  throw new Error("usage: node compare-results.mjs PYTHON_JSON TYPESCRIPT_JSON");
}

const [pythonResult, typescriptResult] = await Promise.all([
  readFile(pythonPath, "utf8").then(JSON.parse),
  readFile(typescriptPath, "utf8").then(JSON.parse),
]);

const comparable = ({ scenario, status, output, toolCalls, childRuns }) => ({
  scenario,
  status,
  output,
  toolCalls,
  ...(childRuns === undefined ? {} : { childRuns }),
});
const pythonComparable = comparable(pythonResult);
const typescriptComparable = comparable(typescriptResult);

if (JSON.stringify(pythonComparable) !== JSON.stringify(typescriptComparable)) {
  console.error("Python:", JSON.stringify(pythonResult, null, 2));
  console.error("TypeScript:", JSON.stringify(typescriptResult, null, 2));
  throw new Error("SDK behavior mismatch");
}
if (pythonResult.version !== typescriptResult.version) {
  throw new Error(
    `package version mismatch: Python=${pythonResult.version}, TypeScript=${typescriptResult.version}`,
  );
}

console.log(`PASS Python and TypeScript SDKs are semantically consistent for ${pythonResult.scenario}`);
console.log(`version: ${pythonResult.version}`);
console.log(`tool calls: ${pythonResult.toolCalls.length}`);
if (pythonResult.childRuns !== undefined) {
  console.log(`child runs: ${pythonResult.childRuns}`);
}
if (pythonResult.modelCalls !== typescriptResult.modelCalls) {
  console.log(
    "NOTICE operational parity differs: "
    + `Python model calls=${pythonResult.modelCalls}, `
    + `TypeScript model calls=${typescriptResult.modelCalls}`,
  );
}
