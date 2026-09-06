// Isolated deterministic store; assertions never operate on production data.
import { InMemoryRunRepository, assertRunRepositoryConforms, checkIntegration, inspectRecovery } from "purra";
const fixture = new InMemoryRunRepository();
const report = await checkIntegration({component:"purra",version:"0.5.0",
  checks:[{capability:"storage",category:"deterministic",probe:()=>assertRunRepositoryConforms(fixture)}]});
if (report.checks.find(row=>row.capability==="storage"&&row.category==="deterministic")?.status !== "passed") throw Error("fixture failed");
const recovery = await inspectRecovery(fixture,"conformance-run-1");
if (recovery.observations.lease !== "unknown") throw Error("missing evidence was inferred");
console.log(JSON.stringify({integration:report,recovery},null,2));
