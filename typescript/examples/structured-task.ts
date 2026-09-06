// Local structured task with one explicit repair; no Provider, Run or Root binding.
import { ModelTaskRunner, StructuredOutputContract, checkIntegration, type ModelCapabilitySnapshot, type ModelGateway } from "purra";
const capabilities: ModelCapabilitySnapshot = {
  schemaVersion: 2,
  profileId: "example:structured-task",
  providerProtocol: "custom",
  contextWindowTokens: 16_000,
  maxGenerationTokens: 512,
  thinkingTokenAccounting: "unknown",
  protocol: {
    reasoningControl: "selectable", reasoningReplay: "ignored", toolCalling: "supported",
    requiredToolChoice: "supported", parallelToolCalls: "supported", streaming: "unavailable",
    cancellation: "supported", assistantContentWithToolCalls: "optional", jsonSchemaLevel: "unknown",
    streamFinishSemantics: "normalized", usageSemantics: "normalized",
  },
};
let calls=0;
const model: ModelGateway={capabilities,async invoke(request){
  calls++;
  return {message:{role:"assistant",content:calls===1?'invalid':'{"ok":true}'},finishReason:"stop",
    appliedGenerationLimit:request.outputBudget.maxGenerationTokens,
    usage:{inputTokens:10,generationTokens:5,totalTokens:15}};
}};
const runner=new ModelTaskRunner({runId:"example",model});
const output=await StructuredOutputContract.create({schemaId:"example",schemaVersion:"1",
  schema:{type:"object",properties:{ok:{type:"boolean"}},required:["ok"],additionalProperties:false}});
const report=await checkIntegration({component:"structured-example",version:"1",checks:[{
  capability:"structured_output",category:"deterministic",async probe(){
    const result=await runner.completeStructured([{role:"user",content:"Return an ok flag."}],{output,repairAttempts:1});
    if (JSON.stringify(result.value)!=='{"ok":true}' || result.receipt.attempts!==2 || calls!==2) throw Error("fixture failed");
    if (result.receipt.persistence!=="none" || result.receipt.rootBudget!=="not_bound") throw Error("missing authority was inferred");
  }
}]});
if(report.checks.find(row=>row.capability==="structured_output"&&row.category==="deterministic")?.status!=="passed") throw Error("fixture failed");
console.log(JSON.stringify(report,null,2));
