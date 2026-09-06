// Deterministic gateway and local records; no Provider or external service.
import { Agent, type ModelCapabilitySnapshot, type ModelGateway } from "purra";

const capabilities: ModelCapabilitySnapshot = {
  schemaVersion: 2,
  profileId: "example:parallel-tools",
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

const records: Readonly<Record<string, {count: number}>> = {alpha:{count:2},beta:{count:3}};
let round = 0;
const model: ModelGateway = {
  capabilities,
  async invoke(request) {
    round++;
    return {appliedGenerationLimit:request.outputBudget.maxGenerationTokens,
      message:round===1 ? {role:"assistant",content:"",toolCalls:Object.keys(records).map(key=>({id:key,name:"readRecord",arguments:{key}}))}
        : {role:"assistant",content:"Read both approved records."},
      finishReason:round===1 ? "tool_calls" : "stop"};
  },
};
const agent = new Agent({model,toolLimits:{maxConcurrency:2},tools:[{
  name:"readRecord",description:"Read an approved local record",
  inputSchema:{type:"object",properties:{key:{type:"string"}},required:["key"],additionalProperties:false},
  policy:{mode:"read",title:"Read record"},concurrencySafe:true,
  scope:args=>Object.hasOwn(records,(args as {key:string}).key) ? undefined : "Unknown record",
  async run(args) {return {content:records[(args as {key:string}).key]!,effectState:"not_started"};},
}]});
const result = await agent.invoke({messages:[{role:"user",content:"Read alpha and beta"}]});
console.log(result.messages.filter(message=>message.role==="tool").map(message=>message.toolCallId));
console.log(result.output);
