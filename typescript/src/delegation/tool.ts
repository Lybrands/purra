import type { JsonValue } from "../model/types.js";
import { AgentError } from "../shared/errors.js";
import type { ToolContext, ToolDefinition } from "../tools/types.js";
import { DelegationCoordinator } from "./coordinator.js";
import { DelegationPolicy } from "./policy.js";

export function buildDelegationTool(options: {
  readonly coordinator: DelegationCoordinator;
  readonly policy?: DelegationPolicy;
}): ToolDefinition {
  if (!(options.coordinator instanceof DelegationCoordinator)) {
    throw new TypeError("Delegation tool requires the canonical coordinator");
  }
  const policy = options.policy ?? new DelegationPolicy();
  const limits = policy.snapshot();
  return Object.freeze({
    name: "delegateToAgents",
    description: (
      `Create 1-${limits.maxAgentsPerCall} task-specific Agents with isolated context `
      + "and currently enabled read-only tools, then return their attributed results."
    ),
    displayNames: Object.freeze({
      "en-US": "Create and invoke Agents",
      "zh-CN": "创建并调用子 Agent",
    }),
    inputSchema: Object.freeze({
      type: "object",
      properties: {
        delegations: {
          type: "array",
          minItems: 1,
          maxItems: limits.maxAgentsPerCall,
          items: {
            type: "object",
            properties: {
              agentName: { type: "string", minLength: 1, maxLength: limits.maxAgentNameChars },
              title: { type: "string", minLength: 1, maxLength: limits.maxTitleChars },
              instruction: { type: "string", minLength: 1, maxLength: limits.maxInstructionChars },
              objective: { type: "string", minLength: 1, maxLength: limits.maxObjectiveChars },
              input: { type: "object" },
              required: { type: "boolean" },
              priority: { type: "integer" },
            },
            required: ["agentName", "title", "instruction", "objective"],
            additionalProperties: false,
          },
        },
      },
      required: ["delegations"],
      additionalProperties: false,
    }),
    policy: Object.freeze({ mode: "propose", title: "Create and invoke Agents", riskLevel: "write" }),
    hostManagedDurability: true,
    cancellationLinearizable: true,
    async run(input: JsonValue, context: ToolContext) {
      if (context.rootRunId === undefined) {
        throw new AgentError("delegation_root_run_required", "Delegation is available only inside a submitted Root Run");
      }
      const raw = input as Readonly<Record<string, JsonValue>>;
      const aggregation = await options.coordinator.executeCall({
        runId: context.rootRunId,
        idempotencyKey: context.call.id,
        delegations: raw.delegations,
        ...(context.enabledTools === undefined ? {} : { enabledTools: context.enabledTools }),
        ...(context.signal === undefined ? {} : { signal: context.signal }),
      });
      return Object.freeze({
        content: Object.freeze({
          state: aggregation.state,
          counts: aggregation.counts,
          requiredFailures: aggregation.requiredFailures,
          results: aggregation.results,
        }),
        effectState: "committed",
        ...(aggregation.state === "blocked" ? { errorCode: "required_delegation_failed" } : {}),
      });
    },
  });
}
