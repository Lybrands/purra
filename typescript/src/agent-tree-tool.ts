import { AgentError } from "./shared/errors.js";
import { DelegationPolicy } from "./delegation/policy.js";
import type { JsonValue } from "./model/types.js";
import type { ToolContext, ToolDefinition } from "./tools/types.js";
import { RunCommandService } from "./agent-tree-execution.js";

/** Model-visible create-and-wait facade over the canonical Agent tree. */
export function buildAgentTreeTool(options: {
  readonly commands: RunCommandService;
  readonly policy: DelegationPolicy;
  readonly childAllowedTools?: readonly string[];
}): ToolDefinition {
  if (!(options.commands instanceof RunCommandService)) {
    throw new TypeError("Agent tree tool requires RunCommandService");
  }
  if (!(options.policy instanceof DelegationPolicy)) {
    throw new TypeError("Agent tree tool requires DelegationPolicy");
  }
  const limits = options.policy.snapshot();
  const childAllowedTools = Object.freeze([...(options.childAllowedTools ?? [])]);
  return Object.freeze({
    name: "delegateToAgents",
    description: (
      `Create 1-${limits.maxAgentsPerCall} bounded Child Agents, run them `
      + "independently, wait for completion, and return attributed results."
    ),
    displayNames: Object.freeze({
      "en-US": "Create and invoke Child Agents",
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
    policy: Object.freeze({
      mode: "propose",
      title: "Create and invoke Child Agents",
      riskLevel: "write",
    }),
    hostManagedDurability: true,
    cancellationLinearizable: true,
    async run(input: JsonValue, context: ToolContext) {
      const runId = requiredText(context.runId, "Agent tree Run id");
      const raw = input as Readonly<Record<string, JsonValue>>;
      const children = options.policy.validate(raw.delegations).map((item) => ({
        name: item.agentName,
        title: item.title,
        instruction: item.instruction,
        objective: item.objective,
        ...(item.input === undefined ? {} : { input: item.input }),
        ...(item.required === undefined ? {} : { required: item.required }),
        ...(item.priority === undefined ? {} : { priority: item.priority }),
      }));
      const grant = await options.commands.compileChildGrant(runId, {
        canSpawnAgents: limits.allowsRecursiveDelegation,
        allowedTools: childAllowedTools,
      });
      const claim = context.leaseOwnerId === undefined
        ? Object.freeze({})
        : Object.freeze({
            leaseOwnerId: context.leaseOwnerId,
            leaseEpoch: context.leaseEpoch,
          });
      const spawned = await options.commands.spawnAgents({
        parentRunId: runId,
        idempotencyKey: context.call.id,
        ...claim,
        children: children.map((child) => Object.freeze({
          ...child,
          capabilityGrant: grant,
        })),
      });
      const aggregate = await options.commands.joinRuns(
        runId,
        spawned.items.map((item) => item.run.runId),
        context.signal,
        claim,
      );
      return Object.freeze({
        content: Object.freeze({
          state: aggregate.state,
          pendingRunIds: aggregate.pendingRunIds,
          requiredFailures: aggregate.requiredFailures,
          results: aggregate.results,
        }),
        effectState: "committed",
        ...(aggregate.state === "blocked"
          ? { errorCode: "required_delegation_failed" }
          : {}),
      });
    },
  });
}

function requiredText(value: unknown, label: string): string {
  const text = typeof value === "string" ? value.trim() : "";
  if (text === "") throw new AgentError("agent_scope_violation", `${label} is required`);
  return text;
}
