import type { JsonValue, ToolCall, ToolSpec } from "../model/types.js";
import type { ContextEvidenceReceipt } from "../context/types.js";
import type { JsonSchema } from "./schema.js";

export type ToolExecutionMode = "read" | "propose" | "confirm";
export type ToolRiskLevel = "read" | "write" | "destructive";
export type ToolEffectState = "not_started" | "committed" | "unknown";
export type ToolPlanningRequirement = "optional" | "required";
export type ToolApprovalStatus = "approved" | "rejected" | "timed_out" | "canceled";

export interface ToolPolicy {
  readonly mode: ToolExecutionMode;
  readonly title: string;
  readonly riskLevel?: ToolRiskLevel;
}

export interface ToolContext {
  readonly call: ToolCall;
  readonly signal?: AbortSignal;
  readonly runId?: string;
  readonly rootRunId?: string;
  readonly agentId?: string;
  readonly parentRunId?: string;
  readonly leaseOwnerId?: string;
  readonly leaseEpoch?: number;
}

export interface ToolHandlerResult {
  readonly content: JsonValue;
  readonly effectState: ToolEffectState;
  readonly errorCode?: string;
  readonly contextEvidence?: readonly ContextEvidenceReceipt[];
  readonly planningDisposition?: "continue" | "replan";
  readonly planningReason?: string;
}

export interface ToolPlanningMetadata {
  readonly capability: ToolSpec;
  readonly prerequisiteTools?: readonly string[];
}

export interface ToolDefinition {
  readonly argumentContract?: import("../structured.js").StructuredOutputContract;
  readonly concurrencySafe?: boolean;
  readonly name: string;
  readonly description: string;
  readonly displayNames?: Readonly<Record<string, string>>;
  readonly inputSchema: JsonSchema;
  readonly policy: ToolPolicy;
  readonly enabled?: boolean;
  readonly cancellationLinearizable?: boolean;
  readonly hostManagedDurability?: boolean;
  /** Required tools cannot execute before a governed plan is admitted. */
  readonly planningRequirement?: ToolPlanningRequirement;
  readonly planning?: ToolPlanningMetadata;
  scope?(input: JsonValue, context: ToolContext): Promise<string | boolean | void> | string | boolean | void;
  run(input: JsonValue, context: ToolContext): Promise<ToolHandlerResult> | ToolHandlerResult;
}

export interface ToolApprovalRequest {
  readonly call: ToolCall;
  readonly title: string;
  readonly riskLevel: ToolRiskLevel;
  readonly summary: string;
}

export interface ToolApprovalGateway {
  request(
    approval: ToolApprovalRequest,
    signal?: AbortSignal,
  ): Promise<ToolApprovalStatus> | ToolApprovalStatus;
}

export interface ToolIdempotencyGateway {
  executeOnce(
    key: string,
    operation: () => Promise<ToolHandlerResult>,
  ): Promise<ToolHandlerResult>;
}

export interface ToolExecutionLimits {
  readonly maxConcurrency?: number;
  readonly maxCallsPerBatch?: number;
  readonly maxResultChars?: number;
  readonly approvalSummaryChars?: number;
  readonly approvalTimeoutMs?: number;
}

export type ToolExecutionEvent =
  | {
      readonly type: "tool_started";
      readonly toolCallId: string;
      readonly toolName: string;
    }
  | {
      readonly type: "tool_completed";
      readonly toolCallId: string;
      readonly toolName: string;
      readonly effectState: ToolEffectState;
      readonly errorCode?: string;
    };

export type ToolExecutionObserver = (event: ToolExecutionEvent) => Promise<void> | void;

export interface ToolBatchResult {
  readonly messages: readonly import("../model/types.js").Message[];
  readonly toolNames: readonly string[];
  readonly failures: readonly {
    readonly errorCode: string;
    readonly effectState: ToolEffectState;
  }[];
  readonly contextEvidence: readonly ContextEvidenceReceipt[];
  readonly replan?: {
    readonly reason: string;
    readonly errorCode?: string;
  };
}
