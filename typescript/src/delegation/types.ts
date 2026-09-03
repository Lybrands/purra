import type { ContextOptions } from "../context/types.js";
import type { ModelInputEvidenceValidator } from "../context/types.js";
import type { AgentOperationController } from "../operations/index.js";
import type { RecoveryPolicy } from "../recovery/index.js";
import type { JsonValue, ModelGateway } from "../model/types.js";
import type { ModelStreamLimits } from "../model/stream.js";
import type { ToolDefinition } from "../tools/types.js";

export type DelegationStatus = "queued" | "running" | "done" | "failed" | "canceled";
export type DelegationContextMode = "isolated";
export type DelegatedAgentOutcome = "completed" | "failed" | "canceled";

export interface DelegationDefinition {
  readonly agentName: string;
  readonly title: string;
  readonly instruction: string;
  readonly objective: string;
  readonly input?: Readonly<Record<string, JsonValue>>;
  readonly required?: boolean;
  readonly priority?: number;
}

export interface DelegationPolicyOptions {
  readonly maxAgentsPerCall?: number;
  readonly maxParallel?: number;
  readonly maxAgentNameChars?: number;
  readonly maxTitleChars?: number;
  readonly maxInstructionChars?: number;
  readonly maxObjectiveChars?: number;
  readonly maxDepth?: number;
  readonly maxAgentsPerRoot?: number;
  readonly allowRecursiveDelegation?: boolean;
}

export interface DelegationPolicySnapshot {
  readonly enabled: true;
  readonly maxAgentsPerCall: number;
  readonly maxParallel: number;
  readonly maxAgentNameChars: number;
  readonly maxTitleChars: number;
  readonly maxInstructionChars: number;
  readonly maxObjectiveChars: number;
  readonly maxDepth: number;
  readonly maxAgentsPerRoot: number;
  readonly contextMode: DelegationContextMode;
  readonly toolMode: "read";
  readonly allowsRecursiveDelegation: boolean;
}

export interface AgentDelegation extends DelegationDefinition {
  readonly id: string;
  readonly batchId: string;
  readonly runId: string;
  readonly idempotencyKey: string;
  readonly input: Readonly<Record<string, JsonValue>>;
  readonly contextMode: DelegationContextMode;
  readonly status: DelegationStatus;
  readonly required: boolean;
  readonly priority: number;
  readonly resultSummary?: JsonValue;
  readonly errorCode?: string;
  readonly createdAt: string;
  readonly updatedAt: string;
}

export interface DelegationBatchCommand {
  readonly runId: string;
  readonly batchId: string;
  readonly idempotencyKey: string;
  readonly delegations: readonly DelegationDefinition[];
}

export interface DelegationBatchReceipt {
  readonly delegations: readonly AgentDelegation[];
  readonly replayed: boolean;
}

export interface DelegationAggregation {
  readonly state: "pending" | "ready" | "blocked";
  readonly counts: Readonly<Record<DelegationStatus, number>>;
  readonly requiredFailures: readonly string[];
  readonly results: readonly {
    readonly delegationId: string;
    readonly agentName: string;
    readonly agentTitle: string;
    readonly summary: JsonValue;
  }[];
}

export interface DelegationRepository {
  createBatch(command: DelegationBatchCommand): Promise<DelegationBatchReceipt>;
  start(delegationId: string, runId: string, batchId: string): Promise<AgentDelegation | undefined>;
  complete(
    delegationId: string,
    runId: string,
    batchId: string,
    resultSummary: JsonValue,
  ): Promise<boolean>;
  fail(delegationId: string, runId: string, batchId: string, errorCode: string): Promise<boolean>;
  cancel(delegationId: string, runId: string, batchId: string, reason: string): Promise<boolean>;
  listForRun(runId: string): Promise<readonly AgentDelegation[]>;
  aggregateBatch(runId: string, batchId: string): Promise<DelegationAggregation>;
  cancelBatch(runId: string, batchId: string, reason?: string): Promise<number>;
}

export interface DelegatedAgentRequest {
  readonly runId: string;
  readonly batchId: string;
  readonly delegationId: string;
  readonly agentName: string;
  readonly agentTitle: string;
  readonly agentInstruction: string;
  readonly objective: string;
  readonly input: Readonly<Record<string, JsonValue>>;
  readonly contextMode: DelegationContextMode;
  readonly enabledTools?: readonly string[];
}

export interface DelegatedAgentResult {
  readonly outcome: DelegatedAgentOutcome;
  readonly content?: JsonValue;
  readonly errorCode?: string;
}

export interface DelegatedAgentExecutor {
  execute(request: DelegatedAgentRequest, signal?: AbortSignal): Promise<DelegatedAgentResult>;
}

export interface DelegationLifecycleEvent {
  readonly runId: string;
  readonly batchId: string;
  readonly delegationId: string;
  readonly agentName: string;
  readonly agentTitle: string;
  readonly status: DelegationStatus;
  readonly errorCode?: string;
}

export type DelegationEventSink = (event: DelegationLifecycleEvent) => Promise<void> | void;

export interface DelegationOptions {
  readonly policy?: DelegationPolicyOptions;
  readonly repository?: DelegationRepository;
  readonly executor?: DelegatedAgentExecutor;
}

export interface DynamicDelegatedAgentExecutorOptions {
  readonly model: ModelGateway;
  readonly runtimeLimits?: ModelStreamLimits;
  readonly tools?: readonly ToolDefinition[];
  readonly context?: ContextOptions;
  readonly maxRounds?: number;
  readonly recovery?: RecoveryPolicy;
  readonly operations?: AgentOperationController;
  readonly evidenceValidator?: ModelInputEvidenceValidator;
}
