import type { PlanningScope } from "../planning/stream.js";
import type { ContextEvidenceReceipt, PreparedContextSnapshot } from "../context/types.js";
import type {
  InvocationOutputBudget,
  JsonValue,
  Message,
  ModelTokenUsage,
  ToolSpec,
} from "../model/types.js";
import type { OutputEvent, OutputEventQuery } from "../output/types.js";
import type { DurableContinuation, DurableRunResult } from "../durable/types.js";
import type { RecoveryCause } from "../recovery/index.js";
import type { AgentTreePolicySnapshot } from "../agent-tree-policy.js";

export type RunStatus = "running" | "completed" | "failed" | "canceled";
export type PlanningMode = "auto" | "reactive" | "planned";

export interface RunResult {
  readonly output: JsonValue;
  readonly messages: readonly Message[];
  readonly rounds: number;
  readonly durable?: DurableRunResult;
}

export interface PromptSection {
  readonly id: string;
  readonly role: "system" | "developer";
  readonly content: JsonValue;
}

export interface AgentPreset {
  readonly id: string;
  readonly revision: string;
  readonly promptSections?: readonly PromptSection[];
}

export interface RunRequest {
  readonly messages: readonly Message[];
  /** Selects adaptive, direct, or governed execution. Omitted requests use Auto. */
  readonly planningMode?: PlanningMode;
  readonly enabledTools?: readonly string[];
  /** User-selected total generation allowance for each Provider invocation. */
  readonly maxGenerationTokens?: number;
  readonly contextEvidence?: readonly ContextEvidenceReceipt[];
  readonly metadata?: Readonly<Record<string, JsonValue>>;
}

export interface RunBudgetOptions {
  readonly maxModelAttempts?: number | null;
  readonly maxInputTokens?: number | null;
  /** Cumulative Provider generation charged across the entire Run; null is explicit. */
  readonly maxRunGenerationTokens: number | null;
  readonly maxReasoningTokens?: number | null;
  readonly maxOutputBytes?: number | null;
  readonly maxOutputEvents?: number | null;
}

export interface RunBudgets {
  readonly maxModelAttempts: number | null;
  readonly maxInputTokens: number | null;
  readonly maxRunGenerationTokens: number | null;
  readonly maxReasoningTokens: number | null;
  readonly maxOutputBytes: number | null;
  readonly maxOutputEvents: number | null;
}

export interface RunUsage {
  readonly modelAttempts: number;
  readonly unreportedUsageAttempts: number;
  readonly inputTokens: number;
  readonly generationTokens: number;
  readonly reasoningTokens: number;
  readonly unreportedReasoningAttempts: number;
  readonly outputBytes: number;
  readonly outputEvents: number;
}

interface RunOptionBase {
  readonly signal?: AbortSignal;
  readonly deadlineAt?: string | null;
}

export interface NewRunOptions extends RunOptionBase {
  readonly budgets: RunBudgetOptions;
  /** Workflow sizing target; never changes the Provider generation allowance. */
  readonly resultCapacityTargetTokens?: number;
  readonly durableContinuation?: never;
}

export interface ContinuationRunOptions extends RunOptionBase {
  readonly budgets?: never;
  readonly deadlineAt?: never;
  readonly resultCapacityTargetTokens?: never;
  readonly durableContinuation: DurableContinuation;
}

export type RunOptions = NewRunOptions | ContinuationRunOptions;

interface AgentPresetSnapshotBase {
  readonly presetId: string;
  readonly presetRevision: string;
  readonly promptFingerprint: string;
  readonly toolFingerprint: string;
  readonly capabilityProfileId: string | null;
  readonly compositionFingerprint: string;
  readonly runtimeLimits: AgentRuntimeLimitSnapshot;
}

export interface AgentPresetSnapshot extends AgentPresetSnapshotBase {
  readonly schemaVersion: 5;
  readonly agentTree:
    | {
        readonly protocolVersion: 1;
        readonly enabled: false;
      }
    | (AgentTreePolicySnapshot & {
        readonly protocolVersion: 1;
        readonly capabilityGrant: Readonly<Record<string, JsonValue>>;
      });
}

export interface AgentRuntimeLimitSnapshot {
  readonly runTimeoutMs: number | null;
  readonly activityIdleTimeoutMs: number | null;
  readonly progressIdleTimeoutMs: number | null;
  readonly invocationTimeoutMs: number | null;
  readonly maxChunks: number;
  readonly maxContentChars: number;
  readonly maxReasoningChars: number;
  readonly maxToolArgumentChars: number;
}

export interface RunSnapshot {
  readonly runId: string;
  readonly status: RunStatus;
  readonly version: number;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly deadlineAt: string | null;
  readonly budgets: RunBudgets;
  readonly usage: RunUsage;
  readonly preset: AgentPresetSnapshot;
  readonly finalOutput?: JsonValue;
  readonly errorCode?: string;
  readonly executionCheckpoint?: AgentExecutionCheckpoint;
}

export interface AgentExecutionCheckpoint {
  readonly inputRevision?: number;
  readonly schemaVersion: 2;
  readonly runId: string;
  readonly phase: "model_ready";
  readonly executionProfile: "reactive" | "auto" | "planned";
  readonly planning?: import("../planning/coordinator.js").PlanningCheckpoint;
  readonly roundLimit?: number;
  readonly pendingReplan?: { readonly round: number; readonly reason: string; readonly errorCode?: string };
  readonly initialPlanningOpen: boolean;
  readonly nextRound: number;
  readonly messages: readonly Message[];
  readonly context: PreparedContextSnapshot | null;
  readonly contextEvidence: readonly ContextEvidenceReceipt[];
  readonly responseAttempts: number;
  readonly recoveryAttempts: readonly {
    readonly cause: RecoveryCause;
    readonly scope: string;
    readonly attempts: number;
  }[];
}

export interface RunCancellationReceipt {
  readonly runId: string;
  readonly accepted: boolean;
  readonly status: RunStatus;
  readonly event?: OutputEvent;
  readonly events?: readonly OutputEvent[];
}

export interface RunLeaseClaim {
  readonly leaseOwnerId?: string;
  readonly leaseEpoch?: number;
}

export interface RunCommand {
  readonly type: "cancel";
}

export interface RunHandle {
  readonly runId: string;
  readonly result: Promise<RunResult>;
  snapshot(): Promise<RunSnapshot>;
  cancel(): Promise<RunCancellationReceipt>;
  command(command: RunCommand): Promise<RunCancellationReceipt>;
  events(query?: OutputEventQuery): AsyncIterable<OutputEvent>;
}

export interface ModelInvocationReceipt {
  readonly outputProtocol?: "purra.planning-stream/v1";
  readonly planningScope?: PlanningScope;
  readonly planningAttempt?: number;
  readonly schemaVersion: 2;
  readonly runId: string;
  readonly invocationId: string;
  readonly attempt: number;
  readonly messageFingerprint: string;
  readonly toolFingerprint: string;
  readonly requestFingerprint: string;
  readonly evidenceFingerprint: string;
  readonly contextEvidence: readonly ContextEvidenceReceipt[];
  readonly capabilityProfileId: string | null;
  readonly outputBudget: InvocationOutputBudget | null;
  readonly openedAt: string;
}

export interface InvocationSettlement {
  readonly invocationId: string;
  readonly status: "completed" | "failed";
  readonly usage?: ModelTokenUsage;
  readonly errorCode?: string;
}

export interface RunBeginParams {
  readonly requestedRunId?: string;
  readonly rootRunId?: string;
  readonly agentId?: string;
  readonly parentRunId?: string;
  readonly leaseOwnerId?: string;
  readonly leaseEpoch?: number;
  readonly preset: AgentPresetSnapshot;
  readonly deadlineAt: string | null;
  readonly budgets: RunBudgets;
  readonly metadata: Readonly<Record<string, JsonValue>>;
}

export interface InvocationReceiptInput {
  readonly outputProtocol?: "purra.planning-stream/v1";
  readonly planningScope?: PlanningScope;
  readonly planningAttempt?: number;
  readonly messages: readonly Message[];
  readonly tools: readonly ToolSpec[];
  readonly evidence: readonly ContextEvidenceReceipt[];
  readonly capabilityProfileId: string | null;
  readonly outputBudget: InvocationOutputBudget | null;
}
