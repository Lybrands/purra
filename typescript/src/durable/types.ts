import type { JsonValue, Message, ModelTokenUsage } from "../model/types.js";
import type { ExecutionPlan } from "../planning/types.js";
import type { AgentPresetSnapshot, RunBudgets } from "../run/types.js";
import type { AgentNode, AgentTreeRun } from "../agent-tree.js";

export type ExecutionMode = "inline" | "durable" | "clarify" | "reject";
export type BudgetExhaustionDisposition = "pause_recoverable" | "fail_permanent";
export type LongTaskStatus = "pending" | "running" | "paused" | "completed" | "failed" | "canceled";
export type LongTaskUnitStatus =
  | "pending"
  | "waiting_retry"
  | "claimed"
  | "running"
  | "blocked"
  | "completed"
  | "failed"
  | "canceled";
export type LongTaskRunRelation = "created" | "continuation" | "reference";

export interface ExecutionRecipeStep {
  readonly id: string;
  readonly kind: string;
  readonly dependsOn?: readonly string[];
  readonly inputRef?: string;
  readonly executor: string;
  readonly planStepId: string;
  readonly maxAttempts?: number;
  readonly metadata?: Readonly<Record<string, JsonValue>>;
}

export interface ExecutionRecipe {
  readonly kind: string;
  readonly steps: readonly ExecutionRecipeStep[];
  readonly maxParallelism?: number;
  readonly metadata?: Readonly<Record<string, JsonValue>>;
}

export interface TaskAdmissionDecision {
  readonly mode: ExecutionMode;
  readonly reasonCode: string;
  readonly estimatedUnits?: number;
  readonly estimatedModelCalls?: number;
  readonly requiresConfirmation?: boolean;
  readonly message?: string;
  readonly coveredStepIds?: readonly string[];
  readonly executionRecipe?: ExecutionRecipe;
  readonly metadata?: Readonly<Record<string, JsonValue>>;
}

export interface TaskAdmissionEvaluator {
  evaluate(input: {
    readonly messages: readonly Message[];
    readonly plan: ExecutionPlan;
    readonly signal?: AbortSignal;
  }): Promise<TaskAdmissionDecision> | TaskAdmissionDecision;
}

export interface LongTaskUsage {
  readonly invocationCount: number;
  readonly unreportedUsageAttempts: number;
  readonly inputTokens: number;
  readonly generationTokens: number;
  readonly reasoningTokens: number | null;
}

export interface LongTaskBudgetLimits {
  readonly maxInvocationAttempts: number | null;
  readonly maxInputTokens: number | null;
  readonly maxRunGenerationTokens: number | null;
  readonly maxReasoningTokens: number | null;
}

export interface LongTaskUnitSpec {
  readonly id: string;
  readonly position: number;
  readonly semanticKey?: string;
  readonly dependencies?: readonly string[];
  readonly required?: boolean;
  readonly inputRef?: string;
  readonly executor: string;
  readonly planStepId: string;
  readonly maxAttempts?: number;
  readonly metadata?: Readonly<Record<string, JsonValue>>;
}

export interface LongTaskCreateCommand {
  readonly namespace: string;
  readonly kind: string;
  readonly ownerId: string;
  readonly createdByRunId: string;
  readonly idempotencyKey: string;
  readonly units: readonly LongTaskUnitSpec[];
  readonly maxParallelism?: number;
  readonly deadlineAtMs: number | null;
  readonly budgets: LongTaskBudgetLimits;
  readonly budgetExhaustionDisposition?: BudgetExhaustionDisposition;
  readonly metadata?: Readonly<Record<string, JsonValue>>;
}

export interface LongTaskRecord {
  readonly id: string;
  readonly namespace: string;
  readonly kind: string;
  readonly ownerId: string;
  readonly createdByRunId: string;
  readonly idempotencyKey: string;
  readonly status: LongTaskStatus;
  readonly revision: number;
  readonly totalUnits: number;
  readonly completedUnits: number;
  readonly failedUnits: number;
  readonly maxParallelism: number;
  readonly deadlineAtMs: number | null;
  readonly budgets: LongTaskBudgetLimits;
  readonly budgetExhaustionDisposition: BudgetExhaustionDisposition;
  readonly cancellationRequestedAtMs: number | null;
  readonly usage: LongTaskUsage;
  readonly metadata: Readonly<Record<string, JsonValue>>;
}

export interface LongTaskUnitRecord {
  readonly runId: string | null;
  readonly taskId: string;
  readonly id: string;
  readonly position: number;
  readonly semanticKey: string;
  readonly dependencies: readonly string[];
  readonly required: boolean;
  readonly inputRef: string | null;
  readonly executor: string;
  readonly planStepId: string;
  readonly status: LongTaskUnitStatus;
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly workerId: string | null;
  readonly claimToken: string | null;
  readonly leaseEpoch: number;
  readonly leaseExpiresAtMs: number | null;
  readonly retryReadyAtMs: number | null;
  readonly outputRef: string | null;
  readonly artifactDigest: string | null;
  readonly errorCode: string | null;
  readonly usage: LongTaskUsage;
  readonly metadata: Readonly<Record<string, JsonValue>>;
}

export interface LongTaskClaim {
  readonly taskId: string;
  readonly unitId: string;
  readonly workerId: string;
  readonly claimToken: string;
  readonly leaseEpoch: number;
}

export interface LongTaskCheckpoint {
  readonly taskId: string;
  readonly unitId: string;
  readonly sequence: number;
  readonly claimToken: string;
  readonly leaseEpoch: number;
  readonly payload: JsonValue;
  readonly createdAtMs: number;
}

export interface LongTaskUnitResult {
  readonly runId?: string;
  readonly outputRef: string;
  readonly artifactDigest?: string;
  readonly usage?: ModelTokenUsage | null;
  readonly metadata?: Readonly<Record<string, JsonValue>>;
}

export interface LongTaskRunBinding {
  readonly taskId: string;
  readonly runId: string;
  readonly relation: LongTaskRunRelation;
  readonly sequence: number;
}

export interface DurableTaskDescriptor {
  readonly namespace: string;
  readonly ownerId: string;
  readonly idempotencyKey: string;
  readonly message?: string;
  readonly deadlineAtMs?: number | null;
  readonly budgets?: LongTaskBudgetLimits;
  readonly metadata?: Readonly<Record<string, JsonValue>>;
}

export interface DurableTaskDescriptorResolver {
  resolve(input: {
    readonly plan: ExecutionPlan;
    readonly admission: TaskAdmissionDecision;
    readonly runId: string;
  }): Promise<DurableTaskDescriptor> | DurableTaskDescriptor;
}

export interface DurableUnitExecutionContext {
  /** Owning execution; parallel operations do not create Agent Runs. */
  readonly runId: string;
  readonly task: LongTaskRecord;
  readonly unit: LongTaskUnitRecord;
  /** The validated Tree Child that owns this exact Unit attempt. */
  readonly treeRun?: AgentTreeRun;
  readonly treeAgent?: AgentNode;
  readonly dependencyOutputs: Readonly<Record<string, string>>;
  readonly signal?: AbortSignal;
  bindRun(runId: string): Promise<void>;
  checkpoint(payload: JsonValue): Promise<LongTaskCheckpoint>;
  recordUsage(usage: ModelTokenUsage | null): Promise<void>;
}

export interface DurableUnitExecutor {
  execute(context: DurableUnitExecutionContext): Promise<LongTaskUnitResult> | LongTaskUnitResult;
}

export interface LongTaskDispatchReceipt {
  readonly schemaVersion: 1;
  readonly taskId: string;
  readonly message: string;
  readonly admission: TaskAdmissionDecision;
  readonly recipeFingerprint: string;
  readonly metadata: Readonly<Record<string, JsonValue>>;
}

export interface LongTaskExecutionUpdate {
  readonly type: "long_task.progress" | "long_task.checkpoint";
  readonly payload: Readonly<Record<string, JsonValue>>;
}

export interface LongTaskExecutionResult {
  readonly taskId: string;
  readonly status: "completed" | "failed" | "canceled" | "paused";
  readonly finalResponse: string;
  readonly errorCode?: string;
}

export type LongTaskExecutionObserver = (
  update: LongTaskExecutionUpdate,
) => Promise<void> | void;

export interface LongTaskDispatcher {
  dispatch(input: {
    readonly plan: ExecutionPlan;
    readonly admission: TaskAdmissionDecision;
    readonly runId: string;
    readonly deadlineAt: string | null;
    readonly budgets: RunBudgets;
    readonly signal?: AbortSignal;
  }): Promise<LongTaskDispatchReceipt>;
  execute(input: {
    readonly receipt: LongTaskDispatchReceipt;
    readonly runId: string;
    readonly observer?: LongTaskExecutionObserver;
    readonly signal?: AbortSignal;
  }): Promise<LongTaskExecutionResult>;
}

export interface ComponentBinding {
  readonly id: string;
  readonly revision: string;
}

export interface DurableOptions {
  readonly binding: ComponentBinding;
  readonly admission: TaskAdmissionEvaluator;
  readonly dispatcher: LongTaskDispatcher;
  readonly recoveryAuthenticator: RecoveryAuthenticator;
}

export interface DurableRecoveryPayload {
  readonly schemaVersion: 1;
  readonly sourceRunId: string;
  readonly plan: ExecutionPlan;
  readonly receipt: LongTaskDispatchReceipt;
  readonly preset: AgentPresetSnapshot;
  readonly deadlineAt: string | null;
  readonly remainingBudgets: RunBudgets;
}

export interface DurableRecoverySnapshot extends DurableRecoveryPayload {
  readonly authorityProof: string;
}

export interface DurableContinuation {
  readonly snapshot: DurableRecoverySnapshot;
  readonly command: string;
}

export interface RecoveryAuthenticator {
  sign(payload: DurableRecoveryPayload): Promise<string> | string;
  verify(payload: DurableRecoveryPayload, proof: string): Promise<boolean> | boolean;
}

export interface DurableRunResult {
  readonly status: "completed" | "failed" | "canceled" | "paused";
  readonly receipt: LongTaskDispatchReceipt;
  readonly recoverySnapshot: DurableRecoverySnapshot;
  readonly continuation: boolean;
}

export interface LongTaskRepository {
  create(taskId: string, command: LongTaskCreateCommand): Promise<LongTaskRecord>;
  findByIdempotencyKey(namespace: string, idempotencyKey: string): Promise<LongTaskRecord | undefined>;
  load(taskId: string): Promise<LongTaskRecord | undefined>;
  listUnits(taskId: string): Promise<readonly LongTaskUnitRecord[]>;
  bindRun(taskId: string, runId: string, relation: LongTaskRunRelation): Promise<LongTaskRunBinding>;
  listRunBindings(taskId: string): Promise<readonly LongTaskRunBinding[]>;
  start(taskId: string): Promise<LongTaskRecord>;
  claimReadyUnit(taskId: string, workerId: string, leaseDurationMs: number): Promise<LongTaskUnitRecord | undefined>;
  /** Claim exactly this Unit under the normal admission gates; never select a sibling. */
  claimUnit(taskId: string, unitId: string, workerId: string, leaseDurationMs: number): Promise<LongTaskUnitRecord | undefined>;
  markUnitRunning(claim: LongTaskClaim): Promise<LongTaskUnitRecord>;
  bindUnitRun(claim: LongTaskClaim, runId: string): Promise<LongTaskUnitRecord>;
  heartbeat(claim: LongTaskClaim, leaseDurationMs: number): Promise<LongTaskUnitRecord>;
  appendCheckpoint(claim: LongTaskClaim, payload: JsonValue): Promise<LongTaskCheckpoint>;
  recordUsage(claim: LongTaskClaim, usage: ModelTokenUsage): Promise<LongTaskUnitRecord>;
  completeUnit(claim: LongTaskClaim, result: LongTaskUnitResult, settlementKey: string): Promise<LongTaskUnitRecord>;
  failUnit(claim: LongTaskClaim, errorCode: string, retryable: boolean, retryDelayMs: number): Promise<LongTaskUnitRecord>;
  listCheckpoints(taskId: string, unitId: string): Promise<readonly LongTaskCheckpoint[]>;
  finalizeIfComplete(taskId: string): Promise<LongTaskRecord>;
  pause(
    taskId: string,
    options?: {
      readonly expectedRevision?: number;
      readonly reasonCode?: string;
    },
  ): Promise<LongTaskRecord>;
  resume(taskId: string, additionalAttempts?: number): Promise<LongTaskRecord>;
  requestCancel(taskId: string, requestedAtMs?: number): Promise<LongTaskRecord>;
  cancel(taskId: string): Promise<LongTaskRecord>;
}
