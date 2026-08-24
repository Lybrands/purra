import type { JsonValue, Message, ToolSpec } from "../model/types.js";
import type { ModelTaskRunner } from "../extensions/model-tasks.js";

export type ContextStrategy = "single_pass" | "staged";

export interface ContextEvidenceReceipt {
  readonly evidenceId: string;
  readonly contextBlock?: string;
  readonly source: string;
  readonly itemId?: string;
  readonly version?: string;
}

export interface ContextBudgetClaim {
  readonly name: string;
  readonly desiredTokens: number;
  readonly minimumTokens?: number;
  readonly maximumTokens?: number;
  readonly priority?: number;
}

export interface ContextBudget {
  readonly windowTokens: number;
  readonly outputReserveTokens: number;
  readonly safetyReserveTokens: number;
  readonly runtimeReserveTokens: number;
  readonly toolSchemaTokens: number;
  readonly providerInputTokens: number;
  readonly minimumMessageTokens: number;
  readonly contextAllocations: Readonly<Record<string, number>>;
}

export interface ContextBlock {
  readonly name: string;
  readonly content: string;
  readonly tokenCount?: number;
  readonly untrusted?: boolean;
  readonly evidence?: readonly ContextEvidenceReceipt[];
}

export interface ContextBundle {
  readonly blocks: readonly ContextBlock[];
  readonly diagnostics?: Readonly<Record<string, JsonValue>>;
}

export interface ContextRequest {
  readonly messages: readonly Message[];
  readonly enabledTools?: readonly string[];
  readonly metadata?: Readonly<Record<string, JsonValue>>;
}

export interface TaskContextRequest {
  readonly goal: string;
  readonly plannedTools?: readonly string[];
  readonly availableTools?: readonly string[];
  readonly requiredContextBlocks?: readonly string[];
  readonly evidenceKinds?: readonly string[];
}

export interface ContextProvider {
  describeContextDemands?(
    request: ContextRequest,
    signal?: AbortSignal,
  ): Promise<readonly ContextBudgetClaim[]> | readonly ContextBudgetClaim[];
  buildContext(
    request: ContextRequest,
    budget: ContextBudget,
    signal?: AbortSignal,
  ): Promise<ContextBundle> | ContextBundle;
}

export interface StagedContextProvider extends ContextProvider {
  describeTaskContextDemands?(
    request: ContextRequest,
    task: TaskContextRequest,
    signal?: AbortSignal,
  ): Promise<readonly ContextBudgetClaim[]> | readonly ContextBudgetClaim[];
  buildPlanningContext(
    request: ContextRequest,
    budget: ContextBudget,
    signal?: AbortSignal,
  ): Promise<ContextBundle> | ContextBundle;
  buildTaskContext(
    request: ContextRequest,
    budget: ContextBudget,
    task: TaskContextRequest,
    signal?: AbortSignal,
  ): Promise<ContextBundle> | ContextBundle;
}

export interface ContextCompressionRequest {
  readonly messages: readonly Message[];
  readonly budget: ContextBudget;
  readonly contextTokens: number;
  readonly availableMessageTokens: number;
  readonly messageTokens: number;
  readonly projectedInputTokens: number;
  readonly pressureRatio: number;
  readonly compressionRequired: boolean;
  readonly triggerReason: "below_threshold" | "pressure_threshold" | "message_budget_exceeded";
}

export interface ContextCompressionResult {
  readonly messages: readonly Message[];
  readonly summary?: ContextBlock;
}

export interface ContextCompressionHook {
  compress(
    request: ContextCompressionRequest,
    signal?: AbortSignal,
  ): Promise<ContextCompressionResult> | ContextCompressionResult;
}

export interface ContextReserves {
  readonly safetyTokens?: number;
  readonly runtimeTokens?: number;
  readonly minimumMessageTokens?: number;
}

export interface ContextOptions {
  readonly strategy?: ContextStrategy;
  readonly provider?: ContextProvider;
  readonly providerFactory?: (modelTasks: ModelTaskRunner) => ContextProvider;
  readonly claims?: readonly ContextBudgetClaim[];
  readonly compression?: ContextCompressionHook;
  readonly compressionFactory?: (modelTasks: ModelTaskRunner) => ContextCompressionHook;
  readonly triggerRatio?: number;
  readonly maxCompactions?: number;
  readonly reserves?: ContextReserves;
}

export interface PreparedContext {
  readonly budget: ContextBudget;
  readonly evidence: readonly ContextEvidenceReceipt[];
  project(messages: readonly Message[], signal?: AbortSignal): Promise<readonly Message[]>;
}

export interface ContextPreparationInput {
  readonly request: ContextRequest;
  readonly tools: readonly ToolSpec[];
  readonly windowTokens: number;
  readonly outputReserveTokens: number;
  readonly signal?: AbortSignal;
}

export interface StagedContextPreparation {
  readonly planning: ContextBundle;
  prepareExecution(task: TaskContextRequest, signal?: AbortSignal): Promise<PreparedContext>;
}
