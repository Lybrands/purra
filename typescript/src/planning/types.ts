import type { ContextBlock, TaskContextRequest } from "../context/types.js";
import type { JsonValue, Message, ToolSpec } from "../model/types.js";
import type { ModelTaskRunner } from "../extensions/model-tasks.js";
import type { ToolRiskLevel } from "../tools/types.js";

export type StepExecutor = "model" | "tool";
export type StepType = "read" | "analyze" | "write" | "confirm" | "review";

export interface TaskSpec {
  readonly goal: string;
  readonly target?: JsonValue;
  readonly operation?: string;
  readonly instruction?: string;
  readonly deliverable?: string;
  readonly constraints?: readonly string[];
  readonly preserve?: readonly string[];
}

export interface WorkStep {
  readonly id: string;
  readonly title: string;
  readonly type: StepType;
  readonly executor: StepExecutor;
  readonly riskLevel?: ToolRiskLevel;
  readonly capabilityNames?: readonly string[];
  readonly dependsOn?: readonly string[];
  readonly description?: string;
}

export interface WorkPlan {
  readonly title: string;
  readonly goal?: string;
  readonly taskSpec?: TaskSpec;
  readonly steps: readonly WorkStep[];
}

export interface ExecutionStep extends WorkStep {
  readonly status: "pending" | "running" | "done" | "failed";
  readonly runtimeToolNames: readonly string[];
  readonly protocolPrivate: boolean;
  readonly planningCapability?: string;
}

export interface ExecutionPlan {
  readonly title: string;
  readonly goal?: string;
  readonly taskSpec?: TaskSpec;
  readonly steps: readonly ExecutionStep[];
  readonly workStepIds: readonly string[];
}

export interface PlanningConstraints {
  readonly maxSteps?: number;
  readonly allowedCapabilityNames?: readonly string[];
  readonly excludedCapabilityNames?: readonly string[];
}

export interface PlanningCapabilities {
  readonly availableTools: readonly ToolSpec[];
  readonly planningContext: readonly ContextBlock[];
  readonly constraints: PlanningConstraints;
}

export interface PlanningRequest {
  readonly messages: readonly Message[];
  readonly enabledTools?: readonly string[];
  readonly metadata?: Readonly<Record<string, JsonValue>>;
}

export interface PlanningResult {
  readonly workPlan: WorkPlan;
}

export interface PlanningTurn {
  readonly revision: number;
  readonly round: number;
  readonly remainingModelRounds: number;
  readonly messages: readonly Message[];
  readonly completedSteps: readonly ExecutionStep[];
  readonly reason: string;
  readonly errorCode?: string;
}

export interface WorkPlanner {
  createPlan(
    request: PlanningRequest,
    capabilities: PlanningCapabilities,
    signal?: AbortSignal,
  ): Promise<PlanningResult> | PlanningResult;
}

export interface DynamicWorkPlanner extends WorkPlanner {
  revisePlan(
    request: PlanningRequest,
    capabilities: PlanningCapabilities,
    turn: PlanningTurn,
    signal?: AbortSignal,
  ): Promise<PlanningResult> | PlanningResult;
}

export interface PlanningPolicy {
  shouldPlan(request: PlanningRequest, capabilities: PlanningCapabilities): boolean;
  planningConstraints(
    request: PlanningRequest,
    capabilities: Omit<PlanningCapabilities, "constraints">,
  ): PlanningConstraints;
}

export interface ResponseValidationResult {
  readonly violationCode?: string;
  readonly repairGuidance?: string;
  readonly details?: Readonly<Record<string, JsonValue>>;
}

export interface ResponseValidator {
  validate(input: {
    readonly content: JsonValue;
    readonly messages: readonly Message[];
  }): ResponseValidationResult;
}

export interface ResponseJudge {
  judge(input: {
    readonly content: JsonValue;
    readonly messages: readonly Message[];
    readonly signal?: AbortSignal;
  }): Promise<ResponseValidationResult> | ResponseValidationResult;
}

interface PlanningOptionsBase {
  readonly policy: PlanningPolicy;
  readonly binding?: {
    readonly id: string;
    readonly revision: string;
  };
}

export type PlanningOptions = PlanningOptionsBase & (
  | { readonly planner: WorkPlanner; readonly plannerFactory?: never }
  | {
      readonly planner?: never;
      readonly plannerFactory: (modelTasks: ModelTaskRunner) => WorkPlanner;
    }
);

export interface ResolvedPlanningOptions extends PlanningOptionsBase {
  readonly planner: WorkPlanner;
}

export interface ResponseValidationOptions {
  readonly validators?: readonly ResponseValidator[];
  readonly judges?: readonly ResponseJudge[];
  readonly judgeFactories?: readonly ((modelTasks: ModelTaskRunner) => ResponseJudge)[];
  readonly maxAttempts?: number;
}

export interface ResponseJudgePolicy {
  buildMessages(input: {
    readonly content: JsonValue;
    readonly messages: readonly Message[];
  }): readonly Message[];
  evaluate(input: {
    readonly judgmentContent: string;
    readonly candidateContent: JsonValue;
  }): ResponseValidationResult;
}

export interface PlanningToolRegistration {
  readonly runtimeName: string;
  readonly runtimeSpec: ToolSpec;
  readonly planningCapability?: ToolSpec;
  readonly prerequisiteTools: readonly string[];
  readonly riskLevel: ToolRiskLevel;
  readonly title: string;
}

export interface CompiledExecutionPlan {
  readonly executionPlan: ExecutionPlan;
  readonly insertedToolNames: readonly string[];
  readonly loweredToolNames: readonly string[];
}

export interface ExecutionTransition {
  readonly stepId: string;
  readonly executor: StepExecutor;
  readonly allowedToolNames: readonly string[];
  readonly futureToolNames: readonly string[];
}

export interface ExecutionStateFactory {
  create(plan: ExecutionPlan): PlanExecutionState;
}

export interface PlanExecutionState {
  readonly plan: ExecutionPlan;
  readonly completedSteps: readonly ExecutionStep[];
  transition(): ExecutionTransition | undefined;
  beginToolRound(toolNames: readonly string[]): void;
  completeToolRound(): void;
  completeFinal(): void;
  revise(plan: ExecutionPlan): void;
}

export function taskContextFromPlan(plan: ExecutionPlan): TaskContextRequest {
  return Object.freeze({
    goal: plan.taskSpec?.goal ?? plan.goal ?? plan.title,
    plannedTools: Object.freeze(plan.steps.flatMap((step) => step.runtimeToolNames)),
    requiredContextBlocks: Object.freeze([]),
  });
}
