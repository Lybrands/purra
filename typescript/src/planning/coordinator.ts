import { AgentOperationController } from "../operations/index.js";
import type { PlanningScope } from "./stream.js";
import type { TaskAdmissionDecision } from "../durable/types.js";
import type { ContextBlock } from "../context/types.js";
import type { Message } from "../model/types.js";
import { AgentCanceledError, AgentError } from "../shared/errors.js";
import { compileWorkPlan, copyPlanningConstraints, copyWorkPlan, planningToolSpecs } from "./compiler.js";
import { CoreExecutionState, CoreExecutionStateFactory } from "./state.js";
import type {
  DynamicWorkPlanner,
  ExecutionPlan,
  PlanExecutionState,
  PlanningCapabilities,
  ResolvedPlanningOptions,
  PlanningRequest,
  PlanningToolRegistration,
  WorkPlan,
} from "./types.js";

export interface PlannedStart {
  readonly workPlan: WorkPlan;
  readonly state: PlanExecutionState;
}

export interface PlanningCheckpoint {
  readonly request: PlanningRequest;
  readonly capabilities: PlanningCapabilities;
  readonly plan: ExecutionPlan;
  readonly workPlan: WorkPlan;
  readonly revision: number;
  readonly maxRounds: number;
  readonly roundOffset: number;
}

export class PlannedExecutionCoordinator {
  readonly #options: ResolvedPlanningOptions;
  readonly #request: PlanningRequest;
  readonly #registrations: readonly PlanningToolRegistration[];
  readonly #planningContext: readonly ContextBlock[];
  readonly #maxRounds: number;
  readonly #roundOffset: number;
  readonly #factory = new CoreExecutionStateFactory();
  #capabilities: PlanningCapabilities | undefined;
  #state: PlanExecutionState | undefined;
  #workPlan: WorkPlan | undefined;
  #revision = 0;
  readonly #operations: AgentOperationController | undefined;
  readonly #runId: string | undefined;
  readonly #admit: ((plan: ExecutionPlan) => Promise<TaskAdmissionDecision>) | undefined;
  readonly #publish: ((plan: WorkPlan, revision: number) => Promise<void>) | undefined;
  readonly #countAttempts: ((operationId: string) => Promise<number>) | undefined;
  #admission: TaskAdmissionDecision | undefined;
  #validationStarted: number | undefined;

  public constructor(input: {
    readonly options: ResolvedPlanningOptions;
    readonly request: PlanningRequest;
    readonly registrations: readonly PlanningToolRegistration[];
    readonly planningContext?: readonly ContextBlock[];
    readonly maxRounds: number;
    readonly roundOffset?: number;
    readonly operations?: AgentOperationController;
    readonly runId?: string;
    readonly admit?: (plan: ExecutionPlan) => Promise<TaskAdmissionDecision>;
    readonly publishPlan?: (plan: WorkPlan, revision: number) => Promise<void>;
    readonly countAttempts?: (operationId: string) => Promise<number>;
  }) {
    if (typeof input.options.planner?.createPlan !== "function") {
      throw new TypeError("Planned execution requires a WorkPlanner");
    }
    if (
      input.options.policy !== undefined
      && typeof input.options.policy.planningConstraints !== "function"
    ) {
      throw new TypeError("PlanningPolicy must implement planningConstraints");
    }
    this.#options = input.options;
    this.#request = input.request;
    this.#registrations = input.registrations;
    this.#planningContext = Object.freeze([...(input.planningContext ?? [])]);
    this.#maxRounds = input.maxRounds;
    this.#roundOffset = input.roundOffset ?? 0;
    this.#operations = input.operations;
    this.#runId = input.runId;
    this.#admit = input.admit;
    this.#publish = input.publishPlan;
    this.#countAttempts = input.countAttempts;
  }

  public get admission(): TaskAdmissionDecision | undefined { return this.#admission; }

  public get state(): PlanExecutionState | undefined {
    return this.#state;
  }

  public get workPlan(): WorkPlan | undefined {
    return this.#workPlan;
  }

  public checkpoint(): PlanningCheckpoint {
    if (!this.#state || !this.#capabilities || !this.#workPlan) throw new AgentError("planning_unavailable", "No plan to checkpoint");
    return { request: this.#request, capabilities: this.#capabilities, plan: this.#state.plan,
      workPlan: this.#workPlan, revision: this.#revision, maxRounds: this.#maxRounds, roundOffset: this.#roundOffset };
  }

  public restore(checkpoint: PlanningCheckpoint): void {
    if (!Number.isSafeInteger(checkpoint.revision) || checkpoint.revision < 0) throw new TypeError("Invalid planning revision");
    this.#state = CoreExecutionState.restore(checkpoint.plan);
    this.#capabilities = checkpoint.capabilities;
    this.#workPlan = copyWorkPlan(checkpoint.workPlan);
    this.#revision = checkpoint.revision;
  }

  public async start(signal?: AbortSignal): Promise<PlannedStart | undefined> {
    throwIfCanceled(signal);
    const unconstrained = Object.freeze({
      availableTools: planningToolSpecs(this.#registrations),
      planningContext: this.#planningContext,
    });
    const constraints = copyPlanningConstraints(
      this.#options.policy?.planningConstraints(this.#request, unconstrained) ?? {},
    );
    const capabilities: PlanningCapabilities = Object.freeze({
      ...unconstrained,
      availableTools: planningToolSpecs(this.#registrations, constraints),
      constraints,
    });
    this.#capabilities = capabilities;
    return this.#phase(signal, async (scope) => {
      const result = await abortable(
        Promise.resolve(this.#options.planner.createPlan(this.#scopedRequest(scope), capabilities, signal)),
        signal,
      );
      this.#validationStarted = performance.now();
      const workPlan = plannerWorkPlan(result);
      const compiled = compileWorkPlan(workPlan, this.#registrations, constraints);
      this.#assertRemainingAuthority(compiled.executionPlan, this.#maxRounds);
      this.#admission = compiled.executionPlan.taskSpec === undefined ? undefined : await this.#admit?.(compiled.executionPlan);
      throwIfCanceled(signal);
      if (this.#admission?.mode === "reject" || this.#admission?.mode === "clarify" || this.#admission?.requiresConfirmation === true) {
        return undefined;
      }
      await this.#publish?.(workPlan, this.#revision);
      const state = this.#factory.create(compiled.executionPlan);
      this.#state = state;
      this.#workPlan = workPlan;
      return Object.freeze({ workPlan, state });
    });
  }

  public async replan(input: {
    readonly round: number;
    readonly messages: readonly Message[];
    readonly reason: string;
    readonly errorCode?: string;
    readonly signal?: AbortSignal;
  }): Promise<WorkPlan> {
    throwIfCanceled(input.signal);
    const state = this.#state;
    const capabilities = this.#capabilities;
    if (state === undefined || capabilities === undefined) {
      throw new AgentError("replanning_unavailable", "Replanning requires an active execution plan");
    }
    const planner = this.#options.planner as DynamicWorkPlanner;
    if (typeof planner.revisePlan !== "function") {
      throw new AgentError("replanning_unavailable", "Configured Planner does not support revision");
    }
    this.#revision += 1;
    const remainingModelRounds = this.#maxRounds + this.#roundOffset - input.round;
    if (remainingModelRounds < 1) {
      throw new AgentError("replanning_budget_exhausted", "No model rounds remain for replanning");
    }
    return this.#phase(input.signal, async (scope) => {
      const result = await abortable(Promise.resolve(planner.revisePlan(
        this.#scopedRequest(scope),
        capabilities,
        Object.freeze({
          revision: this.#revision,
          round: input.round,
          remainingModelRounds,
          messages: Object.freeze([...input.messages]),
          completedSteps: state.completedSteps,
          reason: requiredText(input.reason, "replan reason"),
          ...(input.errorCode === undefined ? {} : { errorCode: input.errorCode }),
        }),
        input.signal,
      )), input.signal);
      this.#validationStarted = performance.now();
      const workPlan = plannerWorkPlan(result);
      const satisfied = new Set(state.completedSteps.flatMap((step) => step.runtimeToolNames));
      const compiled = compileWorkPlan(workPlan, this.#registrations, capabilities.constraints, satisfied);
      this.#assertRemainingAuthority(compiled.executionPlan, remainingModelRounds);
      throwIfCanceled(input.signal);
      state.revise(compiled.executionPlan);
      await this.#publish?.(workPlan, this.#revision);
      this.#workPlan = workPlan;
      return workPlan;
    });
  }

  #scopedRequest(scope: PlanningScope | undefined): PlanningRequest {
    return Object.freeze({ ...this.#request, ...(scope === undefined ? {} : { scope }) });
  }

  async #phase<T>(signal: AbortSignal | undefined, work: (scope: PlanningScope | undefined) => Promise<T>): Promise<T> {
    throwIfCanceled(signal);
    this.#validationStarted = undefined;
    const operation = this.#runId === undefined ? undefined : await this.#operations?.start("planning", {
      runId: this.#runId, display: { labelKey: "agent.operation.planning", labelParams: { revision: this.#revision } },
    });
    const scope = operation === undefined ? undefined : Object.freeze({
      runId: operation.runId, operationId: operation.operationId, revision: this.#revision,
    });
    let failed = false;
    let failure: unknown;
    try {
      const result = await work(scope);
      throwIfCanceled(signal);
      return result;
    } catch (error) {
      failed = true;
      failure = error;
      throw error;
    } finally {
      if (operation !== undefined) {
        try {
          const attempts = await this.#countAttempts?.(operation.operationId) ?? 0;
          const display = { labelKey: "agent.operation.planning", labelParams: {
            revision: this.#revision, modelAttempts: attempts, repairCount: Math.max(0, attempts - 1),
            validationMs: this.#validationStarted === undefined ? null : performance.now() - this.#validationStarted,
          } };
          if (signal?.aborted === true || failure instanceof AgentCanceledError) {
            await this.#operations!.cancel(operation.operationId, "request_canceled", display);
          } else if (failed) {
            await this.#operations!.fail(operation.operationId, failure instanceof AgentError ? failure.code : "planning_failed", display);
          } else if (this.#admission?.mode === "reject" || this.#admission?.mode === "clarify" || this.#admission?.requiresConfirmation === true) {
            await this.#operations!.fail(operation.operationId, "planning_not_admitted", display);
          } else await this.#operations!.succeed(operation.operationId, display);
        } catch (error) { if (!failed) throw error; }
      }
    }
  }

  #assertRemainingAuthority(plan: ExecutionPlan, remainingModelRounds: number): void {
    const toolSteps = plan.steps.filter((step) => step.executor === "tool").length;
    if (toolSteps > Math.max(0, remainingModelRounds - 1)) {
      throw new AgentError(
        "plan_exceeds_round_authority",
        "Compiled plan needs more tool transitions than the remaining model-round budget",
      );
    }
  }
}

function plannerWorkPlan(value: unknown): WorkPlan {
  if (value === null || typeof value !== "object" || !("workPlan" in value)) {
    throw new AgentError("invalid_planner_output", "Planner must return a PlanningResult");
  }
  return copyWorkPlan((value as { readonly workPlan: WorkPlan }).workPlan);
}

function requiredText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim() === "") throw new TypeError(`${label} is required`);
  return value.trim();
}

async function abortable<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (signal === undefined) return promise;
  if (signal.aborted) {
    void promise.catch(() => undefined);
    throw new AgentCanceledError();
  }
  let cancel: (() => void) | undefined;
  const canceled = new Promise<never>((_resolve, reject) => {
    cancel = () => reject(new AgentCanceledError());
    signal.addEventListener("abort", cancel, { once: true });
  });
  try {
    return await Promise.race([promise, canceled]);
  } finally {
    if (cancel !== undefined) signal.removeEventListener("abort", cancel);
    void promise.catch(() => undefined);
  }
}

function throwIfCanceled(signal?: AbortSignal): void {
  if (signal?.aborted === true) throw new AgentCanceledError();
}
