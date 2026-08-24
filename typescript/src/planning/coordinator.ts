import type { ContextBlock } from "../context/types.js";
import type { Message } from "../model/types.js";
import { AgentCanceledError, AgentError } from "../shared/errors.js";
import { compileWorkPlan, copyPlanningConstraints, copyWorkPlan, planningToolSpecs } from "./compiler.js";
import { CoreExecutionStateFactory } from "./state.js";
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

export class PlannedExecutionCoordinator {
  readonly #options: ResolvedPlanningOptions;
  readonly #request: PlanningRequest;
  readonly #registrations: readonly PlanningToolRegistration[];
  readonly #planningContext: readonly ContextBlock[];
  readonly #maxRounds: number;
  readonly #factory = new CoreExecutionStateFactory();
  #capabilities: PlanningCapabilities | undefined;
  #state: PlanExecutionState | undefined;
  #workPlan: WorkPlan | undefined;
  #revision = 0;

  public constructor(input: {
    readonly options: ResolvedPlanningOptions;
    readonly request: PlanningRequest;
    readonly registrations: readonly PlanningToolRegistration[];
    readonly planningContext?: readonly ContextBlock[];
    readonly maxRounds: number;
  }) {
    if (typeof input.options.planner?.createPlan !== "function") {
      throw new TypeError("Planned execution requires a WorkPlanner");
    }
    if (
      typeof input.options.policy?.shouldPlan !== "function"
      || typeof input.options.policy.planningConstraints !== "function"
    ) {
      throw new TypeError("Planned execution requires a PlanningPolicy");
    }
    this.#options = input.options;
    this.#request = input.request;
    this.#registrations = input.registrations;
    this.#planningContext = Object.freeze([...(input.planningContext ?? [])]);
    this.#maxRounds = input.maxRounds;
  }

  public get state(): PlanExecutionState | undefined {
    return this.#state;
  }

  public get workPlan(): WorkPlan | undefined {
    return this.#workPlan;
  }

  public async start(signal?: AbortSignal): Promise<PlannedStart | undefined> {
    throwIfCanceled(signal);
    const unconstrained = Object.freeze({
      availableTools: planningToolSpecs(this.#registrations),
      planningContext: this.#planningContext,
    });
    const constraints = copyPlanningConstraints(
      this.#options.policy.planningConstraints(this.#request, unconstrained),
    );
    const capabilities: PlanningCapabilities = Object.freeze({
      ...unconstrained,
      availableTools: planningToolSpecs(this.#registrations, constraints),
      constraints,
    });
    this.#capabilities = capabilities;
    if (!this.#options.policy.shouldPlan(this.#request, capabilities)) return undefined;
    const result = await abortable(
      Promise.resolve(this.#options.planner.createPlan(this.#request, capabilities, signal)),
      signal,
    );
    const workPlan = plannerWorkPlan(result);
    const compiled = compileWorkPlan(workPlan, this.#registrations, constraints);
    this.#assertRemainingAuthority(compiled.executionPlan, this.#maxRounds);
    const state = this.#factory.create(compiled.executionPlan);
    this.#state = state;
    this.#workPlan = workPlan;
    return Object.freeze({ workPlan, state });
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
    const remainingModelRounds = this.#maxRounds - input.round;
    if (remainingModelRounds < 1) {
      throw new AgentError("replanning_budget_exhausted", "No model rounds remain for replanning");
    }
    const result = await abortable(Promise.resolve(planner.revisePlan(
      this.#request,
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
    const workPlan = plannerWorkPlan(result);
    const satisfied = new Set(state.completedSteps.flatMap((step) => step.runtimeToolNames));
    const compiled = compileWorkPlan(workPlan, this.#registrations, capabilities.constraints, satisfied);
    this.#assertRemainingAuthority(compiled.executionPlan, remainingModelRounds);
    state.revise(compiled.executionPlan);
    this.#workPlan = workPlan;
    return workPlan;
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
