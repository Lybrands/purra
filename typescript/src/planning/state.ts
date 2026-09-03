import { AgentError } from "../shared/errors.js";
import type {
  ExecutionPlan,
  ExecutionStateFactory,
  ExecutionStep,
  ExecutionTransition,
  PlanExecutionState,
} from "./types.js";

export class CoreExecutionState implements PlanExecutionState {
  readonly #title: string;
  readonly #goal: string | undefined;
  readonly #taskSpec: ExecutionPlan["taskSpec"];
  readonly #workStepIds: string[];
  #steps: ExecutionStep[];

  public constructor(plan: ExecutionPlan) {
    if (plan.steps.length === 0) {
      throw new AgentError("invalid_execution_plan", "ExecutionPlan must contain at least one step");
    }
    this.#title = plan.title;
    this.#goal = plan.goal;
    this.#taskSpec = plan.taskSpec;
    this.#workStepIds = [...plan.workStepIds];
    this.#steps = plan.steps.map((step, index) => withStatus(step, index === 0 ? "running" : "pending"));
  }

  public get plan(): ExecutionPlan {
    return Object.freeze({
      title: this.#title,
      ...(this.#goal === undefined ? {} : { goal: this.#goal }),
      ...(this.#taskSpec === undefined ? {} : { taskSpec: this.#taskSpec }),
      steps: Object.freeze([...this.#steps]),
      workStepIds: Object.freeze([...this.#workStepIds]),
    });
  }

  public static restore(plan: ExecutionPlan): CoreExecutionState {
    const state = new CoreExecutionState(plan);
    if (plan.steps.filter(step => step.status === "running").length > 1
      || (!plan.steps.some(step => step.status === "running") && plan.steps.some(step => step.status === "pending"))
      || plan.steps.some(step => !["pending", "running", "done"].includes(step.status))) {
      throw new AgentError("invalid_execution_plan", "Checkpoint requires one active plan transition");
    }
    state.#steps = plan.steps.map(step => Object.freeze({ ...step }));
    return state;
  }

  public get completedSteps(): readonly ExecutionStep[] {
    return Object.freeze(this.#steps.filter((step) => step.status === "done"));
  }

  public transition(): ExecutionTransition | undefined {
    const runningIndex = this.#steps.findIndex((step) => step.status === "running");
    if (runningIndex < 0) return undefined;
    const running = this.#steps[runningIndex]!;
    const currentToolIndex = running.executor === "tool"
      ? runningIndex
      : this.#nextUnfinishedToolIndex(runningIndex + 1);
    const allowedToolNames = currentToolIndex < 0
      ? []
      : this.#steps[currentToolIndex]!.runtimeToolNames;
    const futureToolNames = currentToolIndex < 0
      ? []
      : this.#steps.slice(currentToolIndex + 1)
          .filter((step) => step.status === "pending" && step.executor === "tool")
          .flatMap((step) => step.runtimeToolNames);
    return Object.freeze({
      stepId: running.id,
      executor: running.executor,
      allowedToolNames: Object.freeze([...new Set(allowedToolNames)]),
      futureToolNames: Object.freeze([...new Set(futureToolNames)]),
    });
  }

  public beginToolRound(toolNames: readonly string[]): void {
    if (!Array.isArray(toolNames) || toolNames.length === 0) {
      throw new AgentError("plan_transition_violation", "Planned tool round must not be empty");
    }
    const transition = this.transition();
    const allowed = new Set(transition?.allowedToolNames ?? []);
    if (toolNames.some((name) => !allowed.has(name))) {
      throw new AgentError(
        "plan_transition_violation",
        "Tool calls are outside the current plan transition",
      );
    }
    const runningIndex = this.#steps.findIndex((step) => step.status === "running");
    if (runningIndex < 0) {
      throw new AgentError("plan_transition_violation", "Execution plan has no running step");
    }
    if (this.#steps[runningIndex]!.executor === "model") {
      const toolIndex = this.#nextUnfinishedToolIndex(runningIndex + 1);
      if (toolIndex < 0) {
        throw new AgentError("plan_transition_violation", "Execution plan has no current tool step");
      }
      for (let index = runningIndex; index < toolIndex; index += 1) {
        if (this.#steps[index]!.executor === "model") {
          this.#steps[index] = withStatus(this.#steps[index]!, "done");
        }
      }
      this.#steps[toolIndex] = withStatus(this.#steps[toolIndex]!, "running");
    }
  }

  public completeToolRound(): void {
    const toolIndex = this.#steps.findIndex(
      (step) => step.status === "running" && step.executor === "tool",
    );
    if (toolIndex < 0) {
      throw new AgentError("plan_transition_violation", "Execution plan has no running tool step");
    }
    this.#steps[toolIndex] = withStatus(this.#steps[toolIndex]!, "done");
    const next = this.#steps.findIndex((step, index) => index > toolIndex && step.status === "pending");
    if (next >= 0) this.#steps[next] = withStatus(this.#steps[next]!, "running");
  }

  public completeFinal(): void {
    if (this.#steps.some((step) => step.executor === "tool" && step.status !== "done")) {
      throw new AgentError(
        "plan_incomplete",
        "Model returned a final response before the execution plan completed",
      );
    }
    this.#steps = this.#steps.map((step) => (
      step.executor === "model" && (step.status === "pending" || step.status === "running")
        ? withStatus(step, "done")
        : step
    ));
  }

  public revise(plan: ExecutionPlan): void {
    const completedIds = new Set(this.completedSteps.map((step) => step.id));
    if (plan.steps.some((step) => completedIds.has(step.id))) {
      throw new AgentError(
        "invalid_replan",
        "A revised plan cannot replace or reuse completed step identities",
      );
    }
    const completed = this.#steps.filter((step) => step.status === "done");
    const replacement = plan.steps.map((step, index) => withStatus(step, index === 0 ? "running" : "pending"));
    this.#steps = [...completed, ...replacement];
    this.#workStepIds.push(...plan.workStepIds.filter((id) => !this.#workStepIds.includes(id)));
  }

  #nextUnfinishedToolIndex(start: number): number {
    for (let index = start; index < this.#steps.length; index += 1) {
      const step = this.#steps[index]!;
      if (step.status === "done") continue;
      if (step.executor === "tool") return index;
    }
    return -1;
  }
}

export class CoreExecutionStateFactory implements ExecutionStateFactory {
  public create(plan: ExecutionPlan): PlanExecutionState {
    return new CoreExecutionState(plan);
  }
}

function withStatus(step: ExecutionStep, status: ExecutionStep["status"]): ExecutionStep {
  return Object.freeze({ ...step, status });
}
