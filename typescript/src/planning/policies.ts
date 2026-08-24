import type {
  PlanningCapabilities,
  PlanningConstraints,
  PlanningPolicy,
  PlanningRequest,
} from "./types.js";

export class ToolPlanningPolicy implements PlanningPolicy {
  readonly #constraints: PlanningConstraints;

  public constructor(constraints: PlanningConstraints = {}) {
    this.#constraints = Object.freeze({ ...constraints });
  }

  public shouldPlan(
    _request: PlanningRequest,
    capabilities: PlanningCapabilities,
  ): boolean {
    return capabilities.availableTools.length > 0;
  }

  public planningConstraints(): PlanningConstraints {
    return this.#constraints;
  }
}
