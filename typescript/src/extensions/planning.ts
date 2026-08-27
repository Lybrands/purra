import type { ModelTaskRunner } from "./model-tasks.js";
import type { JsonValue, Message } from "../model/types.js";
import { copyWorkPlan } from "../planning/compiler.js";
import type {
  DynamicWorkPlanner,
  PlanningCapabilities,
  PlanningRequest,
  PlanningResult,
  PlanningTurn,
  ResponseJudge,
  ResponseJudgePolicy,
  ResponseValidationResult,
  WorkPlan,
} from "../planning/types.js";
import { AgentError } from "../shared/errors.js";

export interface ModelWorkPlannerOptions {
  readonly maxRepairAttempts?: number;
  readonly maxCallOutputTokens?: number;
}

export interface ModelResponseJudgeOptions {
  readonly maxCallOutputTokens?: number;
}

const PLANNER_INSTRUCTION = [
  "You are the planning component of a host-controlled Agent.",
  "Return one JSON object only with the shape {\"workPlan\":{\"title\":string,\"goal\"?:string,\"taskSpec\"?:object,\"steps\":array}}.",
  "Each step requires id, title, type, and executor.",
  "Keep user-visible titles and goals in the language of the current request.",
  "Return the smallest non-redundant set of user-visible semantic steps needed to complete the request.",
  "Each step must represent a distinct result, evidence phase, or domain milestone; do not split out reasoning, retries, approvals, persistence, internal validation, protocol lowering, or tool prerequisites.",
  "A tool step must select exactly one name from availableTools in capabilityNames; a model step must not select tools.",
  "Dependencies may reference only earlier step ids. Never create hidden permissions, tools, or execution stages.",
  "For a direct response, return one model/review step. For a revision, return only unfinished work and never reuse a completed step id.",
].join(" ");

export class ModelWorkPlanner implements DynamicWorkPlanner {
  readonly #modelTasks: ModelTaskRunner;
  readonly #maxRepairAttempts: number;
  readonly #maxCallOutputTokens: number | undefined;

  public constructor(modelTasks: ModelTaskRunner, options: ModelWorkPlannerOptions = {}) {
    if (modelTasks === null || typeof modelTasks?.complete !== "function") {
      throw new TypeError("ModelWorkPlanner requires a ModelTaskRunner");
    }
    const maxRepairAttempts = options.maxRepairAttempts ?? 1;
    if (!Number.isSafeInteger(maxRepairAttempts) || maxRepairAttempts < 0 || maxRepairAttempts > 3) {
      throw new TypeError("planner maxRepairAttempts must be between zero and three");
    }
    if (
      options.maxCallOutputTokens !== undefined
      && (
        !Number.isSafeInteger(options.maxCallOutputTokens)
        || options.maxCallOutputTokens < 1
      )
    ) {
      throw new TypeError("planner maxCallOutputTokens must be a positive integer");
    }
    this.#modelTasks = modelTasks;
    this.#maxRepairAttempts = maxRepairAttempts;
    this.#maxCallOutputTokens = options.maxCallOutputTokens;
  }

  public createPlan(
    request: PlanningRequest,
    capabilities: PlanningCapabilities,
    signal?: AbortSignal,
  ): Promise<PlanningResult> {
    return this.#plan(request, capabilities, undefined, signal);
  }

  public revisePlan(
    request: PlanningRequest,
    capabilities: PlanningCapabilities,
    turn: PlanningTurn,
    signal?: AbortSignal,
  ): Promise<PlanningResult> {
    return this.#plan(request, capabilities, turn, signal);
  }

  async #plan(
    request: PlanningRequest,
    capabilities: PlanningCapabilities,
    turn: PlanningTurn | undefined,
    signal: AbortSignal | undefined,
  ): Promise<PlanningResult> {
    let messages = plannerMessages(request, capabilities, turn);
    for (let repairAttempt = 0; ; repairAttempt += 1) {
      const completion = await this.#modelTasks.complete(messages, {
        ...(this.#maxCallOutputTokens === undefined
          ? {}
          : { maxCallOutputTokens: this.#maxCallOutputTokens }),
        ...(signal === undefined ? {} : { signal }),
      });
      try {
        const workPlan = normalizePlan(completion.turn.message.content, capabilities, turn);
        return Object.freeze({ workPlan });
      } catch (error) {
        const failure = plannerError(error);
        if (repairAttempt >= this.#maxRepairAttempts) throw failure;
        messages = Object.freeze([
          ...messages,
          completion.turn.message,
          Object.freeze({
            role: "developer" as const,
            content: [
              `The previous JSON plan was invalid: ${failure.message}`,
              "Return a corrected JSON object from the original request.",
              "Do not add unavailable capabilities or reuse completed step ids.",
            ].join(" "),
            attributes: Object.freeze({ plannerRepair: true }),
          }),
        ]);
      }
    }
  }
}

export class ModelResponseJudge implements ResponseJudge {
  readonly #modelTasks: ModelTaskRunner;
  readonly #policy: ResponseJudgePolicy;
  readonly #maxCallOutputTokens: number | undefined;

  public constructor(
    modelTasks: ModelTaskRunner,
    policy: ResponseJudgePolicy,
    options: ModelResponseJudgeOptions = {},
  ) {
    if (modelTasks === null || typeof modelTasks?.complete !== "function") {
      throw new TypeError("ModelResponseJudge requires a ModelTaskRunner");
    }
    if (
      typeof policy?.buildMessages !== "function"
      || typeof policy.evaluate !== "function"
    ) {
      throw new TypeError("ModelResponseJudge requires a ResponseJudgePolicy");
    }
    if (
      options.maxCallOutputTokens !== undefined
      && (
        !Number.isSafeInteger(options.maxCallOutputTokens)
        || options.maxCallOutputTokens < 1
      )
    ) {
      throw new TypeError("judge maxCallOutputTokens must be a positive integer");
    }
    this.#modelTasks = modelTasks;
    this.#policy = policy;
    this.#maxCallOutputTokens = options.maxCallOutputTokens;
  }

  public async judge(input: {
    readonly content: JsonValue;
    readonly messages: readonly Message[];
    readonly signal?: AbortSignal;
  }): Promise<ResponseValidationResult> {
    const completion = await this.#modelTasks.complete(
      this.#policy.buildMessages({ content: input.content, messages: input.messages }),
      {
        ...(this.#maxCallOutputTokens === undefined
          ? {}
          : { maxCallOutputTokens: this.#maxCallOutputTokens }),
        ...(input.signal === undefined ? {} : { signal: input.signal }),
      },
    );
    const judgmentContent = completion.turn.message.content;
    if (typeof judgmentContent !== "string") {
      throw new AgentError(
        "response_judge_contract_violation",
        "Model response judge must return text",
      );
    }
    return this.#policy.evaluate({
      judgmentContent,
      candidateContent: input.content,
    });
  }
}

function plannerMessages(
  request: PlanningRequest,
  capabilities: PlanningCapabilities,
  turn: PlanningTurn | undefined,
): readonly Message[] {
  const trustedInstructions = request.messages.filter((message) => (
    message.role === "system" || message.role === "developer"
  ));
  const conversation = request.messages.filter((message) => (
    message.role !== "system" && message.role !== "developer"
  ));
  return Object.freeze([
    Object.freeze({ role: "system" as const, content: PLANNER_INSTRUCTION }),
    ...trustedInstructions,
    Object.freeze({
      role: "developer" as const,
      content: JSON.stringify({
        availableTools: capabilities.availableTools,
        constraints: capabilities.constraints,
        planningContext: capabilities.planningContext,
        ...(turn === undefined ? {} : {
          executionState: {
            revision: turn.revision,
            round: turn.round,
            remainingModelRounds: turn.remainingModelRounds,
            completedSteps: turn.completedSteps,
            reason: turn.reason,
            ...(turn.errorCode === undefined ? {} : { errorCode: turn.errorCode }),
          },
        }),
      }),
      attributes: Object.freeze({ planningContract: true }),
    }),
    Object.freeze({
      role: "user" as const,
      content: JSON.stringify({
        conversation,
        ...(request.metadata === undefined ? {} : { metadata: request.metadata }),
      }),
      attributes: Object.freeze({ planningInput: true }),
    }),
  ]);
}

function normalizePlan(
  content: JsonValue,
  capabilities: PlanningCapabilities,
  turn: PlanningTurn | undefined,
): WorkPlan {
  let parsed: unknown = content;
  if (typeof content === "string") {
    try {
      const text = content.trim();
      const fenced = /^```(?:json)?\s*([\s\S]*?)\s*```$/iu.exec(text);
      parsed = JSON.parse(fenced?.[1] ?? text);
    } catch (error) {
      throw new AgentError("invalid_planner_output", "Planner output is not valid JSON", {
        cause: error,
      });
    }
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new AgentError("invalid_planner_output", "Planner output must be a JSON object");
  }
  const workPlan = copyWorkPlan((parsed as { readonly workPlan?: WorkPlan }).workPlan!);
  if (workPlan.steps.some((step) => step.type === "confirm")) {
    throw new AgentError("invalid_planner_output", "Planner must not create confirmation steps");
  }
  const maxSteps = capabilities.constraints.maxSteps;
  if (maxSteps !== undefined && workPlan.steps.length > maxSteps) {
    throw new AgentError("invalid_planner_output", "WorkPlan exceeds the host step limit");
  }
  const available = new Set(capabilities.availableTools.map((tool) => tool.name));
  for (const name of workPlan.steps.flatMap((step) => step.capabilityNames ?? [])) {
    if (!available.has(name)) {
      throw new AgentError("invalid_planner_output", `Planner selected an unavailable capability: ${name}`);
    }
  }
  if (turn !== undefined) {
    const completed = new Set(turn.completedSteps.map((step) => step.id));
    if (workPlan.steps.some((step) => completed.has(step.id))) {
      throw new AgentError("invalid_planner_output", "Revised plan reuses a completed step id");
    }
  }
  return workPlan;
}

function plannerError(error: unknown): AgentError {
  return error instanceof AgentError && error.code === "invalid_planner_output"
    ? error
    : new AgentError("invalid_planner_output", "Planner returned an invalid WorkPlan", {
        cause: error,
      });
}
