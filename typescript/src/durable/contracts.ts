import { copyJsonValue } from "../model/validation.js";
import { AgentError } from "../shared/errors.js";
import type { ExecutionPlan } from "../planning/types.js";
import type {
  ComponentBinding,
  DurableTaskDescriptor,
  ExecutionRecipe,
  ExecutionRecipeStep,
  TaskAdmissionDecision,
} from "./types.js";

export function copyComponentBinding(value: ComponentBinding, label: string): ComponentBinding {
  if (value === null || typeof value !== "object") throw new TypeError(`${label} must be an object`);
  return Object.freeze({
    id: requiredText(value.id, `${label} id`),
    revision: requiredText(value.revision, `${label} revision`),
  });
}

export function copyExecutionRecipe(value: ExecutionRecipe): ExecutionRecipe {
  if (value === null || typeof value !== "object" || !Array.isArray(value.steps)) {
    throw new AgentError("invalid_execution_recipe", "Execution recipe must contain steps");
  }
  if (value.steps.length === 0) {
    throw new AgentError("invalid_execution_recipe", "Execution recipe must not be empty");
  }
  const known = new Set<string>();
  const allIds = new Set(value.steps.map((step) => requiredText(step.id, "recipe step id")));
  if (allIds.size !== value.steps.length) {
    throw new AgentError("invalid_execution_recipe", "Execution recipe step ids must be unique");
  }
  const steps = Object.freeze(value.steps.map((step) => {
    const copied = copyRecipeStep(step);
    const unknown = copied.dependsOn!.filter((dependency) => !allIds.has(dependency));
    if (unknown.length > 0) {
      throw new AgentError("invalid_execution_recipe", `Unknown recipe dependency: ${unknown[0]}`);
    }
    if (copied.dependsOn!.some((dependency) => !known.has(dependency))) {
      throw new AgentError("invalid_execution_recipe", "Execution recipe must be topologically ordered");
    }
    known.add(copied.id);
    return copied;
  }));
  return Object.freeze({
    kind: requiredText(value.kind, "execution recipe kind"),
    steps,
    maxParallelism: positiveInteger(value.maxParallelism ?? 1, "recipe maxParallelism"),
    metadata: copyMapping(value.metadata ?? {}, "recipe metadata"),
  });
}

export function copyAdmissionDecision(
  value: TaskAdmissionDecision,
  plan: ExecutionPlan,
): TaskAdmissionDecision {
  if (value === null || typeof value !== "object") {
    throw new AgentError("invalid_task_admission", "Task admission must be an object");
  }
  if (!["inline", "durable", "clarify", "reject"].includes(value.mode)) {
    throw new AgentError("invalid_task_admission", "Task admission mode is invalid");
  }
  const coveredStepIds = copyNames(value.coveredStepIds ?? [], "covered step ids");
  const executionRecipe = value.executionRecipe === undefined
    ? undefined
    : copyExecutionRecipe(value.executionRecipe);
  if (value.mode === "durable") {
    if (executionRecipe === undefined || coveredStepIds.length === 0) {
      throw new AgentError(
        "invalid_task_admission",
        "Durable admission requires a recipe and covered steps",
      );
    }
    validateAdmissionCoverage(plan, coveredStepIds, executionRecipe);
  } else if (executionRecipe !== undefined || coveredStepIds.length > 0) {
    throw new AgentError(
      "invalid_task_admission",
      "Only durable admission may carry a recipe or covered steps",
    );
  }
  return Object.freeze({
    mode: value.mode,
    reasonCode: requiredText(value.reasonCode, "task admission reasonCode"),
    estimatedUnits: nonNegativeInteger(value.estimatedUnits ?? 1, "estimatedUnits"),
    estimatedModelCalls: nonNegativeInteger(value.estimatedModelCalls ?? 1, "estimatedModelCalls"),
    requiresConfirmation: value.requiresConfirmation ?? false,
    ...(value.message === undefined ? {} : { message: requiredText(value.message, "admission message") }),
    coveredStepIds,
    ...(executionRecipe === undefined ? {} : { executionRecipe }),
    metadata: copyMapping(value.metadata ?? {}, "admission metadata"),
  });
}

export function copyDurableTaskDescriptor(value: DurableTaskDescriptor): DurableTaskDescriptor {
  if (value === null || typeof value !== "object") {
    throw new AgentError("invalid_durable_descriptor", "Durable descriptor must be an object");
  }
  return Object.freeze({
    namespace: requiredText(value.namespace, "durable namespace"),
    ownerId: requiredText(value.ownerId, "durable ownerId"),
    idempotencyKey: requiredText(value.idempotencyKey, "durable idempotencyKey"),
    ...(value.message === undefined ? {} : { message: requiredText(value.message, "durable message") }),
    metadata: copyMapping(value.metadata ?? {}, "durable descriptor metadata"),
  });
}

export function validateAdmissionCoverage(
  plan: ExecutionPlan,
  coveredStepIds: readonly string[],
  recipe: ExecutionRecipe,
): void {
  const planned = new Set(plan.steps.map((step) => step.id));
  const covered = new Set(coveredStepIds);
  if (planned.size !== covered.size || [...planned].some((id) => !covered.has(id))) {
    throw new AgentError(
      "durable_plan_coverage_invalid",
      "Durable admission must cover every compiled plan step",
    );
  }
  if (recipe.steps.some((step) => !covered.has(step.planStepId))) {
    throw new AgentError(
      "durable_plan_coverage_invalid",
      "Execution recipe references an uncovered plan step",
    );
  }
  const represented = new Set(recipe.steps.map((step) => step.planStepId));
  if ([...covered].some((id) => !represented.has(id))) {
    throw new AgentError(
      "durable_plan_coverage_invalid",
      "Execution recipe must represent every covered plan step",
    );
  }
}

function copyRecipeStep(value: ExecutionRecipeStep): ExecutionRecipeStep & { readonly dependsOn: readonly string[] } {
  if (value === null || typeof value !== "object") {
    throw new AgentError("invalid_execution_recipe", "Recipe step must be an object");
  }
  const id = requiredText(value.id, "recipe step id");
  const dependsOn = copyNames(value.dependsOn ?? [], `recipe step ${id} dependencies`);
  if (dependsOn.includes(id)) {
    throw new AgentError("invalid_execution_recipe", "Recipe step cannot depend on itself");
  }
  return Object.freeze({
    id,
    kind: requiredText(value.kind, `recipe step ${id} kind`),
    dependsOn,
    ...(value.inputRef === undefined ? {} : { inputRef: requiredText(value.inputRef, `recipe step ${id} inputRef`) }),
    executor: requiredText(value.executor, `recipe step ${id} executor`),
    planStepId: requiredText(value.planStepId, `recipe step ${id} planStepId`),
    maxAttempts: positiveInteger(value.maxAttempts ?? 1, `recipe step ${id} maxAttempts`),
    metadata: copyMapping(value.metadata ?? {}, `recipe step ${id} metadata`),
  });
}

function copyNames(value: readonly string[], label: string): readonly string[] {
  if (!Array.isArray(value)) throw new TypeError(`${label} must be an array`);
  const result = value.map((item) => requiredText(item, label));
  if (new Set(result).size !== result.length) throw new TypeError(`${label} must be unique`);
  return Object.freeze(result);
}

function copyMapping(
  value: Readonly<Record<string, import("../model/types.js").JsonValue>>,
  label: string,
): Readonly<Record<string, import("../model/types.js").JsonValue>> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object`);
  }
  return copyJsonValue(value) as Readonly<Record<string, import("../model/types.js").JsonValue>>;
}

function requiredText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim() === "") throw new TypeError(`${label} is required`);
  return value.trim();
}

function positiveInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 1) throw new TypeError(`${label} must be positive`);
  return value;
}

function nonNegativeInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new TypeError(`${label} must be non-negative`);
  return value;
}
