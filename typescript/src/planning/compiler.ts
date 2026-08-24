import { copyJsonValue } from "../model/validation.js";
import { AgentError } from "../shared/errors.js";
import type {
  CompiledExecutionPlan,
  ExecutionPlan,
  ExecutionStep,
  PlanningConstraints,
  PlanningToolRegistration,
  StepExecutor,
  StepType,
  TaskSpec,
  WorkPlan,
  WorkStep,
} from "./types.js";
import type { ToolSpec } from "../model/types.js";

const STEP_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;
const STEP_TYPES = new Set<StepType>(["read", "analyze", "write", "confirm", "review"]);
const EXECUTORS = new Set<StepExecutor>(["model", "tool"]);

export function copyPlanningConstraints(value: PlanningConstraints = {}): PlanningConstraints {
  if (value === null || typeof value !== "object") {
    throw new AgentError("invalid_planning_constraints", "Planning constraints must be an object");
  }
  const maxSteps = value.maxSteps === undefined
    ? undefined
    : positiveInteger(value.maxSteps, "planning maxSteps");
  const allowedCapabilityNames = copyNames(value.allowedCapabilityNames, "allowed capabilities");
  const excludedCapabilityNames = copyNames(value.excludedCapabilityNames, "excluded capabilities");
  if (
    allowedCapabilityNames !== undefined
    && excludedCapabilityNames?.some((name) => allowedCapabilityNames.includes(name)) === true
  ) {
    throw new AgentError(
      "invalid_planning_constraints",
      "A planning capability cannot be both allowed and excluded",
    );
  }
  return Object.freeze({
    ...(maxSteps === undefined ? {} : { maxSteps }),
    ...(allowedCapabilityNames === undefined ? {} : { allowedCapabilityNames }),
    ...(excludedCapabilityNames === undefined ? {} : { excludedCapabilityNames }),
  });
}

export function copyWorkPlan(value: WorkPlan): WorkPlan {
  if (value === null || typeof value !== "object" || !Array.isArray(value.steps)) {
    throw new AgentError("invalid_planner_output", "Planner must return a WorkPlan");
  }
  if (value.steps.length === 0) {
    throw new AgentError("invalid_planner_output", "WorkPlan must contain at least one step");
  }
  const ids = new Set<string>();
  const steps = Object.freeze(value.steps.map((step, index) => {
    const copied = copyWorkStep(step);
    if (ids.has(copied.id)) {
      throw new AgentError("invalid_planner_output", `Duplicate WorkPlan step id: ${copied.id}`);
    }
    for (const dependency of copied.dependsOn ?? []) {
      if (!ids.has(dependency)) {
        throw new AgentError(
          "invalid_planner_output",
          `WorkPlan step ${copied.id} depends on a missing or later step: ${dependency}`,
        );
      }
    }
    ids.add(copied.id);
    if (copied.executor === "tool" && copied.capabilityNames?.length !== 1) {
      throw new AgentError(
        "invalid_planner_output",
        `Tool step ${copied.id} must select exactly one public capability`,
      );
    }
    if (copied.executor === "model" && (copied.capabilityNames?.length ?? 0) > 0) {
      throw new AgentError(
        "invalid_planner_output",
        `Model step ${copied.id} cannot select a tool capability`,
      );
    }
    return copied;
  }));
  return Object.freeze({
    title: requiredText(value.title, "WorkPlan title"),
    ...(value.goal === undefined ? {} : { goal: requiredText(value.goal, "WorkPlan goal") }),
    ...(value.taskSpec === undefined ? {} : { taskSpec: copyTaskSpec(value.taskSpec) }),
    steps,
  });
}

export function compileWorkPlan(
  rawPlan: WorkPlan,
  registrations: readonly PlanningToolRegistration[],
  constraints: PlanningConstraints = {},
  satisfiedToolNames: ReadonlySet<string> = new Set(),
): CompiledExecutionPlan {
  const plan = copyWorkPlan(rawPlan);
  const normalizedConstraints = copyPlanningConstraints(constraints);
  if (plan.steps.length > (normalizedConstraints.maxSteps ?? Number.MAX_SAFE_INTEGER)) {
    throw new AgentError("invalid_planner_output", "WorkPlan exceeds the host step limit");
  }
  const byRuntime = new Map(registrations.map((item) => [item.runtimeName, item]));
  const capabilities = groupCapabilities(registrations);
  const availablePlanningNames = new Set([
    ...registrations.filter((item) => item.planningCapability === undefined).map((item) => item.runtimeName),
    ...capabilities.keys(),
  ]);
  const allowed = normalizedConstraints.allowedCapabilityNames === undefined
    ? availablePlanningNames
    : new Set(normalizedConstraints.allowedCapabilityNames);
  const excluded = new Set(normalizedConstraints.excludedCapabilityNames ?? []);
  for (const name of allowed) {
    if (!availablePlanningNames.has(name)) {
      throw new AgentError("invalid_planning_constraints", `Unknown allowed capability: ${name}`);
    }
  }

  const completed = new Set(satisfiedToolNames);
  const inserted: string[] = [];
  const lowered: string[] = [];
  const expanded: ExecutionStep[] = [];
  const existingIds = new Set(plan.steps.map((step) => step.id));
  let prerequisiteSequence = 0;

  const appendPrerequisites = (runtimeName: string, path: readonly string[]): void => {
    if (path.includes(runtimeName)) {
      throw new AgentError(
        "invalid_tool_planning_contract",
        `Tool prerequisite cycle: ${[...path, runtimeName].join(" -> ")}`,
      );
    }
    const registration = byRuntime.get(runtimeName);
    if (registration === undefined) {
      throw new AgentError(
        "invalid_tool_planning_contract",
        `Tool prerequisite is unavailable: ${runtimeName}`,
      );
    }
    for (const dependency of registration.prerequisiteTools) {
      if (completed.has(dependency)) continue;
      appendPrerequisites(dependency, [...path, runtimeName]);
      if (completed.has(dependency)) continue;
      const prerequisite = byRuntime.get(dependency)!;
      prerequisiteSequence += 1;
      expanded.push(executionStep({
        id: uniqueId(`host-prerequisite-${dependency}-${prerequisiteSequence}`, existingIds),
        title: prerequisite.title,
        type: prerequisite.riskLevel === "read" ? "read" : "analyze",
        executor: "tool",
        riskLevel: prerequisite.riskLevel,
        capabilityNames: Object.freeze([]),
        description: `Core-inserted prerequisite for ${runtimeName}`,
      }, [dependency], true));
      completed.add(dependency);
      inserted.push(dependency);
    }
  };

  for (const step of plan.steps) {
    if (step.executor === "model") {
      expanded.push(executionStep(step, [], false));
      continue;
    }
    const selected = step.capabilityNames![0]!;
    const direct = byRuntime.get(selected);
    if (direct?.planningCapability !== undefined) {
      throw new AgentError(
        "private_runtime_tool_selected",
        `Plan must select public capability ${direct.planningCapability.name} instead of ${selected}`,
      );
    }
    if (!allowed.has(selected) || excluded.has(selected)) {
      throw new AgentError("plan_capability_not_allowed", `Plan capability is not allowed: ${selected}`);
    }
    if (direct !== undefined) {
      appendPrerequisites(selected, []);
      expanded.push(executionStep(step, [selected], false));
      completed.add(selected);
      continue;
    }
    const members = capabilities.get(selected);
    if (members === undefined) {
      throw new AgentError("unknown_planning_capability", `Unknown planning capability: ${selected}`);
    }
    const sequence = orderedRuntimeNames(members).filter((name) => !completed.has(name));
    if (sequence.length === 0) {
      throw new AgentError(
        "redundant_planning_capability",
        `Planning capability is already satisfied: ${selected}`,
      );
    }
    for (let index = 0; index < sequence.length; index += 1) {
      const runtimeName = sequence[index]!;
      appendPrerequisites(runtimeName, []);
      const terminal = index === sequence.length - 1;
      const registration = byRuntime.get(runtimeName)!;
      const loweredStep: WorkStep = terminal
        ? step
        : Object.freeze({
            ...step,
            id: uniqueId(`${step.id}-protocol-${index + 1}`, existingIds),
            title: registration.title,
            description: `Core-private execution protocol for ${selected}`,
          });
      expanded.push(executionStep(loweredStep, [runtimeName], !terminal, selected));
      completed.add(runtimeName);
      lowered.push(runtimeName);
    }
  }

  const executionPlan: ExecutionPlan = Object.freeze({
    title: plan.title,
    ...(plan.goal === undefined ? {} : { goal: plan.goal }),
    ...(plan.taskSpec === undefined ? {} : { taskSpec: plan.taskSpec }),
    steps: Object.freeze(expanded),
    workStepIds: Object.freeze(plan.steps.map((step) => step.id)),
  });
  return Object.freeze({
    executionPlan,
    insertedToolNames: Object.freeze(inserted),
    loweredToolNames: Object.freeze(lowered),
  });
}

export function planningToolSpecs(
  registrations: readonly PlanningToolRegistration[],
  constraints: PlanningConstraints = {},
): readonly ToolSpec[] {
  const normalized = copyPlanningConstraints(constraints);
  const runtimeNames = new Set(registrations.map((item) => item.runtimeName));
  const specs = new Map<string, ToolSpec>();
  for (const registration of registrations) {
    const spec = registration.planningCapability ?? registration.runtimeSpec;
    if (registration.planningCapability !== undefined && runtimeNames.has(spec.name)) {
      throw new AgentError(
        "invalid_tool_planning_contract",
        `Planning capability collides with runtime tool: ${spec.name}`,
      );
    }
    const previous = specs.get(spec.name);
    if (previous !== undefined && JSON.stringify(previous) !== JSON.stringify(spec)) {
      throw new AgentError(
        "invalid_tool_planning_contract",
        `Planning capability declarations conflict: ${spec.name}`,
      );
    }
    specs.set(spec.name, spec);
  }
  const allowed = normalized.allowedCapabilityNames === undefined
    ? undefined
    : new Set(normalized.allowedCapabilityNames);
  const excluded = new Set(normalized.excludedCapabilityNames ?? []);
  for (const name of [...(allowed ?? []), ...excluded]) {
    if (!specs.has(name)) {
      throw new AgentError(
        "invalid_planning_constraints",
        `Planning constraints name an unavailable capability: ${name}`,
      );
    }
  }
  return Object.freeze([...specs.values()].filter((spec) => (
    (allowed === undefined || allowed.has(spec.name)) && !excluded.has(spec.name)
  )));
}

function groupCapabilities(
  registrations: readonly PlanningToolRegistration[],
): Map<string, readonly PlanningToolRegistration[]> {
  const runtimeNames = new Set(registrations.map((item) => item.runtimeName));
  const schemas = new Map<string, string>();
  const grouped = new Map<string, PlanningToolRegistration[]>();
  for (const registration of registrations) {
    const capability = registration.planningCapability;
    if (capability === undefined) continue;
    if (runtimeNames.has(capability.name)) {
      throw new AgentError(
        "invalid_tool_planning_contract",
        `Planning capability collides with runtime tool: ${capability.name}`,
      );
    }
    const fingerprint = JSON.stringify(capability);
    if (schemas.has(capability.name) && schemas.get(capability.name) !== fingerprint) {
      throw new AgentError(
        "invalid_tool_planning_contract",
        `Planning capability declarations conflict: ${capability.name}`,
      );
    }
    schemas.set(capability.name, fingerprint);
    const members = grouped.get(capability.name) ?? [];
    members.push(registration);
    grouped.set(capability.name, members);
  }
  return new Map([...grouped].map(([name, members]) => [name, Object.freeze(members)]));
}

function orderedRuntimeNames(registrations: readonly PlanningToolRegistration[]): readonly string[] {
  const members = new Map(registrations.map((item) => [item.runtimeName, item]));
  const ordered: string[] = [];
  const visiting = new Set<string>();
  const visited = new Set<string>();
  const visit = (name: string): void => {
    if (visited.has(name)) return;
    if (visiting.has(name)) {
      throw new AgentError("invalid_tool_planning_contract", "Planning capability has a dependency cycle");
    }
    visiting.add(name);
    for (const dependency of members.get(name)!.prerequisiteTools) {
      if (members.has(dependency)) visit(dependency);
    }
    visiting.delete(name);
    visited.add(name);
    ordered.push(name);
  };
  for (const name of members.keys()) visit(name);
  return Object.freeze(ordered);
}

function executionStep(
  step: WorkStep,
  runtimeToolNames: readonly string[],
  protocolPrivate: boolean,
  planningCapability?: string,
): ExecutionStep {
  return Object.freeze({
    ...step,
    status: "pending",
    runtimeToolNames: Object.freeze([...runtimeToolNames]),
    protocolPrivate,
    ...(planningCapability === undefined ? {} : { planningCapability }),
  });
}

function copyWorkStep(value: WorkStep): WorkStep {
  if (value === null || typeof value !== "object") {
    throw new AgentError("invalid_planner_output", "WorkPlan step must be an object");
  }
  if (!STEP_ID.test(value.id)) {
    throw new AgentError("invalid_planner_output", "WorkPlan step id is invalid");
  }
  if (!STEP_TYPES.has(value.type)) {
    throw new AgentError("invalid_planner_output", `WorkPlan step ${value.id} has an invalid type`);
  }
  if (!EXECUTORS.has(value.executor)) {
    throw new AgentError("invalid_planner_output", `WorkPlan step ${value.id} has an invalid executor`);
  }
  const capabilityNames = copyNames(value.capabilityNames, `step ${value.id} capabilities`);
  const dependsOn = copyNames(value.dependsOn, `step ${value.id} dependencies`);
  const riskLevel = value.riskLevel ?? "read";
  if (riskLevel !== "read" && riskLevel !== "write" && riskLevel !== "destructive") {
    throw new AgentError("invalid_planner_output", `WorkPlan step ${value.id} has an invalid risk level`);
  }
  return Object.freeze({
    id: value.id,
    title: requiredText(value.title, `WorkPlan step ${value.id} title`),
    type: value.type,
    executor: value.executor,
    riskLevel,
    ...(capabilityNames === undefined ? {} : { capabilityNames }),
    ...(dependsOn === undefined ? {} : { dependsOn }),
    ...(value.description === undefined
      ? {}
      : { description: requiredText(value.description, `WorkPlan step ${value.id} description`) }),
  });
}

function copyTaskSpec(value: TaskSpec): TaskSpec {
  if (value === null || typeof value !== "object") {
    throw new AgentError("invalid_planner_output", "TaskSpec must be an object");
  }
  const constraints = copyTextArray(value.constraints, "TaskSpec constraints");
  const preserve = copyTextArray(value.preserve, "TaskSpec preserve rules");
  return Object.freeze({
    goal: requiredText(value.goal, "TaskSpec goal"),
    ...(value.target === undefined ? {} : { target: copyJsonValue(value.target) }),
    ...(value.operation === undefined ? {} : { operation: requiredText(value.operation, "TaskSpec operation") }),
    ...(value.instruction === undefined ? {} : { instruction: requiredText(value.instruction, "TaskSpec instruction") }),
    ...(value.deliverable === undefined ? {} : { deliverable: requiredText(value.deliverable, "TaskSpec deliverable") }),
    ...(constraints === undefined ? {} : { constraints }),
    ...(preserve === undefined ? {} : { preserve }),
  });
}

function copyNames(values: readonly string[] | undefined, label: string): readonly string[] | undefined {
  if (values === undefined) return undefined;
  if (!Array.isArray(values)) throw new AgentError("invalid_planner_output", `${label} must be an array`);
  const result = values.map((value) => requiredText(value, label));
  if (new Set(result).size !== result.length) {
    throw new AgentError("invalid_planner_output", `${label} must be unique`);
  }
  return Object.freeze(result);
}

function copyTextArray(values: readonly string[] | undefined, label: string): readonly string[] | undefined {
  return copyNames(values, label);
}

function uniqueId(candidate: string, existing: Set<string>): string {
  const base = candidate.replace(/[^A-Za-z0-9._-]/g, "-").slice(0, 64) || "step";
  let value = base;
  let suffix = 2;
  while (existing.has(value)) {
    const tail = `-${suffix}`;
    value = base.slice(0, 64 - tail.length) + tail;
    suffix += 1;
  }
  existing.add(value);
  return value;
}

function requiredText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim() === "") {
    throw new AgentError("invalid_planner_output", `${label} must be non-empty text`);
  }
  return value.trim();
}

function positiveInteger(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new AgentError("invalid_planning_constraints", `${label} must be a positive integer`);
  }
  return value;
}
