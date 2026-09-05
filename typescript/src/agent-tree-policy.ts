import { copyJsonValue } from "./model/validation.js";
import type { JsonValue } from "./model/types.js";
import { AgentError } from "./shared/errors.js";

export interface ChildAgentDefinition {
  readonly name: string;
  readonly title: string;
  readonly instruction: string;
  readonly objective: string;
  readonly input?: Readonly<Record<string, JsonValue>>;
  readonly required?: boolean;
  readonly priority?: number;
}

export interface AgentTreePolicyOptions {
  readonly maxChildrenPerCall?: number;
  readonly maxParallelRuns?: number;
  readonly maxAgentNameChars?: number;
  readonly maxTitleChars?: number;
  readonly maxInstructionChars?: number;
  readonly maxObjectiveChars?: number;
  readonly maxDepth?: number;
  readonly maxAgentsPerRoot?: number;
  readonly allowRecursiveAgents?: boolean;
}

export interface AgentTreePolicySnapshot {
  readonly enabled: true;
  readonly maxChildrenPerCall: number;
  readonly maxParallelRuns: number;
  readonly maxAgentNameChars: number;
  readonly maxTitleChars: number;
  readonly maxInstructionChars: number;
  readonly maxObjectiveChars: number;
  readonly maxDepth: number;
  readonly maxAgentsPerRoot: number;
  readonly allowsRecursiveAgents: boolean;
}

export class AgentTreePolicy {
  readonly #snapshot: AgentTreePolicySnapshot;

  public constructor(options: AgentTreePolicyOptions = {}) {
    const maxChildrenPerCall = positive(
      options.maxChildrenPerCall ?? 3,
      "maxChildrenPerCall",
    );
    const maxParallelRuns = positive(
      options.maxParallelRuns ?? 3,
      "maxParallelRuns",
    );
    if (maxParallelRuns > maxChildrenPerCall) {
      throw new TypeError(
        "maxParallelRuns cannot exceed maxChildrenPerCall",
      );
    }
    if (
      options.allowRecursiveAgents !== undefined
      && typeof options.allowRecursiveAgents !== "boolean"
    ) {
      throw new TypeError("allowRecursiveAgents must be boolean");
    }
    this.#snapshot = Object.freeze({
      enabled: true,
      maxChildrenPerCall,
      maxParallelRuns,
      maxAgentNameChars: positive(
        options.maxAgentNameChars ?? 64,
        "maxAgentNameChars",
      ),
      maxTitleChars: positive(options.maxTitleChars ?? 120, "maxTitleChars"),
      maxInstructionChars: positive(
        options.maxInstructionChars ?? 4_000,
        "maxInstructionChars",
      ),
      maxObjectiveChars: positive(
        options.maxObjectiveChars ?? 4_000,
        "maxObjectiveChars",
      ),
      maxDepth: positive(options.maxDepth ?? 3, "maxDepth"),
      maxAgentsPerRoot: positive(
        options.maxAgentsPerRoot ?? 16,
        "maxAgentsPerRoot",
      ),
      allowsRecursiveAgents: options.allowRecursiveAgents ?? false,
    });
  }

  public snapshot(): AgentTreePolicySnapshot {
    return this.#snapshot;
  }

  public validateChildren(value: unknown): readonly ChildAgentDefinition[] {
    if (
      !Array.isArray(value)
      || value.length < 1
      || value.length > this.#snapshot.maxChildrenPerCall
    ) {
      throw new AgentError(
        "invalid_child_agent_batch",
        `children must contain between one and ${this.#snapshot.maxChildrenPerCall} Child Agents`,
      );
    }
    const names = new Set<string>();
    return Object.freeze(value.map((item) => {
      if (item === null || typeof item !== "object" || Array.isArray(item)) {
        throw new AgentError(
          "invalid_child_agent",
          "Child Agent definition must be an object",
        );
      }
      const raw = item as Record<string, unknown>;
      const name = bounded(
        raw.name,
        "name",
        this.#snapshot.maxAgentNameChars,
      );
      if (names.has(name)) {
        throw new AgentError(
          "duplicate_child_agent",
          "Child Agent names must be unique within one call",
        );
      }
      names.add(name);
      if (raw.required !== undefined && typeof raw.required !== "boolean") {
        throw new AgentError(
          "invalid_child_agent",
          "Child Agent required must be boolean",
        );
      }
      return Object.freeze({
        name,
        title: bounded(raw.title, "title", this.#snapshot.maxTitleChars),
        instruction: bounded(
          raw.instruction,
          "instruction",
          this.#snapshot.maxInstructionChars,
        ),
        objective: bounded(
          raw.objective,
          "objective",
          this.#snapshot.maxObjectiveChars,
        ),
        input: copyInput(raw.input),
        required: raw.required ?? true,
        priority: raw.priority === undefined
          ? 0
          : integer(raw.priority, "priority"),
      });
    }));
  }
}

function copyInput(value: unknown): Readonly<Record<string, JsonValue>> {
  if (value === undefined) return Object.freeze({});
  const copied = copyJsonValue(value);
  if (copied === null || typeof copied !== "object" || Array.isArray(copied)) {
    throw new AgentError(
      "invalid_child_agent",
      "Child Agent input must be an object",
    );
  }
  return copied as Readonly<Record<string, JsonValue>>;
}

function bounded(value: unknown, label: string, maximum: number): string {
  const text = typeof value === "string" ? value.trim() : "";
  if (text === "" || text.length > maximum) {
    throw new AgentError(
      "invalid_child_agent",
      `Child Agent ${label} must contain between 1 and ${maximum} characters`,
    );
  }
  return text;
}

function positive(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new TypeError(`${label} must be positive`);
  }
  return value;
}

function integer(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value)) {
    throw new AgentError("invalid_child_agent", `${label} must be an integer`);
  }
  return value as number;
}
