import { AgentError } from "../shared/errors.js";
import { copyJsonValue } from "../model/validation.js";
import type {
  DelegationDefinition,
  DelegationPolicyOptions,
  DelegationPolicySnapshot,
} from "./types.js";

export class DelegationPolicy {
  readonly #snapshot: DelegationPolicySnapshot;

  public constructor(options: DelegationPolicyOptions = {}) {
    const maxAgentsPerCall = positive(options.maxAgentsPerCall ?? 3, "maxAgentsPerCall");
    const maxParallel = positive(options.maxParallel ?? 3, "maxParallel");
    if (maxParallel > maxAgentsPerCall) {
      throw new TypeError("maxParallel cannot exceed maxAgentsPerCall");
    }
    if (
      options.allowRecursiveDelegation !== undefined
      && typeof options.allowRecursiveDelegation !== "boolean"
    ) {
      throw new TypeError("allowRecursiveDelegation must be boolean");
    }
    this.#snapshot = Object.freeze({
      enabled: true,
      maxAgentsPerCall,
      maxParallel,
      maxAgentNameChars: positive(options.maxAgentNameChars ?? 64, "maxAgentNameChars"),
      maxTitleChars: positive(options.maxTitleChars ?? 120, "maxTitleChars"),
      maxInstructionChars: positive(options.maxInstructionChars ?? 4_000, "maxInstructionChars"),
      maxObjectiveChars: positive(options.maxObjectiveChars ?? 4_000, "maxObjectiveChars"),
      maxDepth: positive(options.maxDepth ?? 3, "maxDepth"),
      maxAgentsPerRoot: positive(options.maxAgentsPerRoot ?? 16, "maxAgentsPerRoot"),
      contextMode: "isolated",
      toolMode: "read",
      allowsRecursiveDelegation: options.allowRecursiveDelegation ?? false,
    });
  }

  public snapshot(): DelegationPolicySnapshot {
    return this.#snapshot;
  }

  public validate(value: unknown): readonly DelegationDefinition[] {
    if (!Array.isArray(value) || value.length < 1 || value.length > this.#snapshot.maxAgentsPerCall) {
      throw new AgentError(
        "invalid_delegation_batch",
        `delegations must contain between one and ${this.#snapshot.maxAgentsPerCall} tasks`,
      );
    }
    const names = new Set<string>();
    return Object.freeze(value.map((item) => {
      if (item === null || typeof item !== "object" || Array.isArray(item)) {
        throw new AgentError("invalid_delegation", "Delegation task must be an object");
      }
      const raw = item as Record<string, unknown>;
      const agentName = bounded(raw.agentName, "agentName", this.#snapshot.maxAgentNameChars);
      if (names.has(agentName)) {
        throw new AgentError("duplicate_delegated_agent", "Delegated Agent names must be unique");
      }
      names.add(agentName);
      const input = raw.input === undefined
        ? Object.freeze({})
        : copyInput(raw.input);
      const priority = raw.priority === undefined ? 0 : integer(raw.priority, "priority");
      if (raw.required !== undefined && typeof raw.required !== "boolean") {
        throw new AgentError("invalid_delegation", "Delegation required must be a boolean");
      }
      return Object.freeze({
        agentName,
        title: bounded(raw.title, "title", this.#snapshot.maxTitleChars),
        instruction: bounded(raw.instruction, "instruction", this.#snapshot.maxInstructionChars),
        objective: bounded(raw.objective, "objective", this.#snapshot.maxObjectiveChars),
        input,
        required: raw.required ?? true,
        priority,
      });
    }));
  }
}

function copyInput(value: unknown): Readonly<Record<string, import("../model/types.js").JsonValue>> {
  const copied = copyJsonValue(value);
  if (copied === null || typeof copied !== "object" || Array.isArray(copied)) {
    throw new AgentError("invalid_delegation", "Delegation input must be an object");
  }
  return copied as Readonly<Record<string, import("../model/types.js").JsonValue>>;
}

function bounded(value: unknown, label: string, maximum: number): string {
  const text = typeof value === "string" ? value.trim() : "";
  if (text === "" || text.length > maximum) {
    throw new AgentError(
      "invalid_delegation",
      `Delegation ${label} must contain between 1 and ${maximum} characters`,
    );
  }
  return text;
}

function positive(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 1) throw new TypeError(`${label} must be positive`);
  return value;
}

function integer(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value)) throw new AgentError("invalid_delegation", `${label} must be an integer`);
  return value as number;
}
