import type { JsonValue } from "../model/types.js";

export type OutputChannel =
  | "reasoning"
  | "model"
  | "commentary"
  | "final"
  | "tool"
  | "plan"
  | "lifecycle";

export type OutputVisibility = "private" | "public";

export type OutputEventKind =
  | "agentRunTrace"
  | "run.started"
  | "run.completed"
  | "run.failed"
  | "run.canceled"
  | "invocation.started"
  | "invocation.completed"
  | "invocation.failed"
  | "invocation.aborted"
  | "model.delta"
  | "model.completed"
  | "reasoning.delta"
  | "commentary"
  | "final"
  | "tool.started"
  | "tool.completed"
  | "plan.updated"
  | "task_admission.decided"
  | "delegation.status"
  | "long_task.dispatched"
  | "long_task.progress"
  | "long_task.checkpoint"
  | "durable.recovery_snapshot";

export interface OutputEventDraft {
  readonly sourceKey: string;
  readonly kind: OutputEventKind;
  readonly channel: OutputChannel;
  readonly visibility: OutputVisibility;
  readonly payload?: Readonly<Record<string, JsonValue>>;
}

export interface OutputEvent extends OutputEventDraft {
  readonly eventId: string;
  readonly runId: string;
  readonly sequence: number;
  readonly occurredAt: string;
  readonly payload: Readonly<Record<string, JsonValue>>;
}

export interface OutputPolicy {
  authorize(event: OutputEventDraft): Promise<OutputEventDraft | null> | OutputEventDraft | null;
}

export interface OutputPublisher {
  publishCommitted(event: OutputEvent): Promise<void>;
  waitForSequence(runId: string, afterSequence: number, signal?: AbortSignal): Promise<void>;
}

export interface OutputEventQuery {
  readonly afterSequence?: number;
  readonly visibility?: "public" | "all";
  readonly signal?: AbortSignal;
}
