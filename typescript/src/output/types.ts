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
  | "planning.progress"
  | "agent.progress"
  | "model.diagnostics"
  | "operation.started"
  | "operation.finished"
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
  | "provider.delta_batch"
  | "model.usage"
  | "model.finish"
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
  | "durable.recovery_snapshot"
  | "agent.execution_checkpoint"
  | "input.required"
  | "input.answered";

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
  readonly rootRunId: string;
  readonly agentId: string;
  readonly parentRunId: string | null;
  readonly sequence: number;
  readonly rootSequence: number;
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

export interface OutputBatchLimits {
  readonly maxPayloadBytes: number;
  readonly maxFragments: number;
  readonly maxLatencyMs: number;
  readonly maxBackgroundLatencyMs: number;
}
