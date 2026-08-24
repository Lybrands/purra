import type { ContextEvidenceReceipt } from "../context/types.js";
import type { JsonValue, Message, ModelTokenUsage, ToolSpec } from "../model/types.js";
import type { OutputEvent, OutputEventQuery } from "../output/types.js";
import type { DurableContinuation, DurableRunResult } from "../durable/types.js";

export type RunStatus = "running" | "completed" | "failed" | "canceled";

export interface RunResult {
  readonly output: JsonValue;
  readonly messages: readonly Message[];
  readonly rounds: number;
  readonly durable?: DurableRunResult;
}

export interface PromptSection {
  readonly id: string;
  readonly role: "system" | "developer";
  readonly content: JsonValue;
}

export interface AgentPreset {
  readonly id: string;
  readonly revision: string;
  readonly promptSections?: readonly PromptSection[];
}

export interface RunRequest {
  readonly messages: readonly Message[];
  readonly enabledTools?: readonly string[];
  readonly maxOutputTokens?: number;
  readonly contextEvidence?: readonly ContextEvidenceReceipt[];
  readonly metadata?: Readonly<Record<string, JsonValue>>;
}

export interface RunBudgetOptions {
  readonly maxModelAttempts?: number | null;
  readonly maxTotalTokens?: number | null;
  readonly maxOutputBytes?: number | null;
  readonly maxOutputEvents?: number | null;
}

export interface RunBudgets {
  readonly maxModelAttempts: number | null;
  readonly maxTotalTokens: number | null;
  readonly maxOutputBytes: number | null;
  readonly maxOutputEvents: number | null;
}

export interface RunUsage {
  readonly modelAttempts: number;
  readonly knownTokens: number;
  readonly outputBytes: number;
  readonly outputEvents: number;
}

export interface RunOptions {
  readonly signal?: AbortSignal;
  readonly deadlineAt?: string;
  readonly budgets?: RunBudgetOptions;
  readonly durableContinuation?: DurableContinuation;
}

export interface AgentPresetSnapshot {
  readonly schemaVersion: 2;
  readonly presetId: string;
  readonly presetRevision: string;
  readonly promptFingerprint: string;
  readonly toolFingerprint: string;
  readonly capabilityProfileId: string | null;
  readonly compositionFingerprint: string;
}

export interface RunSnapshot {
  readonly runId: string;
  readonly status: RunStatus;
  readonly version: number;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly deadlineAt: string | null;
  readonly budgets: RunBudgets;
  readonly usage: RunUsage;
  readonly preset: AgentPresetSnapshot;
  readonly finalOutput?: JsonValue;
  readonly errorCode?: string;
}

export interface RunCancellationReceipt {
  readonly runId: string;
  readonly accepted: boolean;
  readonly status: RunStatus;
  readonly event?: OutputEvent;
  readonly events?: readonly OutputEvent[];
}

export interface RunCommand {
  readonly type: "cancel";
}

export interface RunHandle {
  readonly runId: string;
  readonly result: Promise<RunResult>;
  snapshot(): Promise<RunSnapshot>;
  cancel(): Promise<RunCancellationReceipt>;
  command(command: RunCommand): Promise<RunCancellationReceipt>;
  events(query?: OutputEventQuery): AsyncIterable<OutputEvent>;
}

export interface ModelInvocationReceipt {
  readonly schemaVersion: 1;
  readonly runId: string;
  readonly invocationId: string;
  readonly attempt: number;
  readonly messageFingerprint: string;
  readonly toolFingerprint: string;
  readonly requestFingerprint: string;
  readonly evidenceFingerprint: string;
  readonly contextEvidence: readonly ContextEvidenceReceipt[];
  readonly capabilityProfileId: string | null;
  readonly outputLimit: number | null;
  readonly openedAt: string;
}

export interface InvocationSettlement {
  readonly invocationId: string;
  readonly status: "completed" | "failed";
  readonly usage?: ModelTokenUsage;
  readonly errorCode?: string;
}

export interface RunBeginParams {
  readonly preset: AgentPresetSnapshot;
  readonly deadlineAt: string | null;
  readonly budgets: RunBudgets;
  readonly metadata: Readonly<Record<string, JsonValue>>;
}

export interface InvocationReceiptInput {
  readonly messages: readonly Message[];
  readonly tools: readonly ToolSpec[];
  readonly evidence: readonly ContextEvidenceReceipt[];
  readonly capabilityProfileId: string | null;
  readonly outputLimit: number | null;
}
