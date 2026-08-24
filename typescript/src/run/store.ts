import type { JsonValue } from "../model/types.js";
import { copyJsonValue } from "../model/validation.js";
import type { OutputEvent, OutputEventDraft } from "../output/types.js";
import { AgentError } from "../shared/errors.js";
import type {
  InvocationSettlement,
  ModelInvocationReceipt,
  RunBeginParams,
  RunCancellationReceipt,
  RunSnapshot,
  RunStatus,
} from "./types.js";

export interface RunRepository {
  begin(params: RunBeginParams): Promise<{ readonly snapshot: RunSnapshot; readonly event: OutputEvent }>;
  openInvocation(
    runId: string,
    receipt: Omit<ModelInvocationReceipt, "attempt" | "openedAt">,
  ): Promise<{
    readonly snapshot: RunSnapshot;
    readonly receipt: ModelInvocationReceipt;
    readonly event: OutputEvent;
  }>;
  appendEvent(runId: string, draft: OutputEventDraft): Promise<OutputEvent>;
  settleInvocation(
    runId: string,
    settlement: InvocationSettlement,
  ): Promise<{ readonly event: OutputEvent; readonly budgetError?: string }>;
  settleRun(
    runId: string,
    status: Exclude<RunStatus, "running">,
    options?: {
      readonly relatedEvents?: readonly OutputEventDraft[];
      readonly finalOutput?: JsonValue;
      readonly errorCode?: string;
    },
  ): Promise<{ readonly snapshot: RunSnapshot; readonly events: readonly OutputEvent[] }>;
  cancel(runId: string): Promise<RunCancellationReceipt>;
  get(runId: string): Promise<RunSnapshot>;
  listEvents(runId: string, afterSequence: number, limit?: number): Promise<readonly OutputEvent[]>;
}

interface StoredRun {
  snapshot: RunSnapshot;
  readonly events: OutputEvent[];
  readonly bySourceKey: Map<string, OutputEvent>;
  readonly openInvocations: Set<string>;
}

const METERED_KINDS = new Set([
  "model.delta",
  "model.completed",
  "reasoning.delta",
  "commentary",
  "final",
]);

export class InMemoryRunRepository implements RunRepository {
  readonly #runs = new Map<string, StoredRun>();

  public async begin(
    params: RunBeginParams,
  ): Promise<{ readonly snapshot: RunSnapshot; readonly event: OutputEvent }> {
    const runId = globalThis.crypto.randomUUID();
    const now = new Date().toISOString();
    const snapshot = freezeSnapshot({
      runId,
      status: "running",
      version: 1,
      createdAt: now,
      updatedAt: now,
      deadlineAt: params.deadlineAt,
      budgets: params.budgets,
      usage: { modelAttempts: 0, knownTokens: 0, outputBytes: 0, outputEvents: 0 },
      preset: params.preset,
    });
    const stored: StoredRun = {
      snapshot,
      events: [],
      bySourceKey: new Map(),
      openInvocations: new Set(),
    };
    this.#runs.set(runId, stored);
    const event = append(stored, runId, {
      sourceKey: `run:${runId}:started`,
      kind: "run.started",
      channel: "lifecycle",
      visibility: "public",
      payload: copyJsonValue({ status: "running", preset: params.preset }) as Readonly<Record<string, JsonValue>>,
    }, false);
    return Object.freeze({ snapshot: stored.snapshot, event });
  }

  public async openInvocation(
    runId: string,
    input: Omit<ModelInvocationReceipt, "attempt" | "openedAt">,
  ) {
    const run = this.#active(runId);
    const nextAttempt = run.snapshot.usage.modelAttempts + 1;
    const limit = run.snapshot.budgets.maxModelAttempts;
    if (limit !== null && nextAttempt > limit) {
      throw new AgentError("run_attempt_budget_exceeded", "Run model-attempt budget is exhausted");
    }
    if (run.openInvocations.has(input.invocationId)) {
      throw new AgentError("duplicate_model_invocation", "Model invocation is already open");
    }
    const receipt: ModelInvocationReceipt = Object.freeze({
      ...input,
      attempt: nextAttempt,
      openedAt: new Date().toISOString(),
    });
    run.openInvocations.add(receipt.invocationId);
    updateUsage(run, { modelAttempts: nextAttempt });
    const event = append(run, runId, {
      sourceKey: `invocation:${receipt.invocationId}:started`,
      kind: "invocation.started",
      channel: "lifecycle",
      visibility: "private",
      payload: copyJsonValue({ receipt }) as Readonly<Record<string, JsonValue>>,
    }, false);
    return Object.freeze({ snapshot: run.snapshot, receipt, event });
  }

  public async appendEvent(runId: string, draft: OutputEventDraft): Promise<OutputEvent> {
    const run = this.#active(runId);
    const existing = run.bySourceKey.get(draft.sourceKey);
    if (existing !== undefined) return existing;
    return append(run, runId, draft, true);
  }

  public async settleInvocation(
    runId: string,
    settlement: InvocationSettlement,
  ): Promise<{ readonly event: OutputEvent; readonly budgetError?: string }> {
    const run = this.#active(runId);
    if (!run.openInvocations.delete(settlement.invocationId)) {
      throw new AgentError("model_invocation_not_open", "Model invocation is not open");
    }
    const addedTokens = settlement.usage?.totalTokens
      ?? (settlement.usage === undefined
        ? 0
        : settlement.usage.inputTokens + (settlement.usage.outputTokens ?? 0));
    const knownTokens = run.snapshot.usage.knownTokens + addedTokens;
    updateUsage(run, { knownTokens });
    const maximum = run.snapshot.budgets.maxTotalTokens;
    const budgetError = maximum !== null && knownTokens > maximum
      ? "run_token_budget_exceeded"
      : undefined;
    const status = budgetError === undefined ? settlement.status : "failed";
    const errorCode = budgetError ?? settlement.errorCode;
    const event = append(run, runId, {
      sourceKey: `invocation:${settlement.invocationId}:${status}`,
      kind: status === "completed" ? "invocation.completed" : "invocation.failed",
      channel: "lifecycle",
      visibility: "private",
      payload: {
        invocationId: settlement.invocationId,
        status,
        ...(addedTokens === 0 ? {} : { knownTokens: addedTokens }),
        ...(errorCode === undefined ? {} : { errorCode }),
      },
    }, false);
    return Object.freeze({ event, ...(budgetError === undefined ? {} : { budgetError }) });
  }

  public async settleRun(
    runId: string,
    status: Exclude<RunStatus, "running">,
    options: {
      readonly relatedEvents?: readonly OutputEventDraft[];
      readonly finalOutput?: JsonValue;
      readonly errorCode?: string;
    } = {},
  ): Promise<{ readonly snapshot: RunSnapshot; readonly events: readonly OutputEvent[] }> {
    const run = this.#require(runId);
    if (run.snapshot.status !== "running") {
      if (run.snapshot.status === status) return Object.freeze({ snapshot: run.snapshot, events: [] });
      throw new AgentError("run_terminal_conflict", "Run already has a different terminal status");
    }
    if (status === "completed" && options.finalOutput === undefined) {
      throw new TypeError("Completed Run requires finalOutput");
    }
    if (
      status === "completed"
      && run.snapshot.deadlineAt !== null
      && Date.now() >= Date.parse(run.snapshot.deadlineAt)
    ) {
      throw new AgentError("run_deadline_exceeded", "Run deadline has elapsed");
    }
    if (status === "failed" && !validText(options.errorCode)) {
      throw new TypeError("Failed Run requires errorCode");
    }
    const related = Object.freeze((options.relatedEvents ?? []).map(copyDraft));
    checkRelatedBudget(run, related);

    const events: OutputEvent[] = [];
    for (const invocationId of [...run.openInvocations]) {
      run.openInvocations.delete(invocationId);
      events.push(append(run, runId, {
        sourceKey: `invocation:${invocationId}:aborted:${status}`,
        kind: "invocation.aborted",
        channel: "lifecycle",
        visibility: "private",
        payload: { invocationId, cause: "run_terminal_commit" },
      }, false));
    }
    for (const draft of related) events.push(append(run, runId, draft, true));
    const now = new Date().toISOString();
    run.snapshot = freezeSnapshot({
      ...run.snapshot,
      status,
      version: run.snapshot.version + 1,
      updatedAt: now,
      ...(options.finalOutput === undefined
        ? {}
        : { finalOutput: copyJsonValue(options.finalOutput) }),
      ...(options.errorCode === undefined ? {} : { errorCode: normalizeCode(options.errorCode) }),
    });
    events.push(append(run, runId, {
      sourceKey: `run:${runId}:${status}`,
      kind: `run.${status}`,
      channel: "lifecycle",
      visibility: "public",
      payload: {
        status,
        ...(options.errorCode === undefined ? {} : { errorCode: normalizeCode(options.errorCode) }),
      },
    }, false));
    return Object.freeze({ snapshot: run.snapshot, events: Object.freeze(events) });
  }

  public async cancel(runId: string): Promise<RunCancellationReceipt> {
    const run = this.#require(runId);
    if (run.snapshot.status !== "running") {
      return Object.freeze({ runId, accepted: false, status: run.snapshot.status });
    }
    const settled = await this.settleRun(runId, "canceled");
    const event = settled.events.at(-1)!;
    return Object.freeze({
      runId,
      accepted: true,
      status: "canceled",
      event,
      events: settled.events,
    });
  }

  public async get(runId: string): Promise<RunSnapshot> {
    return this.#require(runId).snapshot;
  }

  public async listEvents(
    runId: string,
    afterSequence: number,
    limit = 200,
  ): Promise<readonly OutputEvent[]> {
    const run = this.#require(runId);
    if (!Number.isSafeInteger(afterSequence) || afterSequence < 0) {
      throw new TypeError("afterSequence must be a non-negative integer");
    }
    if (!Number.isSafeInteger(limit) || limit < 1) throw new TypeError("limit must be positive");
    return Object.freeze(run.events.filter((event) => event.sequence > afterSequence).slice(0, limit));
  }

  #active(runId: string): StoredRun {
    const run = this.#require(runId);
    if (run.snapshot.status !== "running") {
      throw new AgentError("run_not_active", "Run is already terminal");
    }
    if (run.snapshot.deadlineAt !== null && Date.now() >= Date.parse(run.snapshot.deadlineAt)) {
      throw new AgentError("run_deadline_exceeded", "Run deadline has elapsed");
    }
    return run;
  }

  #require(runId: string): StoredRun {
    const run = this.#runs.get(runId);
    if (run === undefined) throw new AgentError("run_not_found", "Run does not exist");
    return run;
  }
}

export async function assertRunRepositoryConforms(repository: RunRepository): Promise<void> {
  const begun = await repository.begin({
    preset: {
      schemaVersion: 2,
      presetId: "conformance",
      presetRevision: "1",
      promptFingerprint: "prompt",
      toolFingerprint: "tools",
      capabilityProfileId: null,
      compositionFingerprint: "composition",
    },
    deadlineAt: null,
    budgets: {
      maxModelAttempts: 1,
      maxTotalTokens: null,
      maxOutputBytes: 1_000,
      maxOutputEvents: 10,
    },
    metadata: {},
  });
  const final = await repository.settleRun(begun.snapshot.runId, "completed", {
    relatedEvents: [{
      sourceKey: "conformance:final",
      kind: "final",
      channel: "final",
      visibility: "public",
      payload: { output: "ok" },
    }],
    finalOutput: "ok",
  });
  const events = await repository.listEvents(begun.snapshot.runId, 0);
  if (final.snapshot.status !== "completed" || events.length !== 3) {
    throw new AgentError("run_repository_nonconforming", "Run repository failed atomic lifecycle probe");
  }
  if (events.some((event, index) => event.sequence !== index + 1)) {
    throw new AgentError("run_repository_nonconforming", "Run repository sequences are not ordered");
  }
}

function append(
  run: StoredRun,
  runId: string,
  rawDraft: OutputEventDraft,
  meter: boolean,
): OutputEvent {
  const existing = run.bySourceKey.get(rawDraft.sourceKey);
  if (existing !== undefined) return existing;
  const draft = copyDraft(rawDraft);
  if (meter) applyBudget(run, draft);
  const event: OutputEvent = Object.freeze({
    ...draft,
    eventId: globalThis.crypto.randomUUID(),
    runId,
    sequence: run.events.length + 1,
    occurredAt: new Date().toISOString(),
  });
  run.events.push(event);
  run.bySourceKey.set(event.sourceKey, event);
  run.snapshot = freezeSnapshot({
    ...run.snapshot,
    version: run.snapshot.version + 1,
    updatedAt: event.occurredAt,
  });
  return event;
}

function copyDraft(draft: OutputEventDraft): OutputEventDraft & { readonly payload: Readonly<Record<string, JsonValue>> } {
  return Object.freeze({
    sourceKey: requiredText(draft.sourceKey, "output sourceKey"),
    kind: draft.kind,
    channel: draft.channel,
    visibility: draft.visibility,
    payload: copyJsonValue(draft.payload ?? {}) as Readonly<Record<string, JsonValue>>,
  });
}

function checkBudget(run: StoredRun, draft: OutputEventDraft): void {
  if (!METERED_KINDS.has(draft.kind)) return;
  const bytes = byteLength(draft.payload ?? {});
  const byteLimit = run.snapshot.budgets.maxOutputBytes;
  const eventLimit = run.snapshot.budgets.maxOutputEvents;
  if (byteLimit !== null && run.snapshot.usage.outputBytes + bytes > byteLimit) {
    throw new AgentError("run_output_budget_exceeded", "Run output-byte budget is exhausted");
  }
  if (eventLimit !== null && run.snapshot.usage.outputEvents + 1 > eventLimit) {
    throw new AgentError("run_output_budget_exceeded", "Run output-event budget is exhausted");
  }
}

function checkRelatedBudget(run: StoredRun, drafts: readonly OutputEventDraft[]): void {
  const metered = drafts.filter((draft) => METERED_KINDS.has(draft.kind));
  const addedBytes = metered.reduce((total, draft) => total + byteLength(draft.payload ?? {}), 0);
  const byteLimit = run.snapshot.budgets.maxOutputBytes;
  const eventLimit = run.snapshot.budgets.maxOutputEvents;
  if (byteLimit !== null && run.snapshot.usage.outputBytes + addedBytes > byteLimit) {
    throw new AgentError("run_output_budget_exceeded", "Run output-byte budget is exhausted");
  }
  if (eventLimit !== null && run.snapshot.usage.outputEvents + metered.length > eventLimit) {
    throw new AgentError("run_output_budget_exceeded", "Run output-event budget is exhausted");
  }
}

function applyBudget(run: StoredRun, draft: OutputEventDraft): void {
  checkBudget(run, draft);
  if (!METERED_KINDS.has(draft.kind)) return;
  updateUsage(run, {
    outputBytes: run.snapshot.usage.outputBytes + byteLength(draft.payload ?? {}),
    outputEvents: run.snapshot.usage.outputEvents + 1,
  });
}

function updateUsage(run: StoredRun, patch: Partial<RunSnapshot["usage"]>): void {
  run.snapshot = freezeSnapshot({
    ...run.snapshot,
    version: run.snapshot.version + 1,
    updatedAt: new Date().toISOString(),
    usage: { ...run.snapshot.usage, ...patch },
  });
}

function freezeSnapshot(snapshot: RunSnapshot): RunSnapshot {
  return Object.freeze({
    ...snapshot,
    budgets: Object.freeze({ ...snapshot.budgets }),
    usage: Object.freeze({ ...snapshot.usage }),
    preset: Object.freeze({ ...snapshot.preset }),
  });
}

function byteLength(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength;
}

function normalizeCode(value: unknown): string {
  const code = typeof value === "string" ? value.trim().toLowerCase() : "";
  return /^[a-z0-9][a-z0-9_.-]{0,127}$/.test(code) ? code : "run_failed";
}

function requiredText(value: unknown, label: string): string {
  const text = typeof value === "string" ? value.trim() : "";
  if (text === "") throw new TypeError(`${label} must be non-empty text`);
  return text;
}

function validText(value: unknown): boolean {
  return typeof value === "string" && value.trim() !== "";
}
