import { AgentError } from "../shared/errors.js";
import type { LongTaskRepository } from "./types.js";

export type OrphanRunDisposition = "cancel" | "defer_active" | "pause_recoverable" | "fail";
export type OrphanRunReason =
  | "cancellation_requested"
  | "durable_task_active"
  | "durable_task_interrupted"
  | "execution_interrupted";

export interface OrphanTaskEvidence {
  readonly taskId: string;
  readonly revision: number;
}

export interface OrphanRunCandidate {
  readonly runId: string;
  readonly executionAttempt?: number;
  readonly executionOwnerId?: string | null;
  readonly cancellationRequestedAtMs?: number | null;
  readonly activeTasks?: readonly OrphanTaskEvidence[];
  readonly recoverableTasks?: readonly OrphanTaskEvidence[];
}

export interface OrphanRunDecision {
  readonly runId: string;
  readonly disposition: OrphanRunDisposition;
  readonly reason: OrphanRunReason;
  readonly activeTasks: readonly OrphanTaskEvidence[];
  readonly recoverableTasks: readonly OrphanTaskEvidence[];
  readonly terminalStatus: "failed" | "canceled" | null;
}

export interface OrphanExecutionLease {
  readonly runId: string;
  readonly status: "running" | "completed" | "failed" | "canceled";
  readonly ownerId: string | null;
  readonly cancellationRequestedAtMs: number | null;
}

export interface OrphanRunControlStore {
  listOrphans(input: {
    readonly timestampMs: number;
    readonly afterRestart: boolean;
  }): Promise<readonly OrphanRunCandidate[]>;
  claimOrphan(input: {
    readonly candidate: OrphanRunCandidate;
    readonly ownerId: string;
    readonly leaseDurationMs: number;
    readonly timestampMs: number;
    readonly afterRestart: boolean;
  }): Promise<boolean>;
  loadExecutionLease(runId: string): Promise<OrphanExecutionLease | undefined>;
  release(runId: string, ownerId: string): Promise<boolean>;
}

export type OrphanRunSettlement = (
  decision: OrphanRunDecision,
  afterRestart: boolean,
) => Promise<void> | void;

export function decideOrphanRun(value: OrphanRunCandidate): OrphanRunDecision {
  const candidate = copyCandidate(value);
  let disposition: OrphanRunDisposition;
  let reason: OrphanRunReason;
  let terminalStatus: OrphanRunDecision["terminalStatus"];
  if (candidate.cancellationRequestedAtMs !== null) {
    disposition = "cancel";
    reason = "cancellation_requested";
    terminalStatus = "canceled";
  } else if (candidate.activeTasks.length > 0) {
    disposition = "defer_active";
    reason = "durable_task_active";
    terminalStatus = null;
  } else if (candidate.recoverableTasks.length > 0) {
    disposition = "pause_recoverable";
    reason = "durable_task_interrupted";
    terminalStatus = "canceled";
  } else {
    disposition = "fail";
    reason = "execution_interrupted";
    terminalStatus = "failed";
  }
  return Object.freeze({
    runId: candidate.runId,
    disposition,
    reason,
    activeTasks: candidate.activeTasks,
    recoverableTasks: candidate.recoverableTasks,
    terminalStatus,
  });
}

export class OrphanRecoveryCoordinator {
  readonly #control: OrphanRunControlStore;
  readonly #longTasks: LongTaskRepository;
  readonly #ownerId: string;
  readonly #settle: OrphanRunSettlement;
  readonly #leaseDurationMs: number;
  readonly #clockMs: () => number;

  public constructor(options: {
    readonly control: OrphanRunControlStore;
    readonly longTasks: LongTaskRepository;
    readonly ownerId: string;
    readonly settle: OrphanRunSettlement;
    readonly leaseDurationMs?: number;
    readonly clockMs?: () => number;
  }) {
    if (typeof options.control?.listOrphans !== "function"
        || typeof options.control.claimOrphan !== "function"
        || typeof options.control.loadExecutionLease !== "function"
        || typeof options.control.release !== "function") {
      throw new TypeError("Orphan recovery requires a Run control store");
    }
    if (typeof options.longTasks?.load !== "function" || typeof options.longTasks.pause !== "function") {
      throw new TypeError("Orphan recovery requires a Long Task repository");
    }
    if (typeof options.settle !== "function") {
      throw new TypeError("Orphan recovery requires a settlement callback");
    }
    this.#control = options.control;
    this.#longTasks = options.longTasks;
    this.#ownerId = requiredText(options.ownerId, "orphan recovery ownerId");
    this.#settle = options.settle;
    this.#leaseDurationMs = positiveInteger(
      options.leaseDurationMs ?? 30_000,
      "orphan recovery lease duration",
    );
    this.#clockMs = options.clockMs ?? Date.now;
  }

  public async recover(options: {
    readonly timestampMs?: number;
    readonly afterRestart?: boolean;
  } = {}): Promise<readonly string[]> {
    const timestampMs = options.timestampMs === undefined
      ? nonNegativeInteger(this.#clockMs(), "orphan recovery timestamp")
      : nonNegativeInteger(options.timestampMs, "orphan recovery timestamp");
    const afterRestart = options.afterRestart ?? false;
    const candidates = await this.#control.listOrphans({ timestampMs, afterRestart });
    const priority: Readonly<Record<OrphanRunDisposition, number>> = Object.freeze({
      cancel: 0,
      pause_recoverable: 1,
      fail: 2,
      defer_active: 3,
    });
    const entries = candidates.map((raw) => {
      const candidate = copyCandidate(raw);
      return { candidate, decision: decideOrphanRun(candidate) };
    }).sort((left, right) => (
      priority[left.decision.disposition] - priority[right.decision.disposition]
      || left.candidate.runId.localeCompare(right.candidate.runId)
    ));
    const recovered: string[] = [];
    for (const entry of entries) {
      if (entry.decision.disposition === "defer_active") continue;
      if (await this.#recoverOne(entry.candidate, entry.decision, timestampMs, afterRestart)) {
        recovered.push(entry.candidate.runId);
      }
    }
    return Object.freeze(recovered);
  }

  async #recoverOne(
    candidate: NormalizedOrphanRunCandidate,
    originalDecision: OrphanRunDecision,
    timestampMs: number,
    afterRestart: boolean,
  ): Promise<boolean> {
    const claimed = await this.#control.claimOrphan({
      candidate,
      ownerId: this.#ownerId,
      leaseDurationMs: this.#leaseDurationMs,
      timestampMs,
      afterRestart,
    });
    if (!claimed) return false;
    const current = await this.#control.loadExecutionLease(candidate.runId);
    if (
      current === undefined
      || current.status !== "running"
      || current.ownerId !== this.#ownerId
    ) {
      return false;
    }
    const decision = current.cancellationRequestedAtMs !== null
      && originalDecision.disposition !== "cancel"
      ? decideOrphanRun({ ...candidate, cancellationRequestedAtMs: current.cancellationRequestedAtMs })
      : originalDecision;
    try {
      if (decision.disposition === "pause_recoverable") {
        for (const evidence of decision.recoverableTasks) {
          const task = await this.#longTasks.load(evidence.taskId);
          if (
            task === undefined
            || task.status === "completed"
            || task.status === "failed"
            || task.status === "canceled"
          ) {
            await this.#control.release(candidate.runId, this.#ownerId);
            return false;
          }
          try {
            const paused = await this.#longTasks.pause(evidence.taskId, {
              expectedRevision: evidence.revision,
              reasonCode: decision.reason,
            });
            if (paused.status !== "paused") {
              await this.#control.release(candidate.runId, this.#ownerId);
              return false;
            }
          } catch (error) {
            if (error instanceof AgentError && error.code === "stale_long_task_revision") {
              await this.#control.release(candidate.runId, this.#ownerId);
              return false;
            }
            throw error;
          }
        }
      }
      await this.#settle(decision, afterRestart);
      return true;
    } catch (error) {
      await this.#control.release(candidate.runId, this.#ownerId);
      throw error;
    }
  }
}

interface NormalizedOrphanRunCandidate {
  readonly runId: string;
  readonly executionAttempt: number;
  readonly executionOwnerId: string | null;
  readonly cancellationRequestedAtMs: number | null;
  readonly activeTasks: readonly OrphanTaskEvidence[];
  readonly recoverableTasks: readonly OrphanTaskEvidence[];
}

function copyCandidate(value: OrphanRunCandidate): NormalizedOrphanRunCandidate {
  if (value === null || typeof value !== "object") {
    throw new TypeError("Orphan recovery requires a candidate");
  }
  const activeTasks = copyEvidence(value.activeTasks ?? []);
  const recoverableTasks = copyEvidence(value.recoverableTasks ?? []);
  const activeIds = new Set(activeTasks.map((item) => item.taskId));
  if (recoverableTasks.some((item) => activeIds.has(item.taskId))) {
    throw new TypeError("Orphan task evidence must be disjoint");
  }
  return Object.freeze({
    runId: requiredText(value.runId, "orphan runId"),
    executionAttempt: nonNegativeInteger(value.executionAttempt ?? 0, "orphan execution attempt"),
    executionOwnerId: value.executionOwnerId === undefined || value.executionOwnerId === null
      ? null
      : requiredText(value.executionOwnerId, "orphan execution ownerId"),
    cancellationRequestedAtMs: value.cancellationRequestedAtMs === undefined
      || value.cancellationRequestedAtMs === null
      ? null
      : nonNegativeInteger(value.cancellationRequestedAtMs, "orphan cancellation timestamp"),
    activeTasks,
    recoverableTasks,
  });
}

function copyEvidence(values: readonly OrphanTaskEvidence[]): readonly OrphanTaskEvidence[] {
  if (!Array.isArray(values)) throw new TypeError("Orphan task evidence must be an array");
  const result = new Map<string, OrphanTaskEvidence>();
  for (const value of values) {
    if (value === null || typeof value !== "object") {
      throw new TypeError("Orphan task evidence is invalid");
    }
    const evidence = Object.freeze({
      taskId: requiredText(value.taskId, "orphan task id"),
      revision: positiveInteger(value.revision, "orphan task revision"),
    });
    const previous = result.get(evidence.taskId);
    if (previous !== undefined && previous.revision !== evidence.revision) {
      throw new TypeError("Orphan task evidence revisions conflict");
    }
    result.set(evidence.taskId, evidence);
  }
  return Object.freeze([...result.values()]);
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
