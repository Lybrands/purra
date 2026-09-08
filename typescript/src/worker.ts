import type { RecoveryInspection } from "./observability/inspection.js";

export interface RecoverySchedule {
  check?(runId: string): Promise<Readonly<{ revision: number | null; reason: "retry_exhausted" | "retry_not_due" | null }>>;
  ready(runId: string): Promise<number | null>;
  settle(runId: string, revision: number, failed: boolean): Promise<boolean>;
}

export interface RecoveryWorkerResult {
  readonly runId: string;
  readonly action: "blocked" | "settled" | "failed";
  readonly reasons: readonly string[];
}

/** Host-driven serial recovery scans; persisted Runs remain the queue authority.
 * resume must await the public Run handle outcome, never dispatch a tool directly.
 * The host reconstructs requests/bindings and schedules subsequent scans.
 */
export class RecoveryWorker {
  readonly #limit: number | undefined;
  #queue: string[] = [];
  #phase = "idle";
  #lastScan: Readonly<{ outcome: string; candidates: number; visited: number; deferred: number; blocked: number; settled: number; failed: number }> | null = null;
  readonly #schedule: RecoverySchedule | undefined;
  #active = false;
  #serving = false;
  #wakePending = false;
  #notify: (() => void) | undefined;
  readonly #discover: () => Promise<readonly string[]>;
  readonly #inspect: (runId: string) => Promise<RecoveryInspection>;
  readonly #resume: (runId: string) => Promise<unknown>;

  constructor(options: {
    schedule?: RecoverySchedule;
    maxRunsPerScan?: number;
    discover: () => Promise<readonly string[]>;
    inspect: (runId: string) => Promise<RecoveryInspection>;
    resume: (runId: string) => Promise<unknown>;
  }) {
    if (options.maxRunsPerScan !== undefined && (!Number.isInteger(options.maxRunsPerScan) || options.maxRunsPerScan <= 0 || options.maxRunsPerScan > 2147483647)) throw new TypeError("maxRunsPerScan must be a positive 32-bit integer");
    this.#limit = options.maxRunsPerScan;
    this.#schedule = options.schedule;
    this.#discover = options.discover;
    this.#inspect = options.inspect;
    this.#resume = options.resume;
  }

  diagnostics() {
    return Object.freeze({ schemaVersion: 1 as const, authority: "diagnosis_only" as const,
      phase: this.#phase, serving: this.#serving, maxRunsPerScan: this.#limit ?? null,
      lastScan: this.#lastScan });
  }

  /** Call after committing host state; notifications coalesce and are hints only. */
  wake(): void {
    this.#wakePending = true;
    this.#notify?.();
  }

  async run(options: {
    signal: AbortSignal;
    pollIntervalMs?: number;
    maxBackoffMs?: number;
    onScan?: (results: readonly RecoveryWorkerResult[]) => Promise<void>;
  }): Promise<void> {
    const interval = options.pollIntervalMs ?? 1000;
    const maximum = options.maxBackoffMs ?? 30000;
    for (const value of [interval, maximum]) {
      if (!Number.isInteger(value) || value <= 0 || value > 2147483647) throw new TypeError("worker intervals must be positive 32-bit integers");
    }
    if (maximum < interval) throw new TypeError("worker backoff must cover polling interval");
    if (this.#serving || this.#active) throw new Error("recovery_worker_scan_active");
    this.#serving = true;
    let delay = interval;
    try {
      while (!options.signal.aborted) {
        this.#wakePending = false;
        const report = await this.#scan(() => options.signal.aborted);
        this.#phase = "observing";
        await options.onScan?.(report);
        if (options.signal.aborted) break;
        delay = report.some(r => r.action === "failed") ? Math.min(maximum, delay * 2) : interval;
        this.#phase = "waiting";
        await this.#wait(delay, options.signal);
      }
    } finally {
      this.#serving = false;
      this.#phase = "idle";
    }
  }

  async #wait(delay: number, signal: AbortSignal): Promise<void> {
    if (signal.aborted || this.#wakePending) return;
    await new Promise<void>(resolve => {
      const done = () => {
        globalThis.clearTimeout(timer);
        signal.removeEventListener("abort", done);
        this.#notify = undefined;
        resolve();
      };
      const timer = globalThis.setTimeout(done, delay);
      this.#notify = done;
      signal.addEventListener("abort", done, { once: true });
    });
  }

  async runOnce(): Promise<readonly RecoveryWorkerResult[]> {
    if (this.#serving || this.#active) throw new Error("recovery_worker_scan_active");
    return this.#scan(() => false);
  }

  async #scan(stopped: () => boolean): Promise<readonly RecoveryWorkerResult[]> {
    this.#active = true;
    const results: RecoveryWorkerResult[] = [];
    let candidates = 0, visited = 0, outcome = "interrupted";
    try {
      this.#phase = "discovering";
      const discovered = [...new Set(await this.#discover())];
      const present = new Set(discovered);
      this.#queue = this.#queue.filter(id => present.has(id));
      const queued = new Set(this.#queue);
      this.#queue = this.#queue.concat(discovered.filter(id => !queued.has(id)));
      candidates = this.#queue.length;
      const batch = this.#queue.slice(0, this.#limit);
      for (const runId of batch) {
        if (stopped()) break;
        visited++;
        this.#phase = "scheduling";
        const eligibility = this.#schedule?.check === undefined ? undefined : await this.#schedule.check(runId);
        const revision = eligibility === undefined ? (this.#schedule === undefined ? 0 : await this.#schedule.ready(runId)) : eligibility.revision;
        if (revision === null) {
          results.push(Object.freeze({ runId, action: "blocked", reasons: Object.freeze([eligibility?.reason ?? "retry_not_due"]) }));
          continue;
        }
        let stage = "inspection_failed";
        try {
          this.#phase = "inspecting";
          const report = await this.#inspect(runId);
          if (report.blockers.length > 0) {
            results.push(Object.freeze({ runId, action: "blocked", reasons: Object.freeze([...report.blockers]) }));
          } else {
            if (stopped()) break;
            stage = "resume_failed";
            this.#phase = "resuming";
            await this.#resume(runId);
            results.push(Object.freeze({ runId, action: "settled", reasons: Object.freeze([]) }));
          }
        } catch {
          // Do not expose exception messages containing host/tool data.
          results.push(Object.freeze({ runId, action: "failed", reasons: Object.freeze([stage]) }));
        }
        this.#phase = "settling";
        await this.#schedule?.settle(runId, revision, results[results.length - 1]!.action === "failed");
      }
      outcome = stopped() ? "stopped" : "complete";
      return Object.freeze(results);
    } catch (error) {
      outcome = "failed";
      throw error;
    } finally {
      this.#queue = this.#queue.slice(visited).concat(this.#queue.slice(0, visited));
      this.#lastScan = Object.freeze({ outcome, candidates, visited, deferred: candidates - visited,
        blocked: results.filter(r => r.action === "blocked").length,
        settled: results.filter(r => r.action === "settled").length,
        failed: results.filter(r => r.action === "failed").length });
      this.#active = false;
      this.#phase = "idle";
    }
  }
}
