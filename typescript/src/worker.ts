import type { RecoveryInspection } from "./observability/inspection.js";

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
  #active = false;
  readonly #discover: () => Promise<readonly string[]>;
  readonly #inspect: (runId: string) => Promise<RecoveryInspection>;
  readonly #resume: (runId: string) => Promise<unknown>;

  constructor(options: {
    discover: () => Promise<readonly string[]>;
    inspect: (runId: string) => Promise<RecoveryInspection>;
    resume: (runId: string) => Promise<unknown>;
  }) {
    this.#discover = options.discover;
    this.#inspect = options.inspect;
    this.#resume = options.resume;
  }

  async runOnce(): Promise<readonly RecoveryWorkerResult[]> {
    if (this.#active) throw new Error("recovery_worker_scan_active");
    this.#active = true;
    try {
      const results: RecoveryWorkerResult[] = [];
      for (const runId of new Set(await this.#discover())) {
        let stage = "inspection_failed";
        try {
          const report = await this.#inspect(runId);
          if (report.blockers.length > 0) {
            results.push(Object.freeze({ runId, action: "blocked", reasons: Object.freeze([...report.blockers]) }));
            continue;
          }
          stage = "resume_failed";
          await this.#resume(runId);
          results.push(Object.freeze({ runId, action: "settled", reasons: Object.freeze([]) }));
        } catch {
          // Do not expose exception messages containing host/tool data.
          results.push(Object.freeze({ runId, action: "failed", reasons: Object.freeze([stage]) }));
        }
      }
      return Object.freeze(results);
    } finally {
      this.#active = false;
    }
  }
}
