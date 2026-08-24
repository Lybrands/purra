import { AgentCanceledError } from "../shared/errors.js";
import type { OutputEvent, OutputPublisher } from "./types.js";

interface Waiter {
  readonly afterSequence: number;
  readonly resolve: () => void;
}

export class InMemoryOutputPublisher implements OutputPublisher {
  readonly #latest = new Map<string, number>();
  readonly #waiters = new Map<string, Set<Waiter>>();

  public async publishCommitted(event: OutputEvent): Promise<void> {
    const latest = Math.max(this.#latest.get(event.runId) ?? 0, event.sequence);
    this.#latest.set(event.runId, latest);
    const waiters = this.#waiters.get(event.runId);
    if (waiters === undefined) return;
    for (const waiter of waiters) {
      if (latest <= waiter.afterSequence) continue;
      waiters.delete(waiter);
      waiter.resolve();
    }
    if (waiters.size === 0) this.#waiters.delete(event.runId);
  }

  public async waitForSequence(
    runId: string,
    afterSequence: number,
    signal?: AbortSignal,
  ): Promise<void> {
    if ((this.#latest.get(runId) ?? 0) > afterSequence) return;
    if (signal?.aborted === true) throw new AgentCanceledError();
    await new Promise<void>((resolve, reject) => {
      const waiter: Waiter = { afterSequence, resolve: finish };
      const waiters = this.#waiters.get(runId) ?? new Set<Waiter>();
      waiters.add(waiter);
      this.#waiters.set(runId, waiters);
      const canceled = (): void => finish(new AgentCanceledError());
      signal?.addEventListener("abort", canceled, { once: true });

      function finish(error?: Error): void {
        waiters.delete(waiter);
        signal?.removeEventListener("abort", canceled);
        if (error === undefined) resolve();
        else reject(error);
      }
    });
  }
}
