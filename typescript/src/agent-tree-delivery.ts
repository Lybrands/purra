import type { AgentRunAggregation } from "./agent-tree.js";

export async function deliverAgentResults(
  run: (notify: (value: AgentRunAggregation) => void, signal: AbortSignal) => Promise<AgentRunAggregation>,
  deliver: (results: AgentRunAggregation["results"], signal: AbortSignal) => Promise<void>,
  signal?: AbortSignal,
): Promise<AgentRunAggregation> {
  const controller = new AbortController();
  const stop = () => controller.abort(signal?.reason);
  signal?.addEventListener("abort", stop, { once: true });
  if (signal?.aborted) stop();
  const seen = new Set<string>();
  const deliveries: Promise<void>[] = [];
  let fail!: (error: unknown) => void;
  const failure = new Promise<never>((_, reject) => { fail = reject; });
  const producer = Promise.resolve().then(() => run(aggregate => {
    for (const result of aggregate.results) {
      const id = String(result.runId);
      if (seen.has(id)) continue;
      seen.add(id);
      const task = deliver([result], controller.signal);
      deliveries.push(task);
      void task.catch(fail);
    }
  }, controller.signal));
  try {
    const result = await Promise.race([producer, failure]);
    await Promise.all(deliveries);
    return result;
  } finally {
    controller.abort();
    await Promise.allSettled([producer, ...deliveries]);
    signal?.removeEventListener("abort", stop);
  }
}
