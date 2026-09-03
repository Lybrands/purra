import { createHash } from "node:crypto";
import { Mem0Memory, requiredText, positiveInteger } from "./memory.js";
import type { MemoryOperation, MemoryRecord, MemoryResolution, MemoryReview } from "./memory.js";

export type MemoryDecisionPolicy = (candidate: MemoryRecord, review: MemoryReview) => Promise<MemoryResolution | undefined> | MemoryResolution | undefined;
export interface MemoryWorkflowResult {
  readonly extraction: MemoryOperation;
  readonly resolutions: readonly MemoryOperation[];
  readonly pendingIds: readonly string[];
}
function digest(value: string): string { return createHash("sha256").update(value).digest("hex"); }

/** Reuse the journal on retry; authorization remains an explicit host policy. */
export class MemoryWorkflow {
  readonly #memory: Mem0Memory;
  readonly #policy: MemoryDecisionPolicy | undefined;
  readonly #revision: string;
  readonly #limit: number;
  constructor(memory: Mem0Memory, options: { policy?: MemoryDecisionPolicy; policyRevision?: string; reviewLimit?: number } = {}) {
    this.#memory = memory;
    this.#policy = options.policy;
    if (this.#policy !== undefined && typeof this.#policy !== "function") throw new TypeError("policy must be callable");
    this.#revision = requiredText(options.policyRevision ?? "review-only", "policy revision", 512);
    this.#limit = positiveInteger(options.reviewLimit ?? 8, "review limit", 32);
  }
  async capture(messages: Parameters<Mem0Memory["extract"]>[0], options: Parameters<Mem0Memory["extract"]>[1]): Promise<MemoryWorkflowResult> {
    const prefix = "workflow:" + digest(requiredText(options.key, "workflow key", 512));
    const extraction = await this.#memory.extract(messages, { ...options, key: prefix + ":extract" });
    const resolutions: MemoryOperation[] = [];
    const pendingIds: string[] = [];
    if (extraction.state === "complete") for (const id of extraction.ids) {
      const suffix = digest(this.#revision + "\0" + id);
      const resolveKey = prefix + ":resolve:" + suffix;
      const existing = this.#memory.operation(resolveKey);
      if (existing) {
        resolutions.push(existing);
        if (existing.state !== "complete") pendingIds.push(id);
        continue;
      }
      const signal = options.signal === undefined ? {} : { signal: options.signal };
      const record = await this.#memory.get(id, { includeInactive: true, ...signal });
      if (!record || record.state !== "pending") continue;
      const reviewed = await this.#memory.review({ id, version: record.version }, {
        key: prefix + ":review:" + suffix, limit: this.#limit, ...signal,
      });
      if (reviewed.state !== "complete" || !reviewed.review || !this.#policy) { pendingIds.push(id); continue; }
      const decision = await this.#policy(record, reviewed.review);
      if (!decision) { pendingIds.push(id); continue; }
      if (decision.reviewKey !== reviewed.review.key || !decision.items.some(ref => ref.id === id && ref.version === record.version)) {
        throw new TypeError("Workflow decision must include the candidate and its review key");
      }
      const resolved = await this.#memory.resolve(decision, { key: resolveKey, ...signal });
      resolutions.push(resolved);
      if (resolved.state !== "complete") pendingIds.push(id);
    }
    return Object.freeze({ extraction, resolutions: Object.freeze(resolutions), pendingIds: Object.freeze(pendingIds) });
  }
}
