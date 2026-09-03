import type { ContextBlock, ContextEvidenceReceipt, ContextBudget, ContextBudgetClaim, ContextBundle, ContextProvider, ContextRequest } from "purra";
import type { RetrievalHit } from "purra";
import { estimateJsonTokens } from "purra";
import { Mem0Memory, positiveInteger, requiredText } from "./memory.js";

export class MemoryContext implements ContextProvider {
  readonly #memory: Mem0Memory;
  readonly #query: (request: ContextRequest) => string;
  readonly #countTokens: (content: string) => number;
  readonly #name: string;
  readonly #desiredTokens: number;
  readonly #limit: number;
  constructor(options: {
    memory: Mem0Memory; query: (request: ContextRequest) => string; countTokens?: (content: string) => number;
    name?: string; desiredTokens?: number; limit?: number;
  }) {
    this.#memory = options.memory;
    this.#query = options.query;
    this.#countTokens = options.countTokens ?? estimateJsonTokens;
    this.#name = requiredText(options.name ?? "memory", "context name", 128);
    this.#desiredTokens = positiveInteger(options.desiredTokens ?? 1024, "desiredTokens", 1_000_000);
    this.#limit = positiveInteger(options.limit ?? 8, "limit");
    if (typeof this.#query !== "function" || typeof this.#countTokens !== "function") throw new TypeError("query and countTokens must be host functions");
  }
  describeContextDemands(): readonly ContextBudgetClaim[] {
    return [{ name: this.#name, desiredTokens: this.#desiredTokens }];
  }
  async buildContext(request: ContextRequest, budget: ContextBudget, signal?: AbortSignal): Promise<ContextBundle> {
    const allowance = budget.contextAllocations[this.#name] ?? 0;
    if (!allowance) return { blocks: [] };
    const epoch = this.#memory.epoch;
    const hits = await this.#memory.retrieve({ query: this.#query(request), limit: this.#limit, scope: {} }, signal);
    const result = await assembleMemoryContext(this.#memory, hits.map(hit => hit.id), allowance,
      { name: this.#name, countTokens: this.#countTokens, expectedEpoch: epoch, ...(signal ? { signal } : {}) });
    return { blocks: result.block ? [result.block] : [] };
  }
}

export interface MemoryContextResult {
  readonly block?: ContextBlock;
  readonly included: readonly string[];
  readonly deferred: readonly string[];
  readonly missing: readonly string[];
  readonly receipts: readonly ContextEvidenceReceipt[];
}

/** Fresh authorized records; shared by Agent and non-Run consumers. */
export async function assembleMemoryContext(memory: Mem0Memory, ids: readonly string[], allowance: number,
  options: { name?: string; countTokens?: (content: string) => number; expectedEpoch?: number; signal?: AbortSignal } = {},
): Promise<MemoryContextResult> {
  if (!Number.isSafeInteger(allowance) || allowance < 0 || allowance > 1_000_000) throw new TypeError("invalid context allowance");
  const name = requiredText(options.name ?? "memory", "context name", 128);
  const countTokens = options.countTokens ?? estimateJsonTokens;
  const epoch = options.expectedEpoch ?? memory.epoch;
  memory.assertEpoch(epoch);
  const hits = await memory.select(ids, options.signal ? { signal: options.signal } : {});
  const selected: RetrievalHit[] = [], deferred: string[] = [], rows: unknown[] = [];
  let content = "", tokenCount = 0;
  for (const hit of hits) {
    const row = { id: hit.id, version: hit.version, text: hit.content, sourceId: hit.metadata.sourceId,
      sourceRevision: hit.metadata.sourceRevision, metadata: hit.metadata.metadata };
    const candidate = JSON.stringify([...rows, row]);
    let count = countTokens(candidate);
    if (!Number.isSafeInteger(count) || count < 0) throw new TypeError("countTokens must return a non-negative integer");
    count = Math.max(count, estimateJsonTokens(candidate));
    if (count <= allowance) { rows.push(row); selected.push(hit); content = candidate; tokenCount = count; }
    else deferred.push(hit.id);
  }
  const receipts = Object.freeze(selected.map(hit => Object.freeze({ evidenceId: hit.metadata.evidenceId as string,
    contextBlock: name, source: hit.source, itemId: hit.id, version: String(hit.version) })));
  await memory.validateEvidence(receipts, options.signal ? { signal: options.signal } : {});
  memory.assertEpoch(epoch);
  const found = new Set(hits.map(hit => hit.id));
  return Object.freeze({ ...(selected.length ? { block: { name, content, tokenCount, untrusted: true, evidence: receipts } } : {}),
    included: Object.freeze(selected.map(hit => hit.id)), deferred: Object.freeze(deferred),
    missing: Object.freeze([...new Set(ids.filter(id => !found.has(id)))]), receipts });
}
