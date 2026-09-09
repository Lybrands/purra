import { Mem0Memory, MemoryContext, assembleMemoryContext, createManagedClient, runModel, type Mem0Client, type MemoryBudget, type MemoryResolution, type MemoryRef, type MemoryReview, type MemoryMatch } from "purra-mem0";
import { RetrieverTool, type Retriever, type ContextProvider, type ContextEvidenceReceipt, type ModelTaskRunner } from "purra";

declare const client: Mem0Client;
const memory = new Mem0Memory({ client, scope: { user: "u", project: "p" }, journalPath: "/host/journal.db" });
const retriever: Retriever = memory;
const tool = new RetrieverTool({ retriever, name: "recall", description: "Recall authorized memory." });
const context: ContextProvider = new MemoryContext({ memory, query: () => "host query", countTokens: text => text.length });
const operation = memory.add("A fact", { source: { id: "source", revision: "1" }, key: "add" });
void [tool, context, operation];

declare const runner: ModelTaskRunner;
const budget: MemoryBudget = { key: "run-1", maxLlmCalls: 2, maxEmbeddingCalls: 8, maxInputChars: 10_000, maxOutputTokens: 512, resultCapacityTargetTokens: 256 };
const managedClient = await createManagedClient({ embeddingDims: 2, config: {
  vectorStore: { provider: "memory", config: { dimension: 2, dbPath: "/host/vectors.db" } }, historyDbPath: "/host/history.db",
} });
const managed = new Mem0Memory({ client: managedClient, scope: { user: "u", project: "p" }, journalPath: "/host/journal.db",
  providers: { budget, complete: runModel(runner), async embed(texts, signal) { return { vectors: texts.map(() => [1, 0]) }; } },
});
const receipt = await managed.add("fact", { source: { id: "s", revision: "1" }, key: "k" });
if (receipt.usage !== "unknown") { const calls: number = receipt.usage.unreportedCalls; void calls; }
void managed.budgetUsage();
declare const evidence: readonly ContextEvidenceReceipt[];
await managed.validateEvidence(evidence);
await managed.revokeSource("s", { revision: "1", key: "withdraw:s:1" });
const revoked: boolean = managed.isSourceRevoked({ id: "s", revision: "1" });
void revoked;
const refs: readonly MemoryRef[] = [{ id: "old", version: 1 }, { id: "candidate", version: 1 }];
const decision: MemoryResolution = { kind: "supersede", items: refs, keep: "candidate" };
// @ts-expect-error A duplicate decision must select a keeper.
const missingKeeper: MemoryResolution = { kind: "duplicate", items: refs };
// @ts-expect-error An unresolved conflict cannot select a winner.
const conflictWinner: MemoryResolution = { kind: "conflict", items: refs, keep: "candidate" };
const resolved = await managed.resolve(decision, { key: "review" });
const kind: MemoryResolution["kind"] | undefined = resolved.resolution?.kind;
const resolutionKey: string | undefined = (await managed.get("candidate"))?.resolutionKey;
void [kind, resolutionKey];
void [missingKeeper, conflictWinner];
const reviewed: MemoryReview | undefined = (await managed.review({ id: "candidate", version: 1 }, { key: "review", limit: 8 })).review;
const matches: readonly MemoryMatch[] | undefined = reviewed?.matches;
if (reviewed?.proposal) await managed.resolve(reviewed.proposal, { key: "apply" });
const reviewKey: string | undefined = reviewed?.proposal?.reviewKey;
void [matches, reviewKey];

const page = await managed.list({ state: null, filters: { kind: ["plot", "setting"], pinned: true }, scanLimit: 100 });
const next: string | null = page.next;
const epoch: number = page.epoch;
const result = await assembleMemoryContext(managed, page.items.map(item => item.id), 1024, { expectedEpoch: epoch });
await managed.annotate("candidate", { kind: "plot", pinned: true }, { version: 1, key: "pin" });
await managed.link({ id: "candidate", version: 2 }, { id: "other", version: 1 }, "supports", { key: "link" });
const valid: boolean | undefined = (await managed.links("candidate")).items[0]?.valid;
// @ts-expect-error Control metadata cannot contain nested values.
await managed.annotate("candidate", { nested: { unsafe: true } }, { version: 1, key: "invalid" });
// @ts-expect-error Administration returns an explicit page, not a legacy array.
page.map(item => item.id);
void [next, result, valid];

const selectedContext: ContextProvider = new MemoryContext({ memory,
  async selectIds(request, signal) {
    const hits = await memory.retrieve({ query: 'host query', limit: 8, scope: {} }, signal);
    return hits.map(hit => hit.id);
  },
});
void selectedContext;

import { proposeMemoryRelations, type MemoryRelationExtractor, type MemoryRelationExtractionInput, type MemoryRelationProposal } from "purra-mem0";

const extractRelations: MemoryRelationExtractor = async (input, signal) => {
  const payload: MemoryRelationExtractionInput = input;
  const cancellation: AbortSignal | undefined = signal;
  // @ts-expect-error Extractors cannot change the host's allowed relations.
  payload.relations.push("invented");
  // @ts-expect-error Extractors receive immutable source records.
  payload.records[0]!.text = "replacement";
  void cancellation;
  return [];
};
const proposals: readonly MemoryRelationProposal[] = await proposeMemoryRelations(memory, refs, {
  relations: ["supports"], extract: extractRelations, maxInputChars: 1024, maxProposals: 8,
});
for (const proposal of proposals) {
  const source: string = proposal.fromSource.id;
  const quote: string = proposal.fromQuote;
  // @ts-expect-error Proposals do not allow changing their source revision.
  proposal.from.version = 2;
  // Host adoption uses the existing explicit write API.
  await memory.link(proposal.from, proposal.to, proposal.relation, { key: "host-adoption" });
  void [source, quote];
}
// @ts-expect-error The extractor must be asynchronous.
const syncExtractor: MemoryRelationExtractor = () => [];
void syncExtractor;
