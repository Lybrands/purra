/** Optional Node.js integration; not imported by PurrA Core. */
export { MemoryError } from "./journal.js";
export { Mem0Memory } from "./memory.js";
export type { Mem0Client, MemoryScope, MemorySource, MemoryRecord, MemoryMetadata, MemoryFilters, MemoryPage, MemoryOperation, MemoryRef, MemoryLink, MemoryLinkPage, MemoryResolution, MemoryReview, MemoryMatch } from "./memory.js";
export type { MemoryContextResult } from "./context.js";
export { MemoryContext, assembleMemoryContext } from "./context.js";
export { createManagedClient, runModel } from "./providers.js";
export type { MemoryBudget, MemoryProviders, MemoryUsage, EmbeddingResult, ManagedMem0Config } from "./providers.js";
export { MemoryWorkflow } from "./workflow.js";
export type { MemoryWorkflowResult, MemoryDecisionPolicy } from "./workflow.js";
