/** Private declaration surface for the generated SDK extension. */
import type { Mem0Client } from "./memory.js";
import type { DirectLlm, DirectEmbedder } from "./direct-providers.js";
export class Memory implements Mem0Client {
  constructor(config: Record<string, unknown>, providers: { llm: DirectLlm; embedder: DirectEmbedder });
  ready(): Promise<this>;
  add: Mem0Client["add"];
  get: Mem0Client["get"];
  getAll: Mem0Client["getAll"];
  search: Mem0Client["search"];
  update: Mem0Client["update"];
  delete: Mem0Client["delete"];
  history: Mem0Client["history"];
}
