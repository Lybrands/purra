import { AsyncLocalStorage } from "node:async_hooks";
import type { Message, ModelTaskRunner, ModelTurn } from "purra";
import { Journal, MemoryError } from "./journal.js";
import type { Mem0Client } from "./memory.js";

/** Durable per-namespace envelope. Reservations are never refunded. */
export interface MemoryBudget {
  readonly key: string;
  readonly maxLlmCalls: number;
  readonly maxEmbeddingCalls: number;
  readonly maxInputChars: number;
  readonly maxOutputTokens: number;
  /** Per-call result sizing target. This does not reduce the Provider generation allowance. */
  readonly resultCapacityTargetTokens: number;
}
export interface MemoryUsage {
  readonly llmCalls: number;
  readonly embeddingCalls: number;
  readonly inputChars: number;
  readonly reservedOutputTokens: number;
  readonly reportedInputTokens: number;
  readonly reportedOutputTokens: number;
  readonly unreportedCalls: number;
  readonly unsettledCalls: number;
}
export interface EmbeddingResult { readonly vectors: readonly (readonly number[])[]; readonly inputTokens?: number }
/** Trusted callbacks: preserve the result-capacity target and disable hidden retries. */
export interface MemoryProviders {
  readonly budget: MemoryBudget;
  readonly complete: (messages: readonly Message[], resultCapacityTargetTokens: number, signal: AbortSignal) => Promise<ModelTurn>;
  readonly embed: (texts: readonly string[], signal: AbortSignal) => Promise<EmbeddingResult>;
}

/** Use the real Run-injected runner. Background ingestion must not fabricate a Run. */
export function runModel(runner: ModelTaskRunner): MemoryProviders["complete"] {
  return async (messages, resultCapacityTargetTokens, signal) => (await runner.complete(messages, {
    resultCapacityTargetTokens,
    resultCapacitySource: "workflow_policy",
    signal,
  })).turn;
}

export function providerLimits(providers: MemoryProviders): Record<string, number> {
  if (typeof providers?.complete !== "function" || typeof providers?.embed !== "function") throw new TypeError("invalid memory providers");
  const b = providers.budget;
  if (typeof b?.key !== "string" || !b.key.trim() || [...b.key].length > 512) throw new TypeError("invalid budget key");
  const limits = { max_llm_calls: b.maxLlmCalls, max_embedding_calls: b.maxEmbeddingCalls,
    max_input_chars: b.maxInputChars, max_output_tokens: b.maxOutputTokens,
    result_capacity_target_tokens: b.resultCapacityTargetTokens };
  for (const [name, value] of Object.entries(limits)) {
    if (!Number.isSafeInteger(value) || value < (name === "result_capacity_target_tokens" ? 1 : 0) || value > 2 ** 31 - 1) throw new TypeError(`invalid ${name}`);
  }
  return limits;
}

const current = new AsyncLocalStorage<ProviderExecution>();
export function currentExecution(journal?: Journal): ProviderExecution | undefined {
  const value = current.getStore();
  return journal === undefined || value?.journal === journal ? value : undefined;
}
function execution(): ProviderExecution {
  const value = currentExecution();
  if (!value) throw new MemoryError("memory_provider_unbound");
  return value;
}

export class ProviderExecution {
  readonly controller = new AbortController();
  readonly deadline: number;
  operation: string | undefined;
  error: string | undefined;
  constructor(readonly providers: MemoryProviders, readonly journal: Journal, readonly dimensions: number,
    timeoutMs: number, readonly maxResults: number, readonly maxInput: number) { this.deadline = performance.now() + timeoutMs; }
  stop(code: string): void {
    if (!this.error) {
      this.error = code;
      if (this.operation) this.journal.providerError(this.operation, code);
    }
    this.controller.abort(new MemoryError(this.error));
  }
  check(): void {
    if (performance.now() >= this.deadline) this.stop("memory_timeout");
    if (this.error) throw new MemoryError(this.error);
  }
  run<T>(work: () => Promise<T>): Promise<T> {
    return current.run(this, async () => {
      try {
        this.check();
        const result = await work();
        this.check(); // SDK fallback must not convert a swallowed denial to success.
        return result;
      } catch (error) {
        if (this.error) throw new MemoryError(this.error);
        throw error;
      }
    });
  }
  async invoke(kind: "llm", values: readonly Message[], extraction?: boolean): Promise<string>;
  async invoke(kind: "embedding", values: readonly string[]): Promise<number[][]>;
  async invoke(kind: "llm" | "embedding", values: readonly Message[] | readonly string[], extraction = true): Promise<string | number[][]> {
    this.check();
    const resultTarget = kind === "llm" ? this.providers.budget.resultCapacityTargetTokens : 0;
    const chars = values.reduce<number>((sum, value) => sum + [...(typeof value === "string" ? value : value.content as string)].length, 0);
    let id: string;
    try { id = this.journal.admit(this.providers.budget.key, this.operation, kind, chars, resultTarget); }
    catch (error) { this.stop(error instanceof MemoryError ? error.code : "memory_provider_error"); throw new MemoryError(this.error!); }
    let inputTokens: number | null = null;
    let generationTokens: number | null = null;
    try {
      this.check();
      let content: string | number[][];
      if (kind === "llm") {
        const result = await this.providers.complete(
          values as readonly Message[],
          resultTarget,
          this.controller.signal,
        );
        if (result?.usage) {
          if (!validTokens(result.usage.inputTokens) || (result.usage.generationTokens !== undefined && !validTokens(result.usage.generationTokens))) {
            throw new MemoryError("memory_provider_contract");
          }
          inputTokens = result.usage.inputTokens;
          generationTokens = result.usage.generationTokens ?? null;
        }
        const appliedGenerationLimit = result?.appliedGenerationLimit;
        if (!Number.isSafeInteger(appliedGenerationLimit) || (appliedGenerationLimit as number) < resultTarget
            || result.finishReason !== "stop" || result.message?.role !== "assistant"
            || result.message.toolCalls?.length || typeof result.message.content !== "string"
            || (generationTokens !== null && generationTokens > (appliedGenerationLimit as number))) {
          throw new MemoryError("memory_provider_contract");
        }
        content = result.message.content;
        if (extraction) {
          let parsed;
          try { parsed = JSON.parse(content) as { memory?: unknown }; }
          catch { throw new MemoryError("memory_invalid_extraction"); }
          if (!parsed || !Array.isArray(parsed.memory) || parsed.memory.length > this.maxResults) throw new MemoryError("memory_invalid_extraction");
          for (const item of parsed.memory as { text?: unknown; entities?: unknown }[]) {
            if (!item || typeof item.text !== "string" || !item.text.trim() || [...item.text].length > this.maxInput
                || (item.entities !== undefined && (!Array.isArray(item.entities) || item.entities.some(e => typeof e !== "string")))) {
              throw new MemoryError("memory_invalid_extraction");
            }
          }
        }
      } else {
        const result = await this.providers.embed(values as readonly string[], this.controller.signal);
        if (result?.inputTokens !== undefined && !validTokens(result.inputTokens)) throw new MemoryError("memory_provider_contract");
        inputTokens = result.inputTokens ?? null;
        generationTokens = 0;
        if (!Array.isArray(result.vectors) || result.vectors.length !== values.length || result.vectors.some(v =>
          !Array.isArray(v) || v.length !== this.dimensions || v.some(x => typeof x !== "number" || !Number.isFinite(x)))) {
          throw new MemoryError("memory_provider_contract");
        }
        content = result.vectors.map(v => [...v]);
      }
      this.check();
      this.journal.settle(id, "complete", inputTokens, generationTokens);
      return content;
    } catch (error) {
      this.journal.settle(id, "failed", inputTokens, generationTokens);
      this.stop(error instanceof MemoryError ? error.code : "memory_provider_error");
      throw new MemoryError(this.error!);
    }
  }
}
function validTokens(value: unknown): value is number { return Number.isSafeInteger(value) && (value as number) >= 0 && (value as number) <= 2 ** 31 - 1; }

/** Host owns the SDK handle and its storage resources. */
export class ManagedMem0Client implements Mem0Client {
  constructor(readonly sdk: Mem0Client, readonly dimensions: number) {}
  add(...args: Parameters<Mem0Client["add"]>): Promise<unknown> { return this.sdk.add(...args); }
  get(...args: Parameters<Mem0Client["get"]>): Promise<unknown> { return this.sdk.get(...args); }
  getAll(...args: Parameters<Mem0Client["getAll"]>): Promise<unknown> { return this.sdk.getAll(...args); }
  search(...args: Parameters<Mem0Client["search"]>): Promise<unknown> { return this.sdk.search(...args); }
  update(...args: Parameters<Mem0Client["update"]>): Promise<unknown> { return this.sdk.update(...args); }
  delete(...args: Parameters<Mem0Client["delete"]>): Promise<unknown> { return this.sdk.delete(...args); }
  history(...args: Parameters<Mem0Client["history"]>): Promise<unknown> { return this.sdk.history(...args); }
}

export interface ManagedMem0Config {
  readonly vectorStore: { readonly provider: string; readonly config: Record<string, unknown> };
  readonly historyDbPath: string;
  readonly customInstructions?: string;
}

/** Only storage/history/instructions are accepted; no unmetered reranker or graph provider. */
export async function createManagedClient(options: { config: ManagedMem0Config; embeddingDims: number }): Promise<ManagedMem0Client> {
  const { config, embeddingDims } = options;
  if (!Number.isSafeInteger(embeddingDims) || embeddingDims < 1 || embeddingDims > 65_536) throw new TypeError("invalid embeddingDims");
  if (!config || Object.keys(config).some(k => !["vectorStore", "historyDbPath", "customInstructions"].includes(k))) throw new TypeError("managed config accepts only storage/history/instructions");
  if (!config.vectorStore || typeof config.historyDbPath !== "string" || !config.historyDbPath.trim()) throw new TypeError("explicit vectorStore and historyDbPath are required");
  const { Memory } = await import("mem0ai/oss");
  const sdk = new Memory({ ...config,
    llm: { provider: "langchain", config: { model: { async invoke(messages: { type?: string; getType?: () => string; content: unknown }[]) {
      const roles = { human: "user", ai: "assistant", system: "system" } as const;
      const converted = messages.map(message => {
        const role = roles[(message.type ?? message.getType?.()) as keyof typeof roles];
        if (!role || typeof message.content !== "string") throw new MemoryError("memory_provider_contract");
        return { role, content: message.content };
      });
      return { content: await execution().invoke("llm", converted) };
    } } } },
    embedder: { provider: "langchain", config: { embeddingDims, model: {
      async embedQuery(text: string) { return (await execution().invoke("embedding", [text]))[0]!; },
      async embedDocuments(texts: string[]) { return execution().invoke("embedding", texts); },
    } } },
  } as ConstructorParameters<typeof Memory>[0]);
  return new ManagedMem0Client(sdk, embeddingDims);
}
