/** Mem0's native protocol; every call uses the currently bound PurrA operation. */
import type { Message } from "purra";
import { currentExecution, type ProviderExecution } from "./providers.js";
import { MemoryError } from "./journal.js";

function execution(): ProviderExecution {
  const value = currentExecution();
  if (!value) throw new MemoryError("memory_provider_unbound");
  value.check();
  return value;
}
function reject(value: ProviderExecution): never {
  value.stop("memory_provider_contract");
  throw new MemoryError(value.error!);
}

export class DirectLlm {
  async generateResponse(messages: unknown, format?: unknown, tools?: unknown, ...extra: unknown[]): Promise<string> {
    const value = execution();
    if (extra.length || (tools !== undefined && (!Array.isArray(tools) || tools.length))
      || (format !== undefined && (format === null || typeof format !== "object"
        || Object.keys(format).length !== 1 || (format as { type?: unknown }).type !== "json_object"))
      || !Array.isArray(messages) || !messages.length) reject(value);
    const converted: Message[] = Array.from(messages, (message: unknown) => {
      if (!message || typeof message !== "object" || Object.keys(message).sort().join() !== "content,role") reject(value);
      const m = message as { role: string; content: unknown };
      if (!["system", "user", "assistant"].includes(m.role) || typeof m.content !== "string") reject(value);
      return { role: m.role as "system" | "user" | "assistant", content: m.content };
    });
    return value.invoke("llm", converted);
  }
  async generateChat(): Promise<never> { return reject(execution()); }
}

export class DirectEmbedder {
  async embed(text: unknown, action?: unknown): Promise<number[]> { return (await this.embedBatch([text], action))[0]!; }
  async embedBatch(texts: unknown, action?: unknown): Promise<number[][]> {
    const value = execution();
    if ((action !== undefined && !["add", "search", "update"].includes(action as string))
      || !Array.isArray(texts) || Array.from(texts).some(text => typeof text !== "string")) reject(value);
    return texts.length ? value.invoke("embedding", [...texts]) : [];
  }
}
