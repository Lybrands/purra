import OpenAI from "openai";
import type { Stream } from "openai/core/streaming";
import type { ChatCompletionChunk, ChatCompletionCreateParamsNonStreaming, ChatCompletionMessageParam } from "openai/resources/chat/completions";
import type { CompletionUsage } from "openai/resources/completions";
import { AgentCanceledError, AgentError } from "purra";
import type { JsonValue, Message, ModelCapabilitySnapshot, ModelGateway, ModelRequest, ModelStream, ModelStreamItem, ModelTokenUsage, ModelTurn } from "purra";

function text(value: JsonValue): string {
  if (value === null) return "";
  if (typeof value !== "string") throw new TypeError("OpenAI Chat Completions accepts text content only");
  return value;
}
function messages(input: readonly Message[]): ChatCompletionMessageParam[] {
  return input.map(m => {
    if (m.toolCalls?.length && m.role !== "assistant") throw new TypeError("Only assistant messages may carry tool calls");
    const content = text(m.content);
    if (m.role === "tool") {
      if (!m.toolCallId) throw new TypeError("Tool output requires a call id");
      return { role: "tool", content, tool_call_id: m.toolCallId };
    }
    if (m.role === "assistant") return { role: "assistant", content,
      ...(m.toolCalls?.length ? { tool_calls: m.toolCalls.map(c => ({ id: c.id, type: "function" as const,
        function: { name: c.name, arguments: JSON.stringify(c.arguments) } })) } : {}),
    };
    return { role: m.role, content };
  });
}
function usage(u: CompletionUsage | null | undefined): ModelTokenUsage | undefined {
  if (u?.prompt_tokens == null || u.completion_tokens == null) return undefined;
  return { inputTokens: u.prompt_tokens, outputTokens: u.completion_tokens, totalTokens: u.total_tokens,
    cachedInputTokens: u.prompt_tokens_details?.cached_tokens ?? 0,
    reasoningOutputTokens: u.completion_tokens_details?.reasoning_tokens ?? 0 };
}
function finish(reason: string): ModelTurn["finishReason"] {
  if (reason === "stop" || reason === "tool_calls") return reason;
  if (reason === "length") return "length";
  if (reason === "content_filter") return "filtered";
  throw new AgentError("invalid_model_response", "Unknown OpenAI finish reason");
}
function failed(error: unknown, signal?: AbortSignal): never {
  if (signal?.aborted) throw new AgentCanceledError();
  if (error instanceof OpenAI.APIError) throw new AgentError(error.status === undefined ? "openai_transport_error" : `openai_http_${error.status}`, "OpenAI request failed");
  if (error instanceof AgentError) throw error;
  throw new AgentError("invalid_model_response", "Invalid OpenAI Chat Completions response");
}

export interface OpenAIChatCompletionsOptions {
  readonly client?: OpenAI;
  readonly model: string;
  readonly capabilities: ModelCapabilitySnapshot;
  readonly timeoutMs?: number;
  readonly reasoningEffort?: ChatCompletionCreateParamsNonStreaming["reasoning_effort"];
  readonly temperature?: number;
  readonly topP?: number;
}

export class OpenAIChatCompletionsGateway implements ModelGateway {
  readonly capabilities: ModelCapabilitySnapshot;
  readonly #client: OpenAI;
  readonly #options: OpenAIChatCompletionsOptions;
  constructor(options: OpenAIChatCompletionsOptions) {
    if (!options.model.trim()) throw new TypeError("Model name is required");
    if (!Number.isFinite(options.timeoutMs ?? 60000) || (options.timeoutMs ?? 60000) <= 0) throw new TypeError("Timeout must be positive and finite");
    this.capabilities = options.capabilities;
    this.#options = { ...options };
    this.#client = (options.client ?? new OpenAI()).withOptions({ maxRetries: 0, timeout: options.timeoutMs ?? 60000 });
  }
  #request(request: ModelRequest): ChatCompletionCreateParamsNonStreaming {
    if (!request.outputLimit) throw new TypeError("OpenAI gateway requires a resolved output limit");
    const o = this.#options;
    return { model: o.model, messages: messages(request.messages), store: false, max_completion_tokens: request.outputLimit.maxTokens,
      ...(o.reasoningEffort === undefined ? {} : { reasoning_effort: o.reasoningEffort }),
      ...(o.temperature === undefined ? {} : { temperature: o.temperature }),
      ...(o.topP === undefined ? {} : { top_p: o.topP }),
      ...(request.tools.length ? { tools: request.tools.map(t => ({ type: "function" as const,
        function: { name: t.name, description: t.description, parameters: JSON.parse(JSON.stringify(t.inputSchema)) as Record<string, unknown>, strict: false } })), tool_choice: "auto" as const } : {}),
    };
  }
  async invoke(request: ModelRequest, signal?: AbortSignal): Promise<ModelTurn> {
    const params = this.#request(request);
    if (signal?.aborted) throw new AgentCanceledError();
    try {
      const response = await this.#client.chat.completions.create(params, { signal });
      const c = response.choices[0];
      if (response.choices.length !== 1 || c?.index !== 0 || c.message.role !== "assistant") throw new Error("Invalid completion");
      const tokenUsage = usage(response.usage);
      return { message: { role: "assistant", content: text(c.message.content), toolCalls: (c.message.tool_calls ?? []).map(call => {
        if (call.type !== "function") throw new Error("Unsupported tool type");
        return { id: call.id, name: call.function.name, arguments: JSON.parse(call.function.arguments) as JsonValue };
      }) }, finishReason: c.message.refusal ? "filtered" : finish(c.finish_reason),
      appliedOutputLimit: request.outputLimit!.maxTokens, ...(tokenUsage === undefined ? {} : { usage: tokenUsage }) };
    } catch (error) { return failed(error, signal); }
  }
  async stream(request: ModelRequest, signal?: AbortSignal): Promise<ModelStream> {
    const params = this.#request(request);
    const client = this.#client;
    let consumed = false;
    async function* chunks(): AsyncGenerator<ModelStreamItem> {
      if (consumed) throw new AgentError("invalid_model_response", "OpenAI stream has already been consumed");
      consumed = true;
      if (signal?.aborted) throw new AgentCanceledError();
      let stream: Stream<ChatCompletionChunk> | undefined;
      let terminal: ModelTurn["finishReason"] | undefined;
      let tokenUsage: ModelTokenUsage | undefined;
      let refused = false;
      try {
        stream = await client.chat.completions.create({ ...params, stream: true, stream_options: { include_usage: true } }, { signal });
        for await (const chunk of stream) {
          if (chunk.usage != null) tokenUsage = usage(chunk.usage);
          if (!chunk.choices.length) { yield { type: "activity", kind: "transport" }; continue; }
          const c = chunk.choices[0];
          if (chunk.choices.length !== 1 || c?.index !== 0 || terminal !== undefined) throw new Error("Invalid completion sequence");
          refused ||= Boolean(c.delta.refusal);
          const content = text(c.delta.content ?? null);
          const calls = (c.delta.tool_calls ?? []).map(call => ({ index: call.index,
            ...(call.id === undefined ? {} : { id: call.id }), ...(call.type === undefined ? {} : { type: call.type }),
            ...(call.function?.name === undefined ? {} : { name: call.function.name }), argumentsFragment: call.function?.arguments ?? "" }));
          if (content || calls.length) yield { contentDelta: content, toolCallDeltas: calls };
          else yield { type: "activity", kind: "transport" };
          if (c.finish_reason !== null) terminal = finish(c.finish_reason);
        }
        if (terminal === undefined) throw new AgentError("upstream_stream_interrupted", "OpenAI stream ended without a finish reason");
        yield { finishReason: refused ? "filtered" : terminal, ...(tokenUsage === undefined ? {} : { usage: tokenUsage }) };
      } catch (error) { failed(error, signal); }
      finally { stream?.controller.abort(); }
    }
    return { appliedOutputLimit: request.outputLimit!.maxTokens, activitySupport: "transport", [Symbol.asyncIterator]: chunks };
  }
}
