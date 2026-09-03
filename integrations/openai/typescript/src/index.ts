import OpenAI from "openai";
export { OpenAIChatCompletionsGateway } from "./chat.js";
export type { OpenAIChatCompletionsOptions } from "./chat.js";
import type { Stream } from "openai/core/streaming";
import type { Response, ResponseCreateParamsNonStreaming, ResponseInput, ResponseStreamEvent } from "openai/resources/responses/responses";
import { AgentCanceledError, AgentError } from "purra";
import type { JsonValue, Message, ModelCapabilitySnapshot, ModelGateway, ModelRequest, ModelStream, ModelStreamItem, ModelTokenUsage, ModelTurn } from "purra";

const replayKey = "openai_reasoning_items";
function text(value: JsonValue): string {
  if (value === null) return "";
  if (typeof value !== "string") throw new TypeError("OpenAI adapter currently accepts text messages only");
  return value;
}
function input(messages: readonly Message[]): ResponseInput {
  const rows: ResponseInput = [];
  for (const m of messages) {
    if (m.toolCalls?.length && m.role !== "assistant") throw new TypeError("Only assistant messages may carry function calls");
    if (m.role === "assistant") {
      const replay = m.providerData?.[replayKey] ?? [];
      if (!Array.isArray(replay) || replay.some(r => !r || typeof r !== "object" || Array.isArray(r) || r.type !== "reasoning")) throw new TypeError("Invalid OpenAI reasoning continuation");
      rows.push(...JSON.parse(JSON.stringify(replay)) as ResponseInput);
    }
    const content = text(m.content);
    if (m.role === "tool") {
      if (!m.toolCallId) throw new TypeError("Tool output requires a call id");
      rows.push({ type: "function_call_output", call_id: m.toolCallId, output: content });
      continue;
    }
    if (content || !m.toolCalls?.length) rows.push({ role: m.role, content });
    for (const call of m.toolCalls ?? []) rows.push({ type: "function_call", call_id: call.id, name: call.name, arguments: JSON.stringify(call.arguments) });
  }
  return rows;
}
function usage(response: Response): ModelTokenUsage | undefined {
  const u = response.usage;
  return u == null ? undefined : { inputTokens: u.input_tokens, outputTokens: u.output_tokens, totalTokens: u.total_tokens,
    cachedInputTokens: u.input_tokens_details.cached_tokens, reasoningOutputTokens: u.output_tokens_details.reasoning_tokens };
}
function finish(response: Response): ModelTurn["finishReason"] {
  if (response.status === "incomplete") return response.incomplete_details?.reason === "max_output_tokens" ? "length" : response.incomplete_details?.reason === "content_filter" ? "filtered" : "other";
  if (response.status !== "completed") throw new AgentError("openai_response_failed", "OpenAI response failed");
  if (response.output.some(i => i.type === "function_call")) return "tool_calls";
  if (response.output.some(i => i.type === "message" && i.content.some(p => p.type === "refusal"))) return "filtered";
  return "stop";
}
function attributes(response: Response): Readonly<Record<string, JsonValue>> {
  const items = response.output.filter(i => i.type === "reasoning");
  return items.length ? { [replayKey]: JSON.parse(JSON.stringify(items)) as JsonValue } : {};
}
function failed(error: unknown, signal?: AbortSignal): never {
  if (signal?.aborted) throw new AgentCanceledError();
  if (error instanceof OpenAI.APIError) throw new AgentError(error.status === undefined ? "openai_transport_error" : `openai_http_${error.status}`, "OpenAI request failed");
  throw error;
}

export interface OpenAIResponsesOptions {
  readonly client?: OpenAI;
  readonly model: string;
  /** Host-selected model facts; the adapter never guesses model token limits. */
  readonly capabilities: ModelCapabilitySnapshot;
  readonly timeoutMs?: number;
  readonly reasoning?: ResponseCreateParamsNonStreaming["reasoning"];
}

/** Official SDK transport with one HTTP attempt per managed invocation. */
export class OpenAIResponsesGateway implements ModelGateway {
  readonly capabilities: ModelCapabilitySnapshot;
  readonly #client: OpenAI;
  readonly #model: string;
  readonly #reasoning: OpenAIResponsesOptions["reasoning"];
  constructor(options: OpenAIResponsesOptions) {
    if (!options.model.trim()) throw new TypeError("Model name is required");
    if (!Number.isFinite(options.timeoutMs ?? 60000) || (options.timeoutMs ?? 60000) <= 0) throw new TypeError("Timeout must be positive");
    this.capabilities = options.capabilities;
    this.#model = options.model;
    this.#reasoning = options.reasoning;
    this.#client = (options.client ?? new OpenAI()).withOptions({ maxRetries: 0, timeout: options.timeoutMs ?? 60000 });
  }
  #request(request: ModelRequest): ResponseCreateParamsNonStreaming {
    if (!request.outputLimit) throw new TypeError("OpenAI gateway requires a resolved output limit");
    return { model: this.#model, input: input(request.messages), store: false, include: ["reasoning.encrypted_content"],
      max_output_tokens: request.outputLimit.maxTokens,
      tools: request.tools.map(t => ({ type: "function", name: t.name, description: t.description,
        parameters: JSON.parse(JSON.stringify(t.inputSchema)) as Record<string, unknown>, strict: false })),
      tool_choice: request.tools.length ? "auto" : "none",
      ...(this.#reasoning === undefined ? {} : { reasoning: this.#reasoning }),
    };
  }
  async invoke(request: ModelRequest, signal?: AbortSignal): Promise<ModelTurn> {
    const params = this.#request(request);
    try {
      const response = await this.#client.responses.create(params, { signal });
      const tokenUsage = usage(response);
      return { message: { role: "assistant", content: response.output_text, providerData: attributes(response),
        toolCalls: response.output.filter(i => i.type === "function_call").map(i => ({ id: i.call_id, name: i.name, arguments: JSON.parse(i.arguments) as JsonValue })) },
        finishReason: finish(response), appliedOutputLimit: request.outputLimit!.maxTokens,
        ...(tokenUsage === undefined ? {} : { usage: tokenUsage }),
      };
    } catch (error) { return failed(error, signal); }
  }
  async stream(request: ModelRequest, signal?: AbortSignal): Promise<ModelStream> {
    const params = this.#request(request);
    const client = this.#client;
    let consumed = false;
    async function* chunks(): AsyncGenerator<ModelStreamItem> {
      if (consumed) throw new AgentError("invalid_model_response", "OpenAI stream has already been consumed");
      consumed = true;
      let stream: Stream<ResponseStreamEvent> | undefined;
      let terminal = false;
      try {
        stream = await client.responses.create({ ...params, stream: true }, { signal });
        for await (const event of stream) {
          const item = project(event);
          if (item) yield item;
          if (event.type === "response.completed" || event.type === "response.incomplete" || event.type === "response.failed") { terminal = true; break; }
        }
        if (!terminal) throw new AgentError("upstream_stream_interrupted", "OpenAI stream ended without a terminal response");
      } catch (error) { failed(error, signal); }
      finally { stream?.controller.abort(); }
    }
    return { appliedOutputLimit: request.outputLimit!.maxTokens, activitySupport: "working", [Symbol.asyncIterator]: chunks };
  }
}

function project(event: ResponseStreamEvent): ModelStreamItem | undefined {
  if (event.type === "response.output_text.delta") return { contentDelta: event.delta };
  if (event.type === "response.reasoning_text.delta" || event.type === "response.reasoning_summary_text.delta") return { type: "activity", kind: "working" };
  if (event.type === "response.output_item.added" && event.item.type === "function_call") return {
    toolCallDeltas: [{ index: event.output_index, id: event.item.call_id, name: event.item.name, type: "function", argumentsFragment: event.item.arguments }],
  };
  if (event.type === "response.function_call_arguments.delta") return { toolCallDeltas: [{ index: event.output_index, argumentsFragment: event.delta }] };
  if (event.type === "response.completed" || event.type === "response.incomplete" || event.type === "response.failed") {
    const tokenUsage = usage(event.response);
    return { finishReason: finish(event.response), providerData: attributes(event.response), ...(tokenUsage === undefined ? {} : { usage: tokenUsage }) };
  }
  if (event.type === "error") throw new AgentError("openai_stream_error", "OpenAI stream failed");
  return { type: "activity", kind: "transport" };
}
