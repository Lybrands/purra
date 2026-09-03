import Anthropic from "@anthropic-ai/sdk";
import type { MessageStream } from "@anthropic-ai/sdk/lib/MessageStream";
import type { ContentBlock, ContentBlockParam, Message as AnthropicMessage, MessageCreateParamsNonStreaming, MessageParam, TextBlockParam } from "@anthropic-ai/sdk/resources/messages";
import { AgentCanceledError, AgentError } from "purra";
import type { JsonValue, Message, ModelCapabilitySnapshot, ModelGateway, ModelRequest, ModelStream, ModelStreamItem, ModelTokenUsage, ModelTurn, ToolCall } from "purra";

const replayKey = "anthropic_message";
const maxContinuationChars = 1_000_000;
function copy<T>(value: T): T { return JSON.parse(JSON.stringify(value)) as T; }
function text(value: unknown): string {
  if (value === null) return "";
  if (typeof value !== "string") throw new TypeError("Anthropic adapter accepts text messages only");
  return value;
}
function projection(blocks: readonly ContentBlock[]): { content: string; toolCalls: ToolCall[] } {
  let content = "";
  const toolCalls: ToolCall[] = [];
  for (const block of blocks) {
    if (block.type === "text") content += text(block.text);
    else if (block.type === "tool_use") {
      if (!block.input || typeof block.input !== "object" || Array.isArray(block.input)) throw new TypeError("Tool input must be an object");
      toolCalls.push({ id: block.id, name: block.name, arguments: copy(block.input) as JsonValue });
    } else if (block.type === "thinking") {
      text(block.thinking);
      if (typeof block.signature !== "string" || !block.signature) throw new TypeError("Thinking continuation requires a signature");
    } else if (block.type === "redacted_thinking") {
      if (typeof block.data !== "string" || !block.data) throw new TypeError("Redacted thinking requires opaque data");
    } else throw new TypeError("Unsupported Anthropic content block");
  }
  return { content, toolCalls };
}
function input(messages: readonly Message[], model: string): { messages: MessageParam[]; system: TextBlockParam[] } {
  const system: TextBlockParam[] = [];
  const rows: { role: "user" | "assistant"; content: ContentBlockParam[] }[] = [];
  for (const m of messages) {
    const content = text(m.content);
    if (m.toolCalls?.length && m.role !== "assistant") throw new TypeError("Only assistant messages may carry tool calls");
    if (m.role === "system" || m.role === "developer") {
      if (content) system.push({ type: "text", text: content });
      continue;
    }
    let blocks: ContentBlockParam[];
    const role = m.role === "assistant" ? "assistant" : "user";
    if (m.role === "tool") {
      if (!m.toolCallId) throw new TypeError("Tool output requires a call id");
      blocks = [{ type: "tool_result", tool_use_id: m.toolCallId, content }];
    } else if (m.role === "assistant") {
      const replay = m.providerData?.[replayKey];
      if (replay != null) {
        if (typeof replay !== "object" || Array.isArray(replay) || !("model" in replay) || !("content" in replay) || replay.model !== model || !Array.isArray(replay.content)) throw new TypeError("Anthropic continuation belongs to a different model");
        if (JSON.stringify(replay.content).length > maxContinuationChars) throw new TypeError("Anthropic continuation exceeds limit");
        const original = copy(replay.content) as unknown as ContentBlock[];
        const visible = projection(original);
        if (visible.content !== content || JSON.stringify(visible.toolCalls) !== JSON.stringify(m.toolCalls ?? [])) throw new TypeError("Anthropic continuation does not match assistant message");
        blocks = original as ContentBlockParam[];
      } else {
        blocks = content ? [{ type: "text", text: content }] : [];
        for (const call of m.toolCalls ?? []) {
          if (!call.arguments || typeof call.arguments !== "object" || Array.isArray(call.arguments)) throw new TypeError("Tool input must be an object");
          blocks.push({ type: "tool_use", id: call.id, name: call.name, input: copy(call.arguments) });
        }
      }
    } else blocks = [{ type: "text", text: content }];
    if (!blocks.length) throw new TypeError("Anthropic message cannot be empty");
    const previous = rows.at(-1);
    if (previous?.role === role) previous.content.push(...blocks);
    else rows.push({ role, content: blocks });
  }
  return { messages: rows, system };
}
function attributes(content: ContentBlock[], model: string): Readonly<Record<string, JsonValue>> {
  if (!content.some(b => b.type === "thinking" || b.type === "redacted_thinking")) return {};
  if (JSON.stringify(content).length > maxContinuationChars) throw new TypeError("Anthropic continuation exceeds limit");
  return { [replayKey]: { model, content: copy(content) as unknown as JsonValue } };
}
function usage(u: AnthropicMessage["usage"] | undefined): ModelTokenUsage | undefined {
  if (u?.input_tokens == null || u.output_tokens == null) return undefined;
  const cached = u.cache_read_input_tokens ?? 0;
  const inputs = u.input_tokens + cached + (u.cache_creation_input_tokens ?? 0);
  return { inputTokens: inputs, outputTokens: u.output_tokens, totalTokens: inputs + u.output_tokens, cachedInputTokens: cached };
}
function finish(reason: AnthropicMessage["stop_reason"]): ModelTurn["finishReason"] {
  if (reason === "end_turn" || reason === "stop_sequence") return "stop";
  if (reason === "tool_use") return "tool_calls";
  if (reason === "max_tokens" || reason === "model_context_window_exceeded") return "length";
  if (reason === "refusal") return "filtered";
  throw new AgentError("invalid_model_response", "Unsupported Anthropic stop reason");
}
function failed(error: unknown, signal?: AbortSignal): never {
  if (signal?.aborted) throw new AgentCanceledError();
  if (error instanceof Anthropic.APIError) throw new AgentError(error.status === undefined ? "anthropic_transport_error" : `anthropic_http_${error.status}`, "Anthropic request failed");
  if (error instanceof AgentError) throw error;
  throw new AgentError("invalid_model_response", "Invalid Anthropic response");
}

export interface AnthropicMessagesOptions {
  readonly client?: Anthropic;
  readonly model: string;
  readonly capabilities: ModelCapabilitySnapshot;
  readonly timeoutMs?: number;
  readonly thinking?: MessageCreateParamsNonStreaming["thinking"];
  readonly outputConfig?: MessageCreateParamsNonStreaming["output_config"];
  readonly temperature?: number;
  readonly topP?: number;
  readonly topK?: number;
}

/** Official SDK transport; Core owns execution, retries and cumulative budgets. */
export class AnthropicMessagesGateway implements ModelGateway {
  readonly capabilities: ModelCapabilitySnapshot;
  readonly #client: Anthropic;
  readonly #options: AnthropicMessagesOptions;
  constructor(options: AnthropicMessagesOptions) {
    if (!options.model.trim()) throw new TypeError("Model name is required");
    if (!Number.isFinite(options.timeoutMs ?? 60000) || (options.timeoutMs ?? 60000) <= 0) throw new TypeError("Timeout must be positive and finite");
    this.capabilities = options.capabilities;
    this.#options = { ...options, ...(options.thinking === undefined ? {} : { thinking: copy(options.thinking) }),
      ...(options.outputConfig === undefined ? {} : { outputConfig: copy(options.outputConfig) }) };
    this.#client = (options.client ?? new Anthropic()).withOptions({ maxRetries: 0, timeout: options.timeoutMs ?? 60000 });
  }
  #request(request: ModelRequest): MessageCreateParamsNonStreaming {
    if (!request.outputLimit) throw new TypeError("Anthropic gateway requires a resolved output limit");
    const o = this.#options;
    if (o.thinking?.type === "enabled" && (!Number.isInteger(o.thinking.budget_tokens) || o.thinking.budget_tokens < 1024 || o.thinking.budget_tokens >= request.outputLimit.maxTokens)) throw new TypeError("Thinking budget must be at least 1024 and below the output limit");
    const rows = input(request.messages, o.model);
    return { model: o.model, messages: rows.messages, max_tokens: request.outputLimit.maxTokens,
      ...(rows.system.length ? { system: rows.system } : {}),
      ...(o.thinking === undefined ? {} : { thinking: o.thinking }),
      ...(o.outputConfig === undefined ? {} : { output_config: o.outputConfig }),
      ...(o.temperature === undefined ? {} : { temperature: o.temperature }),
      ...(o.topP === undefined ? {} : { top_p: o.topP }),
      ...(o.topK === undefined ? {} : { top_k: o.topK }),
      ...(request.tools.length ? { tools: request.tools.map(t => {
        if (t.inputSchema.type !== "object") throw new TypeError("Tool schema must describe an object");
        return { name: t.name, description: t.description, input_schema: copy(t.inputSchema) as { type: "object" } };
      }), tool_choice: { type: "auto" as const } } : {}),
    };
  }
  async invoke(request: ModelRequest, signal?: AbortSignal): Promise<ModelTurn> {
    const params = this.#request(request);
    if (signal?.aborted) throw new AgentCanceledError();
    try {
      const response = await this.#client.messages.create(params, { signal });
      const tokenUsage = usage(response.usage);
      return { message: { role: "assistant", ...projection(response.content), providerData: attributes(response.content, params.model) },
        finishReason: finish(response.stop_reason), appliedOutputLimit: request.outputLimit!.maxTokens,
        ...(tokenUsage === undefined ? {} : { usage: tokenUsage }) };
    } catch (error) { return failed(error, signal); }
  }
  async stream(request: ModelRequest, signal?: AbortSignal): Promise<ModelStream> {
    const params = this.#request(request);
    const client = this.#client;
    let consumed = false;
    async function* chunks(): AsyncGenerator<ModelStreamItem> {
      if (consumed) throw new AgentError("invalid_model_response", "Anthropic stream has already been consumed");
      consumed = true;
      if (signal?.aborted) throw new AgentCanceledError();
      let stream: MessageStream | undefined;
      let terminal = false;
      let privateChars = 0;
      const emptyTools = new Set<number>();
      try {
        stream = client.messages.stream(params, { signal });
        for await (const event of stream) {
          if (event.type === "content_block_start") {
            const b = event.content_block;
            if (b.type === "tool_use") {
              emptyTools.add(event.index);
              yield { toolCallDeltas: [{ index: event.index, id: b.id, name: b.name, type: "function" }] };
            } else if (b.type === "text" && b.text) yield { contentDelta: b.text };
            else if (b.type === "thinking" || b.type === "redacted_thinking") {
              privateChars += JSON.stringify(b).length;
              yield { type: "activity", kind: "working" };
            } else if (b.type !== "text") throw new TypeError("Unsupported Anthropic content block");
          } else if (event.type === "content_block_delta") {
            const d = event.delta;
            if (d.type === "text_delta") yield { contentDelta: d.text };
            else if (d.type === "input_json_delta") {
              emptyTools.delete(event.index);
              yield { toolCallDeltas: [{ index: event.index, argumentsFragment: d.partial_json }] };
            } else if (d.type === "thinking_delta" || d.type === "signature_delta") {
              privateChars += d.type === "thinking_delta" ? d.thinking.length : d.signature.length;
              yield { type: "activity", kind: "working" };
            } else throw new TypeError("Unsupported Anthropic content delta");
          } else if (event.type === "content_block_stop" && emptyTools.has(event.index)) {
            emptyTools.delete(event.index);
            yield { toolCallDeltas: [{ index: event.index, argumentsFragment: "{}" }] };
          } else if (event.type === "message_stop") terminal = true;
          else yield { type: "activity", kind: "transport" };
          if (privateChars > maxContinuationChars) throw new TypeError("Anthropic continuation exceeds limit");
        }
        if (!terminal) throw new AgentError("upstream_stream_interrupted", "Anthropic stream ended without message_stop");
        const response = await stream.finalMessage();
        projection(response.content);
        const tokenUsage = usage(response.usage);
        yield { finishReason: finish(response.stop_reason), providerData: attributes(response.content, params.model),
          ...(tokenUsage === undefined ? {} : { usage: tokenUsage }) };
      } catch (error) { failed(error, signal); }
      finally { stream?.abort(); }
    }
    return { appliedOutputLimit: request.outputLimit!.maxTokens, activitySupport: "working", [Symbol.asyncIterator]: chunks };
  }
}
