import { AgentCanceledError, AgentError } from "../shared/errors.js";
import type {
  Message,
  ModelGateway,
  ModelRequest,
  ModelStreamChunk,
  ModelTokenUsage,
  ModelTurn,
  ToolCall,
  ToolCallDelta,
} from "./types.js";
import {
  copyJsonValue,
  throwForIncompleteFinish,
  validateModelStreamChunk,
  validateModelTurn,
} from "./validation.js";

interface ToolCallParts {
  id?: string;
  name?: string;
  arguments: string[];
  argumentChars: number;
}

export interface ModelStreamLimits {
  readonly invocationTimeoutMs: number | null;
  readonly maxChunks: number;
  readonly maxContentChars: number;
  readonly maxReasoningChars: number;
  readonly maxToolArgumentChars: number;
}

const DEFAULT_STREAM_LIMITS: ModelStreamLimits = Object.freeze({
  invocationTimeoutMs: 120_000,
  maxChunks: 100_000,
  maxContentChars: 1_000_000,
  maxReasoningChars: 1_000_000,
  maxToolArgumentChars: 1_000_000,
});

export async function invokeModel(
  gateway: ModelGateway,
  request: ModelRequest,
  signal: AbortSignal | undefined,
  useStream: boolean,
  onChunk?: (chunk: ModelStreamChunk) => Promise<void> | void,
  limits: ModelStreamLimits = DEFAULT_STREAM_LIMITS,
): Promise<ModelTurn> {
  const stop = invocationSignal(signal, limits.invocationTimeoutMs);
  try {
    throwIfCanceled(stop.signal);
    if (!useStream) {
      const turn = validateModelTurn(await awaitWithSignal(
        gateway.invoke(request, stop.signal),
        stop.signal,
      ));
      throwIfCanceled(stop.signal);
      return turn;
    }
    const stream = await awaitWithSignal(
      Promise.resolve(gateway.stream!(request, stop.signal)),
      stop.signal,
    );
    throwIfCanceled(stop.signal);
    return await consumeModelStream(stream, stop.signal, onChunk, limits);
  } catch (error) {
    if (stop.signal.aborted) {
      if (stop.signal.reason instanceof AgentError) throw stop.signal.reason;
      throw new AgentCanceledError({ cause: error });
    }
    if (error instanceof AgentError) throw error;
    throw new AgentError(
      useStream ? "model_stream_error" : "model_gateway_error",
      useStream ? "Model stream failed" : "Model gateway failed",
      { cause: error },
    );
  } finally {
    stop.close();
  }
}

async function consumeModelStream(
  stream: AsyncIterable<ModelStreamChunk>,
  signal: AbortSignal | undefined,
  onChunk: ((chunk: ModelStreamChunk) => Promise<void> | void) | undefined,
  limits: ModelStreamLimits,
): Promise<ModelTurn> {
  if (stream === null || typeof stream?.[Symbol.asyncIterator] !== "function") {
    throw new AgentError("invalid_model_response", "Model gateway returned an invalid stream");
  }
  const iterator = stream[Symbol.asyncIterator]();
  const content: string[] = [];
  const reasoning: string[] = [];
  let contentChars = 0;
  let reasoningChars = 0;
  let chunkCount = 0;
  let finishReason: ModelStreamChunk["finishReason"];
  let usage: ModelTokenUsage | undefined;
  let malformed: string | undefined;
  const calls = new Map<number, ToolCallParts>();

  try {
    while (finishReason === undefined) {
      const step = await nextWithSignal(iterator, signal);
      if (step.done === true) break;
      throwIfCanceled(signal);
      const chunk = validateModelStreamChunk(step.value);
      chunkCount += 1;
      contentChars += chunk.contentDelta?.length ?? 0;
      reasoningChars += chunk.reasoningDelta?.length ?? 0;
      requireStreamLimit(chunkCount, limits.maxChunks, "chunk_count");
      requireStreamLimit(contentChars, limits.maxContentChars, "content_chars");
      requireStreamLimit(reasoningChars, limits.maxReasoningChars, "reasoning_chars");
      for (const delta of chunk.toolCallDeltas ?? []) {
        const current = calls.get(delta.index) ?? { arguments: [], argumentChars: 0 };
        calls.set(delta.index, current);
        current.argumentChars += delta.argumentsFragment?.length ?? 0;
        requireStreamLimit(
          current.argumentChars,
          limits.maxToolArgumentChars,
          "tool_argument_chars",
        );
      }
      await onChunk?.(chunk);
      if (chunk.contentDelta !== undefined && chunk.contentDelta !== "") content.push(chunk.contentDelta);
      if (chunk.reasoningDelta !== undefined && chunk.reasoningDelta !== "") reasoning.push(chunk.reasoningDelta);
      usage = chunk.usage ?? usage;
      finishReason = chunk.finishReason;
      for (const delta of chunk.toolCallDeltas ?? []) {
        malformed = mergeToolCallDelta(calls, delta) ?? malformed;
      }
    }
  } finally {
    await closeIterator(iterator, signal?.aborted === true);
  }

  if (finishReason === undefined) {
    throw new AgentError(
      "upstream_stream_interrupted",
      "Model stream ended without a finish reason",
    );
  }
  throwForIncompleteFinish(finishReason, calls.size);
  const toolCalls = buildToolCalls(calls, malformed);
  const contentText = content.join("");
  const reasoningText = reasoning.join("");
  const message: Message = {
    role: "assistant",
    content: contentText,
    ...(reasoningText.trim() === "" ? {} : { reasoning: reasoningText }),
    ...(toolCalls.length === 0 ? {} : { toolCalls }),
  };
  return validateModelTurn({
    message,
    finishReason,
    ...(usage === undefined ? {} : { usage }),
  });
}

function mergeToolCallDelta(
  calls: Map<number, ToolCallParts>,
  delta: ToolCallDelta,
): string | undefined {
  const current = calls.get(delta.index) ?? { arguments: [], argumentChars: 0 };
  calls.set(delta.index, current);
  if (delta.id !== undefined) {
    if (current.id !== undefined && current.id !== delta.id) {
      return "conflicting_tool_call_id_for_index";
    }
    current.id = delta.id;
  }
  if (delta.name !== undefined) {
    if (current.name !== undefined && current.name !== delta.name) {
      return "conflicting_tool_name_for_index";
    }
    current.name = delta.name;
  }
  if (delta.argumentsFragment !== undefined && delta.argumentsFragment !== "") {
    current.arguments.push(delta.argumentsFragment);
  }
  return undefined;
}

function buildToolCalls(
  partsByIndex: ReadonlyMap<number, ToolCallParts>,
  malformed: string | undefined,
): readonly ToolCall[] {
  const rows = [...partsByIndex.entries()].sort(([left], [right]) => left - right);
  if (malformed !== undefined) throw malformedToolBatch(malformed);
  if (rows.some(([, parts]) => parts.id === undefined)) {
    throw malformedToolBatch("missing_tool_call_id");
  }
  if (rows.some(([, parts]) => parts.name === undefined)) {
    throw malformedToolBatch("missing_tool_call_name");
  }
  const ids = rows.map(([, parts]) => parts.id!);
  if (new Set(ids).size !== ids.length) throw malformedToolBatch("duplicate_tool_call_id");
  return Object.freeze(rows.map(([, parts]) => {
    let parsed: unknown;
    try {
      parsed = JSON.parse(parts.arguments.join(""));
    } catch (error) {
      throw malformedToolBatch("invalid_tool_arguments_json", error);
    }
    return Object.freeze({
      id: parts.id!,
      name: parts.name!,
      arguments: copyJsonValue(parsed),
    });
  }));
}

function malformedToolBatch(reason: string, cause?: unknown): AgentError {
  return new AgentError(
    "malformed_tool_call_batch",
    `Model returned a malformed tool-call batch: ${reason}`,
    cause === undefined ? undefined : { cause },
  );
}

async function nextWithSignal<T>(
  iterator: AsyncIterator<T>,
  signal: AbortSignal | undefined,
): Promise<IteratorResult<T>> {
  return awaitWithSignal(iterator.next(), signal);
}

async function awaitWithSignal<T>(promise: Promise<T>, signal: AbortSignal | undefined): Promise<T> {
  if (signal === undefined) return promise;
  throwIfCanceled(signal);
  let rejectCanceled: (() => void) | undefined;
  const canceled = new Promise<never>((_resolve, reject) => {
    rejectCanceled = () => reject(new AgentCanceledError());
    signal.addEventListener("abort", rejectCanceled, { once: true });
  });
  try {
    return await Promise.race([promise, canceled]);
  } finally {
    if (rejectCanceled !== undefined) signal.removeEventListener("abort", rejectCanceled);
  }
}

async function closeIterator<T>(iterator: AsyncIterator<T>, canceled: boolean): Promise<void> {
  if (typeof iterator.return !== "function") return;
  try {
    const closing = Promise.resolve(iterator.return());
    if (canceled) {
      void closing.catch(() => undefined);
      return;
    }
    await closing;
  } catch {
    // Cleanup must not replace the selected public outcome.
  }
}

function throwIfCanceled(signal: AbortSignal | undefined): void {
  if (signal?.aborted !== true) return;
  if (signal.reason instanceof AgentError) throw signal.reason;
  throw new AgentCanceledError();
}

function requireStreamLimit(value: number, limit: number, kind: string): void {
  if (!Number.isSafeInteger(limit) || limit < 1) throw new TypeError(`${kind} limit must be positive`);
  if (value > limit) {
    throw new AgentError(
      "model_stream_limit_exceeded",
      `Model stream exceeded the ${kind} limit`,
    );
  }
}

function invocationSignal(
  parent: AbortSignal | undefined,
  timeoutMs: number | null,
): { readonly signal: AbortSignal; close(): void } {
  if (timeoutMs !== null && (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1)) {
    throw new TypeError("invocationTimeoutMs must be positive or null");
  }
  const controller = new AbortController();
  const forward = (): void => controller.abort(parent?.reason);
  parent?.addEventListener("abort", forward, { once: true });
  if (parent?.aborted === true) forward();
  const timer = timeoutMs === null ? undefined : globalThis.setTimeout(() => {
    controller.abort(new AgentError(
      "model_invocation_deadline_exceeded",
      "Model invocation deadline has elapsed",
    ));
  }, timeoutMs);
  return {
    signal: controller.signal,
    close(): void {
      if (timer !== undefined) globalThis.clearTimeout(timer);
      parent?.removeEventListener("abort", forward);
    },
  };
}
