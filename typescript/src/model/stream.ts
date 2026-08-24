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
  arguments: string;
}

export async function invokeModel(
  gateway: ModelGateway,
  request: ModelRequest,
  signal: AbortSignal | undefined,
  useStream: boolean,
  onChunk?: (chunk: ModelStreamChunk) => Promise<void> | void,
): Promise<ModelTurn> {
  try {
    throwIfCanceled(signal);
    if (!useStream) {
      return validateModelTurn(await awaitWithSignal(gateway.invoke(request, signal), signal));
    }
    const stream = await awaitWithSignal(
      Promise.resolve(gateway.stream!(request, signal)),
      signal,
    );
    return await consumeModelStream(stream, signal, onChunk);
  } catch (error) {
    if (signal?.aborted === true) throw new AgentCanceledError({ cause: error });
    if (error instanceof AgentError) throw error;
    throw new AgentError(
      useStream ? "model_stream_error" : "model_gateway_error",
      useStream ? "Model stream failed" : "Model gateway failed",
      { cause: error },
    );
  }
}

async function consumeModelStream(
  stream: AsyncIterable<ModelStreamChunk>,
  signal: AbortSignal | undefined,
  onChunk: ((chunk: ModelStreamChunk) => Promise<void> | void) | undefined,
): Promise<ModelTurn> {
  if (stream === null || typeof stream?.[Symbol.asyncIterator] !== "function") {
    throw new AgentError("invalid_model_response", "Model gateway returned an invalid stream");
  }
  const iterator = stream[Symbol.asyncIterator]();
  let content = "";
  let reasoning = "";
  let finishReason: ModelStreamChunk["finishReason"];
  let usage: ModelTokenUsage | undefined;
  let malformed: string | undefined;
  const calls = new Map<number, ToolCallParts>();

  try {
    while (finishReason === undefined) {
      const step = await nextWithSignal(iterator, signal);
      if (step.done === true) break;
      const chunk = validateModelStreamChunk(step.value);
      await onChunk?.(chunk);
      content += chunk.contentDelta ?? "";
      reasoning += chunk.reasoningDelta ?? "";
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
  const message: Message = {
    role: "assistant",
    content,
    ...(reasoning.trim() === "" ? {} : { reasoning }),
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
  const current = calls.get(delta.index) ?? { arguments: "" };
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
  current.arguments += delta.argumentsFragment ?? "";
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
      parsed = JSON.parse(parts.arguments);
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
  if (signal?.aborted === true) throw new AgentCanceledError();
}
