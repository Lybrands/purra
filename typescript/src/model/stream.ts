import { AgentCanceledError, AgentError } from "../shared/errors.js";
import type {
  Message,
  ModelGateway,
  ModelRequest,
  ModelStream,
  ModelStreamActivity,
  ModelStreamActivitySupport,
  ModelStreamChunk,
  ModelStreamItem,
  ModelTokenUsage,
  ModelTurn,
  ModelTransportDiagnostics,
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
  readonly activityIdleTimeoutMs: number | null;
  readonly progressIdleTimeoutMs: number | null;
  readonly invocationTimeoutMs: number | null;
  readonly maxChunks: number;
  readonly maxContentChars: number;
  readonly maxReasoningChars: number;
  readonly maxToolArgumentChars: number;
}

const DEFAULT_STREAM_LIMITS: ModelStreamLimits = Object.freeze({
  activityIdleTimeoutMs: 30_000,
  progressIdleTimeoutMs: 60_000,
  invocationTimeoutMs: 300_000,
  maxChunks: 100_000,
  maxContentChars: 1_000_000,
  maxReasoningChars: 1_000_000,
  maxToolArgumentChars: 1_000_000,
});

export function constrainModelInvocationTimeout(
  limits: ModelStreamLimits | undefined,
  maximumMs: number,
): ModelStreamLimits {
  if (!Number.isSafeInteger(maximumMs) || maximumMs < 1) {
    throw new TypeError("invocation timeout maximum must be positive");
  }
  const base = limits ?? DEFAULT_STREAM_LIMITS;
  return Object.freeze({
    ...base,
    invocationTimeoutMs: base.invocationTimeoutMs === null
      ? maximumMs
      : Math.min(base.invocationTimeoutMs, maximumMs),
  });
}

export async function invokeModel(
  gateway: ModelGateway,
  request: ModelRequest,
  signal: AbortSignal | undefined,
  useStream: boolean,
  onChunk?: (chunk: ModelStreamChunk) => Promise<void> | void,
  limits: ModelStreamLimits = DEFAULT_STREAM_LIMITS,
  onDiagnostics?: (metrics: Readonly<Record<string, number | string | null>>) => void,
): Promise<ModelTurn> {
  const stop = invocationSignal(signal, limits.invocationTimeoutMs);
  const started = performance.now();
  const gatewayStartedAtMs = Date.now();
  let timings: Readonly<Record<string, number | string | null>> = { firstActivityMs: null, firstProgressMs: null, firstSemanticChunkMs: null };
  let openedStream: ModelStream | undefined;
  let streamConsumed = false;
  try {
    throwIfCanceled(stop.signal);
    if (!useStream) {
      const turn = validateModelTurn(await awaitWithSignal(
        gateway.invoke(request, stop.signal),
        stop.signal,
      ));
      requireAppliedOutputLimit(request, turn.appliedOutputLimit, turn.usage);
      throwIfCanceled(stop.signal);
      return turn;
    }
    const stream = await awaitWithSignal(
      Promise.resolve(gateway.stream!(request, stop.signal)).then(async (value) => {
        openedStream = value;
        if (stop.signal.aborted) {
          openedStream = undefined;
          if (typeof value?.[Symbol.asyncIterator] === "function") await closeIterator(value[Symbol.asyncIterator](), true);
          throwIfCanceled(stop.signal);
        }
        return value;
      }),
      stop.signal,
    );
    throwIfCanceled(stop.signal);
    const appliedOutputLimit = stream.appliedOutputLimit === undefined
      ? undefined
      : stream.appliedOutputLimit === null
      ? null
      : positiveInteger(stream.appliedOutputLimit, "applied output limit");
    requireAppliedOutputLimit(request, appliedOutputLimit);
    streamConsumed = true;
    return await consumeModelStream(
      stream,
      stop,
      onChunk,
      limits,
      request,
      appliedOutputLimit,
      started,
      (value) => { timings = value; },
    );
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
    if (!streamConsumed && typeof openedStream?.[Symbol.asyncIterator] === "function") {
      await closeIterator(openedStream[Symbol.asyncIterator](), stop.signal.aborted);
    }
    onDiagnostics?.({ ...timings, gatewayStartedAtMs, invocationDurationMs: performance.now() - started,
      httpRequestSentAtMs: null, httpFirstByteAtMs: null, sdkHttpAttempts: null, ...timings });
  }
}

async function consumeModelStream(
  stream: ModelStream,
  stop: InvocationStop,
  onChunk: ((chunk: ModelStreamChunk) => Promise<void> | void) | undefined,
  limits: ModelStreamLimits,
  request: ModelRequest,
  appliedOutputLimit: number | null | undefined,
  started: number,
  onDiagnostics: (metrics: Readonly<Record<string, number | string | null>>) => void,
): Promise<ModelTurn> {
  if (stream === null || typeof stream?.[Symbol.asyncIterator] !== "function") {
    throw new AgentError("invalid_model_response", "Model gateway returned an invalid stream");
  }
  const iterator = stream[Symbol.asyncIterator]();
  let liveness: StreamLiveness | undefined;
  const content: string[] = [];
  const reasoning: string[] = [];
  let contentChars = 0;
  let reasoningChars = 0;
  let chunkCount = 0;
  let finishReason: ModelStreamChunk["finishReason"];
  let usage: ModelTokenUsage | undefined;
  let providerData: Message["providerData"];
  let malformed: string | undefined;
  const calls = new Map<number, ToolCallParts>();

  try {
    liveness = new StreamLiveness(activitySupport(stream.activitySupport), limits, stop.abort, started);
    liveness.acceptTransportDiagnostics(stream.transportDiagnostics);
    while (finishReason === undefined) {
      const step = await nextWithSignal(iterator, stop.signal);
      if (step.done === true) break;
      throwIfCanceled(stop.signal);
      if (isActivity(step.value)) {
        liveness.acceptActivity(step.value);
        continue;
      }
      const chunk = validateModelStreamChunk(step.value);
      const meaningful = isMeaningfulChunk(chunk);
      if (meaningful) liveness.acceptSemanticProgress(chunk);
      try {
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
        if (onChunk !== undefined) await awaitWithSignal(Promise.resolve(onChunk(chunk)), stop.signal);
        throwIfCanceled(stop.signal);
        if (chunk.contentDelta !== undefined && chunk.contentDelta !== "") content.push(chunk.contentDelta);
        if (chunk.reasoningDelta !== undefined && chunk.reasoningDelta !== "") reasoning.push(chunk.reasoningDelta);
        usage = chunk.usage ?? usage;
        providerData = chunk.providerData ?? providerData;
        finishReason = chunk.finishReason;
        for (const delta of chunk.toolCallDeltas ?? []) {
          malformed = mergeToolCallDelta(calls, delta) ?? malformed;
        }
      } finally {
        if (meaningful) liveness.resume();
      }
    }
  } finally {
    liveness?.close();
    if (liveness !== undefined) onDiagnostics(liveness.diagnostics());
    await closeIterator(iterator, stop.signal.aborted);
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
    ...(providerData === undefined ? {} : { providerData }),
  };
  const turn = validateModelTurn({
    message,
    finishReason,
    ...(appliedOutputLimit === undefined ? {} : { appliedOutputLimit }),
    ...(usage === undefined ? {} : { usage }),
  });
  requireAppliedOutputLimit(request, appliedOutputLimit, turn.usage);
  return turn;
}

function requireAppliedOutputLimit(
  request: ModelRequest,
  appliedOutputLimit: number | null | undefined,
  usage?: ModelTokenUsage,
): void {
  const expected = request.outputLimit?.maxTokens;
  if (expected === undefined && appliedOutputLimit === undefined) return;
  if (appliedOutputLimit !== expected) {
    throw new AgentError(
      "model_gateway_contract_violation",
      `Model gateway applied output limit ${String(appliedOutputLimit)} instead of ${String(expected)}`,
    );
  }
  if (
    expected !== undefined
    && usage?.outputTokens !== undefined
    && usage.outputTokens > expected
  ) {
    throw new AgentError(
      "model_gateway_contract_violation",
      "Model gateway reported output usage above the applied invocation limit",
    );
  }
}

function positiveInteger(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 1) {
    throw new AgentError(
      "model_gateway_contract_violation",
      `${label} must be a positive integer or null`,
    );
  }
  return value as number;
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
  if (signal.aborted) {
    void promise.catch(() => undefined);
    throwIfCanceled(signal);
  }
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
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const closing = Promise.resolve(iterator.return());
    // Drain cooperative adapters, including a pending next(), without allowing
    // an uncooperative SDK to hold local Run terminalization indefinitely.
    await Promise.race([closing, new Promise<void>((resolve) => { timer = setTimeout(resolve, 1000); })]);
  } catch {
    // Cleanup must not replace the selected public outcome.
  } finally {
    if (timer !== undefined) clearTimeout(timer);
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
): InvocationStop {
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
    abort(error: AgentError): void {
      controller.abort(error);
    },
    close(): void {
      if (timer !== undefined) globalThis.clearTimeout(timer);
      parent?.removeEventListener("abort", forward);
    },
  };
}

interface InvocationStop {
  readonly signal: AbortSignal;
  abort(error: AgentError): void;
  close(): void;
}

class StreamLiveness {
  readonly #support: ModelStreamActivitySupport;
  readonly #activityTimeoutMs: number | null;
  readonly #progressTimeoutMs: number | null;
  readonly #abort: (error: AgentError) => void;
  readonly #startedAt: number;
  #firstSemanticAt: number | undefined;
  #activityTimer: ReturnType<typeof setTimeout> | undefined;
  #progressTimer: ReturnType<typeof setTimeout> | undefined;
  #firstActivityAt: number | undefined;
  #lastActivityAt: number | undefined;
  #firstProgressAt: number | undefined;
  #lastProgressAt: number | undefined;
  #maxActivityGapMs: number | null = null;
  #maxProgressGapMs: number | null = null;
  #closed = false;
  #transport: ModelTransportDiagnostics | undefined;

  public constructor(
    support: ModelStreamActivitySupport,
    limits: ModelStreamLimits,
    abort: (error: AgentError) => void,
    started = performance.now(),
  ) {
    this.#startedAt = started;
    this.#support = support;
    this.#activityTimeoutMs = optionalPositiveLimit(
      limits.activityIdleTimeoutMs,
      "activityIdleTimeoutMs",
    );
    this.#progressTimeoutMs = optionalPositiveLimit(
      limits.progressIdleTimeoutMs,
      "progressIdleTimeoutMs",
    );
    this.#abort = abort;
    if (support !== "semantic_only") this.#armBoth();
  }

  public acceptActivity(activity: ModelStreamActivity): void {
    if (this.#support === "semantic_only") {
      throw unsupportedActivity(this.#support, activity.kind);
    }
    if (activity.kind !== "transport" && activity.kind !== "working") {
      throw unsupportedActivity(this.#support, String(activity.kind));
    }
    if (activity.kind === "working" && this.#support !== "working") {
      throw unsupportedActivity(this.#support, activity.kind);
    }
    this.acceptTransportDiagnostics(activity.transportDiagnostics);
    const now = performance.now();
    this.#recordActivity(now);
    this.#armActivity();
    if (activity.kind === "working") {
      this.#recordProgress(now);
      this.#armProgress();
    }
  }

  public acceptSemanticProgress(chunk: ModelStreamChunk): void {
    if (chunk.contentDelta || chunk.reasoningDelta || chunk.progressDelta || chunk.toolCallDeltas?.length) this.#firstSemanticAt ??= performance.now();
    const now = performance.now();
    this.#recordActivity(now);
    this.#recordProgress(now);
    this.#clearTimers();
  }

  public resume(): void {
    if (this.#support !== "semantic_only" && !this.#closed) this.#armBoth();
  }

  public close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#clearTimers();
  }

  public acceptTransportDiagnostics(evidence: ModelTransportDiagnostics | undefined): void {
    if (evidence === undefined) return;
    if (evidence === null || typeof evidence !== "object" || Array.isArray(evidence)
      || Object.keys(evidence).some((key) => !["requestSentAtMs", "firstByteAtMs", "httpAttempts"].includes(key))
      || Object.values(evidence).some((value) => !Number.isSafeInteger(value) || value < 1)
      || (evidence.requestSentAtMs !== undefined && evidence.firstByteAtMs !== undefined && evidence.firstByteAtMs < evidence.requestSentAtMs)) {
      throw new AgentError("model_gateway_contract_violation", "Invalid transport diagnostics");
    }
    this.#transport = Object.freeze({ ...evidence });
  }

  public diagnostics(): Readonly<Record<string, number | string | null>> {
    return { httpRequestSentAtMs: this.#transport?.requestSentAtMs ?? null,
      httpFirstByteAtMs: this.#transport?.firstByteAtMs ?? null, sdkHttpAttempts: this.#transport?.httpAttempts ?? null,
      activitySupport: this.#support, firstActivityMs: offset(this.#firstActivityAt, this.#startedAt),
      firstProgressMs: offset(this.#firstProgressAt, this.#startedAt), firstSemanticChunkMs: offset(this.#firstSemanticAt, this.#startedAt) };
  }

  #recordActivity(now: number): void {
    if (this.#lastActivityAt !== undefined) {
      this.#maxActivityGapMs = Math.max(
        this.#maxActivityGapMs ?? 0,
        now - this.#lastActivityAt,
      );
    }
    this.#firstActivityAt ??= now;
    this.#lastActivityAt = now;
  }

  #recordProgress(now: number): void {
    if (this.#lastProgressAt !== undefined) {
      this.#maxProgressGapMs = Math.max(
        this.#maxProgressGapMs ?? 0,
        now - this.#lastProgressAt,
      );
    }
    this.#firstProgressAt ??= now;
    this.#lastProgressAt = now;
  }

  #armBoth(): void {
    this.#armActivity();
    this.#armProgress();
  }

  #armActivity(): void {
    if (this.#activityTimer !== undefined) clearTimeout(this.#activityTimer);
    this.#activityTimer = this.#activityTimeoutMs === null ? undefined : setTimeout(() => {
      this.#expire("model_activity_deadline_exceeded", "activity");
    }, this.#activityTimeoutMs);
  }

  #armProgress(): void {
    if (this.#progressTimer !== undefined) clearTimeout(this.#progressTimer);
    this.#progressTimer = this.#progressTimeoutMs === null ? undefined : setTimeout(() => {
      this.#expire("model_progress_deadline_exceeded", "progress");
    }, this.#progressTimeoutMs);
  }

  #expire(code: string, boundary: "activity" | "progress"): void {
    if (this.#closed) return;
    this.close();
    const now = performance.now();
    this.#abort(new AgentError(
      code,
      boundary === "activity"
        ? "Model stream activity deadline has elapsed"
        : "Model stream progress deadline has elapsed",
      { cause: Object.freeze({
        support: this.#support,
        phase: "stream",
        elapsedMs: now - this.#startedAt,
        firstActivityMs: offset(this.#firstActivityAt, this.#startedAt),
        lastActivityMs: offset(this.#lastActivityAt, this.#startedAt),
        firstProgressMs: offset(this.#firstProgressAt, this.#startedAt),
        lastProgressMs: offset(this.#lastProgressAt, this.#startedAt),
        maxActivityGapMs: this.#maxActivityGapMs,
        maxProgressGapMs: this.#maxProgressGapMs,
        boundary,
      }) },
    ));
  }

  #clearTimers(): void {
    if (this.#activityTimer !== undefined) clearTimeout(this.#activityTimer);
    if (this.#progressTimer !== undefined) clearTimeout(this.#progressTimer);
    this.#activityTimer = undefined;
    this.#progressTimer = undefined;
  }
}

function activitySupport(value: ModelStreamActivitySupport | undefined): ModelStreamActivitySupport {
  const support = value ?? "semantic_only";
  if (support !== "semantic_only" && support !== "transport" && support !== "working") {
    throw new AgentError(
      "model_stream_activity_unsupported",
      "Model stream declared unsupported activity evidence",
    );
  }
  return support;
}

function isActivity(value: ModelStreamItem): value is ModelStreamActivity {
  return value !== null && typeof value === "object" && "type" in value && value.type === "activity";
}

function unsupportedActivity(support: string, kind: string): AgentError {
  return new AgentError(
    "model_stream_activity_unsupported",
    `Model stream activity ${kind} is not supported by ${support}`,
  );
}

function isMeaningfulChunk(chunk: ModelStreamChunk): boolean {
  return (chunk.contentDelta?.length ?? 0) > 0
    || (chunk.reasoningDelta?.length ?? 0) > 0
    || (chunk.progressDelta?.length ?? 0) > 0
    || (chunk.toolCallDeltas ?? []).some((delta) => (
      delta.id !== undefined
      || delta.type !== undefined
      || delta.name !== undefined
      || (delta.argumentsFragment?.length ?? 0) > 0
    ))
    || chunk.usage !== undefined
    || chunk.finishReason !== undefined;
}

function optionalPositiveLimit(value: number | null, name: string): number | null {
  if (value !== null && (!Number.isSafeInteger(value) || value < 1)) {
    throw new TypeError(`${name} must be positive or null`);
  }
  return value;
}

function offset(value: number | undefined, start: number): number | null {
  return value === undefined ? null : value - start;
}
