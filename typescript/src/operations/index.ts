import type { JsonValue } from "../model/types.js";
import { copyJsonValue } from "../model/validation.js";
import { AgentError } from "../shared/errors.js";

export type OperationKind = "planning" | "model" | "tool" | "validation" | "context_compaction";
export type OperationStatus = "running" | "succeeded" | "failed" | "canceled";

export interface OperationDisplay {
  readonly labelKey?: string;
  readonly labelParams?: Readonly<Record<string, JsonValue>>;
  readonly resourceRef?: string;
}

export interface OperationScope {
  readonly runId: string;
  readonly invocationId?: string;
  readonly parentOperationId?: string;
  readonly display?: OperationDisplay;
}

export interface OperationStarted {
  readonly type: "operation.started";
  readonly operationId: string;
  readonly runId: string;
  readonly invocationId?: string;
  readonly parentOperationId?: string;
  readonly kind: OperationKind;
  readonly startedAt: string;
  readonly display: OperationDisplay;
}

export interface OperationFinished {
  readonly type: "operation.finished";
  readonly operationId: string;
  readonly runId: string;
  readonly invocationId?: string;
  readonly parentOperationId?: string;
  readonly status: Exclude<OperationStatus, "running">;
  readonly finishedAt: string;
  readonly durationMs: number;
  readonly errorCode?: string;
  readonly display: OperationDisplay;
}

export type OperationEvent = OperationStarted | OperationFinished;

export interface OperationReceipt {
  readonly operationId: string;
  readonly kind: OperationKind;
  readonly runId: string;
  readonly invocationId?: string;
  readonly parentOperationId?: string;
  readonly startedAt: string;
  readonly startedEvent: OperationStarted;
}

export interface OperationEventProcessor {
  acceptOperationEvent(event: OperationEvent): Promise<unknown> | unknown;
}

interface RunningOperation {
  readonly receipt: OperationReceipt;
  readonly monotonicStarted: number;
  readonly display: OperationDisplay;
}

const KINDS = new Set<OperationKind>([
  "planning", "model", "tool", "validation", "context_compaction",
]);
const LIFECYCLE_DISPLAY_FIELDS = new Set([
  "operationid", "status", "startedat", "finishedat", "durationms", "errorcode",
]);

export class AgentOperationController {
  readonly #processor: OperationEventProcessor;
  readonly #wallClock: () => Date;
  readonly #monotonicClock: () => number;
  readonly #idFactory: () => string;
  readonly #running = new Map<string, RunningOperation>();
  readonly #settling = new Set<string>();
  readonly #terminal = new Set<string>();

  public constructor(
    processor: OperationEventProcessor,
    options: {
      readonly wallClock?: () => Date;
      readonly monotonicClock?: () => number;
      readonly idFactory?: () => string;
    } = {},
  ) {
    if (typeof processor?.acceptOperationEvent !== "function") {
      throw new TypeError("operation controller requires an event processor");
    }
    this.#processor = processor;
    this.#wallClock = options.wallClock ?? (() => new Date());
    this.#monotonicClock = options.monotonicClock ?? (() => performance.now());
    this.#idFactory = options.idFactory ?? (() => `operation-${globalThis.crypto.randomUUID()}`);
  }

  public get runningOperationIds(): readonly string[] {
    return Object.freeze([...this.#running.keys()]);
  }

  public withOutput(processor: OperationEventProcessor): AgentOperationController {
    return new AgentOperationController({ acceptOperationEvent: async (event) => {
      await processor.acceptOperationEvent(event);
      await this.#processor.acceptOperationEvent(event);
    } }, { wallClock: this.#wallClock, monotonicClock: this.#monotonicClock, idFactory: this.#idFactory });
  }

  public async start(kind: OperationKind, scope: OperationScope): Promise<OperationReceipt> {
    if (!KINDS.has(kind)) throw new TypeError("operation kind is invalid");
    if (scope === null || typeof scope !== "object") throw new TypeError("operation scope is invalid");
    const operationId = requiredText(this.#idFactory(), "operation id");
    if (this.#running.has(operationId) || this.#terminal.has(operationId)) {
      throw new AgentError("operation_contract_violation", "operation id is not unique");
    }
    const runId = requiredText(scope.runId, "run id");
    const invocationId = optionalText(scope.invocationId, "invocation id");
    const display = copyDisplay(scope.display);
    const startedAt = this.#wallTime();
    const startedEvent: OperationStarted = Object.freeze({
      type: "operation.started",
      operationId,
      runId,
      ...(invocationId === undefined ? {} : { invocationId }),
      ...(scope.parentOperationId === undefined ? {} : { parentOperationId: requiredText(scope.parentOperationId, "parent operation id") }),
      kind,
      startedAt,
      display,
    });
    const receipt: OperationReceipt = Object.freeze({
      operationId,
      kind,
      runId,
      ...(invocationId === undefined ? {} : { invocationId }),
      startedAt,
      startedEvent,
    });
    const monotonicStarted = this.#monotonicTime();
    await this.#processor.acceptOperationEvent(startedEvent);
    this.#running.set(operationId, Object.freeze({ receipt, monotonicStarted, display }));
    return receipt;
  }

  public succeed(operationId: string, display?: OperationDisplay): Promise<OperationFinished> {
    return this.#finish(operationId, "succeeded", undefined, display);
  }

  public fail(operationId: string, errorCode: string, display?: OperationDisplay): Promise<OperationFinished> {
    return this.#finish(operationId, "failed", requiredText(errorCode, "operation error code"), display);
  }

  public cancel(
    operationId: string,
    errorCode = "operation_canceled",
    display?: OperationDisplay,
  ): Promise<OperationFinished> {
    return this.#finish(operationId, "canceled", requiredText(errorCode, "operation error code"), display);
  }

  async #finish(
    value: string,
    status: OperationFinished["status"],
    errorCode?: string,
    display?: OperationDisplay,
  ): Promise<OperationFinished> {
    const operationId = requiredText(value, "operation id");
    if (this.#terminal.has(operationId) || this.#settling.has(operationId)) {
      throw new AgentError("operation_contract_violation", "operation is already terminal");
    }
    const running = this.#running.get(operationId);
    if (running === undefined) {
      throw new AgentError("operation_contract_violation", "operation was not started");
    }
    this.#settling.add(operationId);
    try {
      const finished: OperationFinished = Object.freeze({
        type: "operation.finished",
        operationId,
        runId: running.receipt.runId,
        ...(running.receipt.invocationId === undefined
          ? {}
          : { invocationId: running.receipt.invocationId }),
        ...(running.receipt.startedEvent.parentOperationId === undefined ? {} : { parentOperationId: running.receipt.startedEvent.parentOperationId }),
        status,
        finishedAt: this.#wallTime(),
        durationMs: Math.max(0, Math.round(this.#monotonicTime() - running.monotonicStarted)),
        ...(errorCode === undefined ? {} : { errorCode }),
        display: display === undefined ? running.display : copyDisplay(display),
      });
      await this.#processor.acceptOperationEvent(finished);
      this.#running.delete(operationId);
      this.#terminal.add(operationId);
      return finished;
    } finally {
      this.#settling.delete(operationId);
    }
  }

  #wallTime(): string {
    const value = this.#wallClock();
    if (!(value instanceof Date) || !Number.isFinite(value.getTime())) {
      throw new AgentError("operation_contract_violation", "operation wall clock returned an invalid Date");
    }
    return value.toISOString();
  }

  #monotonicTime(): number {
    const value = this.#monotonicClock();
    if (!Number.isFinite(value)) {
      throw new AgentError("operation_contract_violation", "operation monotonic clock returned an invalid number");
    }
    return value;
  }
}

function copyDisplay(value: OperationDisplay | undefined): OperationDisplay {
  if (value === undefined) return Object.freeze({});
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("operation display is invalid");
  }
  const labelKey = optionalText(value.labelKey, "operation labelKey");
  const resourceRef = optionalText(value.resourceRef, "operation resourceRef");
  const labelParams = value.labelParams === undefined
    ? undefined
    : copyJsonValue(value.labelParams) as Readonly<Record<string, JsonValue>>;
  for (const key of Object.keys(labelParams ?? {})) {
    if (LIFECYCLE_DISPLAY_FIELDS.has(key.replaceAll("_", "").toLowerCase())) {
      throw new TypeError("operation display cannot include a lifecycle field");
    }
  }
  return Object.freeze({
    ...(labelKey === undefined ? {} : { labelKey }),
    ...(labelParams === undefined ? {} : { labelParams }),
    ...(resourceRef === undefined ? {} : { resourceRef }),
  });
}

function requiredText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim() === "") throw new TypeError(`${label} is required`);
  return value.trim();
}

function optionalText(value: unknown, label: string): string | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "string") throw new TypeError(`${label} must be text`);
  return value.trim() || undefined;
}
