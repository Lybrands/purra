import type { JsonValue } from "../model/types.js";
import { copyJsonValue } from "../model/validation.js";
import { AgentError } from "../shared/errors.js";

const CODES = [
  "retrieval_scope_unavailable",
  "retrieval_access_denied",
  "retrieval_source_unavailable",
  "retrieval_timeout",
  "retrieval_index_not_ready",
  "invalid_retrieval_result",
  "retrieval_result_too_large",
] as const;

type RetrievalErrorCode = typeof CODES[number];

interface RetrievalErrorOptions extends ErrorOptions {
  readonly retryable?: boolean;
  readonly details?: Readonly<Record<string, JsonValue>>;
}

export class RetrievalError extends AgentError {
  public readonly retryable: boolean;
  public readonly details: Readonly<Record<string, JsonValue>>;

  public constructor(
    code: RetrievalErrorCode,
    message: string,
    options: RetrievalErrorOptions = {},
  ) {
    if (!(CODES as readonly unknown[]).includes(code)) {
      throw new TypeError(`Unsupported retrieval error code: ${String(code)}`);
    }
    super(code, message, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = "RetrievalError";
    this.retryable = options.retryable ?? false;
    if (typeof this.retryable !== "boolean") {
      throw new TypeError("Retrieval error retryable must be boolean");
    }
    const details = options.details ?? {};
    const copied = copyJsonValue(details);
    if (copied === null || Array.isArray(copied) || typeof copied !== "object") {
      throw new TypeError("Retrieval error details must be an object");
    }
    this.details = copied as Readonly<Record<string, JsonValue>>;
  }
}
