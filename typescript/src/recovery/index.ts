export type RecoveryAction = "retry_model" | "fallback_provider_mode" | "replan";

export type RecoveryCause =
  | "provider_required_tool_choice_unsupported"
  | "provider_stream_interrupted"
  | "malformed_tool_call_batch"
  | "missing_required_tool_call"
  | "missing_required_tool_call_replan"
  | "unstructured_tool_protocol"
  | "empty_model_response"
  | "structured_output_invalid"
  | "response_constraint_deterministic"
  | "response_constraint_semantic"
  | "future_tool_step"
  | "unauthorized_tool"
  | "unauthorized_tool_replan"
  | "tool_input_invalid"
  | "tool_execution_failed_replan";

export type RecoveryEffectState = "not_started" | "committed" | "unknown";

export type RecoveryReason =
  | "allowed"
  | "attempt_budget_exhausted"
  | "cause_not_retryable"
  | "policy_disabled"
  | "request_canceled"
  | "round_budget_exhausted"
  | "side_effect_committed"
  | "side_effect_state_unknown"
  | "visible_output_already_emitted";

export interface RecoveryRequest {
  readonly cause: RecoveryCause;
  readonly action: RecoveryAction;
  readonly scope?: string;
  readonly remainingModelRounds?: number;
  readonly minimumRemainingRounds?: number;
  readonly retryable?: boolean;
  readonly cancellationRequested?: boolean;
  readonly visibleOutputEmitted?: boolean;
  readonly effectState?: RecoveryEffectState;
  readonly mayRepeatSideEffect?: boolean;
}

export interface RecoveryDecision {
  readonly cause: RecoveryCause;
  readonly action: RecoveryAction;
  readonly scope: string;
  readonly allowed: boolean;
  readonly reasonCode: RecoveryReason;
  readonly attempt: number;
  readonly maxAttempts: number;
  readonly remainingModelRounds: number;
  readonly minimumRemainingRounds: number;
  readonly effectState: RecoveryEffectState;
  readonly mayRepeatSideEffect: boolean;
}

interface NormalizedRecoveryRequest {
  readonly cause: RecoveryCause;
  readonly action: RecoveryAction;
  readonly scope: string;
  readonly remainingModelRounds: number;
  readonly minimumRemainingRounds: number;
  readonly retryable: boolean;
  readonly cancellationRequested: boolean;
  readonly visibleOutputEmitted: boolean;
  readonly effectState: RecoveryEffectState;
  readonly mayRepeatSideEffect: boolean;
}

export type RecoveryAttemptLimits = Partial<Readonly<Record<RecoveryCause, number>>>;

export const EMPTY_RESPONSE_RETRY_GUIDANCE = [
  "The previous model round ended without an official response.",
  "Continue the same request and return a complete response through the requested output protocol.",
  "Do not return reasoning alone.",
].join(" ");

const STANDARD_LIMITS: Readonly<Record<RecoveryCause, number>> = Object.freeze({
  provider_required_tool_choice_unsupported: 1,
  provider_stream_interrupted: 1,
  malformed_tool_call_batch: 1,
  missing_required_tool_call: 1,
  missing_required_tool_call_replan: 1,
  unstructured_tool_protocol: 1,
  empty_model_response: 2,
  structured_output_invalid: 0,
  response_constraint_deterministic: 1,
  response_constraint_semantic: 1,
  future_tool_step: 1,
  unauthorized_tool: 1,
  unauthorized_tool_replan: 1,
  tool_input_invalid: 1,
  tool_execution_failed_replan: 1,
});

const CAUSES = new Set<RecoveryCause>(Object.keys(STANDARD_LIMITS) as RecoveryCause[]);
const ACTIONS = new Set<RecoveryAction>(["retry_model", "fallback_provider_mode", "replan"]);
const EFFECT_STATES = new Set<RecoveryEffectState>(["not_started", "committed", "unknown"]);

export class RecoveryPolicy {
  readonly #limits: ReadonlyMap<RecoveryCause, number>;

  public constructor(attemptLimits: RecoveryAttemptLimits = STANDARD_LIMITS) {
    if (attemptLimits === null || typeof attemptLimits !== "object" || Array.isArray(attemptLimits)) {
      throw new TypeError("recovery attempt limits must be an object");
    }
    const limits = new Map<RecoveryCause, number>();
    for (const [cause, attempts] of Object.entries(attemptLimits)) {
      if (!CAUSES.has(cause as RecoveryCause)) throw new TypeError(`unknown recovery cause: ${cause}`);
      if (!Number.isSafeInteger(attempts) || attempts! < 0) {
        throw new TypeError("recovery max attempts must be a non-negative integer");
      }
      limits.set(cause as RecoveryCause, attempts!);
    }
    this.#limits = limits;
  }

  public maxAttempts(cause: RecoveryCause): number {
    assertCause(cause);
    return this.#limits.get(cause) ?? 0;
  }

  public withOverrides(overrides: RecoveryAttemptLimits): RecoveryPolicy {
    const limits = this.snapshot();
    return new RecoveryPolicy({ ...limits, ...overrides });
  }

  public snapshot(): Readonly<RecoveryAttemptLimits> {
    return Object.freeze(Object.fromEntries(this.#limits) as RecoveryAttemptLimits);
  }
}

export class RecoveryLedger {
  readonly #policy: RecoveryPolicy;
  readonly #attempts = new Map<string, number>();

  public constructor(policy = new RecoveryPolicy()) {
    if (!(policy instanceof RecoveryPolicy)) throw new TypeError("recovery policy is invalid");
    this.#policy = policy;
  }

  public attempts(cause: RecoveryCause, scope = "run"): number {
    assertCause(cause);
    return this.#attempts.get(key(cause, requiredText(scope, "recovery scope"))) ?? 0;
  }

  public snapshot(): readonly {
    readonly cause: RecoveryCause;
    readonly scope: string;
    readonly attempts: number;
  }[] {
    return Object.freeze([...this.#attempts.entries()]
      .map(([attemptKey, attempts]) => {
        const separator = attemptKey.indexOf("\u0000");
        return Object.freeze({
          cause: attemptKey.slice(0, separator) as RecoveryCause,
          scope: attemptKey.slice(separator + 1),
          attempts,
        });
      })
      .sort((left, right) => (
        left.cause.localeCompare(right.cause) || left.scope.localeCompare(right.scope)
      )));
  }

  public restore(snapshot: readonly {
    readonly cause: RecoveryCause;
    readonly scope: string;
    readonly attempts: number;
  }[]): void {
    if (this.#attempts.size > 0) throw new Error("Recovery ledger has already been used");
    if (!Array.isArray(snapshot)) throw new TypeError("Recovery attempt snapshot must be an array");
    for (const item of snapshot) {
      assertCause(item.cause);
      const scope = requiredText(item.scope, "recovery scope");
      const attempts = nonNegativeInteger(item.attempts, "recovery attempts");
      const attemptKey = key(item.cause, scope);
      if (this.#attempts.has(attemptKey)) {
        throw new TypeError("Recovery attempt snapshot contains duplicates");
      }
      this.#attempts.set(attemptKey, attempts);
    }
  }

  public decide(value: RecoveryRequest): RecoveryDecision {
    const request = normalizeRequest(value);
    const maxAttempts = this.#policy.maxAttempts(request.cause);
    const attemptKey = key(request.cause, request.scope);
    const used = this.#attempts.get(attemptKey) ?? 0;
    const reasonCode = denialReason(request, used, maxAttempts);
    const allowed = reasonCode === undefined;
    const attempt = allowed ? used + 1 : used;
    if (allowed) this.#attempts.set(attemptKey, attempt);
    return Object.freeze({
      ...request,
      allowed,
      reasonCode: reasonCode ?? "allowed",
      attempt,
      maxAttempts,
    });
  }
}

export function recoveryDecisionDetails(
  decision: RecoveryDecision,
): Readonly<Record<string, string | number | boolean>> {
  return Object.freeze({
    cause: decision.cause,
    action: decision.action,
    scope: decision.scope,
    allowed: decision.allowed,
    reasonCode: decision.reasonCode,
    attempt: decision.attempt,
    maxAttempts: decision.maxAttempts,
    remainingModelRounds: decision.remainingModelRounds,
    minimumRemainingRounds: decision.minimumRemainingRounds,
    effectState: decision.effectState,
    mayRepeatSideEffect: decision.mayRepeatSideEffect,
  });
}

function normalizeRequest(value: RecoveryRequest): NormalizedRecoveryRequest {
  if (value === null || typeof value !== "object") throw new TypeError("recovery request is invalid");
  assertCause(value.cause);
  if (!ACTIONS.has(value.action)) throw new TypeError("recovery action is invalid");
  const effectState = value.effectState ?? "not_started";
  if (!EFFECT_STATES.has(effectState)) throw new TypeError("recovery effect state is invalid");
  return Object.freeze({
    cause: value.cause,
    action: value.action,
    scope: requiredText(value.scope ?? "run", "recovery scope"),
    remainingModelRounds: nonNegativeInteger(value.remainingModelRounds ?? 0, "remaining model rounds"),
    minimumRemainingRounds: nonNegativeInteger(value.minimumRemainingRounds ?? 1, "minimum remaining rounds"),
    retryable: value.retryable ?? true,
    cancellationRequested: value.cancellationRequested ?? false,
    visibleOutputEmitted: value.visibleOutputEmitted ?? false,
    effectState,
    mayRepeatSideEffect: value.mayRepeatSideEffect ?? false,
  });
}

function denialReason(
  request: ReturnType<typeof normalizeRequest>,
  used: number,
  maxAttempts: number,
): Exclude<RecoveryReason, "allowed"> | undefined {
  if (request.cancellationRequested) return "request_canceled";
  if (!request.retryable) return "cause_not_retryable";
  if (request.visibleOutputEmitted) return "visible_output_already_emitted";
  if (request.mayRepeatSideEffect && request.effectState === "committed") return "side_effect_committed";
  if (request.mayRepeatSideEffect && request.effectState === "unknown") return "side_effect_state_unknown";
  if (request.remainingModelRounds < request.minimumRemainingRounds) return "round_budget_exhausted";
  if (maxAttempts <= 0) return "policy_disabled";
  if (used >= maxAttempts) return "attempt_budget_exhausted";
  return undefined;
}

function assertCause(value: unknown): asserts value is RecoveryCause {
  if (!CAUSES.has(value as RecoveryCause)) throw new TypeError("recovery cause is invalid");
}

function nonNegativeInteger(value: unknown, label: string): number {
  if (!Number.isSafeInteger(value) || (value as number) < 0) {
    throw new TypeError(`${label} must be a non-negative integer`);
  }
  return value as number;
}

function requiredText(value: unknown, label: string): string {
  if (typeof value !== "string" || value.trim() === "") throw new TypeError(`${label} is required`);
  return value.trim();
}

function key(cause: RecoveryCause, scope: string): string {
  return `${cause}\u0000${scope}`;
}
