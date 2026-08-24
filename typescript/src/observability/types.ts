export type DiagnosticVerdict = "pass" | "warn" | "fail" | "insufficient_data";
export type DiagnosticCheckStatus = DiagnosticVerdict | "not_applicable";

export interface EvidenceEvent {
  readonly eventType?: string;
  readonly kind?: string;
  readonly payload?: unknown;
}

export interface RunEvidence {
  readonly id?: string;
  readonly runId?: string;
  readonly status?: string;
}

export interface DiagnosticCheck {
  readonly name: string;
  readonly status: DiagnosticCheckStatus;
  readonly detail?: unknown;
  readonly value?: number | null;
  readonly numerator?: number;
  readonly denominator?: number | null;
  readonly warnAt?: number;
  readonly failAt?: number;
}

export interface CanonicalRunObservation {
  readonly traces: readonly Readonly<Record<string, unknown>>[];
  readonly byStage: Readonly<Record<string, readonly Readonly<Record<string, unknown>>[]>>;
  readonly contextBudget: Readonly<Record<string, unknown>>;
  readonly contextBudgetSource: "core_event" | "trace" | "none";
}

export interface StabilityTrendPolicyOptions {
  readonly minimumSampleSize?: number;
  readonly runFailureRateWarn?: number;
  readonly runFailureRateFail?: number;
  readonly stabilityFailureRateWarn?: number;
  readonly stabilityFailureRateFail?: number;
  readonly toolProtocolRunRateWarn?: number;
  readonly toolProtocolRunRateFail?: number;
  readonly incompleteToolRunRateWarn?: number;
  readonly incompleteToolRunRateFail?: number;
  readonly contextOverflowRunRateWarn?: number;
  readonly contextOverflowRunRateFail?: number;
  readonly compactionFailureRunRateWarn?: number;
  readonly compactionFailureRunRateFail?: number;
  readonly retryRunRateWarn?: number;
  readonly retryRunRateFail?: number;
  readonly failureStreakWarn?: number;
  readonly failureStreakFail?: number;
}

export interface StabilityRegressionGatePolicyOptions {
  readonly minimumWindowSize?: number;
  readonly maxRateIncrease?: number;
  readonly maxFailureStreakIncrease?: number;
  readonly failOnNewToolErrorCode?: boolean;
}
