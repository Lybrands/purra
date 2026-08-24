import { mapping, nonNegativeInteger, roundRate, text } from "./observation.js";
import type {
  DiagnosticCheck,
  StabilityRegressionGatePolicyOptions,
  StabilityTrendPolicyOptions,
} from "./types.js";

const RATE_METRICS = [
  "runFailureRate", "stabilityFailureRate", "toolProtocolRunRate", "incompleteToolRunRate",
  "contextOverflowRunRate", "compactionFailureRunRate", "retryRunRate",
] as const;
const CRITICAL_METRICS = [
  "failedRuns", "stabilityFailedRuns", "toolProtocolFailureRuns", "incompleteToolRuns",
  "contextOverflowRuns", "compactionFailureRuns",
] as const;

export class StabilityTrendPolicy {
  public readonly minimumSampleSize: number;
  public readonly runFailureRateWarn: number;
  public readonly runFailureRateFail: number;
  public readonly stabilityFailureRateWarn: number;
  public readonly stabilityFailureRateFail: number;
  public readonly toolProtocolRunRateWarn: number;
  public readonly toolProtocolRunRateFail: number;
  public readonly incompleteToolRunRateWarn: number;
  public readonly incompleteToolRunRateFail: number;
  public readonly contextOverflowRunRateWarn: number;
  public readonly contextOverflowRunRateFail: number;
  public readonly compactionFailureRunRateWarn: number;
  public readonly compactionFailureRunRateFail: number;
  public readonly retryRunRateWarn: number;
  public readonly retryRunRateFail: number;
  public readonly failureStreakWarn: number;
  public readonly failureStreakFail: number;

  public constructor(options: StabilityTrendPolicyOptions = {}) {
    this.minimumSampleSize = positive(options.minimumSampleSize ?? 5, "minimumSampleSize");
    this.runFailureRateWarn = rate(options.runFailureRateWarn ?? 0.10, "runFailureRateWarn");
    this.runFailureRateFail = rate(options.runFailureRateFail ?? 0.25, "runFailureRateFail");
    this.stabilityFailureRateWarn = rate(options.stabilityFailureRateWarn ?? 0.10, "stabilityFailureRateWarn");
    this.stabilityFailureRateFail = rate(options.stabilityFailureRateFail ?? 0.25, "stabilityFailureRateFail");
    this.toolProtocolRunRateWarn = rate(options.toolProtocolRunRateWarn ?? 0.05, "toolProtocolRunRateWarn");
    this.toolProtocolRunRateFail = rate(options.toolProtocolRunRateFail ?? 0.15, "toolProtocolRunRateFail");
    this.incompleteToolRunRateWarn = rate(options.incompleteToolRunRateWarn ?? 0.02, "incompleteToolRunRateWarn");
    this.incompleteToolRunRateFail = rate(options.incompleteToolRunRateFail ?? 0.10, "incompleteToolRunRateFail");
    this.contextOverflowRunRateWarn = rate(options.contextOverflowRunRateWarn ?? 0.05, "contextOverflowRunRateWarn");
    this.contextOverflowRunRateFail = rate(options.contextOverflowRunRateFail ?? 0.15, "contextOverflowRunRateFail");
    this.compactionFailureRunRateWarn = rate(options.compactionFailureRunRateWarn ?? 0.05, "compactionFailureRunRateWarn");
    this.compactionFailureRunRateFail = rate(options.compactionFailureRunRateFail ?? 0.15, "compactionFailureRunRateFail");
    this.retryRunRateWarn = rate(options.retryRunRateWarn ?? 0.20, "retryRunRateWarn");
    this.retryRunRateFail = rate(options.retryRunRateFail ?? 0.50, "retryRunRateFail");
    this.failureStreakWarn = positive(options.failureStreakWarn ?? 2, "failureStreakWarn");
    this.failureStreakFail = positive(options.failureStreakFail ?? 3, "failureStreakFail");
    for (const [warn, fail, label] of [
      [this.runFailureRateWarn, this.runFailureRateFail, "runFailureRate"],
      [this.stabilityFailureRateWarn, this.stabilityFailureRateFail, "stabilityFailureRate"],
      [this.toolProtocolRunRateWarn, this.toolProtocolRunRateFail, "toolProtocolRunRate"],
      [this.incompleteToolRunRateWarn, this.incompleteToolRunRateFail, "incompleteToolRunRate"],
      [this.contextOverflowRunRateWarn, this.contextOverflowRunRateFail, "contextOverflowRunRate"],
      [this.compactionFailureRunRateWarn, this.compactionFailureRunRateFail, "compactionFailureRunRate"],
      [this.retryRunRateWarn, this.retryRunRateFail, "retryRunRate"],
    ] as const) if (warn >= fail) throw new TypeError(`${label} thresholds must satisfy warn < fail`);
    if (this.failureStreakFail <= this.failureStreakWarn) {
      throw new TypeError("failureStreakFail must exceed failureStreakWarn");
    }
    Object.freeze(this);
  }
}

export class StabilityRegressionGatePolicy {
  public readonly minimumWindowSize: number;
  public readonly maxRateIncrease: number;
  public readonly maxFailureStreakIncrease: number;
  public readonly failOnNewToolErrorCode: boolean;

  public constructor(options: StabilityRegressionGatePolicyOptions = {}) {
    this.minimumWindowSize = positive(options.minimumWindowSize ?? 5, "minimumWindowSize");
    this.maxRateIncrease = rate(options.maxRateIncrease ?? 0.05, "maxRateIncrease");
    this.maxFailureStreakIncrease = nonNegative(options.maxFailureStreakIncrease ?? 1, "maxFailureStreakIncrease");
    this.failOnNewToolErrorCode = options.failOnNewToolErrorCode ?? true;
    Object.freeze(this);
  }
}

export const DEFAULT_STABILITY_TREND_POLICY = new StabilityTrendPolicy();
export const DEFAULT_STABILITY_REGRESSION_GATE_POLICY = new StabilityRegressionGatePolicy();

export function evaluateAgentRunStabilityTrend(
  recentRuns: Iterable<Readonly<Record<string, unknown>>>,
  policy: StabilityTrendPolicy = DEFAULT_STABILITY_TREND_POLICY,
) {
  const samples = [...recentRuns].map(sampleProjection);
  const enough = samples.length >= policy.minimumSampleSize;
  const failedRuns = samples.filter((sample) => sample.runStatus === "blocked" || sample.runStatus === "failed").length;
  const stabilityFailures = samples.filter((sample) => sample.stabilityVerdict === "fail").length;
  const toolSamples = samples.filter((sample) => sample.metrics.toolCalls > 0);
  const modelSamples = samples.filter((sample) => sample.metrics.modelAttempts > 0);
  const protocolFailures = toolSamples.filter((sample) => sample.metrics.toolProtocolFailures > 0).length;
  const incompleteTools = toolSamples.filter((sample) => sample.metrics.incompleteToolCalls > 0).length;
  const contextOverflows = samples.filter((sample) => sample.metrics.contextOverflows > 0).length;
  const compactionFailures = samples.filter((sample) => sample.metrics.compactionFailures > 0).length;
  const retryRuns = modelSamples.filter((sample) => sample.metrics.retryAttempts > 0).length;
  const failureStreak = currentFailureStreak(samples);
  const checks = [
    rateCheck("runFailureRate", failedRuns, samples.length, policy.runFailureRateWarn, policy.runFailureRateFail, enough),
    rateCheck("stabilityFailureRate", stabilityFailures, samples.length, policy.stabilityFailureRateWarn, policy.stabilityFailureRateFail, enough),
    rateCheck("toolProtocolRunRate", protocolFailures, toolSamples.length, policy.toolProtocolRunRateWarn, policy.toolProtocolRunRateFail, enough),
    rateCheck("incompleteToolRunRate", incompleteTools, toolSamples.length, policy.incompleteToolRunRateWarn, policy.incompleteToolRunRateFail, enough),
    rateCheck("contextOverflowRunRate", contextOverflows, samples.length, policy.contextOverflowRunRateWarn, policy.contextOverflowRunRateFail, enough),
    rateCheck("compactionFailureRunRate", compactionFailures, samples.length, policy.compactionFailureRunRateWarn, policy.compactionFailureRunRateFail, enough),
    rateCheck("retryRunRate", retryRuns, modelSamples.length, policy.retryRunRateWarn, policy.retryRunRateFail, enough),
    Object.freeze({
      name: "failureStreak",
      status: failureStreak >= policy.failureStreakFail ? "fail" : failureStreak >= policy.failureStreakWarn ? "warn" : "pass",
      value: failureStreak,
      numerator: failureStreak,
      denominator: null,
      warnAt: policy.failureStreakWarn,
      failAt: policy.failureStreakFail,
    } satisfies DiagnosticCheck),
  ];
  const toolErrorCodes: Record<string, number> = {};
  for (const sample of samples) for (const [code, count] of Object.entries(sample.metrics.toolErrorCodes)) {
    toolErrorCodes[code] = (toolErrorCodes[code] ?? 0) + count;
  }
  const verdict = checks.some((item) => item.status === "fail")
    ? "fail" : checks.some((item) => item.status === "warn") ? "warn" : enough ? "pass" : "insufficient_data";
  return Object.freeze({
    verdict,
    sampleSize: samples.length,
    minimumSampleSize: policy.minimumSampleSize,
    metrics: Object.freeze({
      failedRuns,
      runFailureRate: roundRate(failedRuns, samples.length),
      stabilityFailedRuns: stabilityFailures,
      stabilityFailureRate: roundRate(stabilityFailures, samples.length),
      toolRuns: toolSamples.length,
      toolProtocolFailureRuns: protocolFailures,
      toolProtocolRunRate: roundRate(protocolFailures, toolSamples.length),
      incompleteToolRuns: incompleteTools,
      incompleteToolRunRate: roundRate(incompleteTools, toolSamples.length),
      contextOverflowRuns: contextOverflows,
      contextOverflowRunRate: roundRate(contextOverflows, samples.length),
      compactionFailureRuns: compactionFailures,
      compactionFailureRunRate: roundRate(compactionFailures, samples.length),
      modelRuns: modelSamples.length,
      retryRuns,
      retryRunRate: roundRate(retryRuns, modelSamples.length),
      currentFailureStreak: failureStreak,
      toolErrorCodes: Object.freeze(sortedRecord(toolErrorCodes)),
      topToolErrorCodes: Object.freeze(Object.entries(toolErrorCodes)
        .sort(([leftCode, left], [rightCode, right]) => right - left || leftCode.localeCompare(rightCode))
        .slice(0, 5)
        .map(([code, count]) => Object.freeze({ code, count }))),
    }),
    checks: Object.freeze(checks),
    alerts: Object.freeze(checks
      .filter((item) => item.status === "warn" || item.status === "fail")
      .map((item) => Object.freeze({
        code: item.name,
        severity: item.status,
        value: item.value ?? null,
        threshold: item.status === "fail" ? item.failAt : item.warnAt,
      }))),
    recentRuns: Object.freeze(samples.map((sample) => Object.freeze({
      runId: sample.runId,
      runStatus: sample.runStatus,
      stabilityVerdict: sample.stabilityVerdict,
      createTime: sample.createTime,
    }))),
  });
}

export function evaluateStabilityRegressionGate(
  candidate: Readonly<Record<string, unknown>>,
  baseline: Readonly<Record<string, unknown>>,
  policy: StabilityRegressionGatePolicy = DEFAULT_STABILITY_REGRESSION_GATE_POLICY,
) {
  const candidateSize = nonNegativeInteger(candidate.sampleSize);
  const baselineSize = nonNegativeInteger(baseline.sampleSize);
  const candidateMetrics = mapping(candidate.metrics);
  const baselineMetrics = mapping(baseline.metrics);
  const comparable = candidateSize >= policy.minimumWindowSize && baselineSize >= policy.minimumWindowSize;
  const candidateVerdict = text(candidate.verdict) || "insufficient_data";
  const checks: Record<string, unknown>[] = [Object.freeze({
    name: "candidateHealth",
    status: candidateVerdict === "fail" ? "fail" : candidateVerdict === "warn" ? "warn" : candidateVerdict === "pass" ? "pass" : "insufficient_data",
    detail: Object.freeze({ verdict: candidateVerdict }),
  })];
  const metricDeltas: Record<string, number | null> = {};
  for (const metricName of RATE_METRICS) {
    const candidateValue = optionalRate(candidateMetrics[metricName]);
    const baselineValue = optionalRate(baselineMetrics[metricName]);
    const delta = candidateValue === undefined || baselineValue === undefined
      ? null : Math.round((candidateValue - baselineValue) * 10_000) / 10_000;
    metricDeltas[metricName] = delta;
    checks.push(Object.freeze({
      name: `rateRegression:${metricName}`,
      status: !comparable ? "insufficient_data" : delta === null ? "not_applicable" : delta > policy.maxRateIncrease ? "fail" : delta > 0 ? "warn" : "pass",
      detail: Object.freeze({ baseline: baselineValue ?? null, candidate: candidateValue ?? null, delta, maxIncrease: policy.maxRateIncrease }),
    }));
  }
  const candidateStreak = nonNegativeInteger(candidateMetrics.currentFailureStreak);
  const baselineStreak = nonNegativeInteger(baselineMetrics.currentFailureStreak);
  const streakDelta = candidateStreak - baselineStreak;
  checks.push(Object.freeze({
    name: "failureStreakRegression",
    status: baselineSize < policy.minimumWindowSize ? "insufficient_data" : streakDelta > policy.maxFailureStreakIncrease ? "fail" : streakDelta > 0 ? "warn" : "pass",
    detail: Object.freeze({ baseline: baselineStreak, candidate: candidateStreak, delta: streakDelta, maxIncrease: policy.maxFailureStreakIncrease }),
  }));
  const candidateCodes = new Set(Object.keys(mapping(candidateMetrics.toolErrorCodes)));
  const baselineCodes = new Set(Object.keys(mapping(baselineMetrics.toolErrorCodes)));
  const newToolErrorCodes = [...candidateCodes].filter((code) => !baselineCodes.has(code)).sort();
  checks.push(Object.freeze({
    name: "newToolErrorCodes",
    status: baselineSize < policy.minimumWindowSize ? "insufficient_data" : newToolErrorCodes.length > 0 && policy.failOnNewToolErrorCode ? "fail" : newToolErrorCodes.length > 0 ? "warn" : "pass",
    detail: Object.freeze({ codes: Object.freeze(newToolErrorCodes) }),
  }));
  const newCriticalSignals = CRITICAL_METRICS.filter((name) => (
    nonNegativeInteger(baselineMetrics[name]) === 0 && nonNegativeInteger(candidateMetrics[name]) > 0
  ));
  checks.push(Object.freeze({
    name: "newCriticalSignals",
    status: baselineSize < policy.minimumWindowSize ? "insufficient_data" : newCriticalSignals.length > 0 ? "fail" : "pass",
    detail: Object.freeze({ metrics: Object.freeze(newCriticalSignals) }),
  }));
  const verdict = checks.some((item) => item.status === "fail")
    ? "fail" : checks.some((item) => item.status === "warn") ? "warn" : comparable ? "pass" : "insufficient_data";
  return Object.freeze({
    verdict,
    candidateSampleSize: candidateSize,
    baselineSampleSize: baselineSize,
    minimumWindowSize: policy.minimumWindowSize,
    metricDeltas: Object.freeze(metricDeltas),
    newToolErrorCodes: Object.freeze(newToolErrorCodes),
    newCriticalSignals: Object.freeze(newCriticalSignals),
    checks: Object.freeze(checks),
    alerts: Object.freeze(checks
      .filter((item) => item.status === "warn" || item.status === "fail")
      .map((item) => Object.freeze({ code: item.name, severity: item.status, detail: item.detail }))),
  });
}

interface TrendSample {
  readonly runId: string;
  readonly runStatus: string;
  readonly stabilityVerdict: string;
  readonly createTime: unknown;
  readonly metrics: {
    readonly toolCalls: number;
    readonly toolProtocolFailures: number;
    readonly incompleteToolCalls: number;
    readonly contextOverflows: number;
    readonly compactionFailures: number;
    readonly modelAttempts: number;
    readonly retryAttempts: number;
    readonly toolErrorCodes: Readonly<Record<string, number>>;
  };
}

function sampleProjection(value: Readonly<Record<string, unknown>>): TrendSample {
  const stability = mapping(value.stability);
  const metrics = mapping(stability.metrics);
  const toolErrorCodes: Record<string, number> = {};
  for (const [code, count] of Object.entries(mapping(metrics.toolErrorCodes))) {
    const normalized = nonNegativeInteger(count);
    if (code.trim() !== "" && normalized > 0) toolErrorCodes[code] = normalized;
  }
  return Object.freeze({
    runId: text(value.runId),
    runStatus: text(value.runStatus) || "unknown",
    stabilityVerdict: text(stability.verdict) || "pass",
    createTime: value.createTime,
    metrics: Object.freeze({
      toolCalls: nonNegativeInteger(metrics.toolCalls),
      toolProtocolFailures: nonNegativeInteger(metrics.toolProtocolFailures),
      incompleteToolCalls: nonNegativeInteger(metrics.incompleteToolCalls),
      contextOverflows: nonNegativeInteger(metrics.contextOverflows),
      compactionFailures: nonNegativeInteger(metrics.compactionFailures),
      modelAttempts: nonNegativeInteger(metrics.modelAttempts),
      retryAttempts: nonNegativeInteger(metrics.retryAttempts),
      toolErrorCodes: Object.freeze(toolErrorCodes),
    }),
  });
}

function currentFailureStreak(samples: readonly TrendSample[]): number {
  let count = 0;
  for (const sample of samples) {
    if (sample.runStatus !== "blocked" && sample.runStatus !== "failed" && sample.stabilityVerdict !== "fail") break;
    count += 1;
  }
  return count;
}

function rateCheck(
  name: string,
  numerator: number,
  denominator: number,
  warnAt: number,
  failAt: number,
  enough: boolean,
): DiagnosticCheck {
  const value = roundRate(numerator, denominator);
  return Object.freeze({
    name,
    status: !enough ? "insufficient_data" : value === null ? "not_applicable" : value >= failAt ? "fail" : value >= warnAt ? "warn" : "pass",
    value,
    numerator,
    denominator,
    warnAt,
    failAt,
  });
}

function optionalRate(value: unknown): number | undefined {
  if (value === null || value === undefined || typeof value === "boolean") return undefined;
  const normalized = Number(value);
  return Number.isFinite(normalized) && normalized >= 0 && normalized <= 1 ? normalized : undefined;
}

function positive(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 1) throw new TypeError(`${label} must be positive`);
  return value;
}

function nonNegative(value: number, label: string): number {
  if (!Number.isSafeInteger(value) || value < 0) throw new TypeError(`${label} must be non-negative`);
  return value;
}

function rate(value: number, label: string): number {
  if (!Number.isFinite(value) || value < 0 || value > 1) throw new TypeError(`${label} must be between zero and one`);
  return value;
}

function sortedRecord<T>(value: Readonly<Record<string, T>>): Record<string, T> {
  return Object.fromEntries(Object.entries(value).sort(([left], [right]) => left.localeCompare(right)));
}
