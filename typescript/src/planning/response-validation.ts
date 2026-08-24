import type { JsonValue, Message } from "../model/types.js";
import type { ModelTaskRunner } from "../extensions/model-tasks.js";
import { copyJsonValue } from "../model/validation.js";
import { AgentCanceledError, AgentError } from "../shared/errors.js";
import type {
  ResponseJudge,
  ResponseValidationOptions,
  ResponseValidationResult,
  ResponseValidator,
} from "./types.js";

export interface ResponseRejection {
  readonly violationCodes: readonly string[];
  readonly repairGuidance: readonly string[];
  readonly recoveryCause: "response_constraint_deterministic" | "response_constraint_semantic";
}

export class ResponseValidationCoordinator {
  readonly #validators: readonly ResponseValidator[];
  readonly #judges: readonly ResponseJudge[];
  public readonly maxAttempts: number;

  public constructor(
    options: ResponseValidationOptions = {},
    modelTasks?: ModelTaskRunner,
  ) {
    this.#validators = copyValidators(options.validators);
    const factories = copyJudgeFactories(options.judgeFactories);
    if (factories.length > 0 && modelTasks === undefined) {
      throw new TypeError("response judge factories require a ModelTaskRunner");
    }
    this.#judges = copyJudges([
      ...(options.judges ?? []),
      ...factories.map((factory) => factory(modelTasks!)),
    ]);
    this.maxAttempts = options.maxAttempts ?? 3;
    if (!Number.isSafeInteger(this.maxAttempts) || this.maxAttempts < 1 || this.maxAttempts > 5) {
      throw new TypeError("response validation maxAttempts must be between 1 and 5");
    }
  }

  public get enabled(): boolean {
    return this.#validators.length > 0 || this.#judges.length > 0;
  }

  public async validate(
    content: JsonValue,
    messages: readonly Message[],
    signal?: AbortSignal,
  ): Promise<ResponseRejection | undefined> {
    const rejected: { readonly result: ResponseValidationResult; readonly source: "validator" | "judge" }[] = [];
    for (const validator of this.#validators) {
      let result: ResponseValidationResult;
      try {
        result = normalizeResult(validator.validate({ content, messages }));
      } catch (error) {
        if (error instanceof AgentError && error.code === "response_validator_contract_violation") throw error;
        throw new AgentError("response_validator_error", "Response validator failed", { cause: error });
      }
      if (result.violationCode !== undefined) rejected.push({ result, source: "validator" });
    }
    for (const judge of this.#judges) {
      throwIfCanceled(signal);
      let result: ResponseValidationResult;
      try {
        result = normalizeResult(await abortable(Promise.resolve(judge.judge({
          content,
          messages,
          ...(signal === undefined ? {} : { signal }),
        })), signal));
      } catch (error) {
        if (error instanceof AgentCanceledError) throw error;
        if (error instanceof AgentError && error.code === "response_validator_contract_violation") {
          throw new AgentError("response_judge_contract_violation", error.message, { cause: error });
        }
        if (error instanceof AgentError && error.code === "response_judge_contract_violation") {
          throw error;
        }
        throw new AgentError("response_judge_error", "Response judge failed", { cause: error });
      }
      if (result.violationCode !== undefined) rejected.push({ result, source: "judge" });
    }
    if (rejected.length === 0) return undefined;
    return Object.freeze({
      violationCodes: Object.freeze(rejected.map((item) => item.result.violationCode!)),
      repairGuidance: Object.freeze(rejected.map((item) => item.result.repairGuidance!)),
      recoveryCause: rejected.every((item) => item.source === "judge")
        ? "response_constraint_semantic"
        : "response_constraint_deterministic",
    });
  }
}

export function responseRepairMessage(rejection: ResponseRejection): Message {
  return Object.freeze({
    role: "developer",
    content: [
      "The previous candidate was withheld by host response validation.",
      ...rejection.repairGuidance.map((guidance) => `- ${guidance}`),
      "Return a corrected final response. Do not call completed tools again.",
    ].join("\n"),
    attributes: Object.freeze({
      responseRepair: true,
      violationCodes: copyJsonValue(rejection.violationCodes),
    }),
  });
}

function normalizeResult(value: ResponseValidationResult): ResponseValidationResult {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new AgentError(
      "response_validator_contract_violation",
      "Response validator returned an invalid result",
    );
  }
  const violationCode = optionalText(value.violationCode);
  const repairGuidance = optionalText(value.repairGuidance);
  if ((violationCode === undefined) !== (repairGuidance === undefined)) {
    throw new AgentError(
      "response_validator_contract_violation",
      "Response rejection requires both violationCode and repairGuidance",
    );
  }
  const details = value.details === undefined
    ? undefined
    : copyJsonValue(value.details) as Readonly<Record<string, JsonValue>>;
  return Object.freeze({
    ...(violationCode === undefined ? {} : { violationCode }),
    ...(repairGuidance === undefined ? {} : { repairGuidance }),
    ...(details === undefined ? {} : { details }),
  });
}

function copyValidators(value: readonly ResponseValidator[] | undefined): readonly ResponseValidator[] {
  if (value === undefined) return Object.freeze([]);
  if (!Array.isArray(value) || value.some((item) => typeof item?.validate !== "function")) {
    throw new TypeError("response validators must implement validate");
  }
  return Object.freeze([...value]);
}

function copyJudges(value: readonly ResponseJudge[] | undefined): readonly ResponseJudge[] {
  if (value === undefined) return Object.freeze([]);
  if (!Array.isArray(value) || value.some((item) => typeof item?.judge !== "function")) {
    throw new TypeError("response judges must implement judge");
  }
  return Object.freeze([...value]);
}

function copyJudgeFactories(
  value: ResponseValidationOptions["judgeFactories"],
): readonly ((modelTasks: ModelTaskRunner) => ResponseJudge)[] {
  if (value === undefined) return Object.freeze([]);
  if (!Array.isArray(value) || value.some((item) => typeof item !== "function")) {
    throw new TypeError("response judge factories must be functions");
  }
  return Object.freeze([...value]);
}

function optionalText(value: unknown): string | undefined {
  if (value === undefined) return undefined;
  if (typeof value !== "string" || value.trim() === "") {
    throw new AgentError("response_validator_contract_violation", "Response validation text is invalid");
  }
  return value.trim();
}

async function abortable<T>(promise: Promise<T>, signal?: AbortSignal): Promise<T> {
  if (signal === undefined) return promise;
  if (signal.aborted) throw new AgentCanceledError();
  let cancel: (() => void) | undefined;
  const canceled = new Promise<never>((_resolve, reject) => {
    cancel = () => reject(new AgentCanceledError());
    signal.addEventListener("abort", cancel, { once: true });
  });
  try {
    return await Promise.race([promise, canceled]);
  } finally {
    if (cancel !== undefined) signal.removeEventListener("abort", cancel);
    void promise.catch(() => undefined);
  }
}

function throwIfCanceled(signal?: AbortSignal): void {
  if (signal?.aborted === true) throw new AgentCanceledError();
}
