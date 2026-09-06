import type { JsonValue } from "./model/types.js";
import { copyJsonValue } from "./model/validation.js";
import { AgentError } from "./shared/errors.js";
import { findSchemaViolation, inspectSchemaNode, type JsonSchema } from "./shared/schema.js";

export const OUTPUT_SCHEMA_PROFILE = "purra.output-schema/v1" as const;
export const JSON_IDENTITY_PROFILE = "purra.json-identity/v1" as const;

export interface StructuredOutputLimits {
  readonly schemaBytes: number;
  readonly outputBytes: number;
  readonly schemaDepth: number;
  readonly outputDepth: number;
  readonly schemaNodes: number;
  readonly outputNodes: number;
  readonly validationSteps: number;
}

const DEFAULT_LIMITS: StructuredOutputLimits = Object.freeze({
  schemaBytes: 65_536, outputBytes: 1_048_576,
  schemaDepth: 32, outputDepth: 64, schemaNodes: 4_096,
  outputNodes: 65_536, validationSteps: 100_000,
});

export interface StructuredOutputOptions {
  readonly schemaId: string;
  readonly schemaVersion: string;
  readonly schema: JsonSchema;
  readonly mode?: "local" | "native_required";
  readonly schemaProfile?: typeof OUTPUT_SCHEMA_PROFILE;
  readonly limits?: Partial<StructuredOutputLimits>;
}

export class StructuredOutputError extends AgentError {
  public readonly details: Readonly<{ path: string; keyword: string; reason: string }>;
  public constructor(code: string, keyword: string, path = "$") {
    super(code, "Structured output contract validation failed");
    this.name = "StructuredOutputError";
    this.details = Object.freeze({ path: encoder.encode(path).length <= 512 ? path : "$", keyword, reason: "constraint_failed" });
  }
}

const encoder = new TextEncoder();
function validString(value: string): void {
  // TextEncoder replaces lone surrogates; reject them before measuring/hashing.
  for (const char of value) {
    const point = char.codePointAt(0)!;
    if (point >= 0xd800 && point <= 0xdfff) throw new Error("invalid Unicode scalar");
  }
}

function snapshot(value: unknown, depth: number, nodes: number, byteLimit: number): JsonValue {
  let remaining = nodes;
  let bytes = 0;
  const active = new Set<object>();
  function visit(item: unknown, level: number): JsonValue {
    if (--remaining < 0 || level > depth) throw new Error("JSON resource limit exceeded");
    let result: JsonValue;
    if (typeof item === "string") {
      if (item.length > byteLimit) throw new Error("JSON byte limit exceeded");
      validString(item);
      bytes += encoder.encode(JSON.stringify(item)).length;
      result = item;
    } else if (item === null || typeof item === "boolean") {
      bytes += 5;
      result = item;
    } else if (typeof item === "number") {
      if (!Number.isFinite(item) || (Number.isInteger(item) && !Number.isSafeInteger(item))) {
        throw new Error("number outside interoperable range");
      }
      bytes += 24;
      result = item === 0 ? 0 : item;
    } else if (typeof item === "object" && item !== null) {
      if (active.has(item)) throw new Error("cyclic JSON");
      if (!Array.isArray(item) && ![Object.prototype, null].includes(Object.getPrototypeOf(item))) {
        throw new Error("unsupported JSON object");
      }
      active.add(item);
      try {
        if (Array.isArray(item)) {
          result = Array.from(item, (child) => visit(child, level + 1));
        } else {
          result = Object.fromEntries(Object.entries(item).map(([key, child]) => {
            visit(key, level + 1);
            return [key, visit(child, level + 1)];
          }));
        }
        bytes += 2 + 2 * Object.keys(item).length;
      } finally { active.delete(item); }
    } else throw new Error("unsupported JSON value");
    if (bytes > byteLimit) throw new Error("JSON byte limit exceeded");
    return result;
  }
  const result = visit(value, 0);
  return result;
}

function hex(data: Uint8Array): string {
  return [...data].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

function identity(value: JsonValue): string {
  if (value === null) return "n";
  if (typeof value === "boolean") return value ? "t" : "f";
  if (typeof value === "number") {
    const buffer = new ArrayBuffer(8);
    new DataView(buffer).setFloat64(0, value === 0 ? 0 : value, false);
    return "d" + hex(new Uint8Array(buffer));
  }
  if (typeof value === "string") {
    const data = encoder.encode(value);
    return `s${data.length}:${hex(data)}`;
  }
  if (Array.isArray(value)) return `a${value.length}:` + value.map(identity).join("");
  const mapping = value as JsonSchema;
  const keys = Object.keys(mapping).sort((a, b) => {
    const left = hex(encoder.encode(a)), right = hex(encoder.encode(b));
    return left < right ? -1 : left > right ? 1 : 0;
  });
  return `o${keys.length}:` + keys.map((key) => identity(key) + identity(mapping[key]!)).join("");
}

async function digest(value: JsonValue): Promise<string> {
  return hex(new Uint8Array(await globalThis.crypto.subtle.digest(
    "SHA-256", encoder.encode(JSON_IDENTITY_PROFILE + "\n" + identity(value)),
  )));
}

function parseDocument(text: string, limits: StructuredOutputLimits): JsonValue {
  if (typeof text !== "string" || text.length > limits.outputBytes
    || encoder.encode(text).length > limits.outputBytes) throw new Error("invalid document");
  let offset = 0;
  let nodes = 0;
  function white(): void { while (/[\x20\t\r\n]/.test(text[offset] ?? "!") && offset < text.length) offset++; }
  function string(): string {
    const start = offset++;
    while (offset < text.length) {
      const char = text[offset++];
      if (char === "\\") offset++;
      else if (char === '"') {
        const result = JSON.parse(text.slice(start, offset)) as string;
        validString(result);
        return result;
      }
    }
    throw new Error("unterminated string");
  }
  function value(level: number): JsonValue {
    if (++nodes > limits.outputNodes || level > limits.outputDepth) throw new Error("resource limit");
    white();
    const char = text[offset];
    if (char === '"') return string();
    if (char === "{" || char === "[") {
      offset++;
      const object = char === "{";
      const close = object ? "}" : "]";
      const entries: [string, JsonValue][] = [];
      const array: JsonValue[] = [];
      const keys = new Set<string>();
      white();
      if (text[offset] === close) { offset++; return object ? {} : []; }
      while (true) {
        white();
        if (object) {
          if (text[offset] !== '"' || ++nodes > limits.outputNodes) throw new Error("invalid key");
          const key = string();
          if (keys.has(key)) throw new Error("duplicate key");
          keys.add(key);
          white();
          if (text[offset++] !== ":") throw new Error("missing colon");
          entries.push([key, value(level + 1)]);
        } else array.push(value(level + 1));
        white();
        const delimiter = text[offset++];
        if (delimiter === close) break;
        if (delimiter !== ",") throw new Error("missing delimiter");
      }
      return object ? Object.fromEntries(entries) : array;
    }
    const token = /^(?:true|false|null|-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)/.exec(text.slice(offset))?.[0];
    if (token === undefined) throw new Error("invalid token");
    offset += token.length;
    return JSON.parse(token) as JsonValue;
  }
  const result = value(0);
  white();
  if (offset !== text.length) throw new Error("trailing content");
  return snapshot(result, limits.outputDepth, limits.outputNodes, limits.outputBytes);
}

/** Immutable output schema identity. Construction performs no external calls. */
export class StructuredOutputContract {
  public readonly schemaId: string;
  public readonly schemaVersion: string;
  public readonly schemaProfile = OUTPUT_SCHEMA_PROFILE;
  public readonly schema: JsonSchema;
  public readonly mode: "local" | "native_required";
  public readonly limits: StructuredOutputLimits;
  public readonly schemaDigest: string;
  public readonly contractDigest: string;

  private constructor(options: StructuredOutputOptions, schema: JsonSchema,
    limits: StructuredOutputLimits, schemaDigest: string, contractDigest: string) {
    this.schemaId = options.schemaId;
    this.schemaVersion = options.schemaVersion;
    this.schema = schema;
    this.mode = options.mode ?? "local";
    this.limits = limits;
    this.schemaDigest = schemaDigest;
    this.contractDigest = contractDigest;
    Object.freeze(this);
  }

  public static async create(options: StructuredOutputOptions): Promise<StructuredOutputContract> {
    let schema: JsonSchema;
    let limits: StructuredOutputLimits;
    // Capture all caller-controlled fields before the first await.
    const input = { ...options };
    try {
      for (const item of [input.schemaId, input.schemaVersion]) {
        if (typeof item !== "string" || item.length > 128 || !item.trim() || encoder.encode(item).length > 128) throw new Error("identity");
        validString(item);
      }
      if (input.schemaProfile !== undefined && input.schemaProfile !== OUTPUT_SCHEMA_PROFILE) throw new Error("profile");
      limits = Object.freeze({ ...DEFAULT_LIMITS, ...input.limits });
      if (Object.keys(limits).some((key) => !Object.hasOwn(DEFAULT_LIMITS, key))) throw new Error("limits");
      for (const key of Object.keys(DEFAULT_LIMITS) as (keyof StructuredOutputLimits)[]) {
        if (!Number.isSafeInteger(limits[key]) || limits[key] < 1 || limits[key] > DEFAULT_LIMITS[key]) throw new Error("limits");
      }
      const value = snapshot(input.schema, limits.schemaDepth, limits.schemaNodes, limits.schemaBytes);
      if (value === null || typeof value !== "object" || Array.isArray(value) || (value as JsonSchema).type !== "object") throw new Error("root");
      inspectSchemaNode(value, "$", "structured-output");
      schema = copyJsonValue(value) as JsonSchema;
    } catch {
      throw new StructuredOutputError("structured_output_schema_invalid", "schema");
    }
    if (input.mode !== undefined && input.mode !== "local" && input.mode !== "native_required") {
      throw new StructuredOutputError("structured_output_mode_unsupported", "mode");
    }
    const schemaDigest = await digest(schema);
    const contractDigest = await digest({ schemaId: input.schemaId, schemaVersion: input.schemaVersion,
      schemaProfile: OUTPUT_SCHEMA_PROFILE, schemaDigest, mode: input.mode ?? "local",
      limits: Object.keys(DEFAULT_LIMITS).map((key) => limits[key as keyof StructuredOutputLimits]),
    });
    return new StructuredOutputContract(input, schema, limits, schemaDigest, contractDigest);
  }

  public identity(): Readonly<Record<string, JsonValue>> {
    return Object.freeze({ schemaId: this.schemaId, schemaVersion: this.schemaVersion,
      schemaProfile: this.schemaProfile, schemaDigest: this.schemaDigest,
      contractDigest: this.contractDigest, mode: this.mode });
  }

  public instruction(): string {
    const header = "Return exactly one complete JSON object. Do not use tools, markdown fences, or extra text.";
    return header + (this.mode === "local" ? " JSON Schema: " + JSON.stringify(this.schema)
      : " Follow the native JSON Schema output format.");
  }

  public parse(text: string): JsonSchema {
    let value: JsonValue;
    try { value = parseDocument(text, this.limits); }
    catch { throw new StructuredOutputError("structured_output_invalid_json", "document"); }
    return this.validateValue(value);
  }

  public validateValue(input: unknown): JsonSchema {
    let value: JsonValue;
    try { value = snapshot(input, this.limits.outputDepth, this.limits.outputNodes, this.limits.outputBytes); }
    catch { throw new StructuredOutputError("structured_output_invalid_json", "value"); }
    let violation;
    try {
      violation = findSchemaViolation(value, this.schema, "$", true, { remaining: this.limits.validationSteps });
    } catch { throw new StructuredOutputError("structured_output_validation_limit_exceeded", "validation_steps"); }
    if (violation !== undefined) {
      throw new StructuredOutputError("structured_output_schema_mismatch", violation.keyword,
        violation.keyword === "additionalProperties" ? "$" : violation.path);
    }
    return copyJsonValue(value) as JsonSchema;
  }
}


/** Hash bounded JSON with the same numeric and Unicode identity as schemas. */
export async function jsonIdentityDigest(value: unknown): Promise<string> {
  return digest(snapshot(value, DEFAULT_LIMITS.outputDepth, DEFAULT_LIMITS.outputNodes, DEFAULT_LIMITS.outputBytes));
}
