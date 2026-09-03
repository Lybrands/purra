import type { JsonValue } from "../model/types.js";
import { copyJsonValue } from "../model/validation.js";
import { AgentError } from "../shared/errors.js";

export const PLANNING_STREAM_SCHEMA = "purra.planning-stream/v1";
export const PLANNING_STREAM_INSTRUCTION = `Use purra.planning-stream/v1: UTF-8 JSON Lines,
one compact JSON object per record. Terminate every progress record with LF.
Emit zero to sixteen {"v":1,"type":"progress","text":"short user-facing intent"}
records, then exactly one {"v":1,"type":"plan","plan":<the required plan object>}.
No other keys, record types, Markdown, or text outside records. Each progress text
has at most 280 Unicode characters, no control characters, and is in the user's
language. Write brief scope/action intentions when useful, preferably before the
plan. Never expose reasoning, private JSON, prompts, or internal identifiers.
Do not claim planned work has already been performed. Progress is not execution
evidence. The final plan uses the existing plan schema and may end with LF or
normal stream EOF; nothing follows it. Each record is at most 262144 UTF-8 bytes
(including LF when present); total at most 1048576 bytes.`;

export interface PlanningScope {
  readonly runId: string;
  readonly operationId: string;
  readonly revision: number;
}

export interface PlanningProgress {
  readonly text: string;
  readonly recordIndex: number;
  readonly sourceStart: number;
  readonly sourceEnd: number;
}

export class PlanningStreamParser {
  #buffer = "";
  #consumed = 0;
  #records = 0;
  #rejectedProgressRecords = 0;
  #plan: Readonly<Record<string, JsonValue>> | undefined;
  #closed = false;
  #failed = false;

  public feed(text: string): readonly PlanningProgress[] {
    if (this.#closed || this.#failed) throw invalid("parser is closed");
    try {
      this.#buffer += text;
      if (this.#consumed + byteLength(this.#buffer) > 1_048_576) throw invalid("total byte limit");
      const progress: PlanningProgress[] = [];
      let boundary: number;
      while ((boundary = this.#buffer.indexOf("\n")) >= 0) {
        const line = this.#buffer.slice(0, boundary);
        this.#buffer = this.#buffer.slice(boundary + 1);
        const size = byteLength(`${line}\n`);
        const record = this.#acceptRecord(line, size);
        if (record !== undefined) progress.push(record);
      }
      if (byteLength(this.#buffer) > 262_144) throw invalid("record byte limit");
      return Object.freeze(progress);
    } catch (error) {
      this.#failed = true;
      this.#buffer = "";
      if (error instanceof AgentError && error.code === "invalid_planning_stream") throw error;
      throw invalid("invalid JSON value or Unicode");
    }
  }

  public get planReceived(): boolean { return this.#plan !== undefined; }

  public get rejectedProgressRecords(): number { return this.#rejectedProgressRecords; }

  public finish(): Readonly<Record<string, JsonValue>> {
    if (this.#closed || this.#failed) {
      this.#failed = true;
      throw invalid("missing final plan or unterminated record");
    }
    try {
      if (this.#buffer !== "") {
        const line = this.#buffer;
        this.#buffer = "";
        if (this.#acceptRecord(line, byteLength(line)) !== undefined) {
          throw invalid("unterminated progress record");
        }
      }
      if (this.#plan === undefined) throw invalid("missing final plan");
      this.#closed = true;
      return this.#plan;
    } catch (error) {
      this.#failed = true;
      this.#buffer = "";
      if (error instanceof AgentError && error.code === "invalid_planning_stream") throw error;
      throw invalid("invalid JSON value or Unicode");
    }
  }

  #acceptRecord(line: string, size: number): PlanningProgress | undefined {
    if (size > 262_144) throw invalid("record byte limit");
    if (this.#plan !== undefined) throw invalid("record after final plan");
    let row: Record<string, unknown>;
    try { row = JSON.parse(line); } catch { throw invalid("invalid JSON record"); }
    if (row === null || typeof row !== "object" || Array.isArray(row) || row.v !== 1) {
      throw invalid("unsupported version or envelope");
    }
    this.#records += 1;
    const keys = Object.keys(row).sort().join(",");
    let progress: PlanningProgress | undefined;
    if (row.type === "progress" && keys === "text,type,v") {
      if (this.#records > 16) throw invalid("progress record limit");
      const value = row.text;
      if (typeof value !== "string" || value.trim() === "" || [...value].length > 280
        || /[\u0000-\u001f\ud800-\udfff]/u.test(value)) {
        this.#rejectedProgressRecords += 1;
      } else {
        progress = Object.freeze({ text: value, recordIndex: this.#records,
          sourceStart: this.#consumed, sourceEnd: this.#consumed + size });
      }
    } else if (row.type === "plan" && keys === "plan,type,v") {
      if (row.plan === null || typeof row.plan !== "object" || Array.isArray(row.plan)) {
        throw invalid("plan must be an object");
      }
      this.#plan = copyJsonValue(row.plan) as Readonly<Record<string, JsonValue>>;
    } else throw invalid("unknown record type or fields");
    this.#consumed += size;
    return progress;
  }
}

function byteLength(text: string): number { return new TextEncoder().encode(text).byteLength; }
function invalid(reason: string): AgentError {
  return new AgentError("invalid_planning_stream", `Invalid planning stream: ${reason}`);
}
