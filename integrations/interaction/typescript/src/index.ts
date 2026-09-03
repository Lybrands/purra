import { createHash } from "node:crypto";
import { UserInputRequired, type Agent, type AgentExecutionCheckpoint, type RunRequest, type RunOptions, type RunLeaseClaim, type ToolDefinition, type RunHandle } from "purra";
import type { SqliteAgentAdapters } from "purra-sqlite";
import { randomUUID } from "node:crypto";
import { DatabaseSync } from "node:sqlite";
import type { JsonValue, TaskAdmissionDecision } from "purra";

export interface InputQuestion { readonly id: string; readonly prompt: string; readonly choices?: readonly string[]; readonly allowFreeform?: boolean }
export interface ClarificationInput {
  readonly key: string; readonly questions: readonly InputQuestion[]; readonly checkpoint: JsonValue;
  readonly sourceRunId?: string | null; readonly expiresAtMs?: number | null;
}
export interface ClarificationSnapshot {
  readonly id: string; readonly questions: readonly Required<InputQuestion>[]; readonly checkpoint: JsonValue;
  readonly sourceRunId: string | null; readonly expiresAtMs: number | null;
  readonly state: "waiting" | "ready" | "resuming" | "resumed" | "canceled";
  readonly revision: number; readonly answers: Readonly<Record<string, string>> | null;
  readonly runId: string | null; readonly resumeToken: string | null;
}
function text(value: unknown, limit = 512): string {
  if (typeof value !== "string" || !value.trim() || [...value].length > limit) throw new TypeError("Expected bounded non-empty text");
  return value;
}
function encode(value: unknown): string {
  const active = new Set<object>();
  function copy(v: unknown): JsonValue {
    if (v === null || typeof v === "string" || typeof v === "boolean") return v;
    if (typeof v === "number" && Number.isFinite(v) && Math.abs(v) <= Number.MAX_SAFE_INTEGER) return v;
    if (!v || typeof v !== "object" || active.has(v)) throw new TypeError("Checkpoint must be finite JSON data");
    active.add(v);
    try {
      if (Array.isArray(v)) return v.map(copy);
      if (Object.getPrototypeOf(v) !== Object.prototype && Object.getPrototypeOf(v) !== null) throw new TypeError("Checkpoint must be plain JSON data");
      return Object.fromEntries(Object.keys(v).sort().map(k => [k, copy((v as Record<string, unknown>)[k])]));
    } finally { active.delete(v); }
  }
  const result = JSON.stringify(copy(value));
  if (Buffer.byteLength(result) > 131072) throw new TypeError("Clarification exceeds 128 KiB");
  return result;
}
function questions(values: readonly InputQuestion[]): readonly Required<InputQuestion>[] {
  if (!Array.isArray(values) || values.length < 1 || values.length > 3) throw new TypeError("Clarification requires 1 to 3 questions");
  const result = values.map(q => {
    if (!q || typeof q !== "object" || Object.keys(q).some(k => !["id", "prompt", "choices", "allowFreeform"].includes(k))) throw new TypeError("Invalid question");
    const choices = q.choices ?? [];
    if (!Array.isArray(choices) || choices.length > 8 || new Set(choices).size !== choices.length) throw new TypeError("Invalid choices");
    const allowFreeform = q.allowFreeform ?? true;
    if (typeof allowFreeform !== "boolean" || (!allowFreeform && !choices.length)) throw new TypeError("Invalid answer mode");
    return { id: text(q.id, 128), prompt: text(q.prompt, 8000), choices: choices.map(c => text(c, 128)), allowFreeform };
  });
  if (new Set(result.map(q => q.id)).size !== result.length) throw new TypeError("Question ids must be unique");
  return result;
}

/** SQLite clarification storage. It does not replace the canonical Core Run journal. */
export class ClarificationStore {
  readonly #db: DatabaseSync;
  readonly #scope: string;
  constructor(path: string, options: { scope: string }) {
    this.#scope = text(options.scope);
    this.#db = new DatabaseSync(path);
    this.#db.exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
      CREATE TABLE IF NOT EXISTS purra_clarifications (
        scope TEXT NOT NULL, id TEXT NOT NULL, request_key TEXT NOT NULL,
        request_json TEXT NOT NULL, state TEXT NOT NULL, revision INTEGER NOT NULL,
        answer_key TEXT, answers_json TEXT, resume_token TEXT, run_id TEXT,
        PRIMARY KEY(scope, id), UNIQUE(scope, request_key));`);
  }
  #transaction<T>(action: () => T): T {
    this.#db.exec("BEGIN IMMEDIATE");
    try { const result = action(); this.#db.exec("COMMIT"); return result; }
    catch (error) { this.#db.exec("ROLLBACK"); throw error; }
  }
  ask(input: ClarificationInput): ClarificationSnapshot {
    text(input.key);
    if (input.sourceRunId != null) text(input.sourceRunId);
    if (input.expiresAtMs != null && (!Number.isSafeInteger(input.expiresAtMs) || input.expiresAtMs <= 0)) throw new TypeError("Expiry must be a positive integer");
    const body = encode({ questions: questions(input.questions), checkpoint: input.checkpoint,
      sourceRunId: input.sourceRunId ?? null, expiresAtMs: input.expiresAtMs ?? null });
    const id = this.#transaction(() => {
      const existing = this.#db.prepare("SELECT id,request_json FROM purra_clarifications WHERE scope=? AND request_key=?").get(this.#scope, input.key);
      if (existing) {
        if (encode(JSON.parse(existing.request_json as string)) !== body) throw new Error("clarification_idempotency_conflict");
        return existing.id as string;
      }
      const identifier = randomUUID().replaceAll("-", "");
      this.#db.prepare("INSERT INTO purra_clarifications(scope,id,request_key,request_json,state,revision) VALUES(?,?,?,?,'waiting',1)").run(this.#scope, identifier, input.key, body);
      return identifier;
    });
    return this.get(id);
  }
  get(id: string): ClarificationSnapshot {
    const row = this.#db.prepare("SELECT request_json,state,revision,answers_json,run_id,resume_token FROM purra_clarifications WHERE scope=? AND id=?").get(this.#scope, text(id));
    if (!row) throw new Error("clarification_not_found");
    return { id, ...JSON.parse(row.request_json as string), state: row.state, revision: row.revision,
      answers: row.answers_json === null ? null : JSON.parse(row.answers_json as string), runId: row.run_id, resumeToken: row.resume_token } as ClarificationSnapshot;
  }
  answer(id: string, options: { revision: number; key: string; answers: Readonly<Record<string, string>> }): ClarificationSnapshot {
    text(options.key);
    const encoded = encode(options.answers);
    this.#transaction(() => {
      const saved = this.get(id);
      const previous = this.#db.prepare("SELECT answer_key,answers_json FROM purra_clarifications WHERE scope=? AND id=?").get(this.#scope, id)!;
      if (previous.answer_key === options.key) {
        if (encode(JSON.parse(previous.answers_json as string)) !== encoded) throw new Error("clarification_idempotency_conflict");
        return;
      }
      this.#require(saved, "waiting", options.revision);
      if (!options.answers || typeof options.answers !== "object" || Array.isArray(options.answers)
        || Object.keys(options.answers).length !== saved.questions.length || saved.questions.some(q => !Object.hasOwn(options.answers, q.id))) throw new TypeError("Answers must cover exactly the requested questions");
      for (const q of saved.questions) {
        const answer = text(options.answers[q.id], 8000);
        if (!q.allowFreeform && !q.choices.includes(answer)) throw new TypeError("Answer is not one of the offered choices");
      }
      this.#db.prepare("UPDATE purra_clarifications SET state='ready',revision=revision+1,answer_key=?,answers_json=? WHERE scope=? AND id=?").run(options.key, encoded, this.#scope, id);
    });
    return this.get(id);
  }
  claim(id: string, options: { revision: number }): { token: string; snapshot: ClarificationSnapshot } {
    const token = this.#transaction(() => {
      this.#require(this.get(id), "ready", options.revision);
      const value = randomUUID().replaceAll("-", "");
      this.#db.prepare("UPDATE purra_clarifications SET state='resuming',revision=revision+1,resume_token=? WHERE scope=? AND id=?").run(value, this.#scope, id);
      return value;
    });
    return { token, snapshot: this.get(id) };
  }
  reconcile(id: string, options: { token: string; runId?: string; notSubmitted?: boolean }): ClarificationSnapshot {
    if (options.notSubmitted !== undefined && typeof options.notSubmitted !== "boolean") throw new TypeError("notSubmitted must be boolean");
    if ((options.runId === undefined) === (options.notSubmitted !== true)) throw new TypeError("Provide an existing Run id or notSubmitted=true");
    if (options.runId !== undefined) text(options.runId);
    this.#transaction(() => {
      const row = this.#db.prepare("SELECT state,resume_token,run_id FROM purra_clarifications WHERE scope=? AND id=?").get(this.#scope, id);
      if (!row || row.resume_token !== options.token) throw new Error("clarification_claim_conflict");
      if (row.state === "resumed" && row.run_id === options.runId) return;
      if (row.state !== "resuming") throw new Error("clarification_state_conflict");
      this.#db.prepare("UPDATE purra_clarifications SET state=?,revision=revision+1,run_id=? WHERE scope=? AND id=?").run(options.notSubmitted ? "ready" : "resumed", options.runId ?? null, this.#scope, id);
    });
    return this.get(id);
  }
  cancel(id: string, options: { revision: number }): ClarificationSnapshot {
    this.#transaction(() => {
      const saved = this.get(id);
      if (saved.revision !== options.revision || !["waiting", "ready"].includes(saved.state)) throw new Error("clarification_state_conflict");
      this.#db.prepare("UPDATE purra_clarifications SET state='canceled',revision=revision+1 WHERE scope=? AND id=?").run(this.#scope, id);
    });
    return this.get(id);
  }
  static publicView(saved: ClarificationSnapshot): Omit<ClarificationSnapshot, "checkpoint" | "resumeToken"> {
    const { checkpoint: _, resumeToken: __, ...view } = saved; return view;
  }
  #require(saved: ClarificationSnapshot, state: string, revision: number): void {
    if (!Number.isSafeInteger(revision) || saved.state !== state || saved.revision !== revision) throw new Error("clarification_state_conflict");
    if (saved.expiresAtMs !== null && saved.expiresAtMs <= Date.now()) throw new Error("clarification_expired");
  }
  close(): void { this.#db.close(); }
}

export class ClarificationWorkflow {
  constructor(readonly store: ClarificationStore) {}
  admission(input: ClarificationInput): TaskAdmissionDecision {
    const saved = this.store.ask(input);
    return { mode: "clarify", reasonCode: "user_input_required", message: saved.questions.map(q => q.prompt).join("\n"), metadata: { inputRequestId: saved.id } };
  }
  async resume(id: string, options: { revision: number; submit: (snapshot: ClarificationSnapshot, continuationKey: string) => Promise<string> }): Promise<ClarificationSnapshot> {
    const { token, snapshot } = this.store.claim(id, options);
    const runId = await options.submit(snapshot, "clarification:" + id);
    return this.store.reconcile(id, { token, runId });
  }
}

interface NativeInput {
  id: string; runId: string; rootRunId: string; questions: readonly Required<InputQuestion>[];
  state: "waiting" | "ready"; revision: number; answers: Readonly<Record<string, string>> | null;
  answerKey: string | null; request: RunRequest;
}

/** Compose with SqliteAgentAdapters and Agent.checkpointHandler. */
export class SqliteClarification {
  readonly tool: ToolDefinition = {
    name: "request_user_input", description: "Ask the user for missing task information. Execution waits for the answer.",
    inputSchema: { type: "object", properties: { questions: { type: "array", minItems: 1, maxItems: 3,
      items: { type: "object", properties: { id: { type: "string" }, prompt: { type: "string" },
        choices: { type: "array", items: { type: "string" }, maxItems: 8 }, allowFreeform: { type: "boolean" } },
        required: ["id", "prompt"], additionalProperties: false } } }, required: ["questions"], additionalProperties: false },
    policy: { mode: "read", title: "Ask the user" },
    run: (input) => ({ content: { purraInputRequest: questions((input as unknown as { questions: InputQuestion[] }).questions) }, effectState: "not_started" }),
  };
  constructor(readonly storage: SqliteAgentAdapters) {}

  readonly checkpointHandler = (checkpoint: AgentExecutionCheckpoint, request: RunRequest, claim: RunLeaseClaim = {}) => this.checkpoint(checkpoint, request, claim);

  async checkpoint(checkpoint: AgentExecutionCheckpoint, request: RunRequest, claim: RunLeaseClaim = {}): Promise<AgentExecutionCheckpoint> {
    const calls = new Set(checkpoint.messages.flatMap(m => m.toolCalls ?? []).filter(c => c.name === "request_user_input").map(c => c.id));
    const waiting = await this.storage.transaction(async (all, extra) => {
      let tree;
      try { tree = await all.runTree.getRun(checkpoint.runId); } catch (error) { if ((error as any).code !== "child_run_not_found") throw error; }
      const rootRunId = tree?.rootRunId ?? checkpoint.runId;
      const rows: Record<string, NativeInput> = extra.clarifications ??= Object.create(null);
      for (const message of checkpoint.messages) {
        if (message.role !== "tool" || !calls.has(message.toolCallId ?? "")) continue;
        let value: any = message.content;
        if (typeof value === "string") { try { value = JSON.parse(value); } catch { continue; } }
        if (!value?.purraInputRequest) continue;
        const id = createHash("sha256").update(checkpoint.runId + "\0" + message.toolCallId).digest("hex");
        if (!rows[id]) {
          const row: NativeInput = { id, runId: checkpoint.runId, rootRunId, questions: questions(value.purraInputRequest), state: "waiting", revision: 1, answers: null, answerKey: null, request };
          rows[id] = row;
          await all.runs.appendEvent(row.runId, { sourceKey: `input:${id}:required`, kind: "input.required", channel: "lifecycle", visibility: "public", payload: this.#public(row) }, claim);
        }
      }
      const descendants = new Set(tree ? (await all.runTree.listDescendants(checkpoint.runId)).map(run => run.runId) : []);
      descendants.add(checkpoint.runId);
      for (const row of Object.values(rows)) {
        if (descendants.has(row.runId) && row.state === "waiting" && (await all.runs.get(row.runId)).status === "running") return row;
      }
      return undefined;
    });
    if (waiting) throw new UserInputRequired(checkpoint.runId, waiting.id);
    return checkpoint;
  }

  #public(row: NativeInput) { return { id: row.id, runId: row.runId, rootRunId: row.rootRunId, questions: row.questions as unknown as JsonValue, state: row.state, revision: row.revision }; }

  async get(id: string) {
    return this.storage.transaction(async (all, extra) => {
      const row: NativeInput | undefined = extra.clarifications?.[id];
      if (!row) throw new Error("clarification_not_found");
      const run = await all.runs.get(row.runId);
      const lease = extra.leases[row.runId];
      const state = run.status !== "running" ? run.status : row.state === "ready" && lease?.owner && lease.expires > Date.now() ? "running" : row.state;
      return { ...this.#public(row), state, answers: row.answers };
    });
  }

  async listPending() {
    const ids = await this.storage.transaction(async (all, extra) => {
      const result = [];
      for (const row of Object.values((extra.clarifications ?? {}) as Record<string, NativeInput>)) {
        if ((await all.runs.get(row.runId)).status === "running") result.push(row.id);
      }
      return result;
    });
    return Promise.all(ids.map(id => this.get(id)));
  }

  async listWaiting() {
    return this.storage.transaction(async (all, extra) => {
      const result = [];
      for (const row of Object.values((extra.clarifications ?? {}) as Record<string, NativeInput>)) {
        if (row.state === "waiting" && (await all.runs.get(row.runId)).status === "running") result.push(this.#public(row));
      }
      return result;
    });
  }

  async answer(id: string, options: { revision: number; key: string; answers: Readonly<Record<string, string>> }) {
    text(options.key); encode(options.answers);
    await this.storage.transaction(async (all, extra) => {
      const row: NativeInput = extra.clarifications[id];
      if (row.answerKey === options.key) {
        if (encode(row.answers) !== encode(options.answers)) throw new Error("clarification_idempotency_conflict");
        return;
      }
      if (row.state !== "waiting" || row.revision !== options.revision) throw new Error("clarification_revision_conflict");
      if (!options.answers || Array.isArray(options.answers) || Object.keys(options.answers).length !== row.questions.length) throw new Error("answers must cover the questions");
      for (const q of row.questions) {
        const value = text(options.answers[q.id], 8000);
        if (!q.allowFreeform && !q.choices.includes(value)) throw new Error("invalid answer choice");
      }
      const snapshot = await all.runs.get(row.runId);
      if (snapshot.status !== "running") throw new Error("clarification_run_terminal");
      if (snapshot.deadlineAt !== null && Date.parse(snapshot.deadlineAt) <= Date.now()) throw new Error("clarification_expired");
      const checkpoint = snapshot.executionCheckpoint;
      if (!checkpoint) throw new Error("clarification_checkpoint_missing");
      await all.runs.saveExecutionCheckpoint(row.runId, { ...checkpoint, inputRevision: (checkpoint.inputRevision ?? 0) + 1, messages: [...checkpoint.messages,
        { role: "user", content: "Answers to requested task information:\n" + encode(options.answers), attributes: { inputRequestId: id } }] });
      row.answers = options.answers; row.answerKey = options.key; row.state = "ready"; row.revision += 1;
      await all.runs.appendEvent(row.runId, { sourceKey: `input:${id}:answered`, kind: "input.answered", channel: "lifecycle", visibility: "public", payload: this.#public(row) });
    });
    return this.get(id);
  }

  async resume(agent: Agent, id: string): Promise<RunHandle> {
    const row = await this.storage.transaction(async (all, extra) => {
      const saved: NativeInput = extra.clarifications[id];
      if (saved.state !== "ready") throw new Error("clarification_not_ready");
      for (const r of Object.values(extra.clarifications as Record<string, NativeInput>)) {
        if (r.rootRunId === saved.rootRunId && r.state === "waiting" && (await all.runs.get(r.runId)).status === "running") throw new Error("clarification_answers_incomplete");
      }
      return saved;
    });
    return agent.resume(row.rootRunId, row.request);
  }

  async cancel(id: string): Promise<boolean> {
    return this.storage.transaction(async (all, extra) => {
      const row: NativeInput = extra.clarifications[id];
      let tree;
      try { tree = await all.runTree.getRun(row.rootRunId); } catch (error) { if ((error as any).code !== "child_run_not_found") throw error; }
      const ids = [row.rootRunId, ...(tree ? (await all.runTree.listDescendants(row.rootRunId)).map(run => run.runId) : [])];
      if (ids.some(rid => extra.leases[rid]?.owner)) throw new Error("cancel the active Run through its handle");
      const accepted = (await all.runs.cancel(row.rootRunId)).accepted;
      if (tree) {
        for (const rid of ids.slice(1)) {
          try { await all.runs.cancel(rid); } catch (error) { if ((error as any).code !== "run_not_found") throw error; }
        }
        await all.runTree.cancelSubtree(row.rootRunId);
      }
      return accepted;
    });
  }
}
