/** Opt-in real-provider evaluation. Shared synthetic cases; no implicit credentials or retries. */
import { readFileSync, writeFileSync, renameSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { createHash } from "node:crypto";
import { dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

export const LIMITS = Object.freeze({ chat: 40, embedding: 96, input_chars: 1_000_000, reserved_output_tokens: 40_960 });
const ANSWER_CAP = 512;
export class EvaluationError extends Error {}
const chars = value => [...value].length;
const snake = value => Object.fromEntries(Object.entries(value).map(([key, v]) => [key.replace(/[A-Z]/g, c => "_" + c.toLowerCase()), v]));
const print = value => process.stdout.write(JSON.stringify(value) + "\n");

export function validateFixture(fixture) {
  const cases = fixture.cases, ids = new Set();
  const bounded = (text, max) => typeof text === "string" && text.length > 0 && chars(text) <= max;
  const invalid = () => { throw new EvaluationError("invalid_evaluation_cases"); };
  if (fixture.version !== 1 || !Array.isArray(cases) || cases.length < 1 || cases.length > 9) invalid();
  for (const test of cases) {
    if (typeof test.id !== "string" || !/^[a-z0-9_]{1,64}$/.test(test.id) || ids.has(test.id)) invalid();
    ids.add(test.id);
    if (!["extract", "review", "update", "revoke", "none", "other_scope"].includes(test.action)) invalid();
    const required = ["query", ...(["extract", "review"].includes(test.action) ? ["incoming"] : []), ...(test.action === "extract" ? [] : ["seed"]), ...(test.action === "update" ? ["replacement"] : [])];
    if (required.some(k => !bounded(test[k], 4000))) invalid();
    if (!Array.isArray(test.answers) || test.answers.length > 8 || test.answers.some(a => !bounded(a, 1000))) invalid();
    if (test.action === "review" && (!Array.isArray(test.review_kinds) || !test.review_kinds.length || test.review_kinds.some(k => !["independent", "duplicate", "supersede", "conflict", "uncertain"].includes(k)))) invalid();
    if (test.apply !== undefined && (test.action !== "review" || !["duplicate", "supersede"].includes(test.apply) || test.allow_empty)) invalid();
    if (test.allow_empty !== undefined && typeof test.allow_empty !== "boolean") invalid();
  }
  if (["answer_policy", "extraction_policy", "review_policy"].some(k => !bounded(fixture[k], k === "review_policy" ? 4000 : 8000))) invalid();
  if (!Array.isArray(fixture.distractors) || fixture.distractors.length > 2 || fixture.distractors.some(t => !bounded(t, 4000))) invalid();
}

export function saveReport(path, report) {
  const summary = {};
  for (const call of report.calls) {
    const key = call.phase + "/" + call.kind;
    const row = summary[key] ??= { calls: 0, input_tokens: 0, output_tokens: 0, unreported_calls: 0, duration_ms: 0 };
    row.calls++;
    for (const field of ["input_tokens", "output_tokens", "duration_ms"]) row[field] += call[field] ?? 0;
    row.unreported_calls += Number(call.input_tokens === null || call.kind === "chat" && call.output_tokens === null);
  }
  report.usage_by_phase = summary;
  mkdirSync(dirname(resolve(path)), { recursive: true });
  writeFileSync(path + ".tmp", JSON.stringify(report, null, 2) + "\n"); renameSync(path + ".tmp", path);
}

export function preflight(config) {
  const missing = [];
  for (const kind of ["chat", "embedding"]) {
    const value = config[kind] ?? {};
    let url;
    try { url = new URL(value.base_url); } catch { throw new EvaluationError("invalid_provider_url"); }
    const local = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
    if ((!local || url.protocol !== "http:") && url.protocol !== "https:" || url.username || url.password || url.search || url.hash) throw new EvaluationError("invalid_provider_url");
    if (typeof value.model !== "string" || !value.model.trim()) throw new EvaluationError("missing_model");
    if (typeof value.key_env !== "string" || !/^[A-Za-z_][A-Za-z0-9_]*$/.test(value.key_env)) throw new EvaluationError("invalid_key_env");
    if (!process.env[value.key_env]) missing.push(value.key_env);
    if (url.hostname.endsWith(".example")) missing.push(kind + ".base_url");
  }
  const dims = config.embedding.dimensions;
  if (!Number.isSafeInteger(dims) || dims < 1 || dims > 65_536) throw new EvaluationError("invalid_embedding_dimensions");
  const extra = config.chat.extra ?? {};
  if (!extra || typeof extra !== "object" || Array.isArray(extra) || Object.keys(extra).some(k => !["temperature", "top_p", "thinking", "reasoning_effort"].includes(k))) throw new EvaluationError("unsupported_chat_options");
  return [...new Set(missing)].sort();
}

export function grade(text, rows, answers, supporting = rows.map(r => r.id)) {
  try {
    const result = JSON.parse(text);
    if (!result || Object.keys(result).sort().join() !== "answer,evidence"
        || result.answer !== null && typeof result.answer !== "string"
        || !Array.isArray(result.evidence) || result.evidence.some(i => typeof i !== "string")
        || new Set(result.evidence).size !== result.evidence.length) throw Error();
    const available = new Set(rows.map(row => row.id)), support = new Set(supporting);
    const valid = result.evidence.every(id => available.has(id));
    const abstains = result.answer === null && result.evidence.length === 0;
    const correct = answers.length ? typeof result.answer === "string" && answers.includes(result.answer.trim())
      && result.evidence.length > 0 && valid && result.evidence.every(id => support.has(id)) : abstains;
    return { valid, correct, abstains };
  } catch { return { valid: false, correct: false, abstains: false }; }
}

export class Transport {
  constructor(config, request = fetch, checkpoint = () => {}) {
    this.config = config; this.request = request; this.calls = []; this.phase = "setup"; this.case = "";
    this.started = performance.now(); this.chars = 0; this.reserved = 0;
    this.checkpoint = checkpoint;
  }
  async post(kind, payload, inputChars, cap, signal) {
    if (signal?.aborted) throw new EvaluationError("cancelled");
    if (this.calls.filter(c => c.kind === kind).length >= LIMITS[kind] || this.chars + inputChars > LIMITS.input_chars
        || this.reserved + cap > LIMITS.reserved_output_tokens || performance.now() - this.started >= 900_000) throw new EvaluationError("trial_budget_exceeded");
    this.chars += inputChars; this.reserved += cap;
    const entry = { kind, case: this.case, phase: this.phase, input_chars: inputChars, reserved_output_tokens: cap,
      input_tokens: null, output_tokens: null, outcome: "unknown" };
    this.calls.push(entry);
    this.checkpoint();
    const cfg = this.config[kind], started = performance.now();
    const deadline = AbortSignal.timeout(Math.max(1, Math.floor(Math.min(60_000, 900_000 - (started - this.started)))));
    const combined = signal ? AbortSignal.any([signal, deadline]) : deadline;
    try {
      const response = await this.request(cfg.base_url.replace(/\/$/, "") + (kind === "chat" ? "/chat/completions" : "/embeddings"), {
        method: "POST", redirect: "manual", signal: combined, headers: { "Content-Type": "application/json", Authorization: "Bearer " + process.env[cfg.key_env] }, body: JSON.stringify(payload),
      });
      if (response.status !== 200) { await response.body?.cancel(); throw new EvaluationError("provider_http_" + response.status); }
      const reader = response.body.getReader(), chunks = [];
      let size = 0;
      try {
        while (true) {
          const { value, done } = await reader.read();
          if (done) break;
          size += value.length;
          if (size > 8_000_000) throw new EvaluationError("provider_response_too_large");
          chunks.push(value);
        }
      } finally { await reader.cancel(); reader.releaseLock(); }
      const value = JSON.parse(Buffer.concat(chunks).toString("utf8"));
      for (const [target, source] of [["input_tokens", "prompt_tokens"], ["output_tokens", "completion_tokens"]]) {
        const count = value.usage?.[source] ?? null;
        if (count !== null && (!Number.isSafeInteger(count) || count < 0)) throw new EvaluationError("invalid_provider_usage");
        entry[target] = count;
      }
      entry.outcome = "returned";
      return value;
    } catch (error) {
      const code = signal?.aborted ? "cancelled" : deadline.aborted ? "transport_timeout" : error instanceof EvaluationError ? error.message : "transport_error";
      entry.outcome = code;
      throw new EvaluationError(code);
    } finally { entry.duration_ms = Math.round(performance.now() - started); this.checkpoint(); }
  }
  async complete(messages, cap, signal) {
    const raw = await this.post("chat", { model: this.config.chat.model, messages: messages.map(({ role, content }) => ({ role, content })),
      max_tokens: cap, stream: false, ...this.config.chat.extra }, messages.reduce((n, m) => n + chars(m.content), 0), cap, signal);
    try {
      if (raw.choices?.length !== 1) throw Error();
      const { message, finish_reason: finishReason } = raw.choices[0];
      if (finishReason !== "stop" || message.tool_calls?.length || message.role !== "assistant" || typeof message.content !== "string") throw Error();
      const usage = raw.usage?.prompt_tokens == null || raw.usage?.completion_tokens == null ? undefined
        : { inputTokens: raw.usage.prompt_tokens, outputTokens: raw.usage.completion_tokens };
      if (usage && usage.outputTokens > cap) throw Error();
      return { message: { role: "assistant", content: message.content }, finishReason, appliedOutputLimit: cap, ...(usage ? { usage } : {}) };
    } catch { throw new EvaluationError("invalid_chat_response"); }
  }
  async embed(texts, signal) {
    const raw = await this.post("embedding", { model: this.config.embedding.model, input: texts, encoding_format: "float" }, texts.reduce((n, text) => n + chars(text), 0), 0, signal);
    try {
      const rows = raw.data.toSorted((a, b) => a.index - b.index);
      if (rows.length !== texts.length || rows.some((row, i) => row.index !== i)) throw Error();
      const vectors = rows.map(row => row.embedding);
      if (vectors.some(v => !Array.isArray(v) || v.length !== this.config.embedding.dimensions || v.some(x => typeof x !== "number" || !Number.isFinite(x)))) throw Error();
      return { vectors, ...(raw.usage?.prompt_tokens == null ? {} : { inputTokens: raw.usage.prompt_tokens }) };
    } catch { throw new EvaluationError("invalid_embedding_response"); }
  }
}

export async function evaluateCase(test, fixture, root, transport, index) {
  const { Mem0Memory, MemoryContext, createManagedClient } = await import("purra-mem0");
  transport.case = test.id; transport.phase = "ingestion";
  const checks = {}, ledgers = [];
  let decisions = [], relevant = new Set();
  const dims = transport.config.embedding.dimensions;
  const providers = { budget: { key: "trial", maxLlmCalls: 4, maxEmbeddingCalls: 20, maxInputChars: 150_000, maxOutputTokens: 8192, maxCallOutputTokens: 2048 },
    complete: transport.complete.bind(transport), embed: transport.embed.bind(transport) };
  mkdirSync(root);
  async function openMemory(other = false) {
    const client = await createManagedClient({ embeddingDims: dims, config: {
      vectorStore: { provider: "memory", config: { dimension: dims, dbPath: join(root, "vectors.db"), collectionName: "memory" } },
      historyDbPath: join(root, "history.db"), customInstructions: fixture.extraction_policy,
    } });
    const memory = new Mem0Memory({ client, scope: { user: "synthetic-user", project: test.id + (other ? "-other" : "") },
      journalPath: join(root, "journal.db"), allowInference: true, providers, timeoutMs: 65_000, maxResults: 16 });
    return { client, memory };
  }
  async function close({ client, memory }) {
    await memory.drain(); ledgers.push(snake(memory.budgetUsage())); memory.close();
    // Test-only teardown: the pinned SDK has no aggregate public close().
    client.sdk.db.close(); client.sdk.vectorStore.db.close(); client.sdk._entityStore?.db.close();
  }
  let current = await openMemory(), active;
  try {
    const { memory } = current;
    for (const [i, text] of fixture.distractors.entries()) await memory.add(text, { source: { id: "distractor-" + i, revision: "1" }, key: "distractor-" + i });
    let old;
    if (test.seed) { old = await memory.add(test.seed, { source: { id: "confirmed", revision: "1" }, key: "seed" }); relevant = new Set(old.ids); }
    if (["extract", "review"].includes(test.action)) {
      const pending = await memory.extract([{ role: "user", content: test.incoming }], { source: { id: "incoming", revision: "1" }, key: "extract" });
      checks.extraction_cardinality = pending.ids.length === 1 || pending.ids.length === 0 && test.allow_empty === true;
      checks.candidate_not_active = (await Promise.all(pending.ids.map(id => memory.get(id)))).every(v => v === undefined);
      if (pending.ids.length === 1) {
        const candidate = await memory.get(pending.ids[0], { includeInactive: true });
        if (test.action === "extract") {
          await memory.setState(candidate.id, "active", { version: candidate.version, key: "accept" });
          relevant.add(candidate.id);
        } else {
          transport.phase = "review";
          const op = await memory.review({ id: candidate.id, version: candidate.version }, { key: "review", limit: 4, instructions: fixture.review_policy });
          decisions = op.review.matches.filter(m => relevant.has(m.item.id)).map(m => m.kind);
          checks.review_classification = decisions.length === 1 && test.review_kinds.includes(decisions[0]);
          if (test.apply) {
            const proposal = op.review.proposal;
            checks.authorized_proposal = !!proposal && proposal.kind === test.apply;
            if (checks.authorized_proposal) { await memory.resolve(proposal, { key: "apply" }); relevant = new Set([proposal.keep]); }
          }
          checks.candidate_visibility = (await memory.get(candidate.id) !== undefined) === (test.apply === "supersede" && checks.authorized_proposal === true);
        }
      }
    } else if (test.action === "update") {
      await memory.update(old.ids[0], test.replacement, { source: { id: "confirmed", revision: "2" }, version: 1, key: "correct" });
      checks.updated_version = (await memory.get(old.ids[0])).version === 2;
    } else if (test.action === "revoke") {
      await memory.revokeSource("confirmed", { key: "withdraw" });
      checks.withdrawn_hidden = await memory.get(old.ids[0], { includeInactive: true }) === undefined;
    }
    active = (await memory.list({ limit: 16 })).items.map(item => item.id).sort();
  } finally { await close(current); }
  transport.phase = "recall";
  current = await openMemory(test.action === "other_scope");
  let rows, usage;
  try {
    const { memory } = current;
    checks.restart_visibility = JSON.stringify((await memory.list({ limit: 16 })).items.map(item => item.id).sort()) === JSON.stringify(test.action === "other_scope" ? [] : active);
    const context = new MemoryContext({ memory, query: () => test.query, limit: 4, desiredTokens: 2048 });
    const bundle = await context.buildContext(undefined, { contextAllocations: { memory: 2048 } });
    rows = bundle.blocks.flatMap(block => JSON.parse(block.content));
    const hasRelevant = rows.some(row => relevant.has(row.id));
    checks.relevant_recall = test.answers.length ? hasRelevant : ["revoke", "other_scope"].includes(test.action) ? !hasRelevant : true;
    usage = snake(memory.budgetUsage());
  } finally { await close(current); }
  const outcomes = {};
  for (const arm of index % 2 === 0 ? ["without_memory", "with_memory"] : ["with_memory", "without_memory"]) {
    transport.phase = arm;
    const supplied = arm === "with_memory" ? rows : [];
    const result = await transport.complete([{ role: "system", content: fixture.answer_policy },
      { role: "user", content: JSON.stringify({ question: test.query, memories: supplied }) }], ANSWER_CAP);
    outcomes[arm] = grade(result.message.content, supplied, test.answers, relevant);
  }
  checks.memory_answer = outcomes.with_memory.correct;
  checks.baseline_no_fabrication = outcomes.without_memory.valid && outcomes.without_memory.abstains;
  return { id: test.id, passed: Object.values(checks).every(Boolean), checks, review_kinds: decisions,
    selected_count: rows.length, answers: outcomes, memory_usage: usage, journal_snapshots: ledgers };
}

export async function run(config, fixture, report, output) {
  const root = mkdtempSync(join(tmpdir(), "purra-memory-eval-"));
  process.env.MEM0_TELEMETRY = "false"; process.env.MEM0_DIR = join(root, "sdk");
  const transport = new Transport(config, fetch, () => saveReport(output, report));
  report.calls = transport.calls; report.status = "running";
  report.provider_mode = "real";
  const originals = { log: console.log, warn: console.warn, error: console.error, info: console.info, debug: console.debug };
  // Upstream may log failed model content. Reports use process.stdout directly.
  for (const key of Object.keys(originals)) console[key] = () => {};
  try {
    for (const [index, test] of fixture.cases.entries()) {
      print({ case: test.id, state: "running" });
      const result = await evaluateCase(test, fixture, join(root, test.id), transport, index);
      report.results.push(result); saveReport(output, report); print({ case: test.id, passed: result.passed });
    }
    report.status = report.results.every(r => r.passed) ? "passed" : "failed";
  } catch (error) {
    report.status = "failed";
    report.error = error instanceof EvaluationError ? error.message : typeof error?.code === "string" && /^memory_[a-z_]+$/.test(error.code) ? error.code : "evaluation_error";
    report.failed_case = transport.case;
  } finally {
    Object.assign(console, originals);
    report.calls = transport.calls; report.duration_ms = Math.round(performance.now() - transport.started);
    report.input_chars = transport.chars; report.reserved_output_tokens = transport.reserved;
    rmSync(root, { recursive: true, force: true });
  }
}

async function main() {
  const { values } = parseArgs({ options: { config: { type: "string" }, output: { type: "string" }, cases: { type: "string" }, run: { type: "boolean", default: false } } });
  if (!values.config || !values.output) throw new EvaluationError("config_and_output_required");
  const report = { schema: 1, runtime: "typescript", started_at: new Date().toISOString(), status: "blocked", provider_mode: "not_called",
    limits: LIMITS, answer_cap: ANSWER_CAP, results: [], calls: [], monetary_cost: null, scope: "component_pipeline", execution: "bounded_standalone" };
  try {
    const configBytes = readFileSync(values.config);
    const config = JSON.parse(configBytes.toString("utf8"));
    report.configuration_sha256 = createHash("sha256").update(configBytes).digest("hex");
    const fixtureBytes = readFileSync(values.cases ?? new URL("../../fixtures/evaluation.json", import.meta.url));
    const fixture = JSON.parse(fixtureBytes.toString("utf8")); validateFixture(fixture);
    report.fixture_sha256 = createHash("sha256").update(fixtureBytes).digest("hex");
    report.planned_cases = fixture.cases.length;
    const missing = preflight(config);
    if (missing.length) report.missing = missing;
    else {
      report.models = { chat: config.chat.model, embedding: config.embedding.model }; report.status = "preflight_passed";
      if (values.run) await run(config, fixture, report, values.output);
    }
  } catch (error) { report.status = report.provider_mode === "real" ? "failed" : "blocked"; report.error = error instanceof EvaluationError ? error.message : "invalid_evaluation_configuration"; }
  saveReport(values.output, report);
  print({ status: report.status, completed: report.results.length, calls: report.calls.length });
  process.exitCode = ["passed", "preflight_passed"].includes(report.status) ? 0 : 2;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(() => { print({ status: "blocked", error: "invalid_evaluation_arguments" }); process.exitCode = 2; });
}
