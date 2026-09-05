import type { DatabaseSync } from "node:sqlite";
import { AgentError, type OutputEvent } from "purra";

export const STORAGE_VERSION = 4;

function decode(text: string): OutputEvent {
  const freeze = (value: any): any => {
    if (value !== null && typeof value === "object") {
      for (const child of Object.values(value)) freeze(child);
      Object.freeze(value);
    }
    return value;
  };
  return freeze(JSON.parse(text));
}

function decodeRow(row: Record<string, unknown>): OutputEvent {
  const event = decode(String(row.body));
  if (event.runId !== row.run_id || event.rootRunId !== row.root_run_id
    || event.sequence !== row.sequence || event.rootSequence !== row.root_sequence) {
    throw new TypeError("invalid output journal identity or sequence");
  }
  return event;
}

export class OutputJournal {
  constructor(private readonly db: DatabaseSync, private readonly scope: string) {
    db.exec(`CREATE TABLE IF NOT EXISTS purra_journal_runs (
      scope TEXT NOT NULL, sdk TEXT NOT NULL, run_id TEXT NOT NULL,
      root_run_id TEXT NOT NULL, PRIMARY KEY(scope,sdk,run_id),
      FOREIGN KEY(scope,sdk) REFERENCES purra_state(scope,sdk) ON DELETE CASCADE);
      CREATE TABLE IF NOT EXISTS purra_output_events (
      scope TEXT NOT NULL, sdk TEXT NOT NULL, run_id TEXT NOT NULL,
      sequence INTEGER NOT NULL, root_run_id TEXT NOT NULL,
      root_sequence INTEGER NOT NULL, body TEXT NOT NULL,
      PRIMARY KEY(scope,sdk,run_id,sequence),
      UNIQUE(scope,sdk,root_run_id,root_sequence),
      FOREIGN KEY(scope,sdk,run_id) REFERENCES purra_journal_runs(scope,sdk,run_id) ON DELETE CASCADE);
      CREATE UNIQUE INDEX IF NOT EXISTS purra_typescript_output_source
      ON purra_output_events(scope,root_run_id,json_extract(body,'$.sourceKey')) WHERE sdk='typescript';
      CREATE INDEX IF NOT EXISTS purra_output_sequence_cover
      ON purra_output_events(scope,sdk,root_run_id,run_id,sequence,root_sequence);
      CREATE INDEX IF NOT EXISTS purra_journal_roots
      ON purra_journal_runs(scope,sdk,root_run_id,run_id);`);
  }

  rootForRun(runId: string): string | undefined {
    const row = this.db.prepare("SELECT root_run_id FROM purra_journal_runs WHERE scope=? AND sdk='typescript' AND run_id=?").get(this.scope, runId);
    return row === undefined ? undefined : String(row.root_run_id);
  }

  restore(rootRunId?: string): readonly OutputEvent[] {
    const query = this.db.prepare("SELECT body FROM purra_output_events WHERE scope=? AND sdk='typescript'"
      + (rootRunId === undefined ? "" : " AND root_run_id=?") + " ORDER BY root_run_id,root_sequence");
    const rows = rootRunId === undefined ? query.all(this.scope) : query.all(this.scope, rootRunId);
    return rows.map((row) => decode(String(row.body)));
  }

  deferred(rootRunId: string) {
    const rows = this.db.prepare(`SELECT r.run_id,COUNT(e.sequence) AS count,
      MIN(e.sequence) AS first,MAX(e.sequence) AS last,
      MIN(e.root_sequence) AS root_first,MAX(e.root_sequence) AS root_last
      FROM purra_journal_runs r LEFT JOIN purra_output_events e
      ON e.scope=r.scope AND e.sdk=r.sdk AND e.run_id=r.run_id AND e.root_run_id=r.root_run_id
      WHERE r.scope=? AND r.sdk='typescript' AND r.root_run_id=? GROUP BY r.run_id`).all(this.scope, rootRunId);
    const counts = new Map<string, number>();
    let rootFirst = Infinity, rootLast = 0, total = 0;
    for (const row of rows) {
      const count = Number(row.count);
      if (count && (row.first !== 1 || row.last !== count)) throw new TypeError("invalid output journal sequence");
      counts.set(String(row.run_id), count);
      total += count;
      if (count) { rootFirst = Math.min(rootFirst, Number(row.root_first)); rootLast = Math.max(rootLast, Number(row.root_last)); }
    }
    if (total && (rootFirst !== 1 || rootLast !== total)) throw new TypeError("incomplete output journal");
    return {
      counts,
      readRun: (runId: string) => this.restoreRun(runId),
      readRoot: () => this.restore(rootRunId),
      findSource: (key: string) => {
        const row = this.db.prepare("SELECT run_id,root_run_id,sequence,root_sequence,body FROM purra_output_events WHERE scope=? AND sdk='typescript' AND root_run_id=? AND json_extract(body,'$.sourceKey')=?").get(this.scope, rootRunId, key);
        return row === undefined ? undefined : decodeRow(row);
      },
    };
  }

  restoreRun(runId: string): readonly OutputEvent[] {
    return this.db.prepare("SELECT run_id,root_run_id,sequence,root_sequence,body FROM purra_output_events WHERE scope=? AND sdk='typescript' AND run_id=? ORDER BY sequence")
      .all(this.scope, runId).map(decodeRow);
  }

  append(journals: readonly { readonly runId: string; readonly rootRunId: string; readonly afterSequence?: number; readonly events: readonly OutputEvent[] }[], prior: ReadonlyMap<string, number>): void {
    const insertRun = this.db.prepare("INSERT INTO purra_journal_runs VALUES (?, 'typescript', ?, ?)");
    const insertEvent = this.db.prepare("INSERT INTO purra_output_events VALUES (?, 'typescript', ?, ?, ?, ?, ?)");
    for (const journal of journals) {
      if (!prior.has(journal.runId)) insertRun.run(this.scope, journal.runId, journal.rootRunId);
      const offset = journal.afterSequence ?? 0;
      const count = prior.get(journal.runId) ?? 0;
      if (offset > count) throw new TypeError("output journal append has a gap");
      for (const event of journal.events.slice(count - offset)) {
        insertEvent.run(this.scope, event.runId, event.sequence, event.rootRunId, event.rootSequence, JSON.stringify(event));
      }
    }
  }

  read(runId: string, after: number, limit = 200, root = false): readonly OutputEvent[] {
    const row = this.db.prepare("SELECT version FROM purra_state WHERE scope=? AND sdk='typescript'").get(this.scope);
    if (row && row.version !== STORAGE_VERSION) throw new Error("unsupported SQLite storage version");
    const run = row && this.db.prepare("SELECT root_run_id FROM purra_journal_runs WHERE scope=? AND sdk='typescript' AND run_id=?").get(this.scope, runId);
    if (!run) throw new AgentError("run_not_found", "Run does not exist");
    if (root && run.root_run_id !== runId) throw new AgentError("run_scope_conflict", "Root journal query requires a Root Run");
    if (!Number.isSafeInteger(after) || after < 0) throw new TypeError(`${root ? "afterRootSequence" : "afterSequence"} must be a non-negative integer`);
    if (!Number.isSafeInteger(limit) || limit < 1) throw new TypeError("limit must be positive");
    const [identity, sequence] = root ? ["root_run_id", "root_sequence"] : ["run_id", "sequence"];
    const rows = this.db.prepare(`SELECT body FROM purra_output_events WHERE scope=? AND sdk='typescript' AND ${identity}=? AND ${sequence}>? ORDER BY ${sequence} LIMIT ?`).all(this.scope, runId, after, limit);
    return Object.freeze(rows.map((item) => decode(String(item.body))));
  }
}
