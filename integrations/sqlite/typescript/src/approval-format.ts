import type { DatabaseSync } from "node:sqlite";
import { StorageSession } from "purra";
import { OutputJournal } from "./journal.js";

export function storageVersion(db: DatabaseSync): 4 | 5 {
  if (!db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='purra_state'").get()) return 4;
  const marker = db.prepare("SELECT version,body FROM purra_state WHERE scope='' AND sdk='purra.approvals'").get();
  const version = marker?.version === 5 && marker.body === "{}" ? 5 : 4;
  if (marker && version !== 5 || db.prepare("SELECT 1 FROM purra_state WHERE version != ? LIMIT 1").get(version)) throw new Error("unsupported SQLite storage version");
  return version;
}

export interface ApprovalUpgradeInspection {
  readonly schemaVersion: 1;
  readonly authority: "diagnosis_only";
  readonly storageVersion: 4 | 5;
  readonly targetVersion: 5;
  readonly status: "ready" | "blocked" | "already_enabled";
  readonly blockers: readonly string[];
}
function activationBlocker(db: DatabaseSync): string | undefined {
  for (const row of db.prepare("SELECT scope,sdk,body FROM purra_state ORDER BY scope,sdk").all()) {
    if (row.sdk !== "typescript") return "approval_activation_foreign_sdk";
    const journal = new OutputJournal(db, String(row.scope), false);
    const session = new StorageSession(String(row.body), "all", journal.restore());
    if (session.hasUnsettledExecution()) return "approval_activation_execution_pending";
  }
  return undefined;
}
export function inspectApprovalUpgrade(db: DatabaseSync): ApprovalUpgradeInspection {
  const version = storageVersion(db);
  const blocker = version === 5 ? undefined : activationBlocker(db);
  return Object.freeze({schemaVersion: 1, authority: "diagnosis_only", storageVersion: version,
    targetVersion: 5, status: version === 5 ? "already_enabled" : blocker ? "blocked" : "ready",
    blockers: Object.freeze(blocker ? [blocker] : [])});
}
export function enableApprovals(db: DatabaseSync): void {
  if (storageVersion(db) === 5) return;
  const blocker = activationBlocker(db);
  if (blocker) throw new Error(blocker);
  db.exec(`UPDATE purra_state SET version=5;
    INSERT INTO purra_state VALUES('', 'purra.approvals', 5, '{}');
    CREATE TABLE purra_approvals (
      scope TEXT NOT NULL, sdk TEXT NOT NULL, approval_id TEXT NOT NULL,
      run_id TEXT NOT NULL, call_id TEXT NOT NULL, body TEXT NOT NULL,
      PRIMARY KEY(scope,sdk,approval_id), UNIQUE(scope,sdk,run_id,call_id));
    CREATE TRIGGER purra_approval_format_insert BEFORE INSERT ON purra_state WHEN NEW.version != 5
      BEGIN SELECT RAISE(ABORT, 'unsupported SQLite storage version'); END;
    CREATE TRIGGER purra_approval_format_update BEFORE UPDATE ON purra_state WHEN NEW.version != 5
      BEGIN SELECT RAISE(ABORT, 'unsupported SQLite storage version'); END;`);
}
