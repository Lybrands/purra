/** Persistent scheduling hints; never ownership or authorization. */
type Extra = Record<string, any>;
type Transaction = <T>(operation: (extra: Extra) => Promise<T>, readOnly: boolean) => Promise<T>;
function integer(value: number): number {
  if (!Number.isSafeInteger(value) || value < 0 || value >= Number.MAX_SAFE_INTEGER) throw new TypeError('invalid recovery schedule integer');
  return value;
}
export class SqliteRecoverySchedule {
  constructor(private readonly transaction: Transaction,
    private readonly options: { intervalMs?: number; maxBackoffMs?: number; clockMs?: () => number } = {}) {
    this.options = Object.freeze({ ...options });
    const interval = integer(options.intervalMs ?? 1000), maximum = integer(options.maxBackoffMs ?? 30000);
    if (!(interval > 0 && interval <= maximum && maximum <= 2147483647)) throw new TypeError('invalid recovery schedule intervals');
  }
  static row(extra: Extra, runId: string): { revision: number; failures: number; notBeforeMs: number } {
    if (typeof runId !== 'string' || !runId.trim()) throw new TypeError('run id required');
    const rows = extra.recoverySchedule === undefined ? {} : extra.recoverySchedule;
    if (typeof rows !== 'object' || rows === null || Array.isArray(rows)) throw new TypeError('invalid recovery schedule');
    const row = Object.hasOwn(rows, runId) ? rows[runId] : { revision: integer(extra.recoveryScheduleRevision === undefined ? 0 : extra.recoveryScheduleRevision), failures: 0, notBeforeMs: 0 };
    if (row === null || typeof row !== 'object' || Object.keys(row).sort().join() !== 'failures,notBeforeMs,revision') throw new TypeError('invalid recovery schedule');
    for (const value of Object.values(row)) integer(value as number);
    return row;
  }
  static save(extra: Extra, runId: string, row: object): void {
    const rows = Object.assign(Object.create(null), extra.recoverySchedule ?? {});
    rows[runId] = row; extra.recoverySchedule = rows;
  }
  async ready(runId: string): Promise<number | null> {
    return this.transaction(async extra => {
      const row = SqliteRecoverySchedule.row(extra, runId);
      return row.notBeforeMs <= integer((this.options.clockMs ?? Date.now)()) ? row.revision : null;
    }, true);
  }
  async wake(runId: string): Promise<number> {
    return this.transaction(async extra => {
      return wakeRecoverySchedule(extra, runId)!;
    }, false);
  }
  async settle(runId: string, revision: number, failed: boolean): Promise<boolean> {
    integer(revision);
    if (typeof failed !== 'boolean') throw new TypeError('failed must be boolean');
    return this.transaction(async extra => {
      const row = SqliteRecoverySchedule.row(extra, runId);
      if (row.revision !== revision) return false;
      const failures = failed ? Math.min(row.failures + 1, 31) : 0;
      const delay = Math.min(this.options.maxBackoffMs ?? 30000, (this.options.intervalMs ?? 1000) * 2 ** failures);
      SqliteRecoverySchedule.save(extra, runId, { revision: nextRevision(extra, revision), failures,
        notBeforeMs: integer(integer((this.options.clockMs ?? Date.now)()) + delay) });
      return true;
    }, false);
  }
}

export function wakeRecoverySchedule(extra: Extra, runId: string, existingOnly = false): number | undefined {
  const row = SqliteRecoverySchedule.row(extra, runId);
  if (existingOnly && !Object.hasOwn(extra.recoverySchedule ?? {}, runId)) return undefined;
  const revision = nextRevision(extra, row.revision);
  SqliteRecoverySchedule.save(extra, runId, { revision, failures: 0, notBeforeMs: 0 });
  return revision;
}

function nextRevision(extra: Extra, current: number): number {
  const revision = integer(Math.max(integer(extra.recoveryScheduleRevision === undefined ? 0 : extra.recoveryScheduleRevision), current) + 1);
  extra.recoveryScheduleRevision = revision;
  return revision;
}

export function removeRecoverySchedule(extra: Extra, runId: string): boolean {
  const row = SqliteRecoverySchedule.row(extra, runId);
  if (!Object.hasOwn(extra.recoverySchedule ?? {}, runId)) return false;
  nextRevision(extra, row.revision);
  delete extra.recoverySchedule[runId];
  return true;
}
