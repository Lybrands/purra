/** Host-named traversal progress, never execution ownership. */
type Extra = Record<string, any>;
type Transaction = <T>(operation: (extra: Extra) => Promise<T>, readOnly: boolean) => Promise<T>;
type Page = Readonly<{ runIds: readonly string[]; nextAfterRunId: string | null }>;
type Position = { revision: number; afterRunId: string | null };
export class SqliteRecoveryCursor {
  #page: { position: Position; page: Page } | undefined;
  constructor(private readonly transaction: Transaction,
    private readonly list: (afterRunId: string | null, limit: number) => Promise<Page>,
    private readonly name: string, private readonly pageSize = 100) {
    if (typeof name !== 'string' || !name.trim()) throw new TypeError('cursor name required');
    if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 1000) throw new TypeError('invalid cursor page size');
  }
  private row(extra: Extra): Position {
    const rows = extra.recoveryCursors === undefined ? {} : extra.recoveryCursors;
    if (rows === null || typeof rows !== 'object' || Array.isArray(rows)) throw new TypeError('invalid recovery cursors');
    const row = Object.hasOwn(rows, this.name) ? rows[this.name] : { revision: 0, afterRunId: null };
    if (row === null || typeof row !== 'object' || Object.keys(row).sort().join() !== 'afterRunId,revision'
      || !Number.isSafeInteger(row.revision) || row.revision < 0 || row.revision >= Number.MAX_SAFE_INTEGER
      || (row.afterRunId !== null && (typeof row.afterRunId !== 'string' || !row.afterRunId))) throw new TypeError('invalid recovery cursor');
    return row;
  }
  async discover(): Promise<readonly string[]> {
    this.#page = undefined;
    const position = await this.transaction(async extra => ({ ...this.row(extra) }), true);
    const page = await this.list(position.afterRunId, this.pageSize);
    this.#page = {position, page};
    return page.runIds;
  }
  async acknowledge(processedIds: readonly string[]): Promise<void> {
    if (this.#page === undefined) throw new Error('cursor page not discovered');
    const { position, page } = this.#page;
    if (processedIds.length > page.runIds.length || processedIds.some((id, i) => id !== page.runIds[i])) throw new TypeError('cursor acknowledgement must be a processed prefix');
    if (processedIds.length === 0 && (page.runIds.length > 0 || position.afterRunId === null)) return;
    const afterRunId = processedIds.length === page.runIds.length && page.nextAfterRunId === null ? null : processedIds[processedIds.length-1]!;
    await this.transaction(async extra => {
      const current = this.row(extra);
      if (current.revision !== position.revision || current.afterRunId !== position.afterRunId) throw new Error('recovery_cursor_conflict');
      if (position.revision + 1 >= Number.MAX_SAFE_INTEGER) throw new Error('invalid recovery cursor revision');
      const rows = Object.assign(Object.create(null), extra.recoveryCursors ?? {});
      rows[this.name] = {revision:position.revision+1, afterRunId};
      extra.recoveryCursors = rows;
    }, false);
    this.#page = undefined;
  }
}
