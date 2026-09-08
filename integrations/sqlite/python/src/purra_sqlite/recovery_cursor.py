"""Host-named traversal progress, independent of execution ownership."""
from .recovery_schedule import _integer


class SqliteRecoveryCursor:
    def __init__(self, storage, name, *, page_size=100):
        if not isinstance(name, str) or not name.strip(): raise ValueError('cursor name required')
        if type(page_size) is not int or not 1 <= page_size <= 1000: raise ValueError('invalid cursor page size')
        self._storage, self._name, self._size = storage, name, page_size
        self._page = None

    def _row(self, extra):
        rows = extra.get('recoveryCursors', {})
        if not isinstance(rows, dict): raise ValueError('invalid recovery cursors')
        row = rows.get(self._name, {'revision': 0, 'afterRunId': None})
        if not isinstance(row, dict) or set(row) != {'revision', 'afterRunId'}: raise ValueError('invalid recovery cursor')
        _integer(row['revision'])
        if row['afterRunId'] is not None and (not isinstance(row['afterRunId'], str) or not row['afterRunId']): raise ValueError('invalid recovery cursor position')
        return row

    async def discover(self):
        self._page = None
        async with self._storage._metadata_transaction(read_only=True) as session:
            row = dict(self._row(session.extra))
        page = await self._storage.list_run_candidates(after_run_id=row['afterRunId'], limit=self._size)
        self._page = (row, page)
        return page['runIds']

    async def acknowledge(self, processed_ids):
        if self._page is None: raise ValueError('cursor page not discovered')
        row, page = self._page
        processed = tuple(processed_ids)
        if len(processed) > len(page['runIds']) or processed != page['runIds'][:len(processed)]:
            raise ValueError('cursor acknowledgement must be a processed prefix')
        if not processed and (page['runIds'] or row['afterRunId'] is None): return
        after = (None if len(processed) == len(page['runIds']) and page['nextAfterRunId'] is None
                 else processed[-1])
        async with self._storage._metadata_transaction() as session:
            if self._row(session.extra) != row: raise ValueError('recovery_cursor_conflict')
            session.extra.setdefault('recoveryCursors', {})[self._name] = {
                'revision': _integer(row['revision'] + 1), 'afterRunId': after,
            }
        self._page = None
