"""Persistent scheduling hints; never execution ownership or authorization."""
import time


def _integer(value):
    if type(value) is not int or not 0 <= value < 9007199254740991:
        raise ValueError('invalid recovery schedule integer')
    return value


class SqliteRecoverySchedule:
    def __init__(self, storage, *, interval_ms=1000, max_backoff_ms=30000, clock_ms=None):
        if not 0 < _integer(interval_ms) <= _integer(max_backoff_ms) <= 2147483647:
            raise ValueError('invalid recovery schedule intervals')
        self._storage = storage
        self._interval = interval_ms
        self._maximum = max_backoff_ms
        self._clock = clock_ms or (lambda: int(time.time() * 1000))

    def _row(self, extra, run_id):
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError('run id required')
        rows = extra.get('recoverySchedule', {})
        if not isinstance(rows, dict): raise ValueError('invalid recovery schedule')
        row = rows.get(run_id, {'revision': 0, 'failures': 0, 'notBeforeMs': 0})
        if not isinstance(row, dict) or set(row) != {'revision', 'failures', 'notBeforeMs'}:
            raise ValueError('invalid recovery schedule')
        for value in row.values(): _integer(value)
        return row

    async def ready(self, run_id):
        async with self._storage._transaction(read_only=True, with_journal=False) as session:
            row = self._row(session.extra, run_id)
            return row['revision'] if row['notBeforeMs'] <= _integer(self._clock()) else None

    async def wake(self, run_id):
        async with self._storage._transaction(with_journal=False) as session:
            row = self._row(session.extra, run_id)
            revision = _integer(row['revision'] + 1)
            session.extra.setdefault('recoverySchedule', {})[run_id] = {'revision': revision, 'failures': 0, 'notBeforeMs': 0}
            return revision

    async def settle(self, run_id, revision, failed):
        _integer(revision)
        if type(failed) is not bool: raise ValueError('failed must be boolean')
        async with self._storage._transaction(with_journal=False) as session:
            row = self._row(session.extra, run_id)
            if row['revision'] != revision: return False
            failures = min(row['failures'] + 1, 31) if failed else 0
            delay = min(self._maximum, self._interval * 2 ** failures)
            session.extra.setdefault('recoverySchedule', {})[run_id] = {
                'revision': _integer(revision + 1), 'failures': failures,
                'notBeforeMs': _integer(_integer(self._clock()) + delay),
            }
            return True
