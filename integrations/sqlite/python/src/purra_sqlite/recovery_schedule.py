"""Persistent scheduling hints; never execution ownership or authorization."""
import time


def _integer(value):
    if type(value) is not int or not 0 <= value < 9007199254740991:
        raise ValueError('invalid recovery schedule integer')
    return value


class SqliteRecoverySchedule:
    def __init__(self, storage, *, interval_ms=1000, max_backoff_ms=30000, clock_ms=None, max_failures=None):
        if not 0 < _integer(interval_ms) <= _integer(max_backoff_ms) <= 2147483647:
            raise ValueError('invalid recovery schedule intervals')
        if max_failures is not None and not 1 <= _integer(max_failures) <= 31:
            raise ValueError("max_failures must be between 1 and 31")
        self._max_failures = max_failures
        self._storage = storage
        self._interval = interval_ms
        self._maximum = max_backoff_ms
        self._clock = clock_ms or (lambda: int(time.time() * 1000))

    @staticmethod
    def _row(extra, run_id):
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError('run id required')
        rows = extra.get('recoverySchedule', {})
        if not isinstance(rows, dict): raise ValueError('invalid recovery schedule')
        row = rows.get(run_id, {'revision': _integer(extra.get('recoveryScheduleRevision', 0)), 'failures': 0, 'notBeforeMs': 0})
        if not isinstance(row, dict) or set(row) != {'revision', 'failures', 'notBeforeMs'}:
            raise ValueError('invalid recovery schedule')
        for value in row.values(): _integer(value)
        return row

    async def check(self, run_id):
        """Observe scheduling eligibility, not permission to execute."""
        async with self._storage._transaction(read_only=True, with_journal=False) as session:
            row = self._row(session.extra, run_id)
            reason = ("retry_exhausted" if self._max_failures is not None and row['failures'] >= self._max_failures
                      else "retry_not_due" if row['notBeforeMs'] > _integer(self._clock()) else None)
            return {"revision": row['revision'] if reason is None else None, "reason": reason}

    async def ready(self, run_id):
        return (await self.check(run_id))["revision"]

    async def wake(self, run_id):
        async with self._storage._transaction(with_journal=False) as session:
            return wake_recovery_schedule(session.extra, run_id)

    async def settle(self, run_id, revision, failed):
        _integer(revision)
        if type(failed) is not bool: raise ValueError('failed must be boolean')
        async with self._storage._transaction(with_journal=False) as session:
            row = self._row(session.extra, run_id)
            if row['revision'] != revision: return False
            failures = min(row['failures'] + 1, 31) if failed else 0
            delay = min(self._maximum, self._interval * 2 ** failures)
            session.extra.setdefault('recoverySchedule', {})[run_id] = {
                'revision': next_revision(session.extra, revision), 'failures': failures,
                'notBeforeMs': _integer(_integer(self._clock()) + delay),
            }
            return True


def wake_recovery_schedule(extra, run_id, *, existing_only=False):
    """Mutate the caller's transaction; replayed commands must not call this."""
    row = SqliteRecoverySchedule._row(extra, run_id)
    if existing_only and run_id not in extra.get('recoverySchedule', {}):
        return None
    revision = next_revision(extra, row['revision'])
    extra.setdefault('recoverySchedule', {})[run_id] = {
        'revision': revision, 'failures': 0, 'notBeforeMs': 0,
    }
    return revision


def next_revision(extra, current):
    revision = _integer(max(_integer(extra.get('recoveryScheduleRevision', 0)), current) + 1)
    extra['recoveryScheduleRevision'] = revision
    return revision


def remove_recovery_schedule(extra, run_id):
    row = SqliteRecoverySchedule._row(extra, run_id)
    if run_id not in extra.get('recoverySchedule', {}):
        return False
    next_revision(extra, row['revision'])
    del extra['recoverySchedule'][run_id]
    return True
