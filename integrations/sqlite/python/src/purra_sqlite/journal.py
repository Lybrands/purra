"""Indexed canonical output journal, committed with the execution snapshot."""

import operator
from collections.abc import Sequence
from itertools import tee

from purra.errors import ContractViolationError
from purra.output.contracts import AgentOutputEvent
from .codec import dumps, load_many, loads

STORAGE_VERSION = 3
JOURNAL_FIELDS = {"output_events", "root_output_events", "events_by_source_key"}


def _validate_row(event, row):
    if (not isinstance(event, AgentOutputEvent)
            or (event.run_id, event.root_run_id, event.sequence, event.root_sequence) != row[:4]):
        raise ValueError("invalid output journal identity or sequence")
    return event


def _load_rows(rows):
    metadata, bodies = tee(rows)
    for row, event in zip(metadata, load_many(row[4] for row in bodies), strict=True):
        yield _validate_row(event, row)


class _BufferedEvents(Sequence):
    """Transaction-local history with an append buffer and deferred evidence reads."""

    def __init__(self, count, load):
        self.count, self.load = count, load
        self.pending = []
        self.history = None

    def __len__(self):
        return self.count + len(self.pending)

    def _history(self):
        if self.history is None:
            self.history = tuple(self.load())
            if len(self.history) != self.count:
                raise ValueError("incomplete output journal")
        return self.history

    def __iter__(self):
        yield from self._history()
        yield from self.pending

    def __getitem__(self, index):
        if isinstance(index, slice):
            start, stop, step = index.indices(len(self))
            if step > 0 and start >= self.count:
                return self.pending[start - self.count:stop - self.count:step]
            return tuple(self)[index]
        index = operator.index(index)
        if index < 0:
            index += len(self)
        if index < 0 or index >= len(self):
            raise IndexError(index)
        return self.pending[index - self.count] if index >= self.count else self._history()[index]

    def append(self, event):
        self.pending.append(event)


class _SourceEvents(dict):
    def __init__(self, journal):
        super().__init__()
        self.journal = journal

    def __missing__(self, key):
        event = self._load(key)
        if event is None:
            raise KeyError(key)
        return event

    def _load(self, key):
        row = self.journal.db.execute(
            "SELECT run_id,root_run_id,sequence,root_sequence,body FROM purra_output_events WHERE scope=? AND sdk='python' AND json_extract(body, '$[2].source_event_key[1]')=?",
            (self.journal.scope, key),
        ).fetchone()
        if row is None:
            return None
        event = _validate_row(loads(row[4]), row)
        if event.source_event_key != key:
            raise ValueError("invalid output journal source key")
        self[key] = event
        return event

    def get(self, key, default=None):
        if key in self:
            return self[key]
        event = self._load(key)
        return default if event is None else event


class OutputJournal:
    def __init__(self, db, scope):
        self.db, self.scope = db, scope
        db.execute("""CREATE TABLE IF NOT EXISTS purra_journal_runs (
            scope TEXT NOT NULL, sdk TEXT NOT NULL, run_id TEXT NOT NULL,
            root_run_id TEXT NOT NULL, PRIMARY KEY(scope,sdk,run_id),
            FOREIGN KEY(scope,sdk) REFERENCES purra_state(scope,sdk) ON DELETE CASCADE)""")
        db.execute("""CREATE TABLE IF NOT EXISTS purra_output_events (
            scope TEXT NOT NULL, sdk TEXT NOT NULL, run_id TEXT NOT NULL,
            sequence INTEGER NOT NULL, root_run_id TEXT NOT NULL,
            root_sequence INTEGER NOT NULL, body TEXT NOT NULL,
            PRIMARY KEY(scope,sdk,run_id,sequence),
            UNIQUE(scope,sdk,root_run_id,root_sequence),
            FOREIGN KEY(scope,sdk,run_id) REFERENCES purra_journal_runs(scope,sdk,run_id)
                ON DELETE CASCADE)""")
        db.execute("""CREATE UNIQUE INDEX IF NOT EXISTS purra_python_output_source
            ON purra_output_events(scope, json_extract(body, '$[2].source_event_key[1]'))
            WHERE sdk='python'""")
        db.execute("""CREATE INDEX IF NOT EXISTS purra_output_sequence_cover
            ON purra_output_events(scope,sdk,root_run_id,run_id,sequence,root_sequence)""")
        db.execute("""CREATE INDEX IF NOT EXISTS purra_journal_roots
            ON purra_journal_runs(scope,sdk,root_run_id,run_id)""")

    def restore(self, state, *, root_run_id=None, lazy=False):
        if lazy and root_run_id is not None:
            self._restore_deferred(state, root_run_id)
            return
        if root_run_id is not None:
            state.events_by_source_key = _SourceEvents(self)
        rows = self.db.execute(
            "SELECT run_id,root_run_id,sequence,root_sequence,body FROM purra_output_events WHERE scope=? AND sdk='python'"
            + (" AND root_run_id=?" if root_run_id is not None else "")
            + " ORDER BY root_run_id,root_sequence",
            (self.scope, root_run_id) if root_run_id is not None else (self.scope,),
        )
        for event in _load_rows(rows):
            if event.run_id not in state.runs:
                raise ValueError("output journal references a missing Run")
            if event.sequence != len(state.output_events.get(event.run_id, ())) + 1 or event.root_sequence != len(state.root_output_events.get(event.root_run_id, ())) + 1:
                raise ValueError("invalid output journal sequence")
            state.output_events.setdefault(event.run_id, []).append(event)
            state.root_output_events.setdefault(event.root_run_id, []).append(event)
            state.events_by_source_key[event.source_event_key] = event
        if any(len(state.output_events.get(run_id, ())) != sequence
               for run_id, sequence in state.sequences.items()
               if root_run_id is None or (state.runs[run_id].params.root_run_id or run_id) == root_run_id):
            raise ValueError("incomplete output journal")

    def _restore_deferred(self, state, root_run_id):
        # Check sequence completeness using indexed columns before accepting writes.
        # Event bodies remain in SQLite until a Core rule asks for history evidence.
        rows = self.db.execute(
            "SELECT run_id,COUNT(*),MIN(sequence),MAX(sequence),MIN(root_sequence),MAX(root_sequence) "
            "FROM purra_output_events WHERE scope=? AND sdk='python' AND root_run_id=? GROUP BY run_id",
            (self.scope, root_run_id),
        ).fetchall()
        counts = {}
        for run_id, count, first, last, _, _ in rows:
            record = state.runs.get(run_id)
            if record is None or (record.params.root_run_id or run_id) != root_run_id:
                raise ValueError("output journal references a missing Run or wrong Root")
            if first != 1 or last != count:
                raise ValueError("invalid output journal sequence")
            counts[run_id] = count
        total = sum(counts.values())
        if (total != state.root_sequences.get(root_run_id, 0)
                or (rows and (min(row[4] for row in rows) != 1 or max(row[5] for row in rows) != total))):
            raise ValueError("incomplete output journal")
        for run_id, record in state.runs.items():
            if (record.params.root_run_id or run_id) != root_run_id:
                continue
            count = counts.get(run_id, 0)
            if count != state.sequences.get(run_id, 0):
                raise ValueError("incomplete output journal")
            state.output_events[run_id] = _BufferedEvents(
                count, lambda run_id=run_id: self._load_history(run_id, root_run_id),
            )
        state.root_output_events[root_run_id] = _BufferedEvents(
            total, lambda: self._load_history(root_run_id, root_run_id, root=True),
        )
        state.events_by_source_key = _SourceEvents(self)

    def _load_history(self, run_id, root_run_id, *, root=False):
        identity, sequence = ("root_run_id", "root_sequence") if root else ("run_id", "sequence")
        rows = self.db.execute(
            f"SELECT run_id,root_run_id,sequence,root_sequence,body FROM purra_output_events WHERE scope=? AND sdk='python' AND {identity}=? ORDER BY {sequence}",
            (self.scope, run_id),
        )
        for expected, event in enumerate(_load_rows(rows), 1):
            if (event.root_run_id != root_run_id or (not root and event.run_id != run_id)
                    or (event.root_sequence if root else event.sequence) != expected):
                raise ValueError("invalid output journal sequence")
            yield event

    def append(self, state, prior_sequences, prior_runs):
        for run_id in state.runs.keys() - prior_runs:
            record = state.runs[run_id]
            self.db.execute(
                "INSERT INTO purra_journal_runs VALUES (?, 'python', ?, ?)",
                (self.scope, run_id, record.params.root_run_id or run_id),
            )
        for run_id, events in state.output_events.items():
            self.db.executemany(
                "INSERT INTO purra_output_events VALUES (?, 'python', ?, ?, ?, ?, ?)",
                ((self.scope, run_id, event.sequence, event.root_run_id,
                  event.root_sequence, dumps(event))
                 for event in events[prior_sequences.get(run_id, 0):]),
            )

    def read(self, run_id, after, limit, *, root=False):
        if after < 0:
            raise ValueError("after root sequence must be non-negative" if root else "after sequence must be non-negative")
        if limit <= 0:
            raise ValueError("limit must be positive")
        limit = operator.index(limit)
        row = self.db.execute(
            "SELECT version FROM purra_state WHERE scope=? AND sdk='python'", (self.scope,),
        ).fetchone()
        if row and row[0] != STORAGE_VERSION:
            raise ValueError("unsupported SQLite storage version")
        run = self.db.execute(
            "SELECT root_run_id FROM purra_journal_runs WHERE scope=? AND sdk='python' AND run_id=?",
            (self.scope, run_id),
        ).fetchone() if row else None
        if run is None:
            raise ContractViolationError(f"run {run_id!r} does not exist", code="run_not_found")
        if root and run[0] != run_id:
            raise ContractViolationError("Root journal query requires a Root Run", code="run_scope_conflict")
        identity, sequence = ("root_run_id", "root_sequence") if root else ("run_id", "sequence")
        rows = self.db.execute(
            f"SELECT run_id,root_run_id,sequence,root_sequence,body FROM purra_output_events WHERE scope=? AND sdk='python' AND {identity}=? AND {sequence}>? ORDER BY {sequence} LIMIT ?",
            (self.scope, run_id, min(after, 2**63 - 1), min(limit, 2**63 - 1)),
        )
        return tuple(_load_rows(rows))
