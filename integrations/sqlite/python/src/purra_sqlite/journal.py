"""Indexed canonical output journal, committed with the execution snapshot."""

import operator
from itertools import tee

from purra.errors import ContractViolationError
from purra.output.contracts import AgentOutputEvent
from purra.storage import dump_storage_value as dumps, load_storage_values as load_many, load_storage_value as loads

STORAGE_VERSION = 4


def _validate_row(event, row):
    if (not isinstance(event, AgentOutputEvent)
            or (event.run_id, event.root_run_id, event.sequence, event.root_sequence) != row[:4]):
        raise ValueError("invalid output journal identity or sequence")
    return event


def _load_rows(rows):
    metadata, bodies = tee(rows)
    for row, event in zip(metadata, load_many(row[4] for row in bodies), strict=True):
        yield _validate_row(event, row)


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

    def find_source(self, key):
        row = self.db.execute(
            "SELECT run_id,root_run_id,sequence,root_sequence,body FROM purra_output_events WHERE scope=? AND sdk='python' AND json_extract(body, '$[2].source_event_key[1]')=?",
            (self.scope, key),
        ).fetchone()
        if row is None:
            return None
        event = _validate_row(loads(row[4]), row)
        if event.source_event_key != key:
            raise ValueError("invalid output journal source key")
        return event

    def restore(self, session, *, root_run_id=None, lazy=False):
        if lazy and root_run_id is not None:
            rows = self.db.execute(
                "SELECT run_id,COUNT(*),MIN(sequence),MAX(sequence),MIN(root_sequence),MAX(root_sequence) "
                "FROM purra_output_events WHERE scope=? AND sdk='python' AND root_run_id=? GROUP BY run_id",
                (self.scope, root_run_id),
            ).fetchall()
            counts = {}
            for run_id, count, first, last, _, _ in rows:
                if first != 1 or last != count:
                    raise ValueError("invalid output journal sequence")
                counts[run_id] = count
            total = sum(counts.values())
            if rows and (min(row[4] for row in rows) != 1 or max(row[5] for row in rows) != total):
                raise ValueError("incomplete output journal")
            session.defer_output_events(root_run_id, counts,
                load_run=lambda run_id: self._load_history(run_id, root_run_id),
                load_root=lambda: self._load_history(root_run_id, root_run_id, root=True),
                find_source=self.find_source)
            return
        rows = self.db.execute(
            "SELECT run_id,root_run_id,sequence,root_sequence,body FROM purra_output_events WHERE scope=? AND sdk='python'"
            + (" AND root_run_id=?" if root_run_id is not None else "") + " ORDER BY root_run_id,root_sequence",
            (self.scope, root_run_id) if root_run_id is not None else (self.scope,),
        )
        session.restore_output_events(_load_rows(rows), root_run_id=root_run_id,
            find_source=self.find_source if root_run_id is not None else None)

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

    def append(self, session):
        roots, events = session.output_delta()
        self.db.executemany("INSERT INTO purra_journal_runs VALUES (?, 'python', ?, ?)",
                            ((self.scope, run_id, root_id) for run_id, root_id in roots.items()))
        self.db.executemany("INSERT INTO purra_output_events VALUES (?, 'python', ?, ?, ?, ?, ?)",
                            ((self.scope, event.run_id, event.sequence, event.root_run_id,
                              event.root_sequence, dumps(event)) for event in events))

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
