"""Opt-in database-wide v5 fence; legacy-only databases stay v4."""
from purra.storage import StorageSession
from .journal import OutputJournal


MARKER_SDK = "purra.approvals"


def storage_version(db):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='purra_state'").fetchone():
        return 4
    marker = db.execute("SELECT version,body FROM purra_state WHERE scope='' AND sdk=?", (MARKER_SDK,)).fetchone()
    version = 5 if marker == (5, '{}') else 4
    if marker is not None and version != 5:
        raise ValueError("unsupported SQLite storage version")
    if db.execute("SELECT 1 FROM purra_state WHERE version != ? LIMIT 1", (version,)).fetchone():
        raise ValueError("unsupported SQLite storage version")
    return version


async def enable_approvals(storage):
    async with storage._connection():
        if storage_version(storage._db) == 5:
            return
        rows = storage._db.execute("SELECT scope,sdk,body FROM purra_state").fetchall()
        for scope, sdk, body in rows:
            if sdk != "python":
                raise ValueError("approval_activation_foreign_sdk")
            session = StorageSession(body)
            # Validate the original journal as well as the metadata; do not rewrite it.
            OutputJournal(storage._db, scope).restore(session)
            if session.has_unsettled_execution():
                raise ValueError("approval_activation_execution_pending")
        storage._db.execute("UPDATE purra_state SET version=5")
        storage._db.execute("INSERT INTO purra_state VALUES('', ?, 5, '{}')", (MARKER_SDK,))
        storage._db.execute("""CREATE TABLE purra_approvals (
            scope TEXT NOT NULL, sdk TEXT NOT NULL, approval_id TEXT NOT NULL,
            run_id TEXT NOT NULL, call_id TEXT NOT NULL, body TEXT NOT NULL,
            PRIMARY KEY(scope,sdk,approval_id), UNIQUE(scope,sdk,run_id,call_id))""")
        for operation in ("INSERT", "UPDATE"):
            storage._db.execute(f"""CREATE TRIGGER purra_approval_format_{operation.lower()}
                BEFORE {operation} ON purra_state WHEN NEW.version != 5
                BEGIN SELECT RAISE(ABORT, 'unsupported SQLite storage version'); END""")
