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


def _activation_blocker(db):
    for scope, sdk, body in db.execute("SELECT scope,sdk,body FROM purra_state ORDER BY scope,sdk").fetchall():
        if sdk != "python":
            return "approval_activation_foreign_sdk"
        session = StorageSession(body)
        OutputJournal(db, scope, initialize=False).restore(session)
        if session.has_unsettled_execution():
            return "approval_activation_execution_pending"
    return None


def inspect_approval_upgrade(db):
    """Observe upgrade readiness in the caller's read transaction; never authorize it."""
    version = storage_version(db)
    blocker = None if version == 5 else _activation_blocker(db)
    return {"schemaVersion": 1, "authority": "diagnosis_only", "storageVersion": version,
            "targetVersion": 5, "status": "already_enabled" if version == 5 else "blocked" if blocker else "ready",
            "blockers": [] if blocker is None else [blocker]}


async def enable_approvals(storage):
    async with storage._connection():
        if storage_version(storage._db) == 5:
            return
        blocker = _activation_blocker(storage._db)
        if blocker:
            raise ValueError(blocker)
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
