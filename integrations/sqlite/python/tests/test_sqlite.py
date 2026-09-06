import asyncio
import sqlite3
import pytest
from purra_sqlite import SqliteAgentAdapters


@pytest.mark.asyncio
async def test_reads_use_committed_snapshot_without_writer_lock_or_serialization(tmp_path, monkeypatch):
    from purra.contracts import RunCreateParams
    from purra.events import AgentEvent
    import purra_sqlite

    path = tmp_path / "readers.db"
    storage = SqliteAgentAdapters(path, scope="reader", busy_timeout=0.05)
    writer = sqlite3.connect(path, isolation_level=None)
    try:
        run_id = (await storage.runs.begin(
            RunCreateParams(None, "read", None), AgentEvent("run.started"),
        )).run_id
        expected = await storage.runs.get(run_id)

        def reject_serialization(value):
            raise AssertionError("read queries must not serialize storage")

        monkeypatch.setattr(purra_sqlite.StorageSession, "export_snapshot", reject_serialization)
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("DELETE FROM purra_state WHERE scope='reader'")
        assert await storage.runs.get(run_id) == expected
        assert await storage.outputs.list_events(run_id, after_sequence=0) == ()
        assert await storage.outputs.list_root_events(run_id, after_root_sequence=0) == ()
        assert await storage.list_running() == (run_id,)
        assert (await storage.inspect_recovery(run_id))["observations"]["checkpoint"] == "missing"
        assert (await storage.leases.get(run_id)).status == expected.status
        writer.execute("COMMIT")
        assert await storage.list_running() == ()
        assert await storage.leases.get(run_id) is None
        assert writer.execute("SELECT count(*) FROM purra_state").fetchone()[0] == 0
    finally:
        if writer.in_transaction:
            writer.rollback()
        writer.close()
        storage.close()


@pytest.mark.parametrize("version", [1, 2, 3])
def test_old_storage_is_rejected_before_any_database_write(tmp_path, version):
    path = tmp_path / "legacy.db"
    with sqlite3.connect(path) as database:
        database.execute(
            "CREATE TABLE purra_state (scope TEXT NOT NULL, sdk TEXT NOT NULL, "
            "version INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY(scope,sdk))"
        )
        database.execute("INSERT INTO purra_state VALUES (?, 'python', ?, ?)", ("legacy", version, "[]"))
    before = path.read_bytes()
    # Even a different scope must not initialize tables/indexes in an old database.
    with pytest.raises(ValueError, match="unsupported SQLite storage version"):
        SqliteAgentAdapters(path, scope="new")
    assert path.read_bytes() == before
    assert not path.with_name(path.name + "-wal").exists()

@pytest.mark.asyncio
async def test_transactions_rollback_and_scope_isolation(tmp_path):
    path = tmp_path / "state.db"
    first = SqliteAgentAdapters(path, scope="a")
    second = SqliteAgentAdapters(path, scope="a")
    other = SqliteAgentAdapters(path, scope="b")
    try:
        with pytest.raises(RuntimeError):
            async with first.transaction():
                first.extra["partial"] = True
                raise RuntimeError("rollback")
        async with second.transaction(): assert "partial" not in second.extra
        async def increment(store):
            async with store.transaction():
                store.extra["n"] = store.extra.get("n", 0) + 1
                await asyncio.sleep(0)
        await asyncio.gather(*(increment(first if i % 2 else second) for i in range(10)))
        async with first.transaction(): assert first.extra["n"] == 10
        async with other.transaction(): assert "n" not in other.extra
    finally:
        first.close(); second.close(); other.close()


@pytest.mark.asyncio
async def test_storage_ports_keep_existing_atomic_contracts(tmp_path):
    from purra.testing import assert_artifact_store_conforms, assert_long_task_repository_conforms, assert_host_adapters_conform, assert_execution_lease_store_conforms
    storage = SqliteAgentAdapters(tmp_path / "ports.db", scope="ports")
    try:
        await assert_host_adapters_conform(runs=storage.runs, outputs=storage.outputs, publisher=storage.publisher, session_id="sqlite")
        await assert_artifact_store_conforms(artifacts=storage.artifacts, claims=storage.artifact_claims, maintenance=storage.artifact_maintenance)
        await assert_long_task_repository_conforms(storage.long_tasks)
        from purra.contracts import RunCreateParams
        from purra.events import AgentEvent
        async def create_run():
            return (await storage.runs.begin(RunCreateParams(session_id=None, prompt="lease", mode=None), AgentEvent("run.started"))).run_id
        await assert_execution_lease_store_conforms(storage.leases, create_run)
    finally: storage.close()


@pytest.mark.asyncio
async def test_unknown_tool_effect_is_not_replayed_after_restart(tmp_path):
    from purra.contracts import RunCreateParams, ToolCall, ToolHandlerResult
    from purra.events import AgentEvent
    path = tmp_path / "tools.db"
    storage = SqliteAgentAdapters(path, scope="tools")
    run_id = (await storage.runs.begin(RunCreateParams(None, "write", None), AgentEvent("run.started"))).run_id
    call = ToolCall("write-1", "write", "{}")
    attempts = 0
    async def uncertain():
        nonlocal attempts
        attempts += 1
        raise RuntimeError("lost receipt")
    with pytest.raises(RuntimeError): await storage.idempotency.execute_once(run_id, call, uncertain)
    storage.close(); storage = SqliteAgentAdapters(path, scope="tools")
    try:
        with pytest.raises(Exception, match="Reconcile"):
            await storage.idempotency.execute_once(run_id, call, uncertain)
        assert attempts == 1
        await storage.reconcile_tool(run_id, call, result=ToolHandlerResult("committed"))
        assert (await storage.idempotency.execute_once(run_id, call, uncertain)).content == "committed"
        assert attempts == 1
    finally: storage.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("group", ["run", "tree", "artifact", "task"])
async def test_invalid_core_state_aborts_transaction_without_changing_rows(tmp_path, group):
    from purra.storage import dump_storage_value, load_storage_value
    path = tmp_path / "invalid-state.db"
    storage = SqliteAgentAdapters(path, scope="invalid")
    try:
        async with storage.transaction():
            storage.extra["committed"] = True
        with sqlite3.connect(path) as db:
            state = load_storage_value(db.execute("SELECT body FROM purra_state").fetchone()[0])
            state["groups"][group]["unexpected_private_field"] = {}
            db.execute("UPDATE purra_state SET body=?", (dump_storage_value(state),))
        with sqlite3.connect(path) as db:
            before = tuple(db.iterdump())
        with pytest.raises(ValueError, match="storage fields"):
            async with storage.transaction():
                pytest.fail("invalid state reached the transaction callback")
        with sqlite3.connect(path) as db:
            assert tuple(db.iterdump()) == before
    finally:
        storage.close()
