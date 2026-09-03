import asyncio
import pytest
from purra_sqlite import SqliteAgentAdapters

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

