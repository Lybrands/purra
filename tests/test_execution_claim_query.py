"""Public, read-only access to the supervisor's existing fencing context."""

import asyncio

import pytest

from purra.execution import current_execution_claim
from purra.execution.ownership import execution_claim


def test_current_execution_claim_is_none_without_a_bound_execution():
    assert current_execution_claim() is None


def test_current_execution_claim_reads_and_restores_the_existing_context():
    expected = ("run-a", "worker-a", 2)
    token = execution_claim.set(expected)
    try:
        assert current_execution_claim() == expected
        assert isinstance(current_execution_claim(), tuple)
    finally:
        execution_claim.reset(token)
    assert current_execution_claim() is None


@pytest.mark.asyncio
async def test_execution_query_inherits_context_without_cross_task_leakage():
    async def read_after_yield():
        await asyncio.sleep(0)
        return current_execution_claim()

    async def bound_execution(value):
        token = execution_claim.set(value)
        try:
            await asyncio.sleep(0)
            return await asyncio.create_task(read_after_yield())
        finally:
            execution_claim.reset(token)

    expected = [("run-a", "worker-a", 1), ("run-b", "worker-b", 3)]
    assert await asyncio.gather(*(bound_execution(value) for value in expected)) == expected
    assert current_execution_claim() is None


@pytest.mark.asyncio
async def test_inherited_claim_is_context_not_proof_of_parent_liveness():
    release = asyncio.Event()
    expected = ("finished-run", "former-worker", 4)

    async def inherited_reader():
        await release.wait()
        return current_execution_claim()

    token = execution_claim.set(expected)
    try:
        task = asyncio.create_task(inherited_reader())
    finally:
        execution_claim.reset(token)
    release.set()
    assert current_execution_claim() is None
    assert await task == expected
