import asyncio

import pytest

from purra.adapters.durable_memory import InMemoryLongTaskRepository
from purra.errors import ContractViolationError
from purra.long_tasks import (
    DurableExecutorRegistry, LongTaskCreateCommand, LongTaskUnitSpec,
    RecipeLongTaskDispatcher,
)
from purra.recovery import FailureCategory, FailureSignal


@pytest.mark.asyncio
@pytest.mark.parametrize("hook,mode", [
    ("classify_failure", "raise"), ("classify_failure", "invalid"),
    ("split_unit", "raise"), ("split_unit", "invalid"),
])
async def test_faulty_failure_hook_pauses_task_and_preserves_cause(hook, mode):
    class Executor:
        async def execute(self, context, signal=None):
            raise RuntimeError("execution failed")

        def classify_failure(self, error):
            if hook == "classify_failure":
                if mode == "raise":
                    raise LookupError("classifier failed")
                return "invalid failure"
            return FailureSignal(category=FailureCategory.MODEL_OUTPUT_INVALID,
                                 code="output_limit", retryable=False, part_splittable=True)

        def split_unit(self, context, error):
            if mode == "raise":
                raise LookupError("splitter failed")
            return "invalid split"

    tasks = InMemoryLongTaskRepository()
    await tasks.create("task", LongTaskCreateCommand(
        namespace="test", kind="synthetic", owner_id="test", created_by_run_id="root",
        units=(LongTaskUnitSpec(id="unit", position=0, metadata={"executor": "fixture"}),),
    ))
    dispatcher = RecipeLongTaskDispatcher(
        long_task_repository=tasks, descriptor_resolver=None,
        executor_registry=DurableExecutorRegistry({"fixture": Executor()}), worker_id="test",
    )
    async def observe(update):
        pass
    with pytest.raises(ContractViolationError) as caught:
        await asyncio.wait_for(dispatcher.execute("task", run_id="root", observer=observe), 2)
    assert caught.value.code == "long_task_failure_hook_failed"
    assert isinstance(caught.value.__cause__, LookupError if mode == "raise" else TypeError)
    assert (await tasks.load("task")).status.value == "paused"
    unit, = await tasks.list_units("task")
    assert unit.attempt == 1
    assert unit.status.value != "running"
