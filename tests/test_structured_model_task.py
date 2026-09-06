import json
from pathlib import Path
from dataclasses import replace

import pytest

from purra.contracts import AgentMessage, ModelCompletion, ModelTokenUsage, ModelRequest
from purra.errors import ContractViolationError
from purra.model_execution import AgentModelTask, AgentModelTaskRunner
from purra.model_invocation import AgentModelInvocationManager, ModelInvocationContext
from purra.model_protocol import generic_capability_snapshot
from purra.structured import StructuredOutputContract


CASES = json.loads((Path(__file__).parents[1] / "conformance/fixtures/structured_model_task.json").read_text())["cases"]
OUTPUT = StructuredOutputContract("test", "1", {
    "type": "object", "properties": {"ok": {"type": "boolean"}},
    "required": ["ok"], "additionalProperties": False,
})


class Gateway:
    def __init__(self, case):
        self.case = case
        self.calls = []

    async def complete(self, messages, invocation, signal=None):
        self.calls.append((messages, invocation))
        return ModelCompletion(AgentMessage("assistant", self.case["outputs"][len(self.calls) - 1]),
            model="test", finish_reason=self.case.get("finish", "stop"),
            usage=ModelTokenUsage(10, 5) if self.case.get("usage", True) else None,
            applied_generation_limit=invocation.max_generation_tokens)

    async def stream(self, *args):
        raise AssertionError("complete only")


class Observer:
    def __init__(self, failure=None):
        self.receipts = []
        self.terminals = []
        self.failure = failure

    async def open_model_stream(self, receipt, spec):
        if self.failure == "open":
            raise OSError("storage failed")
        self.receipts.append(receipt)
        assert spec.commit_mode.value == "private"

    async def accept_provider_chunk(self, *args):
        if self.failure == "chunk":
            raise OSError("storage failed")

    async def finish_model_stream(self, stream_id, reason):
        if self.failure == "finish":
            raise OSError("storage failed")
        self.terminals.append((stream_id, "completed"))

    async def abort_model_stream(self, stream_id, code):
        if self.failure == "abort":
            raise OSError("storage failed")
        self.terminals.append((stream_id, code))


class Budget:
    def __init__(self, fail_settle=False, maximum=99):
        self.reserved = []
        self.usage = []
        self.fail_settle = fail_settle
        self.maximum = maximum

    async def reserve_model_attempt(self, run, key):
        if len(self.reserved) >= self.maximum:
            raise ContractViolationError("exhausted", code="runtime_budget_exceeded")
        self.reserved.append(key)

    async def settle_model_attempt(self, run, key, usage):
        if self.fail_settle:
            raise OSError("budget storage failed")
        self.usage.append(usage)


def runner(gateway, observer=None, budget=None, **manager_options):
    request = ModelRequest("test", "test", replace(generic_capability_snapshot(), max_generation_tokens=200))
    managed = AgentModelInvocationManager(gateway, output_observer=observer,
        budget_repository=budget, **manager_options)
    return AgentModelTaskRunner(managed, ModelInvocationContext("run", attempt_source_key="must-not-reuse"), request), AgentModelTask(request)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", CASES, ids=lambda row: row["name"])
async def test_shared_structured_task(case):
    gateway, observer, budget = Gateway(case), Observer(), Budget()
    tasks, call = runner(gateway, observer, budget)
    if "code" in case:
        with pytest.raises(Exception) as caught:
            await tasks.complete_structured((), call, output=OUTPUT, repair_attempts=case.get("repairs", 0))
        assert caught.value.code == case["code"]
        refs = caught.value.invocation_refs
        assert all(ref.settled for ref in refs)
    else:
        result = await tasks.complete_structured((), call, output=OUTPUT, repair_attempts=case.get("repairs", 0))
        assert result.value == {"ok": True}
        assert result.receipt.attempts == case["attempts"]
        assert result.receipt.root_budget == "bound"
        assert result.receipt.usage_state == ("reported" if case.get("usage", True) else "unknown")
        if case.get("usage", True):
            assert result.receipt.usage.generation_tokens == 5 * case["attempts"]
        refs = result.receipt.invocation_refs
    assert len(gateway.calls) == len(refs) == case["attempts"]
    assert len(set(budget.reserved)) == len(refs)
    assert [ref.invocation_id for ref in refs] == budget.reserved
    assert len(budget.usage) == len(refs)
    for messages, invocation in gateway.calls:
        assert invocation.output_contract == OUTPUT
        assert '"additionalProperties":false' in messages[-1].content
    assert all(receipt.to_mapping()["schemaVersion"] == 3 for receipt in observer.receipts)
    assert all(receipt.output_contract["contractDigest"] == OUTPUT.contract_digest for receipt in observer.receipts)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["open", "abort", "chunk", "finish", "budget"])
async def test_persistence_failure_never_returns_or_repairs(failure):
    text = "not-json" if failure in {"abort", "budget"} else '{"ok":true}'
    gateway = Gateway({"outputs": [text]})
    tasks, call = runner(gateway, Observer(failure), Budget(fail_settle=failure == "budget"))
    with pytest.raises(OSError) as caught:
        await tasks.complete_structured((), call, output=OUTPUT, repair_attempts=3)
    assert len(gateway.calls) == (0 if failure == "open" else 1)
    assert not caught.value.invocation_refs[-1].settled


@pytest.mark.asyncio
async def test_budget_exhaustion_stops_second_invocation():
    gateway = Gateway({"outputs": ["invalid"]})
    tasks, call = runner(gateway, Observer(), Budget(maximum=1))
    with pytest.raises(ContractViolationError) as caught:
        await tasks.complete_structured((), call, output=OUTPUT, repair_attempts=3)
    assert caught.value.code == "runtime_budget_exceeded"
    assert len(gateway.calls) == 1
    assert caught.value.invocation_refs[-1].dispatched is False


@pytest.mark.asyncio
async def test_unknown_native_mode_is_zero_call():
    gateway = Gateway({"outputs": []})
    tasks, call = runner(gateway)
    with pytest.raises(Exception) as caught:
        await tasks.complete_structured((), call, output=replace(OUTPUT, mode="native_required"))
    assert caught.value.code == "structured_output_mode_unsupported"
    assert gateway.calls == []


@pytest.mark.asyncio
async def test_low_level_runner_reports_missing_bindings():
    tasks, call = runner(Gateway({"outputs": ['{"ok":true}']}))
    result = await tasks.complete_structured((), call, output=OUTPUT)
    assert result.receipt.persistence == "none"
    assert result.receipt.root_budget == "not_bound"


@pytest.mark.asyncio
async def test_repairs_revalidate_evidence_without_replaying_candidates():
    from purra.contracts import ContextEvidenceReceipt
    from purra.model_invocation.evidence import bind_model_input_evidence
    class Validator:
        calls = 0
        async def validate_evidence(self, receipts, signal=None):
            self.calls += 1
            if self.calls == 2:
                raise ContractViolationError("stale", code="external_evidence_stale")
    validator = Validator()
    gateway = Gateway({"outputs": ["private-invalid-candidate"]})
    tasks, call = runner(gateway, Observer(), Budget(), evidence_validator=validator)
    with bind_model_input_evidence((ContextEvidenceReceipt("evidence", "context", "host", "item"),)):
        with pytest.raises(ContractViolationError) as caught:
            await tasks.complete_structured((), call, output=OUTPUT, repair_attempts=2)
    assert caught.value.code == "external_evidence_stale"
    assert validator.calls == 2 and len(gateway.calls) == 1
    assert len(caught.value.invocation_refs) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("deadline", [False, True])
async def test_bound_stop_is_kept_when_extension_omits_signal(deadline):
    import asyncio
    import time
    from purra.cancellation import OperationCanceled, ExecutionDeadlineExceeded
    started = asyncio.Event()
    root = asyncio.Event()
    class Stalled(Gateway):
        async def complete(self, messages, invocation, signal=None):
            self.calls.append(invocation)
            started.set()
            await asyncio.Event().wait()
    gateway = Stalled({})
    model = ModelRequest("test", "test", replace(generic_capability_snapshot(), max_generation_tokens=200))
    tasks = AgentModelTaskRunner(AgentModelInvocationManager(gateway, output_observer=Observer(), budget_repository=Budget()),
        ModelInvocationContext("run", deadline_at_ms=int(time.time() * 1000) + 50 if deadline else None), model, signal=root)
    job = asyncio.create_task(tasks.complete_structured((), AgentModelTask(model), output=OUTPUT, repair_attempts=3))
    await asyncio.wait_for(started.wait(), 2)
    if not deadline:
        root.set()
    with pytest.raises(ExecutionDeadlineExceeded if deadline else OperationCanceled):
        await asyncio.wait_for(job, 2)
    assert len(gateway.calls) == 1
