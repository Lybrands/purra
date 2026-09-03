import pytest
from purra_mem0 import MemoryWorkflow, MemorySource
from test_providers import managed
from test_review import classifier, LIMITS


@pytest.mark.asyncio
async def test_workflow_requires_policy_and_resumes_without_repeating_extraction(managed):
    create, sdk = managed
    calls = []
    memory = create(limits=LIMITS, completion=classifier(["independent"], calls))
    await memory.add("existing", source=MemorySource("old", "1"), key="old")
    messages = [{"role": "user", "content": "Use Chinese"}]
    options = dict(source=MemorySource("conversation", "1"), key="capture")
    result = await MemoryWorkflow(memory).capture(messages, **options)
    assert result.extraction.state == "complete" and result.pending_ids
    assert await memory.get(result.pending_ids[0]) is None

    async def approve(candidate, review):
        return review.proposal

    workflow = MemoryWorkflow(memory, policy=approve, policy_revision="preferences-v1")
    activated = await workflow.capture(messages, **options)
    assert not activated.pending_ids and len(activated.resolutions) == 1
    assert await memory.get(result.pending_ids[0]) is not None
    before = len(sdk.calls)
    memory.close()
    memory = create(limits=LIMITS, completion=classifier(["independent"], calls))
    assert await MemoryWorkflow(memory, policy=approve, policy_revision="preferences-v1").capture(messages, **options) == activated
    assert len(sdk.calls) == before
    memory.close()


@pytest.mark.asyncio
async def test_workflow_rejects_changed_input_and_stale_host_decision(managed):
    create, _ = managed
    memory = create(limits=LIMITS, completion=classifier(["independent"], []))
    await memory.add("existing", source=MemorySource("old", "1"), key="old")
    async def stale(candidate, review):
        await memory.set_state(candidate.id, "disabled", version=candidate.version, key="revoke")
        return review.proposal
    workflow = MemoryWorkflow(memory, policy=stale, policy_revision="v1")
    with pytest.raises(Exception, match="memory_.*"):
        await workflow.capture([{"role": "user", "content": "Chinese"}], source=MemorySource("chat", "1"), key="one")
    with pytest.raises(Exception, match="memory_idempotency_conflict"):
        await workflow.capture([{"role": "user", "content": "changed"}], source=MemorySource("chat", "1"), key="one")
    memory.close()
