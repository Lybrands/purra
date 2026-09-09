import json

import pytest
from purra_mem0 import (MemoryCaptureAuthorization, MemoryError, MemoryWorkflow,
                        MemoryRef, MemoryResolution, MemorySource, memory_capture_intent)
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


@pytest.mark.asyncio
async def test_capture_authorization_is_checked_before_provider_and_bound_to_retry(managed):
    create, sdk = managed
    memory = create()
    messages = [{"role": "user", "content": "Remember Chinese replies"}]
    source = MemorySource("conversation", "1")
    workflow = MemoryWorkflow(memory, capture_policy_id="capture", capture_policy_revision="v3")

    with pytest.raises(MemoryError, match="memory_capture_authorization_required"):
        await workflow.capture(messages, source=source, key="authorized")
    wrong = MemoryCaptureAuthorization(
        memory_capture_intent([{"role": "user", "content": "other"}], source=source),
        "capture", "v3", "host:user", "decision-1")
    with pytest.raises(MemoryError, match="memory_capture_authorization_mismatch"):
        await workflow.capture(messages, source=source, key="authorized", authorization=wrong)
    assert sdk.calls == []

    authorization = MemoryCaptureAuthorization(
        memory_capture_intent(messages, source=source),
        "capture", "v3", "host:user", "decision-1")
    await memory.record_capture_authorization(
        authorization, key="authorize:decision-1", expires_at="2099-01-01T00:00:00Z")
    result = await workflow.capture(messages, source=source, key="authorized", authorization=authorization)
    plan = json.loads(memory._journal.db.execute(
        "SELECT plan FROM purra_mem0_ops WHERE json_extract(plan, '$.kind')='extract'"
    ).fetchone()[0])
    assert plan["capture_authorization"] == {
        "intent_digest": authorization.intent_digest,
        "policy_id": "capture",
        "policy_revision": "v3",
        "principal_id": "host:user",
        "decision_id": "decision-1",
    }
    assert "Remember Chinese replies" not in json.dumps(plan)
    before_adds = sum(call[0] == "add" for call in sdk.calls)
    before_usage = memory.budget_usage()
    assert result.extraction.state == "complete"
    assert await workflow.capture(messages, source=source, key="authorized", authorization=authorization) == result
    assert sum(call[0] == "add" for call in sdk.calls) == before_adds
    assert memory.budget_usage() == before_usage

    replacement = MemoryCaptureAuthorization(
        authorization.intent_digest, "capture", "v3", "host:user", "decision-2")
    with pytest.raises(MemoryError, match="memory_idempotency_conflict"):
        await workflow.capture(messages, source=source, key="authorized", authorization=replacement)
    assert sum(call[0] == "add" for call in sdk.calls) == before_adds
    assert memory.budget_usage() == before_usage
    memory.close()


@pytest.mark.asyncio
async def test_capture_authorization_validity_and_revocation_persist(managed):
    create, sdk = managed
    memory = create()
    messages = [{"role": "user", "content": "Remember this"}]
    source = MemorySource("conversation", "1")
    intent = memory_capture_intent(messages, source=source)
    workflow = MemoryWorkflow(memory, capture_policy_id="capture", capture_policy_revision="v3")

    future = MemoryCaptureAuthorization(intent, "capture", "v3", "host:user", "future")
    await memory.record_capture_authorization(future, key="authorize:future",
                                              valid_from="2099-01-01T00:00:00Z")
    with pytest.raises(MemoryError, match="memory_capture_authorization_not_yet_valid"):
        await workflow.capture(messages, source=source, key="future", authorization=future)

    expired = MemoryCaptureAuthorization(intent, "capture", "v3", "host:user", "expired")
    await memory.record_capture_authorization(expired, key="authorize:expired",
                                              expires_at="2000-01-01T00:00:00Z")
    with pytest.raises(MemoryError, match="memory_capture_authorization_expired"):
        await workflow.capture(messages, source=source, key="expired", authorization=expired)
    assert sdk.calls == []

    active = MemoryCaptureAuthorization(intent, "capture", "v3", "host:user", "active")
    await memory.record_capture_authorization(active, key="authorize:active",
                                              expires_at="2099-01-01T00:00:00Z")
    conflicting = MemoryCaptureAuthorization(intent, "capture", "v3", "host:other", "active")
    with pytest.raises(MemoryError, match="memory_capture_authorization_conflict"):
        await memory.record_capture_authorization(conflicting, key="authorize:active:conflict",
                                                  expires_at="2099-01-01T00:00:00Z")
    captured = await workflow.capture(messages, source=source, key="active", authorization=active)
    item_id = captured.extraction.ids[0]
    await memory.revoke_capture_authorization("capture", "active", key="revoke:active")
    assert await memory.get(item_id, include_inactive=True) is None
    usage = memory.budget_usage()
    with pytest.raises(MemoryError, match="memory_capture_authorization_revoked"):
        await memory.review(MemoryRef(item_id, 1), key="review:revoked", authorization=active)
    with pytest.raises(MemoryError, match="memory_capture_authorization_revoked"):
        await memory.resolve(MemoryResolution("independent", (MemoryRef(item_id, 1),), item_id),
                             key="resolve:revoked", authorization=active)
    assert memory.budget_usage() == usage
    with pytest.raises(MemoryError, match="memory_capture_authorization_revoked"):
        await memory.record_capture_authorization(active, key="authorize:active:again")
    memory.close()

    restored = create()
    assert await restored.get(item_id, include_inactive=True) is None
    assert restored.operation("revoke:active").state == "complete"
    restored.close()


def test_capture_intent_digest_is_shared_with_typescript():
    assert memory_capture_intent(
        [{"role": "user", "content": "作者😺"}],
        source=MemorySource("chat", "1"),
        metadata={"language": "中文"},
        expires_at="2026-09-10T00:00:00Z",
    ) == "sha256:bb0c82494bb1847cb823cb38d44cc4e33baf73d31b05d97c1984d07cb58a9006"
