import asyncio
import pytest
from purra_interaction import ClarificationStore, ClarificationWorkflow


def ask(store, **changes):
    return store.ask(key="draft-1", questions=[dict(id="length", prompt="选择篇幅", choices=["短篇", "长篇"], allowFreeform=False)],
                     checkpoint={"messages": [{"role": "user", "content": "写故事"}], "task": "draft-1"}, **changes)


@pytest.mark.asyncio
async def test_restart_answer_claim_and_continue_exactly_once(tmp_path):
    path = str(tmp_path / "interaction.db")
    first = ClarificationStore(path, scope="user/project")
    pending = ask(first)
    assert pending["state"] == "waiting"
    first.close()
    store = ClarificationStore(path, scope="user/project")
    competitor = ClarificationStore(path, scope="user/project")
    ready = store.answer(pending["id"], revision=1, key="answer-1", answers={"length": "短篇"})
    assert competitor.answer(pending["id"], revision=1, key="answer-1", answers={"length": "短篇"}) == ready
    calls = []
    async def submit(snapshot, key):
        calls.append(key)
        assert snapshot["answers"] == {"length": "短篇"}
        with pytest.raises(ValueError, match="state_conflict"):
            competitor.claim(snapshot["id"], revision=ready["revision"])
        return "continued-run"
    resumed = await ClarificationWorkflow(store).resume(pending["id"], revision=ready["revision"], submit=submit)
    assert resumed["runId"] == "continued-run" and len(calls) == 1
    assert "checkpoint" not in store.public(resumed) and "resumeToken" not in store.public(resumed)
    assert ask(store)["id"] == pending["id"]
    store.close(); competitor.close()


@pytest.mark.asyncio
async def test_ambiguous_submission_is_not_repeated_and_can_be_reconciled_after_restart(tmp_path):
    path = str(tmp_path / "interaction.db")
    store = ClarificationStore(path, scope="owner")
    pending = ask(store)
    ready = store.answer(pending["id"], revision=1, key="a", answers={"length": "长篇"})
    async def disconnected(snapshot, key):
        raise ConnectionError("lost receipt")
    with pytest.raises(ConnectionError):
        await ClarificationWorkflow(store).resume(pending["id"], revision=ready["revision"], submit=disconnected)
    store.close()
    store = ClarificationStore(path, scope="owner")
    saved = store.get(pending["id"])
    assert saved["state"] == "resuming"
    with pytest.raises(ValueError, match="state_conflict"):
        store.claim(saved["id"], revision=saved["revision"])
    resolved = store.reconcile(saved["id"], token=saved["resumeToken"], run_id="existing-run")
    assert resolved["state"] == "resumed"
    store.close()


def test_scope_validation_expiry_and_cancellation(tmp_path):
    path = str(tmp_path / "interaction.db")
    store = ClarificationStore(path, scope="one")
    other = ClarificationStore(path, scope="two")
    pending = ask(store)
    with pytest.raises(KeyError):
        other.get(pending["id"])
    for answers in ({"length": "unknown"}, {"extra": "短篇"}, {}):
        with pytest.raises(ValueError):
            store.answer(pending["id"], revision=1, key="a", answers=answers)
    store.cancel(pending["id"], revision=1)
    with pytest.raises(ValueError, match="state_conflict"):
        store.answer(pending["id"], revision=2, key="a", answers={"length": "短篇"})
    expired = ask(other, expires_at_ms=1)
    with pytest.raises(ValueError, match="expired"):
        other.answer(expired["id"], revision=1, key="a", answers={"length": "短篇"})
    store.close(); other.close()
