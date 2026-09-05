"""Real mem0ai 2.0.19 + local Qdrant/SQLite; deterministic provider substitutes.

Run in an isolated environment with purra-mem0 installed. No real provider,
credentials, network server or quality benchmark is involved.
"""

import asyncio
import json
import os
import runpy
import tempfile
from dataclasses import replace
from pathlib import Path


async def check(root):
    os.environ["MEM0_TELEMETRY"] = "false"
    os.environ["MEM0_DIR"] = str(root / "sdk")
    from purra_mem0 import Mem0Memory, MemoryScope, MemorySource, MemoryRef, MemoryResolution, MemoryBudget, MemoryProviders, EmbeddingResult, create_managed_client, MemoryError
    from purra.contracts import AgentMessage, ModelCompletion, ModelTokenUsage
    from purra.evidence import ContextEvidenceReceipt
    from purra.retrieval import RetrievalRequest

    calls = {"llm": 0, "embedding": 0}
    captured = []

    def vector(text):
        calls["embedding"] += 1
        return [float("简短" in text), float("详细" in text), float("中文" in text), 1.0]

    async def embed(texts, signal):
        return EmbeddingResult([vector(text) for text in texts], sum(map(len, texts)))

    async def generate_response(messages, cap, signal):
        calls["llm"] += 1
        captured.append(messages)
        payload = {"memory": [{"text": "用户希望使用中文回复。", "entities": []}]}
        if messages[0].content.startswith("Review a pending memory"):
            related = json.loads(messages[1].content)["related"]
            payload = {"relations": [{"item": r["item"], "kind": "independent"} for r in related]}
        return ModelCompletion(message=AgentMessage(role="assistant", content=json.dumps(payload, ensure_ascii=False)),
                               model="fixture", finish_reason="stop", applied_generation_limit=cap, usage=ModelTokenUsage(100, 20))

    providers = MemoryProviders(MemoryBudget("sdk-test", 4, 64, 100_000, 8192, 2048), generate_response, embed)

    def sdk():
        return create_managed_client(config={
            "vector_store": {"provider": "qdrant", "config": {"collection_name": "memory", "path": str(root / "vectors"), "embedding_model_dims": 4}},
            "history_db_path": str(root / "history.db"),
        }, embedding_dims=4)

    def close_sdk(client):
        client = client.sdk
        client.db.close()
        client.vector_store.client.close()
        if client._entity_store is not None:
            client._entity_store.client.close()

    client = sdk()
    memory = Mem0Memory(client=client, scope=MemoryScope("user", "project"), journal_path=str(root / "journal.db"), allow_inference=True, providers=providers)
    source = MemorySource("message:1", "1")
    request = RetrievalRequest(query="简短", limit=8)
    try:
        admin = await memory.add("管理员待审记录。", source=source, key="admin", state="pending",
                                 metadata={"kind": "note", "pinned": False})
        admin_id, = admin.ids
        assert await memory.get(admin_id) is None
        before = dict(calls)
        await memory.annotate(admin_id, {"kind": "note", "pinned": True}, version=1, key="admin-pin")
        await memory.set_state(admin_id, "active", version=2, key="admin-accept")
        assert calls == before
        record = await memory.get(admin_id)
        assert record.version == 3 and record.metadata["pinned"] is True
        assert (await memory.list(filters={"pinned": True})).items[0].id == admin_id
        assert client.sdk.get(admin_id)["metadata"]["purra_metadata"]["pinned"] is False
        await memory.delete(admin_id, version=3, key="admin-delete")
        direct = await memory.add("用户喜欢简短回复。", source=source, key="direct")
        assert calls["llm"] == 0
        candidate = await memory.extract([{"role": "user", "content": "请使用中文回复。"}], source=source, key="extract")
        assert calls["llm"] == 1 and len(candidate.ids) == 1
        assert "用户喜欢简短回复。" not in captured[0][1].content  # separate extraction session
        assert candidate.usage.llm_calls == 1 and candidate.usage.reported_output_tokens == 20
        assert {hit.id for hit in await memory.retrieve(request)} == set(direct.ids)
        reviewed = await memory.review(MemoryRef(candidate.ids[0], 1), key="semantic-review")
        assert reviewed.review.proposal.kind == "independent"
        assert reviewed.review.proposal.review_key == "semantic-review"
        assert reviewed.usage.llm_calls == reviewed.usage.embedding_calls == 1
        assert await memory.get(candidate.ids[0]) is None  # advice alone cannot activate
        before = dict(calls)
        assert await memory.review(MemoryRef(candidate.ids[0], 1), key="semantic-review") == reviewed
        assert calls == before
        applied = await memory.resolve(reviewed.review.proposal, key="apply-review")
        assert applied.resolution.review_key == "semantic-review"
        assert (await memory.get(candidate.ids[0])).state == "active"
        assert len(await memory.retrieve(request)) == 2
        await memory.update(direct.ids[0], "复杂问题需要详细解释。", source=MemorySource("correction:1", "2"), version=1, key="update")
        assert (await memory.get(direct.ids[0])).version == 2
        assert len(await memory.history(direct.ids[0])) >= 2
    finally:
        await memory.drain()
        memory.close()
        close_sdk(client)

    client = sdk()
    restored = Mem0Memory(client=client, scope=MemoryScope("user", "project"), journal_path=str(root / "journal.db"), providers=providers, allow_inference=True)
    try:
        assert (await restored.get(direct.ids[0])).text == "复杂问题需要详细解释。"
        assert await restored.add("用户喜欢简短回复。", source=source, key="direct") == direct
        await restored.delete(direct.ids[0], version=2, key="delete")
        assert await restored.get(direct.ids[0], include_inactive=True) is None
        assert await restored.history(direct.ids[0])  # logical delete does not promise erasure
        assert len(await restored.retrieve(request)) == 1
        denied = Mem0Memory(client=client, scope=MemoryScope("user", "project"), journal_path=str(root / "journal.db"), allow_inference=True,
                            providers=replace(providers, budget=replace(providers.budget, key="denied", max_embedding_calls=1)))
        try:
            try:
                await denied.extract([{"role": "user", "content": "我使用中文。"}], source=source, key="denied")
                raise AssertionError("SDK swallowed a denial")
            except MemoryError as error:
                assert error.code == "memory_budget_exceeded"
            assert denied.operation("denied").state == "unknown"
            assert denied.budget_usage().embedding_calls == 1
            assert (await denied.discard_extraction("denied", writer_stopped=True)).state == "discarded"
        finally:
            await denied.drain()
            denied.close()
        hit = (await restored.retrieve(request))[0]
        evidence = (ContextEvidenceReceipt(evidence_id=hit.metadata["evidenceId"], context_block="memory",
                                          source=hit.source, item_id=hit.id, version=hit.version),)
        await restored.validate_evidence(evidence)
        before = dict(calls)
        withdrawn = await restored.revoke_source(source.id, revision=source.revision, key="withdraw")
        assert withdrawn.usage.llm_calls == withdrawn.usage.embedding_calls == 0 and calls == before
        assert restored.is_source_revoked(source)
        assert await restored.get(hit.id, include_inactive=True) is None
        assert not await restored.retrieve(request)
        try:
            await restored.validate_evidence(evidence)
            raise AssertionError("withdrawn checkpoint evidence accepted")
        except MemoryError as error:
            assert error.code == "memory_context_stale"
        await restored.update(hit.id, "用户希望中文回复，引用保留原文。", source=MemorySource(source.id, "2"), version=2, key="correct")
        corrected = (await restored.retrieve(request))[0]
        assert corrected.version == 3
        await restored.validate_evidence((ContextEvidenceReceipt(evidence_id=corrected.metadata["evidenceId"], context_block="memory",
                                                                source=corrected.source, item_id=corrected.id, version=3),))
        await restored.revoke_source(source.id, key="withdraw-all")
        before = dict(calls)
        try:
            await restored.add("不得重新写入。", source=MemorySource(source.id, "future"), key="reingest")
            raise AssertionError("withdrawn source reingested")
        except MemoryError as error:
            assert error.code == "memory_source_revoked"
        assert calls == before and not (await restored.list()).items
        await restored.delete(hit.id, version=3, key="cleanup")
        assert await restored.history(hit.id)  # source withdrawal is not audit erasure
        old = await restored.add("用户希望默认英文回复。", source=MemorySource("old", "1"), key="old")
        new = await restored.extract([{"role": "user", "content": "今后默认中文回复。"}], source=MemorySource("new", "1"), key="new")
        refs = (MemoryRef(old.ids[0], 1), MemoryRef(new.ids[0], 1))
        before = dict(calls)
        resolved = await restored.resolve(MemoryResolution("supersede", refs, new.ids[0]), key="resolve")
        assert calls == before and resolved.usage.embedding_calls == resolved.usage.llm_calls == 0
        # SDK payload is unchanged; the journal atomically owns resolved visibility.
        assert client.sdk.get(new.ids[0])["metadata"]["purra_state"] == "pending"
        reopened = Mem0Memory(client=client, scope=MemoryScope("user", "project"), journal_path=str(root / "journal.db"), providers=providers)
        try:
            assert reopened.operation("resolve") == resolved
            assert [h.id for h in await reopened.retrieve(request)] == list(new.ids)
            assert (await reopened.get(new.ids[0])).resolution_key == "resolve"
            await reopened.resolve(MemoryResolution("conflict", tuple(MemoryRef(r.id, 2) for r in refs)), key="conflict")
            assert not await reopened.retrieve(request)
            await reopened.resolve(MemoryResolution("duplicate", tuple(MemoryRef(r.id, 3) for r in refs), old.ids[0]), key="dedupe")
            await reopened.update(old.ids[0], "用户允许中英文回复。", source=MemorySource("accepted", "1"), version=4, key="after-review")
            assert (await reopened.get(old.ids[0])).version == 5
            assert (await reopened.get(old.ids[0])).resolution_key is None
            await reopened.delete(new.ids[0], version=4, key="retired-cleanup")
        finally:
            await reopened.drain()
            reopened.close()
    finally:
        await restored.drain()
        restored.close()
        close_sdk(client)
    # Exercise the evaluator's entire storage/context/grade path without claiming
    # real semantic quality. All classifications and vectors below are substitutes.
    evaluate_case = runpy.run_path(str(Path(__file__).with_name("evaluate.py")))["evaluate_case"]
    fixture = json.loads((Path(__file__).resolve().parents[2] / "fixtures/evaluation.json").read_text())
    class FixtureTransport:
        config = {"embedding": {"dimensions": 2}}
        async def complete(self, messages, cap, signal=None):
            if messages[0].content.startswith("Review a pending memory"):
                body = json.loads(messages[1].content)
                payload = {"relations": [{"item": row["item"], "kind": self.test["review_kinds"][0] if row["text"] == self.test.get("seed") else "independent"} for row in body["related"]]}
            elif self.phase == "ingestion":
                payload = {"memory": [{"text": self.test["incoming"], "entities": []}]}
            else:
                body = json.loads(messages[1].content)
                supporting = next(((answer, row["id"]) for answer in self.test["answers"] for row in body["memories"] if answer in row["text"]), None)
                payload = {"answer": None if supporting is None else supporting[0], "evidence": [] if supporting is None else [supporting[1]]}
            return ModelCompletion(message=AgentMessage(role="assistant", content=json.dumps(payload, ensure_ascii=False)),
                                   model="fixture", finish_reason="stop", applied_generation_limit=cap, usage=ModelTokenUsage(10, 20))
        async def embed(self, texts, signal):
            return EmbeddingResult([[1.0, 0.0] for _ in texts], sum(map(len, texts)))
    transport = FixtureTransport()
    for index, case in enumerate(fixture["cases"]):
        transport.test = case
        result = await evaluate_case(case, fixture, root / case["id"], transport, index)
        assert result["passed"], {"case": case["id"], "checks": result["checks"]}
    print(json.dumps({"sdk": "mem0ai==2.0.19", "checks": "CRUD/inference/restart/idempotency/managed-budget/swallowed-denial/source-withdrawal/correction/evidence/atomic-resolution/semantic-review/evaluation-harness", "provider": "deterministic fixture via supported LangChain config", "calls": calls, "offline_evaluation_cases": len(fixture["cases"])}))


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="purra-mem0-sdk-") as temporary:
        asyncio.run(check(Path(temporary)))
