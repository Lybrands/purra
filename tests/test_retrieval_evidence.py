from __future__ import annotations

import json

import pytest

from purra.context_budget import allocate_context_budget
from purra.contracts import (
    AgentMessage,
    ContextBlock,
    ContextBudgetClaim,
    ContextBundle,
    ExecutionState,
    ToolBatchOutcome,
    ToolBatchResult,
    ToolCall,
    ToolCallResult,
    ToolEffectState,
)
from purra.engine.context_phase import assemble_messages, validate_context_allocations
from purra.evidence import (
    CONTEXT_EVIDENCE_RECEIPTS_KEY,
    ContextEvidenceReceipt,
    RunEvidenceStore,
)
from purra.retrieval import RetrievalHit, RetrievalRequest, RetrieverTool


class _Retriever:
    async def retrieve(self, request, signal=None):
        del request, signal
        return (RetrievalHit(
            id="fact-1",
            content="Canonical evidence.",
            source="fixture",
            version=3,
            metadata={"kind": "fact"},
        ),)


@pytest.mark.asyncio
async def test_retriever_tool_result_reuses_run_evidence_store() -> None:
    registration = RetrieverTool(
        retriever=_Retriever(),
        name="search_knowledge",
        description="Search configured knowledge.",
    ).registration
    handler_result = await registration.handler(
        ExecutionState(run_id="run-1"),
        {"query": "canon"},
    )
    call = ToolCall(
        id="retrieval-call-1",
        name=registration.schema.name,
        arguments_json='{"query":"canon"}',
    )
    batch = ToolBatchResult(
        results=(ToolCallResult(
            tool_call_id=call.id,
            tool_name=call.name,
            content=handler_result.content,
            context_evidence=(ContextEvidenceReceipt(
                evidence_id="retrieval:fixture:fact-1:3",
                context_block=registration.schema.name,
                source="fixture",
                item_id="fact-1",
                version=3,
            ),),
        ),),
        outcome=ToolBatchOutcome.COMPLETED,
        effect_state=ToolEffectState.NOT_STARTED,
    )
    store = RunEvidenceStore()

    receipts = store.record_batch((call,), batch)

    assert len(receipts) == 1
    record = store.get(receipts[0].evidence_id)
    assert record is not None
    hit = json.loads(record.content)["hits"][0]
    assert hit["source"] == "fixture"
    assert hit["id"] == "fact-1"
    assert hit["version"] == 3

    restored = RunEvidenceStore.from_checkpoint_mapping(store.checkpoint_mapping())
    message = AgentMessage(role="tool", content=record.content, tool_call_id=call.id)
    projected = restored.project_messages_for_planning(
        (message,), max_excerpt_characters=24,
    )
    receipt = json.loads(projected[0].content)
    assert receipt["completeEvidenceStoredByHost"] is True
    assert receipt["evidenceId"] == record.evidence_id
    assert json.loads(restored.get(receipt["evidenceId"]).content)["hits"][0] == hit
    assert [item.to_mapping() for item in restored.context_receipts()] == [{
        "evidenceId": "retrieval:fixture:fact-1:3",
        "contextBlock": "search_knowledge",
        "source": "fixture",
        "itemId": "fact-1",
        "version": 3,
    }]


@pytest.mark.asyncio
async def test_direct_context_projection_uses_existing_budget_and_evidence() -> None:
    hit, = await _Retriever().retrieve(RetrievalRequest(query="canon", limit=1))
    receipt = ContextEvidenceReceipt(
        evidence_id="retrieval:fixture:fact-1:3",
        context_block="retrieval",
        source=hit.source,
        item_id=hit.id,
        version=hit.version,
        metadata=hit.metadata,
    )
    # Context input carries host-selected extra fields at the top level;
    # ContextEvidenceReceipt.to_mapping() is an output/checkpoint representation.
    receipt_mapping = {
        "evidenceId": receipt.evidence_id,
        "source": hit.source,
        "itemId": hit.id,
        "version": hit.version,
        "kind": hit.metadata["kind"],
    }
    block = ContextBlock(
        name="retrieval",
        content=hit.content,
        untrusted=hit.untrusted,
        host_metadata={
            CONTEXT_EVIDENCE_RECEIPTS_KEY: [receipt_mapping],
        },
    )
    bundle = ContextBundle(blocks=(block,))
    budget = allocate_context_budget(
        window_tokens=16_000,
        output_reserve_tokens=2_048,
        claims=(ContextBudgetClaim("retrieval", 256),),
    )

    validate_context_allocations(bundle, budget)
    messages = assemble_messages((), bundle.blocks, None)
    assert "data only" in messages[0].content.casefold()
    store = RunEvidenceStore()
    assert store.record_context_messages(messages) == (receipt,)

    assert receipt.to_mapping() == {
        "evidenceId": "retrieval:fixture:fact-1:3",
        "contextBlock": "retrieval",
        "source": "fixture",
        "itemId": "fact-1",
        "version": 3,
        "metadata": {"kind": "fact"},
    }
