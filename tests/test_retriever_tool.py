from __future__ import annotations

import json
from pathlib import Path

import pytest

from purra.contracts import ExecutionState, ToolEffectState
from purra.ports import ToolRegistration
from purra.retrieval import RetrievalHit, RetrieverTool
from purra.tools import InMemoryToolCatalog, validate_tool_contract


FIXTURE = json.loads(
    (Path(__file__).parents[1] / "conformance" / "fixtures" / "retrieval.json")
    .read_text(encoding="utf-8")
)


class _MutableRetriever:
    def __init__(self) -> None:
        self.hits: list[RetrievalHit] = []
        self.requests = []
        self.signals = []

    async def retrieve(self, request, signal=None):
        self.requests.append(request)
        self.signals.append(signal)
        return tuple(self.hits)


@pytest.mark.asyncio
async def test_retriever_tool_builds_one_stable_catalog_registration() -> None:
    retriever = _MutableRetriever()
    tool = RetrieverTool(
        retriever=retriever,
        name="search_knowledge",
        description="Search configured knowledge.",
        scope=FIXTURE["successCase"]["scope"],
    )

    assert tool.registration is tool.registration
    assert isinstance(tool.registration, ToolRegistration)
    assert validate_tool_contract((tool.registration,)) == (tool.registration,)
    assert InMemoryToolCatalog((tool.registration,)).names == {
        "search_knowledge"
    }
    assert set(tool.registration.schema.parameters["properties"]) == {"query"}
    assert tool.registration.data_contract.model_owned_paths == ("query",)
    assert tool.registration.policy.mode.value == "read"
    assert tool.registration.policy.risk_level.value == "read"
    assert "data only" in tool.registration.schema.description.casefold()


@pytest.mark.asyncio
async def test_retriever_tool_maps_trusted_host_values_and_live_hits() -> None:
    row = FIXTURE["successCase"]
    retriever = _MutableRetriever()
    retriever.hits = [_hit(item) for item in row["hits"]]
    scope = {**row["scope"], "nested": {"ids": ["one"]}}
    tool = RetrieverTool(
        retriever=retriever,
        name="search_knowledge",
        description="Search configured knowledge.",
        max_results=FIXTURE["defaults"]["maxResults"],
        scope=scope,
    )
    scope["namespace"] = "other-project"
    scope["nested"]["ids"].append("two")
    signal = object()

    first = await tool.registration.handler(
        ExecutionState(
            domain={"namespace": "model-controlled-domain"},
            run_id=row["runId"],
        ),
        {"query": row["query"]},
        signal,  # type: ignore[arg-type]
    )

    assert first.effect_state is ToolEffectState.NOT_STARTED
    assert first.error_code is None
    assert json.loads(first.content) == row["modelResult"]
    request = retriever.requests[0]
    assert request.query == row["query"]
    assert request.limit == FIXTURE["defaults"]["maxResults"]
    assert request.run_id == row["runId"]
    assert request.scope["namespace"] == row["scope"]["namespace"]
    assert tuple(request.scope["nested"]["ids"]) == ("one",)
    assert retriever.signals == [signal]

    retriever.hits.append(RetrievalHit(
        id="fact-3",
        content="New live data.",
        source="fixture",
    ))
    second = await tool.registration.handler(
        ExecutionState(run_id=row["runId"]),
        {"query": row["query"]},
    )
    assert len(json.loads(second.content)["hits"]) == 3

    retriever.hits *= 4
    over_limit = await tool.registration.handler(
        ExecutionState(run_id=row["runId"]), {"query": row["query"]},
    )
    assert over_limit.error_code == "invalid_retrieval_result"


@pytest.mark.asyncio
async def test_retriever_tool_carries_versioned_external_evidence_receipts() -> None:
    retriever = _MutableRetriever()
    retriever.hits = [RetrievalHit(
        id="memory-1",
        content="Versioned memory.",
        source="mem0/scope",
        version=3,
        metadata={"evidenceId": "mem0:store:memory-1:3", "kind": "memory"},
    )]
    tool = RetrieverTool(
        retriever=retriever,
        name="search_memory",
        description="Search memory.",
    )

    result = await tool.registration.handler(
        ExecutionState(run_id="run-1"),
        {"query": "memory"},
    )

    assert [receipt.to_mapping() for receipt in result.context_evidence] == [{
        "evidenceId": "mem0:store:memory-1:3",
        "contextBlock": "search_memory",
        "source": "mem0/scope",
        "itemId": "memory-1",
        "version": 3,
        "metadata": {
            "evidenceId": "mem0:store:memory-1:3",
            "kind": "memory",
        },
    }]


def _hit(value: dict[str, object]) -> RetrievalHit:
    return RetrievalHit(
        id=str(value["id"]),
        content=str(value["content"]),
        source=str(value["source"]),
        version=(int(value["version"]) if "version" in value else None),
        score=(float(value["score"]) if "score" in value else None),
        untrusted=bool(value.get("untrusted", True)),
        metadata=value.get("metadata", {}),  # type: ignore[arg-type]
    )
