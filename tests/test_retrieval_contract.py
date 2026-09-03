from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from purra.retrieval import (
    RetrievalError,
    RetrievalHit,
    RetrievalRequest,
    Retriever,
)
from purra.testing import assert_retriever_conforms


FIXTURE = json.loads(
    (Path(__file__).parents[1] / "conformance" / "fixtures" / "retrieval.json")
    .read_text(encoding="utf-8")
)


class _Retriever:
    async def retrieve(self, request, signal=None):
        del request, signal
        return ()


def test_retrieval_contracts_are_validated_and_immutable() -> None:
    row = FIXTURE["successCase"]
    request = RetrievalRequest(
        query=f"  {row['query']}  ",
        limit=FIXTURE["defaults"]["maxResults"],
        run_id=row["runId"],
        scope=row["scope"],
    )
    hit = _hit(row["hits"][0])

    assert request.query == row["query"]
    assert request.limit == 8
    assert dict(request.scope) == row["scope"]
    assert hit.version == 3
    assert hit.score == 0.92
    assert hit.untrusted is False
    assert dict(hit.metadata) == {"kind": "fact"}
    with pytest.raises(TypeError):
        request.scope["namespace"] = "other"  # type: ignore[index]
    with pytest.raises(TypeError):
        hit.metadata["kind"] = "other"  # type: ignore[index]


def test_retrieval_contract_rejects_shared_invalid_cases() -> None:
    for row in FIXTURE["invalidContractCases"]:
        with pytest.raises((TypeError, ValueError)):
            if row["kind"] == "request":
                value = row["value"]
                RetrievalRequest(
                    query=value["query"],
                    limit=value["limit"],
                )
            else:
                _hit(row["value"])

    with pytest.raises(ValueError):
        RetrievalHit(
            id="fact",
            content="content",
            source="fixture",
            score=math.inf,
        )
    with pytest.raises(TypeError):
        RetrievalHit(
            id="fact",
            content="content",
            source="fixture",
            untrusted="false",  # type: ignore[arg-type]
        )


def test_retriever_and_stable_error_are_public_contracts() -> None:
    assert isinstance(_Retriever(), Retriever)
    for code in FIXTURE["stableErrors"]:
        error = RetrievalError(
            "safe retrieval failure",
            code=code,
            retryable=code in {
                "retrieval_source_unavailable",
                "retrieval_timeout",
            },
        )
        assert error.code == code
        assert error.retryable is (
            code in {"retrieval_source_unavailable", "retrieval_timeout"}
        )
    with pytest.raises(ValueError):
        RetrievalError("unsupported failure", code="retrieval_failed")


@pytest.mark.asyncio
async def test_retriever_conformance_helper_uses_the_public_contract() -> None:
    await assert_retriever_conforms(
        _Retriever(),
        RetrievalRequest(query="contract", limit=1),
    )

    class TooManyHits:
        async def retrieve(self, request, signal=None):
            return (RetrievalHit(id="fact", content="content", source="fixture"),) * 2

    with pytest.raises(AssertionError):
        await assert_retriever_conforms(
            TooManyHits(), RetrievalRequest(query="contract", limit=1),
        )


def _hit(value: dict[str, object]) -> RetrievalHit:
    return RetrievalHit(
        id=str(value.get("id") or ""),
        content=str(value.get("content") or ""),
        source=str(value.get("source") or ""),
        version=(int(value["version"]) if "version" in value else None),
        score=(float(value["score"]) if "score" in value else None),
        untrusted=bool(value.get("untrusted", True)),
        metadata=value.get("metadata", {}),  # type: ignore[arg-type]
    )
