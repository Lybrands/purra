"""Public retrieval contracts and model Tool adapter."""

from purra.retrieval.contracts import RetrievalHit, RetrievalRequest, Retriever
from purra.retrieval.errors import RetrievalError
from purra.retrieval.tool import RetrieverTool


__all__ = [
    "RetrievalError",
    "RetrievalHit",
    "RetrievalRequest",
    "Retriever",
    "RetrieverTool",
]
