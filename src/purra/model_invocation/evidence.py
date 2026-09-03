"""Task-local evidence propagated to nested managed model calls."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar

from purra.contracts import ContextEvidenceReceipt


_BOUND_EVIDENCE: ContextVar[tuple[ContextEvidenceReceipt, ...]] = ContextVar(
    "purra_model_input_evidence",
    default=(),
)


@contextmanager
def bind_model_input_evidence(
    receipts: Sequence[ContextEvidenceReceipt],
) -> Iterator[None]:
    token = _BOUND_EVIDENCE.set(merge_context_evidence(
        _BOUND_EVIDENCE.get(), receipts
    ))
    try:
        yield
    finally:
        _BOUND_EVIDENCE.reset(token)


def current_model_input_evidence() -> tuple[ContextEvidenceReceipt, ...]:
    return _BOUND_EVIDENCE.get()


def merge_context_evidence(
    *groups: Sequence[ContextEvidenceReceipt],
) -> tuple[ContextEvidenceReceipt, ...]:
    merged: dict[str, ContextEvidenceReceipt] = {}
    for group in groups:
        for receipt in group:
            if not isinstance(receipt, ContextEvidenceReceipt):
                raise TypeError(
                    "model input evidence must contain ContextEvidenceReceipt values"
                )
            existing = merged.get(receipt.evidence_id)
            if existing is not None and existing != receipt:
                raise ValueError(
                    f"conflicting context evidence receipt: {receipt.evidence_id}"
                )
            merged[receipt.evidence_id] = receipt
    return tuple(merged.values())


__all__ = [
    "bind_model_input_evidence",
    "current_model_input_evidence",
    "merge_context_evidence",
]
