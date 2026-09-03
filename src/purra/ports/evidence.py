"""Host validation boundary for model-visible external evidence."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from purra.contracts import ContextEvidenceReceipt
from purra.ports.model import CancellationSignal


@runtime_checkable
class ModelInputEvidenceValidator(Protocol):
    """Revalidate external evidence immediately before a Provider call."""

    async def validate_evidence(
        self,
        receipts: Sequence[ContextEvidenceReceipt],
        *,
        signal: CancellationSignal | None = None,
    ) -> None: ...


__all__ = ["ModelInputEvidenceValidator"]
