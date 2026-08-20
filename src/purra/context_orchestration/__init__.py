"""Core-owned context budgeting, timing, and validation orchestration."""

from purra.context_orchestration.contracts import (
    ContextCompressionRequest,
    ContextCompressionSettings,
    ConversationCompactionResult,
)
from purra.context_orchestration.ledger import (
    ContextCompactionBudget,
    ContextCompactionPhase,
)


def __getattr__(name: str):
    if name == "ContextCompressionCoordinator":
        from purra.context_orchestration.compaction import (
            ContextCompressionCoordinator,
        )

        return ContextCompressionCoordinator
    raise AttributeError(name)

__all__ = [
    "ContextCompressionRequest",
    "ContextCompressionSettings",
    "ConversationCompactionResult",
    "ContextCompactionBudget",
    "ContextCompactionPhase",
    "ContextCompressionCoordinator",
]
