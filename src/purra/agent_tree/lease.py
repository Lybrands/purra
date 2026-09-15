"""Execution-local Agent Run lease proof for canonical mutations."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Iterator

from purra.normalization import non_negative_int, required_text


@dataclass(frozen=True, slots=True)
class AgentRunLeaseClaim:
    run_id: str
    owner_id: str
    epoch: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "run_id", required_text(self.run_id, "Agent Run id"))
        object.__setattr__(
            self,
            "owner_id",
            required_text(self.owner_id, "Agent Run lease owner"),
        )
        object.__setattr__(
            self,
            "epoch",
            non_negative_int(self.epoch, "Agent Run lease epoch"),
        )


_CURRENT_AGENT_RUN_LEASE: ContextVar[AgentRunLeaseClaim | None] = ContextVar(
    "purra_current_agent_run_lease",
    default=None,
)


@contextmanager
def bind_agent_run_lease(
    run_id: str,
    owner_id: str,
    epoch: int,
) -> Iterator[AgentRunLeaseClaim]:
    """Bind the immutable caller proof inherited by this execution's Tasks."""

    claim = AgentRunLeaseClaim(run_id, owner_id, epoch)
    token = _CURRENT_AGENT_RUN_LEASE.set(claim)
    try:
        yield claim
    finally:
        _CURRENT_AGENT_RUN_LEASE.reset(token)


def current_agent_run_lease(run_id: str) -> AgentRunLeaseClaim | None:
    claim = _CURRENT_AGENT_RUN_LEASE.get()
    return claim if claim is not None and claim.run_id == run_id else None


__all__ = [
    "AgentRunLeaseClaim",
    "bind_agent_run_lease",
    "current_agent_run_lease",
]
