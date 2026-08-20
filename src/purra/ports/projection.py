"""Host-owned projection port for opaque domain effects."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from purra.contracts import RunId
from purra.output.contracts import DomainEffectOutput

if TYPE_CHECKING:
    from purra.contracts import RunCreateParams
    from purra.ports.run_lifecycle import RunCommit
    from purra.run_control import RunCancellationReceipt


@runtime_checkable
class DomainEventProjector(Protocol):
    """Project one typed domain effect inside the output transaction.

    The projector never receives or returns the canonical event envelope.
    """

    async def project(
        self,
        run_id: RunId,
        effect: DomainEffectOutput,
    ) -> None: ...


@runtime_checkable
class RunCommitProjector(Protocol):
    """Project host-bound terminal effects inside the Run commit transaction."""

    async def project(self, run_id: RunId, commit: "RunCommit") -> None: ...


@runtime_checkable
class RunBeginProjector(Protocol):
    """Project host-bound identity effects inside the Root begin transaction."""

    async def project(
        self,
        run_id: RunId,
        params: "RunCreateParams",
    ) -> None: ...


@runtime_checkable
class RunCancellationProjector(Protocol):
    """Project a trusted cancellation fence inside its host transaction."""

    async def project(
        self,
        run_id: RunId,
        receipt: "RunCancellationReceipt",
    ) -> None: ...


__all__ = [
    "DomainEventProjector",
    "RunBeginProjector",
    "RunCancellationProjector",
    "RunCommitProjector",
]
