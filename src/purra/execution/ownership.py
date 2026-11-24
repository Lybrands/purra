"""Execution-local identity for durable repository fencing."""
from contextvars import ContextVar

execution_owner = ContextVar("purra_execution_owner", default=None)
execution_claim: ContextVar[tuple[str, str, int] | None] = ContextVar(
    "purra_execution_claim", default=None
)


def current_execution_claim() -> tuple[str, str, int] | None:
    """Return the current supervised Run's ``(run_id, owner_id, attempt)``.

    This read-only execution context supports repository fencing; it is not
    evidence that the lease is still live. Hosts must also validate the durable
    lease's status, owner, attempt, expiration, and cancellation state. Returns
    ``None`` if this task has no bound claim; inherited claims can outlive the
    parent execution and must not be treated as proof of lease liveness.
    """
    return execution_claim.get()
