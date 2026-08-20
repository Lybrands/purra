"""Business-agnostic outcome rules for tool policy and approvals."""

from __future__ import annotations

from collections.abc import Iterable

from purra.contracts import ApprovalStatus, ToolBatchOutcome


_OUTCOME_PRECEDENCE = {
    ToolBatchOutcome.COMPLETED: 0,
    ToolBatchOutcome.PROGRESSED: 1,
    ToolBatchOutcome.DECLINED: 2,
    ToolBatchOutcome.REJECTED: 3,
    ToolBatchOutcome.FAILED: 4,
    ToolBatchOutcome.CANCELED: 5,
}


def aggregate_outcomes(outcomes: Iterable[ToolBatchOutcome]) -> ToolBatchOutcome:
    values = tuple(ToolBatchOutcome(item) for item in outcomes)
    if not values:
        return ToolBatchOutcome.COMPLETED
    # Successful calls in one batch execute in order.  The final call owns the
    # resulting step disposition: an earlier partial append may be followed by
    # the batch that reaches the declared total.  Terminal policy outcomes
    # still dominate the whole batch.
    terminal = tuple(
        item for item in values
        if item not in {
            ToolBatchOutcome.COMPLETED,
            ToolBatchOutcome.PROGRESSED,
        }
    )
    if not terminal:
        return values[-1]
    return max(values, key=_OUTCOME_PRECEDENCE.__getitem__)


def approval_outcome(status: ApprovalStatus) -> ToolBatchOutcome:
    value = ApprovalStatus(status)
    if value is ApprovalStatus.APPROVED:
        return ToolBatchOutcome.COMPLETED
    if value is ApprovalStatus.REJECTED:
        return ToolBatchOutcome.DECLINED
    if value is ApprovalStatus.CANCELED:
        return ToolBatchOutcome.CANCELED
    return ToolBatchOutcome.FAILED


def approval_error_code(status: ApprovalStatus) -> str | None:
    value = ApprovalStatus(status)
    if value is ApprovalStatus.APPROVED:
        return None
    if value is ApprovalStatus.REJECTED:
        return "approval_rejected"
    if value is ApprovalStatus.CANCELED:
        return "approval_canceled"
    if value is ApprovalStatus.TIMED_OUT:
        return "approval_timed_out"
    return "approval_unavailable"
