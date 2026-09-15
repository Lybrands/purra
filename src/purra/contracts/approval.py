"""Approval contracts: requests and results for confirm-mode tools."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from purra.contracts.enums import (
    ApprovalDecision,
    ApprovalStatus,
    ToolRiskLevel,
)
from purra.normalization import (
    optional_text as _optional_text,
    required_text,
)
from purra.contracts.tools import ToolCall

@dataclass(frozen=True, slots=True)
class ApprovalRequest:
    tool_call: ToolCall
    title: str
    risk_level: ToolRiskLevel
    summary: str
    timeout_seconds: float = 300.0
    binding: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.binding is not None:
            from purra.approvals import copy_tool_approval_binding
            object.__setattr__(self, "binding", copy_tool_approval_binding(self.binding))
        title = required_text(self.title, "approval title")
        timeout = float(self.timeout_seconds)
        if timeout <= 0:
            raise ValueError("approval timeout must be positive")
        object.__setattr__(self, "title", title)
        object.__setattr__(self, "risk_level", ToolRiskLevel(self.risk_level))
        object.__setattr__(self, "summary", str(self.summary or ""))
        object.__setattr__(self, "timeout_seconds", timeout)


@dataclass(frozen=True, slots=True)
class ApprovalResult:
    approval_id: str | None
    status: ApprovalStatus

    def __post_init__(self) -> None:
        object.__setattr__(self, "approval_id", _optional_text(self.approval_id))
        object.__setattr__(self, "status", ApprovalStatus(self.status))
        if self.status in {ApprovalStatus.APPROVED, ApprovalStatus.REJECTED} and not self.approval_id:
            raise ValueError("resolved approval requires an approval id")

    @property
    def approved(self) -> bool:
        return self.status is ApprovalStatus.APPROVED
