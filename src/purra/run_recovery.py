"""Typed, replay-safe state for reconstructing one persisted Agent Run."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from purra.contracts import AgentDelegation, ExecutionPlan, RunId, RunStatus
from purra.events import AgentEvent
from purra.json_values import freeze_json_mapping
from purra.normalization import required_text


@dataclass(frozen=True, slots=True)
class RunRecoverySnapshot:
    """Complete Core authority plus replay data at one durable cursor."""

    run_id: RunId
    status: RunStatus
    execution_plan: ExecutionPlan | None
    agent_preset_snapshot: Mapping[str, Any] = field(default_factory=dict)
    events: tuple[AgentEvent, ...] = ()
    delegations: tuple[AgentDelegation, ...] = ()
    next_cursor: int = 0
    has_more: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "run_id",
            required_text(self.run_id, "run recovery snapshot run id"),
        )
        object.__setattr__(self, "status", RunStatus(self.status))
        if self.execution_plan is not None and not isinstance(
            self.execution_plan,
            ExecutionPlan,
        ):
            raise TypeError(
                "run recovery snapshot execution_plan must be ExecutionPlan"
            )
        object.__setattr__(
            self,
            "agent_preset_snapshot",
            freeze_json_mapping(self.agent_preset_snapshot or {}),
        )
        events = tuple(self.events)
        if any(not isinstance(event, AgentEvent) for event in events):
            raise TypeError("run recovery snapshot events must be AgentEvent values")
        delegations = tuple(self.delegations)
        if any(
            not isinstance(delegation, AgentDelegation)
            for delegation in delegations
        ):
            raise TypeError(
                "run recovery snapshot delegations must be AgentDelegation values"
            )
        object.__setattr__(self, "events", events)
        object.__setattr__(self, "delegations", delegations)
        object.__setattr__(self, "next_cursor", max(0, int(self.next_cursor)))
        object.__setattr__(self, "has_more", bool(self.has_more))


__all__ = ["RunRecoverySnapshot"]
