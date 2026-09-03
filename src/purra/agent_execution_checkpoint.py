"""Durable model-ready checkpoints for safe Agent Run continuation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from purra.contracts import (
    AgentMessage,
    MessageOrigin,
    MessageRole,
    ToolBatchOutcome,
    ToolCall,
)
from purra.json_values import (
    freeze_json_mapping,
    freeze_json_value,
    thaw_json_mapping,
    thaw_json_value,
)
from purra.normalization import positive_int, required_text


@dataclass(frozen=True, slots=True)
class AgentExecutionCheckpoint:
    """One fully committed boundary immediately before a model round.

    Provider streams and in-flight external writes are not resumable here.
    Planned authority remains in the Run; planning_state holds its coordinator.
    """

    run_id: str
    next_round: int
    messages: tuple[AgentMessage, ...]
    round_limit: int
    execution_state_domain: Mapping[str, Any] = field(default_factory=dict)
    evidence_state: Mapping[str, Any] = field(default_factory=dict)
    recovery_attempts: tuple[tuple[str, str, int], ...] = ()
    logical_round_number: int = 0
    progress_rounds: int = 0
    tool_input_recovery_epoch: int = 0
    logical_required_tool_call_enabled: bool = False
    provider_required_tool_choice_enabled: bool = False
    declined_response_pending: bool = False
    response_repair_pending: bool = False
    public_presentation_pending: bool = False
    last_tool_outcome: ToolBatchOutcome = ToolBatchOutcome.COMPLETED
    pending_tool_input_retries: tuple[tuple[str, str], ...] = ()
    initial_planning_open: bool = True
    schema_version: int = 2
    phase: str = "model_ready"
    execution_profile: str = "reactive"
    input_revision: int = 0
    planning_state: Mapping[str, Any] = field(default_factory=dict)
    dynamic_replan_pending: bool = False
    pending_recovery_error_code: str | None = None
    failed_tool_recovery_error_code: str | None = None

    def __post_init__(self) -> None:
        if type(self.input_revision) is not int or self.input_revision < 0:
            raise ValueError("checkpoint input revision must be non-negative")
        if self.schema_version != 2:
            raise ValueError("Agent execution checkpoint schema version must be 2")
        if self.phase != "model_ready":
            raise ValueError("Agent execution checkpoint phase must be model_ready")
        if self.execution_profile not in {"reactive", "auto", "planned"}:
            raise ValueError("Invalid checkpoint execution profile")
        object.__setattr__(self, "planning_state", freeze_json_mapping(self.planning_state))
        object.__setattr__(self, "run_id", required_text(
            self.run_id,
            "Agent execution checkpoint Run id",
        ))
        object.__setattr__(self, "next_round", positive_int(
            self.next_round,
            "Agent execution checkpoint next round",
        ))
        messages = tuple(self.messages)
        if not messages or any(not isinstance(item, AgentMessage) for item in messages):
            raise TypeError("Agent execution checkpoint requires Agent messages")
        object.__setattr__(self, "messages", messages)
        object.__setattr__(
            self,
            "execution_state_domain",
            freeze_json_mapping(self.execution_state_domain),
        )
        object.__setattr__(
            self,
            "evidence_state",
            freeze_json_mapping(self.evidence_state),
        )
        attempts = tuple(
            (
                required_text(cause, "checkpoint recovery cause"),
                required_text(scope, "checkpoint recovery scope"),
                int(count),
            )
            for cause, scope, count in self.recovery_attempts
        )
        if any(count < 0 for _cause, _scope, count in attempts):
            raise ValueError("checkpoint recovery attempts must be non-negative")
        if len({(cause, scope) for cause, scope, _count in attempts}) != len(attempts):
            raise ValueError("checkpoint recovery attempts must be unique")
        object.__setattr__(self, "recovery_attempts", attempts)
        for name in (
            "round_limit",
            "logical_round_number",
            "progress_rounds",
            "tool_input_recovery_epoch",
        ):
            value = int(getattr(self, name))
            if value < 0 or (name == "round_limit" and value == 0):
                raise ValueError(f"checkpoint {name} is invalid")
            object.__setattr__(self, name, value)
        for name in (
            "logical_required_tool_call_enabled",
            "provider_required_tool_choice_enabled",
            "declined_response_pending",
            "response_repair_pending",
            "public_presentation_pending",
            "initial_planning_open",
        ):
            object.__setattr__(self, name, bool(getattr(self, name)))
        object.__setattr__(
            self,
            "last_tool_outcome",
            ToolBatchOutcome(self.last_tool_outcome),
        )
        pending_retries = tuple(
            (
                required_text(call_id, "checkpoint retry tool call id"),
                required_text(tool_name, "checkpoint retry tool name"),
            )
            for call_id, tool_name in self.pending_tool_input_retries
        )
        if len({call_id for call_id, _tool_name in pending_retries}) != len(
            pending_retries
        ):
            raise ValueError("checkpoint retry tool call ids must be unique")
        object.__setattr__(
            self,
            "pending_tool_input_retries",
            pending_retries,
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "schemaVersion": self.schema_version,
            "runId": self.run_id,
            "phase": self.phase,
            "executionProfile": self.execution_profile,
            "nextRound": self.next_round,
            "inputRevision": self.input_revision,
            "messages": [_message_to_mapping(item) for item in self.messages],
            "executionStateDomain": thaw_json_mapping(
                self.execution_state_domain
            ),
            "evidenceState": thaw_json_mapping(self.evidence_state),
            "recoveryAttempts": [
                {"cause": cause, "scope": scope, "attempts": count}
                for cause, scope, count in self.recovery_attempts
            ],
            "roundLimit": self.round_limit,
            "logicalRoundNumber": self.logical_round_number,
            "progressRounds": self.progress_rounds,
            "toolInputRecoveryEpoch": self.tool_input_recovery_epoch,
            "logicalRequiredToolCallEnabled": (
                self.logical_required_tool_call_enabled
            ),
            "providerRequiredToolChoiceEnabled": (
                self.provider_required_tool_choice_enabled
            ),
            "declinedResponsePending": self.declined_response_pending,
            "responseRepairPending": self.response_repair_pending,
            "publicPresentationPending": self.public_presentation_pending,
            "lastToolOutcome": self.last_tool_outcome.value,
            "pendingToolInputRetries": [
                {"toolCallId": call_id, "toolName": tool_name}
                for call_id, tool_name in self.pending_tool_input_retries
            ],
            "initialPlanningOpen": self.initial_planning_open,
            "planningState": thaw_json_mapping(self.planning_state),
            "dynamicReplanPending": self.dynamic_replan_pending,
            "pendingRecoveryErrorCode": self.pending_recovery_error_code,
            "failedToolRecoveryErrorCode": self.failed_tool_recovery_error_code,
        }

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "AgentExecutionCheckpoint":
        if not isinstance(value.get("initialPlanningOpen"), bool):
            raise TypeError(
                "checkpoint initialPlanningOpen must be a boolean"
            )
        raw_attempts = value.get("recoveryAttempts") or ()
        if not isinstance(raw_attempts, (list, tuple)):
            raise TypeError("checkpoint recovery attempts must be a sequence")
        raw_messages = value.get("messages") or ()
        if not isinstance(raw_messages, (list, tuple)):
            raise TypeError("checkpoint messages must be a sequence")
        raw_retries = value.get("pendingToolInputRetries") or ()
        if not isinstance(raw_retries, (list, tuple)):
            raise TypeError("checkpoint tool retries must be a sequence")
        return cls(
            schema_version=int(value.get("schemaVersion") or 0),
            run_id=str(value.get("runId") or ""),
            phase=str(value.get("phase") or ""),
            execution_profile=str(value.get("executionProfile") or ""),
            next_round=int(value.get("nextRound") or 0),
            input_revision=value.get("inputRevision", 0),
            messages=tuple(
                _message_from_mapping(item)
                for item in raw_messages
                if isinstance(item, Mapping)
            ),
            execution_state_domain=_mapping(
                value.get("executionStateDomain"),
                "checkpoint execution state",
            ),
            evidence_state=_mapping(
                value.get("evidenceState"),
                "checkpoint evidence state",
            ),
            recovery_attempts=tuple(
                (
                    str(item.get("cause") or ""),
                    str(item.get("scope") or ""),
                    int(item.get("attempts") or 0),
                )
                for item in raw_attempts
                if isinstance(item, Mapping)
            ),
            round_limit=int(value.get("roundLimit") or 0),
            logical_round_number=int(value.get("logicalRoundNumber") or 0),
            progress_rounds=int(value.get("progressRounds") or 0),
            tool_input_recovery_epoch=int(
                value.get("toolInputRecoveryEpoch") or 0
            ),
            logical_required_tool_call_enabled=bool(
                value.get("logicalRequiredToolCallEnabled")
            ),
            provider_required_tool_choice_enabled=bool(
                value.get("providerRequiredToolChoiceEnabled")
            ),
            declined_response_pending=bool(value.get("declinedResponsePending")),
            response_repair_pending=bool(value.get("responseRepairPending")),
            public_presentation_pending=bool(
                value.get("publicPresentationPending")
            ),
            last_tool_outcome=ToolBatchOutcome(
                str(value.get("lastToolOutcome") or "completed")
            ),
            pending_tool_input_retries=tuple(
                (
                    str(item.get("toolCallId") or ""),
                    str(item.get("toolName") or ""),
                )
                for item in raw_retries
                if isinstance(item, Mapping)
            ),
            initial_planning_open=value["initialPlanningOpen"],
            planning_state=_mapping(value.get("planningState"), "checkpoint planning state"),
            dynamic_replan_pending=bool(value.get("dynamicReplanPending", False)),
            pending_recovery_error_code=value.get("pendingRecoveryErrorCode"),
            failed_tool_recovery_error_code=value.get("failedToolRecoveryErrorCode"),
        )


def _message_to_mapping(message: AgentMessage) -> dict[str, Any]:
    return {
        "role": message.role.value,
        "content": thaw_json_value(message.content),
        "reasoning": message.reasoning,
        "toolCalls": [
            {
                "id": call.id,
                "name": call.name,
                "argumentsJson": call.arguments_json,
            }
            for call in message.tool_calls
        ],
        "toolCallId": message.tool_call_id,
        "origin": message.origin.value,
        "attributes": thaw_json_mapping(message.attributes),
        "hostMetadata": thaw_json_mapping(message.host_metadata),
        "providerData": thaw_json_mapping(message.provider_data),
    }


def _message_from_mapping(value: Mapping[str, Any]) -> AgentMessage:
    raw_calls = value.get("toolCalls") or ()
    if not isinstance(raw_calls, (list, tuple)):
        raise TypeError("checkpoint tool calls must be a sequence")
    return AgentMessage(
        role=MessageRole(str(value.get("role") or "")),
        content=freeze_json_value(value.get("content")),
        reasoning=(
            str(value["reasoning"])
            if value.get("reasoning") is not None
            else None
        ),
        tool_calls=tuple(
            ToolCall(
                id=str(item.get("id") or ""),
                name=str(item.get("name") or ""),
                arguments_json=str(item.get("argumentsJson") or ""),
            )
            for item in raw_calls
            if isinstance(item, Mapping)
        ),
        tool_call_id=(
            str(value["toolCallId"])
            if value.get("toolCallId") is not None
            else None
        ),
        origin=MessageOrigin(str(value.get("origin") or "caller")),
        attributes=_mapping(value.get("attributes"), "checkpoint attributes"),
        provider_data=_mapping(value.get("providerData"), "checkpoint provider data"),
        host_metadata=_mapping(
            value.get("hostMetadata"),
            "checkpoint host metadata",
        ),
    )


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise TypeError(f"{label} must be an object")
    return value


__all__ = ["AgentExecutionCheckpoint"]
