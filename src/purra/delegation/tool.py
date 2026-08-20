"""Generic delegation tool backed by the canonical coordinator."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any
from uuid import uuid4

from purra.cancellation import OperationCanceled, await_with_cancellation
from purra.contracts import (
    ExecutionState,
    ToolExecutionMode,
    ToolHandlerResult,
    ToolPlanningDisposition,
    ToolPolicy,
    ToolRiskLevel,
    ToolSchema,
)
from purra.errors import ContractViolationError
from purra.ports import CancellationSignal, ToolRegistration
from purra.delegation.coordinator import DelegationCoordinator
from purra.delegation.policy import DelegationPolicy


def build_delegation_tool_registration(
    coordinator: DelegationCoordinator,
    policy: DelegationPolicy = DelegationPolicy(),
) -> ToolRegistration:
    if not isinstance(coordinator, DelegationCoordinator):
        raise TypeError("delegation tool requires the canonical coordinator")
    if not isinstance(policy, DelegationPolicy):
        raise TypeError("delegation tool requires a DelegationPolicy")

    async def handle(
        state: ExecutionState,
        arguments: dict[str, Any],
        signal: CancellationSignal | None = None,
    ) -> ToolHandlerResult:
        run_id = str(state.run_id or "").strip()
        if not run_id:
            raise ContractViolationError(
                "delegation tool requires a bound Run"
            )
        items = _validated_items(arguments, policy)
        batch_id = f"delegation-batch-{uuid4().hex}"
        created = [
            await coordinator.create(
                run_id=run_id,
                batch_id=batch_id,
                agent_name=item["agentName"],
                agent_title=item["title"],
                agent_instruction=item["instruction"],
                objective=item["objective"],
                input_payload=item["input"],
                context_mode=policy.context_mode,
                required=item["required"],
                priority=item["priority"],
            )
            for item in items
        ]

        try:
            await await_with_cancellation(
                coordinator.execute_batch(created, signal),
                signal,
            )
        except OperationCanceled:
            await coordinator.cancel_batch(run_id, batch_id)
            raise

        aggregate = await coordinator.aggregate_batch(run_id, batch_id)
        return ToolHandlerResult(
            json.dumps(
                {
                    "state": aggregate.state,
                    "counts": dict(aggregate.counts),
                    "requiredFailures": list(aggregate.required_failures),
                    "results": [dict(item) for item in aggregate.results],
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            error_code=(
                "required_delegation_failed"
                if aggregate.state == "blocked"
                else None
            ),
            planning_disposition=ToolPlanningDisposition.REPLAN,
        )

    return ToolRegistration(
        schema=ToolSchema(
            name="delegateToAgents",
            display_names={
                "zh-CN": "创建并调用子 Agent",
                "en-US": "Create and invoke Agents",
            },
            description=(
                f"Create 1-{policy.max_agents_per_call} task-specific Agents, "
                "invoke them with isolated "
                "context and bounded read-only tools, wait for completion, and "
                "return their results. Define each Agent's name, title, and "
                "instruction for the current task."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "delegations": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": policy.max_agents_per_call,
                        "items": {
                            "type": "object",
                            "properties": {
                                "agentName": {
                                    "type": "string",
                                    "minLength": 1,
                                    "maxLength": policy.max_agent_name_chars,
                                },
                                "title": {
                                    "type": "string",
                                    "minLength": 1,
                                    "maxLength": policy.max_title_chars,
                                },
                                "instruction": {
                                    "type": "string",
                                    "minLength": 1,
                                    "maxLength": policy.max_instruction_chars,
                                },
                                "objective": {
                                    "type": "string",
                                    "minLength": 1,
                                    "maxLength": policy.max_objective_chars,
                                },
                                "input": {"type": "object"},
                                "required": {"type": "boolean"},
                                "priority": {"type": "integer"},
                            },
                            "required": [
                                "agentName",
                                "title",
                                "instruction",
                                "objective",
                            ],
                            "additionalProperties": False,
                        },
                    },
                },
                "required": ["delegations"],
                "additionalProperties": False,
            },
        ),
        handler=handle,
        policy=ToolPolicy(
            mode=ToolExecutionMode.PROPOSE,
            title="创建并调用子 Agent",
            risk_level=ToolRiskLevel.WRITE,
        ),
        # The coordinator persists every delegation transition and canonical
        # status event itself. Wrapping it in the generic tool-receipt
        # transaction would nest durable boundaries across asynchronous model
        # work and prevent those events from committing.
        host_managed_durability=True,
    )


def _validated_items(
    arguments: Mapping[str, Any],
    policy: DelegationPolicy,
) -> list[dict[str, Any]]:
    raw_items = arguments.get("delegations")
    if (
        not isinstance(raw_items, Sequence)
        or isinstance(raw_items, (str, bytes, bytearray))
        or not 1 <= len(raw_items) <= policy.max_agents_per_call
    ):
        raise ContractViolationError(
            "delegations must contain between one and "
            f"{policy.max_agents_per_call} tasks"
        )
    items: list[dict[str, Any]] = []
    agent_names: set[str] = set()
    for raw in raw_items:
        if not isinstance(raw, Mapping):
            raise ContractViolationError("delegation task must be an object")
        agent_name = _bounded_text(
            raw.get("agentName"),
            "agentName",
            policy.max_agent_name_chars,
        )
        title = _bounded_text(raw.get("title"), "title", policy.max_title_chars)
        instruction = _bounded_text(
            raw.get("instruction"),
            "instruction",
            policy.max_instruction_chars,
        )
        objective = _bounded_text(
            raw.get("objective"),
            "objective",
            policy.max_objective_chars,
        )
        if agent_name in agent_names:
            raise ContractViolationError(
                "delegated Agent names must be unique within a tool call"
            )
        agent_names.add(agent_name)
        items.append({
            "agentName": agent_name,
            "title": title,
            "instruction": instruction,
            "objective": objective,
            "input": (
                dict(raw.get("input"))
                if isinstance(raw.get("input"), Mapping)
                else {}
            ),
            "required": bool(raw.get("required", True)),
            "priority": int(raw.get("priority") or 0),
        })
    return items


def _bounded_text(value: object, label: str, maximum: int) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > maximum:
        raise ContractViolationError(
            f"delegation {label} must contain between 1 and {maximum} characters"
        )
    return normalized


__all__ = ["build_delegation_tool_registration"]
