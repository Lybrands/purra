"""Model-visible convenience facade over the Agent tree command service."""

from __future__ import annotations

import json
from typing import Any

from purra.agent_tree import ChildAgentSpec, SpawnAgentsCommand
from purra.agent_tree_execution import RunCommandService
from purra.contracts import (
    ExecutionState,
    ToolCall,
    ToolExecutionMode,
    ToolHandlerResult,
    ToolPlanningDisposition,
    ToolPolicy,
    ToolRiskLevel,
    ToolSchema,
)
from purra.delegation.policy import DelegationPolicy
from purra.delegation.tool import _validated_items
from purra.errors import ContractViolationError
from purra.json_values import thaw_json_mapping
from purra.ports import CancellationSignal, ToolRegistration


def build_agent_tree_tool_registration(
    commands: RunCommandService,
    policy: DelegationPolicy,
    *,
    child_allowed_tools: tuple[str, ...] = (),
    lease_owner_id: str | None = None,
    lease_epoch: int | None = None,
) -> ToolRegistration:
    if not isinstance(commands, RunCommandService):
        raise TypeError("Agent tree tool requires RunCommandService")
    if not isinstance(policy, DelegationPolicy):
        raise TypeError("Agent tree tool requires DelegationPolicy")

    async def unavailable(
        state: ExecutionState,
        arguments: dict[str, Any],
        signal: CancellationSignal | None = None,
    ) -> ToolHandlerResult:
        del state, arguments, signal
        raise ContractViolationError("Agent tree tool requires ToolCall identity")

    async def handle(
        state: ExecutionState,
        arguments: dict[str, Any],
        tool_call: ToolCall,
        signal: CancellationSignal | None = None,
    ) -> ToolHandlerResult:
        run_id = str(state.run_id or "").strip()
        if not run_id:
            raise ContractViolationError("Agent tree tool requires a bound Run")
        items = _validated_items(arguments, policy)
        child_grant = await commands.compile_child_grant(
            run_id,
            can_spawn_agents=policy.allows_recursive_delegation,
            allowed_tools=child_allowed_tools,
        )
        receipt = await commands.spawn_agents(SpawnAgentsCommand(
            parent_run_id=run_id,
            idempotency_key=tool_call.id,
            lease_owner_id=lease_owner_id,
            lease_epoch=lease_epoch,
            children=tuple(
                ChildAgentSpec(
                    name=item["agentName"],
                    title=item["title"],
                    instruction=item["instruction"],
                    objective=item["objective"],
                    input_payload=item["input"],
                    required=item["required"],
                    priority=item["priority"],
                    capability_grant=child_grant,
                )
                for item in items
            ),
        ))
        aggregate = await commands.join_runs(
            run_id,
            tuple(item.run.run_id for item in receipt.items),
            signal,
            lease_owner_id=lease_owner_id,
            lease_epoch=lease_epoch,
        )
        return ToolHandlerResult(
            json.dumps(
                {
                    "state": aggregate.state,
                    "pendingRunIds": list(aggregate.pending_run_ids),
                    "requiredFailures": list(aggregate.required_failures),
                    "results": [
                        thaw_json_mapping(item) for item in aggregate.results
                    ],
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
                "en-US": "Create and invoke child Agents",
            },
            description=(
                f"Create 1-{policy.max_agents_per_call} bounded child Agents, "
                "run them independently, wait for completion, and return "
                "attributed results."
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
                                "agentName": {"type": "string", "minLength": 1},
                                "title": {"type": "string", "minLength": 1},
                                "instruction": {"type": "string", "minLength": 1},
                                "objective": {"type": "string", "minLength": 1},
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
        handler=unavailable,
        call_handler=handle,
        policy=ToolPolicy(
            mode=ToolExecutionMode.PROPOSE,
            title="创建并调用子 Agent",
            risk_level=ToolRiskLevel.WRITE,
        ),
        host_managed_durability=True,
    )


__all__ = ["build_agent_tree_tool_registration"]
