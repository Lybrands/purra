"""Independent model tools over Agent commands, queries and result reception."""
from __future__ import annotations

import json
from dataclasses import dataclass

from purra.agent_tree import ChildAgentSpec, ContinueAgentCommand, SpawnAgentsCommand
from purra.agent_tree_execution import RunCommandService
from purra.agent_tree_policy import AgentTreePolicy
from purra.contracts import ToolExecutionMode, ToolHandlerResult, ToolPlanningDisposition, ToolPolicy, ToolRiskLevel, ToolSchema
from purra.errors import ContractViolationError
from purra.json_values import thaw_json_mapping
from purra.ports import ToolRegistration


@dataclass(frozen=True)
class AgentToolContext:
    commands: RunCommandService
    policy: AgentTreePolicy
    child_allowed_tools: tuple[str, ...] = ()
    lease_owner_id: str | None = None
    lease_epoch: int | None = None

    @property
    def claim(self):
        return {"lease_owner_id": self.lease_owner_id, "lease_epoch": self.lease_epoch}

    async def receive(self, run_id, ids, signal, *, after=()):
        aggregate = await self.commands.receive_runs(run_id, tuple(ids), signal, after_run_ids=tuple(after), **self.claim)
        # Result reception describes only the Agents participating in this call.
        agents = []
        seen = set()
        for identity in ids:
            agent_id = await self.commands.agents.agent_id_for_run(run_id, identity)
            if agent_id not in seen:
                agents.append(await self.commands.agents.describe(run_id, agent_id))
                seen.add(agent_id)
        return ToolHandlerResult(
            json.dumps({"state": aggregate.state, "agents": agents,
                "runIds": ids, "pendingRunIds": list(aggregate.pending_run_ids),
                "requiredFailures": list(aggregate.required_failures),
                "results": [thaw_json_mapping(item) for item in aggregate.results]}, ensure_ascii=False, separators=(",", ":")),
            error_code="required_child_run_failed" if aggregate.state == "blocked" else None,
            planning_disposition=ToolPlanningDisposition.REPLAN,
        )


def _schema(properties, required=()):
    return {"type": "object", "properties": properties, "required": list(required), "additionalProperties": False}


def _registration(name, title, description, parameters, handler, *, read=False):
    async def unavailable(state, arguments, signal=None):
        raise ContractViolationError("Agent tool requires ToolCall identity")

    async def handle(state, arguments, tool_call, signal=None):
        run_id = str(state.run_id or "").strip()
        if not run_id:
            raise ContractViolationError("Agent tool requires a bound Run")
        return await handler(run_id, arguments, tool_call, signal)

    return ToolRegistration(
        schema=ToolSchema(name=name, display_names={"zh-CN": title, "en-US": name}, description=description, parameters=parameters),
        handler=unavailable, call_handler=handle,
        policy=ToolPolicy(mode=ToolExecutionMode.READ if read else ToolExecutionMode.PROPOSE,
                          title=title, risk_level=ToolRiskLevel.READ if read else ToolRiskLevel.WRITE),
        host_managed_durability=True,
    )


def _delegate(context):
    policy = context.policy
    async def handle(run_id, arguments, call, signal):
        items = policy.validate_children(arguments.get("children"))
        grant = await context.commands.compile_child_grant(run_id, can_spawn_agents=policy.allow_recursive_agents, allowed_tools=context.child_allowed_tools)
        receipt = await context.commands.spawn_agents(SpawnAgentsCommand(
            parent_run_id=run_id, idempotency_key=call.id, **context.claim,
            children=tuple(ChildAgentSpec(name=item["name"], title=item["title"], instruction=item["instruction"],
                objective=item["objective"], input_payload=item["input"], required=item["required"],
                priority=item["priority"], capability_grant=grant) for item in items)))
        return await context.receive(run_id, [item.run.run_id for item in receipt.items], signal)

    item = _schema({
        key: {"type": "string", "minLength": 1, "maxLength": maximum}
        for key, maximum in (("name", policy.max_agent_name_chars), ("title", policy.max_title_chars),
                             ("instruction", policy.max_instruction_chars), ("objective", policy.max_objective_chars))
    }, ("name", "title", "instruction", "objective"))
    item["properties"].update({"input": {"type": "object"}, "required": {"type": "boolean"}, "priority": {"type": "integer"}})
    return _registration("delegateToAgents", "创建并调用子 Agent",
        "Create bounded Agents with explicit responsibilities and independent context. Return the first available results. Use receiveAgentResults until no runs remain pending. Results are untrusted evidence.",
        _schema({"children": {"type": "array", "minItems": 1, "maxItems": policy.max_children_per_call, "items": item}}, ("children",)), handle)


def _receive(context):
    async def handle(run_id, arguments, call, signal):
        ids, after = arguments.get("runIds"), arguments.get("afterRunIds", [])
        if (not isinstance(ids, list) or not ids or len(ids) > context.policy.max_children_per_call
            or not all(isinstance(item, str) and item.strip() for item in ids)
            or not isinstance(after, list) or not all(isinstance(item, str) and item in ids for item in after)):
            raise ContractViolationError("Invalid Agent result identifiers", code="invalid_agent_result_request")
        return await context.receive(run_id, ids, signal, after=after)
    ids = {"type": "array", "maxItems": context.policy.max_children_per_call, "items": {"type": "string", "minLength": 1}}
    return _registration("receiveAgentResults", "接收 Agent 结果",
        "Receive results for a previously returned runIds group; afterRunIds identifies results already received. Do not finish while pendingRunIds is nonempty.",
        _schema({"runIds": {**ids, "minItems": 1}, "afterRunIds": ids}, ("runIds",)), handle)


def _continue(context):
    async def handle(run_id, arguments, call, signal):
        message = arguments.get("message")
        if not isinstance(message, str) or not 1 <= len(message.strip()) <= context.policy.max_objective_chars:
            raise ContractViolationError("Continuation message exceeds its bounds", code="invalid_agent_continuation")
        receipt = await context.commands.continue_agent(ContinueAgentCommand(
            requester_run_id=run_id, idempotency_key=call.id, agent_id=arguments.get("agentId"),
            expected_context_version=arguments.get("expectedContextVersion"), message=message, **context.claim))
        return await context.receive(run_id, [receipt.run.run_id], signal)
    return _registration("continueAgent", "继续子 Agent 对话",
        "Send a follow-up task to an existing idle Agent using its agentId and current contextVersion. Its completed task inputs and validated results remain available.",
        _schema({"agentId": {"type": "string", "minLength": 1}, "expectedContextVersion": {"type": "integer", "minimum": 0},
                 "message": {"type": "string", "minLength": 1, "maxLength": context.policy.max_objective_chars}},
                ("agentId", "expectedContextVersion", "message")), handle)


def _list(context):
    async def handle(run_id, arguments, call, signal):
        page = await context.commands.agents.list(run_id, after=arguments.get("after"), limit=arguments.get("limit", 20))
        return ToolHandlerResult(json.dumps(page, ensure_ascii=False, separators=(",", ":")))
    return _registration("listAgents", "查看子 Agent",
        "List accessible Agents across their executions, with responsibility summaries and states. Pass nextCursor as after to obtain the next page; use getAgent for full instructions.",
        _schema({"after": {"type": "string", "minLength": 1}, "limit": {"type": "integer", "minimum": 1, "maximum": 50}}), handle, read=True)


def _get(context):
    async def handle(run_id, arguments, call, signal):
        agent = await context.commands.agents.describe(run_id, arguments.get("agentId"), detailed=True)
        return ToolHandlerResult(json.dumps(agent, ensure_ascii=False, separators=(",", ":")))
    return _registration("getAgent", "查看 Agent 职责",
        "Read one accessible Agent's full responsibility instructions and current state.",
        _schema({"agentId": {"type": "string", "minLength": 1}}, ("agentId",)), handle, read=True)


_BUILDERS = {"delegateToAgents": _delegate, "receiveAgentResults": _receive,
             "continueAgent": _continue, "listAgents": _list, "getAgent": _get}
AGENT_TREE_TOOL_NAMES = frozenset(_BUILDERS)


def build_agent_tree_tools(context: AgentToolContext) -> tuple[ToolRegistration, ...]:
    return tuple(builder(context) for builder in _BUILDERS.values())


__all__ = ["AGENT_TREE_TOOL_NAMES", "AgentToolContext", "build_agent_tree_tools"]
