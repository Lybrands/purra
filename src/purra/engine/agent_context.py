"""Build a delegated execution from explicit constraints and Agent history."""
from __future__ import annotations

import json
from purra.agent_tree import AgentTreeRunStatus
from purra.contracts import AgentMessage, MessageRole
from purra.engine.options import AgentCoreRunOptions
from purra.errors import ContractViolationError
from purra.json_values import thaw_json_mapping
from purra.output import ResponseTransactionPolicy, ResponseTransactionMode, PublicPresentationMode


def task_message(run):
    content = run.objective
    if run.input_payload:
        content += "\n\nInput:\n" + json.dumps(thaw_json_mapping(run.input_payload), ensure_ascii=False, separators=(",", ":"))
    return AgentMessage(role=MessageRole.USER, content=content)


class AgentConversationLoader:
    def __init__(self, repository, output_repository):
        self._repository = repository
        self._outputs = output_repository

    async def load(self, run, agent):
        turns, seen = [], set()
        previous_id = run.previous_run_id
        while previous_id is not None:
            if previous_id in seen:
                raise ContractViolationError("Agent history contains a cycle", code="agent_context_conflict")
            seen.add(previous_id)
            previous = await self._repository.get_run(previous_id)
            if previous.agent_id != agent.agent_id:
                raise ContractViolationError("Agent history belongs to another Agent", code="agent_scope_violation")
            if previous.status is AgentTreeRunStatus.DONE:
                content = await self._outputs.load_validated_result(previous_id)
                turns.append((task_message(previous), AgentMessage(role=MessageRole.ASSISTANT, content=content)))
            previous_id = previous.previous_run_id
        return tuple(message for turn in reversed(turns) for message in turn)


def child_run_options(parent, run, agent, checkpoint):
    """Inherit execution constraints, never the parent's task output contract."""
    return AgentCoreRunOptions(
        default_context_window_tokens=parent.default_context_window_tokens,
        safety_reserve_tokens=parent.safety_reserve_tokens,
        runtime_reserve_tokens=parent.runtime_reserve_tokens,
        minimum_message_tokens=parent.minimum_message_tokens,
        model_supports_tools=parent.model_supports_tools,
        force_planned_tool_choice=parent.force_planned_tool_choice,
        reasoning_mode=parent.reasoning_mode,
        deadline_at_ms=parent.deadline_at_ms,
        turn_id=f"agent-tree:{run.run_id}",
        response_transaction_policy=ResponseTransactionPolicy(
            mode=ResponseTransactionMode.VALIDATED_RESULT,
            public_presentation=PublicPresentationMode.NONE,
        ),
        agent_tree_run_id=run.run_id, agent_tree_root_run_id=run.root_run_id,
        agent_tree_agent_id=run.agent_id, agent_tree_parent_run_id=run.parent_run_id,
        agent_tree_lease_owner_id=run.lease_owner_id, agent_tree_lease_epoch=run.lease_epoch,
        agent_capability_grant=agent.capability_grant,
        agent_execution_checkpoint=checkpoint,
    )
