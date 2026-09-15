"""Scoped, bounded views of persistent Agent identities."""
from __future__ import annotations

from purra.agent_tree import AgentNode, RunTreeRepository
from purra.errors import ContractViolationError


class AgentTreeQuery:
    def __init__(self, repository: RunTreeRepository):
        self._repository = repository

    async def _require_descendant(self, requester_run_id: str, agent_id: str) -> AgentNode:
        requester = await self._repository.get_run(requester_run_id)
        agent = await self._repository.get_agent(agent_id)
        current = agent
        seen = set()
        while current.parent_agent_id is not None and current.agent_id not in seen:
            if current.parent_agent_id == requester.agent_id:
                return agent
            seen.add(current.agent_id)
            current = await self._repository.get_agent(current.parent_agent_id)
        raise ContractViolationError("Agent is outside the requester's scope", code="agent_scope_violation")

    async def agent_id_for_run(self, requester_run_id: str, run_id: str) -> str:
        await self._repository.aggregate_runs(requester_run_id, (run_id,))
        return (await self._repository.get_run(run_id)).agent_id

    async def describe(self, requester_run_id: str, agent_id: str, *, detailed: bool = False) -> dict:
        agent = await self._require_descendant(requester_run_id, agent_id)
        latest = await self._repository.get_run(agent.latest_run_id)
        result = {
            "agentId": agent.agent_id, "name": agent.name, "title": agent.title,
            "responsibility": agent.instruction[:256],
            "contextVersion": agent.context_version, "status": agent.state.value,
            "latestRunId": latest.run_id, "latestRunStatus": latest.status.value,
        }
        if detailed:
            result["instruction"] = agent.instruction
        return result

    async def list(self, requester_run_id: str, *, after: str | None = None, limit: int = 20) -> dict:
        if type(limit) is not int or not 1 <= limit <= 50:
            raise ContractViolationError("Agent page size must be between 1 and 50", code="invalid_agent_query")
        if after is not None and (not isinstance(after, str) or not after.strip()):
            raise ContractViolationError("Invalid Agent page cursor", code="invalid_agent_query")
        requester = await self._repository.get_run(requester_run_id)
        agents = await self._repository.list_agent_descendants(requester.agent_id, after=after, limit=limit + 1)
        page = agents[:limit]
        return {
            "agents": [await self.describe(requester_run_id, agent.agent_id) for agent in page],
            "nextCursor": page[-1].agent_id if len(agents) > limit else None,
        }
