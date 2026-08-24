from __future__ import annotations

import json
from pathlib import Path

import pytest

from purra.api import InMemoryAgentAdapters
from purra.contracts import RunCreateParams
from purra.delegation import DelegationPolicy
from purra.events import AgentEvent


FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "delegation_protocol.json").read_text()
)


def test_shared_delegation_policy_matches_python() -> None:
    assert DelegationPolicy().snapshot_mapping() == FIXTURE["policy"]


@pytest.mark.asyncio
@pytest.mark.parametrize("case", FIXTURE["aggregationCases"], ids=lambda case: case["name"])
async def test_shared_delegation_aggregation_matches_python(case: dict) -> None:
    adapters = InMemoryAgentAdapters()
    begun = await adapters.runs.begin(
        RunCreateParams(session_id=None, prompt="delegate", mode="agent"),
        AgentEvent(type="run.started"),
    )
    batch_id = "fixture-batch"
    for index, fixture_row in enumerate(case["rows"]):
        row = await adapters.delegations.create(
            run_id=begun.run_id,
            batch_id=batch_id,
            agent_name=fixture_row["agentName"],
            agent_title=fixture_row["agentName"].title(),
            agent_instruction="Perform the isolated fixture task.",
            objective="fixture objective",
            required=fixture_row["required"],
            priority=index,
        )
        status = fixture_row["status"]
        if status in {"running", "done"}:
            await adapters.delegations.start(
                row.id,
                run_id=begun.run_id,
                batch_id=batch_id,
            )
        if status == "done":
            await adapters.delegations.complete(
                row.id,
                run_id=begun.run_id,
                batch_id=batch_id,
                result_summary=fixture_row.get("summary", ""),
            )
        elif status == "failed":
            await adapters.delegations.fail(
                row.id,
                run_id=begun.run_id,
                batch_id=batch_id,
                error="fixture_failed",
            )
        elif status == "canceled":
            await adapters.delegations.cancel(
                row.id,
                run_id=begun.run_id,
                batch_id=batch_id,
                reason="fixture_canceled",
            )

    aggregate = await adapters.delegations.aggregate_batch(begun.run_id, batch_id)
    assert aggregate.state == case["state"]
    assert len(aggregate.required_failures) == case["requiredFailureCount"]
    assert [item["agentName"] for item in aggregate.results] == case["resultAgentNames"]
