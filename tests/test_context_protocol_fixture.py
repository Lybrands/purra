from __future__ import annotations

import json
from pathlib import Path

from purra.context_budget import (
    allocate_context_budget,
    estimate_json_tokens,
    estimate_text_tokens,
)
from purra.contracts import ContextBudgetClaim


FIXTURE = json.loads(
    (Path(__file__).parents[1] / "conformance" / "fixtures" / "context_protocol.json").read_text()
)


def test_context_protocol_fixture_matches_python_authority() -> None:
    for row in FIXTURE["jsonTokenCases"]:
        assert estimate_json_tokens(row["value"]) == row["tokens"]
    for row in FIXTURE["textTokenCases"]:
        assert estimate_text_tokens(row["value"]) == row["tokens"]

    row = FIXTURE["allocationCase"]
    budget = allocate_context_budget(
        window_tokens=row["windowTokens"],
        output_reserve_tokens=row["outputReserveTokens"],
        safety_reserve_tokens=row["safetyReserveTokens"],
        runtime_reserve_tokens=row["runtimeReserveTokens"],
        minimum_message_tokens=row["minimumMessageTokens"],
        claims=tuple(
            ContextBudgetClaim(
                item["name"],
                item["desiredTokens"],
                item["minimumTokens"],
                priority=item["priority"],
            )
            for item in row["claims"]
        ),
    )
    assert budget.provider_input_tokens == row["providerInputTokens"]
    assert dict(budget.context_allocations) == row["allocations"]
