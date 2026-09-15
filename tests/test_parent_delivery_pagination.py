from types import SimpleNamespace

import pytest

from purra.engine.agent_result_delivery import AgentResultDelivery
from purra.errors import ContractViolationError


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["completed", "started", "aborted"])
async def test_delivery_uses_latest_marker_across_short_pages(state):
    rows = [SimpleNamespace(sequence=i, payload={}) for i in range(1, 10)]
    rows[0].payload = {"eventType": "parent.stage.delivery", "data": {
        "deliveryId": "delivery", "state": "started", "childRunIds": ["child"],
    }}
    rows[-1].payload = {"eventType": "parent.stage.delivery", "data": {
        "deliveryId": "delivery", "state": state, "childRunIds": ["child"],
    }}
    cursors = []

    class Output:
        async def list_events(self, run_id, *, after_sequence, limit=200):
            cursors.append(after_sequence)
            return tuple(row for row in rows if row.sequence > after_sequence)[:2]

    core = object.__new__(AgentResultDelivery)
    core._output_repository = Output()
    if state == "completed":
        assert await core.completed("root") == {"child"}
    else:
        with pytest.raises(ContractViolationError) as error:
            await core.completed("root")
        assert error.value.code == "parent_delivery_reconciliation_required"
    assert cursors == [0, 2, 4, 6, 8, 9]


@pytest.mark.asyncio
async def test_delivery_rejects_nonadvancing_journal():
    class Output:
        async def list_events(self, *args, **kwargs):
            return (SimpleNamespace(sequence=1, payload={}),)

    core = object.__new__(AgentResultDelivery)
    core._output_repository = Output()
    with pytest.raises(ContractViolationError, match="pagination did not advance"):
        await core.completed("root")
