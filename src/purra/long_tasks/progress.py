"""Progress event payload helpers for the recipe task dispatcher."""

from __future__ import annotations

from collections.abc import Sequence

from purra.events import AgentEvent, CoreEventType
from purra.long_tasks.contracts import LongTaskUnitRecord
from purra.long_tasks.contracts import LongTaskExecutionUpdate


def progress_unit_payload(unit: LongTaskUnitRecord) -> dict[str, object]:
    title = str(unit.metadata.get("displayTitle") or "").strip()
    return {
        "id": unit.id,
        "position": unit.position,
        "plannerStepId": str(unit.metadata.get("plannerStepId") or unit.id),
        "kind": str(unit.metadata.get("unitKind") or ""),
        **({"title": title} if title else {}),
        "status": unit.status.value,
        "attempt": unit.attempt,
        "maxAttempts": unit.max_attempts,
        **({"runId": unit.run_id} if unit.run_id else {}),
        **({"outputRef": unit.output_ref} if unit.output_ref else {}),
        **({"errorCode": unit.error_code} if unit.error_code else {}),
        "updateTime": unit.update_time,
    }


def final_response(units: Sequence[LongTaskUnitRecord]) -> str:
    for unit in reversed(units):
        value = unit.metadata.get("finalResponse")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "Durable task completed."
