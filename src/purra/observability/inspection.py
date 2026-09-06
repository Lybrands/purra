"""Read-only recovery observations. A report never grants execution authority."""
from collections.abc import Mapping
from typing import Any


def build_recovery_inspection(state: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize host/storage evidence; omit identifiers, content and exception text."""
    enums = {
        "status": ("running", "terminal", "unknown"),
        "checkpoint": ("present", "missing", "unknown"),
        "lease": ("active", "inactive", "unknown"),
        "configuration": ("matched", "mismatch", "unknown"),
        "permissions": ("allowed", "denied", "unknown"),
        "usage": ("recorded", "unknown"),
        "receiptScope": ("run", "storage", "unknown"),
        "cancellation": ("requested", "clear", "unknown"),
        "deadline": ("expired", "open", "unknown"),
    }
    observed = {}
    for key, choices in enums.items():
        value = state.get(key, "unknown")
        if value not in choices:
            raise ValueError("invalid recovery observation")
        observed[key] = value
    for key in ("attemptsAfterCheckpoint", "unknownToolReceipts"):
        value = state.get(key)
        if value is not None and (type(value) is not int or not 0 <= value <= 9007199254740991):
            raise ValueError("invalid recovery count")
        observed[key] = value
    reasons, cautions, unknown, actions = [], [], [], []
    rules = (
        ("status", "terminal", "run_terminal", "inspect_run"),
        ("checkpoint", "missing", "checkpoint_missing", "inspect_run"),
        ("lease", "active", "run_lease_conflict", "wait_for_lease"),
        ("configuration", "mismatch", "configuration_mismatch", "verify_configuration"),
        ("permissions", "denied", "permission_denied", "verify_permissions"),
        ("cancellation", "requested", "run_canceled", "inspect_run"),
        ("deadline", "expired", "run_deadline_exceeded", "inspect_run"),
    )
    for key, blocked, code, action in rules:
        if observed[key] == blocked:
            reasons.append(code)
            actions.append(action)
    if observed["attemptsAfterCheckpoint"] is not None and observed["attemptsAfterCheckpoint"] > 0:
        reasons.append("run_recovery_requires_reconciliation")
        actions.append("reconcile_attempt")
    if observed["unknownToolReceipts"] is not None and observed["unknownToolReceipts"] > 0:
        if observed["receiptScope"] == "run":
            reasons.append("tool_effect_unknown")
        else:
            cautions.append("unattributed_tool_effect_unknown")
        actions.append("reconcile_tools")
    for key in (*enums, "attemptsAfterCheckpoint", "unknownToolReceipts"):
        if observed[key] in (None, "unknown"):
            unknown.append(key)
    # Even zero storage-wide claims do not prove Run-specific external effects.
    if observed["receiptScope"] != "run":
        unknown.append("runToolEffects")
    unknown.extend(("externalToolEffects", "agentTreeOwnership"))
    for key, action in (("configuration", "verify_configuration"), ("permissions", "verify_permissions"), ("usage", "verify_usage")):
        if observed[key] == "unknown":
            actions.append(action)
    actions.append("revalidate_execution")
    return {"schemaVersion": 1, "authority": "diagnosis_only", "observations": observed,
            "blockers": reasons, "cautions": cautions, "unknown": unknown,
            "suggestedActions": list(dict.fromkeys(actions))}


async def inspect_recovery(repository, run_id: str) -> dict[str, Any]:
    """Read one canonical snapshot. Unavailable lease/effect/usage proofs stay unknown."""
    saved = await repository.get(run_id)
    return build_recovery_inspection({
        "status": "running" if saved.status.value == "running" else "terminal",
        "checkpoint": "present" if saved.execution_checkpoint is not None else "missing",
    })
