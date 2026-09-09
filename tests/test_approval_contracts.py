import json
from dataclasses import replace
from pathlib import Path

import pytest
from purra.approvals import ApprovalIntent, ApprovalRecord, ApprovalDecisionCommand

FIXTURE = json.loads((Path(__file__).parents[1] / "conformance/fixtures/approval_records.json").read_text())


def test_intent_identity_is_shared_immutable_and_binds_every_field():
    raw = json.loads(json.dumps(FIXTURE["intent"]))
    intent = ApprovalIntent.from_mapping(raw)
    assert intent.digest == FIXTURE["intentDigest"]
    raw["arguments"]["value"]["a"] = 99
    assert intent.arguments["value"]["a"] == 1
    assert replace(intent, arguments={"value": {"a": 1, "b": 2}, "target": "fixture/你好"}).digest == intent.digest
    for name in ("run_id", "root_run_id", "tool_call_id", "tool_name", "preset_fingerprint", "binding_id", "binding_revision", "scope_id", "scope_revision"):
        assert replace(intent, **{name: getattr(intent, name) + "-changed"}).digest != intent.digest
    assert replace(intent, effect="destructive").digest != intent.digest
    assert replace(intent, arguments={"value": 2}).digest != intent.digest
    with pytest.raises(TypeError):
        intent.arguments["value"]["a"] = 3


@pytest.mark.parametrize("field,value", [("schemaVersion", True), ("schemaVersion", 2), ("effect", "read"), ("scopeRevision", ""), ("runId", " run"), ("arguments", []), ("extra", True)])
def test_invalid_intent_rejected(field, value):
    with pytest.raises((ValueError, TypeError)):
        ApprovalIntent.from_mapping({**FIXTURE["intent"], field: value})


def test_record_requires_bound_audit_and_roundtrips_without_granting_execution():
    intent = ApprovalIntent.from_mapping(FIXTURE["intent"])
    pending = ApprovalRecord("approval", intent, 1, "pending", 1000, 2000)
    command = ApprovalDecisionCommand("approval", 1, intent.digest, "command", "approve")
    with pytest.raises(ValueError):
        replace(pending, status="approved")
    approved = replace(pending, revision=2, status="approved", decision_audit={"command": command.to_mapping(), "principalId": "host-user", "decidedAtMs": 1200, "revision": 2})
    assert ApprovalRecord.from_mapping(approved.to_mapping()) == approved
    for field, value in [("approvalId", "other"), ("intentDigest", "forged"), ("revision", 1), ("status", "rejected")]:
        with pytest.raises((ValueError, TypeError)):
            ApprovalRecord.from_mapping({**approved.to_mapping(), field: value})
    assert replace(approved, status="expired", revision=3).decision_audit == approved.decision_audit


@pytest.mark.parametrize("revision", [True, 0, -1, 1.5, 9007199254740992])
def test_decision_revisions_are_interoperable_integers(revision):
    with pytest.raises(ValueError):
        ApprovalDecisionCommand("id", revision, "digest", "key", "approve")
