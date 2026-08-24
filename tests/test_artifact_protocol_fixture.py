from __future__ import annotations

import json
from pathlib import Path

from purra.artifacts import (
    ArtifactAccessMode,
    ArtifactAccessPolicy,
    ArtifactAccessReason,
    ArtifactAccessRequest,
    ArtifactOwnerRef,
    ArtifactResumeCandidate,
    ArtifactStatus,
)
from purra.artifacts.contracts import coverage_digest


FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "artifact_protocol.json").read_text()
)


def test_shared_artifact_protocol_fixture() -> None:
    for row in FIXTURE["coverageCases"]:
        assert coverage_digest(tuple(row["keys"])) == row["digest"], row["name"]

    policy = ArtifactAccessPolicy()
    for row in FIXTURE["accessCases"]:
        decision = policy.decide(
            ArtifactResumeCandidate(
                artifact_id="artifact-shared",
                namespace="tests",
                kind="report",
                owner_id="owner-1",
                owner_ref=ArtifactOwnerRef("run", "run-1"),
                created_by_run_id="run-1",
                status=ArtifactStatus(row["status"]),
                revision=2,
            ),
            ArtifactAccessRequest(
                artifact_id="artifact-shared",
                run_id=row["runId"],
                mode=ArtifactAccessMode(row["mode"]),
                expected_revision=row["expectedRevision"],
            ),
            cross_run_authorized=row["crossRunAuthorized"],
        )
        assert decision.allowed is row["allowed"], row["name"]
        assert decision.reason is ArtifactAccessReason(row["reason"]), row["name"]
        assert decision.requires_write_claim is row["requiresWriteClaim"], row["name"]
