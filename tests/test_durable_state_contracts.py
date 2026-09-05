from __future__ import annotations

import asyncio
import inspect

import pytest

from purra.artifacts import (
    ArtifactAccessController,
    ArtifactAccessMode,
    ArtifactAccessRequest,
    ArtifactOwnerRef,
    ArtifactResumeCandidate,
    ArtifactStatus,
    ArtifactWriteClaim,
)
from purra.artifacts.errors import ArtifactAccessDeniedError
from purra.long_tasks import (
    LongTaskCreateCommand,
    LongTaskRunBinding,
    LongTaskRunRelation,
    LongTaskUnitSpec,
)
from purra.observability import build_canonical_run_observation
from purra.recovery import (
    FailureCategory,
    FailureDisposition,
    FailureScope,
    FailureSignal,
    RecoveryEffectState,
    decide_failure,
)


class _ClaimRepository:
    async def acquire(self, command):
        return ArtifactWriteClaim(
            artifact_id=command.artifact_id,
            run_id=command.run_id,
            claim_token="claim-1",
            acquired_revision=command.expected_revision,
            expires_at_ms=10_000,
        )


class _AllowCrossRun:
    async def authorize(self, candidate, request):
        return candidate.owner_ref == ArtifactOwnerRef("task", "task-1")


def _candidate() -> ArtifactResumeCandidate:
    return ArtifactResumeCandidate(
        artifact_id="artifact-1",
        namespace="tests",
        kind="report",
        owner_id="owner-1",
        owner_ref=ArtifactOwnerRef("task", "task-1"),
        created_by_run_id="run-1",
        status=ArtifactStatus.OPEN,
        revision=2,
    )


def test_artifact_cross_run_access_is_fail_closed():
    async def scenario() -> None:
        controller = ArtifactAccessController(_ClaimRepository())
        with pytest.raises(
            ArtifactAccessDeniedError,
            match="artifact access was denied",
        ):
            await controller.authorize(
                _candidate(),
                ArtifactAccessRequest(
                    artifact_id="artifact-1",
                    run_id="run-2",
                    mode=ArtifactAccessMode.READ,
                    expected_revision=2,
                ),
            )

    asyncio.run(scenario())


def test_every_artifact_write_requires_an_exclusive_claim():
    async def scenario() -> None:
        controller = ArtifactAccessController(
            _ClaimRepository(),
            authorizer=_AllowCrossRun(),
        )
        grant = await controller.authorize(
            _candidate(),
            ArtifactAccessRequest(
                artifact_id="artifact-1",
                run_id="run-2",
                mode=ArtifactAccessMode.WRITE,
                expected_revision=2,
            ),
            lease_duration_ms=1_000,
        )
        assert grant.decision.requires_write_claim
        assert grant.write_claim is not None
        assert grant.write_claim.run_id == "run-2"

    asyncio.run(scenario())


def test_durable_task_owns_run_bindings_without_work_item_identity():
    command = LongTaskCreateCommand(
        namespace="tests",
        kind="report",
        owner_id="owner-1",
        created_by_run_id="run-1",
        units=(LongTaskUnitSpec(id="unit-1", position=0),),
    )
    binding = LongTaskRunBinding(
        task_id="task-1",
        run_id="run-2",
        relation=LongTaskRunRelation.CONTINUATION,
    )

    assert "work_item_id" not in inspect.signature(LongTaskCreateCommand).parameters
    assert command.created_by_run_id == "run-1"
    assert binding.relation is LongTaskRunRelation.CONTINUATION


def test_observability_is_derived_from_events_without_recovery_state():
    observation = build_canonical_run_observation((
        {
            "eventType": "agentRunTrace",
            "payload": {"stage": "planning", "outcome": "model_plan"},
        },
    ))

    assert observation.by_stage["planner"][0]["outcome"] == "model_plan"


@pytest.mark.parametrize(
    ("category", "retryable", "scope"),
    (
        (FailureCategory.MODEL_OUTPUT_INVALID, False, FailureScope.LOCAL),
        (FailureCategory.TRANSIENT_PROVIDER, True, FailureScope.LOCAL),
        (
            FailureCategory.PROTOCOL_INCOMPATIBLE,
            False,
            FailureScope.SYSTEMIC,
        ),
    ),
)
def test_exhausted_failures_are_terminal_instead_of_implicitly_paused(
    category,
    retryable,
    scope,
):
    decision = decide_failure(
        FailureSignal(
            category=category,
            code="injected_failure",
            retryable=retryable,
            scope=scope,
        ),
        attempts_remaining=0,
    )

    assert decision.disposition is FailureDisposition.FAIL_PERMANENT


def test_unsafe_effect_uncertainty_fails_without_automatic_replay():
    decision = decide_failure(
        FailureSignal(
            category=FailureCategory.TOOL_EXECUTION,
            code="tool_effect_unknown",
            retryable=True,
            effect_state=RecoveryEffectState.UNKNOWN,
        ),
        attempts_remaining=3,
    )

    assert decision.disposition is FailureDisposition.FAIL_PERMANENT
