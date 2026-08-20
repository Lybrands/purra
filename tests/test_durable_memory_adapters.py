from __future__ import annotations

import asyncio

import pytest

from purra.adapters import InMemoryAgentAdapters, InMemoryDurableAdapters
from purra.artifacts import (
    ArtifactCreateCommand,
    ArtifactOwnerRef,
    ArtifactWriteClaimCommand,
)
from purra.testing import (
    assert_artifact_store_conforms,
    assert_long_task_repository_conforms,
)
from purra.long_tasks import (
    LongTaskCreateCommand,
    LongTaskSplitResult,
    LongTaskUnitSpec,
)
from purra.recovery import (
    FailureCategory,
    FailureDecision,
    FailureDisposition,
    RecoveryEffectState,
)


def test_reference_durable_adapters_conform_to_public_ports():
    async def scenario() -> None:
        adapters = InMemoryDurableAdapters()
        await assert_artifact_store_conforms(
            artifacts=adapters.artifacts,
            claims=adapters.artifact_claims,
            maintenance=adapters.artifact_maintenance,
        )
        await assert_long_task_repository_conforms(adapters.long_tasks)

    asyncio.run(scenario())


def test_agent_adapter_bundle_includes_durable_reference_stores():
    adapters = InMemoryAgentAdapters()

    assert adapters.artifacts is adapters.artifact_claims
    assert adapters.artifacts is adapters.artifact_maintenance
    assert adapters.long_tasks is not None


def test_expired_artifact_claim_can_be_taken_over_atomically():
    async def scenario() -> None:
        now = 1_000
        adapters = InMemoryDurableAdapters(clock_ms=lambda: now)
        artifact = await adapters.artifacts.create(
            "artifact-expiry",
            ArtifactCreateCommand(
                namespace="operations",
                kind="incident_report",
                owner_id="incident-42",
                owner_ref=ArtifactOwnerRef("durable_task", "task-42"),
                created_by_run_id="run-1",
            ),
        )
        first = await adapters.artifact_claims.acquire(
            ArtifactWriteClaimCommand(
                artifact_id=artifact.id,
                run_id="run-1",
                expected_revision=artifact.revision,
                lease_duration_ms=10,
            )
        )
        now = first.expires_at_ms
        second = await adapters.artifact_claims.acquire(
            ArtifactWriteClaimCommand(
                artifact_id=artifact.id,
                run_id="run-2",
                expected_revision=artifact.revision,
                lease_duration_ms=10,
            )
        )

        assert second.run_id == "run-2"
        assert second.claim_token != first.claim_token

    asyncio.run(scenario())


def test_failed_unit_expansion_does_not_partially_change_manifest():
    async def scenario() -> None:
        repository = InMemoryDurableAdapters().long_tasks
        task = await repository.create(
            "task-split",
            LongTaskCreateCommand(
                namespace="operations",
                kind="report",
                owner_id="incident-42",
                created_by_run_id="run-1",
                units=(
                    LongTaskUnitSpec(id="parent", position=0),
                    LongTaskUnitSpec(
                        id="downstream",
                        position=1,
                        dependencies=("parent",),
                    ),
                ),
            ),
        )
        await repository.start(task.id, expected_revision=task.revision)
        parent = await repository.claim_ready_unit(
            task.id,
            worker_id="worker-1",
            lease_duration_ms=30_000,
        )
        assert parent is not None
        decision = FailureDecision(
            category=FailureCategory.BUSINESS_INVARIANT,
            code="split_required",
            disposition=FailureDisposition.SPLIT_PART,
            attempts_remaining=1,
            effect_state=RecoveryEffectState.NOT_STARTED,
            checkpoint_available=False,
            part_splittable=True,
        )
        before = await repository.list_units(task.id)
        with pytest.raises(ValueError, match="replace"):
            await repository.expand_unit(
                task.id,
                parent.id,
                worker_id="worker-1",
                split=LongTaskSplitResult(
                    children=(LongTaskUnitSpec(id="child", position=2),),
                    replacement_dependency_ids=(),
                ),
                decision=decision,
            )

        assert await repository.list_units(task.id) == before

    asyncio.run(scenario())
