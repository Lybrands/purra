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
    LongTaskCoordinator,
    LongTaskCreateCommand,
    LongTaskSplitResult,
    LongTaskUnitResult,
    LongTaskUnitSpec,
    LongTaskStatus,
    LongTaskUnitStatus,
)
from purra.errors import ContractViolationError
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
                lease_epoch=parent.lease_epoch,
                split=LongTaskSplitResult(
                    children=(LongTaskUnitSpec(id="child", position=2),),
                    replacement_dependency_ids=(),
                ),
                decision=decision,
            )

        assert await repository.list_units(task.id) == before

    asyncio.run(scenario())


def test_unit_lease_epoch_fences_same_worker_reclaim_and_renews_atomically():
    async def scenario() -> None:
        now = 1_000
        repository = InMemoryDurableAdapters(clock_ms=lambda: now).long_tasks
        task = await repository.create(
            "task-lease-epoch",
            LongTaskCreateCommand(
                namespace="operations",
                kind="report",
                owner_id="incident-42",
                created_by_run_id="run-1",
                units=(LongTaskUnitSpec(id="report", position=0),),
            ),
        )
        await repository.start(task.id, expected_revision=task.revision)
        first = await repository.claim_ready_unit(
            task.id,
            worker_id="worker-1",
            lease_duration_ms=10,
        )
        assert first is not None

        now = 1_010
        second = await repository.claim_ready_unit(
            task.id,
            worker_id="worker-1",
            lease_duration_ms=10,
        )
        assert second is not None
        assert second.lease_epoch == first.lease_epoch + 1

        retry = FailureDecision(
            category=FailureCategory.TRANSIENT_PROVIDER,
            code="retry",
            disposition=FailureDisposition.RETRY_ATTEMPT,
            attempts_remaining=1,
            effect_state=RecoveryEffectState.NOT_STARTED,
            checkpoint_available=False,
        )
        split = FailureDecision(
            category=FailureCategory.BUSINESS_INVARIANT,
            code="split",
            disposition=FailureDisposition.SPLIT_PART,
            attempts_remaining=1,
            effect_state=RecoveryEffectState.NOT_STARTED,
            checkpoint_available=False,
            part_splittable=True,
        )
        stale_mutations = (
            lambda: repository.bind_unit_run(
                task.id, first.id, worker_id="worker-1",
                lease_epoch=first.lease_epoch, run_id="run-stale",
            ),
            lambda: repository.renew_unit_lease(
                task.id, first.id, worker_id="worker-1",
                lease_epoch=first.lease_epoch, lease_duration_ms=20,
            ),
            lambda: repository.update_unit_progress(
                task.id, first.id, worker_id="worker-1",
                lease_epoch=first.lease_epoch, metadata={"stale": True},
            ),
            lambda: repository.complete_unit(
                task.id,
                first.id,
                worker_id="worker-1",
                lease_epoch=first.lease_epoch,
                result=LongTaskUnitResult(output_ref="memory://stale"),
            ),
            lambda: repository.settle_unit_failure(
                task.id, first.id, worker_id="worker-1",
                lease_epoch=first.lease_epoch, decision=retry,
            ),
            lambda: repository.expand_unit(
                task.id, first.id, worker_id="worker-1",
                lease_epoch=first.lease_epoch,
                split=LongTaskSplitResult(
                    children=(LongTaskUnitSpec(id="stale-child", position=1),),
                    replacement_dependency_ids=("stale-child",),
                ),
                decision=split,
            ),
            lambda: repository.interrupt_unit(
                task.id, first.id, worker_id="worker-1",
                lease_epoch=first.lease_epoch, reason_code="stale",
            ),
        )
        for mutation in stale_mutations:
            with pytest.raises(ContractViolationError) as stale:
                await mutation()
            assert stale.value.code == "long_task_unit_lease_lost"

        renewed = await repository.renew_unit_lease(
            task.id,
            second.id,
            worker_id="worker-1",
            lease_epoch=second.lease_epoch,
            lease_duration_ms=20,
        )
        assert renewed.lease_expires_at_ms == 1_030
        result = LongTaskUnitResult(output_ref="memory://current")
        completed = await repository.complete_unit(
            task.id,
            second.id,
            worker_id="worker-1",
            lease_epoch=second.lease_epoch,
            result=result,
        )
        assert await repository.complete_unit(
            task.id,
            second.id,
            worker_id="worker-1",
            lease_epoch=second.lease_epoch,
            result=result,
        ) == completed

    asyncio.run(scenario())


def test_exhausted_expired_unit_is_a_terminal_failure_not_a_pause():
    async def scenario() -> None:
        now = 1_000
        repository = InMemoryDurableAdapters(clock_ms=lambda: now).long_tasks
        task = await repository.create(
            "task-expired-attempts",
            LongTaskCreateCommand(
                namespace="operations",
                kind="report",
                owner_id="incident-42",
                created_by_run_id="run-1",
                units=(
                    LongTaskUnitSpec(
                        id="report",
                        position=0,
                        max_attempts=1,
                    ),
                ),
            ),
        )
        await repository.start(task.id, expected_revision=task.revision)
        claimed = await repository.claim_ready_unit(
            task.id,
            worker_id="worker-1",
            lease_duration_ms=10,
        )
        assert claimed is not None

        now = 1_010
        assert await repository.claim_ready_unit(
            task.id,
            worker_id="worker-2",
            lease_duration_ms=10,
        ) is None

        failed = await repository.load(task.id)
        units = await repository.list_units(task.id)
        assert failed is not None
        assert failed.status is LongTaskStatus.FAILED
        assert units[0].status is LongTaskUnitStatus.FAILED
        assert units[0].error_code == "lease_expired_attempts_exhausted"

    asyncio.run(scenario())


def test_long_task_deadline_fails_atomically_before_a_new_claim():
    async def scenario() -> None:
        now = 5_000
        repository = InMemoryDurableAdapters(clock_ms=lambda: now).long_tasks
        task = await repository.create(
            "task-deadline",
            LongTaskCreateCommand(
                namespace="operations",
                kind="report",
                owner_id="incident-42",
                created_by_run_id="run-1",
                units=(LongTaskUnitSpec(id="report", position=0),),
                deadline_at_ms=5_010,
            ),
        )
        started = await repository.start(task.id, expected_revision=task.revision)
        assert started.status.value == "running"

        now = 5_010
        assert await repository.claim_ready_unit(
            task.id,
            worker_id="worker-1",
            lease_duration_ms=1_000,
        ) is None
        expired = await repository.load(task.id)
        units = await repository.list_units(task.id)

        assert expired is not None and expired.status.value == "failed"
        assert units[0].error_code == "long_task_deadline_exceeded"

    asyncio.run(scenario())


def test_heartbeat_lease_loss_cancels_executor_and_fails_after_attempts():
    async def scenario() -> None:
        now = 1_000
        base = InMemoryDurableAdapters(clock_ms=lambda: now).long_tasks

        class LosingRepository:
            def __getattr__(self, name):
                return getattr(base, name)

            async def renew_unit_lease(self, *args, **kwargs):
                nonlocal now
                now += 3
                raise ContractViolationError(
                    "forced lease loss",
                    code="long_task_unit_lease_lost",
                )

        class BlockingRunner:
            cancellations = 0

            async def run_unit(self, task, unit, signal=None):
                del task, unit, signal
                try:
                    await asyncio.Event().wait()
                finally:
                    self.cancellations += 1

        task = await base.create(
            "task-heartbeat-loss",
            LongTaskCreateCommand(
                namespace="operations",
                kind="report",
                owner_id="incident-42",
                created_by_run_id="run-1",
                units=(
                    LongTaskUnitSpec(id="report", position=0, max_attempts=2),
                ),
            ),
        )
        runner = BlockingRunner()
        settled = await LongTaskCoordinator(
            LosingRepository(),
            worker_id="worker-1",
            lease_duration_ms=3,
            idle_poll_ms=1,
        ).run(task.id, runner)
        units = await base.list_units(task.id)
        assert settled.status.value == "failed"
        assert runner.cancellations == 2
        assert units[0].status.value == "failed"
        assert units[0].error_code == "lease_expired_attempts_exhausted"
        assert units[0].output_ref is None

    asyncio.run(scenario())


def test_settlement_observer_failure_is_not_reclassified_as_unit_failure():
    async def scenario() -> None:
        repository = InMemoryDurableAdapters().long_tasks
        task = await repository.create(
            "task-settlement-observer-failure",
            LongTaskCreateCommand(
                namespace="operations",
                kind="report",
                owner_id="incident-42",
                created_by_run_id="run-1",
                units=(
                    LongTaskUnitSpec(id="first", position=0),
                    LongTaskUnitSpec(
                        id="second",
                        position=1,
                        dependencies=("first",),
                    ),
                ),
            ),
        )

        class Runner:
            async def run_unit(self, task, unit, signal=None):
                del task, signal
                return LongTaskUnitResult(output_ref=f"memory://{unit.id}")

            async def on_unit_settled(self, task_id):
                del task_id
                raise RuntimeError("checkpoint_observer_failed")

        with pytest.raises(RuntimeError, match="checkpoint_observer_failed"):
            await LongTaskCoordinator(
                repository,
                worker_id="worker-1",
                idle_poll_ms=1,
            ).run(task.id, Runner())

        units = await repository.list_units(task.id)
        assert units[0].status is LongTaskUnitStatus.COMPLETED
        assert units[0].error_code is None
        assert units[1].status is LongTaskUnitStatus.PENDING

    asyncio.run(scenario())
