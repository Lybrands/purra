import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  AgentError,
  ArtifactAccessController,
  ArtifactAccessPolicy,
  ArtifactLifecycle,
  artifactCoverageDigest,
  InMemoryArtifactStore,
  InMemoryLongTaskRepository,
} from "purra";

const artifactFixture = JSON.parse(readFileSync(
  new URL("../../conformance/fixtures/artifact_protocol.json", import.meta.url),
  "utf8",
));

test("shared Artifact coverage and access policies stay aligned", async () => {
  for (const row of artifactFixture.coverageCases) {
    assert.equal(await artifactCoverageDigest(row.keys), row.digest, row.name);
  }
  const policy = new ArtifactAccessPolicy();
  for (const row of artifactFixture.accessCases) {
    const decision = policy.decide({
      artifactId: "artifact-shared",
      namespace: "tests",
      kind: "report",
      ownerId: "owner-1",
      ownerRef: { kind: "run", id: "run-1" },
      createdByRunId: "run-1",
      status: row.status,
      revision: 2,
    }, {
      artifactId: "artifact-shared",
      runId: row.runId,
      mode: row.mode,
      expectedRevision: row.expectedRevision,
    }, row.crossRunAuthorized);
    assert.equal(decision.allowed, row.allowed, row.name);
    assert.equal(decision.reason, row.reason, row.name);
    assert.equal(decision.requiresWriteClaim, row.requiresWriteClaim, row.name);
  }
});

test("Artifact lifecycle commits ordered batches, replays once, and finalizes manifests", async () => {
  const store = new InMemoryArtifactStore({ tokenFactory: () => "claim-1" });
  const lifecycle = new ArtifactLifecycle({
    repository: store,
    idFactory: () => "artifact-1",
  });
  const created = await lifecycle.begin(createCommand({ expectedItemCount: 2 }));
  const grant = await new ArtifactAccessController(store).authorize(
    candidate(created),
    accessRequest(created, "run-1", "write"),
    1_000,
  );
  const lease = mutationLease(grant.writeClaim);
  const first = await lifecycle.append({
    artifactId: created.id,
    expectedRevision: created.revision,
    sequence: 1,
    batchId: "batch-1",
    idempotencyKey: "append-1",
    items: [{ value: 1 }],
    coverageKeys: ["one"],
    writeLease: lease,
  });
  const replay = await lifecycle.append({
    artifactId: created.id,
    expectedRevision: created.revision,
    sequence: 1,
    batchId: "batch-1",
    idempotencyKey: "append-1",
    items: [{ value: 1 }],
    coverageKeys: ["one"],
    writeLease: lease,
  });
  assert.equal(replay.replayed, true);
  assert.equal(replay.committedRevision, first.committedRevision);
  assert.equal((await store.listBatches(created.id)).length, 1);

  await rejectsCode(lifecycle.append({
    artifactId: created.id,
    expectedRevision: first.committedRevision,
    sequence: 2,
    batchId: "batch-conflict",
    idempotencyKey: "append-1",
    items: [{ value: "different" }],
    coverageKeys: ["two"],
    writeLease: lease,
  }), "artifact_idempotency_conflict");
  await rejectsCode(lifecycle.append({
    artifactId: created.id,
    expectedRevision: first.committedRevision,
    sequence: 3,
    batchId: "batch-gap",
    idempotencyKey: "append-gap",
    items: [{ value: 2 }],
    coverageKeys: ["two"],
    writeLease: lease,
  }), "artifact_sequence_conflict");

  const second = await lifecycle.append({
    artifactId: created.id,
    expectedRevision: first.committedRevision,
    sequence: 2,
    batchId: "batch-2",
    idempotencyKey: "append-2",
    items: [{ value: 2 }],
    coverageKeys: ["two"],
    writeLease: lease,
  });
  await rejectsCode(lifecycle.finalize({
    artifactId: created.id,
    expectedRevision: second.committedRevision,
    expectedItemCount: 2,
    expectedCoverageKeys: ["one", "missing"],
    writeLease: lease,
  }), "artifact_coverage_mismatch");
  const finalized = await lifecycle.finalize({
    artifactId: created.id,
    expectedRevision: second.committedRevision,
    expectedItemCount: 2,
    expectedCoverageKeys: ["one", "two"],
    resourceRef: "memory://artifact-1",
    writeLease: lease,
  });
  assert.equal(finalized.status, "finalized");
  assert.equal(finalized.committedItemCount, 2);
  assert.match(finalized.coverageDigest, /^[a-f0-9]{64}$/);
  assert.equal(await store.loadActive(created.id), undefined);
  await rejectsCode(lifecycle.append({
    artifactId: created.id,
    expectedRevision: finalized.revision,
    sequence: 3,
    batchId: "late",
    idempotencyKey: "late",
    items: [{ value: 3 }],
    writeLease: lease,
  }), "artifact_not_open");
});

test("Artifact access is fail-closed across Runs and expired claims fence old writers", async () => {
  let now = 100;
  const tokens = ["claim-old", "claim-new"];
  const store = new InMemoryArtifactStore({
    clockMs: () => now,
    tokenFactory: () => tokens.shift(),
  });
  const lifecycle = new ArtifactLifecycle({
    repository: store,
    idFactory: () => "artifact-access",
  });
  const artifact = await lifecycle.begin(createCommand());
  const access = new ArtifactAccessController(store);
  await rejectsCode(
    access.authorize(candidate(artifact), accessRequest(artifact, "run-2", "read")),
    "cross_run_not_authorized",
  );
  const first = await access.authorize(
    candidate(artifact),
    accessRequest(artifact, "run-1", "write"),
    10,
  );
  await rejectsCode(
    new ArtifactAccessController(store, {
      authorizer: { authorize: () => true },
    }).authorize(
      candidate(artifact),
      accessRequest(artifact, "run-2", "write"),
      10,
    ),
    "artifact_write_claim_conflict",
  );

  now = first.writeClaim.expiresAtMs;
  const second = await new ArtifactAccessController(store, {
    authorizer: { authorize: () => true },
  }).authorize(
    candidate(artifact),
    accessRequest(artifact, "run-2", "write"),
    10,
  );
  assert.notEqual(second.writeClaim.claimToken, first.writeClaim.claimToken);
  await rejectsCode(lifecycle.append({
    artifactId: artifact.id,
    expectedRevision: artifact.revision,
    sequence: 1,
    batchId: "stale",
    idempotencyKey: "stale",
    items: [{ value: "stale" }],
    writeLease: mutationLease(first.writeClaim),
  }), "artifact_write_claim_invalid");

  const receipt = await lifecycle.append({
    artifactId: artifact.id,
    expectedRevision: artifact.revision,
    sequence: 1,
    batchId: "current",
    idempotencyKey: "current",
    items: [{ value: "current" }],
    writeLease: mutationLease(second.writeClaim),
  });
  assert.equal(receipt.committedRevision, 2);
});

test("Artifact validation, coverage, revision, and abort failures do not partially commit", async () => {
  const store = new InMemoryArtifactStore({ tokenFactory: () => "claim-validation" });
  let rejectBatch = true;
  const lifecycle = new ArtifactLifecycle({
    repository: store,
    idFactory: () => "artifact-validation",
    validator: {
      validateBatch() {
        return rejectBatch
          ? { accepted: false, code: "fixture_batch_rejected" }
          : { accepted: true };
      },
      validateFinalization() { return { accepted: true }; },
    },
  });
  const artifact = await lifecycle.begin(createCommand({ expectedItemCount: 1 }));
  const claim = (await new ArtifactAccessController(store).authorize(
    candidate(artifact),
    accessRequest(artifact, "run-1", "write"),
    1_000,
  )).writeClaim;
  const command = {
    artifactId: artifact.id,
    expectedRevision: artifact.revision,
    sequence: 1,
    batchId: "batch",
    idempotencyKey: "append",
    items: [{ value: 1 }],
    coverageKeys: ["one"],
    writeLease: mutationLease(claim),
  };
  await rejectsCode(lifecycle.append(command), "fixture_batch_rejected");
  assert.equal((await lifecycle.get(artifact.id)).revision, 1);
  assert.equal((await store.listBatches(artifact.id)).length, 0);

  rejectBatch = false;
  const receipt = await lifecycle.append(command);
  await rejectsCode(lifecycle.append({
    ...command,
    expectedRevision: receipt.committedRevision,
    sequence: 2,
    batchId: "duplicate-coverage",
    idempotencyKey: "append-2",
  }), "artifact_coverage_duplicate");
  await rejectsCode(lifecycle.finalize({
    artifactId: artifact.id,
    expectedRevision: artifact.revision,
    writeLease: mutationLease(claim),
  }), "artifact_revision_conflict");

  const aborted = await lifecycle.abort({
    artifactId: artifact.id,
    expectedRevision: receipt.committedRevision,
    writeLease: mutationLease(claim),
  });
  assert.equal(aborted.status, "aborted");
});

test("Artifact maintenance is bounded, releases invalid claims, and never purges open state", async () => {
  let now = 1_000;
  const unavailableRuns = new Set(["run-unavailable"]);
  let token = 0;
  const store = new InMemoryArtifactStore({
    clockMs: () => now,
    tokenFactory: () => `claim-${++token}`,
    runIsAvailable: (runId) => !unavailableRuns.has(runId),
  });
  const open = await store.create("artifact-open", createCommand());
  await store.acquire({
    artifactId: open.id,
    runId: "run-unavailable",
    expectedRevision: open.revision,
    leaseDurationMs: 100,
  });
  const finalized = await finalizeDirect(store, "artifact-finalized", "run-finalized");
  const aborted = await abortDirect(store, "artifact-aborted", "run-aborted");

  const report = await store.maintain({
    terminalRetentionMs: 0,
    maxPurgeArtifacts: 1,
  }, now);
  assert.equal(report.unavailableRunClaimsReleased, 1);
  assert.equal(report.purgedArtifacts, 1);
  assert.notEqual(await store.load(open.id), undefined);
  const retainedTerminal = (await Promise.all(
    [finalized.id, aborted.id].map((id) => store.load(id)),
  )).filter((artifact) => artifact !== undefined);
  assert.equal(retainedTerminal.length, 1);
  const snapshot = await store.inspect({ timestampMs: now });
  assert.equal(snapshot.openArtifacts, 1);
  assert.equal(snapshot.finalizedArtifacts + snapshot.abortedArtifacts, 1);
  assert.equal(snapshot.activeClaims, 0);
});

test("Artifact finalization never completes its owner Long Task", async () => {
  const longTasks = new InMemoryLongTaskRepository();
  await longTasks.create("task-owner", {
    namespace: "tests",
    kind: "artifact-owner",
    ownerId: "owner-1",
    createdByRunId: "run-1",
    idempotencyKey: "task-owner",
    units: [{
      id: "unit-1",
      position: 0,
      executor: "fixture",
      planStepId: "step-1",
    }],
    deadlineAtMs: null,
    budgets: {
      maxInvocationAttempts: 2,
      maxInputTokens: 100,
      maxOutputTokens: 100,
      maxReasoningTokens: 100,
    },
  });
  await longTasks.start("task-owner");

  const store = new InMemoryArtifactStore({ tokenFactory: () => "claim-owner" });
  const lifecycle = new ArtifactLifecycle({
    repository: store,
    idFactory: () => "artifact-owner",
  });
  const artifact = await lifecycle.begin(createCommand({
    ownerRef: { kind: "long_task", id: "task-owner" },
    expectedItemCount: 1,
  }));
  const claim = await store.acquire({
    artifactId: artifact.id,
    runId: "run-1",
    expectedRevision: artifact.revision,
    leaseDurationMs: 1_000,
  });
  const receipt = await lifecycle.append({
    artifactId: artifact.id,
    expectedRevision: artifact.revision,
    sequence: 1,
    batchId: "batch",
    idempotencyKey: "append",
    items: [{ done: true }],
    writeLease: mutationLease(claim),
  });
  await lifecycle.finalize({
    artifactId: artifact.id,
    expectedRevision: receipt.committedRevision,
    expectedItemCount: 1,
    writeLease: mutationLease(claim),
  });
  assert.equal((await longTasks.load("task-owner")).status, "running");
});

async function finalizeDirect(store, artifactId, runId) {
  const lifecycle = new ArtifactLifecycle({ repository: store, idFactory: () => artifactId });
  const artifact = await lifecycle.begin(createCommand({
    createdByRunId: runId,
    ownerId: artifactId,
    ownerRef: { kind: "run", id: runId },
    expectedItemCount: 1,
  }));
  const claim = await store.acquire({
    artifactId,
    runId,
    expectedRevision: artifact.revision,
    leaseDurationMs: 100,
  });
  const receipt = await lifecycle.append({
    artifactId,
    expectedRevision: artifact.revision,
    sequence: 1,
    batchId: "batch",
    idempotencyKey: "append",
    items: [{ value: artifactId }],
    writeLease: mutationLease(claim),
  });
  return lifecycle.finalize({
    artifactId,
    expectedRevision: receipt.committedRevision,
    expectedItemCount: 1,
    writeLease: mutationLease(claim),
  });
}

async function abortDirect(store, artifactId, runId) {
  const lifecycle = new ArtifactLifecycle({ repository: store, idFactory: () => artifactId });
  const artifact = await lifecycle.begin(createCommand({
    createdByRunId: runId,
    ownerId: artifactId,
    ownerRef: { kind: "run", id: runId },
  }));
  const claim = await store.acquire({
    artifactId,
    runId,
    expectedRevision: artifact.revision,
    leaseDurationMs: 100,
  });
  return lifecycle.abort({
    artifactId,
    expectedRevision: artifact.revision,
    writeLease: mutationLease(claim),
  });
}

function createCommand(overrides = {}) {
  return {
    namespace: "tests",
    kind: "report",
    ownerId: "owner-1",
    ownerRef: { kind: "run", id: "run-1" },
    createdByRunId: "run-1",
    ...overrides,
  };
}

function candidate(artifact) {
  return {
    artifactId: artifact.id,
    namespace: artifact.namespace,
    kind: artifact.kind,
    ownerId: artifact.ownerId,
    ownerRef: artifact.ownerRef,
    createdByRunId: artifact.createdByRunId,
    status: artifact.status,
    revision: artifact.revision,
    committedItemCount: artifact.committedItemCount,
    expectedItemCount: artifact.expectedItemCount,
  };
}

function accessRequest(artifact, runId, mode) {
  return {
    artifactId: artifact.id,
    runId,
    mode,
    expectedRevision: artifact.revision,
  };
}

function mutationLease(claim) {
  return {
    runId: claim.runId,
    claimToken: claim.claimToken,
    leaseDurationMs: 1_000,
  };
}

async function rejectsCode(promise, code) {
  await assert.rejects(
    promise,
    (error) => error instanceof AgentError && error.code === code,
  );
}
