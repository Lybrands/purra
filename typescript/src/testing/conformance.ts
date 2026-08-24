import { prepareArtifactAppend } from "../artifacts/contracts.js";
import type { ArtifactClaimRepository, ArtifactRepository } from "../artifacts/types.js";
import type { DelegationRepository } from "../delegation/types.js";
import { claimFromUnit } from "../durable/repository.js";
import type { LongTaskRepository } from "../durable/types.js";
import type { JsonValue, ModelGateway, ModelRequest } from "../model/types.js";
import { copyCapabilitySnapshot, validateModelTurn } from "../model/validation.js";
import type { OutputEvent, OutputPublisher } from "../output/types.js";
import { AgentError } from "../shared/errors.js";
import { ToolCatalog } from "../tools/catalog.js";
import type { ToolContext, ToolDefinition } from "../tools/types.js";

export async function assertModelGatewayConforms(input: {
  readonly gateway: ModelGateway;
  readonly request?: ModelRequest;
}): Promise<void> {
  const capabilities = input.gateway.capabilities === undefined
    ? undefined
    : copyCapabilitySnapshot(input.gateway.capabilities);
  if (capabilities?.protocol.streaming === "supported" && typeof input.gateway.stream !== "function") {
    nonconforming("model_gateway_nonconforming", "Model capabilities claim an unavailable stream interface");
  }
  const request = input.request ?? Object.freeze({
    messages: Object.freeze([{ role: "user" as const, content: "conformance probe" }]),
    tools: Object.freeze([]),
  });
  const turn = validateModelTurn(await input.gateway.invoke(request));
  if (turn.finishReason !== "stop" || (turn.message.toolCalls?.length ?? 0) !== 0) {
    nonconforming("model_gateway_nonconforming", "Model probe requires one terminal assistant turn");
  }
}

export async function assertToolDefinitionConforms(input: {
  readonly definition: ToolDefinition;
  readonly validInput: JsonValue;
  readonly invalidInput: JsonValue;
}): Promise<void> {
  let calls = 0;
  const definition: ToolDefinition = Object.freeze({
    ...input.definition,
    run(value: JsonValue, context: ToolContext) {
      calls += 1;
      return input.definition.run(value, context);
    },
  });
  const catalog = new ToolCatalog([definition]);
  const options = { executionKey: `conformance:${uniqueId()}` };
  let executionError: string | undefined;
  await catalog.executeBatch([{
    id: "valid-call",
    name: definition.name,
    arguments: input.validInput,
  }], {
    ...options,
    onEvent(event) {
      if (event.type === "tool_completed") executionError = event.errorCode;
    },
  });
  if (calls !== 1 || executionError !== undefined) {
    nonconforming("tool_definition_nonconforming", "Valid tool input did not produce one valid receipt");
  }
  await requireRejected(
    catalog.executeBatch([{
      id: "invalid-call",
      name: definition.name,
      arguments: input.invalidInput,
    }], options),
    "tool_definition_nonconforming",
    "Invalid tool input crossed the schema boundary",
  );
  if (calls !== 1) nonconforming("tool_definition_nonconforming", "Invalid tool input reached the handler");
}

export async function assertOutputPublisherConforms(publisher: OutputPublisher): Promise<void> {
  const runId = `conformance-output-${uniqueId()}`;
  let settled = false;
  const waiting = publisher.waitForSequence(runId, 0).then(() => { settled = true; });
  await Promise.resolve();
  if (settled) nonconforming("output_publisher_nonconforming", "Output wait resolved before a newer event");
  await publisher.publishCommitted(outputEvent(runId, 1));
  await waiting;
  if (!settled) nonconforming("output_publisher_nonconforming", "Output wait ignored a newer event");
  await publisher.waitForSequence(runId, 0);

  const controller = new AbortController();
  const canceled = publisher.waitForSequence(`${runId}-cancel`, 0, controller.signal);
  controller.abort();
  await requireRejected(
    canceled,
    "output_publisher_nonconforming",
    "Canceled output wait did not reject",
  );
}

export async function assertDelegationRepositoryConforms(
  repository: DelegationRepository,
): Promise<void> {
  const suffix = uniqueId();
  const command = Object.freeze({
    runId: `conformance-run-${suffix}`,
    batchId: `conformance-batch-${suffix}`,
    idempotencyKey: `conformance-key-${suffix}`,
    delegations: Object.freeze([{
      agentName: "reader",
      title: "Conformance reader",
      instruction: "Inspect the fixture.",
      objective: "Return a content-free status.",
    }]),
  });
  const first = await repository.createBatch(command);
  const replay = await repository.createBatch(command);
  const row = first.delegations[0];
  if (row === undefined || first.replayed || !replay.replayed || replay.delegations[0]?.id !== row.id) {
    nonconforming("delegation_repository_nonconforming", "Delegation batch replay was not idempotent");
  }
  await requireRejected(
    repository.start(row.id, `${command.runId}-other`, command.batchId),
    "delegation_repository_nonconforming",
    "Delegation escaped its Root Run scope",
  );
  if ((await repository.start(row.id, command.runId, command.batchId))?.status !== "running") {
    nonconforming("delegation_repository_nonconforming", "Delegation did not enter running state");
  }
  if (!await repository.complete(row.id, command.runId, command.batchId, { status: "ok" })) {
    nonconforming("delegation_repository_nonconforming", "Delegation did not complete");
  }
  const aggregate = await repository.aggregateBatch(command.runId, command.batchId);
  if (aggregate.state !== "ready" || aggregate.counts.done !== 1) {
    nonconforming("delegation_repository_nonconforming", "Delegation aggregation was inconsistent");
  }
}

export async function assertLongTaskRepositoryConforms(repository: LongTaskRepository): Promise<void> {
  const suffix = uniqueId();
  const taskId = `conformance-task-${suffix}`;
  const command = Object.freeze({
    namespace: "conformance",
    kind: "probe",
    ownerId: "conformance-owner",
    createdByRunId: `conformance-run-${suffix}`,
    idempotencyKey: `conformance-key-${suffix}`,
    units: Object.freeze([{
      id: "unit-1",
      position: 0,
      executor: "probe",
      planStepId: "step-1",
    }]),
    deadlineAt: null,
    budgets: Object.freeze({
      maxModelAttempts: 1,
      maxTotalTokens: null,
      maxOutputBytes: 1_000,
      maxOutputEvents: 10,
    }),
  });
  const created = await repository.create(taskId, command);
  const replay = await repository.create(`${taskId}-replay`, command);
  if (created.id !== taskId || replay.id !== taskId) {
    nonconforming("long_task_repository_nonconforming", "Long Task create replay was not idempotent");
  }
  await repository.start(taskId);
  const claimed = await repository.claimReadyUnit(taskId, "worker-1", 60_000);
  if (claimed === undefined) nonconforming("long_task_repository_nonconforming", "Ready unit was not claimable");
  const claim = claimFromUnit(claimed);
  await repository.markUnitRunning(claim);
  const completed = await repository.completeUnit(claim, { outputRef: "result:1" }, "settlement-1");
  const replayed = await repository.completeUnit(claim, { outputRef: "result:1" }, "settlement-1");
  if (completed.status !== "completed" || replayed.outputRef !== "result:1") {
    nonconforming("long_task_repository_nonconforming", "Long Task settlement was not durable and idempotent");
  }
  await requireRejected(
    repository.heartbeat(claim, 60_000),
    "long_task_repository_nonconforming",
    "Settled Long Task claim remained authoritative",
  );
  if ((await repository.finalizeIfComplete(taskId)).status !== "completed") {
    nonconforming("long_task_repository_nonconforming", "Completed Long Task did not finalize");
  }
}

export async function assertArtifactRepositoryConforms(
  store: ArtifactRepository & ArtifactClaimRepository,
): Promise<void> {
  const suffix = uniqueId();
  const artifactId = `conformance-artifact-${suffix}`;
  const runId = `conformance-run-${suffix}`;
  const artifact = await store.create(artifactId, {
    namespace: "conformance",
    kind: "probe",
    ownerId: "conformance-owner",
    ownerRef: { kind: "probe", id: suffix },
    createdByRunId: runId,
    expectedItemCount: 1,
  });
  const claim = await store.acquire({
    artifactId,
    runId,
    expectedRevision: artifact.revision,
    leaseDurationMs: 60_000,
  });
  const operation = await prepareArtifactAppend({
    artifactId,
    expectedRevision: artifact.revision,
    sequence: 1,
    batchId: "batch-1",
    idempotencyKey: "append-1",
    items: [{ ordinal: 1 }],
    writeLease: { runId, claimToken: claim.claimToken, leaseDurationMs: 60_000 },
    coverageKeys: ["item-1"],
  });
  const receipt = await store.append(operation);
  const replay = await store.replayReceipt(operation);
  if (receipt.replayed || replay?.replayed !== true || replay.committedRevision !== 2) {
    nonconforming("artifact_repository_nonconforming", "Artifact append replay was not idempotent");
  }
  await requireRejected(
    store.append(await prepareArtifactAppend({
      ...operation,
      batchId: "batch-stale",
      idempotencyKey: "append-stale",
    })),
    "artifact_repository_nonconforming",
    "Stale Artifact revision was accepted",
  );
  const finalized = await store.finalize({
    artifactId,
    expectedRevision: 2,
    writeLease: { runId, claimToken: claim.claimToken, leaseDurationMs: 60_000 },
    expectedItemCount: 1,
    expectedCoverageKeys: ["item-1"],
    resourceRef: null,
  }, "coverage:1");
  if (finalized.status !== "finalized" || finalized.committedItemCount !== 1) {
    nonconforming("artifact_repository_nonconforming", "Artifact did not finalize consistently");
  }
}

function outputEvent(runId: string, sequence: number): OutputEvent {
  return Object.freeze({
    eventId: `event-${sequence}`,
    runId,
    sequence,
    occurredAt: new Date(0).toISOString(),
    sourceKey: `conformance:${sequence}`,
    kind: "run.started",
    channel: "lifecycle",
    visibility: "public",
    payload: Object.freeze({}),
  });
}

async function requireRejected(
  promise: Promise<unknown>,
  code: string,
  message: string,
): Promise<void> {
  try {
    await promise;
  } catch {
    return;
  }
  nonconforming(code, message);
}

function nonconforming(code: string, message: string): never {
  throw new AgentError(code, message);
}

function uniqueId(): string {
  return globalThis.crypto.randomUUID();
}
