import { prepareArtifactAppend } from "../artifacts/contracts.js";
import type { ArtifactClaimRepository, ArtifactRepository } from "../artifacts/types.js";
import { claimFromUnit } from "../durable/repository.js";
import type { LongTaskRepository } from "../durable/types.js";
import type { JsonValue, ModelGateway, ModelRequest } from "../model/types.js";
import { invokeModel } from "../model/stream.js";
import { copyCapabilitySnapshot, resolveInvocationOutputBudget } from "../model/validation.js";
import type { OutputEvent, OutputPublisher } from "../output/types.js";
import { AgentError } from "../shared/errors.js";
import { ToolCatalog } from "../tools/catalog.js";
import type { ToolContext, ToolDefinition } from "../tools/types.js";

export async function assertModelGatewayConforms(input: {
  readonly gateway: ModelGateway;
  readonly request?: ModelRequest;
}): Promise<void> {
  if (input.gateway.capabilities === undefined) {
    nonconforming("model_gateway_nonconforming", "Model capabilities are required");
  }
  const capabilities = copyCapabilitySnapshot(input.gateway.capabilities);
  if (capabilities.protocol.streaming === "supported" && typeof input.gateway.stream !== "function") {
    nonconforming("model_gateway_nonconforming", "Model capabilities claim an unavailable stream interface");
  }
  const request = input.request ?? Object.freeze({
    messages: Object.freeze([{ role: "user" as const, content: "conformance probe" }]),
    tools: Object.freeze([]),
    capabilitySnapshot: capabilities,
    outputBudget: resolveInvocationOutputBudget(capabilities),
  });
  const turn = await invokeModel(input.gateway, request, undefined, false);
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
    deadlineAtMs: null,
    budgets: Object.freeze({
      maxInvocationAttempts: 1,
      maxInputTokens: null,
      maxRunGenerationTokens: null,
      maxReasoningTokens: null,
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
    rootRunId: runId,
    agentId: runId,
    parentRunId: null,
    sequence,
    rootSequence: sequence,
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

const integrationCapabilities = ["gateway", "tools", "context", "storage", "structured_output", "mcp", "read_concurrency"] as const;
const evidenceCategories = ["deterministic", "installed_artifact", "real_provider_mcp", "downstream"] as const;
export type IntegrationCapability = typeof integrationCapabilities[number];
export type EvidenceCategory = typeof evidenceCategories[number];
export interface IntegrationCheck {
  readonly capability: IntegrationCapability;
  readonly category: EvidenceCategory;
  readonly probe: () => Promise<unknown>;
}

/** Wrap existing conformance assertions. Hosts own isolation and truthful category labels.
 * External probes require explicit category enablement; exceptions never enter the report.
 */
export async function checkIntegration(input: {
  readonly component: string;
  readonly version: string;
  readonly checks?: readonly IntegrationCheck[];
  readonly declaredCapabilities?: Partial<Record<IntegrationCapability, "supported" | "unsupported" | "unknown">>;
  readonly enabledCategories?: readonly EvidenceCategory[];
}) {
  for (const label of [input.component, input.version]) {
    if (typeof label !== "string" || !/^[A-Za-z0-9@_./:+-]{1,80}$/.test(label)) throw new TypeError("invalid public component label");
  }
  const enabled = [...(input.enabledCategories ?? ["deterministic"])];
  if (enabled.some(item => !evidenceCategories.includes(item))) throw new TypeError("invalid evidence category");
  const declared = { ...input.declaredCapabilities };
  if (Object.entries(declared).some(([key, value]) => !integrationCapabilities.includes(key as IntegrationCapability)
    || !["supported", "unsupported", "unknown"].includes(value))) throw new TypeError("invalid declared capability");
  const selected = new Map<string, () => Promise<unknown>>();
  for (const check of input.checks ?? []) {
    const key = `${check.capability}:${check.category}`;
    if (!integrationCapabilities.includes(check.capability) || !evidenceCategories.includes(check.category)
      || selected.has(key) || typeof check.probe !== "function") throw new TypeError("invalid integration check");
    selected.set(key, check.probe);
  }
  const checks: { capability: IntegrationCapability; category: EvidenceCategory; status: "not_run" | "passed" | "failed"; errorCode: string | null }[] = [];
  for (const category of evidenceCategories) for (const capability of integrationCapabilities) {
    const probe = selected.get(`${capability}:${category}`);
    let status: "not_run" | "passed" | "failed" = "not_run", errorCode: string | null = null;
    if (probe !== undefined && enabled.includes(category)) {
      try { await probe(); status = "passed"; }
      catch (error) {
        if (error instanceof Error && error.name === "AbortError") throw error;
        status = "failed"; errorCode = `${capability}_nonconforming`;
      }
    }
    checks.push(Object.freeze({ capability, category, status, errorCode }));
  }
  return Object.freeze({ schemaVersion: 1 as const, component: input.component, version: input.version,
    declaredCapabilities: Object.freeze(Object.fromEntries(integrationCapabilities.map(key => [key, declared[key] ?? "unknown"]))),
    checks: Object.freeze(checks) });
}
