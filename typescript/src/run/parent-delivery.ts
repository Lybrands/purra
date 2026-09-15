import type { JsonValue } from "../model/types.js";
import { AgentError } from "../shared/errors.js";
import type { RunRepository } from "./store.js";

/** Read the entire delivery journal before scheduling or publishing again. */
export async function completedParentDeliveries(repository: RunRepository, runId: string): Promise<ReadonlySet<string>> {
  const markers = new Map<string, Readonly<Record<string, JsonValue>>>();
  let cursor = 0;
  while (true) {
    const page = await repository.listEvents(runId, cursor);
    if (page.length === 0) break;
    for (const event of page) {
      if (event.sequence <= cursor) throw new AgentError("run_repository_nonconforming", "Output journal pagination did not advance");
      cursor = event.sequence;
      if (event.payload.schemaVersion === "purra.parent-delivery/v1") {
        markers.set(String(event.payload.deliveryId), event.payload);
      }
    }
  }
  const delivered = new Set<string>();
  for (const marker of markers.values()) {
    if (marker.state !== "completed") throw new AgentError("parent_delivery_reconciliation_required", "An earlier parent delivery needs reconciliation");
    for (const id of marker.childRunIds as readonly string[]) delivered.add(id);
  }
  return delivered;
}
