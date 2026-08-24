import { InMemoryArtifactStore } from "../artifacts/repository.js";
import { InMemoryDelegationRepository } from "../delegation/repository.js";
import { InMemoryLongTaskRepository } from "../durable/repository.js";
import { InMemoryOutputPublisher } from "../output/publisher.js";
import { InMemoryRunRepository } from "../run/store.js";

/** Dependency-free adapters for examples, tests, and single-process hosts. */
export class InMemoryAgentAdapters {
  public readonly runs = new InMemoryRunRepository();
  public readonly outputs = new InMemoryOutputPublisher();
  public readonly delegations = new InMemoryDelegationRepository();
  public readonly longTasks = new InMemoryLongTaskRepository();
  public readonly artifacts = new InMemoryArtifactStore();
}
