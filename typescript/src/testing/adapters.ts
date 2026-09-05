import { InMemoryArtifactStore } from "../artifacts/repository.js";
import { InMemoryLongTaskRepository } from "../durable/repository.js";
import { InMemoryOutputPublisher } from "../output/publisher.js";
import { InMemoryRunRepository } from "../run/store.js";
import { InMemoryRunTreeRepository } from "../agent-tree.js";

/** Dependency-free adapters for examples, tests, and single-process hosts. */
export class InMemoryAgentAdapters {
  public readonly runTree: InMemoryRunTreeRepository;
  public readonly runs: InMemoryRunRepository;
  public readonly outputs = new InMemoryOutputPublisher();
  public readonly longTasks = new InMemoryLongTaskRepository();
  public readonly artifacts = new InMemoryArtifactStore();

  public constructor(options: { readonly agentTreeClockMs?: () => number } = {}) {
    this.runTree = new InMemoryRunTreeRepository({
      ...(options.agentTreeClockMs === undefined
        ? {}
        : { clockMs: options.agentTreeClockMs }),
    });
    this.runs = new InMemoryRunRepository({
      leaseValidator: (runId, claim) => this.runTree.requireRunClaim(runId, claim),
    });
  }
}
