# Managed memory workflow

`MemoryWorkflow` composes existing extraction, semantic review and guarded resolution
without introducing another journal. Python and TypeScript expose the same sequence:

1. Extract pending candidates with a stable operation key.
2. Review each currently pending candidate against active memory.
3. Ask an explicit host policy for a resolution. No policy or no decision leaves it pending.
4. Apply the decision through the existing source/version/review checks.
5. Retrieve active results through `MemoryContext` or `assemble_memory_context` /
   `assembleMemoryContext`, with the existing budget and evidence validation.

```python
from purra_mem0 import MemoryWorkflow

async def authorize(candidate, review):
    # The host must determine whether this source may change this category of memory.
    if not host_allows_memory_change(candidate):
        return None
    return review.proposal

workflow = MemoryWorkflow(memory, policy=authorize, policy_revision="preferences-v1")
# result = await workflow.capture(messages, source=source, key="turn-42")
```

```ts
import { MemoryWorkflow } from "purra-mem0";
const workflow = new MemoryWorkflow(memory, {
  policyRevision: "preferences-v1",
  policy: (candidate, review) => hostAllowsMemoryChange(candidate) ? review.proposal : undefined,
});
// const result = await workflow.capture(messages, { source, key: "turn-42" });
```

Use the same capture key, input and policy revision to resume. Extraction key reuse
validates the original input. Completed resolutions replay their journal receipts;
they do not re-extract or reactivate a revoked record. Unknown/running operations
remain unresolved and use the existing explicit reconciliation API. Host policy may
be called again when no resolution was persisted, so it must be free of side effects.
Change the policy revision when changing its meaning.

Model similarity is advice, not authorization. An empty search does not automatically
establish independence; `review.proposal` can be absent. The host may make an explicit
resolution referencing the candidate and review key. Conflicting/stale source or
record versions fail before activation. No real-provider memory-quality claim follows
from the deterministic workflow checks.
