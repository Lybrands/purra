# Host-owned retrieval composition

`MemoryContext` accepts exactly one selection mode:

- Existing `query` callback: the component retrieves memory using that query and
  `limit`, then assembles its results.
- New async `select_ids(request, signal)` / `selectIds(request, signal)` callback:
  the host returns ordered memory IDs. No implicit search, ranking, fallback or
  additional model call takes place.

The host can combine semantic search, explicit selection and existing relation
queries in the callback. It decides which sources to query, how to compare scores,
which relations are relevant and how to order candidates. The library supplies no
universal score normalization or graph-ranking policy.

```python
from purra_mem0 import MemoryContext
from purra.retrieval import RetrievalRequest

async def select_ids(request, signal):
    hits = await memory.retrieve(RetrievalRequest(query=host_query(request), limit=8), signal)
    if not hits:
        return []
    links = await memory.links(hits[0].id, direction="outgoing", valid_only=True, signal=signal)
    # This host chooses semantic matches first, then linked records.
    return list(dict.fromkeys([hit.id for hit in hits] + [link.to_ref.id for link in links.items]))

context = MemoryContext(memory=memory, select_ids=select_ids)
```

```typescript
import { MemoryContext } from "purra-mem0";

const context = new MemoryContext({ memory, async selectIds(request, signal) {
  const hits = await memory.retrieve({ query: hostQuery(request), limit: 8, scope: {} }, signal);
  if (!hits.length) return [];
  const links = await memory.links(hits[0]!.id, {
    direction: "outgoing", validOnly: true, ...(signal ? { signal } : {}),
  });
  return [...new Set([...hits.map(hit => hit.id), ...links.items.map(link => link.to.id)])];
} });
```

These are example host policies, not defaults. Cap the callback's output to the
configured memory constructor `max_results` / `maxResults`; `MemoryContext`'s
`limit` applies only to its built-in query mode. The host owns callback timeouts,
Provider budgets and cancellation handling. A zero context allocation bypasses
selection entirely. Errors propagate without automatic retry or fallback.

Both modes use the same assembly path. It snapshots the ordered IDs, reads fresh
records from the configured scoped memory, keeps first occurrences, omits missing
or inactive records, and revalidates source evidence. It preserves only complete
records that fit the token allocation, skipping oversized records rather than
truncating their text. A changed memory epoch rejects the assembly. The callback
cannot inject replacement text, arbitrary evidence receipts, or trusted context.
The output remains untrusted and carries receipts for the records actually used.

This composes records from one scoped memory instance. It does not add a graph
backend, cross-store joins, semantic relevance guarantees or automatic use of K03
relation proposals. Only existing valid links participate in the example. Hosts
must version this callback as part of their existing context-provider composition
binding when using durable Runs; the component cannot fingerprint closure logic.

The public `assemble_memory_context` / `assembleMemoryContext` helper remains
available for non-Agent consumers that need included, deferred and missing IDs.
Deterministic integration tests cover custom ordering, graph/search composition,
deduplication, evidence, cancellation, stale data and failure without fallback;
real Provider retrieval quality is a separate acceptance boundary.

Context allowances must be integers from 0 through 1,000,000. Validation happens
before either selection callback or built-in retrieval; invalid values cannot
trigger host work first. A missing allocation means zero. TypeScript requires an
own allocation property and rejects explicitly invalid values such as `null`,
`NaN` and strings. The assembly helper also rejects a non-callable token counter
before reading storage, including when the selected ID list is empty.
