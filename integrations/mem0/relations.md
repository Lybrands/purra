# Host-directed relation proposals

`propose_memory_relations` (Python) and `proposeMemoryRelations` (TypeScript)
provide optional, read-only relation extraction over host-selected memory
revisions. They do not run during an Agent conversation automatically. No Core
runtime or storage schema changes are required.

The host supplies 2–32 distinct `MemoryRef` values, 1–64 allowed relation names,
and an async extractor. The helper reads active, authorized records from the
supplied `Mem0Memory`, checks their versions, and gives the extractor only their
IDs, versions and text, plus the allowed relations and output count limit. It
omits record metadata and source identifiers from the extractor payload.

The extractor may call a host-configured model using its own prompt and structured
output contract. It receives `(input, signal)` and returns an array of objects
with exactly these five string fields:

```json
[{"from":"memory-a","to":"memory-b","relation":"maintains",
  "fromQuote":"Alice maintains PURRA","toQuote":"PURRA is a framework"}]
```

The IDs must refer to different supplied records, the relation must be in the
host's vocabulary, and each nonempty quote must occur verbatim in its respective
record. Duplicate directed relations, unknown fields, fabricated references and
oversized results reject the whole batch. An empty array is valid. Source text is
untrusted data, not model instructions; the host's extraction prompt should keep
that distinction explicit.

Python composition:

```python
from purra_mem0 import MemoryRef, propose_memory_relations

# host_extract is an async function accepting (input, signal).
proposals = await propose_memory_relations(
    memory, [MemoryRef(first_id, first_version), MemoryRef(second_id, second_version)],
    relations=["maintains"], extract=host_extract,
    max_input_chars=32_000, max_proposals=8,
)
```

TypeScript composition:

```typescript
import { proposeMemoryRelations, type MemoryRelationExtractor } from "purra-mem0";

// hostExtract implements MemoryRelationExtractor.
const proposals = await proposeMemoryRelations(memory, [firstRef, secondRef], {
  relations: ["maintains"], extract: hostExtract,
  maxInputChars: 32_000, maxProposals: 8,
});
```

Returned proposals are immutable and include both exact memory revisions and
source IDs/revisions. Source visibility, record state and revisions are checked
again after extraction; a changed journal epoch rejects the batch conservatively,
even when the change affects an unrelated record. Cancellation is checked before
and after the callback and passed to it. The callback must honor cancellation
and own its timeout, Provider authorization and token/cost budgets. Character and
proposal limits are not token budgets. There is no hidden retry or fallback.

Quotes prove source membership, not that a relation is true. Proposals are neither
persisted links nor `MemoryRelationEvidence` receipts and confer no write or
retrieval authority. The host decides whether to accept a proposal, preserves it
if needed, and explicitly calls the existing `memory.link` API on the same scoped
memory instance with a host-owned operation key. That write rechecks current
endpoint versions and source authorization. Do not transplant proposal IDs to
another scope or treat proposals as canonical recovery records. This helper
provides no crash recovery for a model callback; a retry may incur another call.

This slice supports relations between existing records. New entity creation,
relation truth evaluation, automatic acceptance, built-in extraction prompts,
Provider adapters, and automatic linking are outside this API. Deterministic
Python/TypeScript integration tests cover source checks, stale/revoked input,
cancellation and no automatic writes. Real-model extraction quality remains
unverified.
