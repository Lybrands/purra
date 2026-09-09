# K01/K02 memory capture and relation queries

Status: K02 is complete inside the optional `purra-mem0` integration for scoped,
explicit relation queries and exact relationship evidence. K01 remains open. Core
does not acquire a dependency on Mem0 or its journal.

## Current capture boundary

`add` writes host-selected text; `extract` invokes inference and produces pending
candidates. `MemoryWorkflow.capture` composes extraction, review and host-approved
resolution over stable operation keys. Its decision policy runs after extraction,
so it does not implement a pre-capture permission check. K01 must separately define
when content is eligible to leave the host, what content may be captured, and
whose authorization applies before extraction/Embedding. A later refusal to
activate a candidate cannot undo an earlier Provider disclosure or write.

Reuse existing scope, source revisions, operation identity and unknown-effect
reconciliation. Do not automatically capture complete Run transcripts. Capture
admission and retry/revocation semantics remain K01 work; no new capture gate is
claimed by this query-only slice.

## Bounded relation filtering

K02 first extends the existing `links` audit API additively. Python supports
`direction="both" | "incoming" | "outgoing"`, optional exact `relation`, and
`valid_only=False`; TypeScript uses `direction`, `relation`, and `validOnly`.
Defaults preserve existing audit results, including invalid/stale relations.
Directions are relative to the requested memory ID. Relation names are exact
host-defined strings, not semantic similarity, graph inference or executable
filters. Scope remains bound to the memory instance.

`limit` bounds inspected journal rows, not the number of matching results. Filters
are applied within that bounded page, and `next` advances using the scanned row
even if `items` is empty. Continue until `next` is absent/null. Keep filters fixed
for a traversal; start again when changing them. Cursor strings do not embed the
filters. Pages carry the existing journal epoch; restart a view when it changes.
Pages are observations, not a durable snapshot across multiple calls.

Validity uses the existing active/unexpired/nonwithdrawn endpoint reads and exact
record versions. Filtering never changes memory visibility or writes links.
Foreign scopes yield no owned links. Changing an endpoint version or revoking
its source removes that relation from `valid_only` results, while default audit
results retain it with `valid=False`.

`valid` describes current endpoint eligibility, not the truth of the relationship
or permission to insert it into a prompt. `MemoryLink` is not a context evidence
receipt.

## Relationship evidence and invalidation

Python `relation_evidence` / `validate_relation_evidence` and TypeScript
`relationEvidence` / `validateRelationEvidence` use `MemoryRelationEvidence`.
A receipt binds the integration store, immutable scope, link operation key, exact
endpoint IDs/versions, relation and note. Issuance rereads the complete link plan
and active endpoints and checks that the journal epoch did not change. A caller
cannot turn a stale or forged `MemoryLink` into evidence merely by setting
`valid=True`.

Validation rereads the exact operation and both endpoints. It fails with
`memory_relation_stale` after endpoint version changes, expiry, source withdrawal,
deletion, scope changes, link mismatch or store mismatch. It performs no model or
Embedding call and does not rewrite checkpoints. Validate all host-persisted
relation receipts immediately before reusing relationship-derived context or
resuming a checkpoint. Validation of several receipts is sequential and does not
create a cross-receipt database snapshot; an epoch check protects each receipt.

This relation receipt intentionally differs from `ContextEvidenceReceipt`, whose
single item/version shape cannot attest a link and two exact endpoints. It is
evidence that the explicit stored relation and endpoint versions are currently
eligible, not evidence that its semantic claim is true. A host that projects a
relationship into model context must retain this receipt beside that context and
define its own trusted presentation. Consumer acceptance remains open.
Model-assisted relationship extraction, graph/vector combination and memory
migration remain K03/K04/K05.

Deterministic and installed-package tests cover direction, relation filtering, invalid options, empty
filtered pages followed by matching pages, reopened journals, stale versions,
source withdrawal and foreign scopes. They use synthetic SDK fixtures and do
not establish real Embedding, semantic retrieval quality or downstream adoption.
