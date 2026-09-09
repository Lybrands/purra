# K01/K02 memory capture and relation queries

Status: K02 is complete inside the optional `purra-mem0` integration for scoped,
explicit relation queries and exact relationship evidence. K01 now has an
opt-in pre-capture authorization binding; eligibility policy persistence,
revocation and consumer acceptance remain open. Core does not acquire a
dependency on Mem0 or its journal.

## Current capture boundary

`add` writes host-selected text; `extract` invokes inference and produces pending
candidates. `MemoryWorkflow.capture` composes extraction, review and host-approved
resolution over stable operation keys. Its resolution policy still runs after
extraction and does not authorize Provider disclosure.

Hosts that require capture admission configure `capture_policy_id` and
`capture_policy_revision` (TypeScript: `capturePolicyId` and
`capturePolicyRevision`). Each call must then supply `MemoryCaptureAuthorization`.
`memory_capture_intent` / `memoryCaptureIntent` hashes the normalized messages,
source ID/revision, host metadata and expiry under the
`purra.mem0.capture-intent/v1` domain. The workflow checks that digest and the
configured policy identity before opening a journal operation or calling Mem0,
LLM or Embedding providers. Missing and mismatched grants fail closed.

The extraction fingerprint and persisted operation plan bind the intent digest,
policy identity, authorizing principal and host decision ID. An identical retry
replays the existing operation; changing any bound authorization field under the
same workflow key raises `memory_idempotency_conflict`. The control journal stores
these identifiers and the digest, not the source messages. The Mem0 SDK still
receives the source messages because extraction requires them.

This grant is an authorization receipt supplied by the host, not a policy engine,
identity proof or consent UI. PurrA validates its shape and binding; the host must
authenticate the principal, decide content eligibility, audit issuance and keep
decision IDs unique in its own authority boundary. A later refusal to activate a
candidate cannot undo an earlier Provider disclosure or write.

The opt-in configuration preserves existing 1.0 calls: workflows without a
capture policy keep their former behavior, while supplying a grant without a
configured policy is rejected to prevent silently ignored authorization data.
Reuse existing scope, source revisions, operation identity and unknown-effect
reconciliation. Do not automatically capture complete Run transcripts. Policy
decision persistence, time validity, revocation and revalidation before a first
unresolved dispatch remain K01 work.

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
