# Durable approval contract (1.1 development)

English | [简体中文](durable-approval.zh-CN.md)

This contract now has a Stage B storage foundation: immutable approval records,
host-authorized transactional decisions and explicit SQLite v5 activation.
Run suspension, execution recovery and MCP writes are **not yet available**. Existing
in-memory approvals remain supported. The implemented prerequisite revalidates
host scope after approval, before entering the tool idempotency gateway.
The shared `fixtures/approval_dispatch.json` cases test that prerequisite only.

## Authority and identity

An approval is a decision about one immutable operation intent. It is not a
permission token, a successful tool receipt, or permission to replay a model
round. Model messages, tool results, clarification answers and remote MCP
annotations cannot create or resolve approval authority.

The new opt-in durable approval capability must use the existing tool execution,
Run ownership, cancellation and budget path. Existing `ApprovalGateway` /
`ToolApprovalGateway` methods and terminal status values remain valid. A new
durable request never falls back to an in-memory decision when its store or
authorizer is unavailable. No required method is added to existing host ports.

The three record types below are available from Python `purra.approvals` and
the TypeScript `purra` entry point. Dispatch association remains unimplemented.
Python uses snake_case attributes and methods;
the persisted/interchange view uses the camelCase names below in both SDKs.

| Record | Required binding |
| --- | --- |
| `ApprovalIntent` | `schemaVersion`, `runId`, `rootRunId`, `toolCallId`, `toolName`, validated `arguments`, `presetFingerprint`, `bindingId`, `bindingRevision`, `scopeId`, `scopeRevision`, `effect` |
| `ApprovalRecord` | `approvalId`, immutable `intent`, `intentDigest`, `revision`, `status`, `createdAtMs`, `expiresAtMs`; terminal decision audit when present |
| `ApprovalDecisionCommand` | `approvalId`, `expectedRevision`, `intentDigest`, `commandKey`, `decision`; authenticated principal supplied separately by the host |
| Dispatch association | `approvalId`, `intentDigest`, Run/call identity, lease owner/epoch, existing tool claim/receipt identity |

Intent `schemaVersion` starts at 1. `effect` is the host declaration `write` or
`destructive`; it describes risk, not whether an effect occurred. The existing
tool receipt `effectState` remains `not_started`, `committed` or `unknown`.
No second effect taxonomy or implicit remote idempotency guarantee is introduced.

`intentDigest` uses `purra.json-identity/v1` on the complete intent, including
validated immutable arguments. Reordered object keys retain identity; changes
to any binding, tool/call, parameter, effect or configuration change identity.
A digest proves equality only. It does not authenticate the host or authorize
execution. Private arguments remain private, including in hashes exposed through
diagnostic reports. Public display text is an explicit host projection, never
the authority-bearing intent or model-provided approval instructions.

`bindingRevision` includes the bound MCP catalog digest for MCP calls. The host
versions tool implementation, resource scope and configuration changes that the
catalog cannot see. Restoring a matching snapshot does not establish current
permission. The host must check current resource boundaries at dispatch.

## Decision lifecycle

Durable approval status is separate from the legacy live gateway result union.
It has `pending`, `approved`, `rejected`, `expired` and `canceled` states.
Decision records are immutable after a terminal decision, except for an explicit
expiry/cancellation invalidation of an unused approval. Dispatch and effect
state are recorded separately; approval status does not imply execution.

| Current state | Command/observation | Result |
| --- | --- | --- |
| absent | Create under active Run lease with a committed tool-ready checkpoint | `pending`, revision 1 |
| `pending` | Host-authorized approve, matching revision/digest, before expiry | `approved`; increment revision and persist decision audit |
| `pending` | Host-authorized reject, matching revision/digest | `rejected`; increment revision and persist decision audit |
| `pending` or unused `approved` | Expiry reached | `expired`; no dispatch |
| `pending` or unused `approved` | Run/host cancellation | `canceled`; no dispatch |
| any | Exact replay of an accepted command key and decision | Return the persisted command result; never renew validity or repeat dispatch |
| any | Same command key with different content/principal, stale revision, wrong intent | Conflict; no mutation |
| terminal | Different decision | Conflict; no mutation |

The resolver obtains principal identity from the application's authenticated
context and calls a required host authorizer. Principal IDs are audit labels,
not sufficient authorization by themselves. Store transactions validate the
current revision, intent, Run and validity after asynchronous authorization;
concurrent decisions have one transactional winner. The resolver and raw store
must not be registered as model tools. Clarification answers remain task data.

Expiry uses an absolute persisted deadline and never restarts after reopening.
The effective deadline is bounded by the original Run/Root deadline. At exact
expiry (`now >= expiresAtMs`) dispatch is forbidden. Backward wall-clock changes
must not revive a persisted terminal expiry or accepted cancellation. A replayed
approval command can return its historical result after expiry; that result is
not a fresh dispatch permit.

## Suspension and recovery

The durable pause is **before any handler in the approval-bearing batch**. A
pending intent alone is insufficient: one transaction must save its bound
tool-ready continuation and canonical waiting event. Only after commit may the
runtime signal `ApprovalRequired` and release execution ownership. There is no
waiting worker, model request or held SQLite write transaction. Persistence
failure must neither announce a resumable wait nor dispatch a tool.

The initial opt-in durable approval path accepts one write call per batch.
Mixed or multiple-call durable approval batches fail admission before dispatch.
This preserves Python's existing write-batch restriction without removing
TypeScript's legacy live multi-approval behavior. Read-only batch concurrency
and host-managed recoverable Artifact batches retain their existing contracts.

`ApprovalRequired` is a distinct control signal, not `UserInputRequired`, a tool
failure result or an instruction for the model. The supervisor and Agent Tree
must preserve the nonterminal Run and release its lease on this signal. Waiting
continues to consume elapsed deadline time; it does not reset spent budgets.

The existing v2 `model_ready` checkpoint means a complete tool round. It cannot
be used for an outstanding assistant tool call, nor made recoverable by clearing
post-checkpoint attempt counts. The new opt-in continuation must preserve:

- The settled model invocation and complete provider-neutral continuation,
  including provider-specific message fields and the exact admitted tool calls.
- Current planning authority, execution/evidence state, model usage settlement,
  round position, retry ledger, original configuration, deadlines and budgets.
- Approval references, dispatch/receipt associations, and which calls have
  already completed. No fabricated tool messages or repeated model call.

Use a versioned `tool_ready` continuation with strict validation, not a widening
of the meaning of v2. Same-position checkpoint changes must be narrowly defined
transitions, checked by the canonical Run repository. A generic input revision
increment must not authorize replacing a pending tool call or its arguments.
The current 1.0 `is_input_checkpoint_update` rule remains a clarification/child
join rule, not an approval bypass.

Resume claims the existing Run lease and rechecks checkpoint/configuration
identity. The actual dispatch boundary then rechecks cancellation, Run/Root
deadline, remaining execution budget, current tool/argument/catalog identity,
scope, approval state and expiry, and absence of an unknown effect. Existing
planning and child capability grants still apply. Approving a child cannot
grant the parent or siblings permission to write.

## Dispatch and receipts

The durable gate must bind approval consumption to the existing tool claim in
one transaction under the current lease epoch. Two resumptions or repeated
approval commands cannot acquire two dispatch claims. Replaying a committed
receipt must verify the full intent association before returning it. TypeScript
opaque legacy idempotency keys alone cannot establish that association.

External execution runs outside the storage transaction. A conservative claim
is persisted before the external boundary; a crash after claiming is treated as
unknown even if the process may have stopped before sending. Normal successful
completion commits the existing tool receipt and approval association together.
No queued write enters the safe read-only parallel path.

| Observation | Receipt/effect treatment |
| --- | --- |
| Argument/scope/catalog/approval/lease/cancellation gate rejects before dispatch | `not_started`; no external call |
| Valid successful write response and durable receipt commit | `committed`; replay receipt, never dispatch again |
| Remote error response without a protocol guarantee of no effect | `unknown` |
| Timeout, cancellation, disconnect or response loss after dispatch | `unknown` |
| Invalid/oversized result, changed catalog or receipt persistence failure after dispatch | Retain claim; `unknown`, even if the remote side may have succeeded |
| Host reconciles authoritative completion evidence | Persist result using the existing reconciliation path and matching intent |
| Host proves no execution | Reconcile existing claim; require current approval and all dispatch checks before a new attempt |

`unknown` blocks automatic retry/replan that could repeat the effect. A late
response cannot overwrite a newer owner or a reconciled receipt. No API promises
end-to-end exactly-once without remote protocol support. Reconciliation is a
host operation; neither the model nor a diagnostic report supplies proof.

## MCP binding

Existing read bindings retain their defaults and snapshot behavior. The new
write binding requires an explicit host effect declaration, confirm policy, a
non-null scope validator and stable resource/configuration identity. It cannot
be enabled by `readOnlyHint`, `destructiveHint`, server descriptions, model
arguments or installing the package. Missing durable gate/receipt capability
rejects the write before RPC. A `propose` policy is not an alternative approval.

The adapter checks schema and scope, catalog revision/connection and cancellation
again immediately before RPC. Only a host-authorized write enters `tools/call`.
No write binding may declare `concurrencySafe`. A successful MCP response gives
completion evidence; transport/protocol/tool-result errors after RPC are unknown
unless an explicit supported remote contract proves otherwise. Read-only error
classification remains unchanged.

## Persistence and 1.0 compatibility

The existing public live approval, clarification, idempotency and read-only MCP
contracts remain supported. Opting out of durable approvals keeps ordinary v2
checkpoints and the 1.0 storage behavior. Python and TypeScript execution
snapshots remain SDK-specific; only the new JSON intent/decision semantics and
shared fixtures have cross-SDK identity.

The storage work must implement an explicit opt-in format fence before writing
new tool-ready records. A 1.0 process must reject that database before it can
claim/resume a Run or discard approval metadata. Do not merely put approval
records in an extension field that an older writer could silently lose.

The targeted adapter design is: retain v4 read/write support for legacy-only
databases; explicitly activate the approval-capable v5 format in a transaction,
preserving historical Runs, journals and committed receipts. Activation requires
no active Runs or unresolved tool claims anywhere in the database. It must
inspect all scopes/SDK rows, not just the currently bound scope. Hosts back up
and activate offline; the default constructor does not migrate v4 databases.
Previously created in-memory approvals cannot be imported as durable permission.
Existing unresolved effects are reconciled with the original version first.

The storage foundation implements this activation and a database marker plus
INSERT/UPDATE guards against preopened v4 writers. Because SDK snapshots have
different codecs, activation rejects any foreign-SDK row rather than assuming
it is inactive. All same-SDK scopes are inspected, including queued/waiting tree
Runs and unresolved claims. Tests use synthetic databases only; they cover
historical reads, rejection without row changes, and legacy version/write probes.
No business database migration is part of the PurrA development work.

## Diagnosis and acceptance

Extend the existing read-only inspection with normalized approval counts/states,
intent-match observations and fixed waiting/conflict/expiry/unknown-effect reason
codes. Unknown observations stay unknown. Reports omit identifiers, arguments,
principal identity, raw errors and approval digests; `authority` remains
`diagnosis_only`. Inspection does not expire rows, reconcile, claim or execute.

Deterministic acceptance must cover decision replay/conflict, authorization
denial, exact expiry, cancellation and permission changes, same-Run restart,
no repeated model round, lease competition, parameter/configuration/catalog
changes, rejection of unsupported approval batches before dispatch,
claim/receipt commit failure, late responses,
effect reconciliation, Agent Tree waiting and v4/v5 boundaries in both SDKs.
Passing intent/state tests alone does not establish atomicity or restart safety.

Installed wheels/npm tarballs require independent consumers outside the source
tree and inspection for private files. Controlled independent MCP servers use
synthetic resources and count actual writes across disconnect/restart faults.
Real Provider/MCP evidence names the protocol, specific capability and actual
service/model. Downstream acceptance is separate and remains unpassed until the
host project implements and validates its authorization, storage, UI and recovery.

## Storage foundation API

Python: `await storage.enable_approvals()` then
`storage.approval_store(authorize=callback, clock_ms=clock)`.
TypeScript: `await storage.enableApprovals()` then
`storage.approvalStore({authorize: callback, clockMs: clock})`.
The clock is optional and defaults to current epoch milliseconds. Activation is
a database-wide, explicit offline operation, not an application startup migration.

The store exposes `create`, `get`, `list_pending` / `listPending`, `decide` and
`refresh`. Creation requires an existing running Run, matching Root and persisted
preset fingerprint; expiry is capped by both Run deadlines. Exact creation replay
returns the original record without renewal. Binding/scope revisions are host
declarations here; live verification belongs to the future dispatch gate.

`decide(command, principal_id=...)` / `decide(command, {principalId})` requires
an authenticated host principal supplied independently of model/tool data.
`authorize(principal, record, command)` must return literal true; it runs outside
the database transaction. The transaction then rechecks identity, revision,
canonical Run cancellation, deadline and configuration. Competing decisions
cannot overwrite one another. An exact command-key replay returns its original
decision receipt, including after expiration; that receipt is historical evidence,
not a dispatch permit.

`get` and list operations are read-only persisted projections: they do not expire
records on read. `refresh` explicitly persists expiration or canonical Run
cancellation. `list_pending` / `listPending` includes pending and approved records
whose invalidation has not been persisted. Neither this list nor `approved`
proves current permission. These methods currently create no checkpoint, tool
claim, Run suspension or public approval event. Existing live approvals remain
the only runtime approval path until the remaining Stage B wiring is complete.
