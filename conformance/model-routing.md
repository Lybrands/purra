# R01: selection before Run creation

Status: pure candidate selection and opt-in preset-bound route persistence are
implemented in both SDKs. Saved binding resolution and optional policy identity
are also implemented. Host factory registries now construct per-call hosts from
the selected/resolved route. The PurrA-local R01 contract is complete for Root
selection before Run creation. Real Provider and downstream acceptance remain
separate and are not claimed by these deterministic checks.

Python exports `ModelRouteCandidate` and `select_model_route` from `purra.api`;
TypeScript exports `ModelRouteCandidate`, `ModelRouteRequirements` and
`selectModelRoute` from `purra`. Supply the host registry candidates, authorized
binding IDs and task requirements. Selection follows candidate registration order,
not allowlist order, and returns the first compatible authorized candidate. Unknown
authorized IDs and duplicate registry IDs reject the call. No eligible candidate
raises `model_route_unavailable`. A verified generation ceiling is required.
Python reuses `TaskCapabilityRequirements` and capability preflight. TypeScript
checks the equivalent reasoning, tool, structured-output, streaming and cancellation
requirements. Selected capability data is immutable. Configuration identity is a
host-supplied nonsecret token; these pure helpers do not verify a live gateway or
persist any identity. Disabled tool requirements impose no tool authorization;
actual tool permissions remain owned by the existing runtime.

## Existing integration boundaries

Python `AgentRunRequest.model` carries the model request; `AgentCore` owns the
gateway. TypeScript `Agent` owns the model implementation and copies its capability
snapshot at construction. Selection must therefore resolve an entire host binding,
not merely replace a model name on an existing shared Agent.

Both public resume paths restore canonical checkpoints and compare the persisted
preset with the current composition. Python also checks generation intent and
selected context capacity. TypeScript's persisted preset includes a capability
profile ID. A profile ID alone is insufficient to identify all endpoint, model,
reasoning and capability changes. R01 must explicitly bind those choices without
changing the meaning of existing 1.0 snapshots.

## Additive contract

The host registers a finite set of named model bindings with revisions. Each
binding resolves a gateway/model implementation, its verified capability snapshot,
and effective generation/reasoning configuration. Credentials stay in the host;
only opaque binding identity and nonsecret configuration identity are persisted.

Selection runs before a new Root Run is created. It receives host-authorized
candidate IDs and task requirements. A policy chooses one eligible candidate;
registration order may serve as an explicitly documented deterministic default.
Unknown, unauthorized or incompatible selections fail before Run creation and
before any Provider or tool call. Candidate capability checks reuse the existing
task preflight semantics. Routing does not increase tool grants or runtime budgets.

Freeze the selected binding ID/revision, policy ID/revision, capability identity
and effective configuration identity for the Run. Input objects must be copied or
frozen so later mutation cannot change execution. Two concurrent submissions with
different selections must not mutate a shared Agent or gateway configuration.

Durable selection must be committed with the Run identity before dispatch. A
post-submit side record is insufficient: process exit between Run creation and
that write would leave a recoverable Run with no authoritative selection. The
implementation uses the existing atomic preset snapshot in both storage codecs.
Python accepts `AgentPreset.model_route`; TypeScript accepts `preset.modelRoute`.
The host passes the selected candidate when constructing its bound Agent. No
route field is emitted when this opt-in is absent, preserving legacy identities.
The complete capability snapshot and opaque binding/configuration identifiers
are saved with the Run. Python also fingerprints the actual model request's
provider, model, options and generation allowance; TypeScript binds the request
generation allowance into its composition fingerprint. TypeScript model/endpoint
options are opaque to Core and remain the host's `configIdentity` responsibility.
Never put credentials in that identity or in model request options.

At submission, declared route capabilities must match the configured capabilities.
At recovery, canonical preset comparison rejects missing or changed routing
identity. This does not itself resolve a binding from a registry: the host must
read the saved route and construct the matching Agent. No selector is invoked by
public resume. SQLite reopen tests cover missing route, changed revision/config,
request drift and successful approval-linked single dispatch with the same route.

## Resolving a saved binding

New selections may supply `policy_id` / `policy_revision` keyword arguments to
Python `select_model_route`, or `{id, revision}` as the fourth argument to
TypeScript `selectModelRoute`. Both identity fields must be supplied together.
They describe the host policy used for the registration-order choice, not an
executable policy callback. They are persisted within the selected route.

Python `resolve_model_route(candidates, saved, allowed_binding_ids)` and async
TypeScript `resolveModelRoute(candidates, saved, allowedBindingIds)` match the
saved binding against the current host registry and current authorization.
Read `saved` from the canonical Run's preset, never from user input. Python's
path is `snapshot.agent_preset_snapshot['composition']['modelRoute']`;
TypeScript's is `snapshot.preset.modelRoute`. An unrouted legacy Run continues
through the original API instead of supplying an invented route.

The resolver returns a detached/immutable candidate retaining the original
policy identity. Current registry order and current selection-policy identity
do not reroute a waiting Run. Missing/revoked bindings and changed binding
revision, configuration identity or capabilities raise `model_route_mismatch`.
Duplicate registry IDs reject resolution. A host intentionally invalidating an
old policy must revoke its binding authorization; merely changing the policy
used for new Runs does not revoke existing Runs.

Use the returned binding ID to retrieve the host-owned gateway/request factory,
construct its Agent with the returned route, then invoke public resume. These
helpers resolve identity, not opaque implementation objects or credentials.
The host must keep its actual factory configuration consistent with the declared
revision/configuration identity. Public resume still verifies the full canonical
preset and request, approval and execution ownership. Python's request identity
is checked there, rather than by the registry resolver. SQLite tests rebuild a
host after reopen using this resolver, reject directly substituting a new policy
revision, then finish the original approved write exactly once.

Resume reads the persisted selection and resolves that exact binding. It never
invokes the selection policy again. Missing or changed bindings, capabilities or
effective configuration fail before Provider/tool dispatch. Existing checkpoint,
approval, lease, receipt, deadline and consumed-budget validation still applies.
An unavailable binding does not authorize fallback to another model. Unrouted
legacy Runs continue through the existing APIs without a synthetic route record.

## Required implementation and acceptance slices

### Host factory registry

Python exports `ModelRouteBinding(candidate, create)` and `ModelRouteRegistry`.
TypeScript exports the equivalent binding interface and registry class. `create`
is an async factory accepting the immutable route and returning a host-owned
Agent or host wrapper. It must attach that route to the preset and bind the
corresponding gateway/request configuration. Factories are trusted host code;
the registry cannot detect a dishonest factory or a factory reusing mutable state.

`create_new` / `createNew` selects then invokes exactly one registered factory.
`create_recovery` / `createRecovery` resolves the saved canonical route then
invokes its factory, retaining the original policy identity. Registry construction
copies registrations and rejects duplicate binding IDs. Selection/resolution
failure calls no factory; a factory failure propagates without model fallback.
Concurrent calls have separate selected route values and no shared current-model
slot. The caller owns closing returned hosts, submit/resume options, credentials,
and resource cleanup if its factory fails partway through construction.

The SQLite route tests now use the registry to construct both the submitting and
reopened host. Deterministic concurrent-factory tests cover separate selections,
registration mutation, revoked authorization and failure without fallback. Current
isolated wheel and npm tarball checks cover selection/factories, SQLite approval
recovery and two overlapping routed Runs in both SDKs. Test SDK imports resolve
from temporary installed packages.

The concurrent Run checks use separate factory-created hosts sharing one temporary
SQLite database. A barrier keeps both model calls active until both have entered.
They verify distinct Run IDs, separate persisted binding identities and exactly
one model call per host without tools. Python observes the actual invocation model
name; TypeScript observes the selected host gateway's distinct output. This is
synthetic overlap/isolation evidence, not a real Provider test or a throughput
measurement. The recovery fixture separately verifies approval-linked dispatch.

### Completion decision and boundaries

Both SDKs implement candidate filtering, host authorization, optional policy
identity, factory construction, atomic preset identity and exact saved-route
resolution. Selection rejects malformed authorization lists: a string is never
interpreted as a list or matched by substring. Unknown structured-output levels,
including names inherited from JavaScript Object prototypes, cannot satisfy a
required structured-output capability.

Deterministic and installed-package tests cover registration mutation, concurrent
factory calls, actual overlapping Runs, SQLite reopen, binding/request changes,
original policy preservation and approval-linked single dispatch. Existing
unrouted approval/recovery tests exercise the unchanged entry points; omission
of the route adds no field to their preset identity. Broader Core and SQLite
regression checks passed after the boundary repairs. No routing-specific storage
migration or change to existing checkpoint, lease or receipt authority is needed.

Host factories remain responsible for applying the selected route and accurately
identifying opaque gateway configuration. The API provides deterministic ordered
selection, not an adaptive model-ranking service. Provider interoperability,
business authorization, downstream adoption and production capacity require
their own evidence. The local completion decision does not claim those checks
passed and does not close the remaining 1.0.0 expansion items.

Run-internal switching belongs to R02. Child model selection and continuation
semantics require explicit follow-up analysis; this first Root routing contract
does not silently grant Child routing or model-switch permissions.
