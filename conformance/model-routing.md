# R01: selection before Run creation

Status: pure candidate selection and opt-in preset-bound route persistence are
implemented in both SDKs. Automatic host binding resolution, policy identity,
installed-package acceptance and complete R01 acceptance remain open.

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

## Proposed additive contract

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
implementation must choose a backward-compatible optional canonical field or an
existing atomic extension boundary after verifying both storage codecs. This
storage choice is now implemented through the existing atomic preset snapshot.
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

Resume reads the persisted selection and resolves that exact binding. It never
invokes the selection policy again. Missing or changed bindings, capabilities or
effective configuration fail before Provider/tool dispatch. Existing checkpoint,
approval, lease, receipt, deadline and consumed-budget validation still applies.
An unavailable binding does not authorize fallback to another model. Unrouted
legacy Runs continue through the existing APIs without a synthetic route record.

## Required implementation and acceptance slices

1. Add dual-SDK selection contracts and capability filtering; verify unknown IDs,
   authorization, no eligible candidate and mutable-input isolation.
2. Integrate new-Run dispatch and atomic persisted selection; verify no Run or
   calls on rejection and isolation of concurrent selections.
3. Integrate checkpoint and SQLite restart recovery; verify selection policy is
   not called again and changed binding/configuration/capabilities are rejected.
   Preserve approval expiry, consumed budgets and unknown-effect blocking.
4. Check legacy snapshot import/resume, public exports, installed consumers and
   documentation. Record deterministic, installed, real-service and downstream
   evidence separately. A synthetic selection test is not real Provider evidence.

Run-internal switching belongs to R02. Child model selection and continuation
semantics require explicit follow-up analysis; this first Root routing contract
does not silently grant Child routing or model-switch permissions.
