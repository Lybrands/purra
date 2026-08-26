# Cross-language conformance

The JSON fixtures in `fixtures/` define shared behavioral cases consumed by
the Python and TypeScript test suites. They align safety semantics, wire values,
and stable error outcomes without requiring identical public APIs or shared
runtime code.

`tool_security.json` covers Core-authoritative tool-schema admission and runtime
validation in both languages. The recursively enforced subset supports `type`,
`properties`, `required`, `additionalProperties`, `items`, `anyOf`, `oneOf`,
`enum`, `const`, `minLength`, `maxLength`, `minItems`, `maxItems`,
`uniqueItems`, `minimum`, and `maximum`; `title` and `description` are admitted
annotations. Unknown or malformed keywords fail Tool Catalog assembly.
`uniqueItems`, `enum`, and `const` share recursive JSON-value equality: numeric
representations compare by mathematical value, booleans remain distinct from
numbers, and object property order is irrelevant.
Cross-runtime numeric fixtures cover values represented identically by both
runtimes. TypeScript `JsonValue` remains a finite IEEE-754 number, so integer
literals beyond its safe range are not exact cross-language conformance
evidence.

`durable_protocol.json` freezes protocol version 4, Agent Preset snapshot
version 4, stable Runtime Safety error codes, budget edge cases, Provider delta
batch hashing, lease expiry, and orphan settlement outcomes for both runtimes.

`agent_tree_protocol.json` freezes recursive Agent identity, immutable Run
chains, bounded scheduling, continuation CAS, capability narrowing, stable
errors, Root-scoped budget dimensions, canonical journal attribution, and the
target Agent Preset snapshot v5 for both runtimes. It also freezes the
Reactive `model_ready` checkpoint v1 boundary: completed tool rounds may
resume, while in-flight Provider/tool work remains fail-stop.
