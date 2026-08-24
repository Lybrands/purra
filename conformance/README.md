# Cross-language conformance

The JSON fixtures in `fixtures/` define shared behavioral cases consumed by
the Python and TypeScript test suites. They align safety semantics, wire values,
and stable error outcomes without requiring identical public APIs or shared
runtime code.

`durable_protocol.json` freezes protocol version 3, Agent Preset snapshot
version 3, stable Runtime Safety error codes, budget edge cases, Provider delta
batch hashing, lease expiry, and orphan settlement outcomes for both runtimes.
