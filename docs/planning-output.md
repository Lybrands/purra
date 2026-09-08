# Planning output

Managed planning streams emit public `planning.delta` events for every nonempty
Provider content delta. Delivery does not wait for a newline, a valid JSON
record, or the final plan. The framework persists the source chunk and preview
event before publishing it. A blocked next chunk cannot delay the preceding
preview. This is a delivery contract, not a latency guarantee for a Provider
that buffers its own output.

Python subscribers use `handle.subscribe()`; TypeScript subscribers use
`handle.events()`. Select events with `kind == "planning.delta"` and append
`payload.textDelta` verbatim within the same invocation. Public payload fields:

| Field | Meaning |
| --- | --- |
| schemaVersion | `purra.planning-stream/v1` |
| operationId, revision, attempt | Bound planning operation and retry scope |
| sourceChunkIndex | Original framework chunk index; Python starts at 1, TypeScript at 0 |
| textDelta | Exact nonempty content fragment |

Run, stream and invocation identities are carried by the event envelope in
Python; TypeScript includes `invocationId` and `source: "provider"` in the
payload. Chunk indices may have gaps because reasoning-only, usage-only and
finish-only chunks do not produce previews. Use persisted event identity and
sequence for replay deduplication, not the text or contiguous indices.

The preview contains raw planning wire content, including JSON syntax, progress
records and plan records. Fragments may split an escape sequence or JSON token.
Invalid records and failed attempts remain visible as provisional previews.
This deliberately expands the previous public boundary: planning *content* is
previewable, even when final validation later rejects it. Reasoning deltas,
prompts and tool arguments are not projected by this event.

`planning.progress` remains the existing validated, complete progress-record
projection. It can overlap text already present in the raw preview; these are
two views of the same source. Formal plan validation, repair limits, execution
admission and cancellation remain unchanged. A preview never authorizes work.
Track invocation and attempt separately across repairs; do not concatenate a
failed attempt into its replacement.

Python output policies may optionally implement
`async authorize_planning_delta(spec, chunk)`: return the unchanged chunk to
allow a preview or `None` to suppress it. Absent this hook, previews are allowed.
TypeScript output policies can return `null` for a `planning.delta` event.
Rewriting preview content or provenance is rejected. Suppressing this view alone
does not suppress the separate `planning.progress` view.

Memory and SQLite repositories validate exact persisted source bytes, scope,
active invocation and operation, and source-key uniqueness. Each content chunk
requires immediate persistence, increasing event and storage overhead compared
with batching. Preview events consume existing output event/byte budgets; they
do not add model token usage. The existing first-public-progress timing metric
continues to describe complete progress records.


These events belong to the managed Run/output pipeline. Standalone Planner
methods still return a complete validated plan. Custom repositories/processors
must honor the same projection contract. Deterministic gated-chunk tests verify
delivery and replay; they do not establish live Provider or downstream behavior.
