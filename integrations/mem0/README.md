# PurrA Mem0 integration

English | [简体中文](README.zh-CN.md)

`purra-mem0` adds scoped long-term memory to PurrA using the Mem0 OSS SDK.
Mem0 stores text and vectors; the adapter manages ownership, state, versions,
operation receipts, and context evidence. Applications explicitly choose what to save.

## Start

- [Python installation and usage](python/README.md)
- [TypeScript installation and usage](typescript/README.md)

Use the OSS `Memory` client. Hosted Mem0 Platform clients are not supported.
The component requires its own persistent SQLite control journal alongside the
configured Mem0 store.

## Memory lifecycle

1. `add` stores accepted text directly. It calls Embedding but does not extract new facts.
2. `extract` uses a model to create `pending` candidates and requires inference to be enabled.
3. `review` returns advice about a pending candidate; application policy decides whether to apply it.
4. `resolve` applies an accepted decision using exact record versions.
5. Retrieval and context assembly return only active, unexpired, non-withdrawn records.

`MemoryWorkflow` combines extraction, review, and policy-based resolution.
Without a policy, candidates stay pending. A policy may also leave individual
candidates pending. Model similarity alone does not authorize activation.

| Resolution | Result |
| --- | --- |
| `independent` | Activate one candidate |
| `duplicate` | Keep one accepted record and disable the copies |
| `supersede` | Keep the accepted replacement and disable older records |
| `conflict` | Disable the group until it is resolved |

For workflow retries, keep capture key, input and policy revision unchanged.
Completed resolutions replay their receipts; they do not re-extract or reactivate
withdrawn records. Running/unknown operations require inspection or reconciliation.
Policy may run again before a resolution is persisted, so it must be free of
side effects. Change its revision when changing its meaning. An empty search
does not prove independence, and `review.proposal` may be absent. Stale source
or record versions fail before activation. SDK examples are in the usage guides.

## Read and manage

| API | Purpose |
| --- | --- |
| `get`, `list`, `history` | Read records, administrative pages, and history |
| `update`, `annotate` | Replace text or metadata using an expected version |
| `set_state` / `setState`, `delete` | Change visibility or remove live content |
| `retrieve`, `select` | Semantic search or explicit selection by ID |
| `MemoryContext`, `assemble_memory_context` / `assembleMemoryContext` | Assemble whole records within a token allowance |
| `link`, `links` | Record and inspect explicit relations between record versions |
| `revoke_source` / `revokeSource` | Withdraw a source or one of its revisions |
| `validate_evidence` / `validateEvidence` | Check whether saved memory evidence remains usable |

`list` returns `items`, `next`, and `epoch`. Continue with `next` even when a page
is empty; `null` marks the end. Restart the view if its epoch changes. Its `query`
filter is literal text matching; use `retrieve` for semantic search. Metadata
values are JSON scalars, and metadata edits do not call Embedding.

## Scope and context

Each instance binds an authenticated user, project, and optional Agent identity.
Choose that scope in the application and read through the adapter. When using
`RetrieverTool`, omit its `scope` option because the memory instance is already bound.

Context assembly includes whole records within the allocated tokens and attaches
evidence receipts. Before reusing saved memory-dependent context or checkpoints,
validate those receipts. Core does not automatically revalidate an already-resolved
checkpoint. Rebuild affected context when evidence is stale.

Withdrawing a source revision hides its records. Omitting the revision withdraws
all current and future revisions of that source ID. Deletion and withdrawal do
not erase SDK history, prior prompts, checkpoints, or backups; retention belongs
to the application.

## Model calls and budgets

Raw SDK mode uses the SDK's configured providers and reports internal usage as
unknown. Managed mode uses application callbacks with persistent call/input/output
budgets. `run_model` / `runModel` connects LLM work to the actual Run's task runner;
Embedding usage is accounted for separately.

Extraction and semantic review require model calls; adding, updating, and searching
text may call Embedding. Local storage does not imply local inference. Configure
providers, credentials, and monetary quotas in the application. Keep budget keys
stable across recovery; admitted reservations remain charged after failures.

## Persistence and recovery

Use one persistent journal per Mem0 store. Route owned-record writes through the
adapter and back up the store and journal together. Their writes are not one
atomic transaction. Python and TypeScript SDK stores are not interchangeable.

Keep operation keys stable on retries. After a timeout, inspect `operation(key)`:
`running` may still finish; `unknown` requires reconciliation. Stop all old writers
before `reconcile`. An incomplete extraction can be removed with
`discard_extraction` / `discardExtraction` after its writer has stopped.

Call `drain()` before `close()`. Closing the adapter closes only its journal;
the application owns SDK, provider, and storage resources.
