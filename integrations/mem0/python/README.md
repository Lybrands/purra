# purra-mem0 · Python

English | [简体中文](README.zh-CN.md)

Scoped memory, retrieval, and lifecycle management using the synchronous Mem0 OSS
`Memory` client. Requires Python 3.11+. SDK calls run in a worker thread.

## Install

From the repository root:

```sh
python -m pip install . ./integrations/mem0/python
```

For managed LLM/Embedding callbacks, install the extra:

```sh
python -m pip install . './integrations/mem0/python[managed]'
```

Before importing Mem0, set `MEM0_TELEMETRY=false` and an application-owned
`MEM0_DIR`. Explicitly configure the LLM, embedder, `vector_store`, and
`history_db_path`. Hosted `MemoryClient` and `AsyncMemory` are not supported.

## Save and read

The application supplies `mem0_config`, authenticated `user_id`, authorized
`project_id`, and a persistent `journal_path`:

```python
from mem0 import Memory
from purra_mem0 import Mem0Memory, MemoryScope, MemorySource

sdk = Memory.from_config(mem0_config)
memory = Mem0Memory(
    client=sdk,
    scope=MemoryScope(user=user_id, project=project_id),
    journal_path=journal_path,
)

async def save_preference():
    saved = await memory.add(
        "Reply in Chinese.",
        source=MemorySource("preference:language", "1"),
        key="preference:language:1",
    )
    return await memory.get(saved.ids[0])
```

`add` creates an active record by default; use `state="pending"` for review first.
`get` returns `None` for unavailable records. Updates, state changes, and deletions
require the current `version` and a stable operation `key`.

## Connect to an Agent

```python
from purra.retrieval import RetrieverTool
from purra_mem0 import MemoryContext

recall = RetrieverTool(
    retriever=memory,
    name="recallMemory",
    description="Recall saved preferences and facts.",
)
context = MemoryContext(
    memory=memory,
    query=lambda request: request.latest_user_text(),
)
```

Register `recall.registration` in the Agent's tool catalog, or compose `context`
with its context providers. The memory instance already binds the scope.
For explicit record selection, use `assemble_memory_context(memory, ids, allowance)`.

## Managed model calls

Use the following configuration instead of a raw SDK client when calls need
persistent budgets. Storage paths, embedding dimensions, and the budget key come
from the application; the numeric limits below illustrate a budget configuration.

`model_tasks` is the runner supplied by a PurrA extension factory, and
`model_request` selects its model. The async `embed(texts, signal)` callback must
return `EmbeddingResult` with vectors and any reported input-token usage.

```python
from purra_mem0 import (
    MemoryBudget, MemoryProviders, create_managed_client, run_model,
)

client = create_managed_client(
    embedding_dims=embedding_dimensions,
    config={"vector_store": vector_store_config, "history_db_path": history_path},
)
providers = MemoryProviders(
    budget=MemoryBudget(
        key=budget_key,
        max_llm_calls=4,
        max_embedding_calls=64,
        max_input_chars=100_000,
        max_output_tokens=8192,
        result_capacity_target_tokens=2048,
    ),
    complete=run_model(model_tasks, model_request),
    embed=embed,
)
memory = Mem0Memory(
    client=client,
    providers=providers,
    scope=MemoryScope(user=user_id, project=project_id),
    journal_path=journal_path,
    allow_inference=True,
)
```

Use `memory.budget_usage()` to inspect admitted and reported usage. Raw SDK mode
reports internal usage as unknown.

## Extract and review

With managed providers and `allow_inference=True`:

```python
from purra_mem0 import MemoryWorkflow

workflow = MemoryWorkflow(memory)

async def capture(messages, source, operation_key):
    return await workflow.capture(messages, source=source, key=operation_key)
```

This configuration reviews candidates and leaves them pending. To apply decisions,
pass an async `policy(candidate, review)` and a stable `policy_revision` to
`MemoryWorkflow`. Return an authorized `MemoryResolution` that retains the review
key, or `None` to leave the candidate pending.

## Lifecycle

Before shutdown, call `await memory.drain()` and `memory.close()`, then close SDK
and provider resources. For paging, withdrawal, evidence validation, and uncertain
writes, see the [memory lifecycle guide](../README.md).
