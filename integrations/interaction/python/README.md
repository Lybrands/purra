# purra-interaction · Python

English | [简体中文](README.zh-CN.md)

Persist structured questions, wait for user input, and continue execution.
Requires Python 3.11+.

| API | Use |
| --- | --- |
| `SqliteClarification` | Suspend and resume the same Run using `purra-sqlite` checkpoints |
| `ClarificationStore` + `ClarificationWorkflow` | Collect input before execution, then create a Run through an application callback |

## Install

For same-Run clarification, run from the repository root:

```sh
python -m pip install . ./integrations/sqlite/python './integrations/interaction/python[sqlite]'
```

## Configure a Run

Given a model `gateway`:

```python
from purra.api import AgentCore, AgentPreset
from purra.contracts import RuntimeLimits
from purra.tools import InMemoryToolCatalog
from purra_sqlite import SqliteAgentAdapters
from purra_interaction import SqliteClarification

storage = SqliteAgentAdapters("agent.db", scope="user-1/project-1")
interaction = SqliteClarification(storage)
core = AgentCore(
    model_gateway=gateway,
    preset=AgentPreset(
        id="assistant", revision="1",
        runtime_limits=RuntimeLimits(max_run_generation_tokens=8192),
        tool_catalog=InMemoryToolCatalog((interaction.registration,)),
    ),
    run_repository=storage.runs,
    output_repository=storage.outputs,
    output_publisher=storage.publisher,
    execution_lease_store=storage.leases,
)
```

Submit an `AgentRunRequest` with `tools_enabled=True` through
`await interaction.submit(core, request)`. When the Agent asks a question,
`handle.wait()` raises `purra.api.UserInputRequired`. Use its `request_id` with
`await interaction.get(request_id)` to obtain the public question data.

The Run remains waiting, with no worker or model call kept alive. After the user
answers, provide the question revision, a stable command key, and answers keyed
by question ID:

```python
async def answer_and_resume(request_id, revision, answer_key, answers):
    ready = await interaction.answer(
        request_id, revision=revision, key=answer_key, answers=answers,
    )
    return await interaction.resume(core, ready["id"])
```

The returned handle continues the same Run with its spent budget and deadline.
It may request input again. Answers provide task data and do not grant tool permissions.

## Reload and cancel

Recreate the same storage, Agent preset, gateway, and interaction objects after
restart. `list_pending()` includes waiting and answered requests; resume those
in `ready` state. Use `cancel(request_id)` while waiting, or the Run handle after
execution resumes.

For Agent trees, bind the same Run-tree repository and answer all pending questions
under a Root before resuming. Auto, Reactive, and Planned execution are supported.
Pauses occur at completed tool-round boundaries. Uncertain external writes require
reconciliation. Close Core before closing storage.

## Pre-execution input

`ClarificationStore.ask(...)` saves questions and an application checkpoint.
Expose `store.public(saved)` to the UI, then call `store.answer(...)`.
`ClarificationWorkflow.resume(..., submit=callback)` invokes
`callback(snapshot, continuation_key)` to create a new Run and return its ID.

Persist the continuation key on that Run. If submission is interrupted, reconcile
against the existing Run before retrying. The application restores credentials,
permissions, and remaining task budgets. Close the store on shutdown.
