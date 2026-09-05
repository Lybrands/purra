# purra-interaction · TypeScript

English | [简体中文](README.zh-CN.md)

Persist structured questions, wait for user input, and continue execution.
Requires Node.js 22.13+.

| API | Use |
| --- | --- |
| `SqliteClarification` | Suspend and resume the same Run using `purra-sqlite` checkpoints |
| `ClarificationStore` + `ClarificationWorkflow` | Collect input before execution, then create a Run through an application callback |

## Install

Follow [source installation](../../README.md#source-installation). Build and
install `purra-sqlite` before `integrations/interaction/typescript` when using
`SqliteClarification`.

## Configure a Run

Given a model gateway `model`:

```ts
import { Agent } from "purra";
import { SqliteAgentAdapters } from "purra-sqlite";
import { SqliteClarification } from "purra-interaction";

const storage = new SqliteAgentAdapters("agent.db", { scope: "user-1/project-1" });
const interaction = new SqliteClarification(storage);
const agent = new Agent({
  model,
  preset: { id: "assistant", revision: "1" },
  tools: [interaction.tool],
  checkpointHandler: interaction.checkpointHandler,
  runRepository: storage.runs,
  outputPublisher: storage.publisher,
});
```

Submit through `agent.submit(request, { budgets: { maxRunGenerationTokens: 8192 } })`.
When the Agent asks a question, the handle's `result` rejects with
`UserInputRequired` from `purra`. Use `error.requestId` with
`await interaction.get(error.requestId)` to obtain public question data.

The Run remains waiting without keeping a worker or model call alive. After the
user answers, provide the question revision, a stable command key, and answers
keyed by question ID:

```ts
async function answerAndResume(
  requestId: string,
  revision: number,
  answerKey: string,
  answers: Record<string, string>,
) {
  const ready = await interaction.answer(requestId, {
    revision, key: answerKey, answers,
  });
  return interaction.resume(agent, ready.id);
}
```

The returned handle continues the same Run with its spent budget and deadline.
It may request input again. Answers provide task data and do not grant tool permissions.

## Reload and cancel

Recreate the same storage, Agent preset, gateway, and interaction objects after
restart. `listPending()` includes waiting and answered requests; resume those
in `ready` state. Use `cancel(requestId)` while waiting, or the Run handle after
execution resumes.

For Agent trees, bind the same Run-tree repository and answer all pending questions
under a Root before resuming. Auto, Reactive, and Planned execution are supported.
Pauses occur at completed tool-round boundaries. Reconcile uncertain external
writes and wait for active execution to settle before closing storage.

## Pre-execution input

`ClarificationStore.ask(...)` saves questions and an application checkpoint.
Expose `ClarificationStore.publicView(saved)` to the UI, then call `store.answer(...)`.
`ClarificationWorkflow.resume(id, { revision, submit })` invokes
`submit(snapshot, continuationKey)` to create a new Run and return its ID.

Persist the continuation key on that Run. If submission is interrupted, reconcile
against the existing Run before retrying. The application restores credentials,
permissions, and remaining task budgets. Close the store on shutdown.
