# Unified Run output and Agent feedback

An Agent owns its context and decision loop. A Run is one execution of that
Agent. Ordinary parallel tools and Recipe operations stay in the same Run;
identity remains task ID, unit ID, attempt and lease epoch. `bind_run` / `bindRun`
accepts that owning Run. Hosts decide when a responsibility warrants a child
Agent and explicitly delegate it. A model call, a new context, or an operation
boundary alone never causes automatic Agent creation. A one-shot Agent uses
the same delegation mechanism as a continued Agent.

`delegateToAgents` returns available results early; `receiveAgentResults` receives
later results using `runIds` and `afterRunIds`. The repository remains the inbox
source of truth. Re-reading results does not execute Agents again.

By default results are received without an additional presentation model call.
A host can opt into managed public presentation by supplying
`AgentTreePolicy.result_presentation_instruction` (Python) or
`agentTree.policy.resultPresentationInstruction` (TypeScript). Omitted or null
means disabled; blank strings are invalid. There is no built-in presentation
prompt. When explicitly configured, every terminal direct child result,
including failure, schedules one public explanation from its owning Agent. Results enqueue on arrival;
explanations use one serial output lane. Child execution continues concurrently.
There is no collection time window. Receipt polling and automatic presentation
are separate: receiving a result does not prove its explanation has completed.
The root cannot finish until selected feedback settles. Ordinary parallel
operations do not create feedback presentations or new Agent identities.

The explanation is a managed streaming model invocation charged to the owning
Run, with no tool calls and nonempty text. Child results are untrusted input.
Hosts choosing managed feedback for explicit Recipe Agents configure the root
policy and wire `deliver_results` to the live root's `report_agent_results`
method. Calling that method without a policy fails with
`agent_feedback_policy_required` before journaling or dispatching a model call.
Standalone schedulers also accept host result callbacks without requiring
model presentation. Child private text and tool history are
never copied into the main conversation. This adds one root model invocation
per child result; normal deadlines and token budgets still apply.

`agent.feedback.queued` and `agent.feedback.state` provide public status metadata
without private child content. Streaming state links the root output stream and
invocation. `parent.stage.delivery` and TypeScript `purra.parent-delivery/v1`
remain durable delivery markers. An interrupted started/aborted delivery blocks
unsafe replay with `parent_delivery_reconciliation_required`. A failure to
present is not successful feedback. Historical records are not rewritten.

Legacy result-window parameters remain accepted for compatibility but do not
batch live feedback. A persisted non-null presentation instruction is an explicit host policy and
remains active on recovery; disabling presentation does not erase unresolved
historical delivery markers. Standalone
command services must close receivers; AgentCore owns its receiver lifecycle.
Tests cover single-result arrival, queue order, early reception, public output,
dependencies, cancellation, recovery and ordinary same-Run operations.
