# Read-only tool concurrency

Python `ToolExecutionLimits(max_concurrency=2)` and TypeScript
`Agent({ toolLimits: { maxConcurrency: 2 }, ... })` set a **per-batch** in-flight
limit. The default is 1. Registration `concurrency_safe` / `concurrencySafe`
defaults to false and requires both read mode and read risk. Parallel dispatch
requires more than one call and every selected tool to carry that declaration.
Other batches retain their existing serial/policy behavior, including Python's
restrictions on multi-call write batches.

Core validates all arguments and checks every scope in input order before any
parallel handler starts. It checks current scope again at dispatch, uses the
Run cancellation signal, and retains existing canonical operation/event lease
fences. An unbound executor cannot invent a Run lease or authorization proof;
the host must supply those boundaries when using persistence.

Claims are made in input order, up to the configured limit. The first observed
failure closes the dispatch gate. Already-running reads are joined and their
successful results retained; queued calls receive `tool_batch_aborted` with no
write effect. Completion events reflect actual completion order, while returned
results and accumulated context evidence use input order. A skipped call has
no handler execution. A started operation can fail authorization before network
I/O; event start is not proof of an external request.

Cancellation closes the gate and joins active work. Python returns a canceled
batch; TypeScript retains its `AgentCanceledError` convention. Host callbacks
must cooperate with cancellation and finish cleanup: parallel TypeScript waits
for the underlying handler/scope promise to settle, even if cancellation wins
the local race. No SDK can forcibly stop arbitrary host JavaScript or guarantee
remote MCP termination. Observer, canonical persistence and recognized lease
failures propagate; they are not converted into successful tool data. Fatal
failures cancel/drain siblings and prevent late Core event emission after return.

The declaration asserts that the entire host implementation is safe to overlap,
including shared state, clients, resource access and scope callbacks. Read-only
behavior alone does not prove that. The limit does not provide global Root,
process, data-source, tenant or cross-worker scheduling.

Preset-bound configuration identity includes tool argument-contract identity,
concurrency declarations and effective tool limits. Changing them changes the
configuration fingerprint; it does not silently rebind a checkpoint. Hosts
still version their scope and handler implementations through preset revisions.

The shared fixture and latch/barrier tests establish overlap, bounds, ordering,
queue shutdown, revocation, cancellation cleanup and failure propagation.
Official local MCP client/server tests also verify several requests on one
host-owned connection. They do not demonstrate live Provider speedup or reduce
model calls; normal Agent final presentation remains a separate Core phase.
See the runnable [Python example](../examples/python/parallel_tools.py) and
[TypeScript example](../typescript/examples/parallel-tools.ts).
