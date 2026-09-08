# Host-driven recovery worker

Python exports `RecoveryWorker` from `purra.api`; TypeScript exports it from
`purra`. `run_once()` / `runOnce()` discovers persisted Runs, inspects each, skips
reported blockers, and serially awaits the host's resume callback. Duplicate IDs
are processed once per scan. Reopening SQLite and constructing a new worker needs
no in-memory queue reconstruction. SQLite discovery is scoped to its configured
scope and SDK (`list_running()` / `listRunning()`).

The host must restrict discovery to Runs for which it can reconstruct the original
request and current bindings. It must use public Core resume and await the handle's
outcome. Discovery is not authorization, and a diagnosis without blockers is not an
execution permit. The public entry rechecks ownership, scope, configuration,
approvals, effects and budgets. The worker never resets a lease, reconciles an
unknown effect, resumes a terminal Run, or creates a replacement Run.

```python
from purra.api import RecoveryWorker

async def resume(run_id):
    request, options = await host.resolve_run(run_id)
    handle = await core.resume(run_id, request, options=options)
    return await handle.wait()

worker = RecoveryWorker(
    discover=storage.list_running,
    inspect=storage.inspect_recovery,
    resume=resume,
)
report = await worker.run_once()
```

```ts
const worker = new RecoveryWorker({
  discover: () => storage.listRunning(),
  inspect: id => storage.inspectRecovery(id),
  resume: async id => {
    const request = await host.resolveRun(id);
    return (await agent.resume(id, request)).result;
  },
});
const report = await worker.runOnce();
```

A pending approval or unknown claim produces `blocked` without entering Core or
calling the model. Once the host commits its decision or reconciliation, another
scan reads the new state. Inspection/resume exceptions produce per-Run `failed`
results with fixed stage codes, allowing other Runs to proceed. Exception messages
and tool arguments are omitted. Run IDs are internal host diagnostics, not public
progress. `settled` means the callback returned; inspect the canonical Run for its
actual terminal status. Discovery failures reject the scan; overlapping scans on
one worker are rejected. Different workers still compete through Core's existing
lease authority. Python task cancellation propagates; the host owns cancellation
of active Runs and must not equate stopping a scheduler with canceling remote effects.

## Service lifecycle

`worker.run(stop=event, on_scan=observer)` in Python and
`worker.run({signal, onScan: observer})` in TypeScript scan immediately, then wait.
The polling interval defaults to 1000 ms. Scans containing `failed` results double
the wait up to 30000 ms; scans without failures reset it. Configure these with
`poll_interval_ms` / `pollIntervalMs` and `max_backoff_ms` / `maxBackoffMs` (positive
32-bit integer milliseconds, maximum at least the polling interval). This is a
scan-wide delay, not a persisted per-Run retry budget. A Python `settled` callback
may still represent a failed canonical Run; terminal Runs leave SQLite discovery.
Discovery and observer exceptions stop the service and propagate to the host.

Call `worker.wake()` on its event loop after committing an approval decision or
host reconciliation. Notifications coalesce; one received during scanning or in
the observer is retained for the next scan. A notification bypasses the wait,
including backoff, but never bypasses Core gates. Notifications are process-local
hints, not durable queue entries. Periodic rescans and the immediate startup scan
recover committed changes if a notification is lost or the process restarts.
Cross-process changes are discovered by polling unless the host forwards a hint.

Set the Python stop event or abort the TypeScript signal to stop scheduling. Idle
waits end promptly and release their timers/listeners. During discovery or inspection,
the worker waits for that callback and checks stop before starting a Run. During
resume, it drains the current callback and does not start the next Run. It does not
cancel the Run or remote tool. A hung host callback therefore prevents graceful
shutdown; host Run cancellation/deadline policy remains necessary. Python task
cancellation is distinct from graceful stop and propagates. Await `run` before
closing storage. Concurrent `run`/single-scan calls on the same worker are rejected;
a completed/stopped service can be started again with a fresh stop signal.

W01 remains incomplete: persisted wakeup subscriptions and per-Run retry policy,
bounded batches/fairness, lifecycle diagnostics and capacity measurements remain
open. SQLite discovery currently restores Core state and journal data; it is not a
cheap metadata-only queue query. Configure polling for the actual database size;
do not infer capacity from synthetic tests. Process supervision and business
Provider/MCP/downstream operation remain unvalidated.
