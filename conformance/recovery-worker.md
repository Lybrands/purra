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

W01 remains incomplete: broader wakeup subscriptions and maximum retry policy,
bounded batches/fairness, lifecycle diagnostics and capacity measurements remain
open. SQLite discovery currently restores Core state and journal data; it is not a
cheap metadata-only queue query. Configure polling for the actual database size;
do not infer capacity from synthetic tests. Process supervision and business
Provider/MCP/downstream operation remain unvalidated.

## Persistent per-Run schedule

Create `schedule = storage.recovery_schedule(...)` in Python or
`const schedule = storage.recoverySchedule(...)` in TypeScript, and pass it as the
worker's optional `schedule` constructor argument. Existing callers are unchanged.
The configured interval and maximum use the same names/defaults as the service
loop; `clock_ms` / `clockMs` is injectable for deterministic testing. Use a stable
host wall clock in operation. Clock jumps can change how long a retry waits.

This adapter stores scheduling hints in the scoped, SDK-specific SQLite state
extension. Each Run has a revision, consecutive failure count, and `notBeforeMs`.
It adds no new lease or execution permit. `ready` returns a revision or no token
when not due; the worker reports `blocked` with `retry_not_due` in the latter case.
After inspection/resume, `settle` conditionally advances that revision and writes
the next eligible time. Failure delays double up to the configured cap; a blocked
or settled callback resets failures and waits one interval. Read/write failures in
the scheduling adapter propagate and stop the scan, rather than silently disabling
persistence. The service's scan-wide delay remains independent; polling cadence can
make the actual retry later than its eligible time. These are scheduling delays,
not a retry count limit or proof that a failed operation is safe to repeat.

Register a Run with `await schedule.wake(runId)` before relying on atomic wake
notifications (a settled worker scan also creates its schedule entry). For registered
Runs, approval decisions and tool reconciliation now advance the wake revision in
their own SQLite transaction. Call `worker.wake()` after the operation returns for
a prompt local hint. Explicit `schedule.wake` remains available for other host changes. An old scan cannot overwrite a newer wake: its conditional
settlement returns false. Competing scanners still acquire execution ownership
through public resume. `ready` does not reserve a Run. A restart reads the same
persisted schedule; no process-local failure counter needs reconstruction.

For registered Runs, a successful new approval decision (approve or reject) and a
successful tool reconciliation commit the schedule wake atomically with their
canonical changes. A persistence failure rolls both back. Approval command replay
returns its original receipt without advancing the schedule, so repeated requests
do not clear subsequent backoff. Unregistered Runs gain no schedule entries from
these operations. Terminal Run status is unchanged by a reconciliation wake.
Refresh-only expiry/cancellation, other interactions and external host mutations
are not subscribed; periodic rescans remain necessary. This is scoped durable
notification state, not a general event bus or execution permission.

`storage.prune_recovery_schedule(run_ids)` / `storage.pruneRecoverySchedule(runIds)`
removes entries only for supplied canonical terminal Runs, in one transaction;
running Runs are skipped and missing/corrupt Runs reject the transaction. It returns
the removed IDs. Supply a bounded host-selected batch. Run history, approvals,
claims, receipts and execution leases are untouched. A scoped monotonic revision
counter survives pruning, fencing pre-prune scan tokens even before re-registration
or after reopen. Revisions are opaque comparison tokens, not per-Run counts.
Registration/settlement/pruning retain that counter; do not manually delete it.

Pruning is explicit. Bounded fair metadata discovery, maximum retry policy,
additional subscriptions and lifecycle diagnostics remain W01 work. Do not clear
execution claims to force a retry. SQLite scopes still bound the amount of state
restored per scheduling transaction; these changes do not establish capacity limits.

## Bounded scans and local diagnostics

Set `max_runs_per_scan=N` / `maxRunsPerScan: N` on the worker constructor to visit at
most N distinct candidates per scan (positive 32-bit integer). Omission preserves
the previous unlimited scan behavior. A visit includes checking persistent retry
time, so waiting approvals and `retry_not_due` entries consume a slot and move to
the back of the rotation. They cannot permanently occupy the first batch. Survivors
retain their order across discovery reordering; vanished IDs are removed and newly
seen IDs join the tail. Duplicates are collapsed. A scheduling failure or stop after
a visit advances that candidate, so restarting the same worker loop does not keep
selecting a failing first candidate. A stopped-before-visit candidate retains its
place. The service still waits between batches using its configured polling policy.

This bounds candidate processing and report size, not database discovery or memory:
`discover` still returns the full list, and the worker keeps an O(N) process-local
rotation. SQLite's full state/journal discovery costs are unchanged. Rotation is
not persisted; a new worker instance starts with discovery order. Fairness applies
to successive scans of a live worker and assumes host callbacks finish; one hung
callback can still hold up serial work. Database pagination, durable fair cursors
and load/capacity acceptance remain open.

`worker.diagnostics()` returns a detached/read-only snapshot with schema version 1
and `authority: diagnosis_only`. It includes `serving`, configured batch limit,
current phase (`idle`, `discovering`, `scheduling`, `inspecting`, `resuming`,
`settling`, `observing`, `waiting`) and `lastScan`. The last scan reports outcome
(`complete`, `stopped`, `failed`, or Python cancellation `interrupted`), distinct
candidate count, visits, unvisited/deferred count, and blocked/settled/failed result
counts. A visit interrupted by stop or infrastructure failure may have no result;
result counts need not sum to visits. An empty completed scan reports zeros.
`lastScan` remains null before the first scan. Discovery failure has no candidate
count evidence and reports zero; it is not evidence of an empty queue. Observer
failure belongs to the service boundary, not the already completed scan outcome.

These observations contain no Run identifiers, tool arguments, request contents or
exception messages. They describe this worker instance, not fleet health or Run
success, and never authorize a resume. Reading diagnostics performs no I/O.

## Indexed candidate pages

SQLite exposes `list_run_candidates(after_run_id=None, limit=100)` /
`listRunCandidates({afterRunId, limit})`. Limits are integers 1–1000. The result has
`authority: candidate_only`, `runIds` and `nextAfterRunId` (null at the observed end).
It reads at most limit+1 IDs from the existing scoped, SDK-specific journal Run
index using keyset ordering. Apart from the small approval-format marker check, it reads no Run state body or output event bodies and
uses a read transaction. The old `list_running` / `listRunning` APIs retain their
running-only contract and implementation.

Candidates include terminal Runs and may include Child Runs. They are not runnable
work. Hosts must filter to registered bindings and use existing inspection/public
resume; a terminal candidate is skipped. Index observation does not validate state
or journal integrity. No new permission, lease, status mirror or schema migration
is introduced. Do not use this API as a health check or proof of recovery readiness.

For a worker that processes one page per scan, keep the cursor in its discovery
callback and reset it to null after the last page. Use an unlimited worker scan or
a worker batch limit at least as large as the page; otherwise advancing the page
would discard unvisited candidates when the next discovery list replaces it.

```python
def candidate_discovery(storage):
    cursor = None
    async def discover():
        nonlocal cursor
        page = await storage.list_run_candidates(after_run_id=cursor, limit=100)
        cursor = page['nextAfterRunId']
        return page['runIds']
    return discover
```

```ts
let cursor: string | null = null;
const discover = async () => {
  const page = await storage.listRunCandidates({
    ...(cursor === null ? {} : {afterRunId: cursor}), limit: 100,
  });
  cursor = page.nextAfterRunId;
  return page.runIds;
};
```

Each page observes a committed snapshot, not a snapshot spanning the whole traversal.
New IDs inserted before the cursor are found after wraparound; an existing candidate
can become terminal before inspection. A new worker may restart traversal from the
beginning. The host owns cursor persistence and filtering. This bounds discovery
read volume per page but still traverses retained terminal history, and subsequent
inspection/Run recovery can restore full state and history. An indexed running-only
projection, persistent fair cursor and mixed-load capacity evidence remain open.
