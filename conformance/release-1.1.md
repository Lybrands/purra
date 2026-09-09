# 1.1 development handoff

English | [简体中文](release-1.1.zh-CN.md)

1.1.0 is unreleased. Install matching Core, SQLite and MCP artifacts; publishing,
remote CI and downstream enablement are separate from local development checks.
The [approval contract](durable-approval.md) defines the public APIs.

## Supported execution boundary

Both SDKs support one approval-bearing write per selected batch in persistent
Reactive, Planned and Auto Root Runs. Waiting commits the exact tool continuation,
settled model invocation and approval intent together, then releases the lease.
Restart preserves planning, usage, deadlines and completed read-only Child work.
No model round or completed write is repeated to reconstruct an approval wait.

Host decisions require an authenticated principal and an explicit authorizer.
At dispatch, current binding, arguments, scope, configuration, cancellation,
deadline, budget and lease still apply. MCP writes use separate explicit host
bindings; remote annotations never authorize them. Approved intent and existing
tool claim are associated atomically. Unknown effects retain the claim and block
automatic redispatch; reconciliation requires independent host evidence.

Child writes, mixed/multiple-call approval batches, arbitrary nested approval and
clarification combinations, and general distributed effect recovery are outside
this supported boundary. No end-to-end exactly-once guarantee is made.

## Expanded 1.1 completion boundary

The additional storage and recovery work assigned to 1.1 is complete inside the
supported Root single-write boundary:

- SQLite metadata access uses indexed candidate pages, selected-Root journal
  restoration and validated metadata transactions. The disposable dual-SDK
  load check covers concurrent independent writers, continuous journal sequences,
  killed-transaction rollback, checkpoint reopen and unknown-effect reconciliation.
  Its measurements describe a bounded synthetic operating point; they are not a
  universal throughput, latency or production-capacity guarantee.
- The supported data upgrade is the explicit same-SDK v4-to-v5 offline path. It
  preserves historical reads, rejects active Runs, unresolved claims and foreign
  SDK rows, and provides WAL-aware backup plus restoration to a new path. Arbitrary
  historical formats and active-Run cross-version continuation are not public 1.1
  contracts.
- Effect recovery covers the complete implemented Root single-write matrix:
  confirmed completion reuses a receipt, proof of non-execution returns through
  every current dispatch gate, unknown effects remain blocked, terminal Runs stay
  terminal, and mismatched approval identity is rejected. Cross-machine ownership
  and general distributed reconciliation belong to a later contract.

These completion decisions do not upgrade the separate real business MCP,
downstream, remote-CI, cross-platform migration or production-load evidence gates.

## Host integration sequence

1. Preserve a backup and finish active Runs/reconcile unresolved effects with the
   original runtime. Offline v5 activation rejects active or foreign-SDK rows and
   unresolved claims across all scopes; default construction does not migrate v4.
   Historical same-SDK data is retained. Do not switch an active production Run.
2. Bind authenticated decision handling outside model/tool APIs. Treat approval
   display text as a host projection, not the private intent or authorization input.
3. Select business write names in the tool checkpoint callback. Build the intent
   from the actual call, saved preset and current registration identity. Persist
   the original expiry. Bind the approval gateway and its exact idempotency store.
4. On `ApprovalRequired`, display the pending decision and release the worker.
   After a host decision, resume the original Run/request with the same callback.
   Approval and inspection are observations; neither bypasses dispatch checks.
5. Present unknown effects for host reconciliation. Never clear a claim merely
   because a transport failed, a process exited or the user canceled a local wait.

Legacy live approvals, clarification, read-only MCP and v4-only applications keep
their existing contracts. Old model-ready checkpoints remain schema 2; the opt-in
tool-ready continuation is schema 3. Python/TypeScript storage is not interchangeable.

## Verification entry points and evidence classes

| Evidence | Reproducible entry points | Boundary |
| --- | --- | --- |
| Deterministic | SQLite `test_approvals.py`, `test_approval_resume.py`; TypeScript `approvals.test.mjs`, `approval-resume.test.mjs`; shared `approval_*.json` fixtures | Synthetic authorization, decision races, restart, receipt failure, lease faults and read-only inspection |
| Bounded SQLite load | `integrations/sqlite/python/scripts/verify_load.py` after building TypeScript Core and SQLite | Disposable dual-SDK database, concurrent writers, rollback, recovery and reopen; no production capacity claim |
| Installed artifacts | MCP Python `scripts/check_installed_write.py`, TypeScript `scripts/check-installed-write.mjs` | Install matching wheel/tarballs outside source; check package origins and private-file exclusion |
| Independent MCP process | `integrations/mcp/fixtures/write_server.py`, driven by both installed consumers | Scripted model, synthetic file; success, lost response, exit before/after write and error after write; verify ledger and process cleanup |
| Real Provider/business MCP | Host-selected protocol + capability + actual service/model | Not established by scripted models or synthetic writers |
| Downstream | Host authentication, UI, resource policy, offline activation and restart acceptance | Not run by this PurrA-only task; no adjacent project enabled |

Commands and executable host wiring are in the [Python MCP README](../integrations/mcp/python/README.md)
and [TypeScript MCP README](../integrations/mcp/typescript/README.md). Approval
observations omit private identifiers/arguments/digests and retain `diagnosis_only`.
Validate the exact delivered artifacts in the host before enabling business writes.

## Verified real Provider combination

GLM-5.3-Flash at `open.bigmodel.cn`, Z.ai Chat Completions, thinking enabled with
`reasoning_effort=low`, passed the Python and TypeScript installed-consumer success
and lost-response scenarios against the independent synthetic MCP writer. Each
scenario waited across SQLite reopen without another model call, dispatched only
after host approval, and performed one remote write. Lost responses retained an
unknown claim; repeated terminal resume did not dispatch. All server processes exited.

This evidence uses test-only host HTTP gateways: developer messages become system
messages, JSON tool results become text, and Z.ai uses `max_tokens` plus its thinking
field. Requests were nonstreaming (Python projected the complete response through
its Core stream port). It is not native OpenAI adapter, wire-streaming, Planned/Auto
real-model, native-schema, business MCP, or downstream acceptance. Requests were
bounded at 4096 generation tokens; a Root aggregate usage guarantee was not tested.
No credentials, model content or business resources are part of this evidence.
