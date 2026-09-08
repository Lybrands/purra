# Integration reports and recovery inspection

`check_integration` / `checkIntegration` wrap existing conformance assertions. The
report schema is version 1. Component name and version are host-supplied public
labels; `declaredCapabilities` is a snapshot of declarations, not evidence.
Every capability/category pair is `passed`, `failed`, or `not_run`. A pass means
that the supplied assertion returned successfully. Missing checks stay `not_run`.
A failed assertion has a stable `<capability>_nonconforming` code; its exception,
Provider payload and tool output are never copied into the report.

The default enabled category is `deterministic`. `installed_artifact`,
`real_provider_mcp` and `downstream` require explicit `enabled_categories` /
`enabledCategories`. A category label does not create isolation or prove a live
service was used: the host owns truthful classification and an isolated fixture.
Conformance assertions may call models/tools or mutate their fixture stores. In
particular Python's gateway assertion executes both stream and complete; TS's
executes one invocation. Never supply a production adapter under a fixture label.
The wrapper adds no calls, retries or LLM judge. It awaits each check in report
order; cancellation propagates. Use ordinary Python execution, since the existing
Python conformance helpers use assertions.

Runnable examples: `examples/python/integration_check.py` and
`typescript/examples/integration-check.ts`. They exercise existing store assertions
on disposable in-memory stores and then inspect a canonical Run. They use no live
Provider/MCP. Host checks can wrap the structured-output, MCP and concurrency
assertions from the corresponding public APIs; declaring these capabilities alone
does not pass any check. Labels and probe registrations must be trusted host data.

## Read-only diagnosis

Core exports `inspect_recovery(repository, run_id)` /
`inspectRecovery(repository, runId)`. These call only `get` on the existing Run
repository. No new required port method is introduced. The generic reader knows
only status/checkpoint presence; other evidence stays unknown. A missing Run keeps
the repository's normal missing-Run error, rather than inventing a recovery state.

`SqliteAgentAdapters.inspect_recovery(run_id, expected_preset=...)` /
`inspectRecovery(runId, {expectedPreset})` additionally read a single committed
transaction: checkpoint, attempts after the checkpoint, pending tool claims,
execution lease, cancellation, deadline and an optional complete effective preset
comparison. The expected preset is supplied by the host; matching it says nothing
about future configuration or current authorization. Without it configuration is
unknown. SQLite inspection does not claim, resume, reconcile, append events,
serialize state or run any model/tool. Opening the adapter still performs its
normal database initialization; inspection is read-only on an already open adapter.
TS currently restores the selected Root's journal to count post-checkpoint attempts;
its cost grows with that journal. Python uses stored attempt counters. Neither
adapter changes its storage schema for inspection.

Reports contain only enums, counts and fixed reason/action codes, with
`authority: "diagnosis_only"`. They omit Run/tool identifiers, message bodies,
credentials, checkpoint messages, raw errors and private reasoning. The pure
`build_recovery_inspection` / `buildRecoveryInspection` builder allows a custom host
to supply the same normalized observations. It does not verify host evidence.
Missing counts are null, never zero. Unknowns and known blockers are separate;
even an empty blocker list is not a resume permit. Permissions, usage completeness,
external effects outside the adapter and Agent Tree ownership remain unknown in
the supplied SQLite reader. The actual execution path must revalidate all of them,
as well as leases, configuration, budgets and checkpoint freshness.

Python tool claims are associated with a Run, so the count covers that Run's tracked
claims. TS idempotency keys are opaque and have no canonical Run binding: the count
covers the adapter's storage scope and adds `unattributed_tool_effect_unknown` to
`cautions`, not the current Run's `blockers`;
Run-specific effects stay unknown. Zero claims never proves all external effects
safe. Tool reconciliation can remove a pending claim but cannot remove the separate
`run_recovery_requires_reconciliation` blocker for post-checkpoint model attempts.
The inspection API provides no tool-ready cursor or general side-effect recovery.

`suggestedActions` contains advisory inspection, reconciliation and revalidation
steps. None executes work or authorizes a retry. Reports describe a past observation
and are not a second persistent source of Run truth. Existing
`evaluate_agent_run_recovery` and TS historical recovery reports continue to
summarize recorded decisions; this API observes current stored readiness evidence.
