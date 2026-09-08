# Cross-language conformance

English | [简体中文](README.zh-CN.md)

The JSON files in [fixtures](fixtures/) define behavior shared by the Python and
TypeScript implementations. Both test suites consume these cases to check
protocol values, validation, state transitions, and error outcomes.

| Fixtures | Coverage |
| --- | --- |
| `model_protocol.json`, `context_protocol.json` | Model messages, output limits, and context budgets |
| `tool_security.json`, `retrieval.json` | Tool schemas, retrieval results, and access boundaries |
| `planning_protocol.json`, `planning_activation.json`, `planning_stream.json` | Plans, activation modes, and public progress streams |
| `durable_protocol.json`, `recovery_protocol.json` | Durable execution and recovery |
| `run_resume.json` | Public Root recovery rejection codes, exercised with SQLite |
| `agent_tree_protocol.json` | Agent identity, Child Runs, and shared budgets |
| `artifact_protocol.json`, `observability_protocol.json` | Artifacts, events, and diagnostics |

## Run

From the repository root:

```sh
python -m pip install -e '.[test]'
python -m pytest
npm --prefix typescript ci
npm --prefix typescript run check
```

The public recovery scenarios run in the optional SQLite suites. Install the
SQLite packages and run their checks as described in the
[Python](../integrations/sqlite/python/README.md) and
[TypeScript](../integrations/sqlite/typescript/README.md) guides. These tests
reopen temporary databases and verify that rejected recovery does not call a
Provider or tool or append canonical output. They use deterministic gateways.

## Maintaining fixtures

When changing a shared contract, update its fixture and both SDK test consumers.
Use values that both runtimes can represent exactly, and keep private execution
state out of public-output expectations. SDK-specific checkpoint formats and
public API names do not need to match.

For package installation and consumer checks, see
[SDK parity smoke](../sdk-parity-smoke/README.md).


## Capability contracts and tests

- [Planning chunk delivery](../docs/planning-output.md): `planning_stream.json`.
- [Structured output](../docs/structured-output.md): `structured_output.json`,
  `structured_model_task.json`, and `native_output_schema.json`.
- [Safe read concurrency](../docs/tool-concurrency.md): `tool_concurrency.json`.
- [Integration checks and read-only diagnosis](../docs/integration-inspection.md):
  `recovery_inspection.json`.
- MCP protocol and selected-schema rejection vectors live in
  `integrations/mcp/fixtures/tools.json` and run in both optional package suites.

Protocol fixtures and SDK transport tests do not certify a live model or remote
server. Record deterministic, installed-artifact, real-service and downstream
results separately. See the linked capability guides for their supported behavior.

This directory contains shared fixtures and test instructions. Public capability
guides live in `docs/`; version migration steps live in
[the upgrade guide](../docs/migrations/1.0.md).

## Repository documentation

Keep installation guides, public contracts, reproducible test instructions and
license notices in Git. Keep implementation plans, one-off validation evidence,
release checklists and local environment notes in ignored `docs/plans/`, `docs/prd/` or
`conformance/reports/`. Do not force-add these directories. Removing a tracked
file from the current tree does not remove it from earlier commits.

The optional SQLite multiprocess/load check uses disposable synthetic databases:
`PYTHONPATH=src:integrations/sqlite/python/src python integrations/sqlite/python/scripts/verify_load.py --output /tmp/purra-load.json`.
Build TypeScript Core and SQLite first. This checks local persistence behavior,
not live Provider latency.
