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

## Maintaining fixtures

When changing a shared contract, update its fixture and both SDK test consumers.
Use values that both runtimes can represent exactly, and keep private execution
state out of public-output expectations. SDK-specific checkpoint formats and
public API names do not need to match.

For package installation and consumer checks, see
[SDK parity smoke](../sdk-parity-smoke/README.md).
