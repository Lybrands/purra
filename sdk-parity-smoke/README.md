# PurrA SDK parity smoke

This is a local black-box consumer project for the Python and TypeScript
distributions of PurrA. It builds and installs both package artifacts, runs two
deterministic scenarios, and compares the normalized results.

It checks:

- the Python wheel installs and imports outside the source tree;
- the npm tarball installs, exposes types, and runs as ESM;
- both SDKs execute one schema-validated read tool;
- both SDKs execute a canonical Child Run with a distinct Run identity;
- the Child Agent receives isolated context and only the allowed read tool;
- the parent receives and summarizes the Child Agent result;
- both SDKs produce the same final output, tool input/result, and package
  version;
- model-call counts are reported separately as an operational parity signal.

It does not replace the repository's full unit and conformance suites.

## Run

Requirements: Python 3.11+, Node.js 22+, and npm.

```bash
cd sdk-parity-smoke
./run-local.sh
```

Set `PYTHON=/path/to/python3.11` to override the interpreter; that interpreter
must provide pip. By default the runner uses the repository's `.venv/bin/python`
when it provides pip, then falls back to `python3`.

Successful output ends with:

```text
PASS Python and TypeScript SDKs are semantically consistent for local-tool-roundtrip
PASS Python and TypeScript SDKs are semantically consistent for canonical-child-agent
```

The raw normalized outputs remain in `.work/results/` for inspection.

A model-call count difference is printed as `NOTICE`, not treated as a semantic
failure. It still matters for cost, latency, and safety-policy parity. Both SDKs
perform the additional tool-free public finalization call for a normal Agent
run. Python executes canonical Agent Tree runs in validated-result mode and
skips that call for both Root and Child Runs; TypeScript now follows the same
contract.

The local runner reuses the repository's pinned TypeScript compiler so it does
not download build tooling. If this directory later becomes a standalone
repository, add the same pinned TypeScript version as a development dependency.
