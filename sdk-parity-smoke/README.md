# SDK parity smoke

English | [简体中文](README.zh-CN.md)

A consumer project that builds and installs the Python wheel and npm tarball,
runs a tool round and a child-Agent task, then compares normalized results.
It checks package imports, TypeScript declarations, and execution outside the
source tree. The model gateways are local and require no API credentials.

## Run

Requires Python 3.11+ with `pip`, `wheel`, and `setuptools>=77.0.3`, plus Node.js
22+ and npm. Install the repository's TypeScript build dependencies first:

```sh
npm --prefix typescript ci
cd sdk-parity-smoke
./run-local.sh
```

Set `PYTHON=/path/to/python` to select an interpreter. Otherwise the script uses
the repository's `.venv/bin/python` when it provides pip, then falls back to `python3`.

## Results

The runner prints `PASS` for each matching scenario. Final answers, tool inputs
and results, and package versions are compared. Model-call count differences
are reported as `NOTICE` and do not fail the semantic comparison.

Built packages and normalized results are stored in `.work/artifacts/` and
`.work/results/`. Each run recreates `.work/`, `dist/`, and `node_modules/` inside
this consumer directory.

Use the [conformance suites](../conformance/README.md) for detailed protocol checks.
