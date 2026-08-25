# PurrA

English | [简体中文](https://github.com/Lybrands/purra/blob/main/README.zh-CN.md)

PurrA is a product-neutral Agent runtime for applications that need explicit
control over model calls, tools, state, and recovery.

Python and JavaScript/TypeScript are independent implementations of the same
safety contracts.

## Install

Python 3.11+:

```bash
pip install purra
```

Node.js 22+:

```bash
npm install purra
```

The npm package is native ESM.

## Start

Python Runs enter through `purra.api.AgentCore.submit()`. See the runnable
[Python quickstart](https://github.com/Lybrands/purra/blob/main/examples/python/quickstart.py).

For JavaScript and TypeScript, see the
[package guide](https://github.com/Lybrands/purra/blob/main/typescript/README.md)
and [quickstart](https://github.com/Lybrands/purra/blob/main/typescript/examples/quickstart.ts).

Reactive execution is the default; Planned and Durable execution are opt-in.
The host supplies Provider adapters, tools, business authorization, and
production persistence. See [Architecture](https://github.com/Lybrands/purra/blob/main/ARCHITECTURE.md)
for runtime guarantees and ownership boundaries.

## Compatibility

PurrA is pre-1.0. Breaking public-contract changes require a new minor release.
Python and npm packages use the version encoded by the same Git tag; matching
versions do not imply automatic capability parity.

## Links

- [Runnable examples](https://github.com/Lybrands/purra/blob/main/examples/README.md)
- [Issue tracker](https://github.com/Lybrands/purra/issues)

## License

[MIT](https://github.com/Lybrands/purra/blob/main/LICENSE)
