# Optional packages

English | [简体中文](README.zh-CN.md)

These packages connect PurrA to model services, storage, and memory. Install only
the components your application uses, together with a matching Core version.

| Package | Purpose | Guides |
| --- | --- | --- |
| `purra-openai` | OpenAI Responses and Chat Completions | [Python](openai/python/README.md) · [TypeScript](openai/typescript/README.md) |
| `purra-anthropic` | Anthropic Messages | [Python](anthropic/python/README.md) · [TypeScript](anthropic/typescript/README.md) |
| `purra-sqlite` | Persistent Runs, events, checkpoints, and tool receipts | [Python](sqlite/python/README.md) · [TypeScript](sqlite/typescript/README.md) |
| `purra-interaction` | Structured user questions and resumable input waits | [Python](interaction/python/README.md) · [TypeScript](interaction/typescript/README.md) |
| `purra-compaction` | Model-assisted conversation compression | [Python](compaction/python/README.md) · [TypeScript](compaction/typescript/README.md) |
| `purra-mcp` | Host-owned read-only MCP tools | [Python](mcp/python/README.md) · [TypeScript](mcp/typescript/README.md) |
| `purra-mem0` | Scoped long-term memory and retrieval | [Python](mem0/python/README.md) · [TypeScript](mem0/typescript/README.md) |

Model adapters support native OpenAI and Anthropic APIs. Applications handle
other vendors' protocol differences, model configuration, and credentials.

## Source installation

Run these commands from the repository root. The example selects OpenAI;
substitute the component directory you need.

Python 3.11+:

```sh
python -m pip install . ./integrations/openai/python
```

Node.js 22+ (SQLite, interaction, and Mem0 require 22.13+):

```sh
npm --prefix typescript ci
npm --prefix typescript run build
npm --prefix integrations/openai/typescript ci
npm --prefix integrations/openai/typescript run build
```

Then install the built TypeScript packages in the consuming application:

```sh
npm install /path/to/purra/typescript /path/to/purra/integrations/openai/typescript
```

For same-Run clarification, also install and build `purra-sqlite` before
`purra-interaction`. Mem0's managed-provider dependencies are listed in its guides.

## Application configuration

Applications supply authenticated identities, authorized scopes, model
capabilities, and storage paths. They also own client lifecycles, backups, and
retention. Components use Core's execution and budget interfaces; installing a
package does not enable it on an Agent.

[1.0.0 candidate service evidence and upgrade boundaries](../conformance/release-1.0.md): live protocol/capability/service-model evidence and downstream acceptance are recorded separately; first-party model credentials are not required.
