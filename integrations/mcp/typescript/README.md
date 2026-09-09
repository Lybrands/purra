# purra-mcp (TypeScript)

Development package for host-authorized MCP read tools and durable approval-gated writes. Requires
Node.js 22+, matching `purra`, and the pinned official
`@modelcontextprotocol/sdk@1.30.0` (v1 line). Core has no MCP dependency.
Build Core and this package, then install both in the consuming application.

The application owns the connected official `Client`, transport, credentials,
initialization and shutdown. The adapter sends only paginated `tools/list` and
single `tools/call` requests. Configure monitoring before connecting:

```ts
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { ToolListChangedNotificationSchema } from "@modelcontextprotocol/sdk/types.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { McpCatalogMonitor, discoverMcpTools } from "purra-mcp";

async function attach(transport: Transport) {
  const monitor = new McpCatalogMonitor();
  const setVersion = transport.setProtocolVersion?.bind(transport);
  transport.setProtocolVersion = version => {
    setVersion?.(version);
    monitor.acceptProtocolVersion(version);
  };
  const client = new Client({ name: "my-host", version: "1" });
  client.setNotificationHandler(ToolListChangedNotificationSchema,
    notification => monitor.onNotification(notification));
  client.onclose = () => monitor.close();
  await client.connect(transport);
  const catalog = await discoverMcpTools(client, "documents", {
    "remote.search": {
      localName: "search_documents",
      policy: { mode: "read", title: "Search documents" },
      scope: (args, context) => context.runId ? undefined : "Missing Run",
      concurrencySafe: false,
    },
  }, { monitor });
  // Pass catalog.registrations as Agent tools while client remains connected.
  // Replace the sample scope check with the application's real authorization.
  return { client, catalog, monitor }; // Caller eventually awaits client.close().
}
```

Compose existing notification/close handlers rather than replacing host logic.
The protocol hook receives the actual handshake version from SDK `connect`;
never fill it with a configured guess. Supported negotiated versions are
`2025-06-18` and `2025-11-25`. A previously connected host must have retained its
negotiated version and fed all list-change notifications.

Each binding is a host assertion of read-only behavior and authorized scope.
Remote annotations grant no authority. `scope: null` explicitly opts out of an
additional host scope check. A function returns `undefined`/true to allow, or
false/a non-empty string to deny. Core supplies Run/root/agent/lease context.
`concurrencySafe` records a declaration and does not itself enable scheduling.
The adapter cannot make a falsely declared remote write read-only.

The immutable snapshot has `schemaVersion: 1`, `serverId`, `protocolVersion`,
`revisionDigest` and entries sorted by local name. Identity uses Core's
`purra.json-identity/v1` over these fields without `revisionDigest`; entries
include names, description, schemas, read policy and concurrency declaration.
A list-change notification makes old registrations stale. Discover again and
replace the host's catalog at a safe boundary. Registration never widens itself.

Selected schemas use Core's strict `purra.output-schema/v1` object profile;
unsupported schemas fail discovery without weakening. Arguments are not
coerced. Success contains `{ text: string[], structured: object | null }`.
An output schema requires valid `structuredContent`. Text that resembles JSON
does not satisfy it. Media, resource content/links, required tasks and `isError`
results fail closed. This adapter does not install sampling, elicitation,
resources or task handlers; the host must not enable them on this tools-only path.

Default `limits`: `maxPages: 32`, `maxTools: 256`, `maxCatalogBytes: 1048576`,
`maxSchemaBytes: 65536`, `maxDescriptionBytes: 8192`, `maxResultBytes: 65536`,
`maxContentBlocks: 128`, `timeoutMs: 30000`. Lower any limit, or raise timeout
up to 300000 ms. The result budget also bounds decoded argument objects; Core
adds depth/node/validation budgets. Hosts must separately bound transport
frames/bodies before SDK decoding.

Discovery throws sanitized `McpAdapterError`. Known tool failures produce an
empty envelope and `errorCode`: `mcp_invalid_arguments`, `mcp_scope_denied`,
`mcp_catalog_stale`, `mcp_connection_closed`, `mcp_timeout`,
`mcp_protocol_error`, `mcp_transport_error`, `mcp_tool_error`,
`mcp_result_invalid`, `mcp_result_unsupported`, or `mcp_result_too_large`.
Core's batch admission can fail earlier with Core argument/scope errors.
No remote error message is passed to the model. No adapter retries, implicit
rediscovery or hidden model calls occur.

An AbortSignal stops local waiting; the official SDK sends a best-effort
cancellation notification. It does not prove the remote task stopped. Late
results are discarded. `effectState: "not_started"` describes the declared
absence of host writes for read bindings, not whether a network request was sent.
The separate write entry point below classifies uncertain effects conservatively.

Shared fixtures run against the official local client/server protocol, with a
JSON serialization boundary. The installed smoke also launches and closes a
separate stdio server. Third-party MCP services, real Providers and downstream
applications require separate validation. See the [MCP tools specification](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)
and [cancellation specification](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/cancellation).

Core's separately configured argument and result limits also apply. A result
accepted by the adapter may still exceed a lower Core result limit and fail
with `tool_result_too_large`. Align both budgets in the host configuration.


Unsupported schema metadata is rejected as well as unsupported validation
keywords. For example, `default` and `x-fastmcp-wrap-result` are outside
`purra.output-schema/v1`: a selected tool using either fails discovery with
`mcp_schema_unsupported` before `tools/call`. The adapter never strips these
fields to make a remote tool appear supported. Select compatible tools explicitly;
unselected unsupported tools do not widen the catalog. A working MCP handshake
alone does not establish tool compatibility.

MCP input/output schemas may declare the root `$schema` as exactly
`https://json-schema.org/draft/2020-12/schema`. The adapter recognizes this
MCP dialect declaration, retains it in the immutable snapshot and revision
digest, and compiles the remaining constraints with `purra.output-schema/v1`.
The full remote schema, including the declaration, counts against the schema
byte limit. Other dialect URIs, nested declarations, references, and unknown
keywords remain unsupported; no schema document is fetched. This does not add
`$schema` to the Core structured-output profile or promise full JSON Schema
2020-12 support. See the [MCP schema rules](https://modelcontextprotocol.io/specification/2025-11-25/server/tools).

HTTP connection failures (including `fetch` transport `TypeError`) return
`mcp_transport_error`; SDK response validation failures remain
`mcp_result_invalid`. A lost response stream may remain pending until the
configured request timeout. The host must close the monitor when discarding a
failed session, then initialize a new session and explicitly rediscover tools
before resuming work. Transport recovery never authorizes new tools.

## Durable writes (1.0 development)

The separate write discovery entry point requires explicit host authorization:
confirm policy, matching `write` or `destructive` effect, a callable scope validator,
and nonempty binding/scope IDs and revisions. Server annotations never grant this
permission. Read discovery keeps its 1.0 contract and schema-1 snapshot. Write
snapshots use schema 2 and include the host authorization identity; concurrency
is disabled for these tools.

```ts
import { discoverMcpWriteTools } from "purra-mcp";

const writes = await discoverMcpWriteTools(client, "documents", {
  "remote.update": {
    localName: "update_document",
    policy: { mode: "confirm", title: "Update document", riskLevel: "write" },
    scope: validateCurrentDocumentScope,
    bindingId: "document-service", bindingRevision: "host-config-1",
    scopeId: "sandbox-documents", scopeRevision: "scope-1", effect: "write",
  },
}, { monitor });
const identity = writes.registrations[0]!.approvalBinding!;
```

In `toolCheckpointHandler`, spread the current `identity` into `ApprovalIntent.create`
alongside the actual checkpoint call and current Run preset fingerprint, then call
`approvals.prepare`. Configure Agent with `approval: approvals.gateway()`,
`idempotency: storage.idempotency`, and `toolCheckpointNames: ["update_document"]`.

The registration's effective binding revision is the complete write catalog digest,
which includes the original host binding revision, server identity, selected schemas,
and scope identity. Do not substitute the original host revision or reuse old binding
metadata after rediscovery. Core requires a durable approval gateway and its exact
receipt store, and the SQLite gateway checks the current registration identity against
the approved intent before dispatch. A live in-memory approval gateway is insufficient.
The handler function is an adapter implementation; applications execute it through
Core's tool catalog, never as a substitute for approval/claim checks.

A validated success response records `committed`. Local argument/scope/catalog
rejection before sending records `not_started`. Once a request is submitted,
timeout, cancellation, disconnect, catalog changes, RPC errors, `isError` responses,
and invalid/unsupported output record `unknown`. An error response cannot prove
that no remote write occurred. Unknown effects retain their durable claim; the
adapter never retries or automatically reconciles them. Host reconciliation requires
independent evidence. A successful remote response is protocol completion evidence,
not a read-back audit of the business data.

The protocol tests include SQLite approval restart, current-binding mismatch,
scope revocation, committed receipts and retained unknown claims against a synthetic
SDK server. Build the sibling Core and SQLite packages before TypeScript tests; include
Core, MCP and SQLite source directories on `PYTHONPATH` for Python tests. These are
deterministic local tests, separate from real Provider/MCP service and downstream
acceptance. See [durable approval](../../../conformance/durable-approval.md).

## Independent write-server acceptance

The repository's installed-consumer check now runs five scenarios against the
separate Python SDK process in `integrations/mcp/fixtures/write_server.py`:
success, lost response, exit before write, exit after write, and an error response
after write. The server accepts only a fixed synthetic value, writes inside a
fresh temporary directory, and fsyncs an independent event ledger. The host waits
for durable approval, reopens SQLite before approving, then checks both the local
receipt/unknown claim and the remote file. Reopening after completion/failure must
not dispatch again. Every scenario verifies that the server process has exited.

Copy `scripts/check-installed-write.mjs` into an independent npm consumer
containing matching Core, SQLite and MCP tarballs. Use a Python environment
containing the pinned `mcp` SDK for the fixture server:

```sh
node check-installed-write.mjs /absolute/path/to/write_server.py /absolute/path/to/python
```

These are controlled independent-service fault checks, using a scripted model.
They do not prove real Provider capability or third-party business-service
acceptance. The remote ledger is verification evidence, never an approval or
automatic reconciliation command. The CI workflow includes both consumers;
local success does not claim that remote CI has executed.
