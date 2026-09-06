# purra-mcp (TypeScript)

Development package for explicitly authorized, read-only MCP tools. Requires
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
absence of host writes, not whether a network request was sent. Write tools are
outside this adapter's support.

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
