# purra-mcp (Python)

Development package for explicitly authorized, read-only MCP tools. Requires a
matching PurrA Core and the pinned official `mcp==1.29.1` SDK (v1 line). Core has
no MCP dependency. Install from this checkout with:

```sh
python -m pip install . ./integrations/mcp/python
```

The application opens and initializes `ClientSession`, supplies credentials and
transport limits, and closes the connection. The adapter performs paginated
`tools/list` discovery and single `tools/call` requests on that session.

```python
from mcp import ClientSession
from purra.contracts import ToolPolicy
from purra_mcp import McpCatalogMonitor, McpToolBinding, discover_mcp_tools

monitor = McpCatalogMonitor()
# read_stream/write_stream come from the application's chosen SDK transport.
try:
    async with ClientSession(read_stream, write_stream,
                             message_handler=monitor.on_message) as client:
        initialized = await client.initialize()
        monitor.accept_protocol_version(initialized.protocolVersion)
        catalog = await discover_mcp_tools(client, "documents", {
            "remote.search": McpToolBinding(
                local_name="search_documents",
                policy=ToolPolicy(mode="read", title="Search documents"),
                scope_validator=authorize_document_scope,
                concurrency_safe=False,
            ),
        }, monitor=monitor)
        # Pass catalog.registrations to Core's InMemoryToolCatalog while this
        # session remains open. authorize_document_scope(state, args, signal)
        # returns None to allow or a non-empty denial string.
finally:
    monitor.close()
```

If the host already has a message handler, compose it with `monitor.on_message`.
Feed the negotiated protocol from the actual initialization response, never a
configured guess. Mark the monitor closed in the host's disconnect/finally path.
Supported negotiated versions are `2025-06-18` and `2025-11-25`.

Every binding is an explicit host declaration: the remote tool really is
read-only, its local name and policy are authorized, and its scope validator is
appropriate. Pass `scope_validator=None` only when the host intentionally needs
no additional scope check. Remote annotations grant no authority. An incorrect
host read declaration cannot make a remote write safe. `concurrency_safe` records
the host's declaration; it does not enable parallel scheduling by itself.

Discovery returns a detached immutable snapshot with `schemaVersion=1`,
`serverId`, negotiated `protocolVersion`, `revisionDigest`, and entries sorted by
local name. The digest uses Core's `purra.json-identity/v1` over those fields
excluding the digest. Entry identity includes schemas, description, names,
read policy, and concurrency declaration. `notifications/tools/list_changed`
makes old registrations stale. Explicitly discover again and replace the host's
catalog at a safe boundary; an existing catalog never silently acquires tools.

Selected input and output schemas must satisfy Core's
`purra.output-schema/v1` object profile. Unsupported schemas fail discovery;
there is no schema weakening or parameter coercion. Unbound tools are not
registered. Successful results are JSON envelopes:

```json
{"text":["human-readable text"],"structured":{"count":3}}
```

`structured` is null when absent. An advertised output schema requires valid
`structuredContent`; JSON-looking text is never a substitute. Images, audio,
resource content/links, required remote tasks, and `isError` results fail closed.
The adapter does not register sampling, elicitation, resources, or task handlers.
The host must not enable those capabilities on this tools-only path.

`McpToolLimits` defaults: 32 pages, 256 tools, 1 MiB catalog, 64 KiB selected
schema, 8 KiB description, 64 KiB result/argument object budget, 128 content
blocks, and 30 seconds per RPC. Limits may be lowered; timeout may be raised to
300 seconds. Core adds depth/node/validation budgets. These are decoded-value
limits; enforce wire/frame/body limits in the host transport as well.

Discovery raises sanitized `McpAdapterError` codes. Tool failures return an empty
envelope plus `error_code`: `mcp_invalid_arguments`, `mcp_scope_denied`,
`mcp_catalog_stale`, `mcp_connection_closed`, `mcp_timeout`,
`mcp_protocol_error`, `mcp_transport_error`, `mcp_tool_error`,
`mcp_result_invalid`, `mcp_result_unsupported`, or `mcp_result_too_large`.
Core admission may reject the whole batch before the handler, using Core's
argument/scope error codes. Remote error messages are not returned to the model.
There are no adapter retries, implicit rediscovery, or hidden model calls.

The host must also handle failures around the transport context manager. In the
pinned Python Streamable HTTP SDK, a connection failure in a background HTTP
task can exit the entire session with an `ExceptionGroup`, interrupting the
awaiting tool call. This is a host session failure, not a returned adapter tool
error. Close the monitor in `finally`, discard the failed session and create a
new initialized session and catalog before resuming work. Do not expose raw
transport exceptions to the model. A lost response stream may instead remain
pending until the configured request timeout; disconnect detection is not
guaranteed to be immediate.

Cancellation stops local waiting and discards late results. The pinned Python
SDK removes the pending local request but does not send a cancellation
notification from `send_request`; remote execution can continue. The host owns
remote cancellation policy and connection shutdown. Neither cancellation nor a
read result's `effect_state="not_started"` proves remote execution did not occur:
that field describes the declared absence of host writes. No write tools are
supported by this adapter.

Tests use an official local SDK server and shared fixtures. The installed smoke
runs a separate stdio server owned and stopped by its host script. These checks
do not establish third-party MCP availability, Provider quality, or downstream
integration. See the [MCP tools specification](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)
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
