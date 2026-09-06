import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { CallToolResultSchema, ListToolsResultSchema, ErrorCode, McpError, type CallToolResult, type ServerNotification } from "@modelcontextprotocol/sdk/types.js";
import { AgentCanceledError, AgentError, StructuredOutputContract, StructuredOutputError, jsonIdentityDigest,
  type JsonValue, type ToolDefinition, type ToolPolicy, type ToolContext } from "purra";

export class McpAdapterError extends AgentError {
  constructor(code: string) { super(code, "MCP tool boundary rejected the operation"); }
}
function fail(code: string): never { throw new McpAdapterError(code); }
const encoder = new TextEncoder();
const size = (value: unknown): number => encoder.encode(JSON.stringify(value)).length;

export interface McpToolLimits {
  readonly maxPages: number;
  readonly maxTools: number;
  readonly maxCatalogBytes: number;
  readonly maxSchemaBytes: number;
  readonly maxDescriptionBytes: number;
  readonly maxResultBytes: number;
  readonly maxContentBlocks: number;
  readonly timeoutMs: number;
}
const defaults: McpToolLimits = Object.freeze({ maxPages: 32, maxTools: 256, maxCatalogBytes: 1_048_576,
  maxSchemaBytes: 65_536, maxDescriptionBytes: 8_192, maxResultBytes: 65_536, maxContentBlocks: 128, timeoutMs: 30_000 });
function limitsOf(input: Partial<McpToolLimits> = {}): McpToolLimits {
  if (Object.keys(input).some(key => !(key in defaults))) fail("mcp_binding_invalid");
  const limits = { ...defaults, ...input };
  for (const key of Object.keys(defaults) as (keyof McpToolLimits)[]) {
    if (!Number.isSafeInteger(limits[key]) || limits[key] <= 0 || limits[key] > (key === "timeoutMs" ? 300_000 : defaults[key])) fail("mcp_binding_invalid");
  }
  return Object.freeze(limits);
}

/** Feed negotiated protocol, list-change notifications and disconnects from the host. */
export class McpCatalogMonitor {
  #revision = 0;
  #closed = false;
  #protocol: string | undefined;
  acceptProtocolVersion(version: string): void {
    if (!["2025-06-18", "2025-11-25"].includes(version)) { this.close(); fail("mcp_protocol_unsupported"); }
    if (this.#protocol !== undefined && this.#protocol !== version) { this.close(); fail("mcp_protocol_conflict"); }
    this.#protocol = version;
  }
  onNotification(notification: ServerNotification): void {
    if (notification.method === "notifications/tools/list_changed") this.#revision++;
  }
  close(): void { this.#closed = true; }
  get revision(): number { return this.#revision; }
  get protocolVersion(): string | undefined { return this.#protocol; }
  check(revision: number): void {
    if (this.#closed) fail("mcp_connection_closed");
    if (this.#protocol === undefined) fail("mcp_not_initialized");
    if (revision !== this.#revision) fail("mcp_catalog_stale");
  }
}
export interface McpToolBinding {
  readonly localName: string;
  readonly policy: ToolPolicy;
  readonly scope: NonNullable<ToolDefinition["scope"]> | null;
  readonly concurrencySafe?: boolean;
}
export interface McpToolEntry {
  readonly remoteName: string;
  readonly localName: string;
  readonly description: string;
  readonly inputSchema: Readonly<Record<string, JsonValue>>;
  readonly outputSchema: Readonly<Record<string, JsonValue>> | null;
  readonly policy: Readonly<{ mode: "read"; title: string; riskLevel: "read" }>;
  readonly concurrencySafe: boolean;
}
export interface McpToolSnapshot {
  readonly schemaVersion: 1;
  readonly serverId: string;
  readonly protocolVersion: string;
  readonly revisionDigest: string;
  readonly entries: readonly McpToolEntry[];
}
export interface McpToolCatalog {
  readonly snapshot: McpToolSnapshot;
  readonly registrations: readonly ToolDefinition[];
  readonly stale: boolean;
}
export interface McpDiscoveryOptions {
  readonly monitor: McpCatalogMonitor;
  readonly limits?: Partial<McpToolLimits>;
  readonly signal?: AbortSignal;
}
function stopped(signal?: AbortSignal): void { if (signal?.aborted) throw new AgentCanceledError(); }
async function request<T>(operation: () => Promise<T>, signal?: AbortSignal): Promise<T> {
  stopped(signal);
  try { return await operation(); }
  catch (error) {
    stopped(signal);
    if (error instanceof McpError) {
      if (error.code === ErrorCode.RequestTimeout) fail("mcp_timeout");
      if (error.code === ErrorCode.ConnectionClosed) fail("mcp_transport_error");
      fail("mcp_protocol_error");
    }
    if (error instanceof TypeError || error !== null && typeof error === "object" && "name" in error
      && (error.name === "ZodError" || error.name === "$ZodError")) fail("mcp_result_invalid");
    fail("mcp_transport_error");
  }
}
async function contract(schema: Readonly<Record<string, JsonValue>>, limits: McpToolLimits): Promise<StructuredOutputContract> {
  try { return await StructuredOutputContract.create({ schemaId: "mcp.object", schemaVersion: "1", schema,
    limits: { schemaBytes: limits.maxSchemaBytes, outputBytes: limits.maxResultBytes } }); }
  catch { return fail("mcp_schema_unsupported"); }
}
function resultValue(raw: CallToolResult, output: StructuredOutputContract | undefined, envelope: StructuredOutputContract, limits: McpToolLimits): Readonly<Record<string, JsonValue>> {
  if (raw.isError) fail("mcp_tool_error");
  if (raw.content.length > limits.maxContentBlocks || size(raw) > limits.maxResultBytes) fail("mcp_result_too_large");
  const text: string[] = [];
  for (const block of raw.content) {
    if (block.type !== "text") fail("mcp_result_unsupported");
    text.push(block.text);
  }
  try {
    const value = envelope.validateValue({ text, structured: raw.structuredContent ?? null });
    output?.validateValue(raw.structuredContent ?? null);
    return value;
  } catch (error) {
    if (error instanceof StructuredOutputError) fail("mcp_result_invalid");
    throw error;
  }
}

export async function discoverMcpTools(client: Client, serverId: string, bindings: Readonly<Record<string, McpToolBinding>>,
  options: McpDiscoveryOptions): Promise<McpToolCatalog> {
  const monitor = options.monitor;
  const limits = limitsOf(options.limits);
  if (!(client instanceof Client) || !(monitor instanceof McpCatalogMonitor)
    || typeof serverId !== "string" || !/^[A-Za-z0-9_.-]{1,128}$/.test(serverId)
    || !bindings || typeof bindings !== "object" || Array.isArray(bindings)) fail("mcp_binding_invalid");
  const bound = new Map<string, McpToolBinding>();
  const localNames = new Set<string>();
  for (const [remote, binding] of Object.entries(bindings)) {
    if (!/^[A-Za-z0-9_.-]{1,128}$/.test(remote) || !binding || typeof binding.localName !== "string"
      || !/^[A-Za-z_][A-Za-z0-9_]{0,63}$/.test(binding.localName) || binding.policy?.mode !== "read"
      || binding.policy.riskLevel !== undefined && binding.policy.riskLevel !== "read"
      || typeof binding.policy.title !== "string" || !binding.policy.title.trim()
      || binding.scope !== null && typeof binding.scope !== "function"
      || binding.concurrencySafe !== undefined && typeof binding.concurrencySafe !== "boolean") fail("mcp_binding_invalid");
    if (localNames.has(binding.localName)) fail("mcp_name_conflict");
    localNames.add(binding.localName);
    bound.set(remote, Object.freeze({ ...binding, policy: Object.freeze({ mode: "read", title: binding.policy.title.trim(), riskLevel: "read" }) }));
  }
  if (bound.size === 0 || bound.size > limits.maxTools) fail("mcp_binding_invalid");
  const revision = monitor.revision;
  monitor.check(revision);
  if (client.getServerCapabilities()?.tools === undefined || client.transport === undefined) fail("mcp_tools_unavailable");
  const envelope = await contract({ type: "object" }, limits);
  const seen = new Set<string>(), cursors = new Set<string>();
  const found = new Map<string, { entry: McpToolEntry; binding: McpToolBinding; input: StructuredOutputContract; output: StructuredOutputContract | undefined }>();
  let cursor: string | undefined, totalBytes = 0, finished = false;
  const requestOptions = { timeout: limits.timeoutMs, maxTotalTimeout: limits.timeoutMs,
    ...(options.signal === undefined ? {} : { signal: options.signal }) };
  for (let page = 0; page < limits.maxPages; page++) {
    monitor.check(revision);
    // Do not use callTool/listTools convenience caches or implicit rediscovery.
    const result = await request(() => client.request({ method: "tools/list", ...(cursor === undefined ? {} : { params: { cursor } }) }, ListToolsResultSchema, requestOptions), options.signal);
    monitor.check(revision);
    totalBytes += size(result);
    if (totalBytes > limits.maxCatalogBytes || seen.size + result.tools.length > limits.maxTools) fail("mcp_catalog_limit_exceeded");
    for (const tool of result.tools) {
      if (seen.has(tool.name)) fail("mcp_name_conflict");
      seen.add(tool.name);
      const binding = bound.get(tool.name);
      if (binding === undefined) continue;
      const description = tool.description ?? "";
      if (encoder.encode(description).length > limits.maxDescriptionBytes) fail("mcp_catalog_limit_exceeded");
      if (tool.execution?.taskSupport === "required") fail("mcp_tool_unsupported");
      const input = await contract(tool.inputSchema as Readonly<Record<string, JsonValue>>, limits);
      const output = tool.outputSchema === undefined ? undefined : await contract(tool.outputSchema as Readonly<Record<string, JsonValue>>, limits);
      const entry: McpToolEntry = Object.freeze({ remoteName: tool.name, localName: binding.localName, description,
        inputSchema: input.schema, outputSchema: output?.schema ?? null,
        policy: Object.freeze({ mode: "read", title: binding.policy.title, riskLevel: "read" }), concurrencySafe: binding.concurrencySafe ?? false });
      found.set(tool.name, { entry, binding, input, output });
    }
    cursor = result.nextCursor;
    if (cursor === undefined) { finished = true; break; }
    if (!cursor || encoder.encode(cursor).length > 512 || cursors.has(cursor)) fail("mcp_pagination_invalid");
    cursors.add(cursor);
  }
  if (!finished) fail("mcp_catalog_limit_exceeded");
  if (found.size !== bound.size) fail("mcp_binding_missing");
  const ordered = [...found.entries()].sort((a, b) => a[1].entry.localName < b[1].entry.localName ? -1 : 1);
  const identity = Object.freeze({ schemaVersion: 1 as const, serverId, protocolVersion: monitor.protocolVersion!,
    entries: Object.freeze(ordered.map(([, row]) => row.entry)) });
  let revisionDigest: string;
  try { revisionDigest = await jsonIdentityDigest(identity); }
  catch { return fail("mcp_catalog_limit_exceeded"); }
  const snapshot = Object.freeze({ ...identity, revisionDigest });
  const registrations: readonly ToolDefinition[] = Object.freeze(ordered.map(([remote, row]) => {
    const scope = async (input: JsonValue, context: ToolContext) => {
      monitor.check(revision); stopped(context.signal);
      return row.binding.scope?.(input, context);
    };
    return Object.freeze({ name: row.entry.localName, description: row.entry.description || row.binding.policy.title,
      inputSchema: row.input.schema, argumentContract: row.input, policy: row.binding.policy, concurrencySafe: row.entry.concurrencySafe,
      scope, async run(input: JsonValue, context: ToolContext) {
        try {
          monitor.check(revision);
          let value: Readonly<Record<string, JsonValue>>;
          try { value = row.input.validateValue(input); }
          catch { return { content: { text: [], structured: null }, effectState: "not_started", errorCode: "mcp_invalid_arguments" }; }
          const decision = await scope(value, context);
          if (decision === false || typeof decision === "string" && decision.trim()) fail("mcp_scope_denied");
          monitor.check(revision); stopped(context.signal);
          const result = await request(() => client.request({ method: "tools/call", params: { name: remote, arguments: value } }, CallToolResultSchema,
            { timeout: limits.timeoutMs, maxTotalTimeout: limits.timeoutMs, ...(context.signal === undefined ? {} : { signal: context.signal }) }), context.signal);
          monitor.check(revision);
          return { content: resultValue(result, row.output, envelope, limits), effectState: "not_started" };
        } catch (error) {
          if (error instanceof McpAdapterError) return { content: { text: [], structured: null }, effectState: "not_started", errorCode: error.code };
          throw error;
        }
      },
    } satisfies ToolDefinition);
  }));
  monitor.check(revision);
  return Object.freeze({ snapshot, registrations, get stale() { try { monitor.check(revision); return false; } catch { return true; } } });
}
