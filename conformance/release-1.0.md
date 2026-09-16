# PurrA 1.0.0: support and upgrades

English | [简体中文](release-1.0.zh-CN.md)

The package metadata in this checkout is 1.0.0. Install Core and optional
PurrA packages at the same `1.0.0` version from their registries.

## Included capabilities

Both Python and TypeScript provide Run-bound structured object tasks, explicit
`local` and `native_required` modes, bounded opt-in format repair, scoped MCP
read-only tools, opt-in bounded concurrency for safe read batches, integration
check reports, and read-only recovery inspection. Core has no runtime dependencies.
The seven optional packages are OpenAI, Anthropic, SQLite, interaction,
compaction, Mem0, and MCP.

The application owns model capabilities, credentials, SDK clients, authorization,
resource scope, persistence, budgets, and shutdown. Installing an adapter does
not activate it on an Agent. See [structured output](structured-output.md),
[tool concurrency](tool-concurrency.md), and [inspection](integration-inspection.md).

## Service evidence and limits

The following describes checks performed on 2026-09-06, not an availability SLA,
model-quality benchmark, or a guarantee for other configurations. Deterministic
SDK tests, installed-package checks, live service checks, and downstream business
acceptance are separate evidence categories.

| Path | Evidence | Remaining boundary |
| --- | --- | --- |
| DeepSeek `deepseek-v4-flash`, Responses, `native_required` | Python/TypeScript actual schema requests, local revalidation, Run-bound receipts, usage settlement and persisted replay passed | Verified Responses protocol/service/model combination; not proof of other combinations or every schema constraint |
| DeepSeek Chat, `local` and one format repair | Both runtimes passed; controlled input caused a real schema mismatch followed by a new, settled repair invocation | Host-specific wire mapping required; not a natural-task repair-rate benchmark |
| OpenAI Chat Completions protocol, native schema | Both SDK request/response and failure fixtures passed | Live `response_format.json_schema` validation on a compatible service/model remains pending; local Chat validation does not cover this capability |
| Anthropic Messages protocol, native schema | Both SDK request/response and failure fixtures passed | Live `output_config.format` validation on a compatible service/model remains pending |
| Microsoft Learn MCP `microsoft_docs_fetch` | Actual read-only text calls and bounded concurrency passed in both runtimes | Only the selected, scoped tool; not arbitrary Microsoft MCP tools |
| Cloudflare documentation MCP `search_cloudflare_documentation` | Actual structured JSON, output-schema validation and bounded concurrency passed in both runtimes | Only the selected, scoped public documentation tool |
| MCP cancellation, disconnect and catalog changes | Both runtimes passed separate-process HTTP server termination, catalog notifications, stale in-flight result rejection and queued-call blocking; host cancellation also checked against Cloudflare | Controlled faults, not third-party production outages; disconnect detection may wait for the RPC timeout; Python transport-context failures require host handling |
| Downstream applications | Public examples, package consumers and integration contracts available | Actual application/business acceptance belongs to the consuming project |

`native_required` requires explicit model capability and adapter dialect support.
It fails before a call when unsupported; it does not silently fall back to local
validation. Compatible endpoint names alone do not establish model support.
Acceptance is defined by protocol entry point, capability, and the actual
service/model combination, not by the model vendor. OpenAI Responses, OpenAI
Chat Completions, and Anthropic Messages are distinct protocol paths. A
compatible third-party service can provide live evidence for the capabilities
it implements; models or credentials from OpenAI or Anthropic are not a
mandatory release gate. `native_required` means service-side schema constraints,
not a first-party model. The DeepSeek Responses checks above satisfy live
verification for that selected path. Chat and Messages native-schema checks
remain pending and require a compatible service/model that implements the
respective schema fields. Basic chat compatibility or a successful response
alone does not establish schema constraints, tool, stream, termination, or usage
semantics; record only the capabilities exercised. Unverified combinations stay
unverified; this correction adds no new passing results or support claims.

MCP accepts only its documented schema subset. A root `$schema` may explicitly
select `https://json-schema.org/draft/2020-12/schema`; the declaration stays in
catalog identity and counts against limits. Other dialects, nested declarations,
references, `default`, and vendor keywords remain unsupported. There is no full
JSON Schema conformance claim. Remote read-only annotations do not grant access
or concurrency permission. See the [Python](../integrations/mcp/python/README.md)
and [TypeScript](../integrations/mcp/typescript/README.md) MCP contracts.

## Public compatibility from 1.0

- Patch releases preserve documented public APIs and behavior. Minor releases
  add capabilities through new exports, optional parameters, or explicitly
  negotiated capabilities; they do not add mandatory methods to existing host ports.
- Public types and documented exports are the supported surface. Private module
  paths, internal state codecs, and generated SDK extension internals are not
  application APIs. A version-pinned storage adapter contract is documented separately.
- Existing event/error meanings are preserved. Where a contract explicitly allows
  unknown fields or codes, consumers must retain an unknown state rather than
  interpreting it as success or permission to resume. Closed enums are not
  implicitly extensible merely because they are represented as strings.
- A breaking public change requires a major version. Removing an existing data
  format or reinterpreting persisted state is not a harmless patch. Storage format
  numbers do not bypass application-visible compatibility commitments.

## Moving from 0.x

There are no 0.x aliases, fallback codecs, or automatic migrations promised by
this release. Do not upgrade an active production Run in place.

1. Inventory the installed SDK and optional-package versions, storage formats,
   invocation receipts, presets, model/tool bindings, and active Runs. Retain a
   backup and an environment capable of reading the original data.
2. Finish or explicitly cancel active Runs in the old runtime and reconcile
   external effects. A canceled local wait does not prove a remote effect stopped.
3. Install the exact release in a separate environment. Start with a new
   database, or a disposable backup copy when assessing historical readability.
   Do not point a validation harness at the only production copy.
4. Check historical reads and new Run execution independently. Restart the host,
   verify a new persisted Run and its usage/output, and keep rollback data intact.

SQLite storage is v4 and rejects other storage versions; Python and TypeScript
snapshots are not interchangeable. New invocation receipts use schema v3.
TypeScript imports reject unsupported receipt versions. Matching SQLite v4 alone
therefore does not establish that a 0.x history or active checkpoint is compatible.
No general 0.x history migration is supplied in this release; using a new database does not
authorize deleting the old one. Recovery inspection never grants resume authority.

For future 1.x storage changes, release notes must distinguish historical reads,
validated offline migration, and continuation of active Runs. Requiring active
Runs to drain is explicit; silently discarding them or their history is not an
upgrade policy. Any migration must be verified before it is described as supported.

## Release verification and publication

Validate the exact release commit and all package artifacts, including exported
APIs, dependency pins, file inventories, and hashes. Run the supported CI runtime
matrix separately from a single local platform check. A successful local build
or package version does not mean CI ran or a registry received an upload.

Publication requires a matching `v1.0.0` tag, package metadata, release notes and
artifacts, plus completion or explicit disposition of the open support gates.
