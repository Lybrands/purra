# PurrA 1.0.0 candidate: support and upgrades

English | [简体中文](release-1.0.zh-CN.md)

The package metadata in this checkout is 1.0.0. This is an **unreleased
candidate**, not a publication announcement. Registry installation commands may
still select an earlier published version. Candidate consumers must use the
exact wheel or npm tarball supplied with their candidate manifest. Install Core
and optional PurrA packages at the same version.

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
| DeepSeek `deepseek-v4-flash`, Responses, `native_required` | Python/TypeScript actual schema requests, local revalidation, Run-bound receipts, usage settlement and persisted replay passed | Compatible service; not OpenAI service certification or proof of every schema constraint |
| DeepSeek Chat, `local` and one format repair | Both runtimes passed; controlled input caused a real schema mismatch followed by a new, settled repair invocation | Host-specific wire mapping required; not a natural-task repair-rate benchmark |
| OpenAI Responses/Chat native schema | Both SDK request/response and failure fixtures passed | Original OpenAI service/model validation pending |
| Anthropic Messages native schema | Both SDK request/response and failure fixtures passed | Original Anthropic service/model validation pending |
| Microsoft Learn MCP `microsoft_docs_fetch` | Actual read-only text calls and bounded concurrency passed in both runtimes | Only the selected, scoped tool; not arbitrary Microsoft MCP tools |
| Cloudflare documentation MCP `search_cloudflare_documentation` | Actual structured JSON, output-schema validation and bounded concurrency passed in both runtimes | Only the selected, scoped public documentation tool |
| MCP cancellation, disconnect and catalog changes | Local official-SDK protocol fault tests passed | Controlled remote-origin fault verification pending |
| Downstream applications | Public examples, package consumers and integration contracts available | Actual application/business acceptance belongs to the consuming project |

`native_required` requires explicit model capability and adapter dialect support.
It fails before a call when unsupported; it does not silently fall back to local
validation. Compatible endpoint names alone do not establish model support.
Original-provider checks remain open for final support sign-off.

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
this candidate. Do not upgrade an active production Run in place.

1. Inventory the installed SDK and optional-package versions, storage formats,
   invocation receipts, presets, model/tool bindings, and active Runs. Retain a
   backup and an environment capable of reading the original data.
2. Finish or explicitly cancel active Runs in the old runtime and reconcile
   external effects. A canceled local wait does not prove a remote effect stopped.
3. Install the exact candidate in a separate environment. Start with a new
   database, or a disposable backup copy when assessing historical readability.
   Do not point a validation harness at the only production copy.
4. Check historical reads and new Run execution independently. Restart the host,
   verify a new persisted Run and its usage/output, and keep rollback data intact.

SQLite storage is v4 and rejects other storage versions; Python and TypeScript
snapshots are not interchangeable. New invocation receipts use schema v3.
TypeScript imports reject unsupported receipt versions. Matching SQLite v4 alone
therefore does not establish that a 0.x history or active checkpoint is compatible.
No general 0.x history migration is supplied here; using a new database does not
authorize deleting the old one. Recovery inspection never grants resume authority.

For future 1.x storage changes, release notes must distinguish historical reads,
validated offline migration, and continuation of active Runs. Requiring active
Runs to drain is explicit; silently discarding them or their history is not an
upgrade policy. Any migration must be verified before it is described as supported.

## Candidate verification and publication

Validate the exact candidate commit and all package artifacts, including exported
APIs, dependency pins, file inventories, and hashes. Run the supported CI runtime
matrix separately from a single local platform check. A successful local build
or package version does not mean CI ran or a registry received an upload.

Publication requires a matching `v1.0.0` tag, package metadata, release notes and
artifacts, plus completion or explicit disposition of the open support gates.
Preparation does not create a tag, push a branch, or publish packages.
