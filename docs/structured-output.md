# Object output schema profile

`purra.output-schema/v1` defines complete JSON object validation in Python and
TypeScript. The shared vectors are in `conformance/fixtures/structured_output.json`. This
profile validates syntax and structure, not the factual accuracy of generated
content. Constructing a contract or calling `parse` never invokes a model.

Python imports `StructuredOutputContract`, `StructuredOutputLimits`, and
`StructuredOutputError` from `purra.structured` or `purra.api`. TypeScript imports
the contract and error from `purra`; creation is asynchronous because identity
hashing uses Web Crypto. Neither Core package adds runtime dependencies.

```python
from purra.structured import StructuredOutputContract

output = StructuredOutputContract(
    schema_id="label", schema_version="1",
    schema={"type": "object", "properties": {"label": {"type": "string"}},
            "required": ["label"], "additionalProperties": False},
)
value = output.parse('{"label":"example"}')
```

```typescript
import { StructuredOutputContract } from "purra";

const output = await StructuredOutputContract.create({
  schemaId: "label", schemaVersion: "1",
  schema: { type: "object", properties: { label: { type: "string" } },
    required: ["label"], additionalProperties: false },
});
const value = output.parse('{"label":"example"}');
```

The schema and returned value are detached and recursively immutable. The
TypeScript return type is a JSON object, not an unchecked application generic.
Schema IDs and versions are nonblank strings of at most 128 UTF-8 bytes.
`local` is the default mode; `native_required` is a distinct contract identity.
Accepting that identity does not certify a Gateway or model's native support.
Execution must separately preflight it and must never fall back silently.

## Schema and value rules

The root schema explicitly declares `type: object`. Supported keywords are:
`type`, `properties`, `required`, `additionalProperties`, `items`, `enum`, `const`,
`anyOf`, `oneOf`, `minimum`, `maximum`, `minLength`, `maxLength`, `minItems`,
`maxItems`, `uniqueItems`, `title`, and `description`. The fixture inventory is
checked against both validators. Boolean schemas and unknown keywords are
rejected. `additionalProperties` is boolean and defaults to allowing extra
fields. `items` is a single object schema. `type` accepts a name or a nonempty
array of unique names; the root still requires the literal `object`.

Only one complete JSON document is accepted. Whitespace is allowed; fences,
prefixes, trailing text, duplicate decoded keys (at any depth), BOMs, and
nonfinite numbers are rejected. There is no coercion, default insertion, field
removal, substring extraction, or string-encoded array repair. Existing planner
and import parsers retain their own contracts.

Numbers use finite IEEE-754 binary64 semantics, including rounding of decimal
fractions and underflow. An integral value outside ±(2^53 − 1) is rejected; use
strings for larger integers or exact decimal arithmetic. `1`, `1.0`, and `1e0`
are equal integers; booleans are never numbers. Negative zero equals zero.
Strings must contain Unicode scalar values; lengths count code points. Valid
escaped surrogate pairs are accepted, lone surrogates rejected. `enum`, `const`,
and `uniqueItems` compare JSON structure, independently of object key order.
Inherited JavaScript properties do not satisfy required or declared fields.

## Bounds and failures

| Limit | Default and hard maximum |
| --- | ---: |
| Schema byte budget | 65,536 |
| Output document and detached-value byte budget | 1,048,576 |
| Schema / output depth | 32 / 64 |
| Schema / output nodes | 4,096 / 65,536 |
| Validation work steps | 100,000 |

Hosts may lower each limit to a positive integer. Root depth is zero; keys and
values both count as nodes. Detached-value byte accounting is a conservative,
cross-language allocation bound: UTF-8 JSON-escaped strings, 24 bytes per number,
five per boolean/null, and two bytes plus two per child for each container.
Raw output UTF-8 bytes are also bounded before parsing. These are resource
limits, not performance or workload guarantees. Validation charges every schema
visit and structural equality visit; combinators and uniqueness cannot bypass
the work budget. No result is returned when the budget is exhausted.

Failures use `StructuredOutputError` in the existing error family. Diagnostics
contain only `path`, `keyword`, and `reason`; paths are bounded to 512 UTF-8 bytes
and undeclared candidate keys are not echoed. JSON parser exceptions containing
the rejected document are not attached to the error.

| Code | Meaning |
| --- | --- |
| `structured_output_schema_invalid` | Invalid identity, profile, limits, or schema |
| `structured_output_mode_unsupported` | Unrecognized mode; execution also uses this for unsupported native requirements |
| `structured_output_invalid_json` | Invalid or resource-exceeding document |
| `structured_output_schema_mismatch` | Parsed JSON fails a declared constraint |
| `structured_output_validation_limit_exceeded` | Validation exceeded its work budget; not a format-repair cause |

## Content identity

`schemaDigest` / `schema_digest` identifies schema content. `contractDigest` /
`contract_digest` additionally binds the schema ID, version, profile, digest,
mode, and all seven limits in the table's field order. Changing a mode or limit
changes the contract digest; changing content under the same ID/version changes
both. Reordering schema arrays remains significant; object key order does not.

`purra.json-identity/v1` hashes a prefix of that name followed by LF and an ASCII
typed encoding with SHA-256. Null is `n`, booleans `t`/`f`, numbers `d` followed by
16 lowercase hex digits of big-endian binary64 (zero normalized positive).
Strings are `s<byte-length>:<UTF-8-hex>`. Arrays are `a<count>:` followed by child
encodings. Objects are `o<count>:` followed by encoded key/value pairs sorted by
Unicode scalar order. Lengths/counts are unsigned decimal without leading zeros.
This avoids depending on language-specific float formatting, UTF-16 sorting, or
JavaScript numeric-property enumeration. It is not an authorization credential.

## Managed object tasks

Use a runner injected into a context or compaction factory to bind a task to
its Run. The object is returned only after normal model termination, strict
validation, usage settlement, private output persistence, and operation
settlement. Each attempt is a private, no-tool completion; structured streaming
is not supported by this entry point.

```python
from purra.api import AgentModelTask

result = await runner.complete_structured(
    messages, AgentModelTask(request.model), output=output,
    signal=signal, repair_attempts=1,
)
value, receipt = result.value, result.receipt
```

```typescript
const { value, receipt } = await runner.completeStructured(messages, {
  output, signal, repairAttempts: 1,
});
```

The default repair count is zero. A repair is a new invocation with a new ID,
a new reservation against the same Root budget, and the same bound model,
cancellation, deadline and evidence checks. Only invalid JSON or a schema
mismatch following a normal stop with known usage and successful failure
settlement permits repair. Rejected candidates and their private reasoning are
not replayed. The next request contains the original input, a short format
correction instruction and the output contract. Truncation, refusal/filtering,
transport errors, evidence changes, unknown usage and persistence failures do
not permit format repair. Persistence failure takes precedence over a format
failure. No result object is returned on failure.

`receipt.invocation_refs` / `receipt.invocationRefs` contains all attempted
invocations, including each budget, usage, dispatch/settlement state and failure
code. The aggregate usage remains unknown if any attempt has unknown usage.
Errors carry the same attempt list. A bound port is reported as
`persistence=bound` or `root_budget/rootBudget=bound`; these labels identify the
runtime bindings, not disk durability. A standalone runner without these ports
reports `none` and `not_bound` respectively.

Private invocation receipts use schema version 3. They include output contract
identity and optional native dialect in the request fingerprint, plus a
`structuredTask` link (`taskId`, one-based `attempt`, `previousInvocationId`).
These links identify the attempt chain without storing a rejected candidate in
an error or a public event. Receipt schema versions are independent of storage
and checkpoint versions. This development change does not migrate existing
storage; TypeScript state import rejects unsupported invocation receipt versions.

The complete local format instruction and schema are included before fitting
the invocation's generation budget. Native wire schemas are also included in
the estimated input cost, although they are sent in protocol format fields.
Schema IDs or root model options alone do not authorize an output mode.

## Native adapter dialects

`native_required` requires both a host-supplied capability snapshot declaring
`json_schema` and an adapter's pure `validate_output_contract` /
`validateOutputContract` preflight. Unsupported requests fail before Provider
I/O. Native responses still undergo the complete local profile validation.

| Adapter | Wire field | Dialect |
| --- | --- | --- |
| OpenAI Responses | `text.format` | `purra.openai-json-schema/v1` |
| OpenAI Chat Completions | `response_format.json_schema` | `purra.openai-json-schema/v1` |
| Anthropic Messages | `output_config.format` | `purra.anthropic-json-schema/v1` |

These adapter dialects intentionally accept a conservative, non-transforming
subset: explicit types, a single type or one type plus null, closed objects
with every property required, homogeneous arrays with an `items` schema,
primitive enums, descriptions and titles. They permit at most 100 properties
and 100 enum values in total, and eight levels below the root schema. Other
constraints (including numeric/string/array bounds, unions expressed with
`anyOf`/`oneOf`, `const` and uniqueness) fail admission instead of being dropped.
Empty objects may omit `required`. The complete Core schema limits still apply.
The adapter owns the native format field; Anthropic `outputConfig.format` /
`output_config.format` cannot override a structured task's output contract.

The limits define these adapter dialects, not the full feature set of every
model. Hosts must verify the selected model and endpoint. The protocol mappings
and subset choice follow the official [OpenAI supported-schema documentation](https://developers.openai.com/api/docs/guides/structured-outputs#supported-schemas)
and [Anthropic structured-output documentation](https://platform.claude.com/docs/en/build-with-claude/structured-outputs).
SDK transport tests prove wire mapping and local behavior; they do not prove a
live model or third-party compatible endpoint supports the format.

## Decoded values and tool argument contracts

`validate_value` / `validateValue` accepts an already-decoded object and applies
the same value, schema and resource checks as `parse`, returning a detached
immutable value. It cannot recover duplicate keys already discarded by an
upstream JSON decoder. Use `parse` at boundaries that still have raw JSON.
`json_identity_digest` / `jsonIdentityDigest` hashes arbitrary interoperable JSON
using `purra.json-identity/v1` and the default output byte/depth/node limits.

A tool may opt into this object profile with `argument_contract` /
`argumentContract`. It must be a local-mode contract whose schema exactly
matches the tool's input schema. Core validates every opted-in argument before
any handler starts; Python parses the original argument JSON without legacy
normalization. Tools without this option keep their existing admission rules.
`concurrency_safe` / `concurrencySafe` defaults to false and can only be declared
for read tools. The declaration alone does not enable parallel execution.
