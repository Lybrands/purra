# Static image input (1.1.0 development)

The first M01 slice accepts host-provided inline PNG, JPEG and WebP content and
returns text. It does not select a model, fetch URLs, read files, decode pixels,
generate images, or make application decisions. The host must check that the
bytes actually represent an acceptable static image, authorize disclosure to the
provider, and apply its own byte/count limits before constructing the message.

## Public content contract

Python:

```python
from purra.contracts import AgentMessage
from purra.media import static_image_content

message = AgentMessage("user", static_image_content("Describe the image", [{
    "mediaType": "image/png",
    "dataBase64": encoded_image,
    "inputTokens": image_token_allowance,
}]))
```

TypeScript:

```ts
import { staticImageContent } from "purra";

const message = { role: "user" as const, content: staticImageContent("Describe the image", [{
  mediaType: "image/png",
  dataBase64: encodedImage,
  inputTokens: imageTokenAllowance,
}]) };
```

Both helpers produce the same JSON envelope: `type: "purra.static-images/v1"`,
`text`, and a nonempty `images` array. Image fields are exactly `mediaType`,
`dataBase64`, and `inputTokens`. Canonical padded base64 is required; links and
file references are not accepted. Core validates the reserved envelope at message
admission and accepts it only on the user role. Arbitrary existing JSON content
and text messages keep their existing behavior.

The message owns an immutable copy. Images remain in host-side message/state
serialization; hosts must account for that storage cost and data sensitivity.
Changing bytes changes the JSON input; resource paths that could later point to
different bytes are not used. This introduces no database-specific contract.

## Budget and adapter boundary

`inputTokens` is a positive JavaScript-safe integer supplied by the host for the
selected model; the sum must also be safe. It is an estimate/reservation, not
provider usage or a verified cost ceiling. The estimator adds that allowance to
the conservative text and message-structure estimate, excluding base64 transport
bytes. Canonical messages and wire payloads retain the complete bytes. Encoding
length does not reveal image token cost; the host must provide a suitable
allowance for the selected model and image settings. Underestimating it can
still produce a provider context error. This is not an accurate image tokenizer;
actual provider usage continues through the ordinary settlement path.

The optional OpenAI **Chat Completions** adapters accept this envelope when the
host explicitly sets Python `image_input=True` or TypeScript `imageInput: true`.
The selected model capability snapshot must also declare Python
`protocol.image_input="supported"` or TypeScript `protocol.imageInput: "supported"`.
The transport option enables adapter handling; the snapshot declares the model's
capability. Both are host declarations, not live capability probes.

Core rejects image invocations when support is `unknown`, absent, or `unavailable`,
including private planning tasks and streaming. It checks the frozen invocation
snapshot rather than guessing from the provider name. Route requirements can
use Python `image_input_required=True` or TypeScript `imageInputRequired: true`
to exclude unsuitable authorized candidates before creating a host.

Explicit image capability declarations participate in preset/route identity.
Changing them prevents silently rebinding a saved Run. For old text-only
snapshots, absent and explicit `unknown` have the same identity. The Python
storage codec reads the old record without the field and omits `unknown` when
writing; recognized new declarations round-trip. Older SDKs are not promised to
read records containing newly declared capabilities.
Canonical text/image content is mapped to text blocks and base64 data URLs per
the [official image input protocol](https://developers.openai.com/api/docs/guides/images-vision).
The budget metadata is never sent as prompt content. Provider detail selection
currently uses its default; size/detail-aware estimation is not implemented.

Custom gateways can consume the public envelope directly. Other bundled
protocol adapters remain text-only in this slice and reject these contents.
Use the existing versioned host component bindings when changing adapter policy
for durable runs; arbitrary gateway configuration is not automatically hashed.

## Verified scope

The reference Planners carry images as media in both initial and revision
requests. Their textual payload contains image indexes and associated user text,
never base64 bytes. All input images are retained in that planning projection;
the normal model budget boundary may reject a projection that is too large.
`latest_user_text()` in Python returns only the image message's text. These
projections do not change canonical runtime messages.

Deterministic tests cover dual-SDK validation, image budget inclusion, initial
Planner media projection, complete-turn trimming, Python checkpoint codec
round-trip, and TypeScript repository export/import at a pending tool boundary.
The current image is preserved even when it exceeds the trimming budget;
overflow is reported instead of slicing its bytes. TypeScript custom compression
cannot replace the current image with text. Historical turns remain subject to
the host's ordinary context policy and may be dropped as complete turns.

The TypeScript reactive public Agent path and official SDK Chat request shape
in completion and streaming are tested with deterministic gateways/mocked HTTP.
Disabled adapter input and invalid authority roles fail before dispatch. This
is not automatic model capability discovery: hosts remain responsible for
accurate declarations, and custom gateways implement the media transport.

The TypeScript recovery API is also exercised with an image-bearing checkpoint
under an expired lease in an in-memory repository. It preserves image content,
avoids requerying the resolved context/retrieval source, and rejects an excessive
image allowance before model dispatch. The checkpoint is materialized by the
test; this is not evidence of an OS-process crash or a database reopen.

Python's public `resume()` API is tested using a constructed canonical image
checkpoint, a second AgentCore instance, the existing in-memory repository and
a test execution-lease port. It preserves the Run ID and image content, avoids
rebuilding context, releases the lease, and returns a text result. An excessive
image allowance fails before dispatch with the specific context-overflow reason
instead of a generic runtime exception. This fixture does not simulate a real
process crash or exercise a production lease implementation.

Real-provider smoke tests also passed for `glm-5.3-flash` on the configured
`open.bigmodel.cn` Chat-compatible endpoint at `reasoning_effort: low` with
thinking enabled. Both SDKs identified the colors in two counterfactual synthetic
PNG fixtures: one through the public Agent streaming path and one through the
adapter completion path. The host transport translated `developer` to `system`,
`max_completion_tokens` to `max_tokens`, omitted `store`, and enabled thinking.
This is a compatible-host combination, not native OpenAI service acceptance.
The TypeScript adapter permits missing interim finish reasons while still
requiring a terminal reason before accepting a completed stream.

This narrow smoke does not certify OCR, complex scenes, multiple-image reasoning,
JPEG/WebP on the real service, process-crash recovery, or downstream application
acceptance. M01 as a whole remains open; this slice does not certify those
combinations. See the [GLM image-input contract](https://docs.z.ai/guides/vlm/glm-5.3-flash)
for the model's documented input support; documentation is not a substitute for
the explicit acceptance scopes above.
