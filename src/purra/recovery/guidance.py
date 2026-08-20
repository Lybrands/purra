"""Provider-neutral guidance shared by Core model recovery paths."""

EMPTY_RESPONSE_RETRY_GUIDANCE = (
    "Your preceding model round ended after internal reasoning without any "
    "official response content. Continue the same request now and return the "
    "complete response through the requested output protocol. Do not return "
    "reasoning alone."
)

DECLINED_TOOL_GUIDANCE = (
    "The preceding tool result is authoritative: the user rejected the "
    "approval, so the operation was not executed and must not be retried in "
    "this run. Respond in the user's language with a plain-language summary "
    "of that outcome. Do not call or imitate a tool, and do not emit tool-call "
    "markup as text."
)
TEXTUAL_TOOL_CALL_RETRY_GUIDANCE = (
    "Your preceding response imitated a tool call using plain-text markup. "
    "That text was not executable and was not shown to the user. The rejected "
    "approval remains authoritative and the operation was not executed. Give "
    "one concise plain-language response in the user's language. Do not call, "
    "retry, or imitate any tool."
)
FAILED_TOOL_OUTPUT_RETRY_GUIDANCE = (
    "The preceding recovery response imitated a tool call or dumped tool "
    "arguments as plain text. It was withheld because plain text cannot "
    "execute the failed tool. Give one concise plain-language summary in the "
    "user's language. State that the tool did not complete and do not emit, "
    "retry, or imitate a tool call or its JSON arguments."
)
MISSING_REQUIRED_TOOL_CALL_RETRY_GUIDANCE = (
    "The preceding model round did not return the structured tool call required "
    "by the current approved plan step. That text was discarded and no tool was "
    "executed. Retry this step now by returning exactly one valid structured call "
    "to one of the tools currently exposed by the host. Do not describe, imitate, "
    "or wrap the call in ordinary text."
)
MISSING_REQUIRED_TOOL_CALL_REPLAN_GUIDANCE = (
    "The model still omitted the required structured tool call after one retry. "
    "Treat the current tool step as failed and revise the remaining plan from "
    "the evidence already collected. Do not claim that the omitted tool ran, "
    "invent its result, or repeat an equivalent completed read step."
)
UNAUTHORIZED_TOOL_REPLAN_GUIDANCE = (
    "The current plan step could not be completed because the model repeatedly "
    "selected a tool outside the host-authorized set. Replan this unexecuted "
    "step without weakening tool authorization."
)
TOOL_INPUT_RETRY_GUIDANCE = (
    "The preceding tool call was rejected because its input did not satisfy "
    "the tool contract. The tool did not complete and produced no successful "
    "evidence. Correct the arguments from the structured error result and retry "
    "the same currently exposed tool exactly once. Return a real structured "
    "tool call, not a textual imitation, and do not invent a successful result."
)
MALFORMED_TOOL_CALL_RETRY_GUIDANCE = (
    "The preceding structured tool-call envelope was malformed and was rejected "
    "before any tool handler ran. Retry the current step exactly once using the "
    "provider's native structured tool-call protocol. Include one stable call id, "
    "one currently exposed tool name, and one complete JSON object for arguments. "
    "Do not emit XML-like tool markup or an argument dump as ordinary text."
)
FINAL_PUBLIC_PRESENTATION_GUIDANCE = (
    "The preceding assistant content came from a private tool-capable model "
    "round and was not shown to the user. Return the final user-facing answer "
    "now in this tool-free response. Preserve supported facts, do not mention "
    "this handoff, and do not imitate or request a tool call."
)

__all__ = [name for name in globals() if name.endswith("_GUIDANCE")]
