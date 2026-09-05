"""Control signal emitted only after a durable user-input checkpoint commits."""
from dataclasses import replace
import json


def is_input_checkpoint_update(current, updated):
    if updated.input_revision != current.input_revision + 1:
        return False
    expected_checkpoint = replace(current, input_revision=updated.input_revision, messages=updated.messages)
    if len(updated.messages) == len(current.messages) + 1:
        message = updated.messages[-1]
        return (updated.messages[:-1] == current.messages and message.role.value == "user"
                and bool(message.attributes.get("inputRequestId")) and expected_checkpoint == updated)
    if len(updated.messages) != len(current.messages):
        return False
    calls = {call.id for message in current.messages for call in message.tool_calls if call.name == "delegateToAgents"}
    from purra.evidence import RunEvidenceStore
    evidence = RunEvidenceStore.from_checkpoint_mapping(current.evidence_state)
    changed = False
    for before, after in zip(current.messages, updated.messages):
        if before == after: continue
        if before.role.value != "tool" or before.tool_call_id not in calls or replace(before, content=after.content) != after:
            return False
        try:
            old, new = json.loads(before.content), json.loads(after.content)
            expected = set(old["pendingRunIds"]) | {row["runId"] for row in old["results"]}
            if (old["state"] != "pending" or not old["pendingRunIds"] or new["state"] not in {"ready", "blocked"}
                or new["pendingRunIds"] or {row["runId"] for row in new["results"]} != expected
                or len(new["results"]) != len(expected)
                or any(row not in new["results"] for row in old["results"])):
                return False
            evidence.resolve_child_runs(before.tool_call_id, after.content)
        except (ValueError, TypeError, KeyError):
            return False
        changed = True
    return changed and replace(expected_checkpoint, evidence_state=evidence.checkpoint_mapping()) == updated


class UserInputRequired(Exception):
    code = "user_input_required"

    def __init__(self, run_id: str, request_id: str):
        super().__init__("The Run is waiting for user input")
        self.run_id, self.request_id = run_id, request_id
