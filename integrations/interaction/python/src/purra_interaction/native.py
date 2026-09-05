"""Checkpoint-based user interaction over the optional SQLite adapters."""
from dataclasses import replace
from datetime import datetime, timezone
from hashlib import sha256
import json

from purra.api import AgentCoreRunOptions, UserInputRequired
from purra.contracts import AgentMessage, ToolHandlerResult, ToolSchema, ToolPolicy
from purra.output import AgentOutputEventDraft, RunLifecycleOutputDraft
from purra.events import AgentEvent
from purra.ports import ToolRegistration
from purra.ports.run_lifecycle import RunCommit
from . import _questions, _json, _text


QUESTION_SCHEMA = {"type": "object", "properties": {"questions": {
    "type": "array", "minItems": 1, "maxItems": 3, "items": {"type": "object", "properties": {
        "id": {"type": "string"}, "prompt": {"type": "string"},
        "choices": {"type": "array", "items": {"type": "string"}, "maxItems": 8},
        "allowFreeform": {"type": "boolean"}}, "required": ["id", "prompt"], "additionalProperties": False}}},
    "required": ["questions"], "additionalProperties": False}


class SqliteClarification:
    def __init__(self, storage, *, options=None):
        self.storage, self.options = storage, options or AgentCoreRunOptions()
        self.registration = ToolRegistration(
            schema=ToolSchema("request_user_input", "Ask the user for missing task information. Execution waits for the answer.", QUESTION_SCHEMA),
            handler=self._tool, policy=ToolPolicy(mode="read", title="Ask the user"),
        )

    async def _tool(self, state, arguments, signal=None):
        return ToolHandlerResult(json.dumps({"purraInputRequest": _questions(arguments["questions"])}), effect_state="not_started")

    async def submit(self, core, request):
        async def checkpoint(saved):
            return await self._checkpoint(saved, request)
        return await core.submit(request, options=replace(self.options, checkpoint_handler=checkpoint))

    async def _checkpoint(self, checkpoint, request):
        pending = []
        question_calls = {call.id for message in checkpoint.messages for call in message.tool_calls
                          if call.name == "request_user_input"}
        for message in checkpoint.messages:
            if message.role.value != "tool" or message.tool_call_id not in question_calls: continue
            try: value = json.loads(message.content)
            except (TypeError, ValueError): continue
            if isinstance(value, dict) and "purraInputRequest" in value:
                pending.append((message.tool_call_id, _questions(value["purraInputRequest"])))
        async with self.storage.transaction() as adapters:
            self.storage._guard(checkpoint.run_id)
            tree = adapters.find_tree_run(checkpoint.run_id)
            root_id = tree.root_run_id if tree is not None else checkpoint.run_id
            rows = self.storage.extra.setdefault("clarifications", {})
            for call_id, questions in pending:
                identifier = sha256((checkpoint.run_id + "\0" + call_id).encode()).hexdigest()
                if identifier not in rows:
                    rows[identifier] = dict(id=identifier, runId=checkpoint.run_id, rootRunId=root_id, questions=questions,
                        state="waiting", revision=1, answers=None, request=request, answerKey=None)
                    await self._event(adapters, rows[identifier], "input.required")
            descendants = {run.run_id for run in await adapters.run_tree.list_descendants(checkpoint.run_id)} if tree is not None else set()
            waiting = [row for row in rows.values() if row["runId"] in {checkpoint.run_id, *descendants}
                       and row["state"] == "waiting" and adapters.get_run_info(row["runId"]).status.value == "running"]
        if waiting:
            raise UserInputRequired(checkpoint.run_id, waiting[0]["id"])
        return checkpoint

    async def get(self, identifier):
        async with self.storage.transaction() as adapters:
            row = self.storage.extra.get("clarifications", {})[identifier]
            result = {key: row[key] for key in ("id", "runId", "rootRunId", "questions", "state", "revision", "answers")}
            status = adapters.get_run_info(row["runId"]).status.value
            if status != "running": result["state"] = "completed" if status == "done" else status
            lease = self.storage._leases.get(row["runId"])
            if status == "running" and row["state"] == "ready" and lease is not None and lease.owner_id is not None and (lease.expires_at_ms or 0) > int(datetime.now().timestamp() * 1000):
                result["state"] = "running"
            return result

    async def list_pending(self):
        async with self.storage.transaction() as adapters:
            identifiers = tuple(row["id"] for row in self.storage.extra.get("clarifications", {}).values()
                if adapters.get_run_info(row["runId"]).status.value == "running")
        return tuple([await self.get(identifier) for identifier in identifiers])

    async def list_waiting(self):
        async with self.storage.transaction() as adapters:
            return tuple({key: row[key] for key in ("id", "runId", "questions", "state", "revision")}
                         for row in self.storage.extra.get("clarifications", {}).values() if row["state"] == "waiting"
                         and adapters.get_run_info(row["runId"]).status.value == "running")

    async def answer(self, identifier, *, revision, key, answers):
        _text(key); _json(answers)
        async with self.storage.transaction() as adapters:
            row = self.storage.extra["clarifications"][identifier]
            if row["answerKey"] == key:
                if row["answers"] != answers: raise ValueError("clarification_idempotency_conflict")
            else:
                if row["state"] != "waiting" or row["revision"] != revision: raise ValueError("clarification_revision_conflict")
                if not isinstance(answers, dict) or set(answers) != {q["id"] for q in row["questions"]}: raise ValueError("answers must cover the questions")
                for q in row["questions"]:
                    value = _text(answers[q["id"]], 8000)
                    if not q["allowFreeform"] and value not in q["choices"]: raise ValueError("invalid answer choice")
                run = await adapters.runs.get(row["runId"])
                if run.status.value != "running": raise ValueError("clarification_run_terminal")
                if run.deadline_at_ms is not None and run.deadline_at_ms <= int(datetime.now().timestamp() * 1000): raise ValueError("clarification_expired")
                checkpoint = run.execution_checkpoint
                if checkpoint is None: raise ValueError("clarification_checkpoint_missing")
                message = AgentMessage("user", "Answers to requested task information:\n" + _json(answers), attributes={"inputRequestId": identifier})
                await adapters.runs.commit(row["runId"], RunCommit(execution_checkpoint=replace(checkpoint, input_revision=checkpoint.input_revision + 1, messages=(*checkpoint.messages, message))))
                row.update(state="ready", revision=revision + 1, answers=answers, answerKey=key)
                await self._event(adapters, row, "input.answered")
        return await self.get(identifier)

    async def resume(self, core, identifier):
        async with self.storage.transaction() as adapters:
            row = self.storage.extra["clarifications"][identifier]
            if row["state"] != "ready": raise ValueError("clarification_not_ready")
            if any(r["rootRunId"] == row["rootRunId"] and r["state"] == "waiting"
                   and adapters.get_run_info(r["runId"]).status.value == "running"
                   for r in self.storage.extra["clarifications"].values()): raise ValueError("clarification_answers_incomplete")
            request, run_id = row["request"], row["rootRunId"]
        async def checkpoint(saved): return await self._checkpoint(saved, request)
        return await core.resume(run_id, request, options=replace(self.options, checkpoint_handler=checkpoint))

    async def cancel(self, identifier):
        async with self.storage.transaction() as adapters:
            row = self.storage.extra["clarifications"][identifier]
            run = await adapters.runs.get(row["rootRunId"])
            if run.status.value != "running": return False
            root_id = row["rootRunId"]
            tree = adapters.find_tree_run(root_id)
            run_ids = [root_id, *(r.run_id for r in await adapters.run_tree.list_descendants(root_id))] if tree else [root_id]
            if any((lease := self.storage._leases.get(rid)) is not None and lease.owner_id is not None for rid in run_ids):
                raise ValueError("cancel the active Run through its handle")
            for rid in run_ids:
                canonical = adapters.get_run_info(rid)
                if canonical is None or canonical.status.value != "running": continue
                await adapters.outputs.commit_run_lifecycle(rid, RunCommit(
                    terminal_status="canceled", events=(AgentEvent("run.canceled", {"reason": "user_input_canceled"}, rid),)),
                    RunLifecycleOutputDraft(source_event_key=f"input:{identifier}:{rid}:canceled", status="canceled",
                        payload={"status": "canceled", "reason": "user_input_canceled"}, occurred_at=datetime.now(timezone.utc)))
            if tree: await adapters.run_tree.cancel_subtree(root_id)
            return True

    @staticmethod
    async def _event(adapters, row, kind):
        await adapters.outputs.append_event(AgentOutputEventDraft(
            run_id=row["runId"], turn_id=None, output_stream_id=None, invocation_id=None,
            source_event_key=f"input:{row['id']}:{kind}", source="runtime", kind="runtime.event",
            channel="lifecycle", visibility="public", payload={"type": kind,
                **{k: row[k] for k in ("id", "runId", "questions", "state", "revision")}},
            occurred_at=datetime.now(timezone.utc),
        ))
