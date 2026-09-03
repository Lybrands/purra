"""Durable pre-execution clarification, with an explicit host continuation."""

import json
import math
import sqlite3
import time
from uuid import uuid4

from purra.task_admission import TaskAdmissionDecision, ExecutionMode


def _text(value, limit=512):
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise ValueError("expected bounded non-empty text")
    return value


def _json(value):
    def normalize(v):
        if v is None or type(v) in (str, bool):
            return v
        if type(v) in (int, float):
            if not math.isfinite(v) or abs(v) > 2**53 - 1:
                raise ValueError("checkpoint numbers must be finite and safely representable")
            return int(v) if v == int(v) else v
        if isinstance(v, (tuple, list)):
            return [normalize(i) for i in v]
        if isinstance(v, dict) and all(isinstance(k, str) for k in v):
            return {k: normalize(i) for k, i in v.items()}
        raise ValueError("checkpoint must be JSON data")
    result = json.dumps(normalize(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if len(result.encode()) > 131072:
        raise ValueError("clarification exceeds 128 KiB")
    return result


def _questions(values):
    if not isinstance(values, (list, tuple)) or not 1 <= len(values) <= 3:
        raise ValueError("clarification requires 1 to 3 questions")
    result = []
    for row in values:
        if not isinstance(row, dict) or set(row) - {"id", "prompt", "choices", "allowFreeform"}:
            raise ValueError("invalid question")
        choices = row.get("choices", [])
        if not isinstance(choices, (list, tuple)) or len(choices) > 8 or len(set(choices)) != len(choices):
            raise ValueError("invalid choices")
        choices = [_text(v, 128) for v in choices]
        free = row.get("allowFreeform", True)
        if type(free) is not bool or (not free and not choices):
            raise ValueError("question needs a valid answer mode")
        result.append(dict(id=_text(row.get("id"), 128), prompt=_text(row.get("prompt"), 8000), choices=choices, allowFreeform=free))
    if len({q["id"] for q in result}) != len(result):
        raise ValueError("question ids must be unique")
    return result


class ClarificationStore:
    """SQLite adapter scoped to a host-authorized owner/tenant namespace.

    This database stores clarification checkpoints, not the Core Run journal.
    Never put credentials or executable objects in a checkpoint.
    """

    def __init__(self, path: str, *, scope: str):
        self.scope = _text(scope)
        self._db = sqlite3.connect(path, timeout=5)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute("""CREATE TABLE IF NOT EXISTS purra_clarifications (
            scope TEXT NOT NULL, id TEXT NOT NULL, request_key TEXT NOT NULL,
            request_json TEXT NOT NULL, state TEXT NOT NULL, revision INTEGER NOT NULL,
            answer_key TEXT, answers_json TEXT, resume_token TEXT, run_id TEXT,
            PRIMARY KEY(scope, id), UNIQUE(scope, request_key))""")
        self._db.commit()

    def ask(self, *, key, questions, checkpoint, source_run_id=None, expires_at_ms=None):
        _text(key)
        if source_run_id is not None:
            _text(source_run_id)
        if expires_at_ms is not None and (type(expires_at_ms) is not int or not 0 < expires_at_ms <= 2**53 - 1):
            raise ValueError("expires_at_ms must be a positive integer")
        body = _json(dict(questions=_questions(questions), checkpoint=checkpoint,
                          sourceRunId=source_run_id, expiresAtMs=expires_at_ms))
        with self._db:
            self._db.execute("BEGIN IMMEDIATE")
            existing = self._db.execute("SELECT id,request_json FROM purra_clarifications WHERE scope=? AND request_key=?", (self.scope, key)).fetchone()
            if existing:
                if json.loads(existing[1]) != json.loads(body):
                    raise ValueError("clarification_idempotency_conflict")
                identifier = existing[0]
            else:
                identifier = uuid4().hex
                self._db.execute("INSERT INTO purra_clarifications(scope,id,request_key,request_json,state,revision) VALUES(?,?,?,?,'waiting',1)", (self.scope, identifier, key, body))
        return self.get(identifier)

    def get(self, identifier):
        row = self._db.execute("SELECT request_json,state,revision,answers_json,run_id,resume_token FROM purra_clarifications WHERE scope=? AND id=?", (self.scope, _text(identifier))).fetchone()
        if row is None:
            raise KeyError("clarification_not_found")
        body = json.loads(row[0])
        return dict(id=identifier, **body, state=row[1], revision=row[2],
                    answers=None if row[3] is None else json.loads(row[3]), runId=row[4], resumeToken=row[5])

    def answer(self, identifier, *, revision, key, answers):
        _text(key)
        encoded = _json(answers)
        with self._db:
            self._db.execute("BEGIN IMMEDIATE")
            saved = self.get(identifier)
            previous = self._db.execute("SELECT answer_key,answers_json FROM purra_clarifications WHERE scope=? AND id=?", (self.scope, identifier)).fetchone()
            if previous[0] == key:
                if json.loads(previous[1]) != json.loads(encoded):
                    raise ValueError("clarification_idempotency_conflict")
                return saved
            self._require(saved, "waiting", revision)
            if not isinstance(answers, dict) or set(answers) != {q["id"] for q in saved["questions"]}:
                raise ValueError("answers must cover exactly the requested questions")
            for q in saved["questions"]:
                answer = _text(answers[q["id"]], 8000)
                if not q["allowFreeform"] and answer not in q["choices"]:
                    raise ValueError("answer is not one of the offered choices")
            self._db.execute("UPDATE purra_clarifications SET state='ready',revision=revision+1,answer_key=?,answers_json=? WHERE scope=? AND id=?", (key, encoded, self.scope, identifier))
        return self.get(identifier)

    def claim(self, identifier, *, revision):
        with self._db:
            self._db.execute("BEGIN IMMEDIATE")
            self._require(self.get(identifier), "ready", revision)
            token = uuid4().hex
            self._db.execute("UPDATE purra_clarifications SET state='resuming',revision=revision+1,resume_token=? WHERE scope=? AND id=?", (token, self.scope, identifier))
        return token, self.get(identifier)

    def reconcile(self, identifier, *, token, run_id=None, not_submitted=False):
        """Resolve an interrupted submission from authoritative host evidence.

        Supply its existing Run id, or assert it was never submitted. This is
        never called automatically after an ambiguous transport/process error.
        """
        if type(not_submitted) is not bool or (run_id is None) == (not_submitted is False):
            raise ValueError("provide an existing Run id or not_submitted=True")
        if run_id is not None:
            _text(run_id)
        with self._db:
            self._db.execute("BEGIN IMMEDIATE")
            row = self._db.execute("SELECT state,resume_token,run_id FROM purra_clarifications WHERE scope=? AND id=?", (self.scope, identifier)).fetchone()
            if row is None or row[1] != token:
                raise ValueError("clarification_claim_conflict")
            if row[0] == "resumed" and run_id == row[2]:
                return self.get(identifier)
            if row[0] != "resuming":
                raise ValueError("clarification_state_conflict")
            self._db.execute("UPDATE purra_clarifications SET state=?,revision=revision+1,run_id=? WHERE scope=? AND id=?", ("ready" if not_submitted else "resumed", run_id, self.scope, identifier))
        return self.get(identifier)

    def cancel(self, identifier, *, revision):
        with self._db:
            self._db.execute("BEGIN IMMEDIATE")
            saved = self.get(identifier)
            if saved["revision"] != revision or saved["state"] not in ("waiting", "ready"):
                raise ValueError("clarification_state_conflict")
            self._db.execute("UPDATE purra_clarifications SET state='canceled',revision=revision+1 WHERE scope=? AND id=?", (self.scope, identifier))
        return self.get(identifier)

    @staticmethod
    def public(saved):
        return {k: v for k, v in saved.items() if k not in ("checkpoint", "resumeToken")}

    @staticmethod
    def _require(saved, state, revision):
        if type(revision) is not int or saved["state"] != state or saved["revision"] != revision:
            raise ValueError("clarification_state_conflict")
        if saved["expiresAtMs"] is not None and saved["expiresAtMs"] <= int(time.time() * 1000):
            raise ValueError("clarification_expired")

    def close(self):
        self._db.close()


class ClarificationWorkflow:
    def __init__(self, store: ClarificationStore):
        self.store = store

    def admission(self, **request):
        saved = self.store.ask(**request)
        return TaskAdmissionDecision(mode=ExecutionMode.CLARIFY, reason_code="user_input_required",
            message="\n".join(q["prompt"] for q in saved["questions"]), metadata={"inputRequestId": saved["id"]})

    async def resume(self, identifier, *, revision, submit):
        """submit(snapshot, continuation_key) must return the new Run's id.

        The host reconstructs authorized options and revalidates permissions.
        A failed/ambiguous submit retains the claim for explicit reconciliation.
        """
        token, saved = self.store.claim(identifier, revision=revision)
        run_id = await submit(saved, "clarification:" + identifier)
        return self.store.reconcile(identifier, token=token, run_id=run_id)


__all__ = ["ClarificationStore", "ClarificationWorkflow"]

from .native import SqliteClarification
__all__ += ["SqliteClarification"]
