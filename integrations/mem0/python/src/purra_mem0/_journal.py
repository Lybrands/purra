"""Control metadata only. Mem0 remains the content and vector store.

The partial unique index fences concurrent writers, including other processes.
An interrupted operation keeps its fence until verified; it is never retried
just because a lease or timeout expired.
"""

import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone


class MemoryError(RuntimeError):
    """A stable, redacted failure; inspect the operation separately after writes."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class Journal:
    def __init__(self, path: str, scope: str):
        if not isinstance(path, str) or not path.strip() or path == ":memory:":
            raise ValueError("journal_path must be a persistent SQLite path")
        self.scope = scope
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS purra_mem0_info (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS purra_mem0_epochs (scope TEXT PRIMARY KEY, epoch INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS purra_mem0_ops (
                scope TEXT NOT NULL, key TEXT NOT NULL, fingerprint TEXT NOT NULL,
                state TEXT NOT NULL, plan TEXT NOT NULL, ids TEXT, PRIMARY KEY(scope,key));
            CREATE UNIQUE INDEX IF NOT EXISTS purra_mem0_writer ON purra_mem0_ops(scope)
                WHERE state IN ('running','unknown');
            CREATE TABLE IF NOT EXISTS purra_mem0_items (
                scope TEXT NOT NULL, id TEXT NOT NULL, record TEXT NOT NULL, PRIMARY KEY(scope,id));
            CREATE TABLE IF NOT EXISTS purra_mem0_budgets (
                scope TEXT NOT NULL, key TEXT NOT NULL, limits TEXT NOT NULL, PRIMARY KEY(scope,key));
            CREATE TABLE IF NOT EXISTS purra_mem0_calls (
                scope TEXT NOT NULL, id TEXT NOT NULL, budget TEXT NOT NULL, operation TEXT,
                kind TEXT NOT NULL, state TEXT NOT NULL, input_chars INTEGER NOT NULL,
                reserved_output INTEGER NOT NULL, input_tokens INTEGER, generation_tokens INTEGER,
                PRIMARY KEY(scope,id));
            CREATE INDEX IF NOT EXISTS purra_mem0_calls_budget ON purra_mem0_calls(scope,budget);
            CREATE INDEX IF NOT EXISTS purra_mem0_calls_operation ON purra_mem0_calls(scope,operation);
            CREATE TABLE IF NOT EXISTS purra_mem0_revocations (
                scope TEXT NOT NULL, source TEXT NOT NULL, revision TEXT NOT NULL,
                PRIMARY KEY(scope,source,revision));
        """)
        with self.transaction():
            self.db.execute("INSERT OR IGNORE INTO purra_mem0_info VALUES ('store',?)", (uuid.uuid4().hex,))
            self.db.execute("INSERT OR IGNORE INTO purra_mem0_epochs VALUES (?,0)", (scope,))
            self.store = self.db.execute("SELECT value FROM purra_mem0_info WHERE key='store'").fetchone()[0]

    @contextmanager
    def transaction(self):
        with self.lock:
            self.db.execute("BEGIN IMMEDIATE")
            try:
                yield
                self.db.execute("COMMIT")
            except BaseException:
                self.db.execute("ROLLBACK")
                raise

    def operation(self, key):
        with self.lock:
            row = self.db.execute("SELECT * FROM purra_mem0_ops WHERE scope=? AND key=?", (self.scope, key)).fetchone()
            return None if row is None else {
                **dict(row), "plan": json.loads(row["plan"]),
                "ids": None if row["ids"] is None else json.loads(row["ids"]),
            }

    def begin(self, key, fingerprint, plan):
        with self.transaction():
            previous = self.operation(key)
            if previous:
                if previous["fingerprint"] != fingerprint:
                    raise MemoryError("memory_idempotency_conflict")
                return previous
            try:
                self.db.execute("INSERT INTO purra_mem0_ops VALUES (?,?,?,'running',?,NULL)",
                                (self.scope, key, fingerprint, json.dumps(plan)))
            except sqlite3.IntegrityError:
                raise MemoryError("memory_write_busy") from None
        return None

    def save_plan(self, key, plan):
        with self.lock:
            self.db.execute("UPDATE purra_mem0_ops SET plan=? WHERE scope=? AND key=?", (json.dumps(plan), self.scope, key))

    def save_ids(self, key, ids):
        with self.lock:
            self.db.execute("UPDATE purra_mem0_ops SET ids=? WHERE scope=? AND key=?", (json.dumps(ids), self.scope, key))

    def budget(self, key, limits):
        with self.transaction():
            row = self.db.execute("SELECT limits FROM purra_mem0_budgets WHERE scope=? AND key=?", (self.scope, key)).fetchone()
            if row is not None and json.loads(row[0]) != limits:
                raise MemoryError("memory_budget_conflict")
            self.db.execute("INSERT OR IGNORE INTO purra_mem0_budgets VALUES (?,?,?)", (self.scope, key, json.dumps(limits)))

    def usage(self, *, budget=None, operation=None):
        column, value = ("budget", budget) if budget is not None else ("operation", operation)
        with self.lock:
            row = self.db.execute(f"""SELECT
                COALESCE(SUM(kind='llm'),0) AS llm_calls,
                COALESCE(SUM(kind='embedding'),0) AS embedding_calls,
                COALESCE(SUM(input_chars),0) AS input_chars,
                COALESCE(SUM(reserved_output),0) AS reserved_output_tokens,
                COALESCE(SUM(input_tokens),0) AS reported_input_tokens,
                COALESCE(SUM(generation_tokens),0) AS reported_output_tokens,
                COALESCE(SUM(input_tokens IS NULL OR generation_tokens IS NULL),0) AS unreported_calls,
                COALESCE(SUM(state='started'),0) AS unsettled_calls
                FROM purra_mem0_calls WHERE scope=? AND {column}=?""", (self.scope, value)).fetchone()
            return dict(row)

    def admit(self, budget, operation, kind, input_chars, reserved_output):
        with self.transaction():
            op = None if operation is None else self.operation(operation)
            if op is not None and op["plan"]["kind"] == "review":
                self.assert_snapshot(op["plan"])
            if op is not None and op["plan"]["kind"] != "delete" and op["plan"].get("meta"):
                self.assert_source(op["plan"]["meta"])
            limits = json.loads(self.db.execute("SELECT limits FROM purra_mem0_budgets WHERE scope=? AND key=?", (self.scope, budget)).fetchone()[0])
            used = self.usage(budget=budget)
            if (used[kind + "_calls"] + 1 > limits["max_" + kind + "_calls"]
                    or used["input_chars"] + input_chars > limits["max_input_chars"]
                    or used["reserved_output_tokens"] + reserved_output > limits["max_output_tokens"]):
                raise MemoryError("memory_budget_exceeded")
            call_id = uuid.uuid4().hex
            self.db.execute("INSERT INTO purra_mem0_calls VALUES (?,?,?,?,?,'started',?,?,NULL,NULL)",
                            (self.scope, call_id, budget, operation, kind, input_chars, reserved_output))
            return call_id

    def settle(self, call_id, state, input_tokens=None, generation_tokens=None):
        with self.lock:
            self.db.execute("UPDATE purra_mem0_calls SET state=?,input_tokens=?,generation_tokens=? WHERE scope=? AND id=?",
                            (state, input_tokens, generation_tokens, self.scope, call_id))

    def provider_error(self, key, code):
        with self.transaction():
            op = self.operation(key)
            if op is not None:
                op["plan"]["provider_error"] = code
                self.save_plan(key, op["plan"])

    def verify_providers(self, key):
        with self.transaction():
            op = self.operation(key)
            op["plan"]["providers_verified"] = True
            self.save_plan(key, op["plan"])

    def fail(self, key, dispatched):
        with self.lock:
            self.db.execute("UPDATE purra_mem0_ops SET state=? WHERE scope=? AND key=?",
                            ("unknown" if dispatched else "failed", self.scope, key))

    def discard(self, key):
        with self.lock:
            self.db.execute("UPDATE purra_mem0_ops SET state='discarded' WHERE scope=? AND key=?", (self.scope, key))

    def revoked(self, source, revision):
        with self.lock:
            return self.db.execute("SELECT 1 FROM purra_mem0_revocations WHERE scope=? AND source=? AND revision IN ('',?)",
                                   (self.scope, source, revision)).fetchone() is not None

    def assert_source(self, meta):
        if self.revoked(meta["purra_source"], meta["purra_revision"]):
            raise MemoryError("memory_source_revoked")

    def revoke_source(self, key, fingerprint, source, revision):
        # This control-only transaction must remain available while an SDK writer
        # is running/unknown. It never takes or releases that writer's fence.
        with self.transaction():
            previous = self.operation(key)
            if previous:
                if previous["fingerprint"] != fingerprint:
                    raise MemoryError("memory_idempotency_conflict")
                return
            plan = {"kind": "revoke_source", "target": None, "meta": None,
                    "source": source, "revision": revision}
            self.db.execute("INSERT INTO purra_mem0_ops VALUES (?,?,?,'complete',?,'[]')",
                            (self.scope, key, fingerprint, json.dumps(plan)))
            if not self.revoked(source, revision or ""):
                self.db.execute("INSERT INTO purra_mem0_revocations VALUES (?,?,?)", (self.scope, source, revision or ""))
                self.db.execute("UPDATE purra_mem0_epochs SET epoch=epoch+1 WHERE scope=?", (self.scope,))

    def item(self, item_id):
        with self.lock:
            row = self.db.execute("SELECT record FROM purra_mem0_items WHERE scope=? AND id=?", (self.scope, item_id)).fetchone()
            return None if row is None else json.loads(row[0])

    @staticmethod
    def view(row):
        # Control changes need no re-embedding; SDK metadata stays a verified snapshot.
        meta = row["meta"]
        return {"version": meta["purra_version"], "state": meta["purra_state"],
                "metadata": meta["purra_metadata"], "reason": meta["purra_reason"],
                "created_at": meta["purra_created"], "updated_at": meta["purra_updated"],
                **row.get("view", {})}

    def resolve(self, key, fingerprint, plan, epoch):
        resolution = plan["resolution"]
        changes = {ref["id"]: {
            "state": "active" if ref["id"] == resolution["keep"] else "disabled",
            "reason": None if ref["id"] == resolution["keep"] else resolution["kind"],
            "resolution": key,
        } for ref in resolution["items"]}
        self.control(key, fingerprint, plan, epoch, resolution["items"], changes)

    def control(self, key, fingerprint, plan, epoch, refs, changes):
        """Commit verified record views and their receipt in one transaction."""
        with self.transaction():
            previous = self.operation(key)
            if previous:
                if previous["fingerprint"] != fingerprint:
                    raise MemoryError("memory_idempotency_conflict")
                return
            if self.db.execute("SELECT 1 FROM purra_mem0_ops WHERE scope=? AND state IN ('running','unknown')", (self.scope,)).fetchone():
                raise MemoryError("memory_write_busy")
            if self.epoch() != epoch:
                raise MemoryError("memory_context_stale")
            if plan.get("review_key") is not None:
                self.assert_snapshot(self.review_plan(plan["review_key"]))
            records = []
            now = datetime.now(timezone.utc)
            for ref in refs:
                row = self.item(ref["id"])
                if row is None or row["deleted"]:
                    raise MemoryError("memory_not_found")
                if self.view(row)["version"] != ref["version"]:
                    raise MemoryError("memory_version_conflict")
                self.assert_source(row["meta"])
                expires = row["meta"]["purra_expires"]
                change = changes.get(ref["id"], {})
                if (plan["kind"] in {"resolve", "link"} or change.get("state") == "active") and expires is not None and datetime.fromisoformat(expires.replace("Z", "+00:00")) <= now:
                    raise MemoryError("memory_context_stale")
                if change:
                    row["view"] = {**self.view(row), **change, "version": ref["version"] + 1,
                                   "updated_at": now.isoformat(timespec="milliseconds").replace("+00:00", "Z")}
                records.append(row)
            self.db.execute("INSERT INTO purra_mem0_ops VALUES (?,?,?,'complete',?,?)",
                            (self.scope, key, fingerprint, json.dumps(plan), json.dumps([r["id"] for r in records])))
            for row in records:
                self.db.execute("UPDATE purra_mem0_items SET record=? WHERE scope=? AND id=?", (json.dumps(row), self.scope, row["id"]))
            self.db.execute("UPDATE purra_mem0_epochs SET epoch=epoch+1 WHERE scope=?", (self.scope,))

    def links(self, item_id, after, limit):
        with self.lock:
            return [(r[0], json.loads(r[1])["link"]) for r in self.db.execute(
                """SELECT key,plan FROM purra_mem0_ops WHERE scope=? AND key>? AND state='complete'
                   AND json_extract(plan,'$.kind')='link'
                   AND (json_extract(plan,'$.link.from.id')=? OR json_extract(plan,'$.link.to.id')=?)
                   ORDER BY key LIMIT ?""", (self.scope, after or "", item_id, item_id, limit))]

    def assert_snapshot(self, plan):
        if plan.get("review_epoch") != self.epoch() or not plan.get("review_refs"):
            raise MemoryError("memory_context_stale")
        now = datetime.now(timezone.utc)
        for ref in plan["review_refs"]:
            row = self.item(ref["id"])
            if row is None or row["deleted"] or self.view(row)["version"] != ref["version"]:
                raise MemoryError("memory_context_stale")
            self.assert_source(row["meta"])
            expires = row["meta"]["purra_expires"]
            if expires is not None and datetime.fromisoformat(expires.replace("Z", "+00:00")) <= now:
                raise MemoryError("memory_context_stale")

    def review_plan(self, key):
        op = self.operation(key)
        if op is None or op["state"] != "complete" or op["plan"]["kind"] != "review" or not op["plan"].get("review"):
            raise MemoryError("memory_review_unavailable")
        return op["plan"]

    def finish_review(self, key, plan):
        with self.transaction():
            if self.operation(key)["state"] != "running":
                raise MemoryError("memory_operation_unresolved")
            self.assert_snapshot(plan)
            self.save_plan(key, plan)
            self.db.execute("UPDATE purra_mem0_ops SET state='complete',ids='[]' WHERE scope=? AND key=?", (self.scope, key))

    def items(self, state, after, limit):
        with self.lock:
            return [json.loads(r[0]) for r in self.db.execute(
                """SELECT record FROM purra_mem0_items AS item WHERE scope=? AND id>?
                   AND json_extract(record,'$.deleted')=0
                   AND (? IS NULL OR COALESCE(json_extract(record,'$.view.state'),json_extract(record,'$.meta.purra_state'))=?)
                   AND NOT EXISTS (SELECT 1 FROM purra_mem0_revocations AS r
                       WHERE r.scope=item.scope AND r.source=json_extract(item.record,'$.meta.purra_source')
                       AND r.revision IN ('',json_extract(item.record,'$.meta.purra_revision')))
                   AND (? IS NULL OR ? != 'active' OR json_extract(record,'$.meta.purra_expires') IS NULL
                        OR json_extract(record,'$.meta.purra_expires')>?)
                   ORDER BY id LIMIT ?""",
                (self.scope, after or "", state, state, state, state, datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"), limit))]

    def writing(self, item_id):
        with self.lock:
            return any(json.loads(r[0]).get("target") == item_id for r in self.db.execute(
                "SELECT plan FROM purra_mem0_ops WHERE scope=? AND state IN ('running','unknown')", (self.scope,)))

    def commit(self, key, records):
        with self.transaction():
            for record in records:
                self.db.execute("INSERT OR REPLACE INTO purra_mem0_items VALUES (?,?,?)",
                                (self.scope, record["id"], json.dumps(record)))
            self.db.execute("UPDATE purra_mem0_ops SET state='complete' WHERE scope=? AND key=?", (self.scope, key))
            if records:
                self.db.execute("UPDATE purra_mem0_epochs SET epoch=epoch+1 WHERE scope=?", (self.scope,))

    def epoch(self):
        with self.lock:
            return self.db.execute("SELECT epoch FROM purra_mem0_epochs WHERE scope=?", (self.scope,)).fetchone()[0]

    def close(self):
        with self.lock:
            self.db.close()
