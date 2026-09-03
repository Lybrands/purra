"""Opt-in real-provider memory evaluation. Synthetic inputs; metadata-only report.

Uses installed purra-mem0, httpx (already required by Mem0), local Qdrant/SQLite,
and OpenAI-compatible chat completions / float embeddings. No product credentials
are discovered, no hidden retries, no paid call without --run.
"""

import argparse
import asyncio
from contextlib import redirect_stdout
from dataclasses import asdict
from datetime import datetime, timezone
import io
import hashlib
import json
import logging
import math
import os
import re
from pathlib import Path
import tempfile
import time
from urllib.parse import urlsplit


LIMITS = {"chat": 40, "embedding": 96, "input_chars": 1_000_000, "reserved_output_tokens": 40_960}
ANSWER_CAP = 512


class EvaluationError(Exception):
    pass


def validate_fixture(fixture):
    cases = fixture.get("cases")
    if fixture.get("version") != 1 or not isinstance(cases, list) or not 1 <= len(cases) <= 9:
        raise EvaluationError("invalid_evaluation_cases")
    ids = set()
    for case in cases:
        identity = case.get("id", "")
        if not isinstance(identity, str) or not re.fullmatch(r"[a-z0-9_]{1,64}", identity) or identity in ids:
            raise EvaluationError("invalid_evaluation_cases")
        ids.add(identity)
        action = case.get("action")
        if action not in {"extract", "review", "update", "revoke", "none", "other_scope"}:
            raise EvaluationError("invalid_evaluation_cases")
        required = ["query"] + (["incoming"] if action in ("extract", "review") else []) + (["seed"] if action != "extract" else []) + (["replacement"] if action == "update" else [])
        if any(not isinstance(case.get(k), str) or not 1 <= len(case[k]) <= 4000 for k in required):
            raise EvaluationError("invalid_evaluation_cases")
        if not isinstance(case.get("answers"), list) or len(case["answers"]) > 8 or any(not isinstance(a, str) or not 1 <= len(a) <= 1000 for a in case["answers"]):
            raise EvaluationError("invalid_evaluation_cases")
        if action == "review" and (not isinstance(case.get("review_kinds"), list) or not case["review_kinds"] or any(k not in ("independent", "duplicate", "supersede", "conflict", "uncertain") for k in case["review_kinds"])):
            raise EvaluationError("invalid_evaluation_cases")
        if "apply" in case and (action != "review" or case["apply"] not in ("duplicate", "supersede") or case.get("allow_empty")):
            raise EvaluationError("invalid_evaluation_cases")
        if "allow_empty" in case and type(case["allow_empty"]) is not bool:
            raise EvaluationError("invalid_evaluation_cases")
    if any(not isinstance(fixture.get(k), str) or not 1 <= len(fixture[k]) <= (4000 if k == "review_policy" else 8000) for k in ("answer_policy", "extraction_policy", "review_policy")):
        raise EvaluationError("invalid_evaluation_cases")
    if not isinstance(fixture.get("distractors"), list) or len(fixture["distractors"]) > 2 or any(not isinstance(t, str) or not 1 <= len(t) <= 4000 for t in fixture["distractors"]):
        raise EvaluationError("invalid_evaluation_cases")


def save_report(path, report):
    # An interrupted trial remains running, never a completed zero-cost result.
    summary = {}
    for call in report["calls"]:
        key = call["phase"] + "/" + call["kind"]
        row = summary.setdefault(key, {"calls": 0, "input_tokens": 0, "output_tokens": 0, "unreported_calls": 0, "duration_ms": 0})
        row["calls"] += 1
        for field in ("input_tokens", "output_tokens", "duration_ms"):
            row[field] += call.get(field) or 0
        row["unreported_calls"] += call["input_tokens"] is None or call["kind"] == "chat" and call["output_tokens"] is None
    report["usage_by_phase"] = summary
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(path)


def preflight(config):
    missing = []
    for kind in ("chat", "embedding"):
        value = config.get(kind, {})
        url = urlsplit(value.get("base_url", ""))
        local = url.hostname in ("localhost", "127.0.0.1", "::1")
        if (not url.hostname or url.scheme != "https" and not (local and url.scheme == "http")
                or url.username or url.password or url.query or url.fragment):
            raise EvaluationError("invalid_provider_url")
        if not isinstance(value.get("model"), str) or not value["model"].strip():
            raise EvaluationError("missing_model")
        key_env = value.get("key_env")
        if not isinstance(key_env, str) or not key_env.isidentifier():
            raise EvaluationError("invalid_key_env")
        if not os.environ.get(key_env):
            missing.append(key_env)
        if url.hostname.endswith(".example"):
            missing.append(kind + ".base_url")
    dims = config["embedding"].get("dimensions")
    if type(dims) is not int or not 1 <= dims <= 65_536:
        raise EvaluationError("invalid_embedding_dimensions")
    extra = config["chat"].get("extra", {})
    if not isinstance(extra, dict) or set(extra) - {"temperature", "top_p", "thinking", "reasoning_effort"}:
        raise EvaluationError("unsupported_chat_options")
    return sorted(set(missing))


def grade(text, rows, answers, supporting=None):
    """Exact synthetic facts and supporting IDs; never ask another model to judge."""
    try:
        result = json.loads(text)
        if (not isinstance(result, dict) or set(result) != {"answer", "evidence"}
                or result["answer"] is not None and not isinstance(result["answer"], str)
                or not isinstance(result["evidence"], list)
                or any(not isinstance(i, str) for i in result["evidence"])
                or len(set(result["evidence"])) != len(result["evidence"])):
            raise ValueError()
        evidence = set(result["evidence"])
        available = {row["id"] for row in rows}
        supporting = available if supporting is None else set(supporting)
        abstains = result["answer"] is None and not evidence
        correct = (isinstance(result["answer"], str) and result["answer"].strip() in answers
                   and bool(evidence) and evidence <= available and evidence <= supporting) if answers else abstains
        return {"valid": evidence <= available, "correct": bool(correct), "abstains": abstains}
    except (ValueError, TypeError):
        return {"valid": False, "correct": False, "abstains": False}


class Transport:
    """One ephemeral trial's hard ceiling, covering both memory and baseline calls."""
    def __init__(self, config, client, checkpoint=lambda: None):
        self.config, self.client = config, client
        self.calls = []
        self.phase, self.case = "setup", ""
        self.started = time.monotonic()
        self.chars = self.reserved = 0
        self.checkpoint = checkpoint

    async def post(self, kind, payload, chars, cap, signal=None):
        if signal is not None and signal.is_set():
            raise EvaluationError("cancelled")
        if (sum(c["kind"] == kind for c in self.calls) >= LIMITS[kind]
                or self.chars + chars > LIMITS["input_chars"]
                or self.reserved + cap > LIMITS["reserved_output_tokens"]
                or time.monotonic() - self.started >= 900):
            raise EvaluationError("trial_budget_exceeded")
        self.chars += chars
        self.reserved += cap
        entry = {"kind": kind, "case": self.case, "phase": self.phase, "input_chars": chars,
                 "reserved_output_tokens": cap, "input_tokens": None, "output_tokens": None, "outcome": "unknown"}
        self.calls.append(entry)  # Admission stays charged even after a transport error.
        self.checkpoint()
        cfg = self.config[kind]
        started = time.monotonic()
        cancel = None
        async def request():
            async with self.client.stream("POST", cfg["base_url"].rstrip("/") + (
                "/chat/completions" if kind == "chat" else "/embeddings"), json=payload,
                headers={"Authorization": "Bearer " + os.environ[cfg["key_env"]]}) as response:
                if response.status_code != 200:
                    raise EvaluationError("provider_http_" + str(response.status_code))
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > 8_000_000:
                        raise EvaluationError("provider_response_too_large")
                return json.loads(body)
        task = asyncio.create_task(request())
        try:
            cancel = None if signal is None else asyncio.create_task(signal.wait())
            done, _ = await asyncio.wait({task} if cancel is None else {task, cancel},
                                         timeout=min(60, max(0, 900 - (started - self.started))),
                                         return_when=asyncio.FIRST_COMPLETED)
            if cancel is not None and cancel in done:
                raise EvaluationError("cancelled")
            if task not in done:
                raise EvaluationError("transport_timeout")
            value = task.result()
            usage = value.get("usage") or {}
            for target, source in (("input_tokens", "prompt_tokens"), ("output_tokens", "completion_tokens")):
                count = usage.get(source)
                if count is not None and (type(count) is not int or count < 0):
                    raise EvaluationError("invalid_provider_usage")
                entry[target] = count
            entry["outcome"] = "returned"
            return value
        except EvaluationError as error:
            entry["outcome"] = str(error)
            raise
        except Exception:
            entry["outcome"] = "transport_error"
            raise EvaluationError("transport_error") from None
        finally:
            task.cancel()
            if cancel is not None:
                cancel.cancel()
            await asyncio.gather(task, *([] if cancel is None else [cancel]), return_exceptions=True)
            entry["duration_ms"] = round((time.monotonic() - started) * 1000)
            self.checkpoint()

    async def complete(self, messages, cap, signal=None):
        from purra.contracts import AgentMessage, ModelCompletion, ModelTokenUsage
        payload = {"model": self.config["chat"]["model"], "messages": [
            {"role": str(m.role), "content": m.content} for m in messages], "max_tokens": cap,
            "stream": False, **self.config["chat"].get("extra", {})}
        raw = await self.post("chat", payload, sum(len(m.content) for m in messages), cap, signal)
        try:
            choice, = raw["choices"]
            message = choice["message"]
            if choice["finish_reason"] != "stop" or message.get("tool_calls") or message.get("role") != "assistant" or not isinstance(message.get("content"), str):
                raise ValueError()
            usage = raw.get("usage") or {}
            tokens = None if usage.get("prompt_tokens") is None or usage.get("completion_tokens") is None else ModelTokenUsage(usage["prompt_tokens"], usage["completion_tokens"])
            if tokens is not None and tokens.output_tokens > cap:
                raise ValueError()
            return ModelCompletion(message=AgentMessage(role="assistant", content=message["content"]),
                model=self.config["chat"]["model"], finish_reason="stop", applied_output_limit=cap, usage=tokens)
        except (ValueError, KeyError, TypeError):
            raise EvaluationError("invalid_chat_response") from None

    async def embed(self, texts, signal):
        from purra_mem0 import EmbeddingResult
        raw = await self.post("embedding", {"model": self.config["embedding"]["model"], "input": list(texts), "encoding_format": "float"}, sum(map(len, texts)), 0, signal)
        try:
            rows = sorted(raw["data"], key=lambda row: row["index"])
            if any(type(row["index"]) is not int for row in rows) or [row["index"] for row in rows] != list(range(len(texts))):
                raise ValueError()
            vectors = [row["embedding"] for row in rows]
            if any(len(v) != self.config["embedding"]["dimensions"] or any(type(x) not in (int, float) or not math.isfinite(x) for x in v) for v in vectors):
                raise ValueError()
            return EmbeddingResult(vectors, (raw.get("usage") or {}).get("prompt_tokens"))
        except (KeyError, TypeError, ValueError):
            raise EvaluationError("invalid_embedding_response") from None


async def evaluate_case(case, fixture, root, transport, index):
    from purra_mem0 import Mem0Memory, MemoryContext, MemoryScope, MemorySource, MemoryRef, MemoryProviders, MemoryBudget
    from purra_mem0 import create_managed_client
    from purra.contracts import AgentMessage, ContextBudget
    transport.case, transport.phase = case["id"], "ingestion"
    checks, decisions, ledgers = {}, [], []
    dims = transport.config["embedding"]["dimensions"]
    providers = MemoryProviders(MemoryBudget("trial", 4, 20, 150_000, 8192, 2048), transport.complete, transport.embed)
    root.mkdir()

    def open_memory(other=False):
        client = create_managed_client(embedding_dims=dims, config={
            "vector_store": {"provider": "qdrant", "config": {"collection_name": "memory", "path": str(root / "vectors"), "embedding_model_dims": dims}},
            "history_db_path": str(root / "history.db"), "custom_instructions": fixture["extraction_policy"]})
        memory = Mem0Memory(client=client, scope=MemoryScope("synthetic-user", case["id"] + ("-other" if other else "")),
            journal_path=str(root / "journal.db"), allow_inference=True, providers=providers, timeout_seconds=65, max_results=16)
        return client, memory

    async def close(client, memory):
        await memory.drain()
        ledgers.append(asdict(memory.budget_usage()))
        memory.close()
        # Test-only teardown; the pinned SDK has no aggregate public close().
        client.sdk.db.close()
        client.sdk.vector_store.client.close()
        if client.sdk._entity_store is not None:
            client.sdk._entity_store.client.close()

    client, memory = open_memory()
    relevant = set()
    try:
        for i, text in enumerate(fixture["distractors"]):
            await memory.add(text, source=MemorySource("distractor-" + str(i), "1"), key="distractor-" + str(i))
        if "seed" in case:
            old = await memory.add(case["seed"], source=MemorySource("confirmed", "1"), key="seed")
            relevant.update(old.ids)
        if case["action"] in ("extract", "review"):
            pending = await memory.extract([{"role": "user", "content": case["incoming"]}], source=MemorySource("incoming", "1"), key="extract")
            checks["extraction_cardinality"] = len(pending.ids) == 1 or not pending.ids and case.get("allow_empty", False)
            checks["candidate_not_active"] = all([await memory.get(i) is None for i in pending.ids])
            if len(pending.ids) == 1:
                candidate = await memory.get(pending.ids[0], include_inactive=True)
                if case["action"] == "extract":
                    # This fixture explicitly authorizes a stable fact; no empty-search inference.
                    await memory.set_state(candidate.id, "active", version=candidate.version, key="accept")
                    relevant.add(candidate.id)
                else:
                    transport.phase = "review"
                    op = await memory.review(MemoryRef(candidate.id, candidate.version), key="review", limit=4, instructions=fixture["review_policy"])
                    decisions = [m.kind for m in op.review.matches if m.item.id in relevant]
                    checks["review_classification"] = len(decisions) == 1 and decisions[0] in case["review_kinds"]
                    if case.get("apply"):
                        proposal = op.review.proposal
                        checks["authorized_proposal"] = proposal is not None and proposal.kind == case["apply"]
                        if checks["authorized_proposal"]:
                            await memory.resolve(proposal, key="apply")
                            relevant = {proposal.keep}
                    checks["candidate_visibility"] = (await memory.get(candidate.id) is not None) == (case.get("apply") == "supersede" and checks.get("authorized_proposal", False))
        elif case["action"] == "update":
            await memory.update(old.ids[0], case["replacement"], source=MemorySource("confirmed", "2"), version=1, key="correct")
            checks["updated_version"] = (await memory.get(old.ids[0])).version == 2
        elif case["action"] == "revoke":
            await memory.revoke_source("confirmed", key="withdraw")
            checks["withdrawn_hidden"] = await memory.get(old.ids[0], include_inactive=True) is None
        active = {item.id for item in (await memory.list(limit=16)).items}
    finally:
        await close(client, memory)

    transport.phase = "recall"
    client, memory = open_memory(case["action"] == "other_scope")
    try:
        checks["restart_visibility"] = {item.id for item in (await memory.list(limit=16)).items} == (set() if case["action"] == "other_scope" else active)
        context = MemoryContext(memory=memory, query=lambda _: case["query"], limit=4, desired_tokens=2048)
        budget = ContextBudget(window_tokens=16_384, output_reserve_tokens=ANSWER_CAP, safety_reserve_tokens=0,
            runtime_reserve_tokens=0, provider_input_tokens=15_872, context_allocations={"memory": 2048})
        bundle = await context.build_context(None, budget)
        rows = [row for block in bundle.blocks for row in json.loads(block.content)]
        selected = {row["id"] for row in rows}
        checks["relevant_recall"] = bool(relevant & selected) if case["answers"] else not (relevant & selected) if case["action"] in ("revoke", "other_scope") else True
        usage = asdict(memory.budget_usage())
    finally:
        await close(client, memory)
    outcomes = {}
    # Alternate order to avoid systematically favoring one arm through latency/cache order.
    for arm in (["without_memory", "with_memory"] if index % 2 == 0 else ["with_memory", "without_memory"]):
        transport.phase = arm
        supplied = rows if arm == "with_memory" else []
        messages = (AgentMessage(role="system", content=fixture["answer_policy"]),
                    AgentMessage(role="user", content=json.dumps({"question": case["query"], "memories": supplied}, ensure_ascii=False)))
        result = await transport.complete(messages, ANSWER_CAP)
        outcomes[arm] = grade(result.message.content, supplied, case["answers"], relevant)
    checks["memory_answer"] = outcomes["with_memory"]["correct"]
    checks["baseline_no_fabrication"] = outcomes["without_memory"]["valid"] and outcomes["without_memory"]["abstains"]
    return {"id": case["id"], "passed": all(checks.values()), "checks": checks, "review_kinds": decisions,
            "selected_count": len(rows), "answers": outcomes, "memory_usage": usage, "journal_snapshots": ledgers}


async def run(config, fixture, report, output):
    import httpx
    logging.disable(logging.CRITICAL)  # Upstream errors can contain prompt text.
    os.environ["MEM0_TELEMETRY"] = "false"
    with tempfile.TemporaryDirectory(prefix="purra-memory-eval-") as folder:
        os.environ["MEM0_DIR"] = str(Path(folder) / "sdk")
        async with httpx.AsyncClient(timeout=60, follow_redirects=False, trust_env=False) as client:
            transport = Transport(config, client, lambda: save_report(output, report))
            report["calls"] = transport.calls
            report["status"] = "running"
            report["provider_mode"] = "real"
            try:
                for index, case in enumerate(fixture["cases"]):
                    print(json.dumps({"case": case["id"], "state": "running"}), flush=True)
                    # No SDK stdout/model content enters the report or console.
                    with redirect_stdout(io.StringIO()):
                        result = await evaluate_case(case, fixture, Path(folder) / case["id"], transport, index)
                    report["results"].append(result)
                    save_report(output, report)
                    print(json.dumps({"case": case["id"], "passed": result["passed"]}), flush=True)
                report["status"] = "passed" if all(r["passed"] for r in report["results"]) else "failed"
            except Exception as error:
                from purra_mem0 import MemoryError
                report["status"] = "failed"
                report["error"] = str(error) if isinstance(error, EvaluationError) else error.code if isinstance(error, MemoryError) else "evaluation_error"
                report["failed_case"] = transport.case
            finally:
                report["calls"] = transport.calls
                report["duration_ms"] = round((time.monotonic() - transport.started) * 1000)
                report["input_chars"] = transport.chars
                report["reserved_output_tokens"] = transport.reserved


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cases", type=Path, default=Path(__file__).resolve().parents[2] / "fixtures/evaluation.json")
    parser.add_argument("--run", action="store_true", help="Authorize the bounded paid trial; otherwise zero-network preflight")
    args = parser.parse_args()
    report = {"schema": 1, "runtime": "python", "started_at": datetime.now(timezone.utc).isoformat(),
              "status": "blocked", "provider_mode": "not_called", "limits": LIMITS, "answer_cap": ANSWER_CAP,
              "results": [], "calls": [], "monetary_cost": None, "scope": "component_pipeline", "execution": "bounded_standalone"}
    try:
        config = json.loads(args.config.read_text())
        report["configuration_sha256"] = hashlib.sha256(args.config.read_bytes()).hexdigest()
        fixture = json.loads(args.cases.read_text())
        validate_fixture(fixture)
        report["fixture_sha256"] = hashlib.sha256(args.cases.read_bytes()).hexdigest()
        report["planned_cases"] = len(fixture["cases"])
        missing = preflight(config)
        if missing:
            report["missing"] = missing
        else:
            report["models"] = {kind: config[kind]["model"] for kind in ("chat", "embedding")}
            report["status"] = "preflight_passed"
            if args.run:
                asyncio.run(run(config, fixture, report, args.output))
    except Exception as error:
        report["status"] = "failed" if report["provider_mode"] == "real" else "blocked"
        report["error"] = str(error) if isinstance(error, EvaluationError) else "invalid_evaluation_configuration"
    save_report(args.output, report)
    print(json.dumps({"status": report["status"], "completed": len(report["results"]), "calls": len(report["calls"])}))
    return 0 if report["status"] in ("passed", "preflight_passed") else 2


if __name__ == "__main__":
    raise SystemExit(main())
