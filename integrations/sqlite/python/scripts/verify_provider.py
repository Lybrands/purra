"""Opt-in DeepSeek tool roundtrip with SQLite persistence; synthetic data only.

Reads one explicitly selected model from a PurrTypos settings database read-only.
No credentials, prompts, or model content are included in the timing report.
Requires current Core, SQLite, OpenAI integration, and repository root on PYTHONPATH.
"""
import argparse
import asyncio
from dataclasses import replace
import json
from pathlib import Path
import sqlite3
import tempfile
import time
from types import SimpleNamespace

from openai import AsyncOpenAI
from purra.api import AgentCore, AgentCoreRunOptions, AgentPreset
from purra.contracts import (AgentMessage, AgentRunRequest, DomainContext, ModelRequest,
    RuntimeLimits, RunStatus, ToolEffectState, ToolHandlerResult, ToolPolicy, ToolSchema)
from purra.model_protocol import generic_capability_snapshot
from purra.ports import ToolRegistration
from purra.tools import InMemoryToolCatalog
from purra_openai.chat import OpenAIChatCompletionsGateway
from purra_sqlite import SqliteAgentAdapters
from scripts.provider_liveness_sampler import SamplingModelGateway
from verify_load import draft


class DeepSeekClient:
    """Test-only wire mapping; not an unmodified OpenAI integration acceptance."""
    def __init__(self, client):
        self.client = client
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self.create))

    def with_options(self, **options):
        return DeepSeekClient(self.client.with_options(**options))

    async def create(self, **params):
        params["max_tokens"] = params.pop("max_completion_tokens")
        params.pop("store", None)
        params.pop("reasoning_effort", None)
        params["extra_body"] = {"thinking": {"type": "disabled"}}
        params["messages"] = [{**message, "role": "system"} if message["role"] == "developer"
            else message for message in params["messages"]]
        return await self.client.chat.completions.create(**params)


class EagerHistoryAdapters(SqliteAgentAdapters):
    def _transaction(self, **kwargs):
        kwargs["lazy_journal"] = False
        return super()._transaction(**kwargs)


async def scenario(config, mode, path, history):
    storage_type = EagerHistoryAdapters if mode == "eager-reference" else SqliteAgentAdapters
    storage = storage_type(path, scope="provider-closeout")
    client = AsyncOpenAI(api_key=config["apiKey"], base_url=config["baseUrl"], max_retries=0, timeout=45)
    samples, tool_calls = [], []
    async def lookup(state, arguments, signal=None):
        tool_calls.append(dict(arguments))
        return ToolHandlerResult('{"receipt":"PURRA_SQLITE_VERIFIED"}', effect_state=ToolEffectState.NOT_STARTED)
    catalog = InMemoryToolCatalog((ToolRegistration(
        schema=ToolSchema(name="lookup_receipt", description="Read the local verification receipt.",
            parameters={"type": "object", "properties": {}, "additionalProperties": False}),
        handler=lookup, policy=ToolPolicy(mode="read", title="Read receipt")),))
    original_begin = storage.outputs.begin_run_lifecycle
    timing = {}
    async def begin(*args, **kwargs):
        result = await original_begin(*args, **kwargs)
        async with storage.transaction() as memory:
            for index in range(history):
                await memory.outputs.append_event(draft(result[0].run_id, f"history:{index}"))
        timing["start"] = time.perf_counter()
        return result
    storage.outputs.begin_run_lifecycle = begin
    agent = AgentCore(
        model_gateway=SamplingModelGateway(OpenAIChatCompletionsGateway(DeepSeekClient(client)), samples.append),
        run_repository=storage.runs, output_repository=storage.outputs, output_publisher=storage.publisher,
        tool_idempotency_gateway=storage.idempotency,
        preset=AgentPreset(id="sqlite-provider-closeout", revision="1", tool_catalog=catalog,
            runtime_limits=RuntimeLimits(max_run_generation_tokens=2048, max_model_invocation_attempts=4)))
    try:
        handle = await agent.submit(AgentRunRequest(
            messages=(AgentMessage("user", "Call lookup_receipt once, then reply with only the receipt value returned by the tool."),),
            model=ModelRequest(provider="openai", model=config["name"],
                capability_snapshot=replace(generic_capability_snapshot(), profile_id="deepseek:verification", max_generation_tokens=512),
                max_generation_tokens=512),
            domain_context=DomainContext(namespace="sqlite.verification"), context_window=65536, tools_enabled=True),
            options=AgentCoreRunOptions(reasoning_mode="disabled"))
        result = await asyncio.wait_for(handle.wait(), timeout=180)
        elapsed = (time.perf_counter() - timing["start"]) * 1000
        if result.status is not RunStatus.DONE:
            print(json.dumps({"failed_samples": [s.to_mapping() for s in samples]}), flush=True)
            detail = str(result.error).replace(config["apiKey"], "[redacted]")
            raise RuntimeError(f"Agent terminal status: {result.status.value}; invocation errors: {[s.error_code for s in samples]}; detail: {detail}")
        assert len(tool_calls) == 1 and result.final_response.strip() == "PURRA_SQLITE_VERIFIED"
        before = await storage.outputs.list_events(result.run_id, after_sequence=history, limit=200)
        assert before and any(event.visibility == "public" for event in before)
        await agent.close()
        storage.close()
        storage = storage_type(path, scope="provider-closeout")
        after = await storage.outputs.list_events(result.run_id, after_sequence=history, limit=200)
        saved = await storage.runs.get(result.run_id)
        assert before == after and saved.status is RunStatus.DONE
        return {"mode": mode, "history_events": history, "elapsed_excluding_seed_ms": round(elapsed, 3),
            "provider_total_ms": sum(s.total_duration_ms for s in samples), "tool_calls": len(tool_calls),
            "model_calls": len(samples), "reopened_output_events": len(after), "persisted_replay": "passed",
            "samples": [s.to_mapping() for s in samples]}
    finally:
        await agent.close()
        await client.close()
        storage.close()


async def main(args):
    with sqlite3.connect(args.config_db.resolve().as_uri() + "?mode=ro", uri=True) as db:
        configs = json.loads(db.execute("SELECT value FROM settings WHERE key='ai_model_configs'").fetchone()[0])
    matches = [c for c in configs if c.get("id") == args.config_id]
    if len(matches) != 1 or not matches[0].get("apiKey"):
        raise ValueError("Selected model configuration is missing or has no credential")
    config = matches[0]
    results = []
    with tempfile.TemporaryDirectory(prefix="purra-provider-") as directory:
        for index, mode in enumerate(("eager-reference", "current")):
            result = await scenario(config, mode, str(Path(directory) / f"{index}.db"), args.history)
            results.append(result)
            print(json.dumps({key: value for key, value in result.items() if key != "samples"}), flush=True)
    args.output.write_text(json.dumps({"model": config["name"], "transport": "test-only DeepSeek Chat mapping",
        "comparison": "current code with eager hydration vs deferred hydration; not a historical release baseline",
        "results": results}, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-db", type=Path, required=True)
    parser.add_argument("--config-id", required=True)
    parser.add_argument("--history", type=int, default=2000)
    parser.add_argument("--output", type=Path, required=True)
    asyncio.run(main(parser.parse_args()))
