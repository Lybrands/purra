"""Offline checks of the evaluator, not semantic quality evidence."""
import asyncio
from contextlib import asynccontextmanager
import json
from pathlib import Path
import runpy
from types import SimpleNamespace

import pytest

EVAL = runpy.run_path(str(Path(__file__).parents[1] / "scripts/evaluate.py"))
grade, preflight, Transport = (EVAL[k] for k in ("grade", "preflight", "Transport"))


@pytest.mark.parametrize("body,correct", [
    ({"answer": "新值", "evidence": ["fact"]}, True),
    ({"answer": "旧值", "evidence": ["fact"]}, False),
    ({"answer": "新值", "evidence": []}, False),
    ({"answer": "新值", "evidence": ["distractor"]}, False),
    ({"answer": "新值", "evidence": ["foreign"]}, False),
    ({"answer": "新值", "evidence": ["fact", "fact"]}, False),
    ({"answer": {"nested": "新值"}, "evidence": []}, False),
    ({"answer": "新值", "evidence": ["fact"], "extra": True}, False),
])
def test_answer_requires_correct_value_and_actual_support(body, correct):
    assert grade(json.dumps(body), [{"id": "fact"}, {"id": "distractor"}], ["新值"], {"fact"})["correct"] is correct


def test_abstention_is_not_a_personalization_success():
    body = '{"answer":null,"evidence":[]}'
    assert grade(body, [], ["fact"]) == {"valid": True, "correct": False, "abstains": True}
    assert grade(body, [], []) == {"valid": True, "correct": True, "abstains": True}
    assert not grade("not json", [], [])["valid"]


def test_fixture_rejects_empty_success_and_path_traversal():
    fixture = json.loads((Path(__file__).parents[2] / "fixtures/evaluation.json").read_text())
    EVAL["validate_fixture"](fixture)
    fixture["cases"][0]["id"] = "../../outside"
    with pytest.raises(Exception, match="invalid_evaluation_cases"):
        EVAL["validate_fixture"](fixture)
    fixture["cases"] = []
    with pytest.raises(Exception, match="invalid_evaluation_cases"):
        EVAL["validate_fixture"](fixture)


def test_interrupted_trial_reports_unknown_usage_and_cannot_appear_passed(tmp_path):
    report = {"status": "running", "calls": [{"phase": "review", "kind": "chat", "input_tokens": None, "output_tokens": None}]}
    output = tmp_path / "report.json"
    EVAL["save_report"](output, report)
    saved = json.loads(output.read_text())
    assert saved["status"] == "running" and saved["usage_by_phase"]["review/chat"]["unreported_calls"] == 1
    assert not output.with_suffix(".json.tmp").exists()


def config():
    return {"chat": {"base_url": "https://chat.example", "model": "test", "key_env": "PURRA_EVAL_TEST_KEY"},
            "embedding": {"base_url": "https://embedding.example", "model": "test", "key_env": "PURRA_EVAL_TEST_KEY", "dimensions": 2}}


def test_preflight_requires_explicit_credentials_and_cannot_override_limits(monkeypatch):
    monkeypatch.delenv("PURRA_EVAL_TEST_KEY", raising=False)
    value = config()
    assert preflight(value) == ["PURRA_EVAL_TEST_KEY", "chat.base_url", "embedding.base_url"]
    value["chat"]["extra"] = {"max_tokens": 1_000_000}
    with pytest.raises(Exception, match="unsupported_chat_options"):
        preflight(value)
    value = config()
    value["chat"]["base_url"] = "https://secret:password@chat.example"
    with pytest.raises(Exception, match="invalid_provider_url"):
        preflight(value)


class Http:
    def __init__(self, status=200, block=False):
        self.status, self.block, self.requests = status, block, []
        self.started = asyncio.Event()

    @asynccontextmanager
    async def stream(self, method, url, **options):
        self.requests.append((method, url, options))
        self.started.set()
        if self.block:
            await asyncio.Event().wait()
        async def body():
            yield json.dumps({"choices": [{"finish_reason": "stop", "message": {"role": "assistant", "content": "private model text"}}],
                              "usage": {"prompt_tokens": 7, "completion_tokens": 3}}).encode()
        yield SimpleNamespace(status_code=self.status, aiter_bytes=body)


@pytest.mark.asyncio
async def test_transport_charges_exact_cap_and_rejects_before_network(monkeypatch):
    from purra.contracts import AgentMessage
    monkeypatch.setenv("PURRA_EVAL_TEST_KEY", "private-key")
    http = Http()
    transport = Transport(config(), http)
    result = await transport.complete([AgentMessage(role="user", content="私有😀")], 32)
    assert result.applied_output_limit == 32 and result.usage.output_tokens == 3
    assert http.requests[0][2]["json"]["max_tokens"] == 32
    assert transport.calls[0]["input_chars"] == 3
    assert all(secret not in json.dumps(transport.calls) for secret in ("private-key", "private model text", "私有"))
    transport.reserved = EVAL["LIMITS"]["reserved_output_tokens"]
    with pytest.raises(Exception, match="trial_budget_exceeded"):
        await transport.complete([AgentMessage(role="user", content="x")], 32)
    assert len(http.requests) == 1


@pytest.mark.asyncio
async def test_http_errors_are_redacted_and_do_not_retry(monkeypatch):
    monkeypatch.setenv("PURRA_EVAL_TEST_KEY", "private-key")
    http = Http(status=429)
    transport = Transport(config(), http)
    with pytest.raises(Exception, match="provider_http_429"):
        await transport.post("chat", {}, 1, 32)
    assert len(http.requests) == 1 and transport.reserved == 32
    assert transport.calls[0]["input_tokens"] is None


@pytest.mark.asyncio
async def test_cancellation_stops_waiting_without_reporting_refund(monkeypatch):
    monkeypatch.setenv("PURRA_EVAL_TEST_KEY", "private-key")
    http, signal = Http(block=True), asyncio.Event()
    transport = Transport(config(), http)
    task = asyncio.create_task(transport.post("chat", {}, 1, 32, signal))
    await http.started.wait()
    signal.set()
    with pytest.raises(Exception, match="cancelled"):
        await task
    assert transport.reserved == 32 and transport.calls[0]["input_tokens"] is None
