from __future__ import annotations

import importlib
from datetime import datetime, timezone

import pytest

from purra.contracts import RunStatus


def _contracts():
    try:
        return importlib.import_module("purra.output.contracts")
    except ModuleNotFoundError as error:
        pytest.fail(f"canonical output contracts are missing: {error}")


def _stream_spec(**overrides):
    contracts = _contracts()
    values = {
        "output_stream_id": "output-1",
        "run_id": "run-1",
        "turn_id": "turn-1",
        "invocation_id": "invocation-1",
        "intent": contracts.AgentOutputIntent.FINAL_PUBLIC,
        "commit_mode": contracts.OutputCommitMode.LIVE,
    }
    values.update(overrides)
    return contracts.OutputStreamSpec(**values)


def test_private_intent_cannot_use_live_commit_mode():
    contracts = _contracts()

    with pytest.raises(ValueError, match="private output intent cannot be live"):
        _stream_spec(
            intent=contracts.AgentOutputIntent.STRUCTURED_PRIVATE,
            commit_mode=contracts.OutputCommitMode.LIVE,
        )

def test_public_intent_requires_live_commit_mode():
    contracts = _contracts()

    with pytest.raises(ValueError, match="public output intent must be live"):
        _stream_spec(
            intent=contracts.AgentOutputIntent.FINAL_PUBLIC,
            commit_mode=contracts.OutputCommitMode.GATED,
        )


def test_reasoning_private_requires_private_commit_mode():
    contracts = _contracts()

    with pytest.raises(
        ValueError,
        match="reasoning-private output requires private commit mode",
    ):
        _stream_spec(
            intent=contracts.AgentOutputIntent.REASONING_PRIVATE,
            commit_mode=contracts.OutputCommitMode.GATED,
        )


def test_public_text_draft_requires_provider_source():
    contracts = _contracts()

    with pytest.raises(ValueError, match="public text requires provider source"):
        contracts.AgentOutputEventDraft.public_text(
            run_id="run-1",
            turn_id="turn-1",
            output_stream_id="output-1",
            invocation_id="invocation-1",
            source_event_key="runtime:1",
            source=contracts.OutputSource.RUNTIME,
            channel=contracts.OutputChannel.FINAL,
            delta="不应公开",
            occurred_at=datetime.now(timezone.utc),
        )


def test_public_text_draft_accepts_provider_delta_without_rewriting_it():
    contracts = _contracts()

    draft = contracts.AgentOutputEventDraft.public_text(
        run_id="run-1",
        turn_id="turn-1",
        output_stream_id="output-1",
        invocation_id="invocation-1",
        source_event_key="provider:1",
        source=contracts.OutputSource.PROVIDER,
        channel=contracts.OutputChannel.COMMENTARY,
        delta="甲乙",
        occurred_at=datetime(2026, 8, 11, tzinfo=timezone.utc),
    )

    assert draft.payload == {"delta": "甲乙"}
    assert draft.visibility is contracts.OutputVisibility.PUBLIC
    assert draft.kind is contracts.OutputEventKind.PROVIDER_CONTENT_DELTA


def test_event_payload_is_detached_and_immutable():
    contracts = _contracts()
    payload = {"delta": "原文", "nested": {"attempt": 1}}

    draft = contracts.AgentOutputEventDraft(
        run_id="run-1",
        turn_id="turn-1",
        output_stream_id=None,
        invocation_id=None,
        source_event_key="runtime:1",
        source=contracts.OutputSource.RUNTIME,
        kind=contracts.OutputEventKind.RUN_LIFECYCLE,
        channel=contracts.OutputChannel.LIFECYCLE,
        visibility=contracts.OutputVisibility.PUBLIC,
        payload=payload,
        occurred_at=datetime.now(timezone.utc),
    )
    payload["nested"]["attempt"] = 2

    assert draft.payload["nested"]["attempt"] == 1
    with pytest.raises(TypeError):
        draft.payload["delta"] = "篡改"


def test_canonical_event_sequence_starts_at_one():
    contracts = _contracts()

    with pytest.raises(ValueError, match="sequence must be positive"):
        contracts.AgentOutputEvent(
            event_id="event-1",
            output_stream_id=None,
            run_id="run-1",
            turn_id="turn-1",
            invocation_id=None,
            sequence=0,
            source=contracts.OutputSource.RUNTIME,
            kind=contracts.OutputEventKind.RUN_LIFECYCLE,
            channel=contracts.OutputChannel.LIFECYCLE,
            visibility=contracts.OutputVisibility.PUBLIC,
            payload={"status": "running"},
            occurred_at=datetime.now(timezone.utc),
            emitted_at=datetime.now(timezone.utc),
        )


def test_output_timestamps_must_include_timezone():
    contracts = _contracts()

    with pytest.raises(ValueError, match="occurred_at must be timezone-aware"):
        contracts.AgentOutputEventDraft(
            run_id="run-1",
            turn_id=None,
            output_stream_id=None,
            invocation_id=None,
            source_event_key="runtime:1",
            source=contracts.OutputSource.RUNTIME,
            kind=contracts.OutputEventKind.RUN_LIFECYCLE,
            channel=contracts.OutputChannel.LIFECYCLE,
            visibility=contracts.OutputVisibility.PUBLIC,
            payload={"status": "running"},
            occurred_at=datetime(2026, 8, 11),
        )


def test_public_stream_commit_can_be_runtime_metadata_without_public_text():
    contracts = _contracts()

    draft = contracts.AgentOutputEventDraft(
        run_id="run-1",
        turn_id="turn-1",
        output_stream_id="output-1",
        invocation_id="invocation-1",
        source_event_key="output-1:committed",
        source=contracts.OutputSource.RUNTIME,
        kind=contracts.OutputEventKind.STREAM_COMMITTED,
        channel=contracts.OutputChannel.FINAL,
        visibility=contracts.OutputVisibility.PUBLIC,
        payload={"finishReason": "stop"},
        occurred_at=datetime.now(timezone.utc),
    )

    assert "delta" not in draft.payload


def test_run_lifecycle_draft_requires_stable_source_event_key():
    contracts = _contracts()

    draft = contracts.RunLifecycleOutputDraft(
        source_event_key="run:run-1:done",
        status=RunStatus.DONE,
        payload={"status": "done"},
        occurred_at=datetime.now(timezone.utc),
    )

    assert draft.source_event_key == "run:run-1:done"
