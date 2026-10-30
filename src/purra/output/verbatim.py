"""Prepare bounded journal frames for exact, atomically committed public prose."""
from collections.abc import Sequence
from datetime import datetime

from purra.contracts import ModelFinishReason, OutputTextReference
from purra.errors import ContractViolationError
from purra.output.contracts import (
    AgentOutputEvent, AgentOutputEventDraft, AgentOutputIntent, OutputChannel, OutputEventKind,
    OutputSource, OutputStreamSpec, OutputVisibility,
)


def _entries(event):
    if event.kind is OutputEventKind.PROVIDER_DELTA_BATCH:
        return tuple((entry.get('kind'), entry.get('payload', {}))
                     for entry in event.payload.get('entries', ()))
    return ((event.kind.value, event.payload),)


def _text_frames(text: str) -> tuple[str, ...]:
    frames, frame, size = [], [], 0
    for character in text:
        width = len(character.encode('utf-8'))
        if size + width > 16384:
            frames.append(''.join(frame))
            frame, size = [], 0
        frame.append(character)
        size += width
    if frame:
        frames.append(''.join(frame))
    return tuple(frames)


def verbatim_publication_drafts(
    reference: OutputTextReference,
    *,
    spec: OutputStreamSpec,
    status: str,
    finish_reason: ModelFinishReason | str | None,
    events: Sequence[AgentOutputEvent],
    text: str,
    occurred_at: datetime,
) -> tuple[AgentOutputEventDraft, ...]:
    """No byte rewriting, model call or heuristic choice of a private stream.

    Hosts append all returned drafts in the same transaction as Run completion.
    Frame size bounds transport work, never the total answer length.
    """
    if not isinstance(reference, OutputTextReference):
        raise ContractViolationError('verbatim publication requires an output reference')
    expected = OutputTextReference.for_text(spec.run_id, spec.output_stream_id, spec.invocation_id, text)
    if reference != expected or spec.intent is not AgentOutputIntent.STRUCTURED_PRIVATE or spec.output_protocol is not None:
        raise ContractViolationError('verbatim publication identity does not match its answer')
    if status != 'committed' or str(finish_reason) != 'stop':
        raise ContractViolationError('verbatim publication requires a complete committed stream')
    parts = []
    for event in events:
        if (event.output_stream_id != spec.output_stream_id or event.run_id != spec.run_id
                or event.invocation_id != spec.invocation_id):
            raise ContractViolationError('verbatim publication received foreign output')
        entries = _entries(event)
        if any(kind == OutputEventKind.PROVIDER_TOOL_CALL_DELTA.value for kind, _ in entries):
            raise ContractViolationError('verbatim publication rejects tool arguments')
        if (event.source is OutputSource.PROVIDER and event.channel is OutputChannel.DIAGNOSTIC
                and event.visibility is OutputVisibility.PRIVATE):
            parts.extend(str(payload.get('delta') or '') for kind, payload in entries
                         if kind == OutputEventKind.PROVIDER_CONTENT_DELTA.value)
    if ''.join(parts) != text or not text.strip():
        raise ContractViolationError('verbatim publication content does not match Provider bytes')
    frames = _text_frames(text)
    drafts = tuple(AgentOutputEventDraft.public_text(
        run_id=spec.run_id, turn_id=spec.turn_id, output_stream_id=spec.output_stream_id,
        invocation_id=spec.invocation_id,
        source_event_key=f'provider:{spec.invocation_id}:verbatim:{index}',
        source=OutputSource.PROVIDER, channel=OutputChannel.FINAL, delta=part,
        occurred_at=occurred_at) for index, part in enumerate(frames))
    return (*drafts, AgentOutputEventDraft(
        run_id=spec.run_id, turn_id=spec.turn_id, output_stream_id=spec.output_stream_id,
        invocation_id=spec.invocation_id, source_event_key=f'stream:{spec.output_stream_id}:verbatim:committed',
        source=OutputSource.RUNTIME, kind=OutputEventKind.STREAM_COMMITTED,
        channel=OutputChannel.FINAL, visibility=OutputVisibility.PUBLIC,
        payload={'finishReason': 'stop', 'contentDigest': reference.content_digest}, occurred_at=occurred_at))
