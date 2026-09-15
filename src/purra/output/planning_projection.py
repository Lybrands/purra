"""Project planning previews without owning persistence or publication."""
from datetime import datetime, timezone

from purra.errors import ContractViolationError
from purra.output.contracts import (
    AgentOutputEventDraft, OutputChannel, OutputEventKind, OutputSource,
    OutputStreamSpec, OutputVisibility,
)
from purra.planning_stream import PlanningProgress, PlanningTextDeltaParser, PLANNING_STREAM_SCHEMA


class PlanningOutputProjection:
    def __init__(self):
        self._parsers: dict[str, PlanningTextDeltaParser] = {}

    def open(self, spec: OutputStreamSpec) -> None:
        if spec.output_protocol == PLANNING_STREAM_SCHEMA:
            self._parsers.setdefault(spec.output_stream_id, PlanningTextDeltaParser())

    def discard(self, stream_id: str) -> None:
        self._parsers.pop(stream_id, None)

    @staticmethod
    def _has_source(spec, chunk) -> bool:
        return (spec.output_protocol == PLANNING_STREAM_SCHEMA
                and spec.planning_scope is not None and bool(chunk.content_delta))

    def feed(self, spec, chunk):
        return (self._parsers[spec.output_stream_id].feed(chunk.content_delta)
                if self._has_source(spec, chunk) else ())

    async def _allowed(self, policy, hook_name: str, spec, source) -> bool:
        authorize = getattr(policy, hook_name, None)
        projected = source if authorize is None else await authorize(spec, source)
        if projected is not None and projected != source:
            raise ContractViolationError("output policy cannot rewrite Provider planning source")
        return projected is not None

    async def progress_draft(self, spec: OutputStreamSpec, progress: PlanningProgress, *, policy) -> AgentOutputEventDraft | None:
        if spec.output_protocol != PLANNING_STREAM_SCHEMA or spec.planning_scope is None:
            raise ContractViolationError("planning projection requires a bound planning stream")
        if not await self._allowed(policy, "authorize_planning_progress", spec, progress):
            return None
        return AgentOutputEventDraft(
            run_id=spec.run_id, turn_id=spec.turn_id,
            output_stream_id=spec.output_stream_id, invocation_id=spec.invocation_id,
            source_event_key=f"planning:{spec.invocation_id}:{progress.record_index}",
            source=OutputSource.PROVIDER, kind=OutputEventKind.PLANNING_PROGRESS,
            channel=OutputChannel.COMMENTARY, visibility=OutputVisibility.PUBLIC,
            payload={"schemaVersion": PLANNING_STREAM_SCHEMA,
                     "operationId": spec.planning_scope.operation_id,
                     "revision": spec.planning_scope.revision, "attempt": spec.planning_attempt,
                     **progress.to_mapping()}, occurred_at=datetime.now(timezone.utc),
        )

    async def delta_drafts(self, spec, chunk, deltas, chunk_index, occurred_at, *, policy) -> tuple[AgentOutputEventDraft, ...]:
        if not self._has_source(spec, chunk):
            return ()
        if not await self._allowed(policy, "authorize_planning_delta", spec, chunk):
            return ()
        return tuple(AgentOutputEventDraft(
            run_id=spec.run_id, turn_id=spec.turn_id,
            output_stream_id=spec.output_stream_id, invocation_id=spec.invocation_id,
            source_event_key=f"planning-delta:{spec.invocation_id}:{chunk_index}:{part_index}",
            source=OutputSource.PROVIDER, kind=OutputEventKind.PLANNING_DELTA,
            channel=OutputChannel.COMMENTARY, visibility=OutputVisibility.PUBLIC,
            payload={"schemaVersion": PLANNING_STREAM_SCHEMA,
                     "operationId": spec.planning_scope.operation_id,
                     "revision": spec.planning_scope.revision, "attempt": spec.planning_attempt,
                     "recordIndex": delta.record_index, "sourceChunkIndex": chunk_index,
                     "sourcePartIndex": part_index, "textDelta": delta.text},
            occurred_at=occurred_at,
        ) for part_index, delta in enumerate(deltas, 1))
