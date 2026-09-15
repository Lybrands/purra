"""Provider delta batching and execution-local flush timers."""
from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from purra.contracts import ModelStreamChunk
from purra.errors import ContractViolationError
from purra.output.contracts import (
    AgentOutputEvent, AgentOutputEventDraft, AgentOutputIntent, OutputChannel,
    OutputEventKind, OutputSource, OutputStreamSpec, OutputVisibility,
    PROVIDER_DELTA_BATCH_SCHEMA, provider_delta_batch_digest,
)

@dataclass(frozen=True, slots=True)
class OutputBatchLimits:
    max_payload_bytes: int = 16_384
    max_fragments: int = 64
    max_latency_ms: int = 25
    max_background_latency_ms: int = 250

    def __post_init__(self) -> None:
        for name in (
            "max_payload_bytes",
            "max_fragments",
            "max_latency_ms",
            "max_background_latency_ms",
        ):
            value = int(getattr(self, name))
            if value <= 0:
                raise ValueError(f"{name.replace('_', ' ')} must be positive")
            object.__setattr__(self, name, value)


@dataclass(frozen=True, slots=True)
class _PendingProviderDelta:
    source_chunk_index: int
    source_part_index: int
    kind: OutputEventKind
    channel: OutputChannel
    visibility: OutputVisibility
    payload: Mapping[str, object]
    occurred_at: datetime

    def entry(self) -> dict[str, object]:
        return {
            "sourceChunkIndex": self.source_chunk_index,
            "sourcePartIndex": self.source_part_index,
            "kind": self.kind.value,
            "payload": dict(self.payload),
        }


@dataclass(slots=True)
class _StreamBatch:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    entries: list[_PendingProviderDelta] = field(default_factory=list)
    payload_bytes: int = 0
    timer: asyncio.Task[None] | None = None
    background_error: BaseException | None = None


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


class ProviderOutputBuffer:
    """Serialize per-stream flushes; persist a batch before publishing it."""

    def __init__(self, *, limits: OutputBatchLimits, sleep,
                 persist: Callable[[str, tuple[AgentOutputEventDraft, ...]], Awaitable[tuple[AgentOutputEvent, ...]]],
                 publish: Callable[[AgentOutputEvent], Awaitable[None]]):
        if not callable(sleep):
            raise TypeError("output batch sleep must be callable")
        self._batch_limits = limits
        self._sleep = sleep
        self._persist = persist
        self._publish = publish
        self._stream_batches: dict[str, _StreamBatch] = {}

    def open(self, stream_id: str) -> None:
        self._stream_batches.setdefault(stream_id, _StreamBatch())

    def append(self, batch: _StreamBatch, entries: tuple[_PendingProviderDelta, ...]) -> bool:
        batch.entries.extend(entries)
        batch.payload_bytes += sum(len(_canonical_json(entry.entry())) for entry in entries)
        return (batch.payload_bytes >= self._batch_limits.max_payload_bytes
                or len(batch.entries) >= self._batch_limits.max_fragments)

    def pending_deltas(
        self,
        spec: OutputStreamSpec,
        chunk: ModelStreamChunk,
        chunk_index: int,
        occurred_at: datetime,
    ) -> tuple[_PendingProviderDelta, ...]:
        pending: list[_PendingProviderDelta] = []
        if chunk.content_delta:
            channel, visibility = _content_destination(spec)
            pending.append(_PendingProviderDelta(
                chunk_index,
                0,
                OutputEventKind.PROVIDER_CONTENT_DELTA,
                channel,
                visibility,
                {"delta": chunk.content_delta},
                occurred_at,
            ))
        if chunk.reasoning_delta:
            pending.append(_PendingProviderDelta(
                chunk_index,
                1,
                OutputEventKind.PROVIDER_REASONING_DELTA,
                OutputChannel.DIAGNOSTIC,
                OutputVisibility.DIAGNOSTIC,
                {"delta": chunk.reasoning_delta},
                occurred_at,
            ))
        if chunk.tool_call_deltas:
            pending.append(_PendingProviderDelta(
                chunk_index,
                2,
                OutputEventKind.PROVIDER_TOOL_CALL_DELTA,
                OutputChannel.DIAGNOSTIC,
                OutputVisibility.PRIVATE,
                {
                    "deltas": [
                        {
                            "index": delta.index,
                            "id": delta.id,
                            "type": delta.type,
                            "name": delta.name,
                            "argumentsFragment": delta.arguments_fragment,
                        }
                        for delta in chunk.tool_call_deltas
                    ]
                },
                occurred_at,
            ))
        if chunk.progress_delta:
            pending.append(_PendingProviderDelta(
                chunk_index,
                3,
                OutputEventKind.PROVIDER_PROGRESS_DELTA,
                OutputChannel.DIAGNOSTIC,
                OutputVisibility.PRIVATE,
                {"delta": chunk.progress_delta},
                occurred_at,
            ))
        return tuple(pending)

    def _batch_drafts(
        self,
        spec: OutputStreamSpec,
        entries: tuple[_PendingProviderDelta, ...],
    ) -> tuple[AgentOutputEventDraft, ...]:
        grouped: dict[
            tuple[OutputChannel, OutputVisibility],
            list[_PendingProviderDelta],
        ] = {}
        for entry in entries:
            grouped.setdefault((entry.channel, entry.visibility), []).append(entry)
        groups = sorted(
            grouped.items(),
            key=lambda item: (
                item[1][0].source_chunk_index,
                item[1][0].source_part_index,
            ),
        )
        drafts = []
        for (channel, visibility), values in groups:
            normalized = [entry.entry() for entry in values]
            start = min(entry.source_chunk_index for entry in values)
            end = max(entry.source_chunk_index for entry in values)
            payload = {
                "schemaVersion": PROVIDER_DELTA_BATCH_SCHEMA,
                "sourceChunkStart": start,
                "sourceChunkEnd": end,
                "entries": normalized,
                "payloadDigest": provider_delta_batch_digest(normalized),
            }
            drafts.append(AgentOutputEventDraft(
                run_id=spec.run_id,
                turn_id=spec.turn_id,
                output_stream_id=spec.output_stream_id,
                invocation_id=spec.invocation_id,
                source_event_key=(
                    f"provider-batch:{spec.invocation_id}:"
                    f"{channel.value}:{visibility.value}:{start}:{end}"
                ),
                source=OutputSource.PROVIDER,
                kind=OutputEventKind.PROVIDER_DELTA_BATCH,
                channel=channel,
                visibility=visibility,
                payload=payload,
                occurred_at=values[0].occurred_at,
            ))
        return tuple(drafts)

    async def flush_locked(
        self,
        spec: OutputStreamSpec,
        batch: _StreamBatch,
    ) -> tuple[AgentOutputEvent, ...]:
        if not batch.entries:
            self._cancel_batch_timer(batch)
            return ()
        drafts = self._batch_drafts(spec, tuple(batch.entries))
        events = await self._persist(spec.run_id, drafts)
        batch.entries.clear()
        batch.payload_bytes = 0
        self._cancel_batch_timer(batch)
        for event in events:
            await self._publish(event)
        return events

    def schedule(
        self,
        spec: OutputStreamSpec,
        batch: _StreamBatch,
    ) -> None:
        if batch.timer is None or batch.timer.done():
            batch.timer = asyncio.create_task(
                self._flush_after_latency(spec, batch)
            )

    async def _flush_after_latency(
        self,
        spec: OutputStreamSpec,
        batch: _StreamBatch,
    ) -> None:
        current = asyncio.current_task()
        try:
            latency_ms = (
                self._batch_limits.max_latency_ms
                if spec.intent in {
                    AgentOutputIntent.EXECUTION_PUBLIC,
                    AgentOutputIntent.FINAL_PUBLIC,
                }
                else self._batch_limits.max_background_latency_ms
            )
            await self._sleep(latency_ms / 1000)
            async with batch.lock:
                self.raise_background_error(batch)
                await self.flush_locked(spec, batch)
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            batch.background_error = error
        finally:
            if batch.timer is current:
                batch.timer = None

    @staticmethod
    def _cancel_batch_timer(batch: _StreamBatch) -> None:
        timer = batch.timer
        if timer is not None and timer is not asyncio.current_task():
            timer.cancel()
        batch.timer = None

    @staticmethod
    def raise_background_error(batch: _StreamBatch) -> None:
        if batch.background_error is not None:
            raise batch.background_error

    def require(self, output_stream_id: str) -> _StreamBatch:
        try:
            return self._stream_batches[output_stream_id]
        except KeyError as error:
            raise ContractViolationError(
                f"output stream {output_stream_id!r} is not open"
            ) from error

    def discard(self, output_stream_id: str) -> None:
        batch = self._stream_batches.pop(output_stream_id, None)
        if batch is not None:
            self._cancel_batch_timer(batch)


def _content_destination(
    spec: OutputStreamSpec,
) -> tuple[OutputChannel, OutputVisibility]:
    if spec.intent is AgentOutputIntent.EXECUTION_PUBLIC:
        return OutputChannel.COMMENTARY, OutputVisibility.PUBLIC
    if spec.intent is AgentOutputIntent.FINAL_PUBLIC:
        return OutputChannel.FINAL, OutputVisibility.PUBLIC
    return OutputChannel.DIAGNOSTIC, OutputVisibility.PRIVATE


