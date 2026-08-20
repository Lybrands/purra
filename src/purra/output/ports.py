"""Dependency-inversion ports for canonical Agent output."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from purra.contracts import (
    AgentRunResult,
    ModelFinishReason,
    ModelStreamChunk,
    RunCreateParams,
    RunId,
)
from purra.events import AgentEvent
from purra.output.contracts import (
    AgentOutputEvent,
    AgentOutputEventDraft,
    OutputStreamSpec,
    PublicFactBundle,
    RunLifecycleOutputDraft,
)

if TYPE_CHECKING:
    from purra.ports.run_lifecycle import RunBeginResult, RunCommit


@runtime_checkable
class AgentOutputRepository(Protocol):
    async def begin_run_lifecycle(
        self,
        params: RunCreateParams,
        started_event: AgentEvent,
    ) -> tuple[RunBeginResult, AgentOutputEvent]: ...

    async def open_stream(self, spec: OutputStreamSpec) -> OutputStreamSpec: ...

    async def append_event(
        self,
        draft: AgentOutputEventDraft,
    ) -> AgentOutputEvent: ...

    async def commit_run_lifecycle(
        self,
        run_id: RunId,
        commit: RunCommit,
        draft: RunLifecycleOutputDraft,
        related_drafts: tuple[AgentOutputEventDraft, ...] = (),
    ) -> tuple[AgentOutputEvent, ...]:
        """Commit Run state and output events as one terminal fence.

        A terminal commit must abort every still-open stream for the Run in the
        same transaction and must replay the same canonical events exactly.
        """
        ...

    async def commit_stream(
        self,
        output_stream_id: str,
        finish_reason: ModelFinishReason,
    ) -> AgentOutputEvent: ...

    async def abort_stream(
        self,
        output_stream_id: str,
        error_code: str,
    ) -> AgentOutputEvent: ...

    async def publish_stream_content_as_commentary(
        self,
        output_stream_id: str,
    ) -> tuple[AgentOutputEvent, ...]: ...

    async def list_events(
        self,
        run_id: RunId,
        *,
        after_sequence: int,
        limit: int = 200,
    ) -> tuple[AgentOutputEvent, ...]: ...

    async def load_validated_result(self, run_id: RunId) -> str: ...

@runtime_checkable
class AgentOutputPublisher(Protocol):
    async def publish_committed(self, event: AgentOutputEvent) -> None: ...

    async def wait_for_sequence(
        self,
        run_id: RunId,
        *,
        after_sequence: int,
    ) -> None: ...


@runtime_checkable
class AgentOutputJournalQuery(Protocol):
    """Read-side cursor over committed output, independent of any domain."""

    async def list_session_events(
        self,
        *,
        session_id: int,
        after_cursor: int,
        limit: int = 200,
    ) -> tuple[tuple[int, AgentOutputEvent], ...]: ...


@runtime_checkable
class AgentOutputPolicy(Protocol):
    async def authorize_provider_chunk(
        self,
        spec: OutputStreamSpec,
        chunk: ModelStreamChunk,
    ) -> ModelStreamChunk | None: ...


@runtime_checkable
class CommittedResultFactsProvider(Protocol):
    async def facts_for(
        self,
        run_id: RunId,
        result: AgentRunResult,
    ) -> PublicFactBundle: ...


@runtime_checkable
class ValidatedResultCommitter(Protocol):
    async def commit_candidate(
        self,
        run_id: RunId,
        candidate: str,
    ) -> AgentRunResult: ...


__all__ = [name for name in globals() if not name.startswith("_")]
