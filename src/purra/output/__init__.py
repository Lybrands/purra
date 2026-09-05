"""Canonical output contracts and ports."""

from purra.output.contracts import (
    AgentOutputEvent,
    AgentOutputEventDraft,
    AgentOutputIntent,
    DomainEffectOutput,
    OutputChannel,
    OutputCommitMode,
    OutputEventKind,
    OutputSource,
    OutputStreamSpec,
    OutputVisibility,
    AGENT_PROGRESS_SCHEMA,
    MAX_AGENT_PROGRESS_CHARS,
    PROVIDER_DELTA_BATCH_SCHEMA,
    PublicFact,
    PublicFactBundle,
    PublicPresentationMode,
    ResponseTransactionMode,
    ResponseTransactionPolicy,
    RunLifecycleOutputDraft,
    RuntimeOutputEvent,
    TERMINAL_STREAM_ABORT_CAUSE,
    TERMINAL_STREAM_ABORT_ERROR_CODE,
    ToolOutputEvent,
    provider_delta_batch_digest,
)
from purra.output.ports import (
    AgentOutputPolicy,
    AgentOutputPublisher,
    AgentOutputJournalQuery,
    AgentOutputRepository,
    CommittedResultFactsProvider,
    ValidatedResultCommitter,
)
_LAZY_EXPORT_MODULES = {
    "AgentOutputProcessor": "processor",
    "OutputBatchLimits": "processor",
    "OutputRecoveryObserver": "processor",
    "AgentResponseTransaction": "response_transaction",
    "ResponseTransactionValidationError": "response_transaction",
}


def __getattr__(name: str):
    module_name = _LAZY_EXPORT_MODULES.get(name)
    if module_name == "processor":
        from purra.output import processor

        return getattr(processor, name)
    if module_name == "response_transaction":
        from purra.output import response_transaction

        return getattr(response_transaction, name)
    raise AttributeError(name)


__all__ = [
    "AgentOutputEvent",
    "AgentOutputEventDraft",
    "AgentOutputIntent",
    "AgentOutputJournalQuery",
    "AgentOutputPolicy",
    "AgentOutputProcessor",
    "AgentOutputPublisher",
    "AgentOutputRepository",
    "AGENT_PROGRESS_SCHEMA",
    "AgentResponseTransaction",
    "CommittedResultFactsProvider",
    "DomainEffectOutput",
    "OutputChannel",
    "OutputBatchLimits",
    "OutputCommitMode",
    "OutputEventKind",
    "OutputRecoveryObserver",
    "OutputSource",
    "OutputStreamSpec",
    "OutputVisibility",
    "MAX_AGENT_PROGRESS_CHARS",
    "PROVIDER_DELTA_BATCH_SCHEMA",
    "PublicFact",
    "PublicFactBundle",
    "PublicPresentationMode",
    "ResponseTransactionMode",
    "ResponseTransactionPolicy",
    "ResponseTransactionValidationError",
    "RunLifecycleOutputDraft",
    "RuntimeOutputEvent",
    "TERMINAL_STREAM_ABORT_CAUSE",
    "TERMINAL_STREAM_ABORT_ERROR_CODE",
    "ToolOutputEvent",
    "provider_delta_batch_digest",
    "ValidatedResultCommitter",
]
