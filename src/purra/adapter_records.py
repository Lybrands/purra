"""Data records shared by reference repositories and snapshot codecs."""
from __future__ import annotations
from dataclasses import dataclass, field
from purra.contracts import RunCreateParams, RunStatus, TaskStep, ExecutionPlan, TraceRecord, ModelTokenUsage, ModelFinishReason
from purra.events import AgentEvent
from purra.agent_execution_checkpoint import AgentExecutionCheckpoint
from purra.output.contracts import OutputStreamSpec
from purra.long_tasks.contracts import LongTaskRecord, LongTaskUnitRecord, LongTaskRunBinding, LongTaskUsage


@dataclass(slots=True)
class StoredRun:
    params: RunCreateParams
    status: RunStatus = RunStatus.RUNNING
    conversation_id: int | None = None
    steps: list[TaskStep] = field(default_factory=list)
    execution_plan: ExecutionPlan | None = None
    final_response: str | None = None
    validated_result: str | None = None
    error: str | None = None
    events: list[AgentEvent] = field(default_factory=list)
    traces: list[TraceRecord] = field(default_factory=list)
    model_attempt_ids: set[str] = field(default_factory=set)
    model_usage_by_invocation: dict[str, ModelTokenUsage | None] = field(
        default_factory=dict
    )
    provider_output_events: int = 0
    provider_output_bytes: int = 0
    execution_checkpoint: AgentExecutionCheckpoint | None = None
    checkpoint_attempt_count: int = 0



@dataclass(slots=True)
class StoredStream:
    spec: OutputStreamSpec
    status: str = "open"
    finish_reason: ModelFinishReason | None = None
    error_code: str | None = None



@dataclass(slots=True)
class StoredLongTask:
    record: LongTaskRecord
    units: dict[str, LongTaskUnitRecord]
    bindings: dict[str, LongTaskRunBinding]
    usage_by_run: dict[str, LongTaskUsage]


