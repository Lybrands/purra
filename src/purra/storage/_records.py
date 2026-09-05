"""Explicit wire identifiers and fields for storage schema 1.

Record identifiers are independent of Python import locations.
"""

from purra.adapters.durable_memory import (
    _LongTaskState,
)
from purra.adapters.memory import (
    _RunRecord, _StreamRecord,
)
from purra.agent_execution_checkpoint import (
    AgentExecutionCheckpoint,
)
from purra.agent_tree import (
    AgentCapabilityGrant, AgentNode, AgentNodeState, AgentTreeRun, AgentTreeRunStatus, ContextCheckpoint, ContinueAgentReceipt, SpawnAgentsReceipt, SpawnedAgent,
)
from purra.artifacts.continuity import (
    ArtifactWriteClaim,
)
from purra.artifacts.contracts import (
    ArtifactBatch, ArtifactBatchReceipt, ArtifactRecord, ArtifactStatus,
)
from purra.artifacts.ownership import (
    ArtifactOwnerRef,
)
from purra.contracts import (
    AgentMessage, AgentRunRequest, ContextEvidenceReceipt, DomainContext, DomainEffect, ModelRequest, ModelTokenUsage, RunCreateParams, RunExecutionIntent, RunExecutionLease, RunProvenance, RuntimeLimits, ToolCall, ToolHandlerResult, TraceRecord,
)
from purra.contracts.enums import (
    MessageOrigin, MessageRole, ModelFinishReason, PlanningMode, RunStatus, StepExecutor, StepStatus, StepType, ToolBatchOutcome, ToolEffectState, ToolPlanningDisposition, ToolRiskLevel, ToolStepDisposition,
)
from purra.contracts.host import (
    RunBinding,
)
from purra.contracts.plans import (
    ExecutionPlan, TaskSpec, TaskStep,
)
from purra.events import (
    AgentEvent,
)
from purra.long_tasks.contracts import (
    LongTaskBudgetLimits, LongTaskRecord, LongTaskRunBinding, LongTaskRunRelation, LongTaskStatus, LongTaskUnitRecord, LongTaskUnitStatus, LongTaskUsage,
)
from purra.model_protocol.capabilities import (
    AssistantContentWithToolCalls, ContinuationKind, ContinuationSafety, FeatureSupport, LengthReasonDetail, ModelCapabilitySnapshot, ModelProtocolCapabilities, ReasoningControl, ReasoningLimitKind, ReasoningReplayPolicy, ReasoningUsageDetail, ThinkingTokenAccounting, VisibleOutputReservation,
)
from purra.output.contracts import (
    AgentOutputEvent, AgentOutputIntent, OutputChannel, OutputCommitMode, OutputEventKind, OutputSource, OutputStreamSpec, OutputVisibility,
)
from purra.planning_stream import (
    PlanningScope,
)
from purra.recovery.contracts import (
    FailureDisposition,
)

RECORDS = {
    "AgentCapabilityGrant": (AgentCapabilityGrant, ('can_spawn_agents', 'max_depth', 'max_children_per_call', 'max_agents_per_root', 'max_parallel_runs', 'allowed_tools', 'allowed_models')),
    "AgentEvent": (AgentEvent, ('type', 'payload', 'run_id')),
    "AgentExecutionCheckpoint": (AgentExecutionCheckpoint, ('run_id', 'next_round', 'messages', 'round_limit', 'execution_state_domain', 'evidence_state', 'recovery_attempts', 'logical_round_number', 'progress_rounds', 'tool_input_recovery_epoch', 'logical_required_tool_call_enabled', 'provider_required_tool_choice_enabled', 'declined_response_pending', 'response_repair_pending', 'public_presentation_pending', 'last_tool_outcome', 'pending_tool_input_retries', 'initial_planning_open', 'schema_version', 'phase', 'execution_profile', 'input_revision', 'planning_state', 'dynamic_replan_pending', 'pending_recovery_error_code', 'failed_tool_recovery_error_code')),
    "AgentMessage": (AgentMessage, ('role', 'content', 'reasoning', 'tool_calls', 'tool_call_id', 'origin', 'attributes', 'host_metadata', 'provider_data')),
    "AgentNode": (AgentNode, ('agent_id', 'root_agent_id', 'parent_agent_id', 'depth', 'created_by_run_id', 'created_by_call_id', 'name', 'title', 'instruction', 'capability_grant', 'context_version', 'context_checkpoint_id', 'latest_run_id', 'state')),
    "AgentOutputEvent": (AgentOutputEvent, ('event_id', 'output_stream_id', 'run_id', 'turn_id', 'invocation_id', 'sequence', 'source', 'kind', 'channel', 'visibility', 'payload', 'occurred_at', 'emitted_at', 'root_run_id', 'agent_id', 'parent_run_id', 'root_sequence', 'source_event_key')),
    "AgentRunRequest": (AgentRunRequest, ('messages', 'model', 'domain_context', 'session_id', 'mode', 'context_window', 'tools_enabled', 'planning_mode', 'metadata')),
    "AgentTreeRun": (AgentTreeRun, ('run_id', 'agent_id', 'root_run_id', 'parent_run_id', 'previous_run_id', 'spawn_batch_id', 'objective', 'input_payload', 'required', 'priority', 'status', 'result', 'error_code', 'created_sequence', 'lease_owner_id', 'lease_epoch', 'lease_expires_at_ms')),
    "ArtifactBatch": (ArtifactBatch, ('artifact_id', 'batch_id', 'idempotency_key', 'sequence', 'committed_revision', 'items', 'coverage_keys', 'content_digest')),
    "ArtifactBatchReceipt": (ArtifactBatchReceipt, ('artifact_id', 'batch_id', 'sequence', 'committed_revision', 'next_sequence', 'accepted_count', 'replayed')),
    "ArtifactOwnerRef": (ArtifactOwnerRef, ('kind', 'id')),
    "ArtifactRecord": (ArtifactRecord, ('id', 'namespace', 'kind', 'owner_id', 'owner_ref', 'created_by_run_id', 'schema_version', 'status', 'revision', 'next_sequence', 'committed_item_count', 'expected_item_count', 'metadata', 'resource_ref', 'coverage_digest')),
    "ArtifactWriteClaim": (ArtifactWriteClaim, ('artifact_id', 'run_id', 'claim_token', 'acquired_revision', 'expires_at_ms')),
    "ContextCheckpoint": (ContextCheckpoint, ('checkpoint_id', 'agent_id', 'version', 'previous_checkpoint_id', 'source_run_id', 'content_ref', 'fingerprint')),
    "ContextEvidenceReceipt": (ContextEvidenceReceipt, ('evidence_id', 'context_block', 'source', 'item_id', 'version', 'metadata')),
    "ContinueAgentReceipt": (ContinueAgentReceipt, ('agent', 'run', 'replayed')),
    "DomainContext": (DomainContext, ('namespace', 'payload')),
    "DomainEffect": (DomainEffect, ('type', 'payload')),
    "ExecutionPlan": (ExecutionPlan, ('title', 'steps', 'goal', 'task_spec', 'work_step_ids')),
    "LongTaskBudgetLimits": (LongTaskBudgetLimits, ('max_invocation_attempts', 'max_input_tokens', 'max_run_generation_tokens', 'max_reasoning_tokens')),
    "LongTaskRecord": (LongTaskRecord, ('id', 'namespace', 'kind', 'owner_id', 'created_by_run_id', 'status', 'revision', 'total_units', 'completed_units', 'failed_units', 'max_parallelism', 'deadline_at_ms', 'budget_limits', 'cancellation_requested_at_ms', 'usage', 'metadata', 'create_time', 'update_time')),
    "LongTaskRunBinding": (LongTaskRunBinding, ('task_id', 'run_id', 'relation')),
    "LongTaskUnitRecord": (LongTaskUnitRecord, ('task_id', 'id', 'position', 'status', 'semantic_key', 'dependencies', 'parent_unit_id', 'required', 'attempt', 'max_attempts', 'worker_id', 'lease_epoch', 'lease_expires_at_ms', 'settled_by_worker_id', 'run_id', 'input_ref', 'output_ref', 'artifact_digest', 'validation_receipt', 'failure', 'disposition', 'error_code', 'metadata', 'create_time', 'update_time')),
    "LongTaskUsage": (LongTaskUsage, ('invocation_count', 'unreported_usage_attempts', 'input_tokens', 'generation_tokens', 'reasoning_tokens')),
    "ModelCapabilitySnapshot": (ModelCapabilitySnapshot, ('schema_version', 'profile_id', 'provider_protocol', 'context_window_tokens', 'max_generation_tokens', 'thinking_token_accounting', 'protocol', 'reasoning_usage_detail', 'reasoning_limit_kind', 'visible_output_reservation', 'length_reason_detail', 'continuation_kind', 'continuation_safe_for', 'actionable', 'source')),
    "ModelProtocolCapabilities": (ModelProtocolCapabilities, ('reasoning_control', 'reasoning_replay', 'tool_calling', 'required_tool_choice', 'parallel_tool_calls', 'streaming', 'cancellation', 'public_progress', 'assistant_content_with_tool_calls', 'json_schema_level', 'stream_finish_semantics', 'usage_semantics')),
    "ModelRequest": (ModelRequest, ('provider', 'model', 'capability_snapshot', 'max_generation_tokens', 'options')),
    "ModelTokenUsage": (ModelTokenUsage, ('input_tokens', 'generation_tokens', 'total_tokens', 'cached_input_tokens', 'reasoning_tokens')),
    "OutputStreamSpec": (OutputStreamSpec, ('output_stream_id', 'run_id', 'turn_id', 'invocation_id', 'intent', 'commit_mode', 'output_protocol', 'planning_scope', 'planning_attempt')),
    "PlanningScope": (PlanningScope, ('run_id', 'operation_id', 'revision')),
    "RunBinding": (RunBinding, ('namespace', 'aggregate_id', 'command_id', 'attributes')),
    "RunCreateParams": (RunCreateParams, ('session_id', 'prompt', 'mode', 'provenance', 'binding', 'turn_id', 'deadline_at_ms', 'runtime_limits', 'agent_preset_snapshot', 'requested_user_max_generation_tokens', 'result_capacity_target_tokens', 'selected_context_window_tokens', 'requested_run_id', 'root_run_id', 'agent_id', 'parent_run_id', 'lease_owner_id', 'lease_epoch')),
    "RunExecutionIntent": (RunExecutionIntent, ('requested_reasoning_mode', 'output_contract', 'tool_protocol_contract', 'recovery_policy_id', 'capability_snapshot_digest', 'requested_user_max_generation_tokens', 'result_capacity_target_tokens')),
    "RunExecutionLease": (RunExecutionLease, ('run_id', 'status', 'owner_id', 'expires_at_ms', 'heartbeat_at_ms', 'attempt', 'cancellation_requested_at_ms')),
    "RunProvenance": (RunProvenance, ('model_provider', 'model_name', 'context_window', 'endpoint_digest', 'request_profile_digest', 'capability_snapshot', 'execution_intent')),
    "RuntimeLimits": (RuntimeLimits, ('max_run_generation_tokens', 'max_model_rounds', 'max_progress_rounds', 'provider_activity_idle_timeout_ms', 'provider_progress_idle_timeout_ms', 'provider_invocation_timeout_ms', 'root_run_timeout_ms', 'max_model_invocation_attempts', 'max_input_tokens', 'max_reasoning_tokens', 'max_provider_output_events', 'max_provider_output_bytes', 'max_stream_content_chars', 'max_stream_reasoning_chars', 'max_stream_chunks')),
    "SpawnAgentsReceipt": (SpawnAgentsReceipt, ('batch_id', 'items', 'replayed')),
    "SpawnedAgent": (SpawnedAgent, ('agent', 'run')),
    "TaskSpec": (TaskSpec, ('goal', 'target', 'operation', 'instruction', 'constraints', 'preserve', 'deliverable')),
    "TaskStep": (TaskStep, ('id', 'title', 'type', 'executor', 'status', 'risk_level', 'suggested_tools', 'depends_on', 'description', 'result_summary', 'error', 'protocol_private', 'planning_capability')),
    "ToolCall": (ToolCall, ('id', 'name', 'arguments_json')),
    "ToolHandlerResult": (ToolHandlerResult, ('content', 'from_cache', 'effects', 'context_evidence', 'error_code', 'step_disposition', 'planning_disposition', 'effect_state')),
    "TraceRecord": (TraceRecord, ('stage', 'outcome', 'details', 'duration_ms')),
    "LongTaskState": (_LongTaskState, ('record', 'units', 'bindings', 'usage_by_run')),
    "RunRecord": (_RunRecord, ('params', 'status', 'conversation_id', 'steps', 'execution_plan', 'final_response', 'validated_result', 'error', 'events', 'traces', 'model_attempt_ids', 'model_usage_by_invocation', 'provider_output_events', 'provider_output_bytes', 'execution_checkpoint', 'checkpoint_attempt_count')),
    "StreamRecord": (_StreamRecord, ('spec', 'status', 'finish_reason', 'error_code')),
}

ENUMS = {
    "AgentNodeState": AgentNodeState,
    "AgentOutputIntent": AgentOutputIntent,
    "AgentTreeRunStatus": AgentTreeRunStatus,
    "ArtifactStatus": ArtifactStatus,
    "AssistantContentWithToolCalls": AssistantContentWithToolCalls,
    "ContinuationKind": ContinuationKind,
    "ContinuationSafety": ContinuationSafety,
    "FailureDisposition": FailureDisposition,
    "FeatureSupport": FeatureSupport,
    "LengthReasonDetail": LengthReasonDetail,
    "LongTaskRunRelation": LongTaskRunRelation,
    "LongTaskStatus": LongTaskStatus,
    "LongTaskUnitStatus": LongTaskUnitStatus,
    "MessageOrigin": MessageOrigin,
    "MessageRole": MessageRole,
    "ModelFinishReason": ModelFinishReason,
    "OutputChannel": OutputChannel,
    "OutputCommitMode": OutputCommitMode,
    "OutputEventKind": OutputEventKind,
    "OutputSource": OutputSource,
    "OutputVisibility": OutputVisibility,
    "PlanningMode": PlanningMode,
    "ReasoningControl": ReasoningControl,
    "ReasoningLimitKind": ReasoningLimitKind,
    "ReasoningReplayPolicy": ReasoningReplayPolicy,
    "ReasoningUsageDetail": ReasoningUsageDetail,
    "RunStatus": RunStatus,
    "StepExecutor": StepExecutor,
    "StepStatus": StepStatus,
    "StepType": StepType,
    "ThinkingTokenAccounting": ThinkingTokenAccounting,
    "ToolBatchOutcome": ToolBatchOutcome,
    "ToolEffectState": ToolEffectState,
    "ToolPlanningDisposition": ToolPlanningDisposition,
    "ToolRiskLevel": ToolRiskLevel,
    "ToolStepDisposition": ToolStepDisposition,
    "VisibleOutputReservation": VisibleOutputReservation,
}
