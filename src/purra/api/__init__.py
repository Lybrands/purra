"""Stable host-facing entry points for complete Agent runs.

Product code may register contracts and adapters from the narrower public
packages, but complete execution is entered through this module.  Runtime
implementation modules are not application entry points.
"""

from purra.engine import (
    AgentCore,
    AgentCoreRunOptions,
    ContextStrategy,
    DurableTaskContinuation,
    ExecutionProfile,
)
from purra.adapters import InMemoryAgentAdapters, InMemoryDurableAdapters
from purra.agent_presets import (
    AgentComponentBinding,
    AgentPreset,
    AgentPresetSnapshot,
    PromptSection,
)
from purra.agent_tree import (
    AgentCapabilityGrant,
    AgentNode,
    AgentNodeState,
    AgentRunAggregation,
    AgentTreeRun,
    AgentTreeRunStatus,
    BeginRootAgentCommand,
    ChildAgentSpec,
    ContextCheckpoint,
    ContinueAgentCommand,
    ContinueAgentReceipt,
    InMemoryRunTreeRepository,
    RunTreeRepository,
    SpawnAgentsCommand,
    SpawnAgentsReceipt,
    SpawnedAgent,
)
from purra.agent_tree_execution import (
    AgentTreeExecutionResult,
    AgentTreeRunExecutor,
    AgentTreeRunSupervisor,
    RunCommandService,
)
from purra.agent_tree_policy import AgentTreePolicy
from purra.agent_execution_checkpoint import AgentExecutionCheckpoint
from purra.agent_tree_lease import (
    AgentRunLeaseClaim,
    current_agent_run_lease,
)
from purra.contracts import (
    ContextEvidenceReceipt,
    PlanningMode,
    PlanningResult,
    WorkPlan,
    WorkStep,
)
from purra.execution import AgentRunHandle
from purra.model_execution import (
    AgentModelResponseJudge,
    AgentModelTask,
    AgentModelTaskRunner,
    AgentModelTextResult,
)
from purra.planner import AgentPlanner
from purra.orphan_recovery import (
    OrphanRecoveryCoordinator,
    OrphanRunSettlement,
)
from purra.ports import ModelInputEvidenceValidator, WorkPlanner
from purra.run_recovery import RunRecoverySnapshot
from purra.run_control import (
    OrphanRunCandidate,
    OrphanRunDecision,
    OrphanRunDisposition,
    OrphanRunReason,
    OrphanTaskEvidence,
    RunActivitySnapshot,
    RunCancellationReceipt,
    decide_orphan_run,
)
from purra.run_state import canonicalize_execution_plan

__all__ = [
    "AgentCore",
    "AgentExecutionCheckpoint",
    "AgentCapabilityGrant",
    "AgentComponentBinding",
    "AgentNode",
    "AgentNodeState",
    "AgentPreset",
    "AgentPresetSnapshot",
    "AgentRunAggregation",
    "AgentRunLeaseClaim",
    "AgentTreeRun",
    "AgentTreeExecutionResult",
    "AgentTreeRunExecutor",
    "AgentTreeRunSupervisor",
    "AgentTreeRunStatus",
    "BeginRootAgentCommand",
    "ChildAgentSpec",
    "ContextCheckpoint",
    "ContinueAgentCommand",
    "ContinueAgentReceipt",
    "InMemoryAgentAdapters",
    "InMemoryDurableAdapters",
    "InMemoryRunTreeRepository",
    "AgentCoreRunOptions",
    "ContextStrategy",
    "ContextEvidenceReceipt",
    "PlanningMode",
    "AgentTreePolicy",
    "DurableTaskContinuation",
    "ExecutionProfile",
    "PromptSection",
    "OrphanRunCandidate",
    "OrphanRecoveryCoordinator",
    "OrphanRunSettlement",
    "OrphanRunDecision",
    "OrphanRunDisposition",
    "OrphanRunReason",
    "OrphanTaskEvidence",
    "RunActivitySnapshot",
    "RunCancellationReceipt",
    "RunRecoverySnapshot",
    "RunTreeRepository",
    "RunCommandService",
    "SpawnAgentsCommand",
    "SpawnAgentsReceipt",
    "SpawnedAgent",
    "AgentRunHandle",
    "AgentModelResponseJudge",
    "AgentModelTask",
    "AgentModelTaskRunner",
    "AgentModelTextResult",
    "AgentPlanner",
    "PlanningResult",
    "WorkPlan",
    "WorkPlanner",
    "ModelInputEvidenceValidator",
    "WorkStep",
    "decide_orphan_run",
    "canonicalize_execution_plan",
    "current_agent_run_lease",
]

from purra.planning_context import PlanningContext, current_planning_context
from purra.interaction import UserInputRequired
from purra.planning_stream import PLANNING_STREAM_SCHEMA, PlanningScope, PlanningProgress, PlanningStreamParser, PlanningStreamError

__all__ += ["PlanningContext", "current_planning_context", "PLANNING_STREAM_SCHEMA", "PlanningScope", "PlanningProgress", "PlanningStreamParser", "PlanningStreamError"]
__all__ += ["UserInputRequired"]

from purra.structured import (
    StructuredOutputContract, StructuredOutputError, StructuredOutputLimits,
)

__all__ += ["StructuredOutputContract", "StructuredOutputError", "StructuredOutputLimits"]

from purra.structured_task import StructuredInvocationRef, StructuredModelTaskReceipt, StructuredModelTaskResult

__all__ += ["StructuredInvocationRef", "StructuredModelTaskReceipt", "StructuredModelTaskResult"]

from purra.structured import json_identity_digest
__all__ += ["json_identity_digest"]

from purra.observability.inspection import build_recovery_inspection, inspect_recovery
__all__ += ["build_recovery_inspection", "inspect_recovery"]
from purra.testing import IntegrationCheck, check_integration
__all__ += ["IntegrationCheck", "check_integration"]
