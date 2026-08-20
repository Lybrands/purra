"""Stable public umbrella for PurrA dependency-inversion ports."""

from purra.ports.context import (
    ContextCompressionHook,
    ContextDemandProvider,
    ContextProvider,
    ConversationCompactor,
    StagedContextProvider,
    TaskContextDemandProvider,
)
from purra.ports.model import CancellationSignal, ModelGateway
from purra.ports.persistence import (
    DelegationRepository,
    ExecutionLeaseStore,
    RunControlStore,
    RunRecoveryStore,
)
from purra.ports.planning import (
    DynamicWorkPlanner,
    ExecutionStateFactory,
    PlanningPolicy,
    ResponseJudge,
    ResponseJudgePolicy,
    ResponseValidator,
    WorkPlanner,
    WorkPlanningConstraintProvider,
)
from purra.ports.projection import DomainEventProjector, RunCancellationProjector
from purra.ports.projection import RunBeginProjector, RunCommitProjector
from purra.ports.run_lifecycle import (
    CONTROLLER_OWNED_RUN_EVENT_TYPES,
    TERMINAL_RUN_EVENT_TYPES,
    RunBeginResult,
    RunCommit,
    RunRepository,
    validate_run_commit_lifecycle,
)
from purra.ports.tools import (
    ApprovalGateway,
    CacheProbe,
    EventSink,
    RuntimeObserver,
    RuntimePlanningHook,
    ScopeValidator,
    ToolCatalog,
    ToolExecutionGateway,
    ToolHandler,
    ToolIdempotencyGateway,
    ToolRegistration,
)
from purra.output.ports import (
    AgentOutputPolicy,
    AgentOutputPublisher,
    AgentOutputRepository,
)

__all__ = [name for name in globals() if not name.startswith("_")]
