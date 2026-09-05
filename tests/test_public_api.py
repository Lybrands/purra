from __future__ import annotations

import importlib
import inspect
from types import ModuleType

def test_complete_run_api_is_importable_from_the_package_boundary():
    from purra.api import (
        AgentCore,
        AgentCoreRunOptions,
        AgentCapabilityGrant,
        AgentComponentBinding,
        AgentNode,
        AgentTreeExecutionResult,
        AgentTreeRun,
        AgentTreeRunExecutor,
        AgentTreeRunSupervisor,
        AgentPreset,
        AgentPresetSnapshot,
        AgentPlanner,
        AgentRunHandle,
        AgentRunLeaseClaim,
        ContextStrategy,
        ContextEvidenceReceipt,
        AgentTreePolicy,
        ExecutionProfile,
        InMemoryAgentAdapters,
        InMemoryRunTreeRepository,
        PlanningMode,
        PromptSection,
        OrphanRunCandidate,
        OrphanRecoveryCoordinator,
        OrphanRunDecision,
        OrphanRunDisposition,
        OrphanRunReason,
        OrphanRunSettlement,
        OrphanTaskEvidence,
        RunActivitySnapshot,
        RunCancellationReceipt,
        RunRecoverySnapshot,
        RunCommandService,
        WorkPlan,
        WorkPlanner,
        ModelInputEvidenceValidator,
        WorkStep,
        canonicalize_execution_plan,
        current_agent_run_lease,
        decide_orphan_run,
    )

    assert AgentCore.__module__.startswith("purra.")
    assert AgentCapabilityGrant.__module__.startswith("purra.")
    assert AgentNode.__module__.startswith("purra.")
    assert AgentTreeExecutionResult.__module__.startswith("purra.")
    assert AgentTreeRun.__module__.startswith("purra.")
    assert AgentTreeRunExecutor.__module__.startswith("purra.")
    assert AgentTreeRunSupervisor.__module__.startswith("purra.")
    assert AgentRunLeaseClaim.__module__.startswith("purra.")
    assert callable(current_agent_run_lease)
    assert AgentComponentBinding("host.context", "1").revision == "1"
    assert AgentPreset.__module__.startswith("purra.")
    assert AgentPresetSnapshot.__module__.startswith("purra.")
    assert PromptSection.__module__.startswith("purra.")
    assert OrphanRunCandidate.__module__.startswith("purra.")
    assert OrphanRecoveryCoordinator.__module__.startswith("purra.")
    assert OrphanRunSettlement is not None
    assert OrphanRunDecision.__module__.startswith("purra.")
    assert OrphanRunDisposition.__module__.startswith("purra.")
    assert OrphanRunReason.__module__.startswith("purra.")
    assert OrphanTaskEvidence.__module__.startswith("purra.")
    assert RunActivitySnapshot.__module__.startswith("purra.")
    assert RunCancellationReceipt.__module__.startswith("purra.")
    assert decide_orphan_run.__module__.startswith("purra.")
    assert RunRecoverySnapshot.__module__.startswith("purra.")
    assert RunCommandService.__module__.startswith("purra.")
    assert AgentCoreRunOptions.__module__.startswith("purra.")
    assert AgentRunHandle.__module__.startswith("purra.")
    assert ContextStrategy.SINGLE_PASS.value == "single_pass"
    assert ContextEvidenceReceipt(
        "evidence-1", "memory", "fixture", "item-1", 1
    ).version == 1
    assert ModelInputEvidenceValidator is not None
    assert AgentTreePolicy().max_children_per_call == 3
    assert ExecutionProfile().context_strategy is ContextStrategy.SINGLE_PASS
    adapters = InMemoryAgentAdapters()
    assert InMemoryRunTreeRepository.__module__.startswith("purra.")
    assert adapters.runs is not adapters.outputs
    assert adapters.outputs is not adapters.publisher
    assert PlanningMode.AUTO.value == "auto"
    assert PlanningMode.REACTIVE.value == "reactive"
    assert PlanningMode.PLANNED.value == "planned"
    assert AgentPlanner.__module__.startswith("purra.")
    assert WorkPlan.__module__.startswith("purra.")
    assert WorkStep.__module__.startswith("purra.")
    assert WorkPlanner.__module__.startswith("purra.")
    assert canonicalize_execution_plan.__module__.startswith("purra.")
    assert hasattr(AgentCore, "submit")
    assert hasattr(AgentCore, "spawn_agents")
    assert hasattr(AgentCore, "continue_agent")
    assert hasattr(AgentCore, "join_agent_runs")
    assert hasattr(AgentCore, "cancel_agent_run")
    assert hasattr(AgentCore, "close_agent")
    assert hasattr(AgentCore, "bind_agent_tree_root")
    assert hasattr(AgentCore, "recover_agent_tree_root")
    assert not hasattr(AgentCore, "run")
    assert "agent_tree_policy" not in inspect.signature(
        AgentCore.__init__
    ).parameters


def test_public_host_contract_is_importable_without_runtime_internals():
    from purra.api import AgentCore, AgentCoreRunOptions, AgentPreset
    from purra.contracts import AgentRunRequest, ExecutionRecipe, RunBinding
    from purra.ports import (
        AgentOutputPublisher,
        AgentOutputRepository,
        ContextProvider,
        DomainEventProjector,
        ModelGateway,
        RunCancellationProjector,
        RunRepository,
        ToolCatalog,
    )
    from purra.testing import (
        assert_context_provider_conforms,
        assert_execution_lease_store_conforms,
        assert_host_adapters_conform,
        assert_model_gateway_conforms,
        assert_tool_execution_gateway_conforms,
        assert_tool_idempotency_gateway_conforms,
    )

    public_contracts = (
        AgentCore,
        AgentCoreRunOptions,
        AgentPreset,
        AgentRunRequest,
        ExecutionRecipe,
        RunBinding,
        AgentOutputPublisher,
        AgentOutputRepository,
        ContextProvider,
        DomainEventProjector,
        ModelGateway,
        RunCancellationProjector,
        RunRepository,
        ToolCatalog,
        assert_context_provider_conforms,
        assert_execution_lease_store_conforms,
        assert_host_adapters_conform,
        assert_model_gateway_conforms,
        assert_tool_execution_gateway_conforms,
        assert_tool_idempotency_gateway_conforms,
    )
    assert all(value.__module__.startswith("purra.") for value in public_contracts)


def test_retrieval_contract_is_importable_from_its_public_facade():
    import purra.retrieval as retrieval
    from purra.retrieval import (
        RetrievalError,
        RetrievalHit,
        RetrievalRequest,
        Retriever,
        RetrieverTool,
    )
    from purra.testing import assert_retriever_conforms

    public_contracts = (
        RetrievalError,
        RetrievalHit,
        RetrievalRequest,
        Retriever,
        RetrieverTool,
        assert_retriever_conforms,
    )
    assert all(value.__module__.startswith("purra.") for value in public_contracts)
    assert tuple(retrieval.__all__) == (
        "RetrievalError",
        "RetrievalHit",
        "RetrievalRequest",
        "Retriever",
        "RetrieverTool",
    )
    assert not hasattr(retrieval, "build_retriever_tool_registration")
    assert not hasattr(retrieval, "create_retriever_tool")


def test_facades_do_not_reexport_private_implementation_helpers():
    import purra.engine as engine
    import purra.runtime as runtime

    assert not hasattr(runtime, "_stream_tool_batch")
    assert not hasattr(engine, "_validate_planning_constraints")
    assert not hasattr(engine, "_validate_task_constraint_refinement")


def test_public_facades_have_explicit_non_module_exports():
    facade_names = (
        "adapters",
            "agent_tree",
            "agent_tree_execution",
            "agent_tree_policy",
        "api",
        "artifacts",
        "context_orchestration",
        "contracts",
        "engine",
        "evaluation",
        "execution",
        "long_tasks",
        "model_invocation",
        "model_protocol",
        "observability",
        "operations",
        "output",
        "ports",
        "recovery",
        "retrieval",
        "runtime",
        "task_admission",
        "tools",
    )

    for name in facade_names:
        module = importlib.import_module(f"purra.{name}")
        exports = tuple(module.__all__)
        assert len(exports) == len(set(exports)), name
        assert all(
            exported and not exported.startswith("_")
            for exported in exports
        ), name
        assert all(hasattr(module, exported) for exported in exports), name
        assert not [
            exported
            for exported in exports
            if isinstance(getattr(module, exported), ModuleType)
        ], name


def test_planning_stream_public_contract_is_importable():
    from purra.api import (PLANNING_STREAM_SCHEMA, PlanningScope, PlanningProgress,
                          PlanningContext, PlanningStreamParser, PlanningStreamError, current_planning_context)
    from purra.contracts import ModelTransportDiagnostics
    from purra.model_invocation import AgentModelInvocationManager
    assert PLANNING_STREAM_SCHEMA == "purra.planning-stream/v1"
    assert callable(AgentModelInvocationManager.plan)
    assert current_planning_context() is None
    assert PlanningScope("run", "operation").revision == 0
    assert PlanningProgress("Intent", 1, 0, 10).record_index == 1
    assert all(value is None for value in ModelTransportDiagnostics().to_mapping().values())
    assert issubclass(PlanningStreamError, Exception)
    assert PlanningContext.__module__.startswith("purra.")
    parser = PlanningStreamParser()
    parser.feed('{"v":1,"type":"plan","plan":{}}\n')
    assert parser.finish() == {}
