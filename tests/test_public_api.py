from __future__ import annotations

def test_complete_run_api_is_importable_from_the_package_boundary():
    from purra.api import (
        AgentCore,
        AgentCoreRunOptions,
        AgentPreset,
        AgentPresetSnapshot,
        AgentPlanner,
        AgentRunHandle,
        ContextStrategy,
        DelegatedAgentExecutor,
        DelegatedAgentRequest,
        DelegatedAgentResult,
        DelegationContextMode,
        DelegationPolicy,
        DynamicDelegatedAgentExecutor,
        ExecutionProfile,
        InMemoryAgentAdapters,
        ReactivePlanningPolicy,
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
        ToolPlanningPolicy,
        WorkPlan,
        WorkPlanner,
        WorkStep,
        canonicalize_execution_plan,
        decide_orphan_run,
    )

    assert AgentCore.__module__.startswith("purra.")
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
    assert AgentCoreRunOptions.__module__.startswith("purra.")
    assert AgentRunHandle.__module__.startswith("purra.")
    assert ContextStrategy.SINGLE_PASS.value == "single_pass"
    assert DelegationContextMode.ISOLATED.value == "isolated"
    assert DelegationPolicy().max_agents_per_call == 3
    assert DelegatedAgentExecutor.__module__.startswith("purra.")
    assert DynamicDelegatedAgentExecutor.__module__.startswith("purra.")
    assert DelegatedAgentRequest.__module__.startswith("purra.")
    assert DelegatedAgentResult.__module__.startswith("purra.")
    assert ExecutionProfile().context_strategy is ContextStrategy.SINGLE_PASS
    adapters = InMemoryAgentAdapters()
    assert adapters.runs is not adapters.outputs
    assert adapters.outputs is not adapters.publisher
    assert ReactivePlanningPolicy.__module__.startswith("purra.")
    assert ToolPlanningPolicy.__module__.startswith("purra.")
    assert AgentPlanner.__module__.startswith("purra.")
    assert WorkPlan.__module__.startswith("purra.")
    assert WorkStep.__module__.startswith("purra.")
    assert WorkPlanner.__module__.startswith("purra.")
    assert canonicalize_execution_plan.__module__.startswith("purra.")
    assert hasattr(AgentCore, "submit")
    assert not hasattr(AgentCore, "run")


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
        assert_delegation_repository_conforms,
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
        assert_delegation_repository_conforms,
        assert_execution_lease_store_conforms,
        assert_host_adapters_conform,
        assert_model_gateway_conforms,
        assert_tool_execution_gateway_conforms,
        assert_tool_idempotency_gateway_conforms,
    )
    assert all(value.__module__.startswith("purra.") for value in public_contracts)


def test_facades_do_not_reexport_private_implementation_helpers():
    import purra.engine as engine
    import purra.runtime as runtime

    assert not hasattr(runtime, "_stream_tool_batch")
    assert not hasattr(engine, "_validate_planning_constraints")
    assert not hasattr(engine, "_validate_task_constraint_refinement")
