"""Engine-owned assembly for the optional one-Run delegation capability."""

from __future__ import annotations

from purra.contracts import RuntimeLimits, ToolExecutionLimits
from purra.delegation.coordinator import (
    DelegatedAgentExecutor,
    DelegationCoordinator,
)
from purra.delegation.dynamic_executor import DynamicDelegatedAgentExecutor
from purra.delegation.policy import DelegationPolicy
from purra.delegation.tool import build_delegation_tool_registration
from purra.engine.tool_catalog import AugmentedToolCatalog
from purra.model_invocation import AgentModelInvocationManager
from purra.operations import AgentOperationController
from purra.output.processor import AgentOutputProcessor
from purra.ports import (
    ApprovalGateway,
    ContextProvider,
    ConversationCompactor,
    DelegationRepository,
    ExecutionStateFactory,
    ModelGateway,
    ToolCatalog,
    ToolIdempotencyGateway,
)
from purra.recovery import RecoveryPolicy


def assemble_delegation(
    *,
    base_tool_catalog: ToolCatalog,
    policy: DelegationPolicy | None,
    repository: DelegationRepository | None,
    executor: DelegatedAgentExecutor | None,
    idempotency: ToolIdempotencyGateway | None,
    output: AgentOutputProcessor | None,
    operations: AgentOperationController | None,
    model_gateway: ModelGateway,
    model_manager: AgentModelInvocationManager,
    approval: ApprovalGateway,
    context_provider: ContextProvider | None,
    context_provider_factory,
    conversation_compactor: ConversationCompactor | None,
    conversation_compactor_factory,
    execution_state_factory: ExecutionStateFactory | None,
    runtime_limits: RuntimeLimits,
    recovery_policy: RecoveryPolicy,
    tool_execution_limits: ToolExecutionLimits,
) -> tuple[
    DelegationCoordinator | None,
    DynamicDelegatedAgentExecutor | None,
    ToolCatalog,
]:
    if repository is None:
        if executor is not None:
            raise ValueError(
                "delegated Agent executor requires a delegation repository"
            )
        if policy is not None:
            raise ValueError("delegation policy requires a delegation repository")
        return None, None, base_tool_catalog
    if policy is None:
        raise ValueError(
            "delegation repository requires an enabled delegation policy"
        )
    if idempotency is None:
        raise ValueError("delegated Agents require a tool idempotency gateway")
    if output is None or operations is None:
        raise ValueError("delegation requires canonical output infrastructure")

    dynamic: DynamicDelegatedAgentExecutor | None = None
    if executor is None:
        dynamic = DynamicDelegatedAgentExecutor(
            tool_catalog=base_tool_catalog,
            model_gateway=model_gateway,
            model_manager=model_manager,
            output_processor=output,
            operation_controller=operations,
            approval_gateway=approval,
            tool_idempotency_gateway=idempotency,
            policy=policy,
            context_provider=context_provider,
            context_provider_factory=context_provider_factory,
            conversation_compactor=conversation_compactor,
            conversation_compactor_factory=conversation_compactor_factory,
            execution_state_factory=execution_state_factory,
            runtime_limits=runtime_limits,
            recovery_policy=recovery_policy,
            tool_execution_limits=tool_execution_limits,
        )
        executor = dynamic
    coordinator = DelegationCoordinator(
        repository=repository,
        executor=executor,
        output_processor=output,
        operation_controller=operations,
        max_parallel=policy.max_parallel,
    )
    return (
        coordinator,
        dynamic,
        AugmentedToolCatalog(
            base_tool_catalog,
            (build_delegation_tool_registration(coordinator, policy),),
        ),
    )


__all__: list[str] = []
