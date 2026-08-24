"""Per-run options for the high-level PurrA pipeline."""

from __future__ import annotations

from dataclasses import dataclass, replace

from purra.agent_presets import AgentPresetSnapshot
from purra.errors import ContractViolationError
from purra.contracts import (
    ContextBudgetClaim,
    ReasoningMode,
    ResponseConstraints,
    RunBinding,
    RunProvenance,
)
from purra.normalization import (
    optional_non_negative_int,
    optional_positive_int,
    positive_int,
)
from purra.model_protocol import InvocationOutputLimit
from purra.output.contracts import (
    PublicPresentationMode,
    ResponseTransactionMode,
    ResponseTransactionPolicy,
)
from purra.output.ports import (
    AgentOutputRepository,
    CommittedResultFactsProvider,
)
from purra.ports import ResponseJudge, ResponseJudgePolicy, ResponseValidator
from purra.run_recovery import RunRecoverySnapshot
from purra.task_admission import LongTaskDispatchReceipt


@dataclass(frozen=True, slots=True)
class DurableTaskContinuation:
    """Resume one durable task from a typed, persisted Run snapshot."""

    source: RunRecoverySnapshot
    continuation_command: str
    receipt: LongTaskDispatchReceipt

    def __post_init__(self) -> None:
        if not isinstance(self.source, RunRecoverySnapshot):
            raise TypeError("durable continuation source must be RunRecoverySnapshot")
        if self.source.execution_plan is None:
            raise ValueError("durable continuation source has no ExecutionPlan")
        command = str(self.continuation_command or "").strip()
        if not command:
            raise ValueError("durable continuation command is required")
        object.__setattr__(self, "continuation_command", command)
        if not isinstance(self.receipt, LongTaskDispatchReceipt):
            raise TypeError("durable continuation receipt is invalid")


@dataclass(frozen=True, slots=True)
class AgentCoreRunOptions:
    """Per-run generic limits; domain content remains in injected adapters."""

    context_claims: tuple[ContextBudgetClaim, ...] = ()
    turn_id: str | None = None
    output_limit: InvocationOutputLimit | None = None
    default_context_window_tokens: int = 128_000
    safety_reserve_tokens: int | None = None
    runtime_reserve_tokens: int | None = None
    minimum_message_tokens: int | None = None
    model_supports_tools: bool = True
    force_planned_tool_choice: bool = True
    require_tool_call: bool | None = None
    reasoning_mode: ReasoningMode = ReasoningMode.DEFAULT
    provenance: RunProvenance | None = None
    binding: RunBinding | None = None
    response_constraints: ResponseConstraints = ResponseConstraints()
    response_validators: tuple[ResponseValidator, ...] = ()
    response_judges: tuple[ResponseJudge, ...] = ()
    response_judge_policies: tuple[ResponseJudgePolicy, ...] = ()
    response_transaction_policy: ResponseTransactionPolicy | None = None
    committed_result_facts_provider: CommittedResultFactsProvider | None = None
    durable_continuation: DurableTaskContinuation | None = None
    agent_preset_snapshot: AgentPresetSnapshot | None = None
    deadline_at_ms: int | None = None

    def __post_init__(self) -> None:
        claims = tuple(self.context_claims)
        names = [claim.name for claim in claims]
        if len(names) != len(set(names)):
            raise ValueError("context claim names must be unique")
        object.__setattr__(self, "context_claims", claims)
        normalized_turn_id = str(self.turn_id or "").strip() or None
        object.__setattr__(self, "turn_id", normalized_turn_id)
        object.__setattr__(
            self,
            "default_context_window_tokens",
            positive_int(
                self.default_context_window_tokens,
                "default_context_window_tokens",
            ),
        )
        if self.output_limit is not None and not isinstance(
            self.output_limit,
            InvocationOutputLimit,
        ):
            raise TypeError("output limit must be InvocationOutputLimit")
        for name in (
            "safety_reserve_tokens",
            "runtime_reserve_tokens",
            "minimum_message_tokens",
        ):
            object.__setattr__(self, name, optional_non_negative_int(
                getattr(self, name), name
            ))
        object.__setattr__(self, "model_supports_tools", bool(self.model_supports_tools))
        object.__setattr__(
            self,
            "force_planned_tool_choice",
            bool(self.force_planned_tool_choice),
        )
        if self.require_tool_call is not None:
            object.__setattr__(
                self,
                "require_tool_call",
                bool(self.require_tool_call),
            )
        object.__setattr__(self, "reasoning_mode", ReasoningMode(self.reasoning_mode))
        if self.provenance is not None and not isinstance(
            self.provenance,
            RunProvenance,
        ):
            raise TypeError("run provenance must be a RunProvenance value")
        if self.binding is not None and not isinstance(self.binding, RunBinding):
            raise TypeError("run binding must be a RunBinding value")
        if not isinstance(self.response_constraints, ResponseConstraints):
            raise TypeError("response constraints must be ResponseConstraints")
        validators = tuple(self.response_validators)
        if any(not isinstance(item, ResponseValidator) for item in validators):
            raise TypeError("response validators must implement ResponseValidator")
        object.__setattr__(self, "response_validators", validators)
        judges = tuple(self.response_judges)
        if any(not isinstance(item, ResponseJudge) for item in judges):
            raise TypeError("response judges must implement ResponseJudge")
        object.__setattr__(self, "response_judges", judges)
        judge_policies = tuple(self.response_judge_policies)
        if any(
            not isinstance(item, ResponseJudgePolicy)
            for item in judge_policies
        ):
            raise TypeError(
                "response judge policies must implement ResponseJudgePolicy"
            )
        object.__setattr__(self, "response_judge_policies", judge_policies)
        policy = self.response_transaction_policy
        if policy is not None and not isinstance(
            policy,
            ResponseTransactionPolicy,
        ):
            raise TypeError(
                "response transaction policy must be ResponseTransactionPolicy"
            )
        facts_provider = self.committed_result_facts_provider
        if facts_provider is not None and not isinstance(
            facts_provider,
            CommittedResultFactsProvider,
        ):
            raise TypeError(
                "committed result facts provider must implement facts_for"
            )
        if self.durable_continuation is not None and not isinstance(
            self.durable_continuation,
            DurableTaskContinuation,
        ):
            raise TypeError("durable continuation must be DurableTaskContinuation")
        if self.agent_preset_snapshot is not None and not isinstance(
            self.agent_preset_snapshot,
            AgentPresetSnapshot,
        ):
            raise TypeError(
                "agent preset snapshot must be an AgentPresetSnapshot"
            )
        object.__setattr__(
            self,
            "deadline_at_ms",
            optional_positive_int(self.deadline_at_ms, "Run deadline_at_ms"),
        )
        if (
            self.durable_continuation is not None
            and self.agent_preset_snapshot is not None
        ):
            selected_preset = self.agent_preset_snapshot.to_mapping()
            if (
                self.durable_continuation.source.agent_preset_snapshot
                != selected_preset
            ):
                raise ValueError(
                    "durable continuation must reuse the source AgentPreset snapshot"
                )
        if (
            self.durable_continuation is not None
            and self.deadline_at_ms is not None
            and self.deadline_at_ms
            != self.durable_continuation.source.deadline_at_ms
        ):
            raise ValueError(
                "durable continuation cannot replace the source Run deadline"
            )
        requires_full_text = bool(
            self.response_constraints.exact_top_level_item_count is not None
            or validators
            or judges
            or judge_policies
        )
        if (
            policy is not None
            and policy.mode is ResponseTransactionMode.DIRECT_LIVE
            and requires_full_text
        ):
            raise ValueError(
                "direct-live response cannot require full-text validation"
            )
        if (
            policy is not None
            and policy.public_presentation
            is PublicPresentationMode.MODEL_LIVE
            and facts_provider is None
        ):
            raise ValueError(
                "model-live public presentation requires a facts provider"
            )

    @property
    def resolved_response_transaction_policy(self) -> ResponseTransactionPolicy:
        if self.response_transaction_policy is not None:
            return self.response_transaction_policy
        requires_full_text = bool(
            self.response_constraints.exact_top_level_item_count is not None
            or self.response_validators
            or self.response_judges
            or self.response_judge_policies
        )
        return ResponseTransactionPolicy(
            mode=(
                ResponseTransactionMode.VALIDATED_RESULT
                if requires_full_text
                else ResponseTransactionMode.DIRECT_LIVE
            )
        )


async def restore_continuation_preset(
    options: AgentCoreRunOptions,
    output_repository: AgentOutputRepository | None,
) -> AgentCoreRunOptions:
    """Restore and validate the source Run's versioned composition authority."""

    continuation = options.durable_continuation
    if continuation is None:
        return options
    source = continuation.source
    stored = source.agent_preset_snapshot
    if not stored:
        if output_repository is None:
            raise ContractViolationError(
                "durable continuation cannot load its AgentPreset snapshot"
            )
        events = await output_repository.list_events(
            source.run_id,
            after_sequence=0,
            limit=1,
        )
        stored = events[0].payload.get("agentPreset", {}) if events else {}
    if not stored:
        raise ContractViolationError(
            "durable continuation source has no AgentPreset snapshot"
        )
    try:
        snapshot = AgentPresetSnapshot.from_mapping(stored)
    except (TypeError, ValueError) as error:
        raise ContractViolationError(
            "durable continuation requires a complete AgentPreset snapshot version 3",
            code="agent_preset_snapshot_unsupported",
        ) from error
    if (
        options.agent_preset_snapshot is not None
        and options.agent_preset_snapshot != snapshot
    ):
        raise ContractViolationError(
            "durable continuation selected a different AgentPreset snapshot"
        )
    restored = replace(
        source,
        agent_preset_snapshot=snapshot.to_mapping(),
    )
    return replace(
        options,
        agent_preset_snapshot=snapshot,
        deadline_at_ms=source.deadline_at_ms,
        durable_continuation=replace(continuation, source=restored),
    )
