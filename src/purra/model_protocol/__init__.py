"""Provider-neutral model response protocol classification."""

from purra.model_protocol.capabilities import (
    AssistantContentWithToolCalls,
    ContinuationKind,
    ContinuationSafety,
    FeatureSupport,
    LengthReasonDetail,
    ModelCapabilitySnapshot,
    ModelOutputCapabilities,
    ModelProtocolCapabilities,
    ReasoningControl,
    ReasoningLimitKind,
    ReasoningReplayPolicy,
    ReasoningUsageDetail,
    ThinkingTokenAccounting,
    VisibleOutputReservation,
    generic_capability_snapshot,
)
from purra.model_protocol.requirements import (
    FeatureRequirement,
    TaskCapabilityRequirements,
    preflight_capabilities,
)
from purra.model_protocol.output_limits import (
    GenerationBudgetSource,
    InvocationOutputBudget,
    ResultCapacitySource,
    constrain_output_budget_to_context,
    require_output_budget_matches_request,
    resolve_invocation_output_budget,
)

from purra.model_protocol.termination import (
    InvocationTermination,
    ModelTermination,
    classify_model_termination,
)

__all__ = [
    "AssistantContentWithToolCalls",
    "ContinuationKind",
    "ContinuationSafety",
    "FeatureRequirement",
    "FeatureSupport",
    "GenerationBudgetSource",
    "InvocationOutputBudget",
    "InvocationTermination",
    "LengthReasonDetail",
    "ModelCapabilitySnapshot",
    "ModelOutputCapabilities",
    "ModelProtocolCapabilities",
    "ModelTermination",
    "ReasoningControl",
    "ReasoningLimitKind",
    "ReasoningReplayPolicy",
    "ReasoningUsageDetail",
    "ResultCapacitySource",
    "TaskCapabilityRequirements",
    "ThinkingTokenAccounting",
    "VisibleOutputReservation",
    "classify_model_termination",
    "constrain_output_budget_to_context",
    "generic_capability_snapshot",
    "preflight_capabilities",
    "require_output_budget_matches_request",
    "resolve_invocation_output_budget",
]

from purra.model_protocol.diagnostics import ModelTransportDiagnostics

__all__ = [*__all__, "ModelTransportDiagnostics"]
