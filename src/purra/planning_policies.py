"""Business-neutral planning policies for the standard PurrA execution styles."""

from __future__ import annotations

from purra.contracts import (
    AgentRunRequest,
    PlanningCapabilities,
    PlanningConstraints,
)


class ReactivePlanningPolicy:
    """Run the ordinary model/tool loop without creating a WorkPlan."""

    def planning_constraints(
        self,
        request: AgentRunRequest,
        capabilities: PlanningCapabilities,
    ) -> PlanningConstraints:
        del request, capabilities
        return PlanningConstraints()

    def should_plan(
        self,
        request: AgentRunRequest,
        capabilities: PlanningCapabilities,
    ) -> bool:
        del request, capabilities
        return False


class ToolPlanningPolicy:
    """Create a plan whenever the request exposes at least one tool."""

    def planning_constraints(
        self,
        request: AgentRunRequest,
        capabilities: PlanningCapabilities,
    ) -> PlanningConstraints:
        del request, capabilities
        return PlanningConstraints()

    def should_plan(
        self,
        request: AgentRunRequest,
        capabilities: PlanningCapabilities,
    ) -> bool:
        return bool(request.tools_enabled and capabilities.available_tool_names)

__all__ = [
    "ReactivePlanningPolicy",
    "ToolPlanningPolicy",
]
