"""Pure ordered selection of host-authorized model bindings; no Run dispatch."""
from dataclasses import dataclass
from collections.abc import Sequence

from purra.errors import ContractViolationError, UnsupportedModelFeatureError
from purra.model_protocol import ModelCapabilitySnapshot, TaskCapabilityRequirements, preflight_capabilities


@dataclass(frozen=True, slots=True)
class ModelRouteCandidate:
    binding_id: str
    revision: str
    config_identity: str
    capabilities: ModelCapabilitySnapshot

    def __post_init__(self):
        for name in ("binding_id", "revision", "config_identity"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip() or value != value.strip():
                raise ValueError(f"{name} must be nonempty canonical text")
        if not isinstance(self.capabilities, ModelCapabilitySnapshot):
            raise TypeError("capabilities must be a ModelCapabilitySnapshot")


def select_model_route(
    candidates: Sequence[ModelRouteCandidate],
    allowed_binding_ids: Sequence[str],
    requirements: TaskCapabilityRequirements,
) -> ModelRouteCandidate:
    """Select the first compatible allowed candidate in registration order.

    The result is selection data only, never execution or recovery authority.
    """
    rows = tuple(candidates)
    if not all(isinstance(row, ModelRouteCandidate) for row in rows):
        raise TypeError("invalid model route candidate")
    ids = [row.binding_id for row in rows]
    allowed = tuple(allowed_binding_ids)
    if len(set(ids)) != len(ids) or any(not isinstance(key, str) or key not in ids for key in allowed):
        raise ValueError("duplicate candidate or unknown allowed binding")
    if not isinstance(requirements, TaskCapabilityRequirements):
        raise TypeError("invalid task capability requirements")
    for row in rows:
        if row.binding_id not in allowed:
            continue
        try:
            preflight_capabilities(row.capabilities, requirements)
        except UnsupportedModelFeatureError:
            continue
        if row.capabilities.max_generation_tokens is not None:
            return row
    raise ContractViolationError("No authorized compatible model binding", code="model_route_unavailable")
