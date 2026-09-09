"""Pure ordered selection of host-authorized model bindings; no Run dispatch."""
from dataclasses import dataclass, replace
from collections.abc import Sequence, Mapping, Callable, Awaitable
from typing import Generic, TypeVar
from types import MappingProxyType
from purra.json_values import thaw_json_mapping
import json

from purra.errors import ContractViolationError, UnsupportedModelFeatureError
from purra.model_protocol import ModelCapabilitySnapshot, TaskCapabilityRequirements, preflight_capabilities


def _allowed_ids(value: Sequence[str]) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError('authorized bindings must be a sequence of IDs')
    ids = tuple(value)
    if any(not isinstance(key, str) or not key.strip() or key != key.strip() for key in ids):
        raise ValueError('authorized binding IDs must be nonempty canonical text')
    return ids


@dataclass(frozen=True, slots=True)
class ModelRouteCandidate:
    binding_id: str
    revision: str
    config_identity: str
    capabilities: ModelCapabilitySnapshot
    policy_id: str | None = None
    policy_revision: str | None = None

    def __post_init__(self):
        for name in ("binding_id", "revision", "config_identity"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip() or value != value.strip():
                raise ValueError(f"{name} must be nonempty canonical text")
        if not isinstance(self.capabilities, ModelCapabilitySnapshot):
            raise TypeError("capabilities must be a ModelCapabilitySnapshot")
        if (self.policy_id is None) != (self.policy_revision is None):
            raise ValueError("policy identity requires both id and revision")
        for value in (self.policy_id, self.policy_revision):
            if value is not None and (not isinstance(value, str) or not value.strip() or value != value.strip()):
                raise ValueError("invalid policy identity")

    def to_mapping(self):
        return {"bindingId": self.binding_id, "revision": self.revision,
                "configIdentity": self.config_identity, "capabilities": self.capabilities.to_mapping(),
                **({"policyId": self.policy_id, "policyRevision": self.policy_revision}
                   if self.policy_id is not None else {})}


def select_model_route(
    candidates: Sequence[ModelRouteCandidate],
    allowed_binding_ids: Sequence[str],
    requirements: TaskCapabilityRequirements,
    *, policy_id: str | None = None, policy_revision: str | None = None,
) -> ModelRouteCandidate:
    """Select the first compatible allowed candidate in registration order.

    The result is selection data only, never execution or recovery authority.
    """
    rows = tuple(candidates)
    # Validate policy even when there are no eligible candidates.
    if (policy_id is None) != (policy_revision is None) or any(
        value is not None and (not isinstance(value, str) or not value.strip() or value != value.strip())
        for value in (policy_id, policy_revision)
    ):
        raise ValueError("invalid policy identity")
    if not all(isinstance(row, ModelRouteCandidate) for row in rows):
        raise TypeError("invalid model route candidate")
    ids = [row.binding_id for row in rows]
    allowed = _allowed_ids(allowed_binding_ids)
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
            return replace(row, policy_id=policy_id, policy_revision=policy_revision) if policy_id is not None else row
    raise ContractViolationError("No authorized compatible model binding", code="model_route_unavailable")


def resolve_model_route(candidates: Sequence[ModelRouteCandidate], saved: Mapping[str, object],
                        allowed_binding_ids: Sequence[str]) -> ModelRouteCandidate:
    """Resolve canonical saved selection without rerunning current selection policy.

    Host must read saved from the Run preset, and still call public resume.
    """
    rows = tuple(candidates)
    if not all(isinstance(row, ModelRouteCandidate) for row in rows):
        raise TypeError("invalid model route candidate")
    if len({row.binding_id for row in rows}) != len(rows):
        raise ValueError("duplicate model route candidate")
    allowed = _allowed_ids(allowed_binding_ids)
    value = thaw_json_mapping(saved)
    for row in rows:
        if row.binding_id == value.get('bindingId') and row.binding_id in allowed:
            resolved = replace(row, policy_id=value.get('policyId'), policy_revision=value.get('policyRevision'))
            expected = resolved.to_mapping()
            # Python presets also carry requestIdentity, checked by public resume.
            actual = {key: item for key, item in value.items() if key != 'requestIdentity'}
            if json.dumps(expected, sort_keys=True, allow_nan=False) == json.dumps(actual, sort_keys=True, allow_nan=False):
                return resolved
    raise ContractViolationError("Saved model route is missing, revoked or changed", code="model_route_mismatch")


Host = TypeVar('Host')


@dataclass(frozen=True, slots=True)
class ModelRouteBinding(Generic[Host]):
    candidate: ModelRouteCandidate
    create: Callable[[ModelRouteCandidate], Awaitable[Host]]

    def __post_init__(self):
        if not isinstance(self.candidate, ModelRouteCandidate) or not callable(self.create):
            raise TypeError('Model route binding requires a candidate and async host factory')


class ModelRouteRegistry(Generic[Host]):
    """Snapshot host factories; construct per-call hosts without shared selection state.

    Factories must apply the supplied route to the Agent preset. The caller owns
    the returned host's lifetime and invokes public submit/resume itself.
    """

    def __init__(self, bindings: Sequence[ModelRouteBinding[Host]]):
        rows = tuple(bindings)
        if not all(isinstance(row, ModelRouteBinding) for row in rows):
            raise TypeError('Invalid model route binding')
        if len({row.candidate.binding_id for row in rows}) != len(rows):
            raise ValueError('Duplicate model route binding')
        self._bindings = MappingProxyType({row.candidate.binding_id: row for row in rows})
        self._candidates = tuple(row.candidate for row in rows)

    async def create_new(self, allowed_binding_ids: Sequence[str], requirements: TaskCapabilityRequirements,
                         *, policy_id: str | None = None, policy_revision: str | None = None) -> Host:
        route = select_model_route(self._candidates, allowed_binding_ids, requirements,
                                   policy_id=policy_id, policy_revision=policy_revision)
        return await self._bindings[route.binding_id].create(route)

    async def create_recovery(self, saved: Mapping[str, object], allowed_binding_ids: Sequence[str]) -> Host:
        route = resolve_model_route(self._candidates, saved, allowed_binding_ids)
        return await self._bindings[route.binding_id].create(route)
