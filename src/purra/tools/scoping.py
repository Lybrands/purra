"""Shared batch-level tool scope classification.

``runtime.tool_authorization`` (soft, model-retry disposition) and
``tools.executor`` (hard, fail-closed disposition) are two defense layers
over the same name-subset semantics. This module is their single source of
truth so the layers cannot drift in subset logic or error-code meaning.
The authoritative allowed set itself is derived by the Run state machine.
"""

from __future__ import annotations

from collections.abc import Set


def outside_scope_names(
    requested_names: Set[str],
    *,
    allowed_names: Set[str],
) -> frozenset[str]:
    """Requested tools the current execution scope does not authorize."""
    return frozenset(requested_names - allowed_names)


def unregistered_names(
    requested_names: Set[str],
    *,
    registered_names: Set[str],
) -> frozenset[str]:
    """Requested tools with no registration in the tool catalog."""
    return frozenset(requested_names - set(registered_names))


def scope_names_missing_registration(
    allowed_names: Set[str],
    *,
    registered_names: Set[str],
) -> frozenset[str]:
    """Scope entries that name no registered tool (a misconfigured scope)."""
    return frozenset(set(allowed_names) - set(registered_names))


def batch_overlaps_future_tools(
    requested_names: Set[str],
    *,
    allowed_names: Set[str],
    future_names: Set[str],
) -> bool:
    """Whether the batch reaches for any tool that is only future-scoped."""
    return bool(requested_names - allowed_names) and bool(requested_names & future_names)


def batch_within_allowed_or_future(
    requested_names: Set[str],
    *,
    allowed_names: Set[str],
    future_names: Set[str],
) -> bool:
    """Whether the batch is fully covered once future tools are promoted."""
    return requested_names <= (allowed_names | future_names)


__all__ = [
    "batch_overlaps_future_tools",
    "batch_within_allowed_or_future",
    "outside_scope_names",
    "scope_names_missing_registration",
    "unregistered_names",
]
