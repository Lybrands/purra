"""Stable recursive Agent identities, immutable Run chains, and their storage.

Contracts live in ``contracts.py``, the storage port in ``ports.py``, and the
in-memory reference adapter in ``memory.py``. This facade re-exports the
surface previously provided by the flat ``purra.agent_tree`` module.
"""

from purra.agent_tree.contracts import (
    AgentCapabilityGrant,
    AgentNode,
    AgentNodeState,
    AgentRunAggregation,
    AgentTreeRun,
    AgentTreeRunStatus,
    BeginRootAgentCommand,
    ChildAgentSpec,
    ContextCheckpoint,
    ContinueAgentCommand,
    ContinueAgentReceipt,
    SpawnAgentsCommand,
    SpawnAgentsReceipt,
    SpawnedAgent,
    validate_stored_dependencies,
)
from purra.agent_tree.ports import RunTreeRepository
from purra.agent_tree.memory import InMemoryRunTreeRepository

__all__ = [
    "AgentCapabilityGrant",
    "AgentNode",
    "AgentNodeState",
    "AgentRunAggregation",
    "AgentTreeRun",
    "AgentTreeRunStatus",
    "BeginRootAgentCommand",
    "ChildAgentSpec",
    "ContextCheckpoint",
    "ContinueAgentCommand",
    "ContinueAgentReceipt",
    "InMemoryRunTreeRepository",
    "RunTreeRepository",
    "SpawnAgentsCommand",
    "SpawnAgentsReceipt",
    "SpawnedAgent",
    "validate_stored_dependencies",
]
