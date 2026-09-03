"""PurrA's sole Provider invocation API."""

from purra.model_invocation.contracts import (
    AgentModelCall,
    ManagedInvocationCompletion,
    ManagedInvocationStream,
    ModelInvocationContext,
    ModelInvocationReceipt,
)
from purra.model_invocation.manager import (
    AgentModelInvocationManager,
    ModelInvocationOutputObserver,
)
from purra.ports.evidence import ModelInputEvidenceValidator

__all__ = [
    "AgentModelCall",
    "AgentModelInvocationManager",
    "ManagedInvocationCompletion",
    "ManagedInvocationStream",
    "ModelInvocationContext",
    "ModelInvocationOutputObserver",
    "ModelInputEvidenceValidator",
    "ModelInvocationReceipt",
]
