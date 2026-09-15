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
from purra.model_invocation.factory import create_model_invocation_manager
from purra.ports.evidence import ModelInputEvidenceValidator

__all__ = [
    "AgentModelCall",
    "AgentModelInvocationManager",
    "create_model_invocation_manager",
    "ManagedInvocationCompletion",
    "ManagedInvocationStream",
    "ModelInvocationContext",
    "ModelInvocationOutputObserver",
    "ModelInputEvidenceValidator",
    "ModelInvocationReceipt",
]
