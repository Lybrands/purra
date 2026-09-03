"""Execution-local identity for durable repository fencing."""
from contextvars import ContextVar

execution_owner = ContextVar("purra_execution_owner", default=None)
execution_claim = ContextVar("purra_execution_claim", default=None)
