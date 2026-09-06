"""Receipts for private, Run-bound structured model tasks."""

from dataclasses import dataclass
from collections.abc import Mapping
from typing import Any, Literal

from purra.contracts import ModelTokenUsage
from purra.model_protocol import InvocationOutputBudget


@dataclass(frozen=True, slots=True)
class StructuredInvocationRef:
    invocation_id: str
    output_budget: InvocationOutputBudget
    usage: ModelTokenUsage | None
    dispatched: bool
    settled: bool
    error_code: str | None = None

    @property
    def usage_state(self) -> str:
        return "unknown" if self.usage is None else "reported"


@dataclass(frozen=True, slots=True)
class StructuredModelTaskReceipt:
    run_id: str
    output_contract: Mapping[str, Any]
    invocation_refs: tuple[StructuredInvocationRef, ...]
    usage: ModelTokenUsage | None
    persistence: Literal["bound", "none"]
    root_budget: Literal["bound", "not_bound"]
    validation: Literal["passed"] = "passed"

    @property
    def attempts(self) -> int:
        return len(self.invocation_refs)

    @property
    def output_budget(self) -> InvocationOutputBudget:
        return self.invocation_refs[-1].output_budget

    @property
    def usage_state(self) -> str:
        return "unknown" if self.usage is None else "reported"


@dataclass(frozen=True, slots=True)
class StructuredModelTaskResult:
    value: Mapping[str, Any]
    receipt: StructuredModelTaskReceipt


def total_usage(refs: tuple[StructuredInvocationRef, ...]) -> ModelTokenUsage | None:
    usages = [ref.usage for ref in refs]
    if any(usage is None for usage in usages):
        return None
    values = {}
    for name in ModelTokenUsage.__dataclass_fields__:
        fields = [getattr(usage, name) for usage in usages]
        values[name] = None if any(value is None for value in fields) else sum(fields)
    return ModelTokenUsage(**values)
