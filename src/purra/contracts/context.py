"""Context contracts: budgets, claims, blocks, bundles, and receipts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from purra.contracts.enums import RunId
from purra.contracts.plans import TaskSpec
from purra.json_values import freeze_json_mapping
from purra.normalization import (
    non_negative_int,
    optional_positive_int,
    positive_int,
    required_text,
    unique_text_tuple,
)

@dataclass(frozen=True, slots=True)
class ContextBudget:
    window_tokens: int
    output_reserve_tokens: int
    safety_reserve_tokens: int
    runtime_reserve_tokens: int
    tool_schema_tokens: int = 0
    provider_input_tokens: int = 0
    minimum_message_tokens: int = 0
    context_allocations: Mapping[str, int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        numeric_fields = (
            "window_tokens",
            "output_reserve_tokens",
            "safety_reserve_tokens",
            "runtime_reserve_tokens",
            "tool_schema_tokens",
            "provider_input_tokens",
            "minimum_message_tokens",
        )
        for name in numeric_fields:
            normalizer = positive_int if name == "window_tokens" else non_negative_int
            object.__setattr__(self, name, normalizer(getattr(self, name), name))
        allocations: dict[str, int] = {}
        for raw_name, raw_tokens in self.context_allocations.items():
            name = required_text(raw_name, "context allocation name")
            tokens = non_negative_int(raw_tokens, "context allocation tokens")
            allocations[name] = tokens
        object.__setattr__(
            self,
            "context_allocations",
            freeze_json_mapping(allocations),
        )
        fixed_total = (
            self.output_reserve_tokens
            + self.safety_reserve_tokens
            + self.runtime_reserve_tokens
            + self.tool_schema_tokens
            + self.provider_input_tokens
        )
        if fixed_total > self.window_tokens:
            raise ValueError("context budget exceeds the model window")
        if sum(allocations.values()) + self.minimum_message_tokens > self.provider_input_tokens:
            raise ValueError("context allocations exceed provider input budget")

    @property
    def round_input_tokens(self) -> int:
        return self.provider_input_tokens + self.runtime_reserve_tokens

    @property
    def context_pool_tokens(self) -> int:
        return max(0, self.provider_input_tokens - self.minimum_message_tokens)

    def allocation_for(self, name: str) -> int:
        return int(self.context_allocations.get(str(name), 0))


@dataclass(frozen=True, slots=True)
class ContextBudgetClaim:
    """One domain-neutral context demand submitted to Core's allocator.

    ``minimum_tokens`` is the hard floor needed to keep the context usable,
    ``desired_tokens`` is the complete useful demand, and ``maximum_tokens``
    prevents a source from consuming space beyond that demand.  Priorities are
    compared only after every minimum has been funded.

    The first two fields intentionally preserve the former positional API.
    """

    name: str
    desired_tokens: int
    minimum_tokens: int = 0
    maximum_tokens: int | None = None
    priority: int = 0

    def __post_init__(self) -> None:
        name = required_text(self.name, "context budget claim name")
        desired = non_negative_int(
            self.desired_tokens, "context budget claim desired tokens"
        )
        minimum = non_negative_int(
            self.minimum_tokens, "context budget claim minimum tokens"
        )
        maximum = (
            desired
            if self.maximum_tokens is None
            else non_negative_int(
                self.maximum_tokens,
                "context budget claim maximum tokens",
            )
        )
        priority = int(self.priority)
        if minimum > desired:
            raise ValueError("context budget claim minimum exceeds desired")
        if desired > maximum:
            raise ValueError("context budget claim desired exceeds maximum")
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "desired_tokens", desired)
        object.__setattr__(self, "minimum_tokens", minimum)
        object.__setattr__(self, "maximum_tokens", maximum)
        object.__setattr__(self, "priority", priority)


@dataclass(frozen=True, slots=True)
class ContextBlock:
    name: str
    content: str
    token_count: int = 0
    untrusted: bool = True
    host_metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", required_text(
            self.name, "context block name"
        ))
        object.__setattr__(self, "content", str(self.content or ""))
        object.__setattr__(self, "token_count", non_negative_int(
            self.token_count, "context block token count"
        ))
        object.__setattr__(self, "untrusted", bool(self.untrusted))
        object.__setattr__(
            self,
            "host_metadata",
            freeze_json_mapping(self.host_metadata),
        )


@dataclass(frozen=True, slots=True)
class ContextEvidenceReceipt:
    """Versioned external evidence that contributed to model input."""

    evidence_id: str
    context_block: str
    source: str
    item_id: str
    version: int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "evidence_id", required_text(self.evidence_id, "evidence id")
        )
        object.__setattr__(
            self,
            "context_block",
            required_text(self.context_block, "evidence context block"),
        )
        object.__setattr__(
            self, "source", required_text(self.source, "evidence source")
        )
        object.__setattr__(
            self, "item_id", required_text(self.item_id, "evidence item id")
        )
        object.__setattr__(
            self,
            "version",
            optional_positive_int(self.version, "evidence version"),
        )
        object.__setattr__(self, "metadata", freeze_json_mapping(self.metadata))

    def to_mapping(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in {
                "evidenceId": self.evidence_id,
                "contextBlock": self.context_block,
                "source": self.source,
                "itemId": self.item_id,
                "version": self.version,
                "metadata": dict(self.metadata),
            }.items()
            if value not in (None, "", {})
        }


@dataclass(frozen=True, slots=True)
class ContextBundle:
    blocks: tuple[ContextBlock, ...] = ()
    diagnostics: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        blocks = tuple(self.blocks)
        names = [block.name for block in blocks]
        if len(names) != len(set(names)):
            raise ValueError("context block names must be unique")
        object.__setattr__(self, "blocks", blocks)
        object.__setattr__(
            self,
            "diagnostics",
            freeze_json_mapping(self.diagnostics),
        )


@dataclass(frozen=True, slots=True)
class TaskContextRequest:
    """Host-compiled requirements for post-planning context retrieval.

    ``task_spec`` carries semantic intent proposed by the planner. Every
    dependency and evidence field is compiled from host-owned tool contracts;
    the planner cannot grant itself context by emitting these values.
    """

    task_spec: TaskSpec
    planned_tool_names: tuple[str, ...] = ()
    available_tool_names: tuple[str, ...] = ()
    required_context_blocks: tuple[str, ...] = ()
    evidence_kinds: tuple[str, ...] = ()
    include_response_context: bool = False
    run_id: RunId | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.task_spec, TaskSpec):
            raise TypeError("task context request requires a TaskSpec")
        for name in (
            "planned_tool_names",
            "available_tool_names",
            "required_context_blocks",
            "evidence_kinds",
        ):
            object.__setattr__(
                self,
                name,
                unique_text_tuple(getattr(self, name)),
            )
        object.__setattr__(
            self,
            "include_response_context",
            bool(self.include_response_context),
        )
        if self.run_id is not None:
            run_id = str(self.run_id or "").strip()
            if not run_id:
                raise ValueError("task context run_id must be non-empty")
            object.__setattr__(self, "run_id", run_id)
