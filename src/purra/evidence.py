"""Run-scoped evidence ledger and deterministic tool-result receipts."""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from typing import Any, Mapping, Sequence

from purra.context_budget import estimate_text_tokens
from purra.contracts import (
    AgentMessage,
    MessageRole,
    ToolBatchResult,
    ToolCall,
    ToolCallResult,
)


CONTEXT_EVIDENCE_RECEIPTS_KEY = "context_evidence_receipts"


@dataclass(frozen=True, slots=True)
class EvidenceRecord:
    evidence_id: str
    tool_call_id: str
    tool_name: str
    arguments_json: str
    content: str
    token_estimate: int
    from_cache: bool = False
    error_code: str | None = None
    effects: tuple[Mapping[str, Any], ...] = ()


@dataclass(frozen=True, slots=True)
class ToolResultReceipt:
    evidence_id: str
    tool_call_id: str
    tool_name: str
    status: str
    content_characters: int
    token_estimate: int
    from_cache: bool = False
    error_code: str | None = None
    effect_types: tuple[str, ...] = ()
    summary: str = ""

    def to_mapping(self, *, include_summary: bool = True) -> dict[str, Any]:
        return {
            key: value
            for key, value in {
                "evidenceId": self.evidence_id,
                "toolCallId": self.tool_call_id,
                "tool": self.tool_name,
                "status": self.status,
                "contentCharacters": self.content_characters,
                "tokenEstimate": self.token_estimate,
                "fromCache": self.from_cache,
                "errorCode": self.error_code,
                "effectTypes": list(self.effect_types),
                "summary": self.summary if include_summary else "",
            }.items()
            if value not in (None, "", [], False)
        }


@dataclass(frozen=True, slots=True)
class ContextEvidenceReceipt:
    """Receipt for one host-selected context fact used by this run."""

    evidence_id: str
    context_block: str
    source: str
    item_id: str
    version: int | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

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


def context_evidence_receipts(
    messages: Sequence[AgentMessage],
) -> tuple[ContextEvidenceReceipt, ...]:
    """Extract host-declared context provenance from model-visible messages."""

    return tuple(receipt for _block, _content, receipt in _context_receipt_rows(
        messages
    ))


@dataclass(slots=True)
class RunEvidenceStore:
    """Keep full tool evidence outside the ever-growing protocol transcript."""

    _records: dict[str, EvidenceRecord] = field(default_factory=dict)
    _tool_result_receipts: dict[str, ToolResultReceipt] = field(
        default_factory=dict
    )
    _context_receipts: dict[str, ContextEvidenceReceipt] = field(
        default_factory=dict
    )
    _context_blocks: dict[str, str] = field(default_factory=dict)

    def record_context_messages(
        self,
        messages: Sequence[AgentMessage],
    ) -> tuple[ContextEvidenceReceipt, ...]:
        """Persist host context facts separately from protocol projection."""

        recorded: list[ContextEvidenceReceipt] = []
        for message in messages:
            block_name = str(message.attributes.get("context_name") or "").strip()
            raw_receipts = message.host_metadata.get(
                CONTEXT_EVIDENCE_RECEIPTS_KEY
            )
            if block_name and isinstance(raw_receipts, Sequence) and not isinstance(
                raw_receipts,
                (str, bytes, bytearray),
            ):
                self._context_blocks[block_name] = str(message.content or "")
        for _block_name, _content, receipt in _context_receipt_rows(messages):
            self._context_receipts[receipt.evidence_id] = receipt
            recorded.append(receipt)
        return tuple(recorded)

    def record_batch(
        self,
        calls: Sequence[ToolCall],
        batch: ToolBatchResult,
    ) -> tuple[ToolResultReceipt, ...]:
        by_call = {call.id: call for call in calls}
        receipts: list[ToolResultReceipt] = []
        for result in batch.results:
            call = by_call.get(result.tool_call_id)
            if call is None:
                continue
            record, receipt = _record_and_receipt(call, result)
            self._records[record.evidence_id] = record
            self._tool_result_receipts[receipt.tool_call_id] = receipt
            receipts.append(receipt)
        return tuple(receipts)

    def get(self, evidence_id: str) -> EvidenceRecord | None:
        return self._records.get(str(evidence_id or "").strip())

    def tool_result_receipts(self) -> tuple[ToolResultReceipt, ...]:
        return tuple(self._tool_result_receipts.values())

    def context_receipts(self) -> tuple[ContextEvidenceReceipt, ...]:
        return tuple(self._context_receipts.values())

    def checkpoint_mapping(self) -> dict[str, Any]:
        """Return the private JSON state needed for a model-ready resume."""

        return {
            "records": [
                {
                    "evidenceId": item.evidence_id,
                    "toolCallId": item.tool_call_id,
                    "toolName": item.tool_name,
                    "argumentsJson": item.arguments_json,
                    "content": item.content,
                    "tokenEstimate": item.token_estimate,
                    "fromCache": item.from_cache,
                    "errorCode": item.error_code,
                    "effects": [dict(effect) for effect in item.effects],
                }
                for item in self._records.values()
            ],
            "toolResultReceipts": [
                item.to_mapping() for item in self._tool_result_receipts.values()
            ],
            "contextReceipts": [
                item.to_mapping() for item in self._context_receipts.values()
            ],
            "contextBlocks": dict(self._context_blocks),
        }

    @classmethod
    def from_checkpoint_mapping(
        cls,
        value: Mapping[str, Any],
    ) -> "RunEvidenceStore":
        store = cls()
        for raw in _mapping_rows(value.get("records")):
            record = EvidenceRecord(
                evidence_id=str(raw.get("evidenceId") or ""),
                tool_call_id=str(raw.get("toolCallId") or ""),
                tool_name=str(raw.get("toolName") or ""),
                arguments_json=str(raw.get("argumentsJson") or ""),
                content=str(raw.get("content") or ""),
                token_estimate=int(raw.get("tokenEstimate") or 0),
                from_cache=bool(raw.get("fromCache")),
                error_code=(
                    str(raw["errorCode"])
                    if raw.get("errorCode") is not None
                    else None
                ),
                effects=tuple(
                    dict(effect)
                    for effect in _mapping_rows(raw.get("effects"))
                ),
            )
            store._records[record.evidence_id] = record
        for raw in _mapping_rows(value.get("toolResultReceipts")):
            receipt = ToolResultReceipt(
                evidence_id=str(raw.get("evidenceId") or ""),
                tool_call_id=str(raw.get("toolCallId") or ""),
                tool_name=str(raw.get("tool") or ""),
                status=str(raw.get("status") or ""),
                content_characters=int(raw.get("contentCharacters") or 0),
                token_estimate=int(raw.get("tokenEstimate") or 0),
                from_cache=bool(raw.get("fromCache")),
                error_code=(
                    str(raw["errorCode"])
                    if raw.get("errorCode") is not None
                    else None
                ),
                effect_types=tuple(str(item) for item in raw.get("effectTypes") or ()),
                summary=str(raw.get("summary") or ""),
            )
            store._tool_result_receipts[receipt.tool_call_id] = receipt
        for raw in _mapping_rows(value.get("contextReceipts")):
            receipt = ContextEvidenceReceipt(
                evidence_id=str(raw.get("evidenceId") or ""),
                context_block=str(raw.get("contextBlock") or ""),
                source=str(raw.get("source") or ""),
                item_id=str(raw.get("itemId") or ""),
                version=(
                    int(raw["version"])
                    if raw.get("version") is not None
                    else None
                ),
                metadata=(
                    dict(raw["metadata"])
                    if isinstance(raw.get("metadata"), Mapping)
                    else {}
                ),
            )
            store._context_receipts[receipt.evidence_id] = receipt
        blocks = value.get("contextBlocks")
        if isinstance(blocks, Mapping):
            store._context_blocks.update({
                str(name): str(content)
                for name, content in blocks.items()
            })
        return store

    @property
    def token_estimate(self) -> int:
        return sum(record.token_estimate for record in self._records.values())

    def project_messages_for_planning(
        self,
        messages: Sequence[AgentMessage],
        *,
        max_excerpt_characters: int = 4_000,
    ) -> tuple[AgentMessage, ...]:
        """Replace raw tool payloads with bounded, metadata-bearing receipts.

        The dynamic planner already consumed at most 4k characters per tool
        observation.  Keeping the same excerpt bound avoids a semantic
        regression while removing protocol noise and making completeness
        explicit.
        """

        projected: list[AgentMessage] = []
        for message in messages:
            if message.role is not MessageRole.TOOL or not message.tool_call_id:
                projected.append(message)
                continue
            receipt = self._tool_result_receipts.get(message.tool_call_id)
            if receipt is None:
                projected.append(message)
                continue
            record = self._records.get(receipt.evidence_id)
            excerpt = record.content if record is not None else ""
            if len(excerpt) > max_excerpt_characters:
                excerpt = excerpt[:max_excerpt_characters] + "…"
            payload = receipt.to_mapping(include_summary=False)
            payload["excerpt"] = excerpt
            payload["completeEvidenceStoredByHost"] = record is not None
            projected.append(replace(
                message,
                content=json.dumps(
                    payload,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
            ))
        return tuple(projected)

    def receipt_message(
        self,
        message: AgentMessage,
    ) -> AgentMessage | None:
        """Build a loss-aware protocol replacement for one stored tool result."""

        if message.role is not MessageRole.TOOL or not message.tool_call_id:
            return None
        receipt = self._tool_result_receipts.get(message.tool_call_id)
        if receipt is None:
            return None
        payload = receipt.to_mapping()
        payload["completeEvidenceStoredByHost"] = (
            receipt.evidence_id in self._records
        )
        return replace(
            message,
            content=json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        )


def _record_and_receipt(
    call: ToolCall,
    result: ToolCallResult,
) -> tuple[EvidenceRecord, ToolResultReceipt]:
    evidence_id = f"tool:{call.id}"
    content = str(result.content or "")
    token_estimate = estimate_text_tokens(content)
    effect_rows = tuple({
        "type": effect.type,
        "payload": dict(effect.payload),
    } for effect in result.effects)
    error = result.error
    status = "failed" if error else "completed"
    summary = content.strip().replace("\n", " ")[:480]
    record = EvidenceRecord(
        evidence_id=evidence_id,
        tool_call_id=call.id,
        tool_name=call.name,
        arguments_json=call.arguments_json,
        content=content,
        token_estimate=token_estimate,
        from_cache=result.from_cache,
        error_code=error,
        effects=effect_rows,
    )
    receipt = ToolResultReceipt(
        evidence_id=evidence_id,
        tool_call_id=call.id,
        tool_name=call.name,
        status=status,
        content_characters=len(content),
        token_estimate=token_estimate,
        from_cache=result.from_cache,
        error_code=error,
        effect_types=tuple(effect.type for effect in result.effects),
        summary=summary,
    )
    return record, receipt


def _mapping_rows(value: Any) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _context_receipt_rows(
    messages: Sequence[AgentMessage],
) -> tuple[tuple[str, str, ContextEvidenceReceipt], ...]:
    rows: list[tuple[str, str, ContextEvidenceReceipt]] = []
    for message in messages:
        block_name = str(message.attributes.get("context_name") or "").strip()
        raw_receipts = message.host_metadata.get(CONTEXT_EVIDENCE_RECEIPTS_KEY)
        if not block_name or not (
            isinstance(raw_receipts, Sequence)
            and not isinstance(raw_receipts, (str, bytes, bytearray))
        ):
            continue
        for raw in raw_receipts:
            if not isinstance(raw, Mapping):
                continue
            evidence_id = str(raw.get("evidenceId") or "").strip()
            source = str(raw.get("source") or "").strip()
            item_id = str(raw.get("itemId") or "").strip()
            if not evidence_id or not source or not item_id:
                continue
            rows.append((
                block_name,
                str(message.content or ""),
                ContextEvidenceReceipt(
                    evidence_id=evidence_id,
                    context_block=block_name,
                    source=source,
                    item_id=item_id,
                    version=_optional_int(raw.get("version")),
                    metadata={
                        str(key): value
                        for key, value in raw.items()
                        if str(key) not in {
                            "evidenceId",
                            "source",
                            "itemId",
                            "version",
                        }
                    },
                ),
            ))
    return tuple(rows)


def _optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


__all__ = [
    "CONTEXT_EVIDENCE_RECEIPTS_KEY",
    "ContextEvidenceReceipt",
    "EvidenceRecord",
    "RunEvidenceStore",
    "ToolResultReceipt",
    "context_evidence_receipts",
]
