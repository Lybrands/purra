"""Project whole memory records through PurrA's existing context contracts."""

import json
from dataclasses import dataclass

from purra.contracts import ContextBlock, ContextBudgetClaim, ContextBundle
from purra.context_budget import estimate_json_tokens
from purra.evidence import CONTEXT_EVIDENCE_RECEIPTS_KEY, ContextEvidenceReceipt
from purra.retrieval import RetrievalRequest

from .memory import _integer, _text


@dataclass(frozen=True, slots=True)
class MemoryContextResult:
    block: ContextBlock | None
    included: tuple[str, ...]
    deferred: tuple[str, ...]
    missing: tuple[str, ...]
    receipts: tuple[ContextEvidenceReceipt, ...]


async def assemble_memory_context(memory, ids, allowance, *, name="memory",
                                  count_tokens=estimate_json_tokens, expected_epoch=None, signal=None):
    """One assembly path for Agent context and authorized non-Run consumers.

    Hosts supply ordered IDs, never trusted replacement text. A fresh component
    read enforces visibility; only whole selected records receive receipts.
    """
    if type(allowance) is not int or not 0 <= allowance <= 1_000_000:
        raise ValueError("invalid context allowance")
    name = _text(name, "context name", 128)
    epoch = memory.epoch if expected_epoch is None else expected_epoch
    memory.assert_epoch(epoch)
    hits = await memory.select(ids, signal=signal)
    chosen, deferred, rows, content, tokens = [], [], [], "", 0
    for hit in hits:
        row = {"id": hit.id, "version": hit.version, "text": hit.content,
               "sourceId": hit.metadata["sourceId"], "sourceRevision": hit.metadata["sourceRevision"],
               "metadata": dict(hit.metadata["metadata"])}
        candidate = json.dumps(rows + [row], ensure_ascii=False, separators=(",", ":"))
        count = count_tokens(candidate)
        if type(count) is not int or count < 0:
            raise ValueError("count_tokens must return a non-negative integer")
        count = max(count, estimate_json_tokens(candidate))
        if count <= allowance:
            rows.append(row)
            chosen.append(hit)
            content, tokens = candidate, count
        else:
            deferred.append(hit.id)
    receipts = tuple(ContextEvidenceReceipt(evidence_id=hit.metadata["evidenceId"], context_block=name,
        source=hit.source, item_id=hit.id, version=hit.version, metadata=dict(hit.metadata)) for hit in chosen)
    await memory.validate_evidence(receipts, signal=signal)
    memory.assert_epoch(epoch)
    inputs = [{"evidenceId": receipt.evidence_id, "source": receipt.source,
               "itemId": receipt.item_id, "version": receipt.version, **dict(receipt.metadata)} for receipt in receipts]
    block = ContextBlock(name, content, token_count=tokens, untrusted=True,
                         host_metadata={CONTEXT_EVIDENCE_RECEIPTS_KEY: inputs}) if chosen else None
    found = {hit.id for hit in hits}
    return MemoryContextResult(block, tuple(hit.id for hit in chosen), tuple(deferred),
                               tuple(dict.fromkeys(item_id for item_id in ids if item_id not in found)), receipts)


class MemoryContext:
    def __init__(self, *, memory, query, count_tokens=estimate_json_tokens, name="memory", desired_tokens=1024, limit=8):
        self.memory = memory
        self.query = query
        self.count_tokens = count_tokens
        self.name = _text(name, "context name", 128)
        self.desired_tokens = _integer(desired_tokens, "desired_tokens", 1_000_000)
        self.limit = _integer(limit, "limit")
        if not callable(query) or not callable(count_tokens):
            raise TypeError("query and count_tokens must be host functions")

    async def describe_context_demands(self, request, signal=None):
        return (ContextBudgetClaim(self.name, self.desired_tokens),)

    async def build_context(self, request, budget, signal=None):
        allowance = budget.allocation_for(self.name)
        if allowance == 0:
            return ContextBundle()
        epoch = self.memory.epoch
        hits = await self.memory.retrieve(RetrievalRequest(query=self.query(request), limit=self.limit), signal)
        result = await assemble_memory_context(self.memory, tuple(hit.id for hit in hits), allowance,
            name=self.name, count_tokens=self.count_tokens, expected_epoch=epoch, signal=signal)
        return ContextBundle(blocks=(result.block,) if result.block is not None else ())
