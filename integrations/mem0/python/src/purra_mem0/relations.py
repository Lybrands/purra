"""Read-only, host-directed relation proposals over exact memory revisions."""
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType

from ._journal import MemoryError
from .memory import Mem0Memory, MemoryRef, MemorySource


@dataclass(frozen=True, slots=True)
class MemoryRelationProposal:
    from_ref: MemoryRef
    to_ref: MemoryRef
    relation: str
    from_quote: str
    to_quote: str
    from_source: MemorySource
    to_source: MemorySource


async def propose_memory_relations(
    memory: Mem0Memory, refs: Sequence[MemoryRef], *, relations: Sequence[str],
    extract: Callable[[Mapping, object], Awaitable[object]],
    max_input_chars: int = 32_000, max_proposals: int = 32, signal=None,
) -> tuple[MemoryRelationProposal, ...]:
    """Call the host extractor once without linking records.

    The host owns Provider authorization, timeouts, budgets and semantic review.
    Quotes establish source membership, not the truth of an inferred relation.
    """
    if not callable(extract):
        raise TypeError("extract must be an async callable")
    for value, limit in ((max_input_chars, 10_000_000), (max_proposals, 256)):
        if type(value) is not int or not 1 <= value <= limit:
            raise ValueError("invalid relation extraction limit")
    if isinstance(refs, (str, bytes)) or not isinstance(refs, Sequence):
        raise TypeError("refs must be a sequence")
    refs = tuple(refs)
    if not 2 <= len(refs) <= 32 or any(not isinstance(ref, MemoryRef) for ref in refs):
        raise ValueError("provide 2 to 32 memory references")
    if len({ref.id for ref in refs}) != len(refs):
        raise ValueError("duplicate memory reference")
    if isinstance(relations, (str, bytes)) or not isinstance(relations, Sequence):
        raise TypeError("relations must be a sequence")
    relations = tuple(relations)
    if not 1 <= len(relations) <= 64 or any(
        not isinstance(value, str) or not value.strip() or value != value.strip() or len(value) > 64
        for value in relations
    ) or len(set(relations)) != len(relations):
        raise ValueError("invalid host relation vocabulary")
    epoch = memory.epoch

    def check():
        if signal is not None and signal.is_set():
            raise MemoryError("memory_cancelled")
        memory.assert_epoch(epoch)

    check()
    records = {}
    for ref in refs:
        record = await memory.get(ref.id, signal=signal)
        if record is None or record.version != ref.version:
            raise MemoryError("memory_relation_stale")
        records[ref.id] = record
    if sum(len(record.text) for record in records.values()) > max_input_chars:
        raise ValueError("relation source text exceeds max_input_chars")
    check()
    payload = MappingProxyType({
        "relations": relations, "maxProposals": max_proposals,
        "records": tuple(MappingProxyType({"id": r.id, "version": r.version, "text": r.text}) for r in records.values()),
    })
    raw = await extract(payload, signal)
    check()
    if not isinstance(raw, (list, tuple)) or len(raw) > max_proposals:
        raise MemoryError("memory_invalid_relation_proposal")
    result, seen = [], set()
    for item in raw:
        if not isinstance(item, Mapping) or set(item) != {"from", "to", "relation", "fromQuote", "toQuote"}:
            raise MemoryError("memory_invalid_relation_proposal")
        if any(not isinstance(value, str) for value in item.values()):
            raise MemoryError("memory_invalid_relation_proposal")
        a, b = records.get(item["from"]), records.get(item["to"])
        identity = (item["from"], item["to"], item["relation"])
        if (a is None or b is None or a.id == b.id or item["relation"] not in relations
                or identity in seen or any(not item[key].strip() or item[key] not in record.text
                    for key, record in (("fromQuote", a), ("toQuote", b)))):
            raise MemoryError("memory_invalid_relation_proposal")
        seen.add(identity)
        result.append(MemoryRelationProposal(MemoryRef(a.id, a.version), MemoryRef(b.id, b.version),
            item["relation"], item["fromQuote"], item["toQuote"], a.source, b.source))
    for ref in refs:
        if await memory.get(ref.id, signal=signal) != records[ref.id]:
            raise MemoryError("memory_relation_stale")
    check()
    return tuple(result)
