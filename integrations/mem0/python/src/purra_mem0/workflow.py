"""Restartable extraction/review workflow over the existing operation journal."""

from dataclasses import dataclass
from hashlib import sha256
from typing import Awaitable, Callable

from ._journal import MemoryError
from .memory import (Mem0Memory, MemoryCaptureAuthorization, MemoryOperation, MemoryRecord,
                     MemoryRef, MemoryResolution, MemoryReview, memory_capture_intent)


MemoryDecisionPolicy = Callable[[MemoryRecord, MemoryReview], Awaitable[MemoryResolution | None]]


@dataclass(frozen=True, slots=True)
class MemoryWorkflowResult:
    extraction: MemoryOperation
    resolutions: tuple[MemoryOperation, ...] = ()
    pending_ids: tuple[str, ...] = ()


class MemoryWorkflow:
    """Extract pending records, review them, then ask the host to authorize a resolution.

    No policy means review only. A policy may return None to leave a candidate
    pending. Its revision is part of operation identity; keep it stable on retry.
    Retrieval continues through MemoryContext/assemble_memory_context and only
    uses currently active, authorized records.
    """

    def __init__(self, memory: Mem0Memory, *, policy: MemoryDecisionPolicy | None = None,
                 policy_revision: str = "review-only", review_limit: int = 8,
                 capture_policy_id: str | None = None, capture_policy_revision: str | None = None):
        if not isinstance(policy_revision, str) or not policy_revision.strip() or len(policy_revision) > 512:
            raise ValueError("policy_revision must be non-empty and at most 512 characters")
        if policy is not None and not callable(policy):
            raise TypeError("policy must be callable")
        if type(review_limit) is not int or not 1 <= review_limit <= 32:
            raise ValueError("review_limit must be from 1 to 32")
        if (capture_policy_id is None) != (capture_policy_revision is None):
            raise ValueError("capture policy id and revision must be configured together")
        if capture_policy_id is not None:
            if not isinstance(capture_policy_id, str) or not capture_policy_id.strip() or len(capture_policy_id) > 512:
                raise ValueError("capture_policy_id must be non-empty and at most 512 characters")
            if not isinstance(capture_policy_revision, str) or not capture_policy_revision.strip() or len(capture_policy_revision) > 512:
                raise ValueError("capture_policy_revision must be non-empty and at most 512 characters")
        self.memory, self.policy = memory, policy
        self.policy_revision, self.review_limit = policy_revision, review_limit
        self.capture_policy_id, self.capture_policy_revision = capture_policy_id, capture_policy_revision

    async def capture(self, messages, *, source, key: str, metadata=None, expires_at=None,
                      authorization: MemoryCaptureAuthorization | None = None, signal=None):
        if not isinstance(key, str) or not key.strip() or len(key) > 512:
            raise ValueError("workflow key must be non-empty and at most 512 characters")
        if self.capture_policy_id is None:
            if authorization is not None:
                raise ValueError("capture authorization requires a configured capture policy")
        else:
            if not isinstance(authorization, MemoryCaptureAuthorization):
                raise MemoryError("memory_capture_authorization_required")
            intent = memory_capture_intent(messages, source=source, metadata=metadata, expires_at=expires_at,
                                           max_input_chars=self.memory.max_input_chars)
            if (authorization.intent_digest != intent
                    or authorization.policy_id != self.capture_policy_id
                    or authorization.policy_revision != self.capture_policy_revision):
                raise MemoryError("memory_capture_authorization_mismatch")
        prefix = "workflow:" + sha256(key.encode()).hexdigest()
        # Re-submit the identical extraction input so the journal checks key reuse.
        extraction = await self.memory.extract(messages, source=source, key=prefix + ":extract",
                                               metadata=metadata, expires_at=expires_at,
                                               authorization=authorization, signal=signal)
        if extraction.state != "complete":
            return MemoryWorkflowResult(extraction)
        resolutions, pending = [], []
        for item_id in extraction.ids:
            suffix = sha256((self.policy_revision + "\0" + item_id).encode()).hexdigest()
            resolve_key = prefix + ":resolve:" + suffix
            existing = self.memory.operation(resolve_key)
            if existing is not None:
                resolutions.append(existing)
                if existing.state != "complete":
                    pending.append(item_id)
                continue
            record = await self.memory.get(item_id, include_inactive=True, signal=signal)
            if record is None or record.state != "pending":
                continue  # Never reactivate a revoked or externally resolved record.
            reviewed = await self.memory.review(MemoryRef(record.id, record.version),
                                                key=prefix + ":review:" + suffix,
                                                limit=self.review_limit, signal=signal)
            if reviewed.state != "complete" or reviewed.review is None or self.policy is None:
                pending.append(item_id)
                continue
            decision = await self.policy(record, reviewed.review)
            if decision is None:
                pending.append(item_id)
                continue
            if (not isinstance(decision, MemoryResolution)
                    or decision.review_key != reviewed.review.key
                    or reviewed.review.candidate not in decision.items):
                raise ValueError("workflow decision must include the candidate and its review key")
            # resolve validates all versions, scope, source authority and the reviewed group.
            resolved = await self.memory.resolve(decision, key=resolve_key, signal=signal)
            resolutions.append(resolved)
            if resolved.state != "complete":
                pending.append(item_id)
        return MemoryWorkflowResult(extraction, tuple(resolutions), tuple(pending))


__all__ = ["MemoryWorkflow", "MemoryWorkflowResult", "MemoryDecisionPolicy"]
