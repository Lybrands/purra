"""Immutable durable approval data. These records never grant dispatch authority."""
from dataclasses import dataclass, field
from typing import Any, Mapping

from purra.json_values import freeze_json_mapping, thaw_json_mapping
from purra.structured import StructuredOutputContract, json_identity_digest


APPROVAL_INTENT_PROFILE = "purra.approval-intent/v1"
APPROVAL_STATUSES = frozenset({"pending", "approved", "rejected", "expired", "canceled"})
_ARGUMENTS = StructuredOutputContract("purra.approval-arguments", "1", {"type": "object"})


def _text(value, name):
    if not isinstance(value, str) or not value or value.strip() != value or len(value) > 1024:
        raise ValueError(f"invalid approval {name}")
    return value


def _integer(value, name, minimum=0):
    if type(value) is not int or not minimum <= value <= 9007199254740991:
        raise ValueError(f"invalid approval {name}")
    return value


@dataclass(frozen=True, slots=True)
class ApprovalIntent:
    run_id: str
    root_run_id: str
    tool_call_id: str
    tool_name: str
    arguments: Mapping[str, Any]
    preset_fingerprint: str
    binding_id: str
    binding_revision: str
    scope_id: str
    scope_revision: str
    effect: str
    schema_version: int = 1

    def __post_init__(self):
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise ValueError("invalid approval intent version")
        for name in _INTENT_NAMES:
            _text(getattr(self, name), name)
        if self.effect not in {"write", "destructive"}:
            raise ValueError("invalid approval effect")
        if not isinstance(self.arguments, Mapping):
            raise TypeError("approval arguments must be an object")
        object.__setattr__(self, "arguments", _ARGUMENTS.validate_value(self.arguments))
        json_identity_digest(self.to_mapping())

    def to_mapping(self):
        return {"schemaVersion": self.schema_version, "arguments": thaw_json_mapping(self.arguments),
                **{wire: getattr(self, name) for name, wire in _INTENT_NAMES.items()}}

    @property
    def digest(self):
        return json_identity_digest({"profile": APPROVAL_INTENT_PROFILE, "intent": self.to_mapping()})

    @classmethod
    def from_mapping(cls, value):
        if not isinstance(value, Mapping) or set(value) != {"schemaVersion", "arguments", *_INTENT_NAMES.values()}:
            raise ValueError("invalid approval intent fields")
        return cls(schema_version=value["schemaVersion"], arguments=value["arguments"],
                   **{name: value[wire] for name, wire in _INTENT_NAMES.items()})


_INTENT_NAMES = {"run_id": "runId", "root_run_id": "rootRunId", "tool_call_id": "toolCallId",
                 "tool_name": "toolName", "preset_fingerprint": "presetFingerprint", "binding_id": "bindingId",
                 "binding_revision": "bindingRevision", "scope_id": "scopeId", "scope_revision": "scopeRevision", "effect": "effect"}


@dataclass(frozen=True, slots=True)
class ApprovalDecisionCommand:
    approval_id: str
    expected_revision: int
    intent_digest: str
    command_key: str
    decision: str

    def __post_init__(self):
        for name in ("approval_id", "intent_digest", "command_key"):
            _text(getattr(self, name), name)
        _integer(self.expected_revision, "expected revision", 1)
        if self.decision not in {"approve", "reject"}:
            raise ValueError("invalid approval decision")

    def to_mapping(self):
        return {"approvalId": self.approval_id, "expectedRevision": self.expected_revision,
                "intentDigest": self.intent_digest, "commandKey": self.command_key, "decision": self.decision}


@dataclass(frozen=True, slots=True)
class ApprovalRecord:
    approval_id: str
    intent: ApprovalIntent
    revision: int
    status: str
    created_at_ms: int
    expires_at_ms: int
    decision_audit: Mapping[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self):
        _text(self.approval_id, "id")
        if not isinstance(self.intent, ApprovalIntent):
            raise TypeError("approval intent is required")
        _integer(self.revision, "revision", 1)
        _integer(self.created_at_ms, "creation time")
        _integer(self.expires_at_ms, "expiry", self.created_at_ms + 1)
        if self.status not in APPROVAL_STATUSES:
            raise ValueError("invalid approval status")
        object.__setattr__(self, "decision_audit", freeze_json_mapping(self.decision_audit))
        audit = self.decision_audit
        if audit:
            if set(audit) != {"command", "principalId", "decidedAtMs", "revision"}:
                raise ValueError("invalid approval audit")
            command = audit["command"]
            if not isinstance(command, Mapping) or set(command) != {"approvalId", "expectedRevision", "intentDigest", "commandKey", "decision"}:
                raise ValueError("invalid approval audit command")
            ApprovalDecisionCommand(command["approvalId"], command["expectedRevision"], command["intentDigest"], command["commandKey"], command["decision"])
            _text(audit["principalId"], "principal")
            _integer(audit["decidedAtMs"], "decision time", self.created_at_ms)
            _integer(audit["revision"], "decision revision", 2)
            if (command["approvalId"] != self.approval_id or command["intentDigest"] != self.intent.digest
                    or command["expectedRevision"] + 1 != audit["revision"] or audit["revision"] > self.revision
                    or audit["decidedAtMs"] >= self.expires_at_ms or self.status == "pending"
                    or self.status in {"approved", "rejected"} and self.status != ("approved" if command["decision"] == "approve" else "rejected")):
                raise ValueError("conflicting approval audit")
        elif self.status in {"approved", "rejected"}:
            raise ValueError("approval decision audit is required")

    def to_mapping(self):
        return {"approvalId": self.approval_id, "intent": self.intent.to_mapping(), "intentDigest": self.intent.digest,
                "revision": self.revision, "status": self.status, "createdAtMs": self.created_at_ms,
                "expiresAtMs": self.expires_at_ms, "decisionAudit": thaw_json_mapping(self.decision_audit)}

    @classmethod
    def from_mapping(cls, value):
        if not isinstance(value, Mapping) or set(value) != {"approvalId", "intent", "intentDigest", "revision", "status", "createdAtMs", "expiresAtMs", "decisionAudit"}:
            raise ValueError("invalid approval record fields")
        intent = ApprovalIntent.from_mapping(value["intent"])
        if intent.digest != value["intentDigest"]:
            raise ValueError("conflicting approval intent digest")
        return cls(value["approvalId"], intent, value["revision"], value["status"], value["createdAtMs"], value["expiresAtMs"], value["decisionAudit"])


__all__ = ["APPROVAL_INTENT_PROFILE", "ApprovalIntent", "ApprovalDecisionCommand", "ApprovalRecord"]
