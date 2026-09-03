"""Optional Adapter evidence, never inferred from semantic stream activity."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ModelTransportDiagnostics:
    request_sent_at_ms: int | None = None
    first_byte_at_ms: int | None = None
    http_attempts: int | None = None

    def __post_init__(self):
        for value in (self.request_sent_at_ms, self.first_byte_at_ms, self.http_attempts):
            if value is not None and (type(value) is not int or value < 1 or value > 9_007_199_254_740_991):
                raise ValueError("transport diagnostics must be positive safe integers or unknown")
        if self.request_sent_at_ms is not None and self.first_byte_at_ms is not None and self.first_byte_at_ms < self.request_sent_at_ms:
            raise ValueError("first HTTP byte cannot precede the HTTP request")

    def to_mapping(self):
        return {"httpRequestSentAtMs": self.request_sent_at_ms,
                "httpFirstByteAtMs": self.first_byte_at_ms, "sdkHttpAttempts": self.http_attempts}


__all__ = ["ModelTransportDiagnostics"]
