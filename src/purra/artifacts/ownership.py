"""Opaque ownership references for durable Agent artifacts."""

from __future__ import annotations

from dataclasses import dataclass

from purra.normalization import required_text


@dataclass(frozen=True, slots=True)
class ArtifactOwnerRef:
    """Host-defined owner identity that PurrA stores but never interprets."""

    kind: str
    id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "kind", required_text(
            self.kind,
            "artifact owner kind",
        ))
        object.__setattr__(self, "id", required_text(
            self.id,
            "artifact owner id",
        ))


__all__ = ["ArtifactOwnerRef"]
