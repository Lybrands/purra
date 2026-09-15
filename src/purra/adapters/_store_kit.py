"""Shared primitives for the in-memory reference adapters.

Small guard and clock helpers that were previously duplicated across the
adapter modules; one definition keeps their semantics identical.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from hashlib import sha256
import json


def wall_time_ms() -> int:
    return int(time.time() * 1000)


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def required_text_field(value: object, name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{name} is required")
    return normalized


def canonical_digest(value: object) -> str:
    return sha256(json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")).hexdigest()
