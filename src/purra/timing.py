"""Shared monotonic timing helpers for PurrA traces."""

from time import perf_counter


def duration_ms(started: float) -> int:
    return max(0, round((perf_counter() - started) * 1_000))
