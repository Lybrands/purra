"""Content-neutral security checks for PurrA boundaries."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any, TypeAlias

from purra.contracts import ToolCall, ToolExecutionLimits
from purra.tools.security import (
    parse_tool_arguments,
    preflight_tool_calls,
    sanitize_error_message,
)


SecurityRedTeamCase: TypeAlias = tuple[str, str, Callable[[], bool]]


def get_core_security_redteam_cases() -> tuple[SecurityRedTeamCase, ...]:
    """Return only provider-neutral Core security contracts."""

    limits = ToolExecutionLimits()
    return (
        (
            "RT1-malformed-tool-arguments",
            "Malformed model arguments fail closed instead of becoming an empty object.",
            lambda: parse_tool_arguments("}{not-json")[1] is not None,
        ),
        (
            "RT2-duplicate-tool-call-ids",
            "Duplicate protocol ids reject the entire tool batch.",
            lambda: preflight_tool_calls(
                (
                    ToolCall(id="same", name="read", arguments_json="{}"),
                    ToolCall(id="same", name="read", arguments_json="{}"),
                ),
                limits,
            )[1]
            is not None,
        ),
        (
            "RT3-tool-batch-resource-limit",
            "A single model round cannot request an unbounded number of tools.",
            lambda: preflight_tool_calls(
                tuple(
                    ToolCall(
                        id=str(index),
                        name="read",
                        arguments_json="{}",
                    )
                    for index in range(limits.max_calls_per_batch + 1)
                ),
                limits,
            )[1]
            is not None,
        ),
        (
            "RT5-sensitive-error-redaction",
            "Known secret and filesystem patterns are redacted from tool errors.",
            lambda: "supersecret" not in sanitize_error_message(
                r"C:\\Users\\alice\\private.txt sk-supersecret123",
            ),
        ),
    )


def run_security_redteam_cases(
    cases: Iterable[SecurityRedTeamCase],
) -> dict[str, Any]:
    """Execute caller-owned deterministic checks with diagnostic containment."""

    results: list[dict[str, Any]] = []
    for case_id, threat, check in cases:
        try:
            passed = bool(check())
        except Exception as exc:  # pragma: no cover - diagnostic containment
            passed = False
            detail = type(exc).__name__
        else:
            detail = "boundary_enforced" if passed else "boundary_missing"
        results.append({
            "caseId": case_id,
            "threat": threat,
            "verdict": "pass" if passed else "fail",
            "detail": detail,
        })
    passed = sum(result["verdict"] == "pass" for result in results)
    return {
        "summary": {
            "total": len(results),
            "passed": passed,
            "failed": len(results) - passed,
        },
        "results": results,
    }


__all__ = [
    "SecurityRedTeamCase",
    "get_core_security_redteam_cases",
    "run_security_redteam_cases",
]
