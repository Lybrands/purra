"""Optional regression and security evaluation support for PurrA."""

from purra.evaluation.regression import (
    AgentRuntimeRegressionCase,
    evaluate_runtime_regression_case,
    run_runtime_regression_suite,
)
from purra.evaluation.security import (
    SecurityRedTeamCase,
    get_core_security_redteam_cases,
    run_security_redteam_cases,
)

__all__ = [
    "AgentRuntimeRegressionCase",
    "SecurityRedTeamCase",
    "evaluate_runtime_regression_case",
    "get_core_security_redteam_cases",
    "run_runtime_regression_suite",
    "run_security_redteam_cases",
]
