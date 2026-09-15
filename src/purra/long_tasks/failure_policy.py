"""Normalize optional Unit failure hooks at the scheduling boundary."""
from purra.errors import ContractViolationError
from purra.long_tasks.contracts import LongTaskSplitResult
from purra.recovery import FailureCategory, FailureSignal


class UnitFailurePolicy:
    def __init__(self, runner):
        self._runner = runner

    def _invoke(self, name, expected, task, unit, error):
        hook = getattr(self._runner, name, None)
        if hook is None:
            return None
        try:
            if not callable(hook):
                raise TypeError(f"{name} must be callable")
            value = hook(task, unit, error)
            if value is not None and not isinstance(value, expected):
                raise TypeError(f"{name} returned an invalid result")
            return value
        except Exception as hook_error:
            raise ContractViolationError(
                f"Unit failure hook {name} failed",
                code="long_task_failure_hook_failed",
                details={"hook": name, "unitId": unit.id,
                         "executionErrorType": type(error).__name__,
                         "hookErrorType": type(hook_error).__name__},
            ) from hook_error

    def classify(self, task, unit, error: Exception) -> FailureSignal:
        failure = self._invoke("classify_unit_failure", FailureSignal, task, unit, error)
        if failure is not None:
            return failure
        code = str(getattr(error, "code", "") or "").strip()
        return FailureSignal(
            category=FailureCategory.BUSINESS_INVARIANT,
            code=(code or str(error) or type(error).__name__)[:240],
            retryable=False,
        )

    def split(self, task, unit, error: Exception) -> LongTaskSplitResult | None:
        return self._invoke("split_unit", LongTaskSplitResult, task, unit, error)
