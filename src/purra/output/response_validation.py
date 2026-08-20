"""Shared execution lifecycle for registered response validators and judges."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from time import perf_counter

from purra.cancellation import OperationCanceled, await_with_cancellation
from purra.contracts import AgentMessage, ResponseValidationResult, TraceRecord
from purra.errors import ContractViolationError, ResponseJudgeContractError
from purra.operations import (
    AgentOperationController,
    OperationDisplay,
    OperationKind,
    OperationScope,
)
from purra.ports import CancellationSignal, ResponseJudge, ResponseValidator
from purra.timing import duration_ms


@dataclass(frozen=True, slots=True)
class ResponseValidationOutcome:
    violation_codes: tuple[str, ...] = ()
    repair_guidance: tuple[str, ...] = ()
    validation_details: tuple[dict[str, object], ...] = ()
    traces: tuple[TraceRecord, ...] = ()
    error_code: str | None = None
    error: Exception | None = None
    canceled: bool = False


@dataclass(frozen=True, slots=True)
class ResponseJudgeAttempt:
    operation_id: str | None
    index: int
    started_at: float


class ResponseValidationCoordinator:
    """Run host-injected checks while Core owns cancellation and Operations."""

    def __init__(
        self,
        operation_controller: AgentOperationController | None = None,
    ) -> None:
        self._operations = operation_controller

    async def validate_registered(
        self,
        *,
        content: str,
        messages: Sequence[AgentMessage],
        validators: Sequence[ResponseValidator],
        run_id: str,
        round_number: int,
    ) -> ResponseValidationOutcome:
        violation_codes: list[str] = []
        repair_guidance: list[str] = []
        details: list[dict[str, object]] = []
        for index, validator in enumerate(validators):
            operation_id = await self._start(run_id, source="validator", index=index)
            try:
                result = validator.validate(
                    content=content,
                    messages=tuple(messages),
                )
            except Exception as error:
                await self._fail(operation_id, "response_validator_error")
                return ResponseValidationOutcome(
                    traces=(TraceRecord(
                        stage="model_output",
                        outcome="response_validator_exception",
                        details={
                            "round": round_number,
                            "errorType": _root_error_type(error),
                        },
                    ),),
                    error_code="response_validator_error",
                    error=error,
                )
            if not isinstance(result, ResponseValidationResult):
                await self._fail(
                    operation_id,
                    "response_validator_contract_violation",
                )
                return ResponseValidationOutcome(
                    traces=(TraceRecord(
                        stage="model_output",
                        outcome="response_validator_contract_violation",
                        details={"round": round_number},
                    ),),
                    error_code="response_validator_contract_violation",
                    error=ContractViolationError(
                        "response validator returned an invalid result"
                    ),
                )
            await self._succeed(operation_id)
            if result.accepted:
                continue
            violation_codes.append(str(result.violation_code))
            repair_guidance.append(str(result.repair_guidance))
            details.append({
                "source": "validator",
                "code": str(result.violation_code),
                "details": dict(result.details),
            })
        return ResponseValidationOutcome(
            violation_codes=tuple(violation_codes),
            repair_guidance=tuple(repair_guidance),
            validation_details=tuple(details),
        )

    async def begin_judge(
        self,
        *,
        run_id: str,
        index: int,
    ) -> ResponseJudgeAttempt:
        return ResponseJudgeAttempt(
            operation_id=await self._start(run_id, source="judge", index=index),
            index=index,
            started_at=perf_counter(),
        )

    async def judge(
        self,
        attempt: ResponseJudgeAttempt,
        judge: ResponseJudge,
        *,
        content: str,
        messages: Sequence[AgentMessage],
        signal: CancellationSignal | None,
        round_number: int,
    ) -> ResponseValidationOutcome:
        try:
            result = await await_with_cancellation(
                judge.judge(
                    content=content,
                    messages=tuple(messages),
                    signal=signal,
                ),
                signal,
            )
        except OperationCanceled as error:
            await self._cancel(attempt.operation_id)
            return ResponseValidationOutcome(
                traces=(self._judge_trace(
                    attempt,
                    round_number=round_number,
                    outcome="response_judge_canceled",
                ),),
                error_code="request_canceled",
                error=error,
                canceled=True,
            )
        except ResponseJudgeContractError as error:
            await self._fail(
                attempt.operation_id,
                "response_judge_contract_violation",
            )
            return ResponseValidationOutcome(
                traces=(self._judge_trace(
                    attempt,
                    round_number=round_number,
                    outcome="response_judge_contract_violation",
                    error_type=_root_error_type(error),
                ),),
                error_code="response_judge_contract_violation",
                error=error,
            )
        except Exception as error:
            await self._fail(attempt.operation_id, "response_judge_error")
            return ResponseValidationOutcome(
                traces=(self._judge_trace(
                    attempt,
                    round_number=round_number,
                    outcome="response_judge_exception",
                    error_type=_root_error_type(error),
                ),),
                error_code="response_judge_error",
                error=error,
            )
        if not isinstance(result, ResponseValidationResult):
            await self._fail(
                attempt.operation_id,
                "response_judge_contract_violation",
            )
            error = ContractViolationError(
                "response judge returned an invalid result"
            )
            return ResponseValidationOutcome(
                traces=(self._judge_trace(
                    attempt,
                    round_number=round_number,
                    outcome="response_judge_contract_violation",
                ),),
                error_code="response_judge_contract_violation",
                error=error,
            )
        await self._succeed(attempt.operation_id)
        violation_code = str(result.violation_code or "")
        return ResponseValidationOutcome(
            violation_codes=(() if result.accepted else (violation_code,)),
            repair_guidance=(
                () if result.accepted else (str(result.repair_guidance),)
            ),
            validation_details=(
                ()
                if result.accepted
                else ({
                    "source": "judge",
                    "code": violation_code,
                    "details": dict(result.details),
                },)
            ),
            traces=(self._judge_trace(
                attempt,
                round_number=round_number,
                outcome=(
                    "response_judge_passed"
                    if result.accepted
                    else "response_judge_rejected"
                ),
                violation_code=(None if result.accepted else violation_code),
            ),),
        )

    async def _start(self, run_id: str, *, source: str, index: int) -> str | None:
        if self._operations is None:
            return None
        receipt = await self._operations.start(
            OperationKind.VALIDATION,
            OperationScope(
                run_id=run_id,
                display=OperationDisplay(
                    label_key="agent.operation.validation",
                    label_params={"source": source, "index": index},
                ),
            ),
        )
        return receipt.operation_id

    async def _succeed(self, operation_id: str | None) -> None:
        if self._operations is not None and operation_id is not None:
            await self._operations.succeed(operation_id)

    async def _fail(self, operation_id: str | None, error_code: str) -> None:
        if self._operations is not None and operation_id is not None:
            await self._operations.fail(operation_id, error_code)

    async def _cancel(self, operation_id: str | None) -> None:
        if self._operations is not None and operation_id is not None:
            await self._operations.cancel(
                operation_id,
                "response_validation_canceled",
            )

    @staticmethod
    def _judge_trace(
        attempt: ResponseJudgeAttempt,
        *,
        round_number: int,
        outcome: str,
        error_type: str | None = None,
        violation_code: str | None = None,
    ) -> TraceRecord:
        details: dict[str, object] = {
            "round": round_number,
            "judgeIndex": attempt.index,
        }
        if error_type is not None:
            details["errorType"] = error_type
        if violation_code is not None:
            details["violationCode"] = violation_code
        return TraceRecord(
            stage="model_output",
            outcome=outcome,
            details=details,
            duration_ms=duration_ms(attempt.started_at),
        )


def _root_error_type(error: Exception) -> str:
    cause = error.__cause__
    return type(cause if isinstance(cause, Exception) else error).__name__


__all__ = [
    "ResponseJudgeAttempt",
    "ResponseValidationCoordinator",
    "ResponseValidationOutcome",
]
