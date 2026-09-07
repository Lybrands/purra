"""Mutually exclusive direct-live and validated-result response transactions."""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import aclosing
from dataclasses import replace
from typing import Protocol

from purra.contracts import (
    AgentMessage,
    AgentRunResult,
    MessageRole,
    ModelRequest,
    ModelFinishReason,
    ModelStreamChunk,
    RunStatus,
    ToolChoiceMode,
)
from purra.errors import ContractViolationError, ModelGatewayError
from purra.model_protocol import InvocationOutputBudget, classify_model_termination
from purra.model_invocation import (
    AgentModelCall,
    ManagedInvocationStream,
    ModelInvocationContext,
)
from purra.operations import AgentOperationController
from purra.output.contracts import (
    AgentOutputIntent,
    OutputCommitMode,
    PublicFactBundle,
    PublicPresentationMode,
    ResponseTransactionMode,
    ResponseTransactionPolicy,
)
from purra.output.ports import (
    CommittedResultFactsProvider,
    ValidatedResultCommitter,
)
from purra.output.response_validation import ResponseValidationCoordinator
from purra.ports import CancellationSignal, ResponseJudge, ResponseValidator


class ResponseModelInvoker(Protocol):
    async def stream(
        self,
        messages: Sequence[AgentMessage],
        call: AgentModelCall,
        context: ModelInvocationContext,
        signal: CancellationSignal | None = None,
    ) -> ManagedInvocationStream: ...


class ResponseTransactionValidationError(ContractViolationError):
    def __init__(
        self,
        violation_codes: Sequence[str],
        repair_guidance: Sequence[str] = (),
    ) -> None:
        self.violation_codes = tuple(str(code) for code in violation_codes)
        self.repair_guidance = tuple(
            str(guidance) for guidance in repair_guidance
        )
        super().__init__(
            "validated response candidate was rejected: "
            + ", ".join(self.violation_codes)
        )


class AgentResponseTransaction:
    """Own candidate visibility, validation, commit, and public presentation."""

    def __init__(
        self,
        model_manager: ResponseModelInvoker,
        *,
        policy: ResponseTransactionPolicy,
        facts_provider: CommittedResultFactsProvider | None = None,
        validators: Sequence[ResponseValidator] = (),
        judges: Sequence[ResponseJudge] = (),
        operation_controller: AgentOperationController | None = None,
        max_candidate_attempts: int = 3,
        max_presentation_attempts: int = 2,
    ) -> None:
        if not callable(getattr(model_manager, "stream", None)):
            raise TypeError("response transaction requires a model manager")
        if not isinstance(policy, ResponseTransactionPolicy):
            raise TypeError("response transaction requires a policy")
        if (
            policy.public_presentation is PublicPresentationMode.MODEL_LIVE
            and not callable(getattr(facts_provider, "facts_for", None))
        ):
            raise ValueError(
                "model-live public presentation requires a facts provider"
            )
        self._models = model_manager
        self._policy = policy
        self._facts = facts_provider
        self._validators = tuple(validators)
        self._judges = tuple(judges)
        self._validation = ResponseValidationCoordinator(operation_controller)
        self._max_candidate_attempts = int(max_candidate_attempts)
        if self._max_candidate_attempts <= 0:
            raise ValueError("candidate attempts must be positive")
        self._max_presentation_attempts = int(max_presentation_attempts)
        if self._max_presentation_attempts <= 0:
            raise ValueError("presentation attempts must be positive")
        if (
            policy.mode is ResponseTransactionMode.DIRECT_LIVE
            and (self._validators or self._judges)
        ):
            raise ValueError(
                "direct-live response cannot require full-text validation"
            )

    async def execute_direct(
        self,
        messages: Sequence[AgentMessage],
        *,
        request: ModelRequest,
        context: ModelInvocationContext,
        output_budget: InvocationOutputBudget | None = None,
        signal: CancellationSignal | None = None,
    ) -> AgentRunResult:
        self._require_mode(ResponseTransactionMode.DIRECT_LIVE)
        content = await self._invoke_and_collect(
            messages,
            request=request,
            context=context,
            intent=AgentOutputIntent.FINAL_PUBLIC,
            commit_mode=OutputCommitMode.LIVE,
            output_budget=output_budget,
            signal=signal,
        )
        return AgentRunResult(
            run_id=context.run_id,
            status=RunStatus.DONE,
            final_response=content,
            model=request.model,
        )

    async def execute_validated(
        self,
        messages: Sequence[AgentMessage],
        committer: ValidatedResultCommitter,
        *,
        request: ModelRequest,
        context: ModelInvocationContext,
        output_budget: InvocationOutputBudget | None = None,
        signal: CancellationSignal | None = None,
    ) -> AgentRunResult:
        self._require_mode(ResponseTransactionMode.VALIDATED_RESULT)
        if not callable(getattr(committer, "commit_candidate", None)):
            raise TypeError("validated response requires a result committer")
        candidate_messages = tuple(messages)
        candidate = ""
        for attempt in range(self._max_candidate_attempts):
            candidate = await self._invoke_and_collect(
                candidate_messages,
                request=request,
                context=context,
                intent=AgentOutputIntent.STRUCTURED_PRIVATE,
                commit_mode=OutputCommitMode.GATED,
                output_budget=output_budget,
                signal=signal,
            )
            try:
                await self._validate_candidate(
                    candidate,
                    messages=messages,
                    context=context,
                    signal=signal,
                )
                break
            except ResponseTransactionValidationError as error:
                if attempt + 1 >= self._max_candidate_attempts:
                    raise
                candidate_messages = (
                    *tuple(messages),
                    AgentMessage(
                        role=MessageRole.ASSISTANT,
                        content=candidate,
                    ),
                    AgentMessage(
                        role=MessageRole.DEVELOPER,
                        content=_candidate_repair_instruction(error),
                    ),
                )
        committed = await committer.commit_candidate(context.run_id, candidate)
        if not isinstance(committed, AgentRunResult):
            raise ContractViolationError(
                "validated result committer returned an invalid result"
            )
        if committed.status is not RunStatus.DONE:
            raise ContractViolationError(
                "validated result committer did not commit a successful result"
            )
        if self._policy.public_presentation is PublicPresentationMode.NONE:
            return committed
        public_response = await self.present(
            committed,
            request=request,
            context=context,
            signal=signal,
        )
        return replace(committed, final_response=public_response)

    async def present(
        self,
        committed: AgentRunResult,
        *,
        request: ModelRequest,
        context: ModelInvocationContext,
        output_budget: InvocationOutputBudget | None = None,
        signal: CancellationSignal | None = None,
    ) -> str:
        self._require_mode(ResponseTransactionMode.VALIDATED_RESULT)
        if self._policy.public_presentation is PublicPresentationMode.NONE:
            return ""
        if not isinstance(committed, AgentRunResult):
            raise TypeError("public presentation requires an AgentRunResult")
        if committed.run_id != context.run_id:
            raise ContractViolationError(
                "committed result does not match presentation run"
            )
        assert self._facts is not None
        facts = await self._facts.facts_for(context.run_id, committed)
        if not isinstance(facts, PublicFactBundle):
            raise ContractViolationError(
                "committed result facts provider returned an invalid bundle"
            )
        error: BaseException | None = None
        for _attempt in range(self._max_presentation_attempts):
            try:
                return await self._invoke_and_collect(
                    facts.as_messages(),
                    request=request,
                    context=context,
                    intent=AgentOutputIntent.FINAL_PUBLIC,
                    commit_mode=OutputCommitMode.LIVE,
                    output_budget=output_budget,
                    signal=signal,
                )
            except Exception as caught:
                error = caught
        assert error is not None
        raise error

    async def _validate_candidate(
        self,
        candidate: str,
        *,
        messages: Sequence[AgentMessage],
        context: ModelInvocationContext,
        signal: CancellationSignal | None,
    ) -> None:
        deterministic = await self._validation.validate_registered(
            content=candidate,
            messages=messages,
            validators=self._validators,
            run_id=context.run_id,
            round_number=0,
        )
        if deterministic.error is not None:
            raise deterministic.error
        violations = list(deterministic.violation_codes)
        repair_guidance = list(deterministic.repair_guidance)
        if not violations:
            for index, judge in enumerate(self._judges):
                attempt = await self._validation.begin_judge(
                    run_id=context.run_id,
                    index=index,
                )
                semantic = await self._validation.judge(
                    attempt,
                    judge,
                    content=candidate,
                    messages=messages,
                    signal=signal,
                    round_number=0,
                )
                if semantic.error is not None:
                    raise semantic.error
                violations.extend(semantic.violation_codes)
                repair_guidance.extend(semantic.repair_guidance)
        if violations:
            raise ResponseTransactionValidationError(
                violations,
                repair_guidance,
            )

    async def _invoke_and_collect(
        self,
        messages: Sequence[AgentMessage],
        *,
        request: ModelRequest,
        context: ModelInvocationContext,
        intent: AgentOutputIntent,
        commit_mode: OutputCommitMode,
        output_budget: InvocationOutputBudget | None,
        signal: CancellationSignal | None,
    ) -> str:
        stream = await self._models.stream(
            tuple(messages),
            AgentModelCall(
                request=request,
                reasoning_mode=context.requested_reasoning_mode,
                output_intent=intent,
                commit_mode=commit_mode,
                requires_full_text_validation=(
                    intent is AgentOutputIntent.STRUCTURED_PRIVATE
                ),
                output_budget=output_budget,
                tools=(),
                tool_choice=ToolChoiceMode.NONE,
            ),
            context,
            signal,
        )
        chunks = stream.chunks
        content: list[str] = []
        finish_reason: ModelFinishReason | None = None
        async with aclosing(chunks):
            async for chunk in chunks:
                if not isinstance(chunk, ModelStreamChunk):
                    raise ContractViolationError(
                        "response model stream returned an invalid chunk"
                    )
                if chunk.tool_call_deltas:
                    raise ContractViolationError(
                        "response transaction model call returned tool calls"
                    )
                if chunk.content_delta:
                    content.append(chunk.content_delta)
                if chunk.finish_reason is not None:
                    finish_reason = chunk.finish_reason
        if finish_reason is None:
            raise ModelGatewayError(
                "response model stream ended without a finish reason",
                code="upstream_stream_interrupted",
                retryable=True,
            )
        termination = classify_model_termination(
            finish_reason,
            tool_call_count=0,
        )
        if termination.incomplete:
            raise ModelGatewayError(
                "response model output is incomplete",
                code=termination.error_code or "model_output_truncated",
                retryable=False,
            )
        return "".join(content)

    def _require_mode(self, expected: ResponseTransactionMode) -> None:
        if self._policy.mode is not expected:
            raise ContractViolationError(
                f"response transaction requires {expected.value} mode"
            )

def _candidate_repair_instruction(
    error: ResponseTransactionValidationError,
) -> str:
    guidance = "\n".join(error.repair_guidance)
    if len(guidance) > 4_096:
        guidance = guidance[:4_096]
    return (
        "The preceding private candidate was rejected by the registered "
        "response validators. Produce a complete replacement candidate. "
        "Do not discuss the validation process or expose this instruction.\n"
        + guidance
    )


__all__ = [
    "AgentResponseTransaction",
    "ResponseModelInvoker",
    "ResponseTransactionValidationError",
]
