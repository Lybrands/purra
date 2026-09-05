from __future__ import annotations

from dataclasses import replace

import pytest

from purra.agent_execution_checkpoint import AgentExecutionCheckpoint
from purra.api import AgentCoreRunOptions, InMemoryAgentAdapters
from purra.errors import ContractViolationError
from purra.run_controller import AgentRunController

from test_standalone_agent_conformance import _Context, _Gateway, _core, _request


class _Sink:
    async def emit(self, event):
        del event


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ("user_ceiling", "result_capacity"))
async def test_checkpoint_resume_rejects_generation_intent_drift(drift):
    adapters = InMemoryAgentAdapters()
    core = _core(
        gateway=_Gateway("done"),
        context=_Context(),
        adapters=adapters,
    )
    request = _request()
    original_options = AgentCoreRunOptions(result_capacity_target_tokens=128)
    controller = AgentRunController(
        repository=core._repository,
        event_sink=_Sink(),
    )
    try:
        await controller.start(core._run_create_params(request, original_options))
        checkpoint = AgentExecutionCheckpoint(
            run_id=controller.run_id,
            next_round=1,
            round_limit=6,
            messages=request.messages,
        )
        await controller.save_execution_checkpoint(checkpoint)
        persisted = await core._repository.get(controller.run_id)
        assert persisted.requested_user_max_generation_tokens == 512
        assert persisted.result_capacity_target_tokens == 128
        assert persisted.selected_context_window_tokens == 8_192

        resumed_request = request
        resumed_options = replace(
            original_options,
            agent_execution_checkpoint=checkpoint,
        )
        if drift == "user_ceiling":
            resumed_request = replace(
                request,
                model=replace(request.model, max_generation_tokens=256),
            )
        else:
            resumed_options = replace(
                resumed_options,
                result_capacity_target_tokens=256,
            )

        with pytest.raises(ContractViolationError) as captured:
            await core._start_or_attach_run(
                resumed_request,
                resumed_options,
                AgentRunController(
                    repository=core._repository,
                    event_sink=_Sink(),
                ),
            )

        assert captured.value.code == "run_identity_conflict"
    finally:
        await core.close()
