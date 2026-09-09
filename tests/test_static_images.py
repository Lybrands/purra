import json
import base64

import pytest

from purra.contracts import AgentMessage
from purra.context_budget import estimate_agent_messages_tokens, estimate_json_tokens
from purra.media import static_image_content
from dataclasses import replace
from purra.agent_execution_checkpoint import AgentExecutionCheckpoint
from purra.storage.codec import dump_storage_value, load_storage_value
from purra.context_budget import trim_agent_messages_by_turn
from purra.model_protocol import ModelProtocolCapabilities, generic_capability_snapshot, TaskCapabilityRequirements, preflight_capabilities

PNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aE9sAAAAASUVORK5CYII="


def content(**changes):
    return static_image_content("Describe this image", [{"mediaType": "image/png", "dataBase64": PNG, "inputTokens": 2000, **changes}])


def test_images_survive_json_restore_and_do_not_alias_caller_state():
    original = content()
    message = AgentMessage("user", original)
    original["images"][0]["dataBase64"] = "YQ=="
    restored = AgentMessage.from_mapping(json.loads(json.dumps(message.to_mapping())))
    assert restored.content == message.content
    assert restored.content["images"][0]["dataBase64"] == PNG
    projection = message.to_mapping()["content"]
    projection["images"][0]["dataBase64"] = ""
    assert estimate_agent_messages_tokens([message]) == 6 + 2000 + estimate_json_tokens({"role": "user", "content": projection})


@pytest.mark.parametrize("changes", [
    {"inputTokens": 0}, {"inputTokens": True}, {"inputTokens": 1.5}, {"inputTokens": 2**53},
    {"dataBase64": "YR=="}, {"dataBase64": ""}, {"dataBase64": "https://example.com/image.png"},
    {"mediaType": "image/svg+xml"}, {"url": "file:///private"},
])
def test_invalid_images_rejected(changes):
    with pytest.raises(ValueError):
        content(**changes)


@pytest.mark.parametrize("role", ["system", "developer", "assistant", "tool"])
def test_images_cannot_be_authority_or_model_output(role):
    with pytest.raises(ValueError, match="user role"):
        AgentMessage(role, content())


def test_image_checkpoint_codec_preserves_bytes_and_budget_without_database():
    message = AgentMessage("user", content())
    saved = AgentExecutionCheckpoint(run_id="image-run", next_round=1, round_limit=3, messages=(message,))
    restored = load_storage_value(dump_storage_value(saved))
    assert restored == saved
    assert AgentExecutionCheckpoint.from_mapping(json.loads(json.dumps(saved.to_mapping()))) == saved
    assert estimate_agent_messages_tokens(restored.messages) == estimate_agent_messages_tokens(saved.messages)


def test_trimming_does_not_slice_current_image_to_fit():
    latest = AgentMessage("user", content())
    result = trim_agent_messages_by_turn((AgentMessage("user", "old"), AgentMessage("assistant", "old answer"), latest), 100)
    assert result.messages == (latest,)
    assert result.overflow_tokens == estimate_agent_messages_tokens((latest,)) - 100


def test_image_transport_bytes_are_not_counted_as_prompt_text():
    small = AgentMessage("user", content(dataBase64="YQ=="))
    large = AgentMessage("user", content(dataBase64=base64.b64encode(b"x" * 100_000).decode()))
    assert estimate_agent_messages_tokens((small,)) == estimate_agent_messages_tokens((large,))
    assert len(large.content["images"][0]["dataBase64"]) > 100_000


@pytest.mark.asyncio
@pytest.mark.parametrize("image_tokens", [2000, 1_000_000])
async def test_python_public_run_resumes_image_checkpoint_without_rebuilding_context(image_tokens):
    from test_standalone_agent_conformance import _Context, _Gateway, _core, _request
    from test_generation_resume_identity import _Sink
    from purra.api import AgentCoreRunOptions, InMemoryAgentAdapters
    from purra.run_controller import AgentRunController
    from purra.contracts import PlanningMode, RunStatus
    from test_run_supervisor import _LeaseStore
    adapters = InMemoryAgentAdapters()
    original_context = _Context()
    original = _core(gateway=_Gateway("unused"), context=original_context, adapters=adapters)
    request = _request()
    request = replace(request, context_window=32768, planning_mode=PlanningMode.REACTIVE, messages=(AgentMessage("user", content(inputTokens=image_tokens)),),
        model=replace(request.model, capability_snapshot=replace(request.model.capability_snapshot,
            protocol=replace(request.model.protocol_capabilities, image_input="supported"))))
    request = original._preset.apply(request)
    options = AgentCoreRunOptions(agent_preset_snapshot=original._preset.snapshot(request))
    controller = AgentRunController(repository=original._repository, event_sink=_Sink())
    await controller.start(original._run_create_params(request, options))
    checkpoint = AgentExecutionCheckpoint(run_id=controller.run_id, next_round=1, round_limit=6,
        messages=request.messages, initial_planning_open=False)
    await controller.save_execution_checkpoint(checkpoint)
    restored = load_storage_value(dump_storage_value(checkpoint))
    assert restored == (await adapters.runs.get(controller.run_id)).execution_checkpoint
    gateway = _Gateway("Image recovery complete")
    context = _Context()
    leases = _LeaseStore()
    resumed = _core(gateway=gateway, context=context, adapters=adapters, execution_lease_store=leases)
    try:
        result = await (await resumed.resume(controller.run_id, request, options=options)).wait()
        assert result.run_id == controller.run_id
        if image_tokens == 2000:
            assert result.status is RunStatus.DONE
            assert result.final_response == "Image recovery complete"
            assert next(message for message in gateway.messages[0] if message.role == "user").content["images"][0]["dataBase64"] == PNG
        else:
            assert result.status is RunStatus.FAILED
            assert result.error == "protected_messages_exceed_compression_budget"
            assert gateway.messages == []
        assert context.single_pass_calls == 0
    finally:
        await resumed.close()
        await original.close()
    assert leases.releases == 1


def test_image_capability_is_optional_on_old_records_and_bound_when_declared():
    old = ModelProtocolCapabilities()
    unknown = replace(old, image_input="unknown")
    assert old.digest() == unknown.digest()
    assert "imageInput" not in old.to_mapping()
    encoded = dump_storage_value(old)
    assert "image_input" not in encoded
    assert load_storage_value(encoded) == old
    supported = replace(old, image_input="supported")
    assert supported.digest() != old.digest()
    assert load_storage_value(dump_storage_value(supported)) == supported
    for support in ["unknown", "unavailable"]:
        with pytest.raises(Exception, match="image_input"):
            preflight_capabilities(replace(generic_capability_snapshot(), protocol=replace(old, image_input=support)), TaskCapabilityRequirements("default", image_input_required=True))
    preflight_capabilities(replace(generic_capability_snapshot(), protocol=supported), TaskCapabilityRequirements("default", image_input_required=True))


@pytest.mark.asyncio
@pytest.mark.parametrize("support", ["unknown", "unavailable", "supported"])
async def test_manager_checks_image_capability_before_opening_gateway(support):
    from test_model_invocation_manager import _types, _request, _Gateway, _Observer
    from purra.output import AgentOutputIntent, OutputCommitMode
    from purra.contracts import ReasoningMode
    Call, Manager, Context = _types()
    request = _request()
    request = replace(request, capability_snapshot=replace(request.capability_snapshot, protocol=replace(request.protocol_capabilities, image_input=support)))
    gateway = _Gateway()
    manager = Manager(gateway, output_observer=_Observer())
    call = Call(request=request, output_intent=AgentOutputIntent.FINAL_PUBLIC, commit_mode=OutputCommitMode.LIVE, reasoning_mode=ReasoningMode.DISABLED)
    context = Context(run_id="image-run", requested_reasoning_mode=ReasoningMode.DISABLED)
    if support != "supported":
        with pytest.raises(Exception) as error:
            await manager.stream((AgentMessage("user", content()),), call, context)
        assert error.value.code == "model_capability_incompatible"
        assert gateway.calls == []
    else:
        stream = await manager.stream((AgentMessage("user", content()),), call, context)
        async for _ in stream.chunks:
            pass
        assert len(gateway.calls) == 1


def test_planner_receives_images_as_media_not_stringified_bytes():
    from test_planning_contract import _request
    from purra.contracts import PlanningCapabilities
    from purra.planner import build_planner_messages
    request = replace(_request(), messages=(AgentMessage("user", content()), AgentMessage("assistant", "Earlier answer"), AgentMessage("user", content())))
    assert request.latest_user_text() == "Describe this image"
    _, projected = build_planner_messages(request, PlanningCapabilities())
    value = projected.to_mapping()["content"]
    assert [image["dataBase64"] for image in value["images"]] == [PNG, PNG]
    assert PNG not in value["text"]
    payload = json.loads(value["text"])
    assert [row["imageIndexes"] for row in payload["imageInputs"]] == [[0], [1]]
    assert payload["userText"] == "Describe this image"
    assert request.messages[0].content["images"][0]["dataBase64"] == PNG
