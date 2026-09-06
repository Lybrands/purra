import json
from pathlib import Path

import pytest

from purra.structured import StructuredOutputContract, StructuredOutputError, StructuredOutputLimits
from purra.schema import _SCHEMA_KEYWORDS


FIXTURE = json.loads((Path(__file__).parents[1] / "conformance/fixtures/structured_output.json").read_text())
LIMIT_NAMES = {"schemaBytes": "schema_bytes", "outputBytes": "output_bytes",
               "schemaDepth": "schema_depth", "outputDepth": "output_depth",
               "schemaNodes": "schema_nodes", "outputNodes": "output_nodes",
               "validationSteps": "validation_steps"}


def test_keyword_inventory_and_public_entrypoint():
    from purra.api import StructuredOutputContract as PublicContract
    assert PublicContract is StructuredOutputContract
    assert sorted(_SCHEMA_KEYWORDS) == FIXTURE["keywords"]


def test_parse_error_does_not_retain_json_decoder_candidate():
    contract = StructuredOutputContract("test", "1", {"type": "object"})
    with pytest.raises(StructuredOutputError) as caught:
        contract.parse('{"private": secret-candidate}')
    assert caught.value.__context__ is None
    assert caught.value.__cause__ is None


@pytest.mark.parametrize("case", FIXTURE["cases"], ids=lambda case: case["name"])
def test_shared_output_contract(case):
    try:
        contract = StructuredOutputContract(
            schema_id="test", schema_version="1", schema=case["schema"],
            mode=case.get("mode", "local"),
            limits=StructuredOutputLimits(**{LIMIT_NAMES[key]: value for key, value in case.get("limits", {}).items()}),
        )
        if "schemaDigest" in case:
            assert contract.schema_digest == case["schemaDigest"]
            assert contract.contract_digest == case["contractDigest"]
        value = contract.parse(case["text"])
    except StructuredOutputError as error:
        assert error.code == case["code"]
        assert set(error.details) == {"path", "keyword", "reason"}
        assert "secret-candidate" not in str(error.details)
    else:
        assert case["code"] is None
        assert value == json.loads(case["text"])


def test_contract_and_result_are_detached_and_immutable():
    schema = {"type": "object", "properties": {"v": {"type": "array"}}}
    contract = StructuredOutputContract("test", "1", schema)
    schema["properties"]["v"]["type"] = "string"
    value = contract.parse('{"v":[1]}')
    with pytest.raises(TypeError):
        value["v"].append(2)
    with pytest.raises(TypeError):
        contract.schema["properties"]["v"]["type"] = "number"


def test_identity_is_content_and_mode_bound():
    left = StructuredOutputContract("test", "1", {"type": "object", "const": {"b": 1.0, "a": -0.0}})
    right = StructuredOutputContract("test", "1", {"const": {"a": 0, "b": 1}, "type": "object"})
    native = StructuredOutputContract("test", "1", right.schema, mode="native_required")
    changed = StructuredOutputContract("test", "1", {"type": "object", "const": {"a": 0, "b": 2}})
    assert left.schema_digest == right.schema_digest == native.schema_digest
    assert left.contract_digest == right.contract_digest != native.contract_digest
    assert changed.schema_digest != left.schema_digest


def test_cycles_and_preparse_limits_fail_closed():
    schema = {"type": "object"}
    schema["properties"] = schema
    with pytest.raises(StructuredOutputError, match="validation failed") as caught:
        StructuredOutputContract("test", "1", schema)
    assert caught.value.code == "structured_output_schema_invalid"
    contract = StructuredOutputContract("test", "1", {"type": "object"})
    with pytest.raises(StructuredOutputError) as caught:
        contract.parse('{"v":' + '[' * 10_000 + '0' + ']' * 10_000 + '}')
    assert caught.value.code == "structured_output_invalid_json"


def test_decoded_values_share_strict_profile_and_resource_bounds():
    from purra.api import json_identity_digest
    contract = StructuredOutputContract('decoded', '1', {'type':'object','properties':{'n':{'type':'integer'}},'required':['n']})
    value = contract.validate_value({'n':1.0})
    assert value == contract.parse('{"n":1}')
    assert json_identity_digest(value) == json_identity_digest({'n':1})
    with pytest.raises(StructuredOutputError):
        contract.validate_value({'n':True})
    cyclic = {}; cyclic['self'] = cyclic
    with pytest.raises(StructuredOutputError):
        contract.validate_value(cyclic)
    with pytest.raises(ValueError):
        json_identity_digest(cyclic)
