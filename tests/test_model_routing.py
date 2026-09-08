from dataclasses import replace
import pytest
from purra.api import ModelRouteCandidate, select_model_route
from purra.errors import ContractViolationError
from purra.model_protocol import generic_capability_snapshot, TaskCapabilityRequirements


def candidate(name, **changes):
    return ModelRouteCandidate(name, '1', 'config-1', replace(
        generic_capability_snapshot(), max_generation_tokens=128, **changes))


def test_authorization_and_order():
    a, b = candidate('a'), candidate('b')
    req = TaskCapabilityRequirements('default')
    assert select_model_route([a, b], ['b'], req) == b
    assert select_model_route([a, b], ['b', 'a'], req) == a
    with pytest.raises(ValueError): select_model_route([a, a], ['a'], req)
    with pytest.raises(ValueError): select_model_route([a], ['missing'], req)


@pytest.mark.parametrize('requirements', [
    TaskCapabilityRequirements('default', tool_calling='required'),
    TaskCapabilityRequirements('default', streaming_required=True),
    TaskCapabilityRequirements('default', cancellation_required=True),
    TaskCapabilityRequirements('default', structured_output_level='json_schema'),
])
def test_required_capability_rejects_unknown(requirements):
    row = candidate('a')
    row = replace(row, capabilities=replace(row.capabilities, protocol=replace(
        row.capabilities.protocol, tool_calling='unknown', streaming='unknown', cancellation='unknown')))
    with pytest.raises(ContractViolationError, match='No authorized'):
        select_model_route([row], ['a'], requirements)


def test_skips_ineligible_and_rejects_empty_authority():
    rows = [candidate('bad', actionable=False), candidate('good')]
    req = TaskCapabilityRequirements('default')
    assert select_model_route(rows, ['bad', 'good'], req).binding_id == 'good'
    with pytest.raises(ContractViolationError): select_model_route(rows, [], req)
    with pytest.raises(ValueError): ModelRouteCandidate(' x ', '1', 'cfg', rows[0].capabilities)
