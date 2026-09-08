from dataclasses import replace
import pytest
from purra.api import ModelRouteCandidate, select_model_route, resolve_model_route
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


def test_saved_policy_survives_new_selection_policy_and_order():
    a, b = candidate('a'), candidate('b')
    saved = select_model_route([a, b], ['a', 'b'], TaskCapabilityRequirements('default'),
                               policy_id='ordered', policy_revision='1').to_mapping()
    current = replace(a, policy_id='new-policy', policy_revision='2')
    restored = resolve_model_route([b, current], saved, ['a', 'b'])
    assert restored.binding_id == 'a'
    assert (restored.policy_id, restored.policy_revision) == ('ordered', '1')
    for rows, allowed in [([b], ['b']), ([a], []), ([replace(a, revision='2')], ['a']),
                          ([replace(a, capabilities=replace(a.capabilities, context_window_tokens=2048))], ['a'])]:
        with pytest.raises(ContractViolationError) as error:
            resolve_model_route(rows, saved, allowed)
        assert error.value.code == 'model_route_mismatch'
    with pytest.raises(ValueError): resolve_model_route([a, a], saved, ['a'])
    with pytest.raises(ValueError): replace(a, policy_id='partial')
