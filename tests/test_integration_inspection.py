import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
from purra.api import IntegrationCheck, build_recovery_inspection, check_integration, inspect_recovery
from purra.contracts import RunStatus

FIXTURE = json.loads((Path(__file__).parents[1] / 'conformance/fixtures/recovery_inspection.json').read_text())

@pytest.mark.parametrize('case', FIXTURE['cases'], ids=lambda case: case['id'])
def test_recovery_semantics(case):
    report = build_recovery_inspection({**case['state'], 'content': 'credential-secret', 'privateReasoning': 'credential-secret'})
    assert report['blockers'] == case['blockers']
    assert report['cautions'] == case.get('cautions', [])
    assert report['authority'] == 'diagnosis_only'
    assert 'credential-secret' not in json.dumps(report)
    assert report['suggestedActions'][-1] == 'revalidate_execution'
    assert 'externalToolEffects' in report['unknown']
    assert 'agentTreeOwnership' in report['unknown']
    if not case['state']:
        assert report['observations']['attemptsAfterCheckpoint'] is None
        assert 'usage' in report['unknown']

@pytest.mark.parametrize('state', FIXTURE['invalid'])
def test_recovery_rejects_invalid_evidence(state):
    with pytest.raises(ValueError): build_recovery_inspection(state)

@pytest.mark.asyncio
async def test_generic_inspection_only_reads_get():
    class Repository:
        calls = 0
        async def get(self, run_id):
            assert run_id == 'run'; self.calls += 1
            return SimpleNamespace(status=RunStatus.RUNNING, execution_checkpoint=object())
        def __getattr__(self, name): raise AssertionError('must only read get')
    repository = Repository()
    report = await inspect_recovery(repository, 'run')
    assert repository.calls == 1
    assert report['observations']['checkpoint'] == 'present'
    assert report['observations']['lease'] == 'unknown'
    assert report['observations']['unknownToolReceipts'] is None

@pytest.mark.asyncio
async def test_report_executes_only_enabled_assertions_and_redacts_failures():
    calls = []
    async def good(): calls.append('good')
    async def bad(): calls.append('bad'); raise ValueError('credential-secret')
    async def live(): calls.append('live')
    checks = [IntegrationCheck('storage','deterministic',good), IntegrationCheck('gateway','deterministic',bad), IntegrationCheck('gateway','real_provider_mcp',live)]
    report = await check_integration(component='host',version='1',checks=checks,declared_capabilities={'gateway':'supported'})
    assert calls == ['bad', 'good']
    assert report['declaredCapabilities']['gateway'] == 'supported'
    row = lambda c, k: next(r for r in report['checks'] if r['capability']==c and r['category']==k)
    assert row('gateway','deterministic')['errorCode'] == 'gateway_nonconforming'
    assert row('gateway','real_provider_mcp')['status'] == 'not_run'
    assert row('storage','deterministic')['status'] == 'passed'
    assert 'credential-secret' not in json.dumps(report)
    await check_integration(component='host',version='1',checks=checks,enabled_categories=['real_provider_mcp'])
    assert calls[-1] == 'live'

@pytest.mark.asyncio
async def test_report_preflight_and_cancellation():
    calls = []
    async def probe(): calls.append(1); raise asyncio.CancelledError()
    check = IntegrationCheck('gateway','deterministic',probe)
    with pytest.raises(ValueError): await check_integration(component='host',version='1',checks=[check,check])
    assert not calls
    with pytest.raises(asyncio.CancelledError): await check_integration(component='host',version='1',checks=[check])
