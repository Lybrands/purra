"""Deterministic host check report and read-only recovery inspection; no live services."""
import asyncio
import json
from importlib.metadata import version
from purra.api import IntegrationCheck, check_integration, inspect_recovery
from purra.adapters import InMemoryAgentAdapters
from purra.contracts import RunCreateParams
from purra.events import AgentEvent
from purra.testing import assert_artifact_store_conforms

async def main():
    fixture = InMemoryAgentAdapters()
    async def storage_probe():
        await assert_artifact_store_conforms(artifacts=fixture.artifacts,
            claims=fixture.artifact_claims, maintenance=fixture.artifact_maintenance)
    report = await check_integration(component='purra', version=version('purra'),
        checks=[IntegrationCheck('storage', 'deterministic', storage_probe)])
    assert next(row for row in report['checks'] if row['capability']=='storage' and row['category']=='deterministic')['status'] == 'passed'
    run = await fixture.runs.begin(RunCreateParams(None, 'fixture', None), AgentEvent('run.started'))
    recovery = await inspect_recovery(fixture.runs, run.run_id)
    assert recovery['observations']['lease'] == 'unknown'
    print(json.dumps({'integration':report, 'recovery':recovery}, indent=2))

if __name__ == '__main__': asyncio.run(main())
