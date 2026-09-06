"""Local structured task with one explicit repair; no Provider, Run or Root binding."""
import asyncio
import json
from dataclasses import replace
from purra.api import AgentModelTask, AgentModelTaskRunner, IntegrationCheck, StructuredOutputContract, check_integration
from purra.contracts import AgentMessage, ModelCompletion, ModelRequest, ModelTokenUsage
from purra.model_invocation import AgentModelInvocationManager, ModelInvocationContext
from purra.model_protocol import generic_capability_snapshot

class FixtureGateway:
    calls = 0
    async def complete(self, messages, invocation, signal=None):
        self.calls += 1
        return ModelCompletion(AgentMessage('assistant', 'invalid' if self.calls == 1 else '{"ok":true}'),
            model='fixture', finish_reason='stop', usage=ModelTokenUsage(10, 5),
            applied_generation_limit=invocation.max_generation_tokens)
    async def stream(self, *args): raise AssertionError('structured completion expected')

async def main():
    gateway = FixtureGateway()
    model = ModelRequest('fixture', 'fixture', replace(generic_capability_snapshot(), max_generation_tokens=128))
    runner = AgentModelTaskRunner(AgentModelInvocationManager(gateway), ModelInvocationContext('example'), model)
    output = StructuredOutputContract('example', '1', {'type':'object', 'properties':{'ok':{'type':'boolean'}}, 'required':['ok'], 'additionalProperties':False})
    async def probe():
        result = await runner.complete_structured((AgentMessage('user', 'Return an ok flag.'),), AgentModelTask(model), output=output, repair_attempts=1)
        assert result.value == {'ok':True} and result.receipt.attempts == 2
        assert result.receipt.persistence == 'none' and result.receipt.root_budget == 'not_bound'
        assert gateway.calls == 2
    report = await check_integration(component='structured-example', version='1',
        checks=[IntegrationCheck('structured_output', 'deterministic', probe)])
    assert next(row for row in report['checks'] if row['capability']=='structured_output' and row['category']=='deterministic')['status'] == 'passed'
    print(json.dumps(report, indent=2))

if __name__ == '__main__': asyncio.run(main())
