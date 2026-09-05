"""SDK injection boundaries with local stores and deterministic callbacks."""
import asyncio, json, tempfile, os, sqlite3
import deny_langchain  # Install the import tripwire before importing the component.
from pathlib import Path
from unittest.mock import patch
from purra_mem0 import create_managed_client, Mem0Memory, MemoryProviders, MemoryBudget, MemoryScope, MemorySource, EmbeddingResult, MemoryError
from purra.contracts import ModelCompletion, AgentMessage, ModelTokenUsage

async def check(root):
    os.environ['MEM0_DIR']=str(root/'sdk');os.environ['MEM0_TELEMETRY']='false'
    from mem0.utils.factory import LlmFactory, EmbedderFactory, VectorStoreFactory
    from mem0.configs.base import MemoryConfig
    from purra_mem0._vendor.memory import Memory
    from purra_mem0.direct_providers import DirectLlm, DirectEmbedder
    from types import SimpleNamespace
    from unittest.mock import Mock
    # Missing injection still delegates to the untouched official factories.
    with patch.object(EmbedderFactory, 'create', side_effect=RuntimeError('native sentinel')) as native:
        try: Memory(MemoryConfig())
        except RuntimeError as err: assert str(err) == 'native sentinel'
        else: raise AssertionError('native factory was not used')
        assert native.call_count == 1
    with patch.object(VectorStoreFactory, 'create', side_effect=AssertionError('allocated before validation')) as allocated:
        try: Memory(MemoryConfig(), llm=object(), embedder=DirectEmbedder())
        except TypeError: pass
        else: raise AssertionError('invalid provider accepted')
        assert allocated.call_count == 0
    # History initialization failure releases the newly created Qdrant client.
    owned = Mock()
    store = SimpleNamespace(client=owned)
    settings = MemoryConfig(vector_store={'provider':'qdrant','config':{'path':str(root/'failed'),'embedding_model_dims':2}}, history_db_path=str(root/'missing'/'history.db'))
    with patch.object(VectorStoreFactory, 'create', return_value=store):
        try: Memory(settings, llm=DirectLlm(), embedder=DirectEmbedder())
        except sqlite3.OperationalError: pass
        else: raise AssertionError('invalid history path accepted')
        owned.close.assert_called_once()
    # An injected storage client is still owned by its caller.
    settings.vector_store.config.client = owned
    owned.reset_mock()
    with patch.object(VectorStoreFactory, 'create', return_value=store):
        try: Memory(settings, llm=DirectLlm(), embedder=DirectEmbedder())
        except sqlite3.OperationalError: pass
        else: raise AssertionError('invalid history path accepted')
        owned.close.assert_not_called()
    clients=[];memories=[]
    try:
        with patch.object(LlmFactory,'create',side_effect=AssertionError('native LLM factory')) as l, patch.object(EmbedderFactory,'create',side_effect=AssertionError('native Embedder factory')) as e:
            for name in ['a','b']:
                client=create_managed_client(embedding_dims=2,config={'vector_store':{'provider':'qdrant','config':{'path':str(root/name),'collection_name':'memory','embedding_model_dims':2}},'history_db_path':str(root/f'{name}-history.db')})
                clients.append(client)
                assert client.sdk.provider_binding == {"llm":"injected", "embedder":"injected"}
                client.sdk.reset()
                async def complete(messages,cap,signal,name=name):
                    await asyncio.sleep(.005 if name=='a' else .001)
                    assert f'question-{name}' in messages[-1].content
                    return ModelCompletion(message=AgentMessage('assistant',json.dumps({'memory':[{'text':f'result-{name}','entities':[]}]})),model='fixture',finish_reason='stop',applied_generation_limit=cap,usage=ModelTokenUsage(10,5))
                async def embed(texts,signal):return EmbeddingResult([[1.,0.] for _ in texts],sum(map(len,texts)))
                providers=MemoryProviders(MemoryBudget(name,1,20,100000,64,32),complete,embed)
                memories.append(Mem0Memory(client=client,scope=MemoryScope(name,'design'),journal_path=str(root/f'{name}-journal.db'),allow_inference=True,providers=providers))
            results=await asyncio.gather(*[m.extract([{'role':'user','content':f'question-{name}'}],source=MemorySource('source','1'),key='extract') for m,name in zip(memories,['a','b'])])
            for i,name in enumerate(['a','b']):
                assert results[i].usage.llm_calls==1
                assert clients[i].sdk.get(results[i].ids[0])['memory']==f'result-{name}'
            try:clients[0].sdk.add('unbound',user_id='unbound',infer=False)
            except MemoryError as err:assert err.code=='memory_provider_unbound'
            else:raise AssertionError('unbound accepted')
            assert l.call_count==e.call_count==0
            print(json.dumps({'nativeAdapter':True,'failedInitializationCleanup':True,'callerOwnedClientPreserved':True,'constructorAndResetNativeFactoryCalls':0,'concurrentClients':2,'contextIsolation':True,'unboundRejected':True}))
    finally:
        for m in memories:await m.drain();m.close()
        for c in clients:
            c.sdk.db.close();c.sdk.vector_store.client.close()
            if c.sdk._entity_store is not None:c.sdk._entity_store.client.close()

if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='purra-direct-boundary-') as root:asyncio.run(check(Path(root)))
