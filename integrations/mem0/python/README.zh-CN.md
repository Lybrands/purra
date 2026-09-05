# purra-mem0 · Python

[English](README.md) | 简体中文

使用同步 Mem0 OSS `Memory` 客户端提供作用域记忆、检索和生命周期管理。
要求 Python 3.11+，SDK 调用在线程中执行。

## 安装

在仓库根目录执行：

```sh
python -m pip install . ./integrations/mem0/python
```

原生受控模型和向量回调已包含在本包内，无需安装 LangChain。

导入 Mem0 前，设置 `MEM0_TELEMETRY=false`，并将 `MEM0_DIR` 指向应用管理的数据目录。
显式配置 LLM、embedder、`vector_store` 和 `history_db_path`。
不支持托管的 `MemoryClient` 或 `AsyncMemory`。

## 保存与读取

应用提供 `mem0_config`、已认证的 `user_id`、已授权的 `project_id`，以及持久化 `journal_path`：

```python
from mem0 import Memory
from purra_mem0 import Mem0Memory, MemoryScope, MemorySource

sdk = Memory.from_config(mem0_config)
memory = Mem0Memory(
    client=sdk,
    scope=MemoryScope(user=user_id, project=project_id),
    journal_path=journal_path,
)

async def save_preference():
    saved = await memory.add(
        "Reply in Chinese.",
        source=MemorySource("preference:language", "1"),
        key="preference:language:1",
    )
    return await memory.get(saved.ids[0])
```

`add` 默认创建已启用记录；需要先审核时设置 `state="pending"`。
记录不可用时，`get` 返回 `None`。更新、状态切换和删除需要当前 `version` 与稳定的操作 `key`。

## 接入 Agent

```python
from purra.retrieval import RetrieverTool
from purra_mem0 import MemoryContext

recall = RetrieverTool(
    retriever=memory,
    name="recallMemory",
    description="Recall saved preferences and facts.",
)
context = MemoryContext(
    memory=memory,
    query=lambda request: request.latest_user_text(),
)
```

将 `recall.registration` 注册到 Agent 工具目录，或将 `context` 与其他上下文提供器组合。
记忆实例已经绑定作用域。需要显式选择记录时，使用 `assemble_memory_context(memory, ids, allowance)`。

## 受控模型调用

调用需要持久化预算时，使用以下配置替代原生 SDK 客户端。
存储路径、向量维度和预算键由应用提供，数值仅用于演示预算配置。

`model_tasks` 是 PurrA 扩展工厂提供的执行器，`model_request` 选择模型。
异步回调 `embed(texts, signal)` 需要返回 `EmbeddingResult`，包含向量和服务报告的输入 Token 用量。

```python
from purra_mem0 import (
    MemoryBudget, MemoryProviders, create_managed_client, run_model,
)

client = create_managed_client(
    embedding_dims=embedding_dimensions,
    config={"vector_store": vector_store_config, "history_db_path": history_path},
)
providers = MemoryProviders(
    budget=MemoryBudget(
        key=budget_key,
        max_llm_calls=4,
        max_embedding_calls=64,
        max_input_chars=100_000,
        max_output_tokens=8192,
        result_capacity_target_tokens=2048,
    ),
    complete=run_model(model_tasks, model_request),
    embed=embed,
)
memory = Mem0Memory(
    client=client,
    providers=providers,
    scope=MemoryScope(user=user_id, project=project_id),
    journal_path=journal_path,
    allow_inference=True,
)
```

通过 `memory.budget_usage()` 查看已准入调用和报告用量。原生 SDK 模式的内部用量记为未知。

## 提取与审查

配置受控模型调用并设置 `allow_inference=True` 后：

```python
from purra_mem0 import MemoryWorkflow

workflow = MemoryWorkflow(memory)

async def capture(messages, source, operation_key):
    return await workflow.capture(messages, source=source, key=operation_key)
```

此配置审查候选并让其保持待处理。需要应用决定时，为 `MemoryWorkflow` 传入
异步 `policy(candidate, review)` 和稳定的 `policy_revision`。
策略返回已授权且保留审查键的 `MemoryResolution`，或返回 `None` 保持待处理。

## 生命周期

关闭前调用 `await memory.drain()` 和 `memory.close()`，再关闭 SDK 与模型服务资源。
分页、来源撤回、证据校验和不确定写入的处理见[记忆生命周期指南](../README.zh-CN.md)。


## 原生受控适配

`create_managed_client` 把 PurrA 的原生模型和向量适配器注入包内、版本固定的 Mem0 私有模块，沿用现有预算、取消与结果校验。向量维度必须一致，受控路径拒绝 LangChain 向量库。不修改宿主安装的官方 SDK，也不启动代理服务。来源、许可证与修改说明见[第三方声明](THIRD_PARTY_NOTICES.md)。直接传入的原始 SDK 客户端仍由宿主管理，不自动获得受控回调的计费。
