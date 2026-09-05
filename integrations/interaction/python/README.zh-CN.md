# purra-interaction · Python

[English](README.md) | 简体中文

持久化结构化问题，等待用户输入后继续执行。要求 Python 3.11+。

| API | 用途 |
| --- | --- |
| `SqliteClarification` | 通过 `purra-sqlite` 检查点暂停并恢复同一 Run |
| `ClarificationStore` + `ClarificationWorkflow` | 在执行前收集输入，再通过应用回调创建 Run |

## 安装

需要恢复同一 Run 时，在仓库根目录执行：

```sh
python -m pip install . ./integrations/sqlite/python './integrations/interaction/python[sqlite]'
```

## 配置 Run

已有模型网关 `gateway` 时：

```python
from purra.api import AgentCore, AgentPreset
from purra.contracts import RuntimeLimits
from purra.tools import InMemoryToolCatalog
from purra_sqlite import SqliteAgentAdapters
from purra_interaction import SqliteClarification

storage = SqliteAgentAdapters("agent.db", scope="user-1/project-1")
interaction = SqliteClarification(storage)
core = AgentCore(
    model_gateway=gateway,
    preset=AgentPreset(
        id="assistant", revision="1",
        runtime_limits=RuntimeLimits(max_run_generation_tokens=8192),
        tool_catalog=InMemoryToolCatalog((interaction.registration,)),
    ),
    run_repository=storage.runs,
    output_repository=storage.outputs,
    output_publisher=storage.publisher,
    execution_lease_store=storage.leases,
)
```

将 `tools_enabled=True` 的 `AgentRunRequest` 传给
`await interaction.submit(core, request)`。Agent 提问后，`handle.wait()` 会抛出
`purra.api.UserInputRequired`。使用异常中的 `request_id` 调用
`await interaction.get(request_id)`，获取可展示的问题数据。

Run 保持等待状态，不占用持续运行的执行线程或模型调用。收到用户回答后，传入问题修订号、
稳定的命令键，以及按问题 ID 组织的回答：

```python
async def answer_and_resume(request_id, revision, answer_key, answers):
    ready = await interaction.answer(
        request_id, revision=revision, key=answer_key, answers=answers,
    )
    return await interaction.resume(core, ready["id"])
```

返回的句柄继续执行原 Run，保留已消耗预算和截止时间，也可能再次请求输入。
回答只提供任务数据，不授予工具权限。

## 重新加载与取消

进程重启后，还原相同的存储、Agent preset、网关和交互对象。
`list_pending()` 包含等待中和已回答的请求，可恢复其中处于 `ready` 状态的记录。
等待期间使用 `cancel(request_id)`；恢复执行后使用 Run 句柄取消。

使用 Agent 树时，绑定同一 Run 树仓储，回答 Root 下的全部待答问题后再恢复。
支持 Auto、Reactive 和 Planned。暂停发生在完整工具轮次结束后；
结果不确定的外部写入需要先确认。关闭时先关闭 Core，再关闭存储。

## 执行前收集输入

`ClarificationStore.ask(...)` 保存问题和应用检查点。向界面提供 `store.public(saved)`，
再通过 `store.answer(...)` 保存回答。`ClarificationWorkflow.resume(..., submit=callback)`
调用 `callback(snapshot, continuation_key)` 创建新 Run 并返回其 ID。

将续接键保存在对应 Run 上。提交中断时，先根据已有 Run 确认结果再重试。
应用负责还原凭据、权限和任务剩余预算，并在关闭时关闭 store。
