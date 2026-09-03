# purra-sqlite · Python

[English](README.md) | 简体中文

为 PurrA 的 Run、事件、操作、预算、检查点和工具回执提供 SQLite 持久化。
使用 Python 标准库，要求 Python 3.11+。

## 安装

在仓库根目录执行：

```sh
python -m pip install . ./integrations/sqlite/python
```

## 配置

已有模型网关 `gateway` 和 Agent 配置 `preset` 时：

```python
from purra.api import AgentCore
from purra_sqlite import SqliteAgentAdapters

storage = SqliteAgentAdapters("agent.db", scope="user-1/project-1")
core = AgentCore(
    model_gateway=gateway,
    preset=preset,
    run_repository=storage.runs,
    output_repository=storage.outputs,
    output_publisher=storage.publisher,
    execution_lease_store=storage.leases,
)
```

`scope` 应由应用根据已认证的用户与项目绑定确定。
该组件还提供 `idempotency`、`run_tree`、`delegations`、`artifacts` 和 `long_tasks`，
用于接入对应的 Core 接口。

## 恢复

通过 `storage.list_running()` 查找中断的 Run，使用
`core.resume(run_id, request, options=...)` 恢复满足条件的检查点。
恢复时还原原有 Agent 配置，执行租约会阻止多个执行者同时占用同一 Run。

中断的外部工具调用可能已经生效。重试前，通过 `storage.reconcile_tool(...)`
提供其执行结果或未执行的证据。需要保存提问和回答时，使用
[SqliteClarification](../../interaction/python/README.zh-CN.md)。

## 存储与关闭

每个作用域保存为一个事务快照，加载和序列化成本随历史数据增长，适合数据量受控的本地场景。
Python 与 TypeScript 的执行快照不能互换。

应用负责数据库访问、备份和数据保留。检查点包含私有模型数据。
先调用 `await core.close()`，再调用 `storage.close()`。
