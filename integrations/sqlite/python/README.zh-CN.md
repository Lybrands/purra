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
该组件还提供 `idempotency`、`run_tree`、`artifacts` 和 `long_tasks`，
用于接入对应的 Core 接口。

## 恢复

通过 `storage.list_running()` 查找中断的 Run，使用
`core.resume(run_id, request, options=...)` 恢复满足条件的检查点。
恢复时还原原有 Agent 配置，执行租约会阻止多个执行者同时占用同一 Run。

中断的外部工具调用可能已经生效。重试前，通过 `storage.reconcile_tool(...)`
提供其执行结果或未执行的证据。需要保存提问和回答时，使用
[SqliteClarification](../../interaction/python/README.zh-CN.md)。

## 适配器状态边界

Core 通过版本化 `StorageSession` 负责仓储状态格式；SQLite 负责事务、索引、租约及工具
效果对账。这是组件间需要版本匹配的适配契约，不是业务数据交换格式。Core 和集成包
需要成套安装。

通过 `purra.storage` 导入 `StorageSession`，不再使用 SQLite 内部的反射 codec。
记录使用显式标识和字段，不依赖 Python 模块路径。`storage.transaction()` 返回会话，
包含 `runs`、`outputs`、`run_tree`、`artifacts`、`artifact_claims`、
`artifact_maintenance`、`long_tasks` 端口；恢复元数据通过 `get_run_info()`、
`find_tree_run()` 查询，不读取仓储私有字段。事件日志独立保存，`export_snapshot()`
不是完整备份。会话端口和延迟日志回调不能离开事务后继续使用。

## 存储与关闭

规范输出事件按行增量保存，通过 Run 序号和 Root 序号索引分页读取。事件追加与执行状态
快照在同一个事务中提交；输出查询与订阅轮询不加载执行快照，也不申请写锁。
Run、租约查询和 `list_running()` 保持只读事务。工具回执、租约续期与释放、取消请求、
Agent 树、产物和长任务仓储操作跳过输出历史的还原与写回。能定位 Run 或输出流的写入
先通过 SQL 校验所属 Root 树的序号和事件数量，再缓冲新增事件，不解码整段历史。
规划投影、终态操作收尾等需要历史证据的 Core 校验，按需读取对应 Run 的原始事件。
共享预算仍使用全部兄弟 Run 的计数，事件幂等重放通过索引查询；Run 读取仍完整还原
所属 Root 的输出日志。
跨 Root 的事件幂等键通过索引查询；Python SQLite 需要支持 `json_extract`，首次打开
已有 v4 数据库时会建立该索引。租约认领、公开 `transaction()` 和无法定位 Run 的操作
仍执行完整作用域校验。执行快照仍按作用域加载和保存，包含 Run 历史、
检查点和回执，适合数据量受控的本地场景。
本次存储格式为 v4。构造器在修改数据库参数、表或索引之前拒绝其他所有版本（包括
v1/v2/v3）；拒绝后原库不变。没有自动迁移或旧格式恢复路径。
Python 与 TypeScript 的执行快照不能互换。
Python 与 TypeScript 均在指向已有 Run 的写入中按需加载历史。
SQL 序号校验扫描选定 Root 的覆盖索引，无需读取事件正文所在的数据行或按 Run 排序；
Root 与 Run 的映射也有覆盖索引。已有 v4 数据库在打开时补建索引，需要耗时和额外磁盘，
新增事件也需要维护索引。元数据快照仍按作用域保存，因此写入成本并非常数。
事件正文在读取时校验；租约认领和公开事务仍解码完整日志。
所有事件读取（包括索引重放和分页）都会核对正文中的 Run、Root 和序号是否与 SQL 列
一致。不一致时抛出 `ValueError` 并回滚当前事务，不自动修补数据；未读取的正文仍按需加载。

在仓库根目录运行基准，比较 100、1,000 和 5,000 条历史事件下的空轮询和末尾分页读取成本：

```sh
PYTHONPATH=src:integrations/sqlite/python/src .venv/bin/python integrations/sqlite/python/scripts/benchmark_reads.py
```

基准使用临时数据库，报告预热后的读取中位耗时，不代表并发吞吐量或真实模型端到端性能。

工具回执写入基准使用同样的事件规模，计入认领和结果提交两个事务：

```sh
PYTHONPATH=src:integrations/sqlite/python/src .venv/bin/python integrations/sqlite/python/scripts/benchmark_writes.py
```

该基准使用本地无外部副作用的工具函数，不计真实业务工具或模型调用耗时。

另一个 Root 的历史数据增长时，活动 Run 的事件写入成本可用以下基准测量：

```sh
PYTHONPATH=src:integrations/sqlite/python/src .venv/bin/python integrations/sqlite/python/scripts/benchmark_run_writes.py
```

它测量无关 Root 的隔离效果，不代表活动 Root 自身历史不断增长时的性能。

同一 Root 内的事件追加、模型调用登记和检查点提交基准，每种操作预热两次后测量十次：

```sh
PYTHONPATH=src:integrations/sqlite/python/src .venv/bin/python integrations/sqlite/python/scripts/benchmark_execution_writes.py
```

历史数据为私有领域事件；结果不包含规划证据重放、并发吞吐量或真实 Provider 耗时。
追加 `--profile` 可分别查看日志准备、执行快照编解码和事务其余部分的耗时。
各分段的中位数独立计算，不能直接相加得到总耗时中位数。

应用负责数据库访问、备份和数据保留。检查点包含私有模型数据。
先调用 `await core.close()`，再调用 `storage.close()`。
## 可选收尾验证

先构建 TypeScript Core 和 SQLite，再从仓库根目录执行：

```sh
PYTHONPATH=src:integrations/sqlite/python/src .venv/bin/python integrations/sqlite/python/scripts/verify_load.py --output /tmp/purra-load.json
```

脚本分别验证两个 SDK 的独立进程并发写入、事务中断回滚、未知工具效果对账和
检查点重新打开。默认包含 20 个 Root、60 个 Run、20,000 条事件及每 Root 64 KiB
检查点消息；数据库和副作用标记均为临时合成数据。

`scripts/verify_provider.py` 可使用 PurrTypos 设置数据库中明确选择的 DeepSeek
配置执行真实工具任务，需要网络权限并消耗 API Token。传入 `--config-db`、
`--config-id`、`--output`，在 `PYTHONPATH` 增加 `.:integrations/openai/python/src`
并安装 OpenAI SDK。凭据仅在内存中读取，不写入报告。
该脚本专用适配器映射输出上限和消息角色，关闭 thinking，移除 OpenAI 专用参数；
结果不代表原版 OpenAI 网关可直接兼容 DeepSeek。对照组为当前代码关闭延迟读取，
并非历史发行版；单组对照仅用于功能验证，不构成延迟保证。
