# PurrA Mem0 集成

[English](README.md) | 简体中文

`purra-mem0` 使用 Mem0 OSS SDK 为 PurrA 提供按作用域隔离的长期记忆。
Mem0 保存文本与向量，适配器管理归属、状态、版本、操作回执和上下文证据。
应用显式决定保存哪些内容。

## 开始使用

- [Python 安装与用法](python/README.zh-CN.md)
- [TypeScript 安装与用法](typescript/README.zh-CN.md)

使用 OSS `Memory` 客户端，不支持托管的 Mem0 Platform 客户端。
除配置好的 Mem0 存储外，组件还需要持久化的 SQLite 控制日志。

## 记忆生命周期

1. `add` 直接保存已接受的文本，会调用 Embedding，不提取新事实。
2. `extract` 使用模型生成 `pending` 候选，需显式启用推断。
3. `review` 对待处理候选返回建议，由应用策略决定是否采纳。
4. `resolve` 使用准确的记录版本应用已接受的决定。
5. 检索和上下文组装只返回启用、未过期、来源未撤回的记录。

`MemoryWorkflow` 组合提取、审查与策略驱动的处理。没有配置策略时，候选保持待处理；
策略也可以让单条候选保持待处理。模型判断相似不等于允许激活。

| 处理类型 | 结果 |
| --- | --- |
| `independent` | 激活一条候选 |
| `duplicate` | 保留一条接受的记录，停用重复项 |
| `supersede` | 保留接受的替代记录，停用旧记录 |
| `conflict` | 停用冲突组，等待解决 |

## 读取与管理

| API | 用途 |
| --- | --- |
| `get`、`list`、`history` | 读取记录、管理分页和历史 |
| `update`、`annotate` | 使用预期版本替换文本或元数据 |
| `set_state` / `setState`、`delete` | 改变可见性或删除当前内容 |
| `retrieve`、`select` | 语义搜索或按 ID 显式选择 |
| `MemoryContext`、`assemble_memory_context` / `assembleMemoryContext` | 在 Token 额度内组装完整记录 |
| `link`、`links` | 记录和查询记录版本之间的显式关系 |
| `revoke_source` / `revokeSource` | 撤回来源或某一来源修订 |
| `validate_evidence` / `validateEvidence` | 检查已保存的记忆证据是否仍可使用 |

`list` 返回 `items`、`next` 和 `epoch`。即使当前页为空，也应继续使用 `next` 翻页，
直到它为 `null`；若 epoch 改变，应重新读取。`query` 过滤器按文本字面匹配，语义搜索使用 `retrieve`。
元数据的值为 JSON 标量，编辑元数据不会调用 Embedding。

## 作用域与上下文

每个实例绑定已认证用户、项目和可选的 Agent 身份。作用域由应用确定，读取须经过适配器。
使用 `RetrieverTool` 时省略其 `scope` 选项，因为记忆实例已经绑定作用域。

上下文组装在分配额度内放入完整记录，并附带证据回执。复用依赖记忆的上下文或检查点前，
需要校验这些回执。Core 不会自动重新校验已解析的检查点；证据失效时应重建受影响的上下文。

撤回某个来源修订会隐藏其记录。省略修订号会撤回该来源 ID 的所有当前和未来修订。
删除和撤回不会清除 SDK 历史、旧提示词、检查点或备份，数据保留由应用管理。

## 模型调用与预算

原生 SDK 模式使用 SDK 配置的服务，内部用量记为未知。受控模式使用应用回调，
并持久化调用次数、输入和输出预算。`run_model` / `runModel` 将 LLM 工作接入实际 Run
的任务执行器；Embedding 用量单独核算。

提取与语义审查需要模型调用，添加、更新和搜索文本可能调用 Embedding。
本地存储不代表本地推理。应用负责服务配置、凭据和金额配额。
恢复时沿用预算键；已准入的预留额度在失败后仍计入消耗。

## 持久化与恢复

每个 Mem0 存储使用一个持久化控制日志。记录写入通过适配器完成，存储与日志一起备份。
二者的写入不是同一个原子事务。Python 和 TypeScript 的 SDK 存储不能互换。

重试时保持操作键不变。超时后检查 `operation(key)`：`running` 可能仍会完成，
`unknown` 需要确认结果。调用 `reconcile` 前先停止所有旧执行者。
未完成的提取可在执行者停止后使用 `discard_extraction` / `discardExtraction` 清理。

先调用 `drain()`，再调用 `close()`。关闭适配器只会关闭其控制日志，
SDK、模型服务和存储资源由应用管理。
