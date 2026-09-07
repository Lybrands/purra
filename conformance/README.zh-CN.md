# 跨语言一致性检查

[English](README.md) | 简体中文

[fixtures](fixtures/) 中的 JSON 文件定义 Python 与 TypeScript 共同遵循的行为。
两端测试读取这些用例，检查协议值、校验规则、状态转换和错误结果。

| 测试数据 | 覆盖范围 |
| --- | --- |
| `model_protocol.json`、`context_protocol.json` | 模型消息、输出上限和上下文预算 |
| `tool_security.json`、`retrieval.json` | 工具 Schema、检索结果与访问边界 |
| `planning_protocol.json`、`planning_activation.json`、`planning_stream.json` | 计划、激活模式与公开进度流 |
| `durable_protocol.json`、`recovery_protocol.json` | 持久执行与恢复 |
| `run_resume.json` | 使用 SQLite 验证公开 Root 恢复入口的拒绝错误码 |
| `agent_tree_protocol.json` | Agent 身份、Child Run 与共享预算 |
| `artifact_protocol.json`、`observability_protocol.json` | 产物、事件与诊断 |

## 运行

在仓库根目录执行：

```sh
python -m pip install -e '.[test]'
python -m pytest
npm --prefix typescript ci
npm --prefix typescript run check
```

公开恢复场景在可选 SQLite 包的测试中运行。按
[Python](../integrations/sqlite/python/README.zh-CN.md) 和
[TypeScript](../integrations/sqlite/typescript/README.zh-CN.md) 指南安装组件并运行测试。
这些测试重新打开临时数据库，验证恢复被拒绝后不调用模型或工具、不追加规范输出。
使用的是确定性网关，不代表真实 Provider 验证。

## 维护测试数据

修改共享契约时，同时更新对应测试数据和两端读取它的测试。使用两个运行时均能精确表示的值，
公开输出的预期结果中不得包含私有执行状态。各 SDK 的检查点格式和公开 API 名称可以不同。

包安装与外部调用检查见 [SDK 冒烟检查](../sdk-parity-smoke/README.zh-CN.md)。


## 能力契约与测试

- [结构化输出](structured-output.md)：`structured_output.json`、`structured_model_task.json`、`native_output_schema.json`。
- [计划逐片段输出](../ARCHITECTURE.md#planning-output)：`planning_stream.json`。
- [只读工具并发](../ARCHITECTURE.md#read-only-tool-concurrency)：`tool_concurrency.json`。
- [接入报告与恢复诊断](../ARCHITECTURE.md#integration-reports-and-recovery-inspection)：`recovery_inspection.json`。
- MCP 协议与 Schema 拒绝用例位于 `integrations/mcp/fixtures/tools.json`，由两端可选包测试读取。

本目录保留共享测试数据、运行指南和需要独立查阅的结构化输出参考；
运行时行为统一见架构文档，版本变化与升级边界见 [更新记录](../CHANGELOG.md#从-0x-升级)。
确定性测试、安装包检查、真实服务验证和使用方验收应分别记录，不能互相替代。

## 仓库文档边界

Git 保留安装指南、公开契约、可复现的测试用法及第三方许可声明。
实施计划、单次验收证据、发布清单和本机环境记录放在已忽略的 `docs/` 或
`conformance/reports/`，不要强制加入跟踪。删除当前文件不会清除旧提交中的副本。

SQLite 多进程／负载检查使用临时合成数据库：
`PYTHONPATH=src:integrations/sqlite/python/src python integrations/sqlite/python/scripts/verify_load.py --output /tmp/purra-load.json`。
先构建 TypeScript Core 和 SQLite；此检查不验证真实 Provider 延迟。
