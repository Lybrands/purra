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
| `agent_tree_protocol.json`、`delegation_protocol.json` | Agent 身份、委派与共享预算 |
| `artifact_protocol.json`、`observability_protocol.json` | 产物、事件与诊断 |

## 运行

在仓库根目录执行：

```sh
python -m pip install -e '.[test]'
python -m pytest
npm --prefix typescript ci
npm --prefix typescript run check
```

## 维护测试数据

修改共享契约时，同时更新对应测试数据和两端读取它的测试。使用两个运行时均能精确表示的值，
公开输出的预期结果中不得包含私有执行状态。各 SDK 的检查点格式和公开 API 名称可以不同。

包安装与外部调用检查见 [SDK 冒烟检查](../sdk-parity-smoke/README.zh-CN.md)。
