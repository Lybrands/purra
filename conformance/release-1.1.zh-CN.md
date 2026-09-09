# 1.1 开发接入交接

[English](release-1.1.md) | 简体中文

1.1.0 尚未发布。Core、SQLite、MCP 须使用版本一致的本地产物；本地开发验证与发布、
远端 CI、下游启用分别记录。公共 API 见[持久化审批契约](durable-approval.zh-CN.md)。

## 支持边界

双端支持持久化 Reactive、Planned、Auto Root 的单写调用审批批次。等待时同事务保存
工具续点、已结算模型调用及意图，然后释放 lease。重启保留规划、用量、原 deadline 及
已完成的只读 Child 结果，不重跑模型轮次或已完成写入来重建等待。

审批决策必须来自已认证宿主主体并经过显式 authorizer。派发仍检查当前绑定、参数、
scope、配置、取消、deadline、预算及 lease。MCP 写入口要求独立宿主绑定，远端注解
不能授权。审批意图与既有工具 claim 原子关联；效果不明时保留 claim，阻止自动重派，
对账必须由宿主提供独立证据。

Child 写入、混合／多调用审批批次、任意嵌套审批与 clarification 组合，以及通用分布式
效果恢复不在支持边界内。不承诺端到端 exactly-once。

## 扩展 1.1 完成边界

归入 1.1 的存储与效果恢复工作，已在上述 Root 单写支持边界内完成：

- SQLite 元数据访问已使用索引候选页、选定 Root 日志恢复和经过校验的元数据事务。
  双端一次性负载入口覆盖独立并发 writer、日志序列连续性、被杀事务回滚、checkpoint
  重开和未知效果对账。测量值只描述有界合成运行点，不构成通用吞吐、延迟或生产容量保证。
- 1.1 支持的数据升级是显式的同 SDK v4→v5 离线路径：保留历史读取，拒绝活跃 Run、
  未解决 claim 和其他 SDK 行，并提供包含 WAL 的备份及恢复到新路径。任意历史格式迁移
  与活跃 Run 跨版本续跑不是 1.1 公共契约。
- 效果恢复覆盖已实现的 Root 单写完整矩阵：已确认完成时复用回执，确认未执行时重新经过
  当前全部派发门禁，未知效果继续阻塞，终态 Run 保持终态，审批身份不匹配则拒绝对账。
  跨机器 ownership 与通用分布式对账属于后续契约。

这些完成判断不提升业务 MCP、下游、远端 CI、跨平台迁移或生产负载等独立证据门槛。

## 宿主接入顺序

1. 保留备份，用原运行时完成活跃 Run 并处理未知效果。离线 v5 激活检查全部 scope，
   拒绝活跃 Run、未知 claim 及其他 SDK 数据；保留同 SDK 历史。默认构造不自动迁移 v4，
   不在生产 Run 执行期间切换版本。
2. 审批决策入口独立于模型／工具 API，从认证上下文取得主体。展示文案由宿主投影，
   不将私有意图或模型提供的文字当成授权依据。
3. 工具续点回调明确选择业务写工具，使用真实调用、已存 preset 和当前注册身份构造意图。
   持久化原绝对过期时间；approval gateway 与 idempotency 必须属于同一存储。
4. 收到 `ApprovalRequired` 后展示待审状态并释放 worker；宿主决策后用原 Run/request
   及相同回调恢复。审批与诊断都不能跳过派发复验。
5. 效果不明交给宿主对账。断连、进程退出或取消本地等待均不能作为清空 claim 的依据。

旧 live 审批、clarification、只读 MCP 及 v4 接入保持原契约。旧 model_ready 为 schema 2，
可选 tool_ready 为 schema 3。Python 与 TypeScript 存储不能互换。

## 证据与复现

| 类别 | 入口 | 验证边界 |
| --- | --- | --- |
| 确定性 | SQLite 双端 approvals、approval-resume 测试及共享 approval JSON fixtures | 授权、决策竞争、重启、回执失败、lease 故障和只读诊断 |
| 有界 SQLite 负载 | 构建 TypeScript Core 与 SQLite 后运行 `integrations/sqlite/python/scripts/verify_load.py` | 一次性双端数据库、并发 writer、回滚、恢复和重开；不主张生产容量 |
| 安装产物 | MCP 双端 installed-write 消费者 | 源码目录外安装匹配 wheel/tarball，检查模块来源及内部文件排除 |
| 独立 MCP 进程 | `integrations/mcp/fixtures/write_server.py` | 脚本模型、合成文件；成功、丢响应、写前／写后退出及写后报错，检查账本和进程退出 |
| 真实 Provider／业务 MCP | 协议入口＋能力＋实际服务／模型 | 脚本模型及合成写服务不能证明此项 |
| 下游 | 宿主认证、UI、资源策略、离线激活与重启 | 本任务未执行，也未启用相邻项目 |

可执行宿主接线及命令见 [Python MCP README](../integrations/mcp/python/README.md) 和
[TypeScript MCP README](../integrations/mcp/typescript/README.md)。审批诊断不含私有标识、
参数或摘要，authority 保持 diagnosis_only。启用业务写入前须用实际交付产物完成宿主验收。

## 已验证的真实 Provider 组合

智谱 `open.bigmodel.cn` 的 GLM-5.3-Flash，通过 Z.ai Chat Completions、思考开启且
`reasoning_effort=low`，已在双端安装消费者中通过成功写入和响应丢失两种场景。
MCP 为独立合成写服务；每个场景均在 SQLite 重开后保持待审且不重跑模型，宿主批准后
才执行一次远端写入。响应丢失保留 unknown claim，再次恢复终态 Run 不重派；服务均已退出。

此证据使用测试专用宿主 HTTP 网关：developer 转 system，JSON 工具结果转文本，
使用智谱的 max_tokens 和 thinking 字段。请求为非流式，Python 将完整响应投影到 Core
stream 端口。因此不证明原生 OpenAI 适配器、线级流式、Planned/Auto 真实模型、原生
Schema、业务 MCP 或下游验收。每次生成上限 4096 token；未验证 Root 聚合用量保证。
证据不含凭据、模型正文或业务资源。
