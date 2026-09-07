# PurrA 1.0.0 候选：支持范围与升级

[English](release-1.0.md) | 简体中文

本文记录 1.0.0 **尚未发布候选**的支持范围，不代表发布公告。当前源码已进入 1.1.0 开发，见[审批契约](durable-approval.zh-CN.md)。注册表安装命令可能仍获取旧的已发布版本；验证候选时必须使用候选清单对应的准确 wheel 或 npm tarball。Core 与各个 PurrA 可选包必须使用相同版本。

## 首发能力

Python 和 TypeScript 均提供 Run 绑定的完整对象结构化任务、显式 `local`／`native_required`、有界且默认关闭的格式修复、限定作用域的 MCP 只读工具、显式安全只读批次的有界并行、接入检查报告及只读恢复诊断。Core 没有运行时依赖；七个可选包分别为 OpenAI、Anthropic、SQLite、interaction、compaction、Mem0、MCP。

模型能力、凭据、SDK 客户端、授权、资源范围、持久化、预算和关闭操作归宿主管理。安装适配包不会自动在 Agent 中启用它。参见[结构化输出](structured-output.md)、[工具并行](../ARCHITECTURE.md#read-only-tool-concurrency)、[接入与诊断](../ARCHITECTURE.md#integration-reports-and-recovery-inspection)。

## 服务验证与边界

以下为 2026-09-06 的验证情况，不是可用性 SLA、模型质量基准或对其他配置的保证。确定性 SDK 测试、安装产物、真实服务和下游业务分别计证。

| 路径 | 已有证据 | 保留边界 |
| --- | --- | --- |
| DeepSeek `deepseek-v4-flash` Responses，`native_required` | 双端实际 Schema 请求、本地复验、Run 回执、用量结算与持久重放通过 | 已验证的 Responses 协议／服务／模型组合，不代表其他组合或所有 Schema 约束 |
| DeepSeek Chat，`local` 与一次格式修复 | 双端通过；受控输入使真实模型先返回 Schema 不匹配，再发起新的修复调用并结算 | 需要宿主字段映射，不代表自然业务的修复成功率 |
| OpenAI Chat Completions 协议，原生 Schema | 双端 SDK 请求、响应和故障测试通过 | 兼容服务／模型的 `response_format.json_schema` 真实验证待完成；local Chat 验证不覆盖这项能力 |
| Anthropic Messages 协议，原生 Schema | 双端 SDK 请求、响应和故障测试通过 | 兼容服务／模型的 `output_config.format` 真实验证待完成 |
| Microsoft Learn MCP `microsoft_docs_fetch` | 双端真实文本读取与有界并行通过 | 仅选定并限定范围的工具，不代表其全部工具 |
| Cloudflare 文档 MCP `search_cloudflare_documentation` | 双端真实结构化 JSON、输出 Schema 复验与有界并行通过 | 仅选定的公共文档工具 |
| MCP 取消、断连、目录变化 | 双端独立进程 HTTP 服务退出、目录通知、在途旧结果丢弃及排队调用阻止通过；另有 Cloudflare 宿主取消验证 | 受控故障，不代表第三方生产故障；断连检测可能等待 RPC 超时，Python 传输上下文异常需宿主处理 |
| 下游应用 | 公共示例、独立安装消费者及接入契约已具备 | 实际业务验收由目标项目独立执行 |

`native_required` 同时要求明确的模型能力和适配器方言支持。不支持时调用前失败，不静默降级为本地模式。兼容接口名称本身不证明模型能力。

验收按“协议入口＋具体能力＋实际服务／模型组合”记录，不按模型厂商品牌设置门槛。OpenAI Responses、OpenAI Chat Completions、Anthropic Messages 是三个独立协议入口；兼容第三方服务可以为其实际支持的能力提供真实证据，不强制使用 OpenAI／Anthropic 自家的模型或凭据。`native_required` 指服务端原生 Schema 约束，不指原厂模型。上表 DeepSeek Responses 已满足该选定路径的真实验证；Chat 和 Messages 的原生 Schema 仍待兼容服务／模型实测。基础聊天兼容或一次成功响应不证明 Schema 约束、工具、流式、终止和用量语义全部兼容，只记录实际覆盖的能力。未验证组合仍标未验证；本次口径纠正没有新增通过结果或扩大支持声明。

MCP 仅支持明确声明的 Schema 子集。根部可声明确切的 `https://json-schema.org/draft/2020-12/schema`；声明保留在目录身份中并计入限额。其他方言、嵌套声明、引用、`default` 和厂商关键字仍拒绝，不承诺完整 JSON Schema 支持。远端只读注解不授予访问或并发权限。参见 [Python](../integrations/mcp/python/README.md)／[TypeScript](../integrations/mcp/typescript/README.md) MCP 契约。

## 从 1.0 起的公共兼容承诺

- 补丁版本保持文档化公共 API 和行为；次版本通过新增导出、可选参数或显式协商能力扩展，不向既有宿主端口添加必填方法。
- 公共类型和文档化导出是正式接口。私有模块路径、内部状态编解码及生成 SDK 的内部实现不作为应用 API；固定版本的存储适配契约另行声明。
- 保留已有事件和错误的含义。契约明确允许扩展的字段／代码，消费者应保留 unknown，不能把未知解释为成功或可恢复。封闭枚举不会因为底层是字符串就自动变为可扩展。
- 破坏公共契约需要主版本。删除历史格式支持或重解释持久状态不属于普通补丁；单独编号的存储版本不能绕过应用可观察的兼容承诺。

## 从 0.x 升级

本候选不承诺 0.x 别名、兼容编解码或自动迁移。不要原地切换正在执行的生产 Run。

1. 盘点 SDK／可选包版本、存储格式、调用回执、preset、模型／工具绑定和活跃 Run，保留备份及能读取原数据的环境。
2. 在旧运行时完成或明确取消活跃 Run，并对账外部效果。取消本地等待不等于远端效果已停止。
3. 在独立环境安装准确候选；优先使用新数据库。评估历史读取时使用可丢弃的备份副本，不把唯一生产数据交给验证脚本。
4. 分别验证历史读取和新 Run。重启宿主后检查新的持久 Run、用量和输出，保留回滚数据。

SQLite 存储为 v4，拒绝其他存储版本；Python 与 TypeScript 快照不能互换。新的 invocation receipt 为 schema v3，TypeScript 状态导入拒绝不支持的回执版本。因此，同为 SQLite v4 不等于 0.x 历史或活跃 checkpoint 一定兼容。本候选没有通用 0.x 历史迁移工具；选择新数据库也不授权删除旧数据。恢复诊断不授予 resume 权限。

未来 1.x 存储变更的发布说明必须分别声明历史读取、已验证的离线迁移和活跃 Run 续跑能力。可以明确要求排空活跃 Run，但不能静默遗弃任务和历史。迁移经过验证后才能声明支持。

## 候选验证与发布

以准确候选提交和各包产物验证公共导出、依赖版本、文件清单及哈希。支持的 CI 运行时矩阵与单一本机环境验证分别记录。本地构建成功或包版本变化不代表 CI 已执行或注册表已收到上传。

发布需要 `v1.0.0` 标签、包元数据、发布说明和产物完全对应，并完成或明确处理开放的支持门槛。准备候选不会自动创建标签、推送分支或发布包。
