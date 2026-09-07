# 持久化审批契约（1.1 开发中）

[English](durable-approval.md) | 简体中文

当前已实现阶段 B 的存储基础：不可变审批记录、宿主授权的事务决策及 SQLite v5 显式激活。
Run 暂停、执行恢复及 MCP 写工具**尚未实现**。
当前已落地的前置修复是：审批通过后、进入工具幂等网关前重新验证宿主 scope 和取消状态。
共享案例 `fixtures/approval_dispatch.json` 只验证这一边界。已有内存审批继续可用。

## 权限和意图身份

审批只针对一个不可变操作意图，不能代替当前权限、执行回执或模型轮次重放许可。
模型消息、工具返回、clarification 答案和远端 MCP 注解均不能授予审批权限。
新能力必须显式启用，并接入现有工具执行、Run ownership、取消和预算链路。
不增加旧宿主端口的必需方法，不更改现有 `ApprovalGateway`／`ToolApprovalGateway`
签名及结果状态；持久化存储或宿主授权不可用时，禁止降级为内存批准。

下面三种记录已在双端导出；派发关联仍待实现。Python 使用 snake_case，
持久化 JSON 使用与 TypeScript 一致的 camelCase：

| 对象 | 必需绑定 |
| --- | --- |
| `ApprovalIntent` | `schemaVersion`、`runId`、`rootRunId`、`toolCallId`、`toolName`、验证后的 `arguments`、`presetFingerprint`、`bindingId`、`bindingRevision`、`scopeId`、`scopeRevision`、`effect` |
| `ApprovalRecord` | `approvalId`、不可变 `intent`、`intentDigest`、`revision`、`status`、`createdAtMs`、`expiresAtMs`；决策后的审计记录 |
| `ApprovalDecisionCommand` | `approvalId`、`expectedRevision`、`intentDigest`、`commandKey`、`decision`；宿主另外提供已认证主体 |
| 派发关联 | `approvalId`、`intentDigest`、Run/call 身份、lease owner/epoch、现有工具 claim/receipt 身份 |

意图 schemaVersion 从 1 开始；effect 是宿主声明的 `write` 或 `destructive` 风险。
实际效果继续使用现有 `not_started / committed / unknown`，不另建效果分类。
intentDigest 使用 `purra.json-identity/v1` 覆盖完整意图；对象键顺序不影响身份，
任何参数、资源、配置、工具、调用或效果声明变化都影响身份。摘要只证明相等，不证明授权。
公开诊断不能暴露私有参数或它们的摘要；展示文字由宿主明确投影，不能成为执行依据。

MCP bindingRevision 包含所绑定目录的 digest。目录看不到的 handler、scope 和配置变化由
宿主版本化。恢复旧快照不能证明当前权限；实际派发仍需检查当前资源边界。

## 决策生命周期

持久化状态独立于旧 live gateway 结果类型：`pending`、`approved`、`rejected`、
`expired`、`canceled`。决策终态不可被另一决策覆盖；尚未消费的 approved 可以因过期或
取消失效。派发和效果另行记录，approved 不代表已执行。

| 当前状态 | 命令或观察 | 结果 |
| --- | --- | --- |
| 不存在 | 有效 Run lease 下与 tool-ready checkpoint 一起创建 | pending，revision 1 |
| pending | 已授权批准，revision/digest 一致，尚未过期 | approved；递增 revision，保存审计 |
| pending | 已授权拒绝，revision/digest 一致 | rejected；递增 revision，保存审计 |
| pending 或未消费 approved | 到达绝对到期时间 | expired；不能派发 |
| pending 或未消费 approved | Run／宿主取消 | canceled；不能派发 |
| 任意 | 完全相同的已接受 commandKey 和决策重放 | 返回原命令结果；不续期、不重复派发 |
| 任意 | 同键内容或主体不同、revision 过时、意图不匹配 | 冲突；无修改 |
| 终态 | 新的不同决策 | 冲突；无修改 |

resolver 从应用认证上下文取得主体，并调用必需的宿主 authorizer；主体字符串本身只是
审计标签。异步授权结束后，在存储事务内再次检查 revision、意图、Run 和有效期，确保
并发决策只有一个胜者。resolver 与底层审批存储不得注册为模型工具。

到期时间持久化为绝对时间，并受原 Run/Root deadline 限制；重启不续期，
`now >= expiresAtMs` 即不能派发。系统时钟回退不能复活已持久化的过期或取消状态。
过期后命令重放可以返回历史 approved，但历史结果不能作为新执行许可。

## 等待与恢复

审批批次应在**任何 handler 开始之前**暂停。审批意图、绑定的 tool-ready continuation
和规范等待事件必须在一个事务中持久化；提交之后才能发出独立的 `ApprovalRequired`
控制信号并释放 lease。不保留等待 worker、模型请求或 SQLite 写事务。
提交失败不能派发工具，也不能宣称已进入可恢复等待。

首版显式启用的持久审批路径每批只接收一个写调用；混合或多调用持久审批批次在派发前拒绝。
这保留 Python 原有写批次限制，不删除 TypeScript 旧 live 多审批行为。只读并发和
宿主管理的可恢复 Artifact 批次仍遵循已有契约。

`ApprovalRequired` 不能复用 `UserInputRequired` 或普通工具错误，不能反馈模型进行重试。
supervisor 与 Agent Tree 要保留非终态 Run、释放执行 ownership；等待继续消耗 deadline，
但不重置已花费预算。

现有 v2 `model_ready` 代表工具轮次已完成，不能塞入尚无结果的 assistant tool call，
也不能通过清空 checkpoint 后 attempt 计数绕过恢复检查。新增版本化的 `tool_ready`
continuation 必须保存：

- 已结算模型调用及完整 provider-neutral continuation，包括 Provider 特有消息字段、原始调用身份。
- 当前计划授权、执行与证据状态、用量结算、轮次、重试账本、原配置、deadline 和预算。
- 审批引用、派发/回执关联及已完成的调用；不能伪造工具消息或重跑模型轮次。

同一轮次 checkpoint 的变更必须是规范仓库明确验证的窄状态转换。不能只递增 inputRevision
就改写参数；现有 `is_input_checkpoint_update` 仍只处理 clarification／Child join。
恢复先获取原 Run lease 并核对 checkpoint/configuration，实际派发再检查取消、Run/Root
deadline、剩余执行预算、工具/参数/目录、scope、审批状态和到期时间、未知效果。
原计划和 Child capability grant 仍生效；一个子 Run 的批准不授权父或兄弟 Run。

## 派发、效果和回执

在当前 lease epoch 下，以一个事务将审批消费与**现有工具 claim**绑定。两个恢复者或重复
批准不能取得两次派发权。回放已提交回执时必须核对完整意图关联；TypeScript 原有不透明
幂等 key 无法单独证明 Run/参数关联。

外部调用在事务外执行，claim 必须先持久化。claim 后崩溃即保守视为 unknown，即使进程
可能尚未发出请求。成功时原有工具回执和审批关联一起提交。写工具不能进入安全只读并行路径。

| 观察 | 效果及处理 |
| --- | --- |
| 参数、权限、目录、审批、lease、取消门禁在派发前拒绝 | not_started；无外部调用 |
| 有效成功写响应且回执提交成功 | committed；回放回执，不再次派发 |
| 远端错误但协议不能证明未产生效果 | unknown |
| 派发后超时、取消、断连、响应丢失 | unknown |
| 派发后结果无效/过大、目录变化、回执持久失败 | 保留 claim；unknown |
| 宿主取得可靠完成证据 | 经现有 reconciliation 路径保存匹配意图的结果 |
| 宿主证明未执行 | 协调现有 claim；重试仍需当前有效审批及所有执行检查 |

unknown 禁止可能重复效果的自动重试/重规划。迟到结果不能覆盖新 owner 或协调后的回执。
不承诺缺乏远端协议支持的端到端 exactly-once。模型和诊断报告不能充当 reconciliation 证据。

## MCP 写绑定

旧只读绑定保留默认行为和快照语义。新写绑定要求宿主显式声明效果、confirm policy、非空
scope validator，以及稳定的资源/配置身份；远端 readOnlyHint/destructiveHint、描述、模型
参数或安装包都不能开启写权限。缺少持久门禁/回执能力时 RPC 前失败，propose 不能替代审批。

RPC 前重查 schema、scope、目录 revision/连接和取消。写绑定禁止 concurrencySafe。
成功响应可提供完成证据；RPC 后传输、协议或工具结果错误均为 unknown，除非明确支持的
远端契约能证明未发生效果。原只读工具错误分类不变。

## 持久化与 1.0 兼容

不启用新能力时，保留 v2 checkpoint 和 1.0 存储行为。双端执行快照仍不能互换，
只统一新意图/决策 JSON 语义及共享案例。

写入新 tool-ready 记录之前必须有显式格式隔离，确保 1.0 进程无法继续 claim/resume 或
丢弃审批元数据。不能只把新数据塞入旧 writer 可能忽略的扩展字段。

确定的适配方案是：保留 legacy-only 数据库的 v4 读写支持；通过显式事务激活审批能力的
v5 格式，保留历史 Runs、journals、已提交 receipts。激活要求整个数据库所有 scope/SDK
没有活跃 Run 和未协调 claim。宿主备份后离线激活；默认构造函数不自动迁移 v4。
旧内存审批不能导入为持久化权限；旧未知效果先用原版本协调。

存储基础已实现显式激活、数据库标记及拒绝旧 v4 写入的 INSERT/UPDATE 触发器。
两端快照 codec 不同，因此激活遇到其他 SDK 的行会拒绝，不能假设其没有活跃执行。
同 SDK 的所有 scope、queued/waiting 子 Run 及未协调 claim 都纳入检查。
合成数据库测试覆盖历史读取、拒绝后行不变、旧版本探测和旧连接写入被拒绝。
PurrA 开发不迁移真实业务数据库。

## 诊断与验收

扩展现有只读 inspection，提供规范化审批计数/状态、意图是否匹配及固定等待、冲突、过期、
未知效果原因码。未知仍是未知；不包含标识符、参数、主体、原始错误或意图 digest，
authority 始终为 diagnosis_only。诊断不能执行过期状态更新、协调、claim 或调用工具。

双端确定性验收覆盖：决策重放/冲突、授权拒绝、精确到期、取消与撤权、同 Run 重启且不重跑
模型轮次、lease 竞争、参数/配置/目录变化、不支持的审批批次派发前拒绝、claim/receipt 提交失败、迟到响应、
效果协调、Agent Tree 等待、v4/v5 兼容。纯状态测试不能证明事务或重启安全。

wheel/npm tarball 使用源码目录之外的独立消费者，检查不含内部文件。独立受控 MCP 服务
只写合成资源，并对断连/重启故障统计实际写入。真实 Provider/MCP 证据注明协议入口、能力、
实际服务/模型。下游单独验证宿主授权、存储、UI、恢复；在目标项目完成之前保持未通过。

## 已实现的存储入口

Python 从 `purra.approvals` 导入三种审批记录；TypeScript 从 `purra` 导入。
Python 显式调用 `await storage.enable_approvals()`，再通过
`storage.approval_store(authorize=callback, clock_ms=clock)` 获取存储。
TypeScript 对应 `enableApprovals()` 和 `approvalStore({authorize, clockMs})`。
时钟可省略，默认当前 epoch 毫秒；格式激活是整个数据库的离线操作，不应作为启动自动迁移。

存储提供 `create`、`get`、`list_pending` / `listPending`、`decide`、`refresh`。
创建要求 Run 正在运行、Root 和持久化配置摘要一致；过期时间受 Run/Root deadline 限制。
相同创建请求返回原记录，不续期。此处 binding/scope revision 是宿主声明，尚未接入实时派发校验。

宿主从认证上下文提供主体，不能采信模型或工具返回的主体。`authorize(principal, record, command)`
必须返回严格的 true，在事务外执行；随后事务重查身份、revision、规范 Run 取消、过期及配置。
并发决策不能相互覆盖。相同 commandKey、命令及主体的重放返回原决策回执，即使后来已经过期；
该回执仅为历史证据，不能授予派发权限。

`get` 和列表只读取持久记录，不在读取时自动过期；`refresh` 显式持久化过期或 Run 取消。
列表包含尚未持久化失效的 pending 和 approved，不能据此判断当前可执行。
这些方法尚不创建 checkpoint、工具 claim、Run 暂停或公共审批事件；阶段 B 剩余链路打通前，
运行时仍使用已有 live approval 路径。
