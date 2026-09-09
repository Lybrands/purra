# Approval-linked effect recovery

This matrix describes implemented Root single-write behavior. Reconciliation is a
host operation using independent effect evidence. It does not authorize a model to
resolve its own claims, change a Run's terminal state, or bypass current tool scope.

| Persisted Run | Effect evidence | Claim action | Subsequent recovery |
| --- | --- | --- | --- |
| Running, tool-ready | Confirmed completed result | Commit matching receipt/approval association | Reopen and resume; consume receipt without invoking the write again |
| Running, tool-ready | Proof of non-execution | Remove the matching unresolved claim/association | Reopen and resume through current scope, approval, expiry, identity, lease and budget gates; only then may the write execute |
| Running, tool-ready | Unknown | Retain claim | No automatic redispatch |
| Terminal | Known completion or non-execution | Reconcile the matching claim | Run stays terminal; resume is rejected |
| Any | Approval identity mismatch | Reject reconciliation and retain claim | Host must investigate the mismatch; do not delete the claim to force progress |

Python: `storage.reconcile_tool(run_id, call, result=known_result)` or
`storage.reconcile_tool(run_id, call, not_executed=True)`.
TypeScript: `storage.reconcileTool(key, {result: knownResult})` or
`storage.reconcileTool(key, {notExecuted: true})`.
The host already knows its actual claim identity; do not invent a key from display
text or infer it from a diagnostic report. Approval-linked reconciliation requires
an idle Run and validates the persisted association. Host authentication and proof
collection remain outside these low-level adapter methods.

Deterministic fault tests leave a running tool-ready Run by failing receipt/state
persistence. After host reconciliation and storage reopen, both SDKs verify one
external effect overall, the original tool-producing model round is not repeated,
and current scope revocation blocks further model/tool work. Separate terminal-Run
tests verify that reconciliation does not resurrect failed work. TypeScript also
checks approval expiry on this reconciled-running path: completed receipts may be
replayed, while a not-executed operation cannot dispatch after approval expiry.
These are synthetic faults, not evidence that a remote request was canceled.

## 同机 worker 的恢复规则

worker 不得把所有异常都重置为可重试任务。必须区分：

- 待审批：释放执行 ownership，等待宿主决策；不轮询模型重建调用。
- 正在运行且被未知 claim 阻塞：保持阻塞，等待宿主独立证据；对账后仍通过原恢复入口。
- 已终态：保持终态，对账只修正效果知识，不自动重新打开或创建替代 Run。
- 已确认完成：复用匹配回执；不重复外部写入。
- 已确认未执行：允许进入原门禁重新判断，不能直接派发。

该矩阵已由同机 worker 的 Root 单写调用、进程退出、自然 lease 过期和显式对账测试覆盖。
任意 Child 写入、嵌套交互恢复、分布式对账及自动替代 Run 不属于 W01 完成范围；其中
跨机器调度归 W02。确定性、安装产物、真实服务和下游证据仍分别记录；不把合成恢复测试
当成生产效果证明。
