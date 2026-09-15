# PurrA 重构计划

## 目标与原则

解决三个已确认的核心问题：`engine/orchestrator.py` 上帝类（3350 行）、`contracts/__init__.py` 未拆完的大杂烩（1935 行/51 类）、`agent_tree.py` 三合一顶层文件（1378 行），以及若干跨层耦合（Manager 参数漂移、授权双实现、testing 反向依赖内部等）。

**原则**：公共 API 零变化（`purra.api`/`purra.contracts`/`purra.ports`/`purra.testing` 的 `__all__` 与符号位置不变）；每个阶段独立可交付、测试全绿后再进入下一阶段；同步更新自研棘轮测试。

**关键约束（来自守卫调查）**：
- `tests/test_purra_structure.py`：`REQUIRED_CORE_MODULES`（约 40 个模块路径）、`MOVED_TOP_LEVEL_DEFINITIONS`（约 60 个禁回迁符号）、函数行数上限（`AgentCore.__init__` ≤200、`_execute_run` ≤450、`AgentRuntime.run` ≤500、orchestrator 私有函数 ≤300）——移动/改名模块必须同步更新。
- `tests/test_package_boundaries.py:68-85`：`purra.testing.py` 的 import 白名单（纯 stdlib + purra）。
- `tests/test_public_api.py`：23 个门面模块的 `__all__` 无重复/无下划线导出/无模块对象。
- 无 import-linter/ruff/mypy，全部边界靠这些棘轮测试；conformance fixtures 是纯 JSON，不受内部模块改名影响。
- 项目零运行时依赖（`project.dependencies == []`），不能引入新工具链。

**基线验证命令**：`python -m pytest`（约 836 个测试）；TS 侧 `npm --prefix typescript run check`（约 308 个测试，本计划基本不触碰 TS）。

---

## Phase 1：零行为变化的低风险清理（先行，独立提交）

### 1.1 拆分 `contracts/__init__.py`（零破坏）
新建 `src/purra/contracts/` 域模块，`__init__.py` 变为纯 re-export：
- `model.py`：AgentMessage、ModelRequest、ModelInvocation、ModelStream*/ModelCompletion、ModelTokenUsage 等（现 69–428 行区间）
- `planning.py`：PlanningConstraints/Capabilities/Result/Turn、ResponseConstraints、PlannerLimits（471–779）
- `context.py`：ContextBudget/Claim/Block/Bundle、ContextEvidenceReceipt、TaskContextRequest（781–1016）
- `tools.py`：ToolCall、ToolSchema、ToolDataContract、ToolPolicy、ToolHandlerResult、ToolBatchRequest/Result（1018–1427）
- `approval.py`：ApprovalRequest/Result（1429–1465）
- `run.py`：RunExecutionIntent/Lease、RunProvenance、RunCreateParams、AgentRunResult、RuntimeLimits、TraceRecord（1468–1811）
- `normalize_locale_tag`、`_tool_call_from_mapping` 随所在域迁入
- 同时把 `ports/run_lifecycle.py:25,39,63` 的数据类（RunBeginResult、RunBudgetSnapshot、RunCommit）迁入 `contracts/run.py`，ports 模块只留 Protocol 并从 contracts 导入

兼容性：所有 `from purra.contracts import X` 的 84 处调用点不变；`__all__` 保持 91 项、内容与顺序不变（棘轮锁定）；`enums.py`/`host.py`/`plans.py` 的 re-import 结构维持。同步更新 `test_purra_structure.py` 的模块清单（新增 contracts 域模块）。

### 1.2 `api/__init__.py:175` 改惰性导出
删除顶层 `from purra.testing import IntegrationCheck, check_integration`，改为模块级 `__getattr__` 惰性导入。公共符号不变（`test_public_api.py` 继续通过）。这切断 `import purra` → `testing.py`（1356 行）→ `engine/*` 的启动导入链。

### 1.3 消解 `BufferedEventSink` 同名撞车
`engine/durable_execution.py:35` 的 Protocol 改名为 `BufferedEventSinkProtocol`（或迁入 `output/ports.py`），`engine/canonical_sink.py:15` 的实现保留原名。

### 1.4 `storage/session.py` 去掉 setattr 动态绑定
将 `STORAGE_PORT_METHODS` 白名单 + `setattr` 循环（session.py:46-48）改为显式转发属性（每个端口一个 `@property` 返回 adapter 实现），类型检查器可校验；`:55` 的函数体内延迟 import 提升到模块顶部（确认无循环后再提，有环则保留并注释原因）。

### 1.5 `tools/contract.py` 更名
单数改复数 `tools/contracts.py` 并保留旧名兼容 re-export 一版（或同步更新全部引用 + 棘轮清单），与全项目命名惯例一致。

**验证**：全量 pytest 通过；`python -c "import purra"` 启动导入图显著缩小（可临时打印 sys.modules 对比前后）。

---

## Phase 2：agent_tree 包化 + 消除参数漂移 + 统一授权谓词

### 2.1 `agent_tree.py`（1378 行）拆为 `purra/agent_tree/` 包
- `agent_tree/contracts.py`：AgentNode、AgentTreeRun、各 Command/Receipt、AgentCapabilityGrant（数据 + 序列化）；`authorize_child` 的授权判定逻辑迁往 `agent_tree/policy.py`（与 AgentTreePolicy 同居）
- `agent_tree/ports.py`：RunTreeRepository Protocol（从 contracts 文件里分离，与 `long_tasks/ports.py`、`artifacts/ports.py` 惯例对齐）
- `agent_tree/memory.py`：InMemoryRunTreeRepository（730 行实现）
- 吸收 4 个卫星文件：`agent_tree_lease.py`、`agent_tree_query.py`、`agent_tree_receiver.py`、`agent_tree_delivery.py` → 包内 `lease.py`/`query.py`/`receiver.py`/`delivery.py`
- 修复依赖倒置：`agent_tree.py:5` 的 `from purra.adapter_state import AgentTreeState` —— 将 `AgentTreeState` 的消费点下移到 memory/adapter 侧，契约模块不再 import adapter 层
- `purra/agent_tree/__init__.py` re-export 全部现有公共名，`from purra.agent_tree import X` 的所有调用点（engine/orchestrator.py、adapters/memory.py、storage/session.py 等）零改动；旧卫星模块路径保留兼容 re-export 或同步更新引用
- 同步更新 `REQUIRED_CORE_MODULES` 与 `MOVED_TOP_LEVEL_DEFINITIONS`

### 2.2 Manager 构造工厂（消除 5 处参数漂移）
在 `model_invocation/` 新增配置对象 `ModelInvocationConfig`（吸收 `invocation_timeout_ms`、`runtime_limits`、`evidence_validator`、`max_tool_argument_chars`、`budget_repository`）+ 工厂函数。5 个构造点全部改走工厂：
- `runtime/orchestrator.py:246`（AgentRuntime fallback）
- `engine/orchestrator.py:2651 / 2776 / 3036`
- `planner.py:244`

### 2.3 统一工具范围判定谓词
新增 `tools/scoping.py`：`classify_tool_batch(calls, allowed_names)` 共享谓词，供两处复用、保证错误码一致：
- `runtime/tool_authorization.py:280`（软失败 → RETRY_MODEL 路径）
- `tools/executor.py:126-160`（硬失败兜底路径）
双检结构保留（安全边界合理），仅收敛判定逻辑与错误码来源；权威仍是 `run_state.py:721` 状态机。

**验证**：全量 pytest + 重点跑 `test_purra_structure.py`、`test_agent_tree*.py`、`test_tool_security*.py`。

---

## Phase 3：拆分 `engine/orchestrator.py`（收益最大，工作量最大）

从 AgentCore（417–2978 行，45 方法）抽出 4 个协作对象，AgentCore 保留为薄门面（公共 API 不变）：

1. **`engine/agent_tree_orchestrator.py`**（约 600 行）：`_AgentCoreTreeRunExecutor`（233-407）+ `_configure_agent_tree_capability`、`_bind_agent_tree_run`、`_settle_root_agent_tree_run`、`_bind_agent_tree_lease`、`_require_root_agent_tree_quiescent`、`_restrict_agent_capabilities`
2. **`engine/durable_resume.py`**（约 350 行）：`_resume_checkpointed_run`、`_resume_child_runs`、`_continue_durable_run`、`_settle_durable_updates`、`_prepare_runtime_phase`
3. **`engine/planning_promotion.py`**（约 300 行）：`_promote_auto_run` + `_execute_runtime_with_auto_promotion`
4. **`engine/run_budget.py`**：模块级预算函数 `_selected_context_window_tokens`、`_resolve_run_output_budget`、`_context_result_reserve_tokens`、`_execution_context_budget`（3249-3333）

同文件内的顺带收敛：
- 统一错误映射：`_execute_run`、`_promote_auto_run` 等处重复的 `except OperationCanceled/ContextOverflowError/Exception → record_trace + fail` 模式收敛到已有的 `_settle_execution_exception`/`_record_safe_exception`
- 合并 `_drive_runtime` 内逐行雷同的 `save_checkpoint`/`tool_checkpoint` 两个闭包为一个 helper
- checkpoint 丰富逻辑与 runtime 侧 `_restore_checkpoint`/`_build_execution_checkpoint` 的职责边界写入注释（重构不跨层移动，仅划界）

**棘轮更新**：抽出的符号全部加入 `MOVED_TOP_LEVEL_DEFINITIONS`，防止回迁；确认新模块私有函数 ≤300 行（现 `__init__` 251 行超标问题随装配逻辑外移自然解决，若仍超则把装配收进 `_runtime_dependencies`）。

**验证**：全量 pytest；`test_standalone_agent_conformance.py` 的棘轮断言（不得出现 `_execute_run` 等私有路径）继续通过；sdk-parity-smoke 跑一轮确认两侧行为一致。

---

## Phase 4：中优先级模块修整（可与 Phase 3 并行/穿插）

### 4.1 内存 adapter 共享工具层
新增 `adapters/_store_kit.py`：收敛三处重复的 revision/lease/digest guard、时间戳、`_required` 校验（`durable_memory.py:67-79`、`memory.py:65`、`agent_tree` 侧 `_now_ms`）。

### 4.2 拆分 `InMemoryLongTaskRepository`（1160 行单类）
按"Task 聚合 / Unit 调度 / 续租与失败"拆为 2-3 个协作对象，由 `InMemoryDurableAdapters` 门面继续暴露全部端口方法（端口不变）。

### 4.3 拆分 `long_tasks/dispatcher.py`（719 行）
- `long_tasks/continuation.py`：`prepare_continuation` 的 60 行校验（:208-268）
- `long_tasks/recipe_compiler.py`：`_compile_recipe_units`（:643）
- `long_tasks/progress.py`：`emit_progress`/`_dependency_outputs`/`_final_response`（:583-704）
- dispatcher 收窄为"幂等复用判定 + 创建 + 委托执行"；`coordinator.py` docstring 标注仅服务 dispatcher

### 4.4 收拢 LongTask 词汇
`task_admission/contracts.py:107-159` 的 `LongTaskDispatchReceipt/ExecutionStatus/ExecutionUpdate/ExecutionResult` 迁至 `long_tasks/contracts.py` 并 re-export，`task_admission` 保留导入兼容。方向依据实际依赖图选定（避免成环，Phase 1 结束后用 `test_internal_modules_have_no_import_cycles` 验证）。

### 4.5 `engine/durable_execution.py` 职责外移
`bind_event_to_run`（:520）、`_durable_step_statuses`（:422）、`_bind_durable_progress_to_plan`（:492）迁往事件/计划域模块（`planning_stream.py` 或 `output/`），durable_execution 保留 re-export。

### 4.6 contextvar 租约改显式传参
`adapters/memory.py:126` 的 `current_agent_run_lease` 隐式跨层读取改为显式参数传入 `_InMemoryRunRepository`（需同步修改调用方 `agent_tree_execution.py:380` 的设置点）。

**验证**：全量 pytest + `test_durable_memory_adapters.py`、`test_long_task*`、`test_storage_state.py`。

---

## Phase 5：护栏补强（防回归）

### 5.1 TS↔Python 类型名 parity 棘轮测试
新增脚本化测试（如 `tests/test_sdk_parity_names.py` + 对应 TS 侧检查）：扫描两侧同名类型集合，断言与 `docs/` 下登记的漂移清单一致（现有漂移如 TS `AgentRunInput` vs Python `AgentRunRequest`、`ContextOptions` vs `TaskContextRequest` 逐条登记）；新增漂移须显式登记，防止无声扩散。

### 5.2 `purra.testing` 收敛（可选，第二期做）
将 1356 行 harness 按端口拆到 `purra/conformance/` 子包，`purra/testing.py` 保留 re-export 门面；同步调整 `test_package_boundaries.py:68-85` 白名单到新模块。此项触及公共门面结构，放最后单独评审。

---

## 执行顺序与工作量估计

| 阶段 | 内容 | 规模 | 风险 |
|---|---|---|---|
| Phase 1 | contracts 拆分 / 惰性导出 / 小修 | 2-3 天 | 低 |
| Phase 2 | agent_tree 包化 / Manager 工厂 / 授权谓词 | 3-4 天 | 中 |
| Phase 3 | engine/orchestrator 拆分 | 4-5 天 | 中高 |
| Phase 4 | 长任务/dispatcher/词汇收拢 | 3-4 天 | 中 |
| Phase 5 | parity 护栏 / testing 收敛 | 1-2 天 | 低 |

每阶段结束：全量 `python -m pytest`（836 测试）必须全绿；Phase 3 后追加 `npm --prefix typescript run check` 和 sdk-parity-smoke 各跑一轮。所有提交保持 `project.dependencies == []`（不新增任何工具依赖，parity 检查用纯 AST/正则脚本实现）。

## 明确不做的事
- 不改任何公共 API 符号、`__all__` 内容与顺序（棘轮锁定）
- 不引入 import-linter/ruff/mypy 等新工具链（项目零依赖原则，继续用自研棘轮）
- 不动 conformance/fixtures JSON（双侧消费）
- 不合并 Python/TS 实现为生成式（跨语言契约明确"非镜像"，仅加 parity 清单护栏）
- 不在本次重构中改任何行为语义（错误码、事件顺序、存储格式）