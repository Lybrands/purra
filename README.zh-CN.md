# PurrA

简体中文 | [English](README.md)

`purra` 是与具体产品无关的 Agent 框架，分别提供独立的 Python 与
JavaScript/TypeScript 实现。两端共享行为契约与一致性夹具，但任何一端都不会导入或启动
另一端的运行时。PurrA 负责通用合约、规划与运行时策略、Run 生命周期、工具授权、人工审批及
多 Agent 编排端口。

## 安装

Python：

```bash
pip install purra
```

JavaScript 与 TypeScript 使用同一个 npm 包：

```bash
npm install purra
pnpm add purra
yarn add purra
bun add purra
```

npm API 与示例见 [TypeScript 包 README](https://github.com/Lybrands/purra/blob/main/typescript/README.md)。

PurrA 的默认执行形态是 Reactive：模型可以直接在已授权工具上完成普通 model/tool loop，不要求 Planner、`TaskSpec`、任务准入、`ExecutionRecipe`、长任务仓储或领域上下文 Provider。规划和持久执行是显式装配的第一方能力，不是所有 Agent 的必经阶段。

## 语言发行物

仓库根目录的 Python 包仍是完整的 `0.2.x` 实现。`typescript/` 下重新建立了一份
符合 TypeScript 习惯的独立 ESM 包；当前 `0.1.0-alpha.0` 包以有界 Reactive
model/tool loop、递归工具 Schema 检查、整批授权、审批、作用域、幂等与 effect-state
安全，以及 `AbortSignal` 取消。`Agent.invoke()` 与 `Agent.stream()` 仍是瞬时便捷 API；
`Agent.submit()` 增加了内存中的权威 Run/output journal，包括 Preset 指纹、调用 receipt、
先持久化后发布、有序回放、取消、绝对 deadline、usage/output 预算和原子终态提交。
消息、usage、能力快照和输出限制保持不可变且与 Provider 无关。
可选的上下文组合支持硬输入预算、不透明 allocation claim、Single Pass 检索、经过校验的
请求级裁剪/压缩、Staged 检索契约和绑定到调用的 evidence receipt，并且不会改写权威会话历史。
模型型上下文与压缩工厂会获得受管、无工具的 runner；在 submitted Run 中，这些扩展调用复用
同一 Run 的调用 receipt 与预算。
显式 Planned 装配现已支持语义化 `TaskSpec`/`WorkPlan`、由 Core 编译并持有的当前步骤工具权限、
公开/私有 capability lowering、Staged 任务上下文、响应校验和受宿主约束的有界重规划。
宿主既可以继续注入自己的 Planner/Judge，也可以选择 TypeScript 原生的模型型参考实现；其模型调用
仍受同一 Run 的 receipt、预算与当前步骤权限约束。
显式 Durable 装配已经覆盖任务准入、Long Task DAG、租约、checkpoint、续跑与孤儿恢复；
Artifact 作为独立的可恢复输出聚合提供版本批次、claim 和校验；可选的一次 Run 内委派则让
隔离、受限的 delegated Agent 复用 Root Run 的模型调用、只读工具、取消和生命周期事件权威。
不包含业务正文的运行、稳定性、恢复、性能、趋势与回归门禁报告现已消费与 Python 相同的
冻结证据语义；确定性的回归/安全套件和公共适配器探针覆盖当前宿主端口，但诊断不会获得运行时
控制权。当前 alpha 已加入与 Python 共享决策语义的有界 Provider、工具和响应恢复；仍不包含
生产级持久化或 Provider SDK。带凭据的 completion/streaming 证据属于本次 npm 发布之外的
未来宿主项目工作；通过前不会宣称稳定版能力对等。
两端共享的模型、工具与恢复夹具位于 `conformance/fixtures/`，采用相同的安全语义，
但各自保留符合语言习惯的公开 API 和实现结构。

JavaScript 与 TypeScript 使用同一个 npm artifact；发布门禁要求 npm、pnpm、Yarn 与 Bun
安装同一份精确打包候选物。
Python 与 npm 在 1.0 前独立演进；版本号相同不代表能力自动对等。npm 支持范围以 TypeScript
能力矩阵为准，任何不兼容的 npm 公共 API 变更在 1.0 前必须升级 minor 版本。

宿主中有界的模型型 Hook 统一使用 `purra.model_execution`。宿主只声明模型请求、输出策略、工作单元数和推理偏好；实际供应商额度解析、`ModelInvocation` 构造及结束原因判定全部由 PurrA 完成。缺失结束原因或输出截断都会失败关闭，并且不会重放已经产生部分输出的调用。

每次真正请求 Provider 前，PurrA 都会持久化打开输出流，并写入私有的 `stream.opened` receipt。它把调用与输出流身份关联到脱敏调用参数、精确模型可见消息与工具 Schema 的指纹，以及宿主声明的上下文来源；不会把 Prompt 正文复制进输出日志。receipt 持久化失败时，Provider 调用不会发生。

上下文块通过 `ContextBlock.host_metadata["context_evidence_receipts"]` 声明这些来源凭证。每条凭证只使用通用的 `evidenceId`、`source`、`itemId`、可选 `version` 与宿主元数据；PurrA 不解释产品定义的来源值。

## 公共宿主契约

PurrA 不提供一个“万能 Host 对象”；宿主按需组合现有公共契约：

| 宿主需求 | 公共导入面 | 所有权 |
| --- | --- | --- |
| 装配并提交 Run | `purra.api`（`AgentCore`、`AgentPreset`、`AgentComponentBinding`、`AgentCoreRunOptions`、`PromptSection`） | 提交后由 PurrA 拥有执行。 |
| 描述输入与不透明的领域关联 | `purra.contracts`（`AgentRunRequest`、`RunBinding`、`ExecutionRecipe`） | 宿主映射产品输入，并解释自己的关联。 |
| 实现运行期依赖 | `purra.ports`（模型、上下文、工具、Run/输出持久化、Projector） | 宿主提供具体适配器。 |
| 选择可选能力 | `purra.task_admission`、`purra.long_tasks`、`purra.artifacts`、`purra.delegation`、`purra.output` | 宿主选择并配置；PurrA 强制执行契约。 |
| 验证适配器 | `purra.testing` | PurrA 提供无外部依赖的合规断言。 |

`purra.api` 是完整 Run 的唯一入口。宿主不得通过 `purra.engine`、`purra.runtime` 或其他实现模块启动 Run。产品请求映射、传输、凭据、业务查询和领域投影始终位于 PurrA 外部。`RunBinding`、`ExecutionRecipe` 与 `DomainEventProjector` 只不透明地携带宿主语义，PurrA 从不解释其中的业务字段。

### 0.2 兼容边界

在 `0.2.x` 系列中，`purra.api` 以及上表列出的能力所属公共模块构成受支持的宿主契约。兼容性新增和修复可以发布 patch 版本；PurrA 在 1.0 之前删除或改变现有公共契约时必须升级 minor 版本。`purra.engine`、`purra.runtime` 及其实现子模块不属于宿主兼容面。CI 会同时构建 wheel 与 sdist，在干净环境安装 wheel，并只通过公共导入运行一个 Agent。

本系列受支持的顶层宿主模块为：`api`、`artifacts`、`cancellation`、
`context_budget`、`context_orchestration`、`context_strategies`、`contracts`、
`delegation`、`errors`、`evaluation`、`events`、`evidence`、`json_values`、`long_tasks`、
`model_call_parameters`、`model_execution`、`model_invocation`、
`model_protocol`、`normalization`、`observability`、`orphan_recovery`、
`output`、`ports`、`recovery`、`run_control`、`stream_ownership`、
`structured_output`、`task_admission`、`testing` 与 `tools`。宿主只导入这些
模块根导出的符号；除非本文明确点名，实现子模块不属于兼容面。

## 依赖方向

```text
宿主 / 领域 / 基础设施
          ↓
       应用服务
          ↓
      purra
```

Core 不得导入宿主传输协议、产品领域、模型 SDK、数据库驱动或具体持久化适配器。
独立宿主一致性测试只通过 PurrA 公共契约装配运行，以验证这一依赖方向。

## 模块边界

`runtime`、`engine`、`contracts` 与 `ports` 都按职责组织。宿主使用上面的公共导入面；选择某项能力时，也可直接导入它所属的公共模块：

- `runtime/orchestrator.py` 只协调运行流程；模型流聚合、工具批次和最终回答校验分别位于 `model_round.py`、`tool_round.py`、`response_finalization.py`；
- `engine/orchestrator.py` 组合完整 Run；上下文、可选规划能力、可选任务编排能力、底层持久执行协议和选项分别位于独立模块；
- `ContextStrategy` 独立选择单次上下文或分阶段检索，不再由 Planning Policy 间接决定；
- `ExecutionProfile` 冻结宿主选择的 Planner、Planning Policy、上下文策略与任务准入/分发；
- `contracts` 按 messages、planning、context、tools、runs 提供稳定导入边界，基础枚举位于 `enums.py`；
- `ports` 是 model、context、planning、tools、persistence 和 run lifecycle 端口的稳定公共聚合。

实现包属于私有细节，不构成宿主兼容性入口。

## Agent 装配边界

Core 通过三个业务无关契约支持产品宿主：`RunBinding` 保存不可解释的聚合与命令关联，`ExecutionRecipe` 校验宿主编译的机械 DAG，`DomainEventProjector` 允许持久化适配器在同一提交事务内投影领域 effect。Core 不读取 Binding 的产品含义，不生成产品 Recipe，也不解释 Projector 的业务结果。

产品请求 DTO、请求映射、事件映射、权威状态查询和业务 Run 生命周期必须位于 Core 之外。因此通用 Run Service 与传输映射只包含通用契约和事件。

`AgentPreset` 是 PurrA 完整且已经解析完成的 Agent 装配契约。它只拥有稳定 ID/revision、有序可信 `PromptSection`、Context/Tool 端口、`ExecutionProfile`、压缩策略、运行限制和恢复策略。产品路由、请求补水、Repository、数据库查询、Provider 凭据与进程清理不得进入 Preset。

`AgentCore(preset=...)` 会在 Run 发布前物化可信 Prompt，并在 `run.started` 中写入 schema version 2 的 `AgentPresetSnapshot`。指纹覆盖 Preset ID/revision、Prompt Section、上下文与压缩绑定、执行形态、实际启用工具的 Schema 和授权契约、委派策略、运行限制与恢复策略。PurrA 只为无状态内置实现和不可变压缩参数推导稳定记录；每个影响行为的宿主组件或工厂都必须通过 `AgentComponentBinding` 声明稳定 ID、revision 和可选配置摘要，Core 不反射任意对象状态，也不使用 `repr()` 生成指纹。持久任务续跑会在任何新 Provider、工具、调度器或委派执行前拒绝能力漂移。version 1 或字段不完整的历史快照无法证明有效组合，会以 `agent_preset_snapshot_unsupported` 失败关闭，不会用当前进程配置猜测升级。

显式散装参数形式仍可用于普通 Run，但持久续跑必须配置 `AgentPreset`，否则 Core 无法重算并比对 version 2 权威快照。

## 执行形态

PurrA 使用同一个 Kernel 支持三种逐层增加约束的组合，不把它们实现成可替换安全边界的通用插件系统：

- **Reactive**：`AgentCore` 的默认形态，使用 `ContextStrategy.SINGLE_PASS`。直接运行模型与已授权工具，不创建 Planner，也不会调用 Staged Provider 的 planning/task context 接口；
- **Planned**：同时显式注入 `WorkPlanner` 和领域 `PlanningPolicy`，或使用公共 `AgentPlanner` + `ToolPlanningPolicy`，从而启用 `PlanningCapability`。该能力负责生成、编译与校验计划；需要分阶段检索时由宿主显式选择 `ContextStrategy.STAGED`；
- **Durable**：在 Planned 之上注入 `TaskAdmissionEvaluator` 与 `LongTaskDispatcher`。只有此时 Core 才构造 `TaskOrchestrationCapability`，由它负责准入、持久化交接与续跑，并复用同一套底层持久 DAG 协议；宿主仍拥有 `ExecutionRecipe` 和业务拆分。

```python
from purra.api import (
    AgentCore,
    AgentPlanner,
    AgentPreset,
    ContextStrategy,
    ExecutionProfile,
    ToolPlanningPolicy,
)

# Reactive：通用 Agent 的最小默认组合。
reactive = AgentCore(
    model_gateway=model_gateway,
    run_repository=run_repository,
    preset=AgentPreset(
        id="reactive",
        revision="1",
        tool_catalog=tool_catalog,
    ),
)

# Planned：显式选择 PurrA 的计划驱动风格。
planned_profile = ExecutionProfile(
    planner=AgentPlanner(model_gateway),
    planning_policy=ToolPlanningPolicy(),
    context_strategy=ContextStrategy.STAGED,
)
planned = AgentCore(
    model_gateway=model_gateway,
    run_repository=run_repository,
    preset=AgentPreset(
        id="planned",
        revision="1",
        tool_catalog=tool_catalog,
        execution_profile=planned_profile,
    ),
)
```

Core 不会根据工具、`planner=` 参数或非 Reactive Policy 猜测并创建 Planner。缺少 Planner/Policy 任一方都会在装配期失败；`ToolPlanningPolicy` 只是一个可显式选择的公共策略，不是隐藏默认值。

## 规划上下文与语义边界

规划输入与普通运行输入共用 `ContextBundle.blocks` 和统一预算。`build_planning_context()` 返回的每个 `ContextBlock` 都保留名称、内容和 `untrusted` 信任标记，并作为有界的 `planningContext` 进入 Planner。`ContextBundle.diagnostics` 只用于观测，永远不是模型输入；禁止借助诊断字段建立不计预算、无来源的隐藏上下文通道。

可信 `PromptSection` 同时约束执行模型和 Planner，因此 Preset 的身份、语气与工作原则不会在规划阶段丢失。普通 Host Context 和最近对话仍按不可信数据处理，不能提升为系统指令。

PurrA 的 Planner 只生成通用 `TaskSpec` 和可见步骤，不识别 Artifact 所有权或任何产品协议。宿主可以通过有预算的可信 Planning Context 描述允许的语义目标字段，并在领域边界验证；Core 只负责工具权限、约束、依赖和执行授权。

Planner 产物与 Runtime 权限是两份不同契约。`WorkPlanner` 只能返回由无状态 `WorkStep` 组成的语义 `WorkPlan`；只有 Core 可以把它编译成 `ExecutionPlan`，宿主插入的 prerequisite 和私有协议节点只存在于执行计划中。`RunStateMachine` 会直接拒绝未经编译的 `WorkPlan`。

Runtime 只读取当前派生出的一个 `ExecutionTransition`，其中包含活动步骤和当前/未来工具授权。Todo 事件只投影原始 WorkStep lineage，不公开私有执行节点、Runtime 工具标识或私有依赖。委派是普通的宿主授权工具能力，不再是一种独立计划执行器；真正可持久执行的机械 DAG 仍然是宿主单独提供的 `ExecutionRecipe`。

Kernel 负责模型协议、上下文硬预算、工具执行安全、授权审批、Run/Event、取消和恢复等不可绕过机制；“是否规划、检索什么、是否进入持久执行、如何验证业务结果”由显式能力和宿主组合决定。通用性的验收标准是：一个只有普通工具的 Agent 可以在不了解 `TaskSpec`、Admission、LongTask 和产品数据库的情况下完整运行。

宿主通常把 `ExecutionProfile` 放入 `AgentPreset`。`AgentCore` 的显式散装参数作为刻意手工装配能力的底层 Kernel API 保留，但不能与 Preset 混用。Provider Gateway、审批、持久化、输出日志、Lease 和幂等仍属于 Kernel 基础设施，不进入 Preset。

## 可移植宿主合规样例

`tests/test_standalone_agent_conformance.py` 是可执行的第三方宿主样例。它不导入任何产品 Application、Domain、Infrastructure 或模型 SDK；样例装配 PurrA 官方、仅依赖标准库的 `InMemoryAgentAdapters`，然后通过 `AgentCore.submit()` 分别跑通 Reactive、Planned + Staged Context 与 Durable handoff。包边界门禁会禁止样例退回私有 `_execute_run`。

```python
from purra.api import (
    AgentComponentBinding,
    AgentCore,
    AgentPreset,
    InMemoryAgentAdapters,
    PromptSection,
)
from purra.tools import InMemoryToolCatalog

adapters = InMemoryAgentAdapters()
agent = AgentCore(
    model_gateway=model_gateway,
    run_repository=adapters.runs,
    output_repository=adapters.outputs,
    output_publisher=adapters.publisher,
    preset=AgentPreset(
        id="portable",
        revision="1",
        tool_catalog=InMemoryToolCatalog(()),
        context_provider=context_provider,
        component_bindings={
            "contextProvider": AgentComponentBinding(
                "host.portable-context",
                "1",
            ),
        },
        prompt_sections=(PromptSection(
            name="identity",
            order=-100,
            text="Be concise, explicit, and calm.",
        ),),
    ),
)
```

这组内存适配器保留 Run/输出日志原子提交、source event 幂等、游标顺序、订阅唤醒、Artifact 写者互斥和 DurableTask checkpoint。它是进程内可执行规范：可以验证重启恢复语义，但不会跨真实进程重启保留数据，也不提供多进程协调或生产级持久化。

任何持久化宿主适配器都可以运行同一个、无测试框架依赖的契约探针：

```python
from purra.testing import assert_host_adapters_conform

await assert_host_adapters_conform(
    runs=run_repository,
    outputs=output_repository,
    publisher=output_publisher,
    session_id=test_session_id,
)
```

探针统一检查 Run 创建、规范事件顺序、source key 幂等、输出流终态栅栏、提交后唤醒，以及 validated 终态的原子提交。数据库或远程存储的创建和清理仍由宿主自己的 fixture 负责。

上下文宿主也使用同一个模块，不需要采用 PurrA 专属测试框架：

```python
from purra.testing import (
    assert_context_compression_hook_conforms,
    assert_context_provider_conforms,
)

await assert_context_provider_conforms(
    provider=context_provider,
    request=request,
    budget=budget,
    task_context=task_context,
)
await assert_context_compression_hook_conforms(
    hook=compression_hook,
    request=request,
)
```

Provider 探针验证上下文块硬预算、宿主上下文消息的安全封装，以及 Single-pass/Staged 检索契约。压缩探针使用包含特权指令、完整工具交换和超长历史的压力会话；删除受保护输入、破坏工具协议、改变不可变请求范围或压缩后仍超预算的 Hook 都会被 Core 拒绝。

任务准入与持久执行使用另外两个探针：

```python
from purra.testing import (
    assert_long_task_dispatcher_conforms,
    assert_task_orchestration_conforms,
)

decision = await assert_task_orchestration_conforms(
    evaluator=task_admission,
    request=request,
    plan=plan,
    dispatcher=long_task_dispatcher,
)
await assert_long_task_dispatcher_conforms(
    dispatcher=long_task_dispatcher,
    request=request,
    plan=plan,
    admission=decision,
)
```

探针覆盖全部准入模式、持久任务步骤的精确覆盖、任务交接幂等、更新事件限制，以及从既有 receipt 续跑时不得二次 dispatch。

存储适配器还可以直接运行持久状态探针：

```python
from purra.testing import (
    assert_artifact_store_conforms,
    assert_long_task_repository_conforms,
)

await assert_artifact_store_conforms(
    artifacts=artifact_repository,
    claims=artifact_claim_repository,
    maintenance=artifact_maintenance_repository,
)
await assert_long_task_repository_conforms(long_task_repository)
```

这两组探针检查创建者绑定原子性、续跑关系不可变、Artifact claim 互斥、批次 CAS/幂等、checkpoint 释放、重启恢复，以及已完成单元不可重放，全程不导入产品领域。

Run 控制、委派和工具幂等适配器使用同一个公共模块：

```python
from purra.testing import (
    assert_delegation_repository_conforms,
    assert_execution_lease_store_conforms,
    assert_tool_idempotency_gateway_conforms,
)

await assert_execution_lease_store_conforms(run_control, create_run)
await assert_delegation_repository_conforms(delegations, create_run)
await assert_tool_idempotency_gateway_conforms(tool_idempotency, run_id)
```

模型与工具宿主使用对应的边界探针：

```python
from purra.testing import (
    assert_model_gateway_conforms,
    assert_tool_execution_gateway_conforms,
)

await assert_model_gateway_conforms(
    gateway=model_gateway,
    messages=messages,
    invocation=invocation,
)
await assert_tool_execution_gateway_conforms(
    gateway=tool_gateway,
    request=side_effect_free_read_request,
)
```

模型探针验证流式与非流式结果类型、唯一终止原因、完整 JSON Tool Call 组装及工具声明范围。工具探针验证授权、坏 JSON、未知工具均在执行前失败关闭，预启动取消不会进入处理器，并检查结果顺序和终态事件。传入的合法请求必须是无副作用 READ；探针会实际执行它一次。

Provider 适配器只负责规范化供应商线协议和错误类型。取消仲裁、有限重试策略、输出上限终止，以及完成的 Tool Call 是否允许执行，仍由 Core 统一负责，不下放回各个 Provider SDK 适配器。

### Agent 身份与第二宿主

`tests/test_second_host_conformance.py` 装配了一个事故分诊 Agent，不导入任何产品 Application、领域、数据库或 Provider SDK。它的稳定身份来自有序 `PromptSection`；处置手册来自 `ContextProvider`；服务实时事实来自只读 `ToolCatalog`；模型/工具循环及最终无工具公开回答由 PurrA 负责。

这就是风格边界：身份决定 Agent 如何判断和表达，领域上下文与工具决定它能知道什么、执行什么。PurrA 会在工具轮次间保持可信身份指令，但不会解释或写死事故领域。这里不需要 Persona DSL；类型化 Section 会在 Run 发布前物化成带可信来源的 System Message。

```bash
python -m pytest tests/test_standalone_agent_conformance.py -q
python -m pytest tests/test_second_host_conformance.py -q
```

## 动态规划

初始计划仍是可修订路线，但正常成功执行默认沿用已经编译和授权的计划。Runtime 只会在可恢复失败、协议或授权漂移之后，或者宿主工具的权威结果因选择了分支而显式返回 `ToolPlanningDisposition.REPLAN` 时调用 `DynamicWorkPlanner`。普通的 `PROGRESSED` 与 `COMPLETED` 批次不再额外消耗一次 Planner 模型调用。

工具调用成功不再自动等于 Planner 步骤完成。工具适配器用 `ToolStepDisposition.CONTINUE` 表明“本批已可靠提交，但当前步骤仍需继续”，Core 将其聚合为 `ToolBatchOutcome.PROGRESSED`，保留当前工具授权且不把该步骤写入完成历史；只有适配器返回 `COMPLETE` 才会激活依赖步骤。该语义用于所有有界批次工具，避免一次 append 成功后提前进入 finalize。

Runtime 的轮次约束同样区分“单调的有效分批进展”和“停滞执行”。每个 `PROGRESSED` 轮次可以从 `RuntimeLimits.max_progress_rounds` 解锁一个独立、有上限的进展轮次；普通成功、重试、失败和格式错误都不会获得该额度。这样大型 Artifact 不需要依赖某个领域专属的固定轮次数，同时 Core 仍保留明确的总上限。

已经完成、阻塞或失败的执行步骤属于不可改写的历史。每次修订先生成新的 `WorkPlan`，由 Core 重新校验并编译后，Controller 才会原子替换未来权限并发布投影后的 `run.todos_updated` 事件。Runtime 会为请求范围内的候选工具保留上下文预算，但每一轮只暴露最新 `ExecutionTransition` 授权的工具。因此，重新规划不能绕过工具策略、人工审批、作用域校验或幂等边界。授权拒绝和人工审批拒绝不属于可恢复的工具执行失败。

`ToolPlanningDisposition` 与 `ToolStepDisposition` 相互独立：前者决定未来计划是否已经失效，后者只决定当前步骤是否仍需继续。两者都是宿主工具结果字段，模型不能通过工具参数自行要求重新规划。

## 任务准入与持久化长任务

Planner 仍是唯一的模型语义判断入口。它只把用户目标编译成 `TaskSpec`，不估算调用成本，也不直接决定是否开启长任务。`TaskOrchestrationCapability` 在计划通过约束编译和权限校验后、安装执行计划前调用 `TaskAdmissionEvaluator`；应用与领域实现负责把 `TaskSpec` 对照权威业务状态解析为精确范围，并返回 `inline`、`durable`、`clarify` 或 `reject`。这样无需再增加一次意图模型请求，也不会让 Core 理解任何产品概念。

持久执行准入必须明确声明它将覆盖的全部 Planner 步骤，并携带一份由宿主编译的 `ExecutionRecipe`。每个 Recipe 单元都要映射到一个已准入的 Planner 步骤，多个单元可以共同实现同一步骤；Core 会聚合这些单元的状态，只有全部完成后才把对应 Planner 步骤标记为完成。这样可以防止外部工作流绕过或假完成无关步骤。

`long_tasks` 定义通用的持久化任务、依赖单元、租约、检查点、重试、暂停、恢复和取消契约。Core 的 Coordinator 只认任务 DAG 和执行状态；业务层决定如何拆分、每批输入、领域完整性校验及最终合并。`RecipeLongTaskDispatcher` 把 Recipe 落成唯一的 Long Task，通过宿主管理的执行器注册表运行每个单元，并把已完成依赖的输出引用交给下游执行器。因此静态 Map-Reduce 不需要把产品概念或 Artifact 加载逻辑写进 PurrA。

任务复用同时受 namespace、owner、操作 kind、session 和幂等键约束；同一幂等键如果对应不同 Recipe 会失败关闭。暂停会释放当前租约并从检查点恢复，已经完成的单元不会重放；失败任务只有在宿主明确授权额外尝试次数后才能恢复。多个 Coordinator 遇到同一租约任务时会等待其状态推进，不会把“由另一个 Worker 正在执行”误报成失败。

`OrphanRecoveryCoordinator` 拥有与产品无关的重启恢复边界：它判定失去执行者的 Run，在持久任务租约仍有效时等待，以扫描证据通过 CAS 抢占中断 Run，并按任务 revision 暂停可续跑任务后再结算。宿主只注入一个 `OrphanRunSettlement` 回调，原子保存自己的生命周期事件与输出投影；PurrA 不查询产品表，也不虚构产品终态记录。

`RunRepository` 只能以完整 `ExecutionPlan` 原子替换计划，宿主不能只保存步骤数组而丢失 `work_step_ids` 或私有协议节点。持久任务续跑的最新计划只能来自类型化的 `RunRecoverySnapshot`，准入契约只能来自原始 `LongTaskDispatchReceipt`；应用可以认证并选择恢复快照，但不能在组装 Run Options 时重新编写这两份权威状态。续跑还必须复用源 Run 的 AgentPreset 快照。

## 上下文编排与压缩

PurrA 只负责上下文预算、压缩时机、Hook 调用和技术校验。Core 根据模型窗口、输出与运行时预留、工具 Schema 和 Domain 上下文声明生成统一预算，并在规划边界以及每次模型调用前重新计算压力。Core 每次都会把投影视图交给应用注入的 `ContextCompressionHook`；请求中的 `compression_required` 表示压力是否达到默认 85% 阈值或已经超过消息预算。低于阈值时 Hook 可以复用已有摘要，但 Core 不会宣告一次可见压缩。Hook 获得完整源视图和硬 Token 限制，所有语义选择均由应用负责。

分阶段上下文 Provider 还可以实现 `TaskContextDemandProvider`。规划前的普通需求只为轻量候选清单分配空间；Core 编译出 `TaskSpec` 后，再解析仅属于该任务的附加需求，并在正式检索正文前重新生成最终预算。因此，无关的新问题不会因为存在未完成大产物就被预占一块上下文，领域也不需要写死局部上下文窗口。

`context_orchestration` 不再拥有摘要 Schema、摘要持久化、保留回合数、语义目标或摘要失败降级。没有配置 Hook 时，Core 才启用基础结构裁剪：以最近 20 条消息为起点，再按实际 Token 预算收缩，并保持完整工具调用协议；配置 Hook 后这套默认裁剪不会参与。Core 只校验特权指令、当前用户请求、Tool Call/Tool Result 连续性和最终 Token。语义摘要、摘要状态、业务保留规则及降级链全部由 Application 组装，Domain 提供业务语义规则。

持久化和前端展示始终以原始会话回合作为唯一事实源；压缩结果只是当前模型请求的运行时投影视图，不会反写原始会话。宿主可以额外保存带覆盖范围和摘要指纹的派生摘要，通过 Application 自己的 Repository 与 Summarizer 端口注入实现，并限制单次语义压缩的推进轮数。若依赖故障导致结果仍超过硬预算，也由 Application 明确选择最近消息应急投影，不会让 Core 在 Hook 后再偷偷追加默认裁剪。

## 助手输出可见性协议

Core 将模型流分成彼此不可替代的四类语义：

- `model.reasoning_delta` 是供应商返回的原始推理，只用于 DEV 诊断，不进入用户对话，也不作为业务进度来源；
- `model.content_delta` 是宿主需要检查的原始模型内容，例如尚未验证的结构化输出，同样只属于诊断通道；
- `assistant.commentary_delta` 是可以展示给用户的执行说明，位于工具调用、计划状态和领域进度组成的工作日志中；
- `assistant.final_delta` 是最终自然语言回答，独立于执行过程和产物正文。

Runtime 只有在模型本轮确实发起工具调用时，才把同轮经过完整聚合的普通文本提升为公开 commentary；原始 reasoning 永远不会被提升。没有工具的直答进入 final。产品宿主生成的确定性进度也必须显式发出 commentary，而不能借用 reasoning 或让前端从 JSON、正文片段中猜测。工具与计划事件保持结构化，Artifact 正文通过独立效果或详情接口交付，不复制进对话。

传输层可以把这四类事件映射为不同的 SSE 字段，诊断面板可以订阅原始通道，但普通对话 reducer 只能消费 commentary、final、工具、计划和生命周期事件。`assistant.commentary_delta` 属于可恢复的 Run 历史；三个逐 Token 原始/最终文本通道不作为 Run 事件回放源，最终回答由 Run 终态单独持久化。

## 模型输出与工具数据边界

`model_protocol` 统一解释不同供应商的结束原因。只要供应商声明达到长度上限，本轮输出就属于不完整结果：残缺的文本不会被当作最终回答，残缺的工具调用不会执行，也不会写入后续模型历史。截断请求不会用同一额度重新执行，并始终保留 `tool_call_truncated` 或 `model_output_truncated` 根因和安全诊断。

输出额度分成三个独立事实源：Infrastructure 模型 Profile 只声明供应商能力上限，Application 策略估算并限制单个业务工作单元，`purra.output_budget` 再结合上下文窗口解析本次实际额度。只有这个解析结果可以进入供应商 `max_tokens`。Run 事件会持久化任务策略、模型能力上限、限制来源、实际用量和结束原因。分片任务必须增加或拆分执行单元，不能通过提高全局模型默认值解决。

每个已审计工具通过 `ToolDataContract` 声明模型生成字段、宿主绑定字段和宿主派生字段。宿主字段不得出现在模型可见 JSON Schema 中。长内容工具应优先使用 `delta`、`batch` 或 `resource_reference`，模型只生成新的语义增量，标识、版本、谱系、累计正文和完成状态由宿主绑定或计算。

`ToolSchema.name` 始终是不可翻译的协议标识；面向用户的多语言名称独立保存在宿主管理的 `display_names` 中，语言键使用 `zh-CN`、`en-US` 等标签。Core 根据本次请求语言把展示名用于 Planner 标题和模型叙述提示，但发送给供应商的函数调用仍只有真实名称、描述与参数。运行事件会携带完整展示名映射，前端可以确定性选择语言，不需要让模型改写函数名。

`ToolExecutionLimits.max_argument_chars` 现在只表示可配置的原始 JSON 传输安全包络，不是上下文分配，也不是领域数据预算。JSON 解码及受限的结构恢复完成后，Core 会再次强制执行注册 Schema 的必填字段、类型、枚举、文本长度、数组数量、数值范围和额外字段规则。因此，空白和 Unicode 转义不再消耗一个无关的 32K 工作流预算；领域仍通过版本化 Schema 决定有用语义数据的大小。校验失败会返回工具名、失败阶段、Schema 路径、实际测量值和允许上限，但不会回显被拒绝的正文。

Core 只准入以下递归 JSON Schema 子集：`type`（`object`、`array`、`string`、`integer`、`number`、`boolean`、`null` 或这些名称组成的非空列表）、`properties`、`required`、布尔值 `additionalProperties`、`items`、`anyOf`、`oneOf`、`enum`、`const`、`minLength`、`maxLength`、`minItems`、`maxItems`、`minimum`、`maximum`，以及字符串注解 `title`、`description`。结构错误或其他断言关键字会在 Tool Catalog 装配时被拒绝；运行时仍以失败关闭方式做纵深校验，整批拒绝时不会启动任何处理器。

## 受控恢复策略

`recovery` 是 Runtime 内所有自动恢复的统一决策层。供应商兼容降级、流中断、截断、缺失或越权工具调用、空回答、回答修复及工具输入修正不再各自维护布尔开关；每次候选动作都必须经过同一个 Run 级恢复账本，检查取消状态、剩余模型轮次、按根因配置的尝试额度、正文是否已经对用户可见，以及工具副作用是否可能已经开始。

领域适配器可以在 Composition Root 注入 `RecoveryPolicy`，但只有 Core 可以消费额度和批准恢复。JSON、Schema 或工具调用包络在整批预检阶段失败时，Core 可以要求模型修正一次；工具处理器尚未启动这一事实由 `ToolBatchResult.effect_state` 显式证明。写工具处理器一旦启动而提交状态未知，Core 会拒绝自动重放或失败后的自动重规划。所有允许和拒绝决定都通过现有 Run Trace 持久化，`observability.recovery` 只投影根因、动作、额度和安全拒绝码，不复制模型正文或工具参数。

## 稳定性评估

`observability.stability` 从持久化 Core 事件中生成不包含业务正文的可靠性指标。它关联工具发起、完成和结果事件，明确区分显式失败与“已经发起但始终没有终态结果”的未收口调用；同一份报告还聚合协议错误码、模型中断与重试、上下文溢出、压缩降级和压缩失败。`observability.recovery` 另外解释每次恢复为何被允许或拒绝。基础设施层可以补充 Artifact 数量与完成度，但不得把提示词、工具参数或生成正文复制进稳定性报告。

`StabilityTrendPolicy` 对一个有界、按新到旧排列的 Run 窗口应用由调用方注入的预警和失败阈值。比例指标达到可配置的最小样本量后才触发告警，但连续失败属于绝对信号，不会被样本门槛隐藏。SQLite 适配器只投影 Trace 计数、调用 ID、工具名和错误码；历史提示词、参数及工具结果正文不会进入趋势评估器。

用户主动取消的 Run 不进入趋势窗口，避免一次正常中止被误判为工具未收口回归，也不会稀释真实失败率。

`failure_classification` 把明确且不包含正文的运行证据转换为稳定根因码，同时不会给证据不足的事件强行编造原因。工具协议错误、工具生命周期未收口、上下文故障、规划契约错误、工具处理器失败和模型中断分别保留独立分类及检查建议；如果 Run 已失败但没有具体证据，只会标记为低置信度的可观测性缺口。

`stability_gate` 使用最新 Run 窗口与紧邻的上一窗口比较，识别失败率上升、连续失败扩大及新出现的工具错误码。宿主可在自己的运行时回归框架中维护事故目录并锁定预期根因码。

## 可恢复 Artifact 生命周期

`artifacts` 提供与领域无关的 `open → finalized/aborted` 状态机。大结果可以按有序批次提交；每批携带幂等键、内容摘要、期望 revision、sequence 和 coverage key。Core 在最终确认前校验数量、连续序号、重复/缺失覆盖项，并通过 `ArtifactValidator` 把领域正确性留给外层。具体数据库事务由 `ArtifactRepository` 实现；SQLite 适配器保证批次写入、CAS revision 与返回 receipt 处于同一个可判定提交边界。

只有同时声明为 `batch`、`PROPOSE`、取消线性化并由宿主管理持久化的同名 Artifact 工具，才允许在一个模型轮次内提交多个调用。Core 仍会在任何写入前预检整批 JSON、调用数量、授权和 Schema；普通写工具继续禁止多调用。这让大结果获得吞吐量，同时不放宽一般副作用工具的安全边界。模型轮次上限由领域适配器通过 `RuntimeLimits` 注入，Core 默认值不再承担具体产品的批次数量假设。

Planner 契约与运行时工具契约已经分离。工具注册可以用稳定的业务级 `planning_capability` 映射一个或多个私有运行时工具；Core 先校验公共计划，再按依赖关系确定性下沉为执行协议。私有步骤仍会持久化并接受完整授权校验，但不会出现在公共任务计划 SSE 中，用户只看到业务能力。经过宿主认证的续写状态可以把部分私有工具标记为已完成。直接指定私有运行时工具的 WorkPlan 会被拒绝；没有声明 planning capability 的公共运行时工具仍可直接选择。

产品可以把一个公开能力下沉为私有的开始、分批追加和最终提交工具；但 Artifact 类型、业务操作、来源凭证和领域校验规则必须写在产品文档中，而不进入框架文档。

## 持久任务与 Artifact 所有权

`long_tasks` 是唯一的持久任务聚合，统一持有任务/单元生命周期、恢复状态、用量和追加式 Run 绑定（`created`、`continuation`、`reference`）。Repository 创建任务时必须原子写入任务、单元与创建者绑定。PurrA 不再维护一套平行的 Work Item 状态机。

Artifact 是可恢复输出，不是 Runtime checkpoint，也不是任务记录。`ArtifactOwnerRef(kind, id)` 是宿主定义的不透明身份；PurrA 不查询业务表，也不解释 owner kind。创建 Run 根据来源关系拥有访问权；其他 Run 默认拒绝，只有注入的 `ArtifactAccessAuthorizer` 可以授权。包括创建 Run 在内的每个写入者，都必须在生命周期和 revision 校验后取得原子、独占、可过期的 claim。finalize 只改变 Artifact 自身状态，不再顺带完成其他聚合。

Artifact 维护契约与存储无关：过期或失效 claim 可以回收，open Artifact 内容不可回收。终态保留期必须显式开启，并受 `max_purge_artifacts` 限制；适配器只报告不含正文的状态和 claim 计数。调度、持久化实现及宿主/领域授权都留在 PurrA 之外。

由 Run 和事件证据派生的运行报告位于 `observability`；确定性的回归用例和安全红队用例位于 `evaluation`。两者都不参与 Runtime 状态迁移或 checkpoint 恢复。

## 可复用持久化端口

- `RunRepository`：保证 Run 生命周期状态与 Outbox 事件的原子持久化。
- `ExecutionLeaseStore`：管理执行所有权、心跳续租和持久化取消请求。
- `DelegationRepository`：管理 Root Run 内的委派批次、生命周期和结果。
- `RunRecoveryStore`：提供类型化、基于游标的 `RunRecoverySnapshot`。
- `ApprovalGateway`：管理只能决议一次的人工审批。
- `ToolIdempotencyGateway`：保证具有副作用的工具调用可以安全重放。
- `ContextCompressionHook`：由 Application 实现压缩策略；Core 只负责调用并校验返回结果。
- `ArtifactRepository`：保存可恢复的大结果批次，并保证 revision、顺序和幂等回执原子提交。
- `ArtifactValidator`：由领域注入批次和最终完整性规则，不把业务结构写进 Core。
- `LongTaskRepository`：保存持久任务/单元生命周期及追加式 Run 关系。
- `ArtifactAccessAuthorizer`：提供跨 Run Artifact 访问的宿主策略。
- `ArtifactClaimRepository`：为 Artifact 提供独占、可过期的写入权。
- `ArtifactMaintenanceRepository`：原子回收无效租约、输出不含正文的一致性报告，并执行显式配置的终态保留策略。

具体适配器由宿主的 Composition Root 统一装配。

## 单 Run 多 Agent 委派

只有 `AgentPreset` 显式选择 `DelegationPolicy` 且宿主提供所需委派基础设施时，PurrA 才会把 `delegateToAgents` 暴露为普通模型工具；`None` 表示禁用。完整策略与实际委派工具 Schema 会进入 version 2 快照，Repository 和 Executor 仍是不会被序列化的 Kernel 基础设施。父模型在每次调用中通过 `agentName`、`title`、`instruction`、`objective` 和可选输入定义任务专用 Agent。创建、调用、取消、结果收集和聚合都在这一次工具生命周期内完成。每次调用只在同一个 Root Run 内创建委派批次，不会创建第二个 Run、Run 树、执行租约或独立事件流。

`DelegationCoordinator` 负责有界并发、取消、持久化和生命周期事件；`DynamicDelegatedAgentExecutor` 在进程内执行模型定义的 Agent。模型只拥有 Agent 的语义定义，权限由 PurrA 固定：子 Agent 使用隔离会话，继承 Root Run 有界的 Domain Context 和 ContextProvider，只获得父 Agent 当前启用的只读工具；它看不到父 Agent 身份和私有对话，不能获得写工具，也不能递归调用 `delegateToAgents`。宿主仍可替换执行器端口，但 Core 始终只暴露一个 Root Run 身份。结果只在原始 `batch_id` 内聚合，所有事件仍归属于 Root Run。

委派必须同时配置 `DelegationRepository` 和 `ToolIdempotencyGateway`：同一个父工具调用重试时复用已保存结果，不会重复创建批次；子 Agent 内部工具调用键再按 `delegation_id` 隔离，避免共享 Root Run 内发生碰撞。活动模型流仍是进程内执行；真正需要独立持久租约的工作进入 durable task 编排，而不是重新引入隐藏的 Child Run。PurrA 和业务宿主都不再注册固定子 Agent 角色目录；Agent 语义由父模型在工具边界给出，权限始终是 PurrA 的运行时契约。

## 添加新的适配器

新的持久化适配器必须使用 `purra.testing` 中与数据库无关的行为合规断言。这套断言会验证：

- 执行所有权和单一租约持有者
- 持久化取消及租约释放
- 委派并发额度、批次隔离和取消
- Checkpoint 事件游标和分页恢复
- 工具调用的幂等重放

因此，未来增加 PostgreSQL、内存存储或其他产品专用实现时，可以复用同一套断言验证与 Core 的兼容性。

## 产品扩展边界

以下内容必须保留在 Core 之外，由各产品通过端口、策略或注册表注入：

- Root Agent 显式选择的 `PromptSection`
- 领域上下文提供器
- 工具目录和工具处理器
- 规划策略及响应验证策略
- 模型供应商适配器
- HTTP、SSE、WebSocket 或桌面端事件映射
- SQLite、PostgreSQL 等具体持久化实现

Core 只认识通用的 `agentName`、`title`、`instruction`、`objective`、`input`、`result`、`priority` 和 `required` 等编排概念。`DelegationPolicy` 统一限制数量、并发和字段大小；隔离上下文、只读工具与禁止递归是 Core 固定的权限边界。PurrA 和宿主都不提供默认子 Agent 风格或业务角色目录。

## 第二产品接入原则

验证 Core 通用性的标准是：第二款产品只新增自己的领域适配器和基础设施适配器，不修改 PurrA 的执行语义。

如果新产品必须在 Core 中增加产品名称判断或领域字段，说明抽象边界仍需调整；应优先扩展通用端口或策略，而不是在 Core 中加入产品分支。
