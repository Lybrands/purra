# purra-compaction · Python

[English](README.md) | 简体中文

当 PurrA 的上下文预算不足时，通过模型调用压缩较早的对话轮次。
指令和近期完整轮次会保留在输入中，包括对应的工具结果。

## 安装

使用 Python 3.11+，在仓库根目录执行：

```sh
python -m pip install . ./integrations/compaction/python
```

## 配置

```python
from purra.context_orchestration import ContextCompressionCoordinator
from purra_compaction import SemanticCompaction

def compactor(model_tasks):
    return ContextCompressionCoordinator(SemanticCompaction(
        model_tasks,
        max_summary_tokens=1024,
        max_input_tokens=16000,
        keep_recent_messages=8,
    ))
```

将工厂配置到现有 Agent preset 上。Core 会提供绑定到当前 Run 的模型任务执行器：

```python
from dataclasses import replace
from purra.api import AgentComponentBinding

preset = replace(
    preset,
    conversation_compactor_factory=compactor,
    component_bindings={
        **preset.component_bindings,
        "conversationCompactor": AgentComponentBinding("semantic-compaction", "1"),
    },
)
```

将此 preset 传给 `AgentCore`。恢复时保持组件绑定不变，调整压缩配置时更新其修订号。

| 参数 | 默认值 | 用途 |
| --- | --- | --- |
| `max_summary_tokens` | 1024 | 摘要调用的输出上限 |
| `max_input_tokens` | 16000 | 摘要模型的估算输入 Token 上限 |
| `keep_recent_messages` | 8 | 近期消息保留目标，会按完整轮次调整 |

## 行为

每次压缩使用一次受控模型调用，消耗当前 Run 的预算。摘要保留目标、约束、决策、
已完成工作、待解决问题和证据引用。私有推理和厂商续接数据不进入摘要模型的输入。

摘要作为不可信的历史上下文处理。无效或超限摘要会导致压缩失败，不替换输入。
输入上限应在摘要模型可用的上下文窗口内；原始对话由应用保存。
