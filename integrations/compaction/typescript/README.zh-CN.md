# purra-compaction · TypeScript

[English](README.md) | 简体中文

在上下文预算不足时，使用 PurrA 的受控模型任务执行器压缩较早的对话轮次。
要求 Node.js 22+ 和相同版本的 `purra`。

## 安装

按照[源码安装说明](../../README.zh-CN.md#从源码安装)操作，选择
`integrations/compaction/typescript`。

## 配置

```ts
import { Agent, type ModelGateway } from "purra";
import { SemanticCompaction } from "purra-compaction";

function createAgent(model: ModelGateway) {
  return new Agent({
    model,
    context: {
      compressionFactory: tasks => new SemanticCompaction(tasks, {
        maxSummaryTokens: 1024,
        maxInputTokens: 16000,
        keepRecentMessages: 8,
      }),
    },
  });
}
```

| 参数 | 默认值 | 用途 |
| --- | --- | --- |
| `maxSummaryTokens` | 1024 | 摘要调用的输出上限 |
| `maxInputTokens` | 16000 | 摘要模型的估算输入 Token 上限 |
| `keepRecentMessages` | 8 | 近期消息保留目标，会按完整轮次调整 |

## 行为

每次压缩使用一次受控调用，消耗当前 Run 的预算。指令和近期完整轮次会保留，
较早内容被总结为目标、约束、决策、已完成工作、待解决问题和证据引用。

摘要是不可信的上下文块。私有推理和厂商续接数据不参与摘要。
无效或超限摘要会导致压缩失败，不替换输入。Core 将接受的摘要保存在已准备的上下文检查点中，
原始对话由应用保存。
