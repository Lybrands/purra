# purra-compaction · Python

English | [简体中文](README.zh-CN.md)

Compress older conversation turns with a model call when PurrA's context budget
requires it. Instructions and recent complete turns, including tool results,
remain in the input.

## Install

From the repository root, with Python 3.11+:

```sh
python -m pip install . ./integrations/compaction/python
```

## Configure

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

Attach the factory to your existing Agent preset. Core supplies the Run-bound
model task runner:

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

Pass this preset to `AgentCore`. Keep the component binding stable during recovery;
change its revision when the compression configuration changes.

| Option | Default | Purpose |
| --- | --- | --- |
| `max_summary_tokens` | 1024 | Output limit for the summary call |
| `max_input_tokens` | 16000 | Maximum estimated input tokens for the summarizer |
| `keep_recent_messages` | 8 | Recent-message retention target, adjusted to complete turns |

## Behavior

Each compression uses one managed model call and consumes the Run's budget.
The summary retains goals, constraints, decisions, completed work, open questions,
and evidence references. Private reasoning and provider continuation data are
excluded from the summarizer input.

The summary is untrusted historical context. Invalid or oversized summaries fail
without replacing the input. Set the input limit within the summarizer model's
available context window. The application retains the original conversation.
