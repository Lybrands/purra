"""Mem0's native text/embedding protocol backed by the current PurrA operation."""
from collections.abc import Mapping, Sequence

from purra.contracts import AgentMessage
from ._journal import MemoryError
from .providers import _execution


def _reject(execution):
    execution.stop("memory_provider_contract")
    raise MemoryError(execution.error)


class DirectLlm:
    def generate_response(self, messages, tools=None, tool_choice="auto", **kwargs):
        execution = _execution()
        execution.check()
        if (tools not in (None, []) or tool_choice != "auto"
                or set(kwargs) - {"response_format"}
                or kwargs.get("response_format") not in (None, {"type": "json_object"})
                or not isinstance(messages, Sequence) or isinstance(messages, (str, bytes))
                or not messages):
            _reject(execution)
        converted = []
        for message in messages:
            if (not isinstance(message, Mapping) or set(message) != {"role", "content"}
                    or message["role"] not in ("system", "user", "assistant")
                    or not isinstance(message["content"], str)):
                _reject(execution)
            converted.append(AgentMessage(role=message["role"], content=message["content"]))
        return execution.invoke("llm", tuple(converted))


class DirectEmbedder:
    def embed(self, text, memory_action=None):
        return self.embed_batch([text], memory_action)[0]

    def embed_batch(self, texts, memory_action="add"):
        execution = _execution()
        execution.check()
        if (memory_action not in (None, "add", "search", "update")
                or not isinstance(texts, Sequence) or isinstance(texts, (str, bytes))
                or any(not isinstance(text, str) for text in texts)):
            _reject(execution)
        return execution.invoke("embedding", tuple(texts)) if texts else []
