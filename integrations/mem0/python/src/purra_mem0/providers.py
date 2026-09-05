"""Optional, bounded SDK providers. No SDK import at module load."""

from __future__ import annotations

import asyncio
import json
import math
import threading
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import asdict, dataclass, replace

from purra.contracts import AgentMessage, ModelCompletion, ModelRequest
from purra.model_execution import AgentModelTask, AgentModelTaskRunner
from purra.ports import CancellationSignal

from ._journal import Journal, MemoryError


@dataclass(frozen=True, slots=True)
class MemoryBudget:
    """Durable per-namespace envelope; reservations are never refunded."""

    key: str
    max_llm_calls: int
    max_embedding_calls: int
    max_input_chars: int
    max_output_tokens: int
    result_capacity_target_tokens: int

    def __post_init__(self):
        if not isinstance(self.key, str) or not self.key.strip() or len(self.key) > 512:
            raise ValueError("invalid budget key")
        for name, value in self.limits.items():
            minimum = 1 if name == "result_capacity_target_tokens" else 0
            if type(value) is not int or not minimum <= value <= 2**31 - 1:
                raise ValueError(f"invalid {name}")

    @property
    def limits(self):
        return {k: v for k, v in asdict(self).items() if k != "key"}


@dataclass(frozen=True, slots=True)
class MemoryUsage:
    llm_calls: int
    embedding_calls: int
    input_chars: int
    reserved_output_tokens: int
    reported_input_tokens: int
    reported_output_tokens: int
    unreported_calls: int
    unsettled_calls: int


@dataclass(frozen=True, slots=True)
class EmbeddingResult:
    vectors: Sequence[Sequence[float]]
    input_tokens: int | None = None


@dataclass(frozen=True, slots=True)
class MemoryProviders:
    """Trusted host callbacks with a result-capacity target per call.

    ``complete`` receives messages, the result-capacity target and a signal.
    ``embed`` receives texts and the signal. Both callbacks run on the caller's
    event loop, even though Mem0 itself runs in a worker thread.
    """

    budget: MemoryBudget
    complete: Callable[[Sequence[AgentMessage], int, CancellationSignal], Awaitable[ModelCompletion]]
    embed: Callable[[Sequence[str], CancellationSignal], Awaitable[EmbeddingResult]]

    def __post_init__(self):
        if not isinstance(self.budget, MemoryBudget) or not callable(self.complete) or not callable(self.embed):
            raise TypeError("invalid memory providers")


def run_model(runner: AgentModelTaskRunner, request: ModelRequest):
    """Use the real Run-injected runner, including its authority and accounting.

    Background ingestion should supply its own bounded callback, not a fake Run.
    """
    if not isinstance(runner, AgentModelTaskRunner) or not isinstance(request, ModelRequest):
        raise TypeError("run_model requires a PurrA runner and request")

    async def complete(messages, result_capacity_target_tokens, signal):
        task = AgentModelTask(
            request=request,
            result_capacity_target_tokens=result_capacity_target_tokens,
        )
        return (await runner.complete(messages, task, signal)).completion

    return complete


_current: ContextVar[ProviderExecution | None] = ContextVar("purra_mem0_execution", default=None)


class ProviderExecution:
    def __init__(self, providers, journal: Journal, dimensions, timeout, max_results, max_input):
        self.providers, self.journal, self.dimensions = providers, journal, dimensions
        self.loop = asyncio.get_running_loop()
        self.signal = asyncio.Event()
        self.deadline = time.monotonic() + timeout
        self.max_results, self.max_input = max_results, max_input
        self.operation = None
        self.error = None
        self._lock = threading.Lock()

    def stop(self, code):
        with self._lock:
            if self.error is None:
                self.error = code
                if self.operation is not None:
                    self.journal.provider_error(self.operation, code)
        self.loop.call_soon_threadsafe(self.signal.set)

    def check(self):
        if time.monotonic() >= self.deadline:
            self.stop("memory_timeout")
        if self.error is not None:
            raise MemoryError(self.error)

    def run(self, work):
        token = _current.set(self)
        try:
            self.check()
            result = work()
            self.check()  # Mem0 can swallow provider exceptions in fallback paths.
            return result
        except Exception:
            if self.error is not None:
                raise MemoryError(self.error) from None
            raise
        finally:
            _current.reset(token)

    def invoke(self, kind, values, *, extraction=True):
        self.check()
        cap = (
            self.providers.budget.result_capacity_target_tokens
            if kind == "llm"
            else 0
        )
        chars = sum(len(v.content) for v in values) if kind == "llm" else sum(map(len, values))
        try:
            call_id = self.journal.admit(self.providers.budget.key, self.operation, kind, chars, cap)
        except Exception as error:
            self.stop(error.code if isinstance(error, MemoryError) else "memory_provider_error")
            raise MemoryError(self.error) from None
        input_tokens = generation_tokens = None
        try:
            self.check()
            async def dispatch():
                self.check()
                if kind == "llm":
                    return await self.providers.complete(values, cap, self.signal)
                return await self.providers.embed(values, self.signal)
            result = asyncio.run_coroutine_threadsafe(dispatch(), self.loop).result()
            if kind == "llm":
                if not isinstance(result, ModelCompletion):
                    raise MemoryError("memory_provider_contract")
                if result.usage is not None:
                    input_tokens, generation_tokens = result.usage.input_tokens, result.usage.generation_tokens
                if (result.applied_generation_limit is None or result.finish_reason != "stop"
                        or result.message.tool_calls or result.message.role != "assistant"
                        or result.applied_generation_limit < cap):
                    raise MemoryError("memory_provider_contract")
                content = result.message.content
                if extraction:
                    try:
                        parsed = json.loads(content)
                    except (TypeError, ValueError):
                        raise MemoryError("memory_invalid_extraction")
                    if (not isinstance(parsed, dict) or not isinstance(parsed.get("memory"), list)
                            or len(parsed["memory"]) > self.max_results):
                        raise MemoryError("memory_invalid_extraction")
                    for item in parsed["memory"]:
                        if (not isinstance(item, dict) or not isinstance(item.get("text"), str)
                                or not item["text"].strip() or len(item["text"]) > self.max_input
                                or not isinstance(item.get("entities", []), list)
                                or any(not isinstance(e, str) for e in item.get("entities", []))):
                            raise MemoryError("memory_invalid_extraction")
            else:
                if not isinstance(result, EmbeddingResult):
                    raise MemoryError("memory_provider_contract")
                input_tokens, generation_tokens = result.input_tokens, 0
                if input_tokens is not None and (type(input_tokens) is not int or not 0 <= input_tokens <= 2**31 - 1):
                    input_tokens = None
                    raise MemoryError("memory_provider_contract")
                vectors = [list(v) for v in result.vectors]
                if len(vectors) != len(values) or any(len(v) != self.dimensions or any(
                    isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x) for x in v
                ) for v in vectors):
                    raise MemoryError("memory_provider_contract")
                content = vectors
            self.check()
            self.journal.settle(call_id, "complete", input_tokens, generation_tokens)
            return content
        except Exception as error:
            self.journal.settle(call_id, "failed", input_tokens, generation_tokens)
            code = error.code if isinstance(error, MemoryError) else "memory_provider_error"
            self.stop(code)
            raise MemoryError(self.error) from None


def current_execution(journal=None):
    value = _current.get()
    return value if journal is None or value is not None and value.journal is journal else None


def _execution():
    execution = current_execution()
    if execution is None:
        raise MemoryError("memory_provider_unbound")
    return execution


class ManagedMem0Client:
    """SDK handle paired with guarded provider entry points; host owns ``sdk``."""

    def __init__(self, sdk, dimensions):
        self.sdk, self.dimensions = sdk, dimensions

    def __getattr__(self, name):
        return getattr(self.sdk, name)


def create_managed_client(*, config: Mapping, embedding_dims: int):
    """Construct the package-private Mem0 extension with native PurrA providers.

    Only storage/history/custom extraction instructions are accepted. Rerankers,
    graph memory and alternate provider configuration cannot bypass admission.
    Set MEM0_TELEMETRY=false and paths before calling, as for the raw SDK.
    """
    if type(embedding_dims) is not int or not 1 <= embedding_dims <= 65_536:
        raise ValueError("invalid embedding_dims")
    if not isinstance(config, Mapping) or set(config) - {"vector_store", "history_db_path", "custom_instructions"}:
        raise ValueError("managed config accepts only vector_store, history_db_path, custom_instructions")
    if not config.get("vector_store") or not config.get("history_db_path"):
        raise ValueError("explicit vector_store and history_db_path are required")
    vector = config["vector_store"]
    if not isinstance(vector, Mapping) or str(vector.get("provider", "")).lower() == "langchain" or not isinstance(vector.get("config"), Mapping):
        raise ValueError("explicit native vector_store configuration is required")
    values = dict(vector["config"])
    for name in ("embedding_model_dims", "dimension"):
        if name in values and (type(values[name]) is not int or values[name] != embedding_dims):
            raise ValueError("vector store dimension must match embedding_dims")
    if vector.get("provider") == "qdrant":
        values["embedding_model_dims"] = embedding_dims
    from importlib.metadata import version
    if version("mem0ai") != "2.0.19":
        raise RuntimeError("purra-mem0 requires mem0ai 2.0.19")
    from mem0.configs.base import MemoryConfig
    from ._vendor.memory import Memory
    from .direct_providers import DirectLlm, DirectEmbedder
    settings = MemoryConfig(**{**config, "vector_store": {**vector, "config": values}})
    sdk = Memory(settings, llm=DirectLlm(), embedder=DirectEmbedder())
    return ManagedMem0Client(sdk, embedding_dims)
