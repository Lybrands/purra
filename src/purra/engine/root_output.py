"""Keep Root final output behind tree and delivery barriers."""
import asyncio
from purra.output.contracts import AgentOutputIntent


class RootOutputObserver:
    def __init__(self, output, require_quiescent, require_delivery_settled, public_locks=None):
        self._output = output
        self._require_quiescent = require_quiescent
        self._require_delivery_settled = require_delivery_settled
        self._final_runs = {}
        self._public_locks = public_locks if public_locks is not None else {}
        self._held_locks = {}
        self._stream_runs = {}

    async def open_model_stream(self, receipt, spec):
        if spec.intent is AgentOutputIntent.FINAL_PUBLIC:
            await self._require_quiescent(spec.run_id)
            await self._require_delivery_settled(spec.run_id)
        lock = None
        if spec.intent in {AgentOutputIntent.FINAL_PUBLIC, AgentOutputIntent.EXECUTION_PUBLIC}:
            lock = self._public_locks.setdefault(spec.run_id, asyncio.Lock())
            await lock.acquire()
        try:
            result = await self._output.open_model_stream(receipt, spec)
        except BaseException:
            if lock is not None:
                lock.release()
            raise
        self._stream_runs[spec.output_stream_id] = spec.run_id
        if lock is not None:
            self._held_locks[spec.output_stream_id] = lock
        if spec.intent is AgentOutputIntent.FINAL_PUBLIC:
            self._final_runs[spec.output_stream_id] = spec.run_id
        return result

    async def accept_provider_chunk(self, output_stream_id, chunk):
        run_id = self._final_runs.get(output_stream_id)
        if run_id is not None:
            await self._require_quiescent(run_id)
        return await self._output.accept_provider_chunk(output_stream_id, chunk)

    async def finish_model_stream(self, output_stream_id, finish_reason):
        run_id = self._final_runs.get(output_stream_id)
        try:
            if run_id is not None:
                await self._require_quiescent(run_id)
            return await self._output.finish_model_stream(output_stream_id, finish_reason)
        finally:
            self._final_runs.pop(output_stream_id, None)
            self._release(output_stream_id)

    async def abort_model_stream(self, output_stream_id, error_code):
        try:
            return await self._output.abort_model_stream(output_stream_id, error_code)
        finally:
            self._final_runs.pop(output_stream_id, None)
            self._release(output_stream_id)

    def _release(self, output_stream_id):
        lock = self._held_locks.pop(output_stream_id, None)
        if lock is not None:
            lock.release()

    async def publish_model_stream_commentary(self, output_stream_id):
        async with self._public_locks.setdefault(self._stream_runs[output_stream_id], asyncio.Lock()):
            return await self._output.publish_model_stream_commentary(output_stream_id)

    async def publish_model_stream_final(self, output_stream_id):
        run_id = self._stream_runs[output_stream_id]
        await self._require_quiescent(run_id)
        await self._require_delivery_settled(run_id)
        async with self._public_locks.setdefault(run_id, asyncio.Lock()):
            return await self._output.publish_model_stream_final(output_stream_id)
