"""Dispatch terminal Agent results independently of serialized presentation."""
import asyncio


async def deliver_agent_results(run, deliver):
    """Schedule each arrival promptly; the owner serializes its public lane."""
    seen, deliveries = set(), []
    failure = asyncio.get_running_loop().create_future()

    async def report(result):
        try:
            await deliver((result,))
        except BaseException as error:
            if not failure.done():
                failure.set_result(error)
            raise

    async def notify(aggregate):
        for result in aggregate.results:
            if result["runId"] not in seen:
                seen.add(result["runId"])
                deliveries.append(asyncio.create_task(report(result)))

    producer = asyncio.create_task(run(notify))
    try:
        done, _ = await asyncio.wait((producer, failure), return_when=asyncio.FIRST_COMPLETED)
        if failure in done:
            raise failure.result()
        result = await producer
        await asyncio.gather(*deliveries)
        return result
    finally:
        for task in (producer, *deliveries):
            if not task.done():
                task.cancel()
        await asyncio.gather(producer, *deliveries, return_exceptions=True)
        failure.cancel()
