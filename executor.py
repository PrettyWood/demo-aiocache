import asyncio
import json

import aioredis
from aiocache import Cache

from app import channel_req, channel_resp

cache = Cache(Cache.REDIS, ttl=100)


async def get_random_number(x: int) -> int:
    import random

    print("Generating number...")
    n = random.randint(0, 1_000_000)
    await asyncio.sleep(5)
    print("Generating number...done")
    return n * int(x)


async def _get_random_number_from_cache_or_compute(_id: int, pub):
    print('Got Message:', _id)
    if (result_cached := await cache.get(f'key_{_id}')) is not None:
        await pub.publish(channel_resp, message=json.dumps({"result": result_cached,"cache": True, '_id': _id}))
    else:
        result = await get_random_number(_id)
        await cache.set(f'key_{_id}', result)
        await pub.publish(channel_resp, message=json.dumps({"result": result,"cache": False,  '_id': _id}))


async def reader(ch, redis):
    while await ch.wait_message():
        msg = await ch.get()
        asyncio.ensure_future(_get_random_number_from_cache_or_compute(int(msg.decode()), redis))
        await asyncio.sleep(0.1)


async def main():
    sub = await aioredis.create_redis(address="redis://localhost:6379")
    pub = await aioredis.create_redis(address="redis://localhost:6379")
    (channel,) = await sub.subscribe(channel_req)
    await reader(channel, pub)

if __name__ == '__main__':
    asyncio.run(main())