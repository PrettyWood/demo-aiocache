import asyncio
import json

import aioredis
from aiocache import Cache

from app import REQ_CHANNEL_NAME, RESP_CHANNEL_NAME

cache = Cache(Cache.REDIS, ttl=10)


async def get_random_number(x: int) -> int:
    import random

    print(f"Generating number for {x}...")
    n = random.randint(0, 1_000_000)
    await asyncio.sleep(5)
    print(f"Generating number for {x}...done")
    return n * int(x)


async def _get_random_number_from_cache_or_compute(_id: int, pub: aioredis.Redis):
    while (result_cached := await cache.get(f'key_{_id}')) == 'PENDING':
        await asyncio.sleep(0.1)

    if result_cached:
        await pub.publish(RESP_CHANNEL_NAME, message=json.dumps({"result": result_cached,"cache": True, '_id': _id}))
    else:
        await cache.set(f'key_{_id}', 'PENDING')
        result = await get_random_number(_id)
        await cache.set(f'key_{_id}', result)
        await pub.publish(RESP_CHANNEL_NAME, message=json.dumps({"result": result,"cache": False,  '_id': _id}))


async def reader(req_channel: aioredis.Channel, redis: aioredis.Redis):
    while await req_channel.wait_message():
        msg = await req_channel.get_json()
        asyncio.ensure_future(_get_random_number_from_cache_or_compute(int(msg), redis))


async def main():
    sub = await aioredis.create_redis(address="redis://localhost:6379")
    pub = await aioredis.create_redis(address="redis://localhost:6379")
    (req_channel,) = await sub.subscribe(REQ_CHANNEL_NAME)
    await reader(req_channel, pub)

if __name__ == '__main__':
    asyncio.run(main())
