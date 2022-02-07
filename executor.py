import asyncio
import json

import aioredis
from aiocache import Cache

from app import REQ_CHANNEL_NAME, RESP_CHANNEL_NAME
# Aioredlock is part of aioredis>2 but we still need version 1.3 for now
from aioredlock import Aioredlock, Lock, LockError

cache = Cache(Cache.REDIS, ttl=10)
lock_manager = Aioredlock()


async def get_random_number(x: int) -> int:
    import random

    print(f"[get_random_number] Generating number for {x}...")
    n = random.randint(0, 1_000_000)
    await asyncio.sleep(5)
    print(f"[get_random_number] Generating number for {x}...done")
    return n * int(x)


async def _get_random_number_from_cache_or_compute(_id: int, pub: aioredis.Redis):
    while await lock_manager.is_locked(f"lock_{_id}"):
        print(f"[_get_random_number_from_cache_or_compute] Waiting for 'lock_{_id}' to be released...")
        await asyncio.sleep(0.1)

    if (result_cached := await cache.get(f'key_{_id}')) is not None:
        await pub.publish(RESP_CHANNEL_NAME, message=json.dumps({"result": result_cached,"cache": True, '_id': _id}))
    else:
        try:
            async with await lock_manager.lock(f"lock_{_id}", lock_timeout=10):
                print(f"[fetch_and_set] lock 'lock_{_id}' acquired")
                result = await get_random_number(_id)
                await cache.set(f'key_{_id}', result)
                await pub.publish(RESP_CHANNEL_NAME, message=json.dumps({"result": result,"cache": False,  '_id': _id}))
        except LockError:
            return await _get_random_number_from_cache_or_compute(_id, pub)

async def reader(req_channel: aioredis.Channel, redis: aioredis.Redis):
    while await req_channel.wait_message():
        print(f"[reader] wait message...")
        msg = await req_channel.get_json()
        print(f"[reader] got message...{msg}")
        asyncio.create_task(_get_random_number_from_cache_or_compute(int(msg), redis))


async def main():
    sub = await aioredis.create_redis(address="redis://localhost:6379")
    pub = await aioredis.create_redis(address="redis://localhost:6379")
    (req_channel,) = await sub.subscribe(REQ_CHANNEL_NAME)
    await reader(req_channel, pub)

if __name__ == '__main__':
    asyncio.run(main(), debug=True)
