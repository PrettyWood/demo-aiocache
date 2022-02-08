import asyncio
import json

from aiocache import Cache
import aioredis
# Aioredlock is part of aioredis>2 but we still need version 1.3 for now
# See https://github.com/aio-libs/aiocache/issues/543
from aioredlock import Aioredlock, LockError

from app import REQ_CHANNEL_NAME, RESP_CHANNEL_NAME

cache = Cache(Cache.REDIS, ttl=10)
lock_manager = Aioredlock()


async def get_random_number(x: int) -> int:
    import random

    print(f"[get_random_number] Generating number for {x}...")
    n = random.randint(0, 1_000_000)
    await asyncio.sleep(5)
    print(f"[get_random_number] Generating number for {x}...done")
    return n * int(x)


async def _get_random_number_from_cache_or_compute(x: int, pub: aioredis.Redis):
    while await lock_manager.is_locked(f"lock_{x}"):
        print(f"[_get_random_number_from_cache_or_compute] Waiting for 'lock_{x}' to be released...")
        await asyncio.sleep(0.1)

    if (result_cached := await cache.get(f"key_{x}")) is not None:
        await pub.publish(RESP_CHANNEL_NAME, message=json.dumps({"result": result_cached,"cache": True, "id": x}))
    else:
        try:
            async with await lock_manager.lock(f"lock_{x}", lock_timeout=10):
                print(f"[fetch_and_set] lock 'lock_{x}' acquired")
                result = await get_random_number(x)
                await cache.set(f"key_{x}", result)
                await pub.publish(RESP_CHANNEL_NAME, message=json.dumps({"result": result,"cache": False,  "id": x}))
        except LockError:
            return await _get_random_number_from_cache_or_compute(x, pub)

async def reader(req_channel: aioredis.Channel, redis: aioredis.Redis):
    while await req_channel.wait_message():
        print(f"[reader] wait message...")
        req_id = await req_channel.get_json()
        print(f"[reader] got message...{req_id}")
        asyncio.create_task(_get_random_number_from_cache_or_compute(int(req_id), redis))


async def main():
    sub = await aioredis.create_redis(address="redis://localhost:6379")
    pub = await aioredis.create_redis(address="redis://localhost:6379")
    (req_channel,) = await sub.subscribe(REQ_CHANNEL_NAME)
    await reader(req_channel, pub)

if __name__ == "__main__":
    asyncio.run(main())
