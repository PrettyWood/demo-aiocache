import asyncio
from typing import Optional

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


async def _get_random_number_from_cache_or_compute(
    req_id: int, pub: Optional[aioredis.Redis] = None
):
    if pub is None:
        pub = await aioredis.create_redis(address="redis://localhost:6379")

    while await lock_manager.is_locked(f"lock_{req_id}"):
        print(
            f"[_get_random_number_from_cache_or_compute] Waiting for 'lock_{req_id}' to be released..."
        )
        await asyncio.sleep(0.1)

    if (result_cached := await cache.get(f"key_{req_id}")) is not None:
        await pub.publish_json(
            f"{RESP_CHANNEL_NAME}:{req_id}",
            {"result": result_cached, "cache": True, "id": req_id},
        )
    else:
        try:
            async with await lock_manager.lock(f"lock_{req_id}", lock_timeout=10):
                print(f"[fetch_and_set] lock 'lock_{req_id}' acquired")
                result = await get_random_number(req_id)
                await cache.set(f"key_{req_id}", result)
                await pub.publish_json(
                    f"{RESP_CHANNEL_NAME}:{req_id}",
                    {"result": result, "cache": False, "id": req_id},
                )
        except LockError:
            return await _get_random_number_from_cache_or_compute(req_id, pub)


async def reader(channel):
    async for req_ch, req_id in channel.iter():
        print("Got message in channel:", req_ch, ":", req_id)
        asyncio.create_task(_get_random_number_from_cache_or_compute(int(req_id)))


async def main():
    sub = await aioredis.create_redis(address="redis://localhost:6379")

    try:
        (ch,) = await sub.psubscribe(f"{REQ_CHANNEL_NAME}:*")
        await asyncio.get_running_loop().create_task(reader(ch))
    finally:
        sub.close()
        await sub.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
