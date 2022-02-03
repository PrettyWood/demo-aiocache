import asyncio

from aiocache import Cache
from aiocache.lock import RedLock
from fastapi import FastAPI

app = FastAPI()
cache = Cache(Cache.REDIS, ttl=10)


async def get_random_number(x: int) -> int:
    import random

    print("Generating number...")
    await asyncio.sleep(3)
    n = random.randint(0, 1_000_000)
    print("Generating number...done")

    return n * x


@app.get("/")
async def root(id: int = 0):
    async with RedLock(cache, f'key_{id}', lease=2):
        if (result_cached := await cache.get(f'key_{id}'))is not None:
            return {"random number": result_cached}

    result = await get_random_number(id)
    await cache.set(f'key_{id}', result)
    
    return {"random number": result}
