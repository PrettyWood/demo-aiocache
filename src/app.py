import asyncio

from aiocache import Cache, cached_stampede
from fastapi import FastAPI

app = FastAPI()
cache = Cache(Cache.REDIS)

@cached_stampede(ttl=10, cache=Cache.REDIS, lease=1)
async def get_random_number(x: int) -> int:
    import random

    print("Generating number...")
    await asyncio.sleep(3)
    n = random.randint(0, 1_000_000)
    print("Generating number...done")

    return n * x


@app.get("/")
async def root(id: int = 0):
    random_number = await get_random_number(id)
    
    return {"random number": random_number}
