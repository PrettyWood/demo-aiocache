import asyncio
import json
from asyncio import CancelledError

from aiocache import Cache
from fastapi import FastAPI
import aioredis
from datetime import datetime

STOPWORD = "STOP"

app = FastAPI()
cache = Cache(Cache.REDIS, ttl=100)

channel_req = 'query_def_request_channel'
channel_resp = 'query_def_response_channel'


async def get_random_number(x: int) -> int:
    import random

    print("Generating number...")
    n = random.randint(0, 1_000_000)
    await asyncio.sleep(5)
    print("Generating number...done")
    return n * int(x)


async def _get_random_number_from_cache_or_compute(_id: int):
    redis = await aioredis.create_redis(address="redis://localhost:6379")
    if (result_cached := await cache.get(f'key_{_id}')) is not None:
        await redis.publish(channel_resp, message=json.dumps({"result": result_cached,"cache": True, '_id': _id}))
    else:
        result = await get_random_number(_id)
        await cache.set(f'key_{_id}', result)
        await redis.publish(channel_resp, message=json.dumps({"result": result,"cache": False,  '_id': _id}))



@app.get('/')
async def root(_id: int):
    print(f'start {datetime.now().time()} - id {_id}')

    redis = await aioredis.create_redis(address="redis://localhost:6379")
    await redis.publish(channel_req, _id)
    (chan,) = await redis.subscribe(channel_resp)
    while await chan.wait_message():
        try:
            msg = await chan.get()
            print(f'end {datetime.now().time()} - id {_id}')
            return msg
        except CancelledError:
            return
