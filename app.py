from aiocache import Cache
from fastapi import FastAPI
import asyncio

from classes import RedisPub, RedisSub

app = FastAPI()
cache = Cache(Cache.REDIS, ttl=10)

channel_req = 'query_def_request_channel'
channel_resp = 'query_def_response_channel'


async def get_random_number(x: int) -> int:
    import random

    print("Generating number...")
    n = random.randint(0, 1_000_000)
    print("Generating number...done")
    return n * int(x)


async def _get_random_number_from_cache_or_compute(_id: int):
    if (result_cached := await cache.get(f'key_{_id}')) is not None:
        return {"result": result_cached,"cache": True, '_id': _id}

    result = await get_random_number(_id)
    await cache.set(f'key_{_id}', result)
    return {"result": result,"cache": False,  '_id': _id}


query_def_request_publisher = RedisPub(channel=channel_req)
query_def_response_subscriber = RedisSub()
query_def_response_subscriber.channel.subscribe(channel_resp)



@app.get('/')
async def root(_id: int):
    from datetime import datetime
    # publish on channel_req
    # id => channel_req
    print(f'start {datetime.now().time()}')
    await asyncio.sleep(5)
    query_def_request_publisher.pub(int(_id))
    # Listen for the result in query_def_response_channel
    res = query_def_response_subscriber.watch_and_publish_query_def(int(_id))
    print(f'end {datetime.now().time()}')
    return res