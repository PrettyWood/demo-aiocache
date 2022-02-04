import asyncio
from asyncio import CancelledError

import aioredis
from app import channel_req, _get_random_number_from_cache_or_compute


async def handle_req_msg(msg):
    print('Got Message:', msg)
    await _get_random_number_from_cache_or_compute(int(msg))


STOPWORD = "STOP"


async def reader(ch):
    while await ch.wait_message():
        try:
            msg = await ch.get()
            asyncio.ensure_future(handle_req_msg(int(msg.decode())))
        except CancelledError:
            break



async def main():
    redis = await aioredis.create_redis(address="redis://localhost:6379")
    (channel,) = await redis.subscribe(channel_req)
    await reader(channel)

if __name__ == '__main__':
    asyncio.run(main())