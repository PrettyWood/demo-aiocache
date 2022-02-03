import sys

import redis
from aiocache import Cache
from src.classes import RedisSub, RedisPub
from fastapi import FastAPI
import asyncio
from threading import Thread

app = FastAPI()
cache = Cache(Cache.REDIS, ttl=10)

channel_req = 'query_def_request_channel'
channel_resp = 'query_def_response_channel'


async def get_random_number(x: int) -> int:
    import random

    print("Generating number...")
    n = random.randint(0, 1_000_000)
    print("Generating number...done")

    return n * x


async def get_random_number_from_cache_or_compute(_id: int):
    if (result_cached := await cache.get(f'key_{_id}')) is not None:
        return {"result": result_cached}

    result = await get_random_number(_id)
    await cache.set(f'key_{_id}', result)
    return {"result": result}


query_def_publisher = RedisPub()

query_def_request_subscriber = RedisSub(
    publish_to=channel_resp,
    publisher=query_def_publisher
)
# subscribe on channel_req
query_def_request_subscriber.channel.subscribe(channel_req)

tt = Thread(
    target=asyncio.run,
    args=(query_def_request_subscriber.handle_query_request(),)
)
tt.daemon = True
try:
    tt.start()
    # pub/sub fetch query-definition
    query_def_response_subscriber = RedisSub(
        publish_to=channel_req,
        publisher=query_def_publisher
    )
    # subscribe on channel_resp
    query_def_response_subscriber.channel.subscribe(channel_resp)
except (KeyboardInterrupt, SystemExit):
    sys.exit()

@app.get('/')
async def root(_id: int):
    # publish on channel_req
    # id => channel_req
    query_def_publisher.pub(channel_req, int(_id))
    # Listen for the result in query_def_response_channel
    return await query_def_response_subscriber.watch_and_publish(int(_id))

#
# if (__name__ == '__main__'):
#     query_def_publisher.pub(channel_req, 5)
#
#
#
# # @app.get("/")
# # async def root(id: int = 0):
# #     # la requête arrive
# #     # on fait une "demande" de query def => get_random_number_from_cache_or_compute()
# #     # 2 canaux
# #     # 1 canal "demande de query def"
# #     # 1 canal "resultat de query def"
# #     # on se met en ecoute via un subscriber
# #     # on retourne la query def
# #
# #     # We start the subscriber
# #     query_def_subscriber.sub()
#
