import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
import redis
from app import channel_req, channel_resp, _get_random_number_from_cache_or_compute
from classes import RedisPub, RedisSub

if __name__ == '__main__':
    query_def_response_publisher = RedisPub(channel=channel_resp)
    query_def_request_subscriber = RedisSub()
    query_def_request_subscriber.channel.subscribe(channel_req)

    while True:
        message = query_def_request_subscriber.channel.get_message()
        if message:
            print(message)
        if message and message.get('type') == 'message':
            print(f'Got a message {message}')
            _id = message.get('data').decode('utf-8')
            res = asyncio.run(_get_random_number_from_cache_or_compute(_id))
            query_def_response_publisher.pub(json.dumps(res))
