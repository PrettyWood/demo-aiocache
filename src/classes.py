import asyncio
import time
import json
import redis


class RedisPub:
    def __init__(self, host='127.0.0.1', port=6379, db=0):
        self.queue = redis.StrictRedis(host=host, port=port, db=db)

    def pub(self, channel, value):
        self.queue.publish(channel, value)


class RedisSub:
    def __init__(self, publish_to, host='127.0.0.1', port=6379, db=0, publisher=None):
        self.queue = redis.StrictRedis(host=host, port=port, db=db)
        self.channel = self.queue.pubsub()
        self.publisher = publisher
        self.publish_to = publish_to

    async def handle_query_request(self):
        from src.app import get_random_number_from_cache_or_compute
        # channel here is query def request
        while True:
            message = self.channel.get_message()
            if message and message.get('type') == 'message':
                print(f'Got a message {message}')
                _id = message.get('data').decode('utf-8')
                if _id:
                    self.publisher.pub(
                        self.publish_to,
                        json.dumps(await get_random_number_from_cache_or_compute(int(_id)))
                    )

    async def watch_and_publish(self, _id: int):
        while True:
            message = self.channel.get_message()
            if message and message.get('type') == 'message':
                return message.get('data')
