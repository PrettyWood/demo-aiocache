import asyncio
import time
import json
import redis


class RedisPub:
    def __init__(self, host='127.0.0.1', port=6379, db=0, channel=None):
        self.queue = redis.StrictRedis(host=host, port=port, db=db)
        self.channel = channel

    def pub(self, value):
        self.queue.publish(self.channel, value)


class RedisSub:
    def __init__(self, host='127.0.0.1', port=6379, db=0):
        self.queue = redis.StrictRedis(host=host, port=port, db=db)
        self.channel = self.queue.pubsub()

    def watch_and_publish_query_def(self, _id: int):
        while True:
            message = self.channel.get_message()
            if message and message.get('type') == 'message':
                data = json.loads(message.get('data'))
                if int(data.get('_id')) == _id:
                    return data
