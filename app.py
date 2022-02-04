import asyncio
import json
from asyncio import CancelledError

from aiocache import Cache
from fastapi import FastAPI
import aioredis
from datetime import datetime
STOPWORD = "STOP"

app = FastAPI()

channel_req = 'query_def_request_channel'
channel_resp = 'query_def_response_channel'


@app.on_event('startup')
async def starup_event():
    app.state.pub = await aioredis.create_redis(address="redis://localhost:6379")
    app.state.sub = await aioredis.create_redis(address="redis://localhost:6379")


@app.on_event('shutdown')
async def shutdown_event():
    app.state.pub.close()
    app.state.sub.close()
    await app.state.pub.wait_closed()
    await app.state.sub.wait_closed()


@app.get('/')
async def root(_id: int):
    print(f'start {datetime.now().time()} - id {_id}')
    await app.state.pub.publish(channel_req, _id)
    (chan,) = await app.state.sub.subscribe(channel_resp)
    while await chan.wait_message():
        msg = await chan.get()
        msg = json.loads(msg)
        if msg.get('_id') == int(_id):
            print(f'end {datetime.now().time()} - id {_id}')
            return msg
