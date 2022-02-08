from datetime import datetime
import json

import aioredis
from fastapi import FastAPI

app = FastAPI()

REQ_CHANNEL_NAME = "query_def_request_channel"
RESP_CHANNEL_NAME = "query_def_response_channel"


@app.on_event("startup")
async def starup_event():
    app.state.pub = await aioredis.create_redis(address="redis://localhost:6379")
    app.state.sub = await aioredis.create_redis(address="redis://localhost:6379")


@app.on_event("shutdown")
async def shutdown_event():
    app.state.pub.close()
    app.state.sub.close()
    await app.state.pub.wait_closed()
    await app.state.sub.wait_closed()


@app.get("/")
async def root(id: int):
    assert isinstance(app.state.pub, aioredis.Redis)
    await app.state.pub.publish(REQ_CHANNEL_NAME, id)
    (resp_chan,) = await app.state.sub.subscribe(RESP_CHANNEL_NAME)
    assert isinstance(resp_chan, aioredis.Channel)
    while await resp_chan.wait_message():
        msg = await resp_chan.get()
        msg = json.loads(msg)
        if msg.get("id") == int(id):
            return msg
