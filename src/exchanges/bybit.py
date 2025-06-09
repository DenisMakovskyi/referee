from __future__ import annotations

import time

import ssl
import json
import certifi
import asyncio
import websockets

from typing import List

from src.basics import StreamCallbacks

ALIAS = "bybit"

__BYBIT_KEY_DATA = "data"
__BYBIT_KEY_TOPIC = "topic"
__BYBIT_KEY_COIN_PRICE = "lastPrice"
__BYBIT_KEY_COIN_TIMESTAMP = "ts"

def run(url: str, coins: List[str], callbacks: StreamCallbacks) -> asyncio.Task:
    return asyncio.create_task(coro=_stream_prices(url=url, coins=coins, callbacks=callbacks))

async def _stream_prices(url: str, coins: List[str], callbacks: StreamCallbacks) -> None:
    params = [f"tickers.{coin.upper()}USDT" for coin in coins]
    payload = {"op": "subscribe", "args": params}
    certificate = ssl.create_default_context(cafile=certifi.where())
    async with websockets.connect(uri=url, ssl=certificate) as socket:
        await socket.send(json.dumps(payload))
        async for frame in socket:
            raw = json.loads(frame)
            topic = raw.get(__BYBIT_KEY_TOPIC)
            if topic in params and __BYBIT_KEY_DATA in raw:
                coin = topic.split('.')[1][:-4].lower()
                if coin in callbacks:
                    data = raw[__BYBIT_KEY_DATA]
                    price = float(data.get(__BYBIT_KEY_COIN_PRICE, 0))
                    timestamp = int(raw.get(__BYBIT_KEY_COIN_TIMESTAMP, int(time.time())))
                    callbacks[coin](price, timestamp)