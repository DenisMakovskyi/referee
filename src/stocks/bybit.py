from __future__ import annotations

import time

import ssl
import json
import certifi
import asyncio
import websockets

from typing import Callable

ALIAS = "bybit"

__BYBIT_KEY_DATA = "data"
__BYBIT_KEY_PRICE = "lastPrice"
__BYBIT_KEY_TIMESTAMP = "ts"

async def _stream_prices(url: str, coin: str, callback: Callable[[float, int], None]) -> None:
    symbol = f"{coin.upper()}USDT"
    ticker = f"tickers.{symbol}"
    certificate = ssl.create_default_context(cafile=certifi.where())
    async with websockets.connect(uri=url, ssl=certificate) as socket:
        await socket.send(
            json.dumps({
                "op": "subscribe",
                "args": [ticker]
            })
        )
        async for msg in socket:
            msg = json.loads(msg)
            if msg.get("topic") == ticker and __BYBIT_KEY_DATA in msg:
                data = msg[__BYBIT_KEY_DATA]
                price = float(data[__BYBIT_KEY_PRICE])
                timestamp = int(msg.get(__BYBIT_KEY_TIMESTAMP, int(time.time())))
                callback(price, timestamp)

def run(url: str, coin: str, callback: Callable[[float, int], None]) -> asyncio.Task:
    return asyncio.create_task(
        _stream_prices(url=url, coin=coin, callback=callback)
    )
