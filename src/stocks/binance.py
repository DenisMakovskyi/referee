from __future__ import annotations

import ssl
import json
import certifi
import asyncio
import websockets

from typing import Callable

ALIAS = "binance"

__BINANCE_KEY_PRICE = "c"
__BINANCE_KEY_TIMESTAMP = "E"

async def _stream_prices(url:str, coin: str, callback: Callable[[float, int], None]) -> None:
    ticker = f"{coin.lower()}usdt@ticker"
    certificate = ssl.create_default_context(cafile=certifi.where())
    async with websockets.connect(uri=f"{url}/{ticker}", ssl=certificate) as socket:
        async for msg in socket:
            data = json.loads(msg)
            price = float(data.get(__BINANCE_KEY_PRICE))
            timestamp = int(data.get(__BINANCE_KEY_TIMESTAMP))
            callback(price, timestamp)


def run(url: str, coin: str, callback: Callable[[float, int], None]) -> asyncio.Task:
    return asyncio.create_task(coro=_stream_prices(url=url, coin=coin, callback=callback))
