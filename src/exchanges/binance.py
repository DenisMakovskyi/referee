from __future__ import annotations

import asyncio
import json
from typing import Callable

import websockets

from ..config import Exchange


async def stream_prices(token: str, exchange: Exchange, callback: Callable[[float, float], None]) -> None:
    url = exchange.websocket_url
    stream = f"{token.lower()}usdt@ticker"
    async with websockets.connect(f"{url}/{stream}") as ws:
        async for msg in ws:
            data = json.loads(msg)
            price = float(data.get("c"))
            timestamp = data.get("E") / 1000
            callback(timestamp, price)


def run(token: str, exchange: Exchange, callback: Callable[[float, float], None]) -> asyncio.Task:
    return asyncio.create_task(stream_prices(token, exchange, callback))
