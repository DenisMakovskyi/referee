from __future__ import annotations

import ssl
import json
import certifi
import asyncio
import websockets

from typing import Callable
from datetime import datetime

from websockets import Origin

ALIAS = "coinbase"

__COINBASE_ORIGIN = "https://api.coinbase.com/"
__COINBASE_KEY_PRICE = "price"
__COINBASE_KEY_TIMESTAMP = "time"

async def _stream_prices(url: str, coin: str, callback: Callable[[float, int], None]) -> None:
    symbol = f"{coin.upper()}-USD"
    payload = {
        "type": "subscribe",
        "channels": [
            {"name": "ticker", "product_ids": [symbol]}
        ]
    }
    certificate = ssl.create_default_context(cafile=certifi.where())
    async with websockets.connect(uri=url, ssl=certificate, origin=Origin(__COINBASE_ORIGIN)) as socket:
        await socket.send(json.dumps(payload))
        async for raw in socket:
            msg = json.loads(raw)
            if msg.get("type") == "ticker" and msg.get("product_id") == symbol:
                price = float(msg[__COINBASE_KEY_PRICE])
                iso_date = datetime.fromisoformat(msg[__COINBASE_KEY_TIMESTAMP].replace("Z", "+00:00"))
                timestamp = int(iso_date.timestamp() * 1000)
                callback(price, timestamp)


def run(url: str, coin: str, callback: Callable[[float, int], None]) -> asyncio.Task:
    return asyncio.create_task(coro=_stream_prices(url=url, coin=coin, callback=callback))