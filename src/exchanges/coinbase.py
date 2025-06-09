from __future__ import annotations

import ssl
import json
import certifi
import asyncio
import websockets

from typing import List
from datetime import datetime
from websockets import Origin

from src.basics import StreamCallbacks

ALIAS = "coinbase"

__TYPE_TICKER = "ticker"

__COINBASE_ORIGIN = "https://api.coinbase.com/"
__COINBASE_KEY_TYPE = "type"
__COINBASE_KEY_PRODUCT_ID = "product_id"
__COINBASE_KEY_COIN_PRICE = "price"
__COINBASE_KEY_COIN_TIMESTAMP = "time"

def run(url: str, coins: List[str], callbacks: StreamCallbacks) -> asyncio.Task:
    return asyncio.create_task(coro=_stream_prices(url=url, coins=coins, callbacks=callbacks))

async def _stream_prices(url: str, coins: List[str], callbacks: StreamCallbacks) -> None:
    params = [f"{coin.upper()}-USD" for coin in coins]
    payload = {
        "type": "subscribe",
        "channels": [{"name": "ticker", "product_ids": params}]
    }
    certificate = ssl.create_default_context(cafile=certifi.where())
    async with websockets.connect(uri=url, ssl=certificate, origin=Origin(__COINBASE_ORIGIN)) as socket:
        await socket.send(json.dumps(payload))
        async for msg in socket:
            data = json.loads(msg)
            if data.get(__COINBASE_KEY_TYPE) == __TYPE_TICKER and data.get(__COINBASE_KEY_PRODUCT_ID) in params:
                coin = data[__COINBASE_KEY_PRODUCT_ID].split('-')[0].lower()
                if coin in callbacks:
                    price = float(data.get(__COINBASE_KEY_COIN_PRICE, 0))
                    iso_date = datetime.fromisoformat(data.get(__COINBASE_KEY_COIN_TIMESTAMP).replace("Z", "+00:00"))
                    timestamp = int(iso_date.timestamp() * 1000)
                    callbacks[coin](price, timestamp)