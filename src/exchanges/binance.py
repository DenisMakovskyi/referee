from __future__ import annotations

import ssl
import json
import certifi
import asyncio
import websockets

from typing import List

from src.basics import StreamCallbacks

ALIAS = "binance"

__TICKER_24_H = "24hrTicker"

__BINANCE_KEY_TICKER = "e"
__BINANCE_KEY_SYMBOL = "s"
__BINANCE_KEY_COIN_PRICE = "c"
__BINANCE_KEY_COIN_TIMESTAMP = "E"

def run(url: str, coins: List[str], callbacks: StreamCallbacks) -> asyncio.Task:
    return asyncio.create_task(coro=_stream_prices(url=url, coins=coins, callbacks=callbacks))

async def _stream_prices(url: str, coins: List[str], callbacks: StreamCallbacks) -> None:
    params = [f"{coin.lower()}usdt@ticker" for coin in coins]
    payload = {"id": 1, "method": "SUBSCRIBE", "params": params}
    certificate = ssl.create_default_context(cafile=certifi.where())
    async with websockets.connect(uri=url, ssl=certificate) as socket:
        await socket.send(json.dumps(payload))
        async for msg in socket:
            data = json.loads(msg)
            if data.get(__BINANCE_KEY_TICKER) == __TICKER_24_H:
                sym = data.get(__BINANCE_KEY_SYMBOL, "")
                coin = sym[:-4].lower() if sym.endswith("USDT") else sym.lower()
                if coin in callbacks:
                    price = float(data.get(__BINANCE_KEY_COIN_PRICE))
                    timestamp = int(data.get(__BINANCE_KEY_COIN_TIMESTAMP))
                    callbacks[coin](price, timestamp)