from __future__ import annotations

import ssl
import json
import asyncio
import certifi
import websockets

from typing import List

from src.basics import StreamCallbacks
from src.exchanges.mexc.proto.PushDataV3ApiWrapper_pb2 import PushDataV3ApiWrapper

ALIAS = "mexc"

_CHANNEL = "spot@public.aggre.deals.v3.api.pb@"
_PING_INTERVAL = 25  # треба пінгати MEXC кожні 25 секунд, щоб він не дропнув конекшин

_CODE_OK = 0
_MESSAGE_PONG = "pong"

_MEXC_KEY_CODE = "code"
_MEXC_KEY_MESSAGE = "msg"

def run(url: str, coins: List[str], callbacks: StreamCallbacks) -> asyncio.Task:
    return asyncio.create_task(coro=_stream_prices(url=url, coins=coins, callbacks=callbacks))

async def _stream_prices(url: str, coins: List[str], callbacks: StreamCallbacks) -> None:
    channels = [
        f"{_CHANNEL}100ms@{coin.upper()}USDT"
        for coin in coins
    ]
    payload = {"method": "SUBSCRIPTION", "params": channels, "id": 1}
    certificate = ssl.create_default_context(cafile=certifi.where())

    async def _heartbeat(websocket: websockets.WebSocketClientProtocol) -> None:
        while True:
            await asyncio.sleep(_PING_INTERVAL)
            await websocket.send(json.dumps({"method": "PING"}))

    while True:
        try:
            async with websockets.connect(uri=url, ssl=certificate) as socket:
                await socket.send(json.dumps(payload))
                heartbeat = asyncio.create_task(_heartbeat(socket))
                try:
                    async for frame in socket:
                        if isinstance(frame, str):
                            data = json.loads(frame)
                            if data.get(_MEXC_KEY_CODE) == _CODE_OK or data.get(_MEXC_KEY_MESSAGE) == _MESSAGE_PONG:
                                continue
                        else:
                            wrapper = PushDataV3ApiWrapper()
                            wrapper.ParseFromString(frame)
                            if wrapper.channel.startswith(_CHANNEL):
                                coin = wrapper.symbol[:-4].lower()
                                if coin in callbacks and wrapper.publicAggreDeals.deals:
                                    deal = wrapper.publicAggreDeals.deals[-1]
                                    price = float(deal.price)
                                    timestamp = int(wrapper.sendTime)
                                    callbacks[coin](price, timestamp)
                finally:
                    heartbeat.cancel()
        except Exception:
            await asyncio.sleep(1)