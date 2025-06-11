from __future__ import annotations

import json
import asyncio

from typing import Any, List, Dict, Optional

from src.basics import StreamCallbacks
from src.exchanges.base import BaseStreamer
from src.exchanges.mexc.proto.PushDataV3ApiWrapper_pb2 import PushDataV3ApiWrapper

ALIAS = "mexc"

def run(url: str, coins: List[str], callbacks: StreamCallbacks) -> asyncio.Task:
    streamer = MexcStreamer(url=url, coins=coins, callbacks=callbacks)
    return streamer.run()

class MexcStreamer(BaseStreamer):
    __CHANNEL = "spot@public.aggre.deals.v3.api.pb@"
    __FRAME_INTERVAL = "100ms"
    __HEARTBEAT_INTERVAL = 25

    __CODE_OK = 0
    __MSG_PONG = "pong"

    __KEY_CODE = "code"
    __KEY_MESSAGE = "msg"

    def __init__(
        self,
        url: str,
        coins: List[str],
        callbacks: StreamCallbacks,
    ):
        super().__init__(
            url=url,
            coins=coins,
            callbacks=callbacks,
            origin_header=None,
        )
        self._tickers: List[str] = [
            f"{self.__CHANNEL}{self.__FRAME_INTERVAL}@{coin.upper()}USDT"
            for coin in self.coins
        ]

    def _get_heartbeat_delay(self) -> Optional[int]:
        return self.__HEARTBEAT_INTERVAL

    def _get_heartbeat_message(self) -> Any:
        return {"method": "PING"}

    def _get_subscribe_payload(self) -> Dict[str, Any]:
        return {"method": "SUBSCRIPTION", "params": self._tickers, "id": 1}

    def _parse_websocket_frame(self, frame: str | bytes) -> Optional[tuple[str, float, int]]:
        if isinstance(frame, str):
            try:
                data = json.loads(frame)
            except json.JSONDecodeError:
                return None
            if self.__is_pong_frame(data):
                return None
            return None

        wrapper = PushDataV3ApiWrapper()
        try:
            wrapper.ParseFromString(frame)
        except Exception:
            return None

        if not wrapper.channel.startswith(self.__CHANNEL):
            return None

        coin = wrapper.symbol[:-4].lower()
        deals = wrapper.publicAggreDeals.deals
        if not deals:
            return None

        deal = deals[-1]
        price = float(deal.price)
        timestamp = int(deal.time)
        return coin, price, timestamp

    def __is_pong_frame(self, data: Any) -> bool:
        return data.get(self.__KEY_CODE) == self.__CODE_OK or data.get(self.__KEY_MESSAGE) == self.__MSG_PONG