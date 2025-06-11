from __future__ import annotations

import json
import asyncio

from typing import Any, List, Dict, Optional

from src.basics import StreamCallbacks, time_millis
from src.exchanges.base import BaseStreamer

ALIAS = "bybit"

def run(url: str, coins: List[str], callbacks: StreamCallbacks) -> asyncio.Task:
    streamer = BybitStreamer(url=url, coins=coins, callbacks=callbacks)
    return streamer.run()

class BybitStreamer(BaseStreamer):
    __KEY_DATA = "data"
    __KEY_TOPIC = "topic"
    __KEY_COIN_PRICE = "lastPrice"
    __KEY_COIN_TIMESTAMP = "ts"

    def __init__(
            self,
            url: str,
            coins: List[str],
            callbacks: StreamCallbacks
    ):
        super().__init__(
            url=url,
            coins=coins,
            callbacks=callbacks,
            origin_header=None,
        )
        self._tickers = [f"tickers.{coin.upper()}USDT" for coin in self.coins]

    def _get_heartbeat_delay(self) -> Optional[int]:
        return None

    def _get_heartbeat_message(self) -> Any:
        return None

    def _get_subscribe_payload(self) -> Dict[str, Any]:
        return {
            "op": "subscribe",
            "args": self._tickers,
        }

    def _parse_websocket_frame(self, frame: str | bytes) -> Optional[tuple[str, float, int]]:
        try:
            raw = json.loads(frame)
        except json.JSONDecodeError:
            return None

        topic = raw.get(self.__KEY_TOPIC)
        if topic in self._tickers and self.__KEY_DATA in raw:
            coin = topic.split('.')[1][:-4].lower()
            data = raw[self.__KEY_DATA]
            price = float(data.get(self.__KEY_COIN_PRICE, 0))
            timestamp = int(raw.get(self.__KEY_COIN_TIMESTAMP, time_millis()))
            return coin, price, timestamp
        return None