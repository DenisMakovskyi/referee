from __future__ import annotations

import json
import asyncio

from typing import Any, List, Dict, Optional
from datetime import datetime
from websockets import Origin

from src.basics import StreamCallbacks, time_millis
from src.exchanges.base import BaseStreamer

ALIAS = "coinbase"

def run(url: str, coins: List[str], callbacks: StreamCallbacks) -> asyncio.Task:
    streamer = CoinbaseStreamer(url=url, coins=coins, callbacks=callbacks)
    return streamer.run()

class CoinbaseStreamer(BaseStreamer):
    __ORIGIN = "https://api.coinbase.com/"

    __TYPE_TICKER = "ticker"

    __KEY_TYPE = "type"
    __KEY_PRODUCT_ID = "product_id"
    __KEY_COIN_PRICE = "price"
    __KEY_COIN_TIMESTAMP = "time"

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
            origin_header=Origin(self.__ORIGIN),
        )
        self._tickers: List[str] = [f"{coin.upper()}-USD" for coin in self.coins]

    def _get_heartbeat_delay(self) -> Optional[int]:
        return None

    def _get_heartbeat_message(self) -> Any:
        return None

    def _get_subscribe_payload(self) -> Dict[str, Any]:
        return {
            "type": "subscribe",
            "channels": [{"name": self.__TYPE_TICKER, "product_ids": self._tickers}],
        }

    def _parse_websocket_frame(self, frame: str | bytes) -> Optional[tuple[str, float, int]]:
        try:
            data = json.loads(frame)
        except json.JSONDecodeError:
            return None

        if data.get(self.__KEY_TYPE) != self.__TYPE_TICKER:
            return None
        product_id = data.get(self.__KEY_PRODUCT_ID)
        if product_id not in self._tickers:
            return None

        coin = product_id.split('-')[0].lower()
        price = float(data.get(self.__KEY_COIN_PRICE, 0))
        iso_time = data.get(self.__KEY_COIN_TIMESTAMP)

        try:
            iso_date = datetime.fromisoformat(iso_time.replace("Z", "+00:00"))
            timestamp = int(iso_date.timestamp() * 1000)
        except (TypeError, ValueError):
            timestamp = time_millis()

        return coin, price, timestamp
