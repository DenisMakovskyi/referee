from __future__ import annotations

import json
import asyncio

from typing import Any, List, Dict, Optional

from src.basics import StreamCallbacks, time_millis
from src.exchanges.base import BaseStreamer

ALIAS = "binance"

def run(url: str, coins: List[str], callbacks: StreamCallbacks) -> asyncio.Task:
    streamer = BinanceStreamer(url=url, coins=coins, callbacks=callbacks)
    return streamer.run()

class BinanceStreamer(BaseStreamer):
    __TICKER_24_H = "24hrTicker"

    __KEY_TICKER = "e"
    __KEY_SYMBOL = "s"
    __KEY_COIN_PRICE = "c"
    __KEY_COIN_TIMESTAMP = "E"

    def _get_heartbeat_delay(self) -> Optional[int]:
        return None

    def _get_heartbeat_message(self) -> Any:
        return None

    def _get_subscribe_payload(self) -> Dict[str, Any]:
        params = [f"{coin.lower()}usdt@ticker" for coin in self.coins]
        return {
            "id": 1,
            "method": "SUBSCRIBE",
            "params": params,
        }

    def _parse_websocket_frame(self, frame: str | bytes) -> Optional[tuple[str, float, int]]:
        try:
            data = json.loads(frame)
        except json.JSONDecodeError:
            return None

        if data.get(self.__KEY_TICKER) != self.__TICKER_24_H:
            return None

        sym = data.get(self.__KEY_SYMBOL, "")
        coin = sym[:-4].lower() if sym.endswith("USDT") else sym.lower()
        price = float(data.get(self.__KEY_COIN_PRICE, 0))
        timestamp = int(data.get(self.__KEY_COIN_TIMESTAMP, time_millis()))
        return coin, price, timestamp