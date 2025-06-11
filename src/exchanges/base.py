from __future__ import annotations

import ssl
import json
import asyncio
import certifi
import websockets

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Optional
from websockets import Origin

from src.basics import StreamCallbacks

class BaseStreamer(ABC):
    def __init__(
        self,
        url: str,
        coins: List[str],
        callbacks: StreamCallbacks,
        origin_header: Optional[Origin] = None,
    ):
        self.url = url
        self.coins = coins
        self.callbacks = callbacks
        self.ssl_context = ssl.create_default_context(cafile=certifi.where())
        self.origin_header = origin_header

    @abstractmethod
    def _get_heartbeat_delay(self) -> Optional[int]:
        ...

    @abstractmethod
    def _get_heartbeat_message(self) -> Any:
        ...

    @abstractmethod
    def _get_subscribe_payload(self) -> Dict[str, Any]:
        ...

    @abstractmethod
    def _parse_websocket_frame(self, frame: str | bytes) -> Optional[tuple[str, float, int]]:
        ...

    def run(self) -> asyncio.Task:
        return asyncio.create_task(coro=self.__run_loop())

    async def __run_loop(self) -> None:
        while True:
            try:
                async with websockets.connect(
                    uri=self.url,
                    ssl=self.ssl_context,
                    origin=self.origin_header,
                ) as socket:
                    await socket.send(json.dumps(self._get_subscribe_payload()))

                    interval = self._get_heartbeat_delay()
                    heartbeat: Optional[asyncio.Task] = None
                    if interval is not None:
                        heartbeat = asyncio.create_task(self.__heartbeat(interval=interval, websocket=socket))

                    try:
                        async for frame in socket:
                            result = self._parse_websocket_frame(frame)
                            if result:
                                coin, price, timestamp = result
                                if coin in self.callbacks:
                                    self.callbacks[coin](price, timestamp)
                    finally:
                        if heartbeat:
                            heartbeat.cancel()
            except Exception:
                await asyncio.sleep(1)

    async def __heartbeat(self, interval: int, websocket: websockets.WebSocketClientProtocol) -> None:
        while True:
            await asyncio.sleep(interval)
            message = self._get_heartbeat_message()
            if message is not None:
                await websocket.send(json.dumps(message))