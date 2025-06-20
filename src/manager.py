import asyncio

from typing import List

from config import Settings

from src.basics import StreamCallbacks, chunked, are_contents_the_same
from src.database import Database

from src.interests.bubble import Bubble
from src.interests.bubbles import bubbles_fetch, bubbles_filter

from src.exchanges.mexc.mexc import ALIAS as ALIAS_MEXC, run as run_mexc
from src.exchanges.bybit import ALIAS as ALIAS_BYBIT, run as run_bybit
from src.exchanges.binance import ALIAS as ALIAS_BINANCE, run as run_binance
from src.exchanges.coinbase import ALIAS as ALIAS_COINBASE, run as run_coinbase

class MainManager:
    _CHUNK_SIZE_MEXC = 30
    _CHUNK_SIZE_COINBASE = 25

    def __init__(self, settings: Settings, database: Database):
        self.settings = settings
        self.database = database
        self.watchlist: List[Bubble] = []
        self.asynchronous: List[asyncio.Task] = []

    async def run(self) -> bool:
        def _symbols(bubbles: List[Bubble]) -> List[str]:
            return [b.symbol for b in bubbles]

        watchlist = await bubbles_fetch(self.settings.filter)
        if not watchlist:
            return False
        if are_contents_the_same(_symbols(watchlist), _symbols(self.watchlist)):
            return False

        await self._start(watchlist)
        return True

    async def shutdown(self):
        if self.asynchronous:
            for task in self.asynchronous:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*self.asynchronous, return_exceptions=True)
            self.asynchronous.clear()

        self.watchlist.clear()
        self.database.clear()

    async def _start(self, watchlist: List[Bubble]):
        await self.shutdown()
        self.watchlist = watchlist

        for exchange in self.settings.exchanges:
            symbols = [b.symbol for b in bubbles_filter(bubbles=watchlist, exchange=exchange.name)]
            if not symbols: continue

            callbacks = self._build_callbacks(alias=exchange.name, coins=symbols)
            if exchange.name == ALIAS_MEXC:
                for chunk in chunked(iterable=symbols, size=self._CHUNK_SIZE_MEXC):
                    self.asynchronous.append(
                        run_mexc(
                            url=exchange.websocket,
                            coins=symbols,
                            callbacks={c: callbacks[c] for c in chunk},
                        ),
                    )
            elif exchange.name == ALIAS_BYBIT:
                self.asynchronous.append(
                    run_bybit(
                        url=exchange.websocket,
                        coins=symbols,
                        callbacks=callbacks,
                    ),
                )
            elif exchange.name == ALIAS_BINANCE:
                self.asynchronous.append(
                    run_binance(
                        url=exchange.websocket,
                        coins=symbols,
                        callbacks=callbacks,
                    ),
                )
            elif exchange.name == ALIAS_COINBASE:
                for chunk in chunked(iterable=symbols, size=self._CHUNK_SIZE_COINBASE):
                    self.asynchronous.append(
                        run_coinbase(
                            url=exchange.websocket,
                            coins=chunk,
                            callbacks={c: callbacks[c] for c in chunk},
                        ),
                    )
            else:
                raise ValueError(f"Unknown exchange: {exchange.name}")

    def _build_callbacks(self, alias: str, coins: List[str]) -> StreamCallbacks:
        return {
            item: lambda price, timestamp, coin=item: self.database.insert_price(
                coin_name=coin, exch_name=alias, exch_price=price, timestamp=timestamp
            ) for item in coins
        }

async def main_loop(manager: MainManager, interval_seconds: int = 1800):
    while True:
        start = asyncio.get_event_loop().time()
        await manager.run()
        elapsed = asyncio.get_event_loop().time() - start
        await asyncio.sleep(max(0, int(interval_seconds - elapsed)))