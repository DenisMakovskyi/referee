import asyncio

from typing import List

from src.basics import StreamCallbacks, chunked
from src.config import Stock
from src.bubble import bubbles_watchlist
from src.database import Base, Database
from src.stocks.bybit import ALIAS as ALIAS_BYBIT, run as run_bybit
from src.stocks.binance import ALIAS as ALIAS_BINANCE, run as run_binance
from src.stocks.coinbase import ALIAS as ALIAS_COINBASE, run as run_coinbase

_COINBASE_CHUNK_SIZE = 25

class MainManager:
    def __init__(self, db: Database, stocks: List[Stock]):
        self.db = db
        self.stocks = stocks
        self.watchlist: List[str] = []
        self.asynchronous: List[asyncio.Task] = []

    async def run(self) -> bool:
        bubbles = bubbles_watchlist()
        watchlist = [b.symbol for b in bubbles] or []
        if set(watchlist) == set(self.watchlist):
            return False

        await self._start_all(watchlist)
        return True

    async def shutdown(self):
        for task in self.asynchronous:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.asynchronous, return_exceptions=True)
        self.asynchronous.clear()
        self.watchlist.clear()
        self._wipe_database()

    async def _start_all(self, watchlist: List[str]):
        await self.shutdown()
        self.watchlist = watchlist

        for stock in self.stocks:
            callbacks = self._build_callbacks(stock=stock.name, coins=watchlist)
            if stock.name == ALIAS_BYBIT:
                self.asynchronous.append(run_bybit(url=stock.websocket, coins=watchlist, callbacks=callbacks))
            elif stock.name == ALIAS_BINANCE:
                self.asynchronous.append(run_binance(url=stock.websocket, coins=watchlist, callbacks=callbacks))
            elif stock.name == ALIAS_COINBASE:
                for chunk in chunked(iterable=self.watchlist, size=_COINBASE_CHUNK_SIZE):
                    self.asynchronous.append(
                        run_coinbase(url=stock.websocket, coins=chunk, callbacks={c: callbacks[c] for c in chunk}),
                    )
            else:
                raise ValueError(f"Unknown stock: {stock.name}")

    def _wipe_database(self):
        Base.metadata.drop_all(self.db.engine)
        Base.metadata.create_all(self.db.engine)

    def _build_callbacks(self, stock: str, coins: List[str]) -> StreamCallbacks:
        return {
            item: lambda price, timestamp, coin=item: self.db.insert_price(
                coin_name=coin, stock_name=stock, stock_price=price, timestamp=timestamp
            ) for item in coins
        }

async def main_loop(manager: MainManager, interval_seconds: int = 3660):
    while True:
        start = asyncio.get_event_loop().time()
        await manager.run()
        elapsed = asyncio.get_event_loop().time() - start
        await asyncio.sleep(max(0, int(interval_seconds - elapsed)))