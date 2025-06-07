from __future__ import annotations

import signal
import asyncio

from typing import Callable
from config import load_config
from database import Database

from src.stocks import bybit
from src.stocks.bybit import ALIAS as ALIAS_BYBIT
from src.stocks import binance
from src.stocks.binance import ALIAS as ALIAS_BINANCE
from src.stocks import coinbase
from src.stocks.coinbase import ALIAS as ALIAS_COINBASE

def _db_callback(db: Database, coin: str, stock: str) -> Callable[[float, int], None]:
    def _callback(price: float, timestamp: int) -> None:
        db.insert_price(
            coin_name=coin,
            stock_name=stock,
            stock_price=price,
            timestamp=timestamp,
        )
    return _callback

async def main(shutdown_event: asyncio.Event) -> None:
    config = load_config()
    database = Database()
    promises: list[asyncio.Task] = []
    for stock in config.stocks:
        for coin in config.observables:
            callback = _db_callback(db=database, coin=coin, stock=stock.name)
            if stock.name == ALIAS_BYBIT:
                promises.append(bybit.run(url=stock.websocket, coin=coin, callback=callback))
            elif stock.name == ALIAS_BINANCE:
                promises.append(binance.run(url=stock.websocket, coin=coin, callback=callback))
            elif stock.name == ALIAS_COINBASE:
                promises.append(coinbase.run(url=stock.websocket, coin=coin, callback=callback))
            else:
                raise ValueError(f"Unknown stock name: {stock.name}")
    if not promises:
        database.close()
        return

    shutdown_task = asyncio.create_task(shutdown_event.wait())
    (completed_tasks, _) = await asyncio.wait(
        promises + [shutdown_task],
        return_when=asyncio.FIRST_COMPLETED,
    )
    if shutdown_task in completed_tasks:
        for p in promises:
            if not p.done():
                p.cancel()
        await asyncio.gather(*promises, return_exceptions=True)

    database.close()

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    shutdown = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)

    try:
        loop.run_until_complete(main(shutdown))
    finally:
        loop.close()
        print("🔌 Service has been stopped.")
