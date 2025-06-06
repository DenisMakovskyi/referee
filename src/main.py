from __future__ import annotations

import asyncio

from typing import Callable
from config import load_config
from database import Database
from src.stocks import binance

def db_callback(db: Database, coin: str, stock: str) -> Callable[[float, int], None]:
    def _callback(price: float, timestamp: int) -> None:
        db.insert_price(coin=coin, stock=stock, price=price, timestamp=timestamp)
    return _callback

async def main() -> None:
    config = load_config()
    database = Database()
    async_tasks = []
    for stock_name, coins in config.observables.items():
        stock = config.stocks[stock_name]
        for coin in coins:
            callback = db_callback(db=database, coin=coin, stock=stock_name)
            if stock_name == "binance":
                async_tasks.append(binance.run(coin=coin, stock=stock, callback=callback))
    if not async_tasks:
        return
    await asyncio.gather(*async_tasks)

if __name__ == "__main__":
    asyncio.run(main())
