from __future__ import annotations

import asyncio
from typing import Callable

from .config import load_config
from .db import Database
from .exchanges import binance


def make_callback(db: Database, token: str, exchange: str) -> Callable[[float, float], None]:
    def _callback(timestamp: float, price: float) -> None:
        db.insert_price(token, exchange, timestamp, price)
    return _callback


async def main() -> None:
    config = load_config()
    db = Database()
    tasks = []
    for exch_name, tokens in config.stock.items():
        exchange = config.exchanges[exch_name]
        for token in tokens:
            cb = make_callback(db, token, exch_name)
            if exch_name == "binance":
                tasks.append(binance.run(token, exchange, cb))
    if not tasks:
        return
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
