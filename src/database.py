from __future__ import annotations

from typing import Dict
from pathlib import Path

from sqlalchemy import create_engine, Float, Table, Column, MetaData
from sqlalchemy.engine import Engine

DB_PATH = Path(__file__).resolve().parents[1] / "data.db"

class Database:
    def __init__(self, url: str | None = None) -> None:
        if url is None:
            url = f"sqlite:///{DB_PATH}"
        self.engine: Engine = create_engine(url)
        self.tables: Dict[str, Table] = {}
        self.metadata = MetaData()

    def get_table(self, coin: str, stock: str) -> Table:
        if coin not in self.tables:
            table = Table(
                coin,
                self.metadata,
                Column("timestamp", Float, primary_key=True),
            )
            self.tables[coin] = table
        else:
            table = self.tables[coin]

        if stock not in table.c:
            table.append_column(Column(stock, Float))
        table.create(self.engine, checkfirst=True)
        return table

    def insert_price(self, coin: str, stock: str, price: float, timestamp: int) -> None:
        table = self.get_table(coin, stock)
        with self.engine.begin() as conn:
            conn.execute(table.insert().values(timestamp=timestamp, **{stock: price}))

