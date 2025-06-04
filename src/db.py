from __future__ import annotations

from pathlib import Path
from typing import Dict

from sqlalchemy import create_engine, Table, Column, Float, MetaData
from sqlalchemy.engine import Engine

DB_PATH = Path(__file__).resolve().parents[1] / "data.db"


class Database:
    def __init__(self, url: str | None = None) -> None:
        if url is None:
            url = f"sqlite:///{DB_PATH}"
        self.engine: Engine = create_engine(url)
        self.metadata = MetaData()
        self.tables: Dict[str, Table] = {}

    def get_table(self, token: str, exchange: str) -> Table:
        if token not in self.tables:
            table = Table(
                token,
                self.metadata,
                Column("timestamp", Float, primary_key=True),
            )
            self.tables[token] = table
        else:
            table = self.tables[token]

        if exchange not in table.c:
            table.append_column(Column(exchange, Float))
        table.create(self.engine, checkfirst=True)
        return table

    def insert_price(self, token: str, exchange: str, timestamp: float, price: float) -> None:
        table = self.get_table(token, exchange)
        with self.engine.begin() as conn:
            conn.execute(table.insert().values(timestamp=timestamp, **{exchange: price}))

