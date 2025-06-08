from __future__ import annotations

import time

from pathlib import Path

from sqlalchemy import (
    Integer,
    Float,
    String,
    Column,
    ForeignKey,
    UniqueConstraint,
    create_engine,
    select,
)
from sqlalchemy.orm import Session, relationship, declarative_base
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

_DB_PATH = Path(__file__).resolve().parents[1] / "data.db"
_TABLE_NAME_COIN = "coins"
_TABLE_NAME_STOCK = "stocks"
_TABLE_NAME_PRICE_RECORD = "price_records"

Base = declarative_base()

class Coin(Base):
    __tablename__ = _TABLE_NAME_COIN
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)

class Stock(Base):
    __tablename__ = _TABLE_NAME_STOCK
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)

class PriceRecord(Base):
    __tablename__ = _TABLE_NAME_PRICE_RECORD
    id = Column(Integer, primary_key=True, autoincrement=True)
    coin_id = Column(Integer, ForeignKey("coins.id"), nullable=False)
    stock_id = Column(Integer, ForeignKey("stocks.id"), nullable=False)
    price = Column(Float, nullable=False)
    timestamp = Column(Integer, nullable=False)

    coin = relationship("Coin")
    stock = relationship("Stock")

    __table_args__ = (
        UniqueConstraint("coin_id", "stock_id", name="uix_coin_stock"),
    )

class Database:
    def __init__(self, url: str | None = None) -> None:
        if url is None:
            url = f"sqlite:///{_DB_PATH}"
        self.engine = create_engine(url, echo=False)
        Base.metadata.create_all(self.engine)

    def close(self) -> None:
        self.engine.dispose()

    def add_coin(self, name: str | None = None) -> Coin:
        with Session(self.engine) as session:
            coin = session.scalar(select(Coin).where(Coin.name == name))
            if coin is None:
                coin = Coin(name=name)
                session.add(coin)
                session.commit()
                session.refresh(coin)
            return coin

    def add_stock(self, name: str | None = None) -> Stock:
        with Session(self.engine) as session:
            stock = session.scalar(select(Stock).where(Stock.name == name))
            if stock is None:
                stock = Stock(name=name)
                session.add(stock)
                session.commit()
                session.refresh(stock)
            return stock

    def insert_price(
            self,
            coin_name: str,
            stock_name: str,
            stock_price: float,
            timestamp: int | None = None
    ) -> None:
        if timestamp is None:
            timestamp = int(time.time())

        with Session(self.engine) as session:
            coin = session.scalar(select(Coin).where(Coin.name == coin_name))
            if coin is None:
                coin = self.add_coin(coin_name)

            stock = session.scalar(select(Stock).where(Stock.name == stock_name))
            if stock is None:
                stock = self.add_stock(stock_name)

            statement = sqlite_insert(PriceRecord).values(
                coin_id=coin.id,
                stock_id=stock.id,
                price=stock_price,
                timestamp=timestamp,
            )
            statement = statement.on_conflict_do_update(
                index_elements=["coin_id", "stock_id"],
                set_={
                    "price": statement.excluded.price,
                    "timestamp": statement.excluded.timestamp,
                },
            )
            session.execute(statement)
            session.commit()