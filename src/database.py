from __future__ import annotations

from pathlib import Path

from sqlalchemy import (
    Integer,
    Float,
    String,
    Column,
    ForeignKey,
    UniqueConstraint,
    text,
    create_engine,
    select,
)
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, relationship, declarative_base
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from src.basics import time_millis

_DB_PATH = Path(__file__).resolve().parents[1] / "data.db"
_TABLE_NAME_COIN = "coins"
_TABLE_NAME_EXCHANGE = "exchanges"
_TABLE_NAME_PRICE_RECORD = "price_records"

Base = declarative_base()

class Coin(Base):
    __tablename__ = _TABLE_NAME_COIN
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)

class Exchange(Base):
    __tablename__ = _TABLE_NAME_EXCHANGE
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), unique=True, nullable=False)

class PriceRecord(Base):
    __tablename__ = _TABLE_NAME_PRICE_RECORD
    id = Column(Integer, primary_key=True, autoincrement=True)
    coin_id = Column(Integer, ForeignKey("coins.id"), nullable=False)
    exchange_id = Column(Integer, ForeignKey("exchanges.id"), nullable=False)
    price = Column(Float, nullable=False)
    timestamp = Column(Integer, nullable=False)

    coin = relationship("Coin")
    exchange = relationship("Exchange")

    __table_args__ = (
        UniqueConstraint("coin_id", "exchange_id", name="uix_coin_exchange"),
    )

class Database:
    def __init__(self, url: str | None = None) -> None:
        if url is None:
            url = f"sqlite:///{_DB_PATH}"
        self.engine = create_engine(url, echo=False)
        Base.metadata.create_all(self.engine)

    def close(self) -> None:
        self.engine.dispose()

    def clear(self) -> None:
        with self.engine.begin() as connection:
            is_sqlite = self.engine.url.get_backend_name() == "sqlite"
            for table in reversed(Base.metadata.sorted_tables):
                connection.execute(text(f"DELETE FROM {table.name}"))
                if is_sqlite:
                    try:
                        connection.execute(text(f"DELETE FROM sqlite_sequence WHERE name='{table.name}'"))
                    except OperationalError as e:
                        if "no such table: sqlite_sequence" not in str(e):
                            raise

    def add_coin(self, name: str | None = None) -> Coin:
        with Session(self.engine) as session:
            coin = session.scalar(select(Coin).where(Coin.name == name))
            if coin is None:
                coin = Coin(name=name)
                session.add(coin)
                session.commit()
                session.refresh(coin)
            return coin

    def add_exchange(self, name: str | None = None) -> Exchange:
        with Session(self.engine) as session:
            exchange = session.scalar(select(Exchange).where(Exchange.name == name))
            if exchange is None:
                exchange = Exchange(name=name)
                session.add(exchange)
                session.commit()
                session.refresh(exchange)
            return exchange

    def insert_price(
            self,
            coin_name: str,
            exch_name: str,
            exch_price: float,
            timestamp: int | None = None
    ) -> None:
        if timestamp is None:
            timestamp = time_millis()

        with Session(self.engine) as session:
            coin = session.scalar(select(Coin).where(Coin.name == coin_name))
            if coin is None:
                coin = self.add_coin(coin_name)

            exchange = session.scalar(select(Exchange).where(Exchange.name == exch_name))
            if exchange is None:
                exchange = self.add_exchange(exch_name)

            statement = sqlite_insert(PriceRecord).values(
                coin_id=coin.id,
                exchange_id=exchange.id,
                price=exch_price,
                timestamp=timestamp,
            )
            statement = statement.on_conflict_do_update(
                index_elements=["coin_id", "exchange_id"],
                set_={
                    "price": statement.excluded.price,
                    "timestamp": statement.excluded.timestamp,
                },
            )
            session.execute(statement)
            session.commit()