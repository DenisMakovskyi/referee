import pytest

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.database import Database, Coin, Exchange, PriceRecord

@pytest.fixture
def db():
    return Database(url="sqlite:///:memory:")

def test_add_coin(db):
    # Перше створення
    btc = db.add_coin("BTC")
    assert isinstance(btc, Coin)
    assert btc.name == "BTC"
    # Повторний виклик - той самий запис
    same = db.add_coin("BTC")
    assert same.id == btc.id

def test_add_exchange(db):
    binance = db.add_exchange("binance")
    assert isinstance(binance, Exchange)
    assert binance.name == "binance"
    # Повторний виклик — той самий запис
    again = db.add_exchange("binance")
    assert again.id == binance.id

def test_insert_price(db):
    # Перший INSERT
    db.insert_price(coin_name="BTC", exch_name="binance", exch_price=50000.0, timestamp=1_600_000_000)
    # Перевіримо, що запис один і значення збігаються
    with Session(db.engine) as sess:
        result = sess.execute(
            select(PriceRecord)
            .join(Coin, PriceRecord.coin_id == Coin.id)
            .join(Exchange, PriceRecord.exchange_id == Exchange.id)
            .where(Coin.name == "BTC", Exchange.name == "binance")
        ).scalar_one()
        assert result.price == pytest.approx(50000.0)
        assert result.timestamp == 1_600_000_000

    # Другий виклик з іншими даними повинен оновити існуючий запис
    db.insert_price(coin_name="BTC", exch_name="binance", exch_price=51000.5, timestamp=1_600_000_100)
    with Session(db.engine) as sess:
        # Перевіряємо що запис один і він оновився
        rows = sess.execute(
            select(PriceRecord)
            .join(Coin, PriceRecord.coin_id == Coin.id)
            .join(Exchange, PriceRecord.exchange_id == Exchange.id)
            .where(Coin.name == "BTC", Exchange.name == "binance")
        ).scalars().all()
        assert len(rows) == 1
        updated = rows[0]
        assert updated.price == pytest.approx(51000.5)
        assert updated.timestamp == 1_600_000_100