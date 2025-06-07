import pytest

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.database import Database, Coin, Stock, PriceRecord

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

def test_add_stock(db):
    binance = db.add_stock("binance")
    assert isinstance(binance, Stock)
    assert binance.name == "binance"
    # Повторний виклик — той самий запис
    again = db.add_stock("binance")
    assert again.id == binance.id

def test_insert_price_upsert(db):
    # Перший INSERT
    db.insert_price(coin_name="BTC", stock_name="binance", stock_price=50000.0, timestamp=1_600_000_000)
    # Перевіримо, що запис один і значення збігаються
    with Session(db.engine) as sess:
        result = sess.execute(
            select(PriceRecord)
            .join(Coin, PriceRecord.coin_id == Coin.id)
            .join(Stock, PriceRecord.stock_id == Stock.id)
            .where(Coin.name == "BTC", Stock.name == "binance")
        ).scalar_one()
        assert result.price == pytest.approx(50000.0)
        assert result.timestamp == 1_600_000_000

    # Другий виклик іншими даними — повинен оновити існуючий запис
    db.insert_price(coin_name="BTC", stock_name="binance", stock_price=51000.5, timestamp=1_600_000_100)
    with Session(db.engine) as sess:
        # Перевіряємо що запис один і він оновився
        rows = sess.execute(
            select(PriceRecord)
            .join(Coin, PriceRecord.coin_id == Coin.id)
            .join(Stock, PriceRecord.stock_id == Stock.id)
            .where(Coin.name == "BTC", Stock.name == "binance")
        ).scalars().all()
        assert len(rows) == 1
        updated = rows[0]
        assert updated.price == pytest.approx(51000.5)
        assert updated.timestamp == 1_600_000_100
