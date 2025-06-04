from src.db import Database


def test_insert_price(tmp_path):
    db_path = tmp_path / "test.db"
    db = Database(url=f"sqlite:///{db_path}")
    db.insert_price("btc", "binance", 1.0, 50000.0)
    table = db.get_table("btc", "binance")
    with db.engine.connect() as conn:
        rows = conn.execute(table.select()).fetchall()
        assert len(rows) == 1
        assert rows[0].binance == 50000.0
