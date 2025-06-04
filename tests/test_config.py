from src.config import load_config


def test_load_config():
    settings = load_config()
    assert 'binance' in settings.exchanges
    assert 'binance' in settings.stock
    assert 'btc' in settings.stock['binance']
