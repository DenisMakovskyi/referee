from src.config import load_config


def test_load_config():
    settings = load_config()
    assert 'binance' in settings.exchanges
    assert 'btc' in settings.monitor
